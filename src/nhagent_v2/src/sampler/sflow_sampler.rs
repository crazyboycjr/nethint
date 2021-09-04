use crate::sdn_controller;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{BufReader, Cursor};
use std::net::IpAddr;
use std::net::UdpSocket;
use std::sync::mpsc;

use nethint::cluster::LinkIx;
use nethint::counterunit::{CounterType, CounterUnit};

use etherparse::PacketHeaders;
use sflow::{Datagram, Decodeable, FlowRecord, SampleRecord};

#[derive(Debug, Clone, Default)]
struct FlowTable {
    table: HashMap<FiveTuple, u64>,
}

#[derive(Debug, Clone)]
struct Flow {
    // we use five-tuple as the label instead of the ipv6 label field because five-tuple is more
    // general.
    label: FiveTuple,
    // the `bytes` field is counted from frame length which is more suitable for our scenario
    bytes: u64,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct FiveTuple {
    saddr: IpAddr,
    daddr: IpAddr,
    sport: u16,
    dport: u16,
    protocol: u8,
}

impl Default for FiveTuple {
    fn default() -> Self {
        FiveTuple {
            saddr: [0u8, 0, 0, 0].into(),
            daddr: [0u8, 0, 0, 0].into(),
            sport: 0,
            dport: 0,
            protocol: 0,
        }
    }
}

impl TryFrom<FlowRecord> for FiveTuple {
    // type Error = Error;
    type Error = &'static str;
    fn try_from(flow_record: FlowRecord) -> Result<Self, Self::Error> {
        match flow_record {
            FlowRecord::SampledHeader(sampled_header) => {
                match PacketHeaders::from_ethernet_slice(&sampled_header.header) {
                    Err(_err) => Err("etherparse::from_ethernet_slice error"),
                    Ok(frame) => {
                        log::debug!("ether parse frame: {:?}", frame);

                        let mut five_tuple = FiveTuple::default();
                        if let Some(ip_header) = frame.ip {
                            match ip_header {
                                etherparse::IpHeader::Version4(ipv4) => {
                                    five_tuple.saddr = ipv4.source.into();
                                    five_tuple.daddr = ipv4.destination.into();
                                    five_tuple.protocol = ipv4.protocol;
                                }
                                etherparse::IpHeader::Version6(ipv6) => {
                                    five_tuple.saddr = ipv6.source.into();
                                    five_tuple.daddr = ipv6.destination.into();
                                    five_tuple.protocol = ipv6.next_header;
                                    // Or can we just use flow_label instead of five tuple for
                                    // ipv6?
                                }
                            }
                            if let Some(transport) = frame.transport {
                                match transport {
                                    etherparse::TransportHeader::Tcp(tcp) => {
                                        five_tuple.sport = tcp.source_port;
                                        five_tuple.dport = tcp.destination_port;
                                    }
                                    etherparse::TransportHeader::Udp(udp) => {
                                        five_tuple.sport = udp.source_port;
                                        five_tuple.dport = udp.destination_port;
                                    }
                                }

                                return Ok(five_tuple);
                            }
                        }

                        Err("ethernet frame parse failed")
                    }
                }
            }
            _ => {
                log::error!("flow_record: {:?}", flow_record);
                Err("not SampledHeader")
            }
        }
    }
}

impl TryFrom<FlowRecord> for Flow {
    type Error = &'static str;
    fn try_from(flow_record: FlowRecord) -> Result<Self, Self::Error> {
        let frame_len = match flow_record {
            FlowRecord::SampledHeader(ref sampled_header) => sampled_header.frame_length,
            _ => return Err("not SampledHeader"),
        };

        let five_tuple = FiveTuple::try_from(flow_record)?;
        Ok(Flow {
            label: five_tuple,
            bytes: frame_len as u64,
        })
    }
}

fn process_sflow_datagram(datagram: Datagram, flow_table: &mut FlowTable) {
    log::trace!("process_sflow_datagram: {:?}", datagram);

    for sample_record in datagram.sample_record {
        match sample_record {
            SampleRecord::FlowSample(flow_sample) => {
                let sampling_rate = flow_sample.sampling_rate as u64;
                let drops = flow_sample.drops;
                if drops > 0 {
                    log::warn!("{} drops detected, please increase sampling rate", drops);
                }

                for flow_record in flow_sample.flow_records {
                    match Flow::try_from(flow_record) {
                        Ok(flow) => {
                            // sampling_rate set to N means to sample 1/Nth of the packets in the
                            // monitored flows. That is to say, excatly every Nth packet is not
                            // counted.
                            let est_bytes = flow.bytes * sampling_rate;
                            *flow_table.table.entry(flow.label).or_insert(0) += est_bytes;
                        }
                        Err(err) if err == "not SampledHeader" => { }
                        Err(err) => {
                            log::error!("cannot extract five-tuple: {}", err);
                        }
                    }
                }
            }
            SampleRecord::Unknown => {
                // log::warn!("unknown flow sample received");
                // do nothing
            }
        }
    }
}

pub struct SFlowSampler {
    interval_ms: u64,
    listen_port: u16,
    handle: Option<std::thread::JoinHandle<()>>,
    // tx: mpsc::Sender<Vec<CounterUnit>>,
    tx: mpsc::Sender<HashMap<LinkIx, Vec<CounterUnit>>>,
}

impl SFlowSampler {
    pub fn new(interval_ms: u64, listen_port: u16, tx: mpsc::Sender<HashMap<LinkIx, Vec<CounterUnit>>>) -> Self {
        SFlowSampler {
            interval_ms,
            listen_port,
            handle: None,
            tx,
        }
    }

    pub fn run(&mut self) {
        log::info!("starting sFlow Sampler thread...");

        // let local_ip_table =
        //     sdn_controller::get_local_ip_table().expect("get local_ip_table failed");
        let rack_ip_table = sdn_controller::get_rack_ip_table().expect("get rack_ip_table failed");

        let mut flow_table = FlowTable::default();
        let tx = self.tx.clone();
        let interval = std::time::Duration::from_millis(self.interval_ms);

        let listen_addr = ("0.0.0.0", self.listen_port); // default 6343
        let sock = UdpSocket::bind(listen_addr)
            .unwrap_or_else(|_| panic!("could'n bind to or listen on {:?}", listen_addr));
        sock.set_read_timeout(Some(std::time::Duration::from_millis(1)))
            .expect("set udp socket timeout failed");

        let mut buf = vec![0u8; 65507];
        let mut last_ts = std::time::Instant::now();

        self.handle = Some(std::thread::spawn(move || loop {
            let now = std::time::Instant::now();
            if now - last_ts >= interval || !flow_table.table.is_empty() {
                // TODO(cjr): the code below is a piece of shit, must be refactored
                let mut vnode_counter_map: HashMap<LinkIx, HashMap<IpAddr, CounterUnit>> = {
                    rack_ip_table
                        .iter()
                        .filter(|&(ip, _)| {
                            if let IpAddr::V4(ipv4) = ip {
                                // ip address from a physical server
                                ipv4.octets()[3] % 32 == 2
                            } else {
                                false
                            }
                        })
                        .map(|(&ip, &link_ix)| match ip {
                            IpAddr::V4(ipv4) => {
                                let server_ip_table =
                                    sdn_controller::get_server_ip_table(&ipv4.to_string()).unwrap();
                                let vnode_counter: HashMap<IpAddr, CounterUnit> = server_ip_table
                                    .iter()
                                    .map(|(&ip, name)| (ip, CounterUnit::new(name)))
                                    .collect();
                                (link_ix, vnode_counter)
                            }
                            _ => unreachable!(),
                        })
                        .collect()
                };
                // let mut vnode_counter: HashMap<IpAddr, CounterUnit> = local_ip_table
                //     .iter()
                //     .map(|(&ip, name)| (ip, CounterUnit::new(name)))
                //     .collect();

                use CounterType::*;

                for (label, bytes) in flow_table.table.iter() {
                    if let Some(link_ix) = rack_ip_table.get(&label.saddr) {
                        // tx
                        vnode_counter_map
                            .get_mut(link_ix)
                            .unwrap()
                            .entry(label.saddr)
                            .and_modify(|e| e.add_flow(Tx, *bytes));
                        if sdn_controller::same_rack(&label.saddr, &label.daddr) {
                            // tx_in
                            vnode_counter_map
                                .get_mut(link_ix)
                                .unwrap()
                                .entry(label.saddr)
                                .and_modify(|e| e.add_flow(TxIn, *bytes));
                        }
                    }

                    if let Some(link_ix) = rack_ip_table.get(&label.daddr) {
                        // rx
                        vnode_counter_map
                            .get_mut(link_ix)
                            .unwrap()
                            .entry(label.daddr)
                            .and_modify(|e| e.add_flow(Rx, *bytes));
                        if sdn_controller::same_rack(&label.saddr, &label.daddr) {
                            // rx_in
                            vnode_counter_map
                                .get_mut(link_ix)
                                .unwrap()
                                .entry(label.daddr)
                                .and_modify(|e| e.add_flow(RxIn, *bytes));
                        }
                    }

                    // if local_ip_table.contains_key(&label.saddr) {
                    //     // tx
                    //     vnode_counter
                    //         .entry(label.saddr)
                    //         .and_modify(|e| e.add_flow(Tx, *bytes));
                    //     if sdn_controller::same_rack(&label.saddr, &label.daddr) {
                    //         // tx_in
                    //         vnode_counter
                    //             .entry(label.saddr)
                    //             .and_modify(|e| e.add_flow(TxIn, *bytes));
                    //     }
                    // }
                    // if local_ip_table.contains_key(&label.daddr) {
                    //     // rx
                    //     vnode_counter
                    //         .entry(label.daddr)
                    //         .and_modify(|e| e.add_flow(Rx, *bytes));
                    //     if sdn_controller::same_rack(&label.saddr, &label.daddr) {
                    //         // rx_in
                    //         vnode_counter
                    //             .entry(label.daddr)
                    //             .and_modify(|e| e.add_flow(RxIn, *bytes));
                    //     }
                    // }
                }

                // convert vnode_counter hash_map to vec
                // let counter_unit: Vec<CounterUnit> = vnode_counter
                //     .values()
                //     .filter(|v| !v.is_empty())
                //     .cloned()
                //     .collect();
                let counter_unit: HashMap<LinkIx, Vec<CounterUnit>> = vnode_counter_map
                    .into_iter()
                    .map(|(link_ix, vnode_counter)| {
                        (
                            link_ix,
                            vnode_counter
                                .values()
                                .filter(|v| !v.is_empty())
                                .cloned()
                                .collect(),
                        )
                    })
                    .collect();
                tx.send(counter_unit).unwrap();

                // update the timer
                last_ts = std::time::Instant::now();
                // clear the accumulated state
                flow_table.table.clear();
            }

            match sock.recv_from(&mut buf) {
                Ok((nbytes, _addr)) => {
                    let cursor = Cursor::new(&buf[..nbytes]);
                    let mut reader = BufReader::new(cursor);
                    match Datagram::read_and_decode(&mut reader) {
                        Ok(datagram) => {
                            process_sflow_datagram(datagram, &mut flow_table);
                        }
                        Err(_) => {
                            log::warn!("sFlow Datagram decoding failed");
                            // do nothing
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => {
                    panic!("sFlow Sampler, recv_from: {}", e)
                }
            }
        }));
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.handle
            .expect("sFlow Sampler thread failed to start")
            .join()
    }
}
