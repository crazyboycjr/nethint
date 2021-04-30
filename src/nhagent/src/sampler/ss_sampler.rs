use crate::cluster::CLUSTER;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4, SocketAddrV6};
use std::sync::mpsc;
use thiserror::Error;
use std::time::SystemTime;
use crate::timing::{self, TimeList};

use nethint::counterunit::{CounterType, CounterUnit};

const _SS_CMD: &'static str = "ss -tuni";
pub struct SsSampler {
    interval_ms: u64,
    listen_port: u16,
    handle: Option<std::thread::JoinHandle<()>>,
    tx: mpsc::Sender<(TimeList, Vec<CounterUnit>)>,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SockAddrPair {
    pub local_addr: SocketAddrV4,
    pub peer_addr: SocketAddrV4,
}

impl SockAddrPair {
    #[inline]
    fn rev(&self) -> SockAddrPair {
        Self {
            local_addr: self.peer_addr,
            peer_addr: self.local_addr,
        }
    }
}

// tcp   ESTAB      0      0                 127.0.0.1:58096      127.0.0.1:8000
// 	 cubic wscale:7,7 rto:203.333 rtt:0.023/0.008 ato:40 mss:32768 pmtu:65535 rcvmss:32768 advmss:65483 cwnd:10 bytes_sent:97299 bytes_acked:97300 bytes_received:1990443 segs_out:12056 segs_in:23811 data_segs_out:11801 data_segs_in:12011 send 113975652174bps lastsnd:24297 lastrcv:24297 lastack:24297 pacing_rate 226719135128bps delivery_rate 32768000000bps delivered:11802 app_limited busy:506ms rcv_rtt:75.075 rcv_space:65495 rcv_ssthresh:65495 minrtt:0.008

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SsTcpFlow {
    addr_pair: SockAddrPair,
    bytes_sent: u64,
    bytes_received: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SsTcpFlows(Vec<SsTcpFlow>);

#[derive(Debug, Error)]
#[error("Parse SsTcpFlows error: stage: {0}")]
pub struct ParseError(usize);

impl std::str::FromStr for SsTcpFlows {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut res = Vec::new();
        let mut last_conn = None;

        for line in s.trim().split('\n') {
            if line.starts_with("tcp") {
                let tokens: Vec<&str> = line.split(' ').filter(|x| !x.is_empty()).collect();
                let n = tokens.len();
                assert_eq!(n, 6, "tokens: {:?}", tokens);
                if tokens[n - 2].parse::<SocketAddrV6>().is_ok()
                    || tokens[n - 1].parse::<SocketAddrV6>().is_ok()
                {
                    continue;
                }
                last_conn = Some(SockAddrPair {
                    local_addr: tokens[n - 2].parse().map_err(|_| ParseError(1))?,
                    peer_addr: tokens[n - 1].parse().map_err(|_| ParseError(2))?,
                });
            } else if let Some(addr_pair) = last_conn {
                let kvs: HashMap<&str, &str> = line
                    .split(' ')
                    .filter(|x| x.contains(':'))
                    .map(|x| x.split_once(':').unwrap())
                    .collect();
                let bytes_sent = kvs
                    .get("bytes_sent")
                    .unwrap_or(&"0")
                    .parse()
                    .map_err(|_| ParseError(3))?;
                let bytes_received = kvs
                    .get("bytes_received")
                    .unwrap_or(&"0")
                    .parse()
                    .map_err(|_| ParseError(4))?;
                res.push(SsTcpFlow {
                    addr_pair,
                    bytes_sent,
                    bytes_received,
                });
                last_conn = None;
            }
        }

        Ok(SsTcpFlows(res))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_ss() {
        let input = r#"
Netid State      Recv-Q Send-Q        Local Address:Port    Peer Address:Port Process
udp   ESTAB      0      0      192.168.2.110%wlp2s0:68       192.168.2.1:67
tcp   ESTAB      0      0                 127.0.0.1:8000       127.0.0.1:58096
	 cubic wscale:7,7 rto:203.333 rtt:0.103/0.022 ato:40 mss:32768 pmtu:65535 rcvmss:3032 advmss:65483 cwnd:10 bytes_sent:1990443 bytes_acked:1990443 bytes_received:97299 segs_out:23810 segs_in:12056 data_segs_out:12011 data_segs_in:11801 send 25450873786bps lastsnd:24297 lastrcv:24297 lastack:24297 pacing_rate 50533783128bps delivery_rate 23831272720bps delivered:12012 app_limited busy:1556ms rcv_rtt:1.25 rcv_space:65483 rcv_ssthresh:65483 minrtt:0.019
"#;

        let ss_flows: SsTcpFlows = input.parse().unwrap();
        assert_eq!(
            ss_flows,
            SsTcpFlows(vec![SsTcpFlow {
                addr_pair: SockAddrPair {
                    local_addr: "127.0.0.1:8000".parse().unwrap(),
                    peer_addr: "127.0.0.1:58096".parse().unwrap(),
                },
                bytes_sent: 1990443,
                bytes_received: 97299,
            },],)
        );
    }
}

#[derive(Debug, Clone)]
struct FlowStats {
    bytes: u64,
    create_time: std::time::Instant,
    update_time: std::time::Instant,
}

#[derive(Debug, Clone, Default)]
struct FlowTable {
    table: HashMap<SockAddrPair, FlowStats>,
}

impl SsSampler {
    pub fn new(interval_ms: u64, listen_port: u16, tx: mpsc::Sender<(TimeList, Vec<CounterUnit>)>) -> Self {
        SsSampler {
            interval_ms,
            listen_port,
            handle: None,
            tx,
        }
    }

    pub fn run(&mut self) {
        log::info!("starting ss sampler thread...");

        let local_ip_table = get_local_ip_table().expect("get local_ip_table failed");
        let mut flow_table = FlowTable::default();
        let tx = self.tx.clone();
        let interval = std::time::Duration::from_millis(self.interval_ms);

        let listen_addr = ("0.0.0.0", self.listen_port);
        let sock = std::net::UdpSocket::bind(&listen_addr)
            .unwrap_or_else(|_| panic!("couldn't listen on {:?}", listen_addr));
        sock.set_read_timeout(Some(std::time::Duration::from_millis(1)))
            .expect("set timeout failed");
        let mut buf = vec![0; 65507];
        let mut last_ts = std::time::Instant::now();
        let mut time_list = TimeList::new();

        // group the flows by (src, dst) pair
        let mut flow_group: HashMap<(IpAddr, IpAddr), u64> = Default::default();

        self.handle = Some(std::thread::spawn(move || loop {
            let now = std::time::Instant::now();
            if now - last_ts >= interval {
                let mut vnode_counter: HashMap<IpAddr, CounterUnit> = local_ip_table
                    .iter()
                    .map(|(&ip, name)| (ip, CounterUnit::new(name)))
                    .collect();
                let pcluster = CLUSTER.lock().unwrap();

                use CounterType::*;
                for (f, &delta) in &flow_group {
                    if local_ip_table.contains_key(&f.0) {
                        // tx
                        vnode_counter
                            .entry(f.0)
                            .and_modify(|e| e.add_flow(Tx, delta));
                        if pcluster.is_ip_within_rack(&f.1) {
                            // tx_in
                            vnode_counter
                                .entry(f.0)
                                .and_modify(|e| e.add_flow(TxIn, delta));
                        }
                    }
                    if local_ip_table.contains_key(&f.1) {
                        // rx
                        vnode_counter
                            .entry(f.1)
                            .and_modify(|e| e.add_flow(Rx, delta));
                        if pcluster.is_ip_within_rack(&f.0) {
                            // rx_in
                            vnode_counter
                                .entry(f.1)
                                .and_modify(|e| e.add_flow(RxIn, delta));
                        }
                    }
                }

                // release the lock, do not hold it for too long
                std::mem::drop(pcluster);

                // vnode_counter hash_map to vec
                time_list.push(timing::ON_SAMPLED, SystemTime::now());
                let counter_unit: Vec<CounterUnit> = vnode_counter
                    .values()
                    .filter(|v| !v.is_empty())
                    .cloned()
                    .collect();
                tx.send((time_list.clone(), counter_unit)).unwrap();

                // update the timer
                last_ts = std::time::Instant::now();
                // clear the accumulated state
                flow_group.clear();
            }

            match sock.recv_from(&mut buf) {
                Ok((nbytes, _src)) => {
                    // do not check the src
                    if nbytes >= 65507 {
                        panic!(
                            "read {} bytes from UDP payload, there might more data is cut off",
                            nbytes
                        );
                    }
                    let buf = &buf[..nbytes];
                    let (ts, ss_flows): (SystemTime, SsTcpFlows) =
                        bincode::deserialize(&buf).expect("deserialize ss_flows failed");
                    log::trace!("ts: {:?}, {:?}", ts, ss_flows);
                    time_list.clear();
                    time_list.push(timing::ON_COLLECTED, ts);

                    let now = std::time::Instant::now();
                    for ss_flow in ss_flows.0 {
                        let ap = ss_flow.addr_pair;
                        for (addr_pair, bytes) in
                            vec![(ap, ss_flow.bytes_sent), (ap.rev(), ss_flow.bytes_received)]
                        {
                            let delta = match flow_table.table.entry(addr_pair) {
                                Entry::Vacant(v) => {
                                    v.insert(FlowStats {
                                        bytes,
                                        create_time: now,
                                        update_time: now,
                                    });
                                    bytes
                                }
                                Entry::Occupied(mut o) => {
                                    let f = o.get_mut();
                                    let delta = if bytes < f.bytes {
                                        // it might be a new flow
                                        f.create_time = now;
                                        bytes
                                    } else {
                                        bytes - f.bytes
                                    };
                                    f.bytes = bytes;
                                    f.update_time = now;
                                    delta
                                }
                            };

                            let local_ip: IpAddr = (*addr_pair.local_addr.ip()).into();
                            let peer_ip: IpAddr = (*addr_pair.peer_addr.ip()).into();
                            if delta > 0 {
                                // if both IPs are local, the delta will count in both the tx and
                                // rx traffic
                                if local_ip_table.contains_key(&local_ip) {
                                    // tx
                                    *flow_group.entry((local_ip, peer_ip)).or_insert(0) += delta;
                                }
                                if local_ip_table.contains_key(&peer_ip) {
                                    // rx
                                    *flow_group.entry((local_ip, peer_ip)).or_insert(0) += delta;
                                }
                                if !local_ip_table.contains_key(&local_ip)
                                    && !local_ip_table.contains_key(&peer_ip)
                                {
                                    // we can sliently ignore these cases such as 127.0.0.1, or
                                    // IPs from other subnets
                                    log::debug!(
                                        "ss_sampler: {:?} does not come from or target to this server",
                                        ss_flow,
                                    );
                                }
                            }
                        }
                    }

                    // TODO(cjr): remove expired entries, but it should be OK not to do this, as
                    // the number of entries are very limited.
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => {
                    panic!("SsSampler, recv_from: {}", e)
                }
            }
        }));
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.handle
            .expect("ss sampler thread failed to start")
            .join()
    }
}

pub fn get_local_ip_table() -> anyhow::Result<HashMap<IpAddr, String>> {
    let mut local_ip_table = HashMap::default(); // map eth to node name
    let my_ip = utils::net::get_primary_ipv4("rdma0").unwrap();
    let ds: Vec<u8> = my_ip.split('.').map(|x| x.parse().unwrap()).collect();

    // derive the ip addresses from the ip of the host
    for i in 1..30 {
        let addr = Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3] + i);
        let vfid = i - 1;
        local_ip_table.insert(addr.into(), vfid.to_string());
    }

    // also insert host IP to capture background traffic
    local_ip_table.insert(
        Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3]).into(),
        "30".to_owned(),
    );

    Ok(local_ip_table)
}
