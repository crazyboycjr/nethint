use crate::cluster::CLUSTER;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};
use std::sync::mpsc;
use thiserror::Error;

use nethint::counterunit::{CounterType, CounterUnit};

const _SS_CMD: &'static str = "ss -tuni";
pub struct SsSampler {
    interval_ms: u64,
    listen_port: u16,
    handle: Option<std::thread::JoinHandle<()>>,
    tx: mpsc::Sender<Vec<CounterUnit>>,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct SockAddrPair {
    local_addr: SocketAddrV4,
    peer_addr: SocketAddrV4,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct SsTcpFlow {
    addr_pair: SockAddrPair,
    bytes_sent: u64,
    bytes_received: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SsTcpFlows(Vec<SsTcpFlow>);

#[derive(Debug, Error)]
#[error("Parse SsTcpFlows error: stage: {0}")]
struct ParseError(usize);

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
    pub fn new(interval_ms: u64, listen_port: u16, tx: mpsc::Sender<Vec<CounterUnit>>) -> Self {
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

        let listen_addr = ("0.0.0.0", self.listen_port);
        let sock = std::net::UdpSocket::bind(&listen_addr)
            .unwrap_or_else(|_| panic!("could listen on {:?}", listen_addr));
        sock.set_read_timeout(Some(std::time::Duration::from_millis(self.interval_ms)))
            .expect("set timeout failed");
        let mut buf = vec![0; 65507];
        self.handle = Some(std::thread::spawn(move || loop {
            match sock.recv_from(&mut buf) {
                Ok((nbytes, _src)) => {
                    // do not check the src
                    let buf = &buf[..nbytes];
                    let data = std::str::from_utf8(buf).expect("from_utf8");
                    let ss_flows: SsTcpFlows = data.parse().unwrap();
                    log::trace!("{:?}", ss_flows);

                    let pcluster = CLUSTER.lock().unwrap();
                    let mut vnode_counter: HashMap<IpAddr, CounterUnit> = Default::default();
                    for (&ip, vnodename) in local_ip_table.iter() {
                        vnode_counter
                            .insert(ip, CounterUnit::new(vnodename))
                            .unwrap_none();
                    }

                    let now = std::time::Instant::now();
                    for ss_flow in ss_flows.0 {
                        let ap = ss_flow.addr_pair;
                        for (addr_pair, bytes) in
                            vec![(ap, ss_flow.bytes_sent), (ap, ss_flow.bytes_received)]
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

                            use CounterType::*;
                            let ip_src: IpAddr = (*addr_pair.local_addr.ip()).into();
                            let ip_dst: IpAddr = (*addr_pair.peer_addr.ip()).into();
                            if delta > 0 {
                                if local_ip_table.contains_key(&ip_src) {
                                    // tx
                                    vnode_counter
                                        .entry(ip_src)
                                        .and_modify(|e| e.add_flow(Tx, delta));
                                    if pcluster.is_ip_within_rack(&ip_dst) {
                                        // tx_in
                                        vnode_counter
                                            .entry(ip_src)
                                            .and_modify(|e| e.add_flow(TxIn, delta));
                                    }
                                } else if local_ip_table.contains_key(&ip_dst) {
                                    // rx
                                    vnode_counter
                                        .entry(ip_dst)
                                        .and_modify(|e| e.add_flow(Rx, delta));
                                    if pcluster.is_ip_within_rack(&ip_src) {
                                        // rx_in
                                        vnode_counter
                                            .entry(ip_dst)
                                            .and_modify(|e| e.add_flow(RxIn, delta));
                                    }
                                } else {
                                    log::warn!(
                                        "ss_sampler: {:?} does not come from or target to this server",
                                        ss_flow,
                                    );
                                }
                            }
                        }
                    }
                    // release the lock, do not hold it for too long
                    std::mem::drop(pcluster);

                    // vnode_counter hash_map to vec
                    let counter_unit: Vec<CounterUnit> = vnode_counter.values().cloned().collect();
                    tx.send(counter_unit).unwrap();

                    // TODO(cjr): remove expired entries
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

    Ok(local_ip_table)
}
