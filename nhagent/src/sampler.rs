use std::{borrow::Borrow, process::Command};
use std::sync::mpsc;
use std::{collections::HashMap, convert::TryInto};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::utils;
use crate::cluster::CLUSTER;

const _OVS_CMD: &'static str = "ovs-appctl dpctl/dump-flows type=all -m";
// sudo ovs-appctl dpctl/dump-flows type=all -m
// This will give results like below. All type of flows are displayed!
// recirc_id(0),in_port(2),eth(src=02:49:61:d4:70:e8,dst=02:bc:b6:ff:bf:97),eth_type(0x0800),ipv4(frag=no), packets:584316, bytes:36666200, used:4.940s, actions:3
// recirc_id(0),in_port(2),eth(src=02:49:61:d4:70:e8,dst=02:bc:b6:ff:bf:97),eth_type(0x0806), packets:0, bytes:0, used:3.110s, actions:3
// recirc_id(0),in_port(3),eth(src=02:bc:b6:ff:bf:97,dst=02:49:61:d4:70:e8),eth_type(0x0800),ipv4(frag=no), packets:29461402, bytes:34366302886, used:4.940s, actions:2
// recirc_id(0),in_port(3),eth(src=02:bc:b6:ff:bf:97,dst=02:49:61:d4:70:e8),eth_type(0x0806), packets:2, bytes:120, used:2.090s, actions:2
// recirc_id(0),in_port(3),eth(src=1c:34:da:a5:55:94,dst=01:80:c2:00:00:0e),eth_type(0x88cc), packets:0, bytes:0, used:4.340s, actions:drop
// recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1b:06,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.162,tip=192.168.211.2,op=1/0xff), packets:2120, bytes:127200, used:0.141s, actions:1,2
// recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1a:4e,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.130,tip=192.168.211.2,op=1/0xff), packets:2121, bytes:127260, used:0.633s, actions:1,2
// recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1b:5a,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.194,tip=192.168.211.2,op=1/0xff), packets:2120, bytes:127200, used:0.401s, actions:1,2
// recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1b:26,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.34,tip=192.168.211.2,op=1/0xff), packets:3120, bytes:187200, used:0.721s, actions:1,2
// recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1a:4a,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.66,tip=192.168.211.2,op=1/0xff), packets:2118, bytes:127080, used:0.845s, actions:1,2
pub struct OvsSampler {
    interval_ms: u64,
    handle: Option<std::thread::JoinHandle<()>>,
    tx: mpsc::Sender<Vec<CounterUnit>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EthAddr([u8; 6]);

impl std::str::FromStr for EthAddr {
    type Err = EthParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let addr: Vec<_> = s
            .split(':')
            .map(|x| u8::from_str_radix(x, 16))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| EthParseError(0))?;
        Ok(EthAddr(addr.try_into().map_err(|_| EthParseError(1))?))
    }
}

#[derive(Error, Debug)]
#[error("Parse eth pair error: stage {0}")]
pub struct EthParseError(usize);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
struct Eth {
    src: EthAddr,
    dst: EthAddr,
}

impl std::str::FromStr for Eth {
    type Err = EthParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (src, dst) = s.split_once(',').ok_or(EthParseError(2))?;
        let src = src.strip_prefix("src=").ok_or(EthParseError(3))?.parse()?;
        let dst = dst.strip_prefix("dst=").ok_or(EthParseError(4))?.parse()?;
        Ok(Eth { src, dst })
    }
}

#[derive(Debug, Clone, Default)]
struct OvsFlow {
    recirc_id: u64,
    in_port: u64,
    eth: Eth,
    eth_type: u16,
    // proto: String,
    packets: u64,
    bytes: u64,
    used: f64,
    // action: Vec<Action>,
}

#[derive(Error, Debug)]
enum OvsFlowParseError {
    #[error("No statistics information presented in the record")]
    NoStats,
    #[error("Parse field {0} error: {1}")]
    ParseField(String, Box<dyn std::error::Error>),
    #[error("Parse second part error on token: {0}")]
    ParseSecondPart(String),
    #[error("Unexpected format of field 'used': {0}")]
    ParseUsed(String),
}

macro_rules! ovs_flow_field_err_handler {
    ($name:expr) => {
        |e| OvsFlowParseError::ParseField($name.to_owned(), Box::new(e))
    };
}

// recirc_id(0),in_port(2),eth(src=1c:34:da:a5:55:8c,dst=01:80:c2:00:00:0e),eth_type(0x88cc), packets:0, bytes:0, used:12.620s, actions:drop

impl std::str::FromStr for OvsFlow {
    type Err = OvsFlowParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((first, second)) = s.split_once(", ") {
            let mut ovs_flow = OvsFlow::default();

            // parse the first part
            for tok in first.split("),") {
                let mut tok = tok.to_owned();
                tok.push(')');
                let key: String = tok
                    .bytes()
                    .take_while(|&b| b != b'(')
                    .map(|b| b as char)
                    .collect();
                let value: String = tok
                    .bytes()
                    .skip_while(|&b| b != b'(')
                    .skip(1)
                    .take_while(|&b| b != b')')
                    .map(|b| b as char)
                    .collect();
                match key.as_str() {
                    "recirc_id" => {
                        ovs_flow.recirc_id = value
                            .parse()
                            .map_err(ovs_flow_field_err_handler!("recirc_id"))?
                    }
                    "in_port" => {
                        ovs_flow.in_port = value
                            .parse()
                            .map_err(ovs_flow_field_err_handler!("in_port"))?
                    }
                    "eth" => {
                        // eth(src=1c:34:da:a5:55:94,dst=01:80:c2:00:00:0e)
                        ovs_flow.eth = value.parse().map_err(ovs_flow_field_err_handler!("eth"))?
                    }
                    "eth_type" => {
                        ovs_flow.eth_type = value
                            .strip_prefix("0x")
                            .ok_or(OvsFlowParseError::ParseField(
                                "eth_type".to_string(),
                                "no prefix 0x found".into(),
                            ))
                            .and_then(|x| {
                                u16::from_str_radix(x, 16)
                                    .map_err(ovs_flow_field_err_handler!("eth_type"))
                            })?
                    }
                    _ => {
                        log::trace!("parse ovs flow first part, ignoring {} {}", key, value);
                    }
                }
            }

            // parse the second part
            for tok in second.split(", ") {
                let (key, value) = tok
                    .split_once(':')
                    .ok_or(OvsFlowParseError::ParseSecondPart(tok.to_owned()))?;
                match key {
                    "packets" => {
                        ovs_flow.packets = value
                            .parse()
                            .map_err(ovs_flow_field_err_handler!("packets"))?
                    }
                    "bytes" => {
                        ovs_flow.bytes = value
                            .parse()
                            .map_err(ovs_flow_field_err_handler!("bytes"))?
                    }
                    "used" => {
                        ovs_flow.used = value
                            .strip_suffix("s")
                            .ok_or(OvsFlowParseError::ParseUsed(value.to_owned()))?
                            .parse()
                            .map_err(ovs_flow_field_err_handler!("used"))?;
                    }
                    _ => {
                        log::trace!("parse ovs flow second part, ignoring {} {}", key, value);
                    }
                }
            }

            Ok(ovs_flow)
        } else {
            Err(OvsFlowParseError::NoStats)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_ovs_flow() {
        let stdout = "recirc_id(0),in_port(2),eth(src=1c:34:da:a5:55:8c,dst=01:80:c2:00:00:0e),eth_type(0x88cc), packets:0, bytes:0, used:12.620s, actions:drop";
        let ovs_flows: Vec<OvsFlow> = stdout
            .trim()
            .lines()
            .map(|line| {
                line.parse()
                    .unwrap_or_else(|e| panic!("Fail to parse line: {}, error: {}", line, e))
            })
            .collect();
        log::debug!("parsed ovs_flow: {:?}", ovs_flows);
    }
}

#[derive(Debug, Clone)]
struct FlowStats {
    bytes: usize,
    create_time: std::time::Instant,
    update_time: std::time::Instant,
}

#[derive(Debug, Clone, Default)]
struct FlowTable {
    table: HashMap<Eth, FlowStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CounterType {
    Tx,   // total tx
    TxIn, // tx within rack
    Rx,   // total rx
    RxIn, // rx within rack
}

impl std::ops::Index<CounterType> for [CounterUnitData; 4] {
    type Output = CounterUnitData;
    fn index(&self, index: CounterType) -> &Self::Output {
        use CounterType::*;
        match index {
            Tx => self.get(0).unwrap(),
            TxIn => self.get(1).unwrap(),
            Rx => self.get(2).unwrap(),
            RxIn => self.get(3).unwrap(),
        }
    }
}

impl std::ops::IndexMut<CounterType> for [CounterUnitData; 4] {
    fn index_mut(&mut self, index: CounterType) -> &mut Self::Output {
        use CounterType::*;
        match index {
            Tx => self.get_mut(0).unwrap(),
            TxIn => self.get_mut(1).unwrap(),
            Rx => self.get_mut(2).unwrap(),
            RxIn => self.get_mut(3).unwrap(),
        }
    }
}

/// The information annotated on a virtual link.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterUnit {
    pub vnodename: String, // need to be unique across all VMs, name to sth else
    pub data: [CounterUnitData; 4], // 0 for tx, 1 for rx
}

impl CounterUnit {
    pub fn new(vnodename: &str) -> Self {
        CounterUnit {
            vnodename: vnodename.to_owned(),
            data: [CounterUnitData::default(); 4],
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CounterUnitData {
    pub bytes: u64,           // delta in bytes in the last time slot
    pub num_competitors: u32, // it can be num_flows, num_vms, or num_tenants, depending on the fairness model
}

impl OvsSampler {
    pub fn new(interval_ms: u64, tx: mpsc::Sender<Vec<CounterUnit>>) -> Self {
        OvsSampler {
            interval_ms,
            handle: None,
            tx,
        }
    }

    /// Spawn a thread, periodically query ovs counters, and write the output to communicator.
    pub fn run(&mut self) {
        log::info!("starting ovs sampler thread...");

        let sleep_ms = std::time::Duration::from_millis(self.interval_ms);
        let mut flow_table = FlowTable::default();
        let tx = self.tx.clone();

        // initialize local_eth_table
        let local_eth_table = get_local_eth_table().unwrap(); // map eth to node name
        let mut vnode_counter: HashMap<EthAddr, CounterUnit> = Default::default();
        for (eth, vnodename) in local_eth_table.iter() {
            vnode_counter
                .insert(eth.to_owned(), CounterUnit::new(vnodename))
                .unwrap_none();
        }

        self.handle = Some(std::thread::spawn(move || loop {
            let output = Command::new("ovs-appctl")
                .arg("dpctl/dump-flows")
                .arg("type=all")
                .arg("--more")
                .output()
                .expect("failed to execute process");

            if !output.status.success() {
                log::warn!("status: {}", output.status);
            }

            let out = std::str::from_utf8(&output.stdout).unwrap();
            let err = std::str::from_utf8(&output.stderr).unwrap();
            log::trace!("stdout: {}", out);
            log::trace!("stderr: {}", err);

            let ovs_flows: Vec<OvsFlow> = out
                .trim()
                .lines()
                .map(|line| {
                    line.parse()
                        .unwrap_or_else(|e| panic!("Fail to parse line: {}, error: {}", line, e))
                })
                .collect();
            log::trace!("parsed ovs_flow: {:?}", ovs_flows);

            let pcluster = CLUSTER.borrow().lock().unwrap();

            for ovs_flow in ovs_flows {
                // currently we only handle ipv4
                if ovs_flow.eth_type != 0x0800 {
                    continue;
                }

                let eth_src = ovs_flow.eth.src;
                let eth_dst = ovs_flow.eth.dst;
                let bytes_read = ovs_flow.bytes;
                let used = std::time::Duration::from_secs_f64(ovs_flow.used);

                use std::collections::hash_map::Entry;
                let delta = match flow_table.table.entry(ovs_flow.eth) {
                    Entry::Vacant(v) => {
                        let now = std::time::Instant::now();
                        let stats = FlowStats {
                            bytes: bytes_read as usize,
                            create_time: now - used,
                            update_time: now,
                        };
                        v.insert(stats);
                        bytes_read as usize
                    }
                    Entry::Occupied(mut o) => {
                        let stats = o.get_mut();
                        let now = std::time::Instant::now();
                        if stats.create_time + used < now {
                            // this is a new record in the output
                            stats.create_time = now - used;
                            stats.create_time = now;
                            stats.bytes = bytes_read as usize;
                            bytes_read as usize
                        } else {
                            assert!(
                                bytes_read as usize >= stats.bytes,
                                "ovs_flow: {:?}, stats: {:?}",
                                ovs_flow,
                                stats
                            );
                            let ret = bytes_read as usize - stats.bytes;
                            stats.bytes = bytes_read as usize;
                            stats.update_time = now;
                            ret
                        }
                    }
                };

                log::trace!("delta: {}", delta);

                // add delta
                use CounterType::*;
                let update_counter_unit = |c: &mut CounterUnit, counter_type: CounterType, delta: usize| {
                    c.data[counter_type].bytes += delta as u64;
                    c.data[counter_type].num_competitors += 1;
                };
                if local_eth_table.contains_key(&eth_src) {
                    if pcluster.is_eth_within_rack(&eth_dst) {
                        // tx_in
                        vnode_counter.entry(eth_src).and_modify(|e| update_counter_unit(e, TxIn, delta));
                    } else {
                        // tx
                        vnode_counter.entry(eth_src).and_modify(|e| update_counter_unit(e, Tx, delta));
                    }
                } else if local_eth_table.contains_key(&eth_dst) {
                    if pcluster.is_eth_within_rack(&eth_dst) {
                        // rx_in
                        vnode_counter.entry(eth_dst).and_modify(|e| update_counter_unit(e, RxIn, delta));
                    } else {
                        // rx
                        vnode_counter.entry(eth_dst).and_modify(|e| update_counter_unit(e, Rx, delta));
                    }
                } else {
                    log::warn!(
                        "ovs_flow {:?} does not come from or target to this server",
                        ovs_flow
                    );
                }
            }

            // release the lock, do not hold it for too long
            std::mem::drop(pcluster);

            // vnode_counter hash_map to vec
            let counter_unit: Vec<CounterUnit> = vnode_counter.values().cloned().collect();
            tx.send(counter_unit).unwrap();

            std::thread::sleep(sleep_ms);
        }));
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.handle
            .expect("ovs sampler thread failed to start")
            .join()
    }
}

// vf 0     link/ether 02:5a:78:25:5d:35 brd ff:ff:ff:ff:ff:ff, spoof checking off, link-state disable, trust off, query_rss off
// vf 1     link/ether 02:35:1a:a8:03:6e brd ff:ff:ff:ff:ff:ff, spoof checking off, link-state disable, trust off, query_rss off
pub fn get_local_eth_table() -> anyhow::Result<HashMap<EthAddr, String>> {
    let mut local_eth_table = HashMap::default(); // map eth to node name
    let mut cmd = Command::new("ip");
    cmd.args(&["link", "show", "rdma0"]);
    let iplink_output = utils::get_command_output(cmd)?;

    // parse the output
    for s in iplink_output.lines() {
        let line = s.trim();
        if line.starts_with("vf") {
            let tokens: Vec<_> = line.split(" ").filter(|x| !x.is_empty()).collect();
            let vfid: u64 = tokens[1].parse().unwrap();
            let eth_str = tokens[3];
            let eth: EthAddr = eth_str.parse()?;
            local_eth_table.insert(eth, vfid.to_string()).unwrap_none();
        }
    }

    Ok(local_eth_table)
}
