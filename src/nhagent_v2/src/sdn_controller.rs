use crate::argument::Opts;
use crate::cluster::{hostname, CLUSTER, TOPO};
use nethint::cluster::{LinkIx, NodeIx, Topology};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use structopt::StructOpt;

const NUM_IP_PER_RACK: u8 = 128;

lazy_static::lazy_static! {
    static ref SHADOW_ID: Option<usize> = {
        let opts = Opts::from_args();
        opts.shadow_id
    };
}

fn get_emulated_ipv4() -> Ipv4Addr {
    let m = 256 / NUM_IP_PER_RACK as usize;
    // id in [1, 240]
    let id: usize = hostname()
        .strip_prefix("danyang-")
        .unwrap()
        .parse()
        .unwrap();
    Ipv4Addr::new(
        192,
        168,
        ((id - 1) / m + 1) as u8,
        NUM_IP_PER_RACK * ((id - 1) % m) as u8 + 2,
    )
}

fn get_ipv4() -> Ipv4Addr {
    if SHADOW_ID.is_some() {
        get_emulated_ipv4()
    } else {
        utils::net::get_primary_ipv4("rdma0").unwrap()
    }
}

#[inline]
fn num_ip_per_host() -> u8 {
    let rack_size = TOPO.rack_size();
    let mut ret = 1;
    while ret * 2 * rack_size < NUM_IP_PER_RACK as usize {
        ret *= 2;
    }
    assert!(rack_size != 3 || (rack_size == 3 && ret == 32));
    assert!(rack_size != 20 || (rack_size == 20 && ret == 4));
    assert!(rack_size != 10 || (rack_size == 10 && ret == 8));
    assert!(ret > 2);
    ret as u8
}

#[inline]
pub fn same_physical_server(ip1: &IpAddr, ip2: &IpAddr) -> bool {
    // maybe stateless for now
    let num_ip_per_host = num_ip_per_host();
    match (ip1, ip2) {
        (IpAddr::V4(ip1), IpAddr::V4(ip2)) => {
            // this condition only holds in our testbed as we configure it like this
            ip1.octets()[3] / num_ip_per_host == ip2.octets()[3] / num_ip_per_host
                && ip1.octets()[0] == ip2.octets()[0]
                && ip1.octets()[1] == ip2.octets()[1]
                && ip1.octets()[2] == ip2.octets()[2]
        }
        _ => {
            panic!("unexpeced ipv6 addr: {} {}", ip1, ip2);
        }
    }
}

#[inline]
pub fn same_rack(ip1: &IpAddr, ip2: &IpAddr) -> bool {
    // maybe stateless for now
    match (ip1, ip2) {
        (IpAddr::V4(ip1), IpAddr::V4(ip2)) => {
            // this condition only holds in our testbed as we configure it like this
            ip1.octets()[3] / NUM_IP_PER_RACK == ip2.octets()[3] / NUM_IP_PER_RACK
                && ip1.octets()[0] == ip2.octets()[0]
                && ip1.octets()[1] == ip2.octets()[1]
                && ip1.octets()[2] == ip2.octets()[2]
        }
        _ => {
            panic!("unexpeced ipv6 addr: {} {}", ip1, ip2);
        }
    }
}

pub fn get_local_ip_table() -> anyhow::Result<HashMap<IpAddr, String>> {
    let my_ip = get_ipv4();
    get_server_ip_table(&my_ip.to_string())
}

pub fn get_server_ip_table(ip: &str) -> anyhow::Result<HashMap<IpAddr, String>> {
    let mut server_ip_table = HashMap::default(); // map eth to node name
    let ds: Vec<u8> = ip.split('.').map(|x| x.parse().unwrap()).collect();
    let num_ip_per_host = num_ip_per_host();

    // derive the ip addresses from the ip of the host
    for i in 1..num_ip_per_host - 2 {
        let addr = Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3] + i);
        let vfid = i - 1;
        server_ip_table.insert(addr.into(), vfid.to_string());
    }

    // also insert host IP to capture background traffic
    server_ip_table.insert(
        Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3]).into(),
        (num_ip_per_host - 2).to_string(),
    );

    Ok(server_ip_table)
}

pub fn get_rack_leader_ipv4() -> Ipv4Addr {
    let my_ip = get_ipv4();
    let ds = my_ip.octets();
    let rack_base_ip_last_digit = ds[3] / NUM_IP_PER_RACK * NUM_IP_PER_RACK + 2;
    Ipv4Addr::new(ds[0], ds[1], ds[2], rack_base_ip_last_digit)
}

pub fn get_rack_ip_table() -> anyhow::Result<HashMap<IpAddr, LinkIx>> {
    let mut rack_ip_table = HashMap::default(); // map eth to node name

    let num_ip_per_host = num_ip_per_host();
    let my_ip = get_ipv4();
    let mut ds = get_rack_leader_ipv4().octets(); // array implements copy
    let my_server_offset = (my_ip.octets()[3] - ds[3]) as usize / num_ip_per_host as usize;

    // derive the ip addresses from the ip of the host
    let pcluster = CLUSTER.lock().unwrap();
    let my_node_ix = pcluster.my_node_ix();

    let get_host_id = |vc: &dyn Topology, host_ix: NodeIx| -> usize {
        vc[host_ix]
            .name
            .strip_prefix("danyang-")
            .unwrap()
            .parse()
            .unwrap()
    };
    let start_server_id = get_host_id(pcluster.inner(), my_node_ix) - my_server_offset;
    let rack_size = TOPO.rack_size();
    for server_id in start_server_id..start_server_id + rack_size {
        let name = format!("danyang-{:02}", server_id);
        let link_ix = pcluster
            .inner()
            .get_uplink(pcluster.inner().get_node_index(&name));
        // insert VM ip
        for i in 1..(num_ip_per_host - 2) {
            let addr = Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3] + i);
            rack_ip_table.insert(addr.into(), link_ix);
        }

        // also insert host IP to capture background traffic
        rack_ip_table.insert(Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3]).into(), link_ix);

        ds[3] += num_ip_per_host;
    }

    Ok(rack_ip_table)
}
