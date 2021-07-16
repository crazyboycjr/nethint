use crate::cluster::{CLUSTER, TOPO};
use nethint::cluster::{LinkIx, NodeIx, Topology};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

#[inline]
pub fn same_physical_server(ip1: &IpAddr, ip2: &IpAddr) -> bool {
    // maybe stateless for now
    match (ip1, ip2) {
        (IpAddr::V4(ip1), IpAddr::V4(ip2)) => {
            // this condition only holds in our testbed as we configure it like this
            ip1.octets()[3] / 32 == ip2.octets()[3] / 32
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
            ip1.octets()[3] / 128 == ip2.octets()[3] / 128
        }
        _ => {
            panic!("unexpeced ipv6 addr: {} {}", ip1, ip2);
        }
    }
}

pub fn get_local_ip_table() -> anyhow::Result<HashMap<IpAddr, String>> {
    let my_ip = utils::net::get_primary_ipv4("rdma0").unwrap();
    get_server_ip_table(&my_ip)
}

pub fn get_server_ip_table(ip: &str) -> anyhow::Result<HashMap<IpAddr, String>> {
    let mut server_ip_table = HashMap::default(); // map eth to node name
    let ds: Vec<u8> = ip.split('.').map(|x| x.parse().unwrap()).collect();

    // derive the ip addresses from the ip of the host
    for i in 1..30 {
        let addr = Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3] + i);
        let vfid = i - 1;
        server_ip_table.insert(addr.into(), vfid.to_string());
    }

    // also insert host IP to capture background traffic
    server_ip_table.insert(
        Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3]).into(),
        "30".to_owned(),
    );

    Ok(server_ip_table)
}

pub fn get_rack_ip_table() -> anyhow::Result<HashMap<IpAddr, LinkIx>> {
    let mut rack_ip_table = HashMap::default(); // map eth to node name
    let my_ip = utils::net::get_primary_ipv4("rdma0").unwrap();
    let mut ds: Vec<u8> = my_ip.split('.').map(|x| x.parse().unwrap()).collect();
    // the rank of the server in its rack
    let rack_base_ip_last_digit = ds[3] / 32 * 32 + 2;
    let my_server_id = (ds[3] - rack_base_ip_last_digit) as usize;
    ds[3] = rack_base_ip_last_digit;

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
    let start_server_id = get_host_id(pcluster.inner(), my_node_ix) - my_server_id;
    let rack_size = TOPO.rack_size();
    for server_id in start_server_id..start_server_id + rack_size {
        let name = format!("danyang-{:02}", server_id);
        let link_ix = pcluster
            .inner()
            .get_uplink(pcluster.inner().get_node_index(&name));
        // insert VM ip
        for i in 1..30 {
            let addr = Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3] + i);
            rack_ip_table.insert(addr.into(), link_ix);
        }

        // also insert host IP to capture background traffic
        rack_ip_table.insert(Ipv4Addr::new(ds[0], ds[1], ds[2], ds[3]).into(), link_ix);

        ds[3] += 32;
    }

    Ok(rack_ip_table)
}
