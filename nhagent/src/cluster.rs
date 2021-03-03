use lazy_static::lazy_static;
use std::process::Command;

use nethint::{
    architecture::build_arbitrary_cluster,
    bandwidth::BandwidthTrait,
    cluster::{Cluster, NodeIx, Topology},
};

use crate::Role;

const NRACKS: usize = 2;
const RACK_SIZE: usize = 3;
const RACK_BW: f64 = 100.0;
const HOST_BW: f64 = 100.0;

pub fn init_cluster() -> Cluster {
    // reuse the code
    let mut cluster = build_arbitrary_cluster(NRACKS, RACK_SIZE, HOST_BW.gbps(), RACK_BW.gbps());

    // rename the host_i to danyang-[01-06] to match our cluster settings
    for i in 0..NRACKS * RACK_SIZE {
        let name = format!("host_{}", i);
        let node_ix = cluster.get_node_index(&name);
        cluster[node_ix].name = format!("danyang-{:02}", i + 1);
    }

    cluster.refresh_node_map();

    cluster
}

lazy_static! {
    static ref HOSTNAME: String = {
        let result = Command::new("hostname").output().unwrap();
        assert!(result.status.success());
        std::str::from_utf8(&result.stdout).unwrap().trim().to_owned()
    };
}

pub fn hostname<'h>() -> &'h String {
    &*HOSTNAME
}

fn is_first_child(cluster: &Cluster, node_ix: NodeIx) -> bool {
    let tor_ix = cluster.get_target(cluster.get_uplink(node_ix));
    if let Some(&link_ix) = cluster.get_downlinks(tor_ix).next() {
        let first_child_ix = cluster.get_target(link_ix);
        first_child_ix == node_ix
    } else {
        false
    }
}

pub fn get_my_role(cluster: &Cluster) -> Role {
    // without considering fault tolerant, we pick the first node in each rack as the rack leader.
    // We pick the first rack leader as the global leader.
    let hostname = hostname();
    let host_ix = cluster.get_node_index(hostname);
    let tor_ix = cluster.get_target(cluster.get_uplink(host_ix));
    if is_first_child(cluster, host_ix) {
        if is_first_child(cluster, tor_ix) {
            Role::GlobalLeader
        } else {
            Role::RackLeader
        }
    } else {
        Role::Worker
    }
}
