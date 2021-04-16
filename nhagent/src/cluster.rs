use lazy_static::lazy_static;
use std::process::Command;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::net::IpAddr;

use nethint::{
    architecture::build_arbitrary_cluster,
    bandwidth::BandwidthTrait,
    cluster::{Cluster, NodeIx, Topology},
};

use crate::sampler::EthAddr;
use crate::Role;

pub const NRACKS: usize = 2;
pub const RACK_SIZE: usize = 3;
pub const RACK_BW: f64 = 10.0;
pub const HOST_BW: f64 = 10.0;
pub const MAX_SLOTS: usize = 4;

lazy_static! {
    static ref HOSTNAME: String = utils::cmd_helper::get_command_output(Command::new("hostname"))
        .unwrap()
        .trim()
        .to_owned();
}

pub fn hostname<'h>() -> &'h String {
    &*HOSTNAME
}

lazy_static! {
    // the topology of the underlay physical cluster
    pub static ref CLUSTER: Arc<Mutex<PhysCluster>> = Arc::new(Mutex::new(PhysCluster::new()));
}

#[derive(Debug)]
pub struct PhysCluster {
    inner: Cluster,
    my_node_ix: NodeIx,
    // map eth addr to hostname
    // this table with be updated later
    eth_hostname: HashMap<EthAddr, String>,
    ip_hostname: HashMap<IpAddr, String>,
}

impl PhysCluster {
    pub fn new() -> PhysCluster {
        // reuse the code
        let mut cluster =
            build_arbitrary_cluster(NRACKS, RACK_SIZE, HOST_BW.gbps(), RACK_BW.gbps());

        // rename the host_i to danyang-[01-06] to match our cluster settings
        for i in 0..NRACKS * RACK_SIZE {
            let name = format!("host_{}", i);
            let node_ix = cluster.get_node_index(&name);
            cluster[node_ix].name = format!("danyang-{:02}", i + 1);
        }

        cluster.refresh_node_map();

        let my_hostname = hostname().clone();

        let my_node_ix = cluster.get_node_index(&my_hostname);

        let local_eth_table = crate::sampler::get_local_eth_table().unwrap();
        assert!(!local_eth_table.is_empty(), "in our prototype, we assume the VF and the VM are up");
        let eth_hostname = local_eth_table.into_keys().map(|eth_addr| {
            (eth_addr, my_hostname.clone())
        }).collect();

        let local_ip_table = crate::sampler::get_local_ip_table().unwrap();
        let ip_hostname = local_ip_table.into_keys().map(|ip_addr| {
            (ip_addr, my_hostname.clone())
        }).collect();

        PhysCluster {
            inner: cluster,
            my_node_ix,
            eth_hostname,
            ip_hostname,
        }
    }

    pub fn my_node_ix(&self) -> NodeIx {
        self.my_node_ix
    }

    pub fn inner(&self) -> &Cluster {
        &self.inner
    }

    pub fn eth_hostname(&self) -> &HashMap<EthAddr, String> {
        &self.eth_hostname
    }

    pub fn ip_hostname(&self) -> &HashMap<IpAddr, String> {
        &self.ip_hostname
    }

    pub fn update_eth_hostname(&mut self, table: HashMap<EthAddr, String>) {
        // COMMENT(cjr): what if there are repeat keys from remote table
        self.eth_hostname.extend(table);
    }

    pub fn update_ip_hostname(&mut self, table: HashMap<IpAddr, String>) {
        self.ip_hostname.extend(table);
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

    pub fn get_role(&self, hostname: &str) -> Role {
        let cluster = &self.inner;
        // without considering fault tolerant, we pick the first node in each rack as the rack leader.
        // We pick the first rack leader as the global leader.
        let host_ix = cluster.get_node_index(hostname);
        let tor_ix = cluster.get_target(cluster.get_uplink(host_ix));
        if Self::is_first_child(cluster, host_ix) {
            if Self::is_first_child(cluster, tor_ix) {
                Role::GlobalLeader
            } else {
                Role::RackLeader
            }
        } else {
            Role::Worker
        }
    }

    pub fn get_my_role(&self) -> Role {
        self.get_role(&hostname())
    }

    pub fn get_my_rack_ix(&self) -> NodeIx {
        self.inner.get_target(self.inner.get_uplink(self.my_node_ix))
    }

    fn cluster_is_same_rack(cluster: &Cluster, x: NodeIx, y: NodeIx) -> bool {
        cluster.get_target(cluster.get_uplink(x)) == cluster.get_target(cluster.get_uplink(y))
    }

    fn cluster_is_cross_rack(cluster: &Cluster, x: NodeIx, y: NodeIx) -> bool {
        !Self::cluster_is_same_rack(cluster, x, y)
    }

    pub fn is_same_rack(&self, x: NodeIx, y: NodeIx) -> bool {
        Self::cluster_is_same_rack(&self.inner, x, y)
    }

    pub fn is_cross_rack(&self, x: NodeIx, y: NodeIx) -> bool {
        Self::cluster_is_cross_rack(&self.inner, x, y)
    }

    pub fn is_eth_within_rack(&self, eth_addr: &EthAddr) -> bool {
        if let Some(host) = self.eth_hostname.get(eth_addr) {
            // TODO(cjr): make this robust because the query may fail
            let host_ix = self.inner.get_node_index(host);
            let my_host_ix = self.inner.get_node_index(&hostname());
            self.is_same_rack(host_ix, my_host_ix)
        } else {
            false
        }
    }

    pub fn is_ip_within_rack(&self, ip_addr: &IpAddr) -> bool {
        if let Some(host) = self.ip_hostname.get(ip_addr) {
            let host_ix = self.inner.get_node_index(host);
            let my_host_ix = self.inner.get_node_index(&hostname());
            self.is_same_rack(host_ix, my_host_ix)
        } else {
            false
        }
    }
}
