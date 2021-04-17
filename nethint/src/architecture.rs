use crate::bandwidth::Bandwidth;
use crate::cluster::{Cluster, Node, NodeType};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Serialize, Deserialize)]
#[serde(tag = "type", content = "args")]
pub enum TopoArgs {
    /// FatTree, parameters include the number of ports of each switch, bandwidth, and oversubscription ratio
    FatTree {
        /// Set the the number of ports
        nports: usize,
        /// Bandwidth of a host, in Gbps
        bandwidth: f64,
        /// Oversubscription ratio
        oversub_ratio: f64,
    },

    /// 'Arbitrary' cluster, parameters include the number of racks and rack_size, host_bw, and rack_bw
    Arbitrary {
        /// Specify the number of racks
        nracks: usize,
        /// Specify the number of hosts under one rack
        rack_size: usize,
        /// Bandwidth of a host, in Gbps
        host_bw: f64,
        /// Bandwidth of a ToR switch, in Gbps
        rack_bw: f64,
    },
}

impl TopoArgs {
    pub fn get_host_bw(&self) -> &f64{
        match self {
            TopoArgs::Arbitrary {
                nracks,
                rack_size,
                host_bw,
                rack_bw,
            } => {
                return host_bw;
            },
            _ => { panic!("can not unwrap TopoArgs to get host_bw"); return &0.0},
        }
    }
}

//helper function to unwrap the enum and return host_bw


impl std::fmt::Display for TopoArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopoArgs::FatTree {
                nports,
                bandwidth,
                oversub_ratio,
            } => write!(f, "fattree_{}_{}g_{:.2}", nports, bandwidth, oversub_ratio),
            TopoArgs::Arbitrary {
                nracks,
                rack_size,
                host_bw,
                rack_bw,
            } => write!(
                f,
                "arbitrary_{}_{}_{}g_{}g",
                nracks, rack_size, host_bw, rack_bw,
            ),
        }
    }
}

pub fn build_fatree_fake(nports: usize, bw: Bandwidth, oversub_ratio: f64) -> Cluster {
    assert!(
        nports % 2 == 0,
        "the number of ports of a switch is required to be even for FatTree"
    );

    let k = nports;
    let num_pods = k;
    let _num_cores = k * k / 4;
    let num_aggs_in_pod = k / 2;
    let _num_aggs = num_pods * num_aggs_in_pod;
    let num_edges_in_pod = k / 2;
    let num_edges = num_pods * num_edges_in_pod;
    let num_hosts_under_edge = k / 2;
    let _num_hosts = num_edges * num_hosts_under_edge;

    build_arbitrary_cluster(
        num_edges,
        num_hosts_under_edge,
        bw,
        bw * num_hosts_under_edge / oversub_ratio,
    )
}

pub fn build_arbitrary_cluster(
    nracks: usize,
    rack_size: usize,
    host_bw: Bandwidth,
    rack_bw: Bandwidth,
) -> Cluster {
    let mut cluster = Cluster::new();
    let cloud = Node::new("cloud", 1, NodeType::Switch);
    cluster.add_node(cloud);

    let mut host_id = 0;
    for i in 0..nracks {
        let tor_name = format!("tor_{}", i);
        let tor = Node::new(&tor_name, 2, NodeType::Switch);
        cluster.add_node(tor);
        cluster.add_link_by_name("cloud", &tor_name, rack_bw);

        for j in host_id..host_id + rack_size {
            let host_name = format!("host_{}", j);
            let host = Node::new(&host_name, 3, NodeType::Host);
            cluster.add_node(host);
            cluster.add_link_by_name(&tor_name, &host_name, host_bw);
        }

        host_id += rack_size;
    }

    cluster
}
