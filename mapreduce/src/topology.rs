use nethint::bandwidth::Bandwidth;
use nethint::cluster::{Cluster, Node, NodeType};

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

    build_virtual_cluster(
        num_edges,
        num_hosts_under_edge,
        bw,
        bw * num_hosts_under_edge / oversub_ratio,
    )
}

pub fn build_virtual_cluster(
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
