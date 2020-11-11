use nethint::cluster::{Cluster, Node};
use nethint::{Bandwidth, Simulator, Executor, Trace};

fn main() {
    let nodes = vec!["a1", "a2", "a3", "a5", "a6", "vs1", "vs2", "cloud"]
        .into_iter()
        .map(|n| Node::new(n))
        .collect();

    let mut cluster = Cluster::from_nodes(nodes);

    vec![
        ("a1", "vs1", Bandwidth::Gbps(20.0)),
        ("a2", "vs1", Bandwidth::Gbps(10.0)),
        ("a3", "vs1", Bandwidth::Gbps(9.0)),
        ("a5", "vs2", Bandwidth::Gbps(10.0)),
        ("a6", "vs2", Bandwidth::Gbps(5.0)),
        ("vs1", "cloud", Bandwidth::Gbps(35.0)),
        ("vs2", "cloud", Bandwidth::Gbps(15.0)),
    ]
    .into_iter()
    .for_each(|args| cluster.add_edge(args.0, args.1, args.2));


    let trace = Trace::new();
    let mut simulator = Simulator::new(cluster);
    let output = simulator.run_with_trace(trace);
    println!("{:?}", output);
}
