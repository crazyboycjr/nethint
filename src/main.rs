use nethint::bandwidth::BandwidthTrait;
use nethint::cluster::{Cluster, Node};
use nethint::{Executor, Flow, Simulator, Trace, TraceRecord};

fn main() {
    nethint::logging::init_log();

    let nodes = vec!["a1", "a2", "a3", "a5", "a6", "vs1", "vs2", "cloud"]
        .into_iter()
        .map(|n| Node::new(n))
        .collect();

    let mut cluster = Cluster::from_nodes(nodes);

    vec![
        ("a1", "vs1", 20.gbps()),
        ("a2", "vs1", 10.gbps()),
        ("a3", "vs1", 9.gbps()),
        ("a5", "vs2", 10.gbps()),
        ("a6", "vs2", 5.gbps()),
        ("vs1", "cloud", 35.gbps()),
        ("vs2", "cloud", 15.gbps()),
    ]
    .into_iter()
    .for_each(|args| cluster.add_edge(args.0, args.1, args.2));

    let mut trace = Trace::new();
    let records: Vec<TraceRecord> = vec![
        (0, 1e6 as usize, "a1", "a5"),
        (0, 1e6 as usize, "a2", "a6"),
        (1000000, 1e6 as usize, "a2", "a3"),
    ]
    .into_iter()
    .map(|args| TraceRecord::new(args.0, Flow::new(args.1, args.2, args.3, None), None))
    .collect();
    records.into_iter().for_each(|r| trace.add_record(r));

    let mut simulator = Simulator::new(cluster);
    let output = simulator.run_with_trace(trace);
    println!("{:#?}", output);
}
