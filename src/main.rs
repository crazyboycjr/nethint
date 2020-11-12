use nethint::cluster::{Cluster, Node};
use nethint::{Bandwidth, Executor, Flow, Simulator, Trace, TraceRecord};

// #[macro_use]
// extern crate log;

use log::{info};

fn init_log() {
    use chrono::Utc;
    use std::io::Write;

    let env = env_logger::Env::default().default_filter_or("debug");
    env_logger::Builder::from_env(env)
        .format(|buf, record| {
            writeln!(
                buf,
                "[{} {} {}:{}] {}",
                Utc::now().format("%Y-%m-%d %H:%M:%S%.6f"),
                record.level(),
                record.file().unwrap_or("<unnamed>"),
                record.line().unwrap_or(0),
                &record.args()
            )
        })
    .init();

    info!("env_logger initialized");
}


fn main() {
    init_log();

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

    let mut trace = Trace::new();
    trace.add_record(TraceRecord::new(
        0,
        Flow::new(1e6 as usize, "a1", "a5", None),
        None,
    ));
    let mut simulator = Simulator::new(cluster);
    let output = simulator.run_with_trace(trace);
    println!("{:?}", output);
}
