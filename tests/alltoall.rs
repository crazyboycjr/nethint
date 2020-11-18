#[cfg(test)]
mod logging;

use nethint::bandwidth::{BandwidthTrait, Bandwidth};
use nethint::cluster::{Cluster, Node, Link, NodeRef};
use nethint::{Executor, Flow, Simulator, Trace, TraceRecord};
use std::rc::Rc;

type Layer = [NodeRef];

trait Network {
    fn add_layer(&mut self, layer: &Layer);
    fn fully_connect(&mut self, layer1: &Layer, layer2: &Layer, bw: Bandwidth);
}

impl Network for Cluster {
    fn add_layer(&mut self, layer: &Layer) {
        for n in layer {
            self.add_node(Rc::clone(n));
        }
    }

    fn fully_connect(&mut self, layer1: &Layer, layer2: &Layer, bw: Bandwidth) {
        for n1 in layer1 {
            for n2 in layer2 {
                self.add_link(Link::new(Rc::clone(n1), Rc::clone(n2), bw));
                self.add_link(Link::new(Rc::clone(n2), Rc::clone(n1), bw));
            }
        }
    }
}

fn build_fatree(nports: usize, bw: Bandwidth) -> Cluster {
    assert!(nports % 2 == 0, "the number of ports of a switch is required to be even");
    let k = nports;
    let num_pods = k;
    let num_cores = k * k / 4;
    let num_aggs = num_pods * k / 2;
    let num_edges = num_pods * k / 2;
    let num_hosts = num_edges * k / 2;

    let mut cluster = Cluster::new();

    // create core layer switches
    let cores: Vec<NodeRef> = (0..num_cores)
        .map(|i| Node::new(&format!("core_{}", i)))
        .collect();

    cluster.add_layer(&cores);

    let mut host_id = 0;
    // create each pods individually
    for i in 0..num_pods {
        // create aggregation layer switches
        let start = i * num_pods;
        let aggs: Vec<NodeRef> = (start..start + num_aggs)
            .map(|i| Node::new(&format!("agg_{}", i)))
            .collect();

        // create aggregation layer switches
        let edges: Vec<NodeRef> = (start..start + num_edges)
            .map(|i| Node::new(&format!("edge_{}", i)))
            .collect();

        cluster.add_layer(&aggs);
        cluster.add_layer(&edges);

        // fully connect
        cluster.fully_connect(&cores, &aggs, bw);
        cluster.fully_connect(&aggs, &edges, bw);

        for sw in edges {
            // create hosts
            let hosts: Vec<NodeRef> = (host_id..host_id + num_hosts)
                .map(|i| Node::new(&format!("host_{}", i)))
                .collect();
            host_id += num_hosts;

            cluster.add_layer(&hosts);
            cluster.fully_connect(&[sw], &hosts, bw);
        }
    }

    cluster
}

fn alltoall_trace(num_hosts: usize, flow_size: usize) -> Trace {
    let mut trace = Trace::new();

    for i in 0..num_hosts {
        for j in 0..num_hosts {
            if i == j {
                continue;
            }
            let src = format!("host_{}", i);
            let dst = format!("host_{}", j);
            let flow = Flow::new(flow_size, &src, &dst, None);
            let rec = TraceRecord::new(0, flow, None);
            trace.add_record(rec);
        }
    }

    trace
}

#[test]
fn main() {
    env_logger::init();

    let nports = 6;
    let cluster = build_fatree(nports, 100.gbps());

    let nhosts = nports * nports * nports / 4;
    let trace = alltoall_trace(nhosts, 1_000_000);

    let mut simulator = Simulator::new(cluster);
    let output = simulator.run_with_trace(trace);
    println!("{:?}", output);
}