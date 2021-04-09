use crate::AllReduceAlgorithm;
use nethint::{cluster::Topology, Flow};

#[derive(Debug, Default)]
pub struct TopologyAwareRingAllReduce {
    seed: u64,
    num_rings: usize,
}

impl TopologyAwareRingAllReduce {
    pub fn new(seed: u64, num_rings: usize) -> Self {
        TopologyAwareRingAllReduce { seed, num_rings }
    }
}

impl AllReduceAlgorithm for TopologyAwareRingAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
        use rand::prelude::SliceRandom;
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut flows = Vec::new();

        for _ in 0..self.num_rings {
            let mut ring = Vec::new();

            for i in 0..vcluster.num_switches() - 1 {
                let mut ringlet = Vec::new();
                let tor = format!("tor_{}", i);

                for link_ix in vcluster.get_downlinks(vcluster.get_node_index(&tor)) {
                    let h = vcluster.get_target(*link_ix);
                    let host_idx = vcluster[h].name.strip_prefix("host_").unwrap().parse::<usize>().unwrap();
                    ringlet.push(host_idx)
                }
                ringlet.shuffle(&mut rng);
                for node_idx in ringlet {
                    ring.push(node_idx);
                }
            }

            let n = vcluster.num_hosts();
            for _ in 0..2 {
                for i in 0..n {
                    let sender = format!("host_{}", ring[i]);
                    let receiver = format!("host_{}", ring[(i + 1) % n]);
                    let flow = Flow::new(size as usize * (n - 1) / n / self.num_rings, &sender, &receiver, None);
                    flows.push(flow);
                }
            }
        }

        flows
    }
}
