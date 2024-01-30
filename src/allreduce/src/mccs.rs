use crate::AllReduceAlgorithm;
use nethint::{cluster::Topology, Flow};
use std::rc::Rc;

#[derive(Debug, Default)]
pub struct MccsRingAllReduce {
    seed: u64,
    num_rings: usize,
}

impl MccsRingAllReduce {
    pub fn new(seed: u64, num_rings: usize) -> Self {
        MccsRingAllReduce { seed, num_rings }
    }
}

impl AllReduceAlgorithm for MccsRingAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: Rc<dyn Topology>) -> Vec<Flow> {
        use rand::prelude::SliceRandom;
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut flows = Vec::new();

        let mut ring = Vec::new();

        for i in 0..vcluster.num_racks() {
            let mut ringlet = Vec::new();
            let tor = format!("tor_{}", i);

            for link_ix in vcluster.get_downlinks(vcluster.get_node_index(&tor)) {
                let h = vcluster.get_target(*link_ix);
                let host_idx = vcluster[h]
                    .name
                    .strip_prefix("host_")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                ringlet.push(host_idx)
            }
            ringlet.shuffle(&mut rng);
            for node_idx in ringlet {
                ring.push(node_idx);
            }
        }

        let n = vcluster.num_hosts();
        for channel_id in 0..self.num_rings {
            for _ in 0..2 {
                for i in 0..n {
                    let sender = format!("host_{}", ring[i]);
                    let receiver = format!("host_{}", ring[(i + 1) % n]);
                    let mut flow = Flow::new(
                        size as usize * (n - 1) / n / self.num_rings,
                        &sender,
                        &receiver,
                        None,
                    );
                    // Set UDP source port here
                    flow.udp_src_port = Some(channel_id as u16);
                    flows.push(flow);
                }
            }
        }

        flows
    }
}
