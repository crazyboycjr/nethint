use nethint::{
    cluster::Topology,
    Flow,
};
use crate::{AllReduceAlgorithm};
use std::rc::Rc;

#[derive(Debug, Default)]
pub struct RandomRingAllReduce {
    seed: u64,
    num_rings: usize,
}

impl RandomRingAllReduce {
    pub fn new(seed: u64, num_rings: usize) -> Self {
        RandomRingAllReduce {
            seed,
            num_rings,
        }
    }
}

impl AllReduceAlgorithm for RandomRingAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: Rc<dyn Topology>) -> Vec<Flow> {
        let n = vcluster.num_hosts();

        use rand::prelude::SliceRandom;
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut flows = Vec::new();
        for _ in 0..self.num_rings {
            let mut alloced_hosts: Vec<usize> = (0..n).into_iter().collect();
            alloced_hosts.shuffle(&mut rng);
            assert!(n > 0);
            for _ in 0..2 {
                for i in 0..n {
                    let pred = format!("host_{}", alloced_hosts[i]);
                    let succ = format!("host_{}", alloced_hosts[(i + 1) % n]);
                    log::debug!("pred: {}, succ: {}", pred, succ);
                    let flow = Flow::new(size as usize * (n - 1) / n / self.num_rings, &pred, &succ, None);
                    flows.push(flow);
                }
            }
        }

        flows
    }
}
