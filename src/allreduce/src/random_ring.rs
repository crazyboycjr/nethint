use crate::AllReduceAlgorithm;
use nethint::{cluster::Topology, Flow};
use rand::prelude::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use std::rc::Rc;

#[derive(Debug)]
pub struct RandomRingAllReduce {
    #[allow(unused)]
    seed: u64,
    num_rings: usize,
    rng: StdRng,
}

impl RandomRingAllReduce {
    pub fn new(seed: u64, num_rings: usize) -> Self {
        RandomRingAllReduce {
            seed,
            num_rings,
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl AllReduceAlgorithm for RandomRingAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: Rc<dyn Topology>) -> Vec<Flow> {
        let n = vcluster.num_hosts();

        let mut flows = Vec::new();
        for _ in 0..self.num_rings {
            let mut alloced_hosts: Vec<usize> = (0..n).into_iter().collect();
            alloced_hosts.shuffle(&mut self.rng);
            assert!(n > 0);
            for _ in 0..2 {
                for i in 0..n {
                    let pred = format!("host_{}", alloced_hosts[i]);
                    let succ = format!("host_{}", alloced_hosts[(i + 1) % n]);
                    log::debug!("pred: {}, succ: {}", pred, succ);
                    let flow = Flow::new(
                        size as usize * (n - 1) / n / self.num_rings,
                        &pred,
                        &succ,
                        None,
                    );
                    flows.push(flow);
                }
            }
        }

        flows
    }
}
