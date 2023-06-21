use crate::RLAlgorithm;
use nethint::{cluster::Topology, Flow};
use rand::prelude::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use std::rc::Rc;

#[derive(Debug)]
pub struct RandomChain {
    #[allow(unused)]
    seed: u64,
    num_trees: usize,
    rng: StdRng,
}

impl RandomChain {
    pub fn new(seed: u64, num_trees: usize) -> Self {
        RandomChain {
            seed,
            num_trees,
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl RLAlgorithm for RandomChain {
    fn run_rl_traffic(
        &mut self,
        root_index: usize,
        group: Option<Vec<usize>>,
        size: u64,
        vcluster: Rc<dyn Topology>,
    ) -> Vec<Flow> {
        let mut flows = Vec::new();

        for _ in 0..self.num_trees {
            let mut alloced_hosts: Vec<usize> = if group.is_none() {
                let n = vcluster.num_hosts();
                let mut hs: Vec<usize> = (0..n).into_iter().collect();
                hs.remove(root_index);
                hs
            } else {
                group.clone().unwrap()
            };
            alloced_hosts.shuffle(&mut self.rng);

            alloced_hosts.insert(0, root_index);

            assert!(
                alloced_hosts.len() >= 2,
                "vcluster size must >= 2, worker group cannot be empty"
            );

            for (&x, &y) in alloced_hosts.iter().zip(alloced_hosts.iter().skip(1)) {
                let pred = format!("host_{}", x);
                let succ = format!("host_{}", y);
                let flow = Flow::new(size as usize, &pred, &succ, None);
                flows.push(flow);
            }
        }

        for f in &mut flows {
            f.bytes /= self.num_trees;
        }

        log::info!("flows: {:?}", flows);
        flows
    }
}
