use nethint::{
    cluster::Topology,
    Flow,
};
use crate::{RLAlgorithm};

#[derive(Debug, Default)]
pub struct RandomTree {
    seed: u64,
}

impl RandomTree {
    pub fn new(seed: u64) -> Self {
        RandomTree {
            seed,
        }
    }
}

impl RLAlgorithm for RandomTree {
    fn run_rl_traffic(&mut self, root_index: usize, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
        let n = vcluster.num_hosts();

        use rand::prelude::SliceRandom;
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut alloced_hosts: Vec<usize> = (0..n).into_iter().collect();
        alloced_hosts.remove(root_index);
        alloced_hosts.shuffle(&mut rng);

        let mut flows = Vec::new();

        assert!(n > 0);
        let pred = format!("host_{}", root_index);
        let succ = format!("host_{}", alloced_hosts[0]);
        let flow = Flow::new(size as usize, &pred, &succ, None);
        flows.push(flow);

        for i in 1..n-1 {
            let pred = format!("host_{}", alloced_hosts[i-1]);
            let succ = format!("host_{}", alloced_hosts[i]);
            let flow = Flow::new(size as usize, &pred, &succ, None);
            flows.push(flow);
        }
        flows
    }
}
