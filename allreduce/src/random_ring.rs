use nethint::{
    cluster::Topology,
    Flow,
};
use crate::{AllReduceAlgorithm};

#[derive(Debug, Default)]
pub struct RandomRingAllReduce {
    seed: u64,
}

impl RandomRingAllReduce {
    pub fn new(seed: u64) -> Self {
        RandomRingAllReduce {
            seed,
        }
    }
}

impl AllReduceAlgorithm for RandomRingAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
        use rand::prelude::SliceRandom;
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut alloced_hosts: Vec<usize> = (0..vcluster.num_hosts()).into_iter().collect();
        alloced_hosts.shuffle(&mut rng);

        let mut flows = Vec::new();

        for i in 0..vcluster.num_hosts() {
            let sender = format!("host_{}", alloced_hosts.get(i).unwrap());
            let receiver = format!(
                "host_{}",
                alloced_hosts.get((i + 1) % vcluster.num_hosts()).unwrap()
            );
            let phy_sender = vcluster.translate(&sender);
            let phy_receiver = vcluster.translate(&receiver);
            let flow = Flow::new(size as usize, &phy_sender, &phy_receiver, None);
            flows.push(flow);
        }
        flows
    }
}
