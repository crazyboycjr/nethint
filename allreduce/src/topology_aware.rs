use log::info;
use crate::AllReduceAlgorithm;
use nethint::{cluster::Topology, Flow};

#[derive(Debug, Default)]
pub struct TopologyAwareRingAllReduce {
    seed: u64,
}

impl TopologyAwareRingAllReduce {
    pub fn new(seed: u64) -> Self {
        TopologyAwareRingAllReduce { seed }
    }
}

impl AllReduceAlgorithm for TopologyAwareRingAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
        use rand::prelude::SliceRandom;
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(self.seed);

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
                info!("{}", node_idx);
                ring.push(node_idx);
            }
        }

        let mut flows = Vec::new();

        for i in 0..vcluster.num_hosts() {
            let sender = format!("host_{}", ring.get(i).unwrap());
            let receiver = format!("host_{}", ring.get((i + 1) % vcluster.num_hosts()).unwrap());
            let phy_sender = vcluster.translate(&sender);
            let phy_receiver = vcluster.translate(&receiver);
            let flow = Flow::new(size as usize, &phy_sender, &phy_receiver, None);
            flows.push(flow);
        }
        flows
    }
}
