use crate::RLAlgorithm;
use nethint::{cluster::Topology, Flow};

#[derive(Debug, Default)]
pub struct TopologyAwareTree {
    seed: u64,
}

impl TopologyAwareTree {
    pub fn new(seed: u64) -> Self {
        TopologyAwareTree { seed }
    }
}

impl RLAlgorithm for TopologyAwareTree {
    fn run_rl_traffic(&mut self, root_index: usize, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
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

            let pos = ringlet.iter().position(|x| *x == root_index);

            if pos == None {
                ringlet.shuffle(&mut rng);
            } else {
                let pos = pos.unwrap();
                ringlet.remove(pos);
                ringlet.shuffle(&mut rng);
                ringlet.insert(0, root_index);
            }
            for node_idx in ringlet {
                ring.push(node_idx);
            }
        }

        let mut flows = Vec::new();

        let n = vcluster.num_hosts();

        let pos = ring.iter().position(|x| *x == root_index).unwrap();

        // log::error!("pos {} n {}", pos, n);
        // log::error!("{}",root_index);
        // log::error!("{:?}",ring);

        for i in pos..n {
            let sender = format!("host_{}", ring[i]);
            let receiver = format!("host_{}", ring[(i + 1) % n]);
            if (i+1) % n == pos {
                break;
            }
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
        }

        if pos > 0 {
        for i in 0..pos-1 {
            let sender = format!("host_{}", ring[i]);
            let receiver = format!("host_{}", ring[i + 1]);
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
        }
        }

        flows
    }
}
