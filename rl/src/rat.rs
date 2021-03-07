use crate::RLAlgorithm;
use nethint::{cluster::Topology, Flow};

#[derive(Debug, Default)]
pub struct RatTree {
    seed: u64,
}

impl RatTree {
    pub fn new(seed: u64) -> Self {
        RatTree { seed }
    }
}

impl RLAlgorithm for RatTree {
    fn run_rl_traffic(&mut self, root_index: usize, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
        let mut ringlets = Vec::new();

        for i in 0..vcluster.num_switches() - 1 {
            let mut ringlet = Vec::new();
            let tor = format!("tor_{}", i);

            let mut tor_upbandwidth = vcluster[vcluster.get_uplink(vcluster.get_node_index(&tor))].bandwidth.val();

            for link_ix in vcluster.get_downlinks(vcluster.get_node_index(&tor)) {
                let h = vcluster.get_target(*link_ix);
                let host_idx = vcluster[h].name.strip_prefix("host_").unwrap().parse::<usize>().unwrap();
                ringlet.push(host_idx)
            }

            let pos = ringlet.iter().position(|x| *x == root_index);

            if pos == None {
                ringlet.sort_by_key(|x| {
                    let name = format!("host_{}", x);
                    let node_ix = vcluster.get_node_index(&name);
                    vcluster[vcluster.get_uplink(node_ix)].bandwidth.val()
                });
                let name = format!("host_{}", ringlet[0]);
                let node_ix = vcluster.get_node_index(&name);
                let upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();

                if tor_upbandwidth > upbandwidth {
                    tor_upbandwidth = upbandwidth;
                }

                ringlet.reverse();
                ringlets.push((ringlet, tor_upbandwidth));

            } else {
                let pos = pos.unwrap();
                ringlet.remove(pos);
                ringlet.sort_by_key(|x| {
                    let name = format!("host_{}", x);
                    let node_ix = vcluster.get_node_index(&name);
                    vcluster[vcluster.get_uplink(node_ix)].bandwidth.val()
                });
                ringlet.reverse();
                ringlet.insert(0, root_index);
                ringlets.push((ringlet, u64::MAX));

            }
        }

        ringlets.sort_by_key(|x| x.1);
        ringlets.reverse();

        let mut ring = Vec::new();
        for (ringlet, _upbandwidth) in ringlets {
            for ele in ringlet {
                ring.push(ele);
            }
        }

        let mut flows = Vec::new();

        let n = vcluster.num_hosts();

        log::error!("root_index: {}", root_index);
        log::error!("{:?}",ring);

        for i in 0..n-1 {
            let sender = format!("host_{}", ring[i]);
            let receiver = format!("host_{}", ring[(i + 1) % n]);
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
        }

        flows
    }
}
