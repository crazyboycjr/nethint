use crate::RLAlgorithm;
use nethint::{
    cluster::Topology,
    Flow,
};

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
    fn run_rl_traffic(
        &mut self,
        root_index: usize,
        size: u64,
        vcluster: &dyn Topology,
    ) -> Vec<Flow> {
        self.run_rl_traffic(root_index, size, vcluster)
    }
}

#[allow(dead_code)]
fn compact_chain(
    chain: &mut Vec<usize>,
    side_chain: &mut Vec<(usize, usize)>,
    vcluster: &dyn Topology,
) -> (usize, u64, u64) {
    let name = format!("host_{}", chain[0]);
    let node_ix = vcluster.get_node_index(&name);
    let mut min_rx = vcluster[vcluster.get_reverse_link(vcluster.get_uplink(node_ix))]
    .bandwidth
    .val();
    for x in chain.clone() {
        let name = format!("host_{}", x);
        let node_ix = vcluster.get_node_index(&name);
        let rx = vcluster[vcluster.get_reverse_link(vcluster.get_uplink(node_ix))]
            .bandwidth
            .val();
        if rx < min_rx {
            min_rx = rx;
        }
    }
    let mut m = std::collections::HashMap::<usize, u64>::new();

    if chain.len() > 2 {
        let mut name = format!("host_{}", chain[chain.len() - 2]);
        let mut node_ix = vcluster.get_node_index(&name);
        let mut upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();

        let mut old_chain_rate = std::cmp::min(upbandwidth, min_rx);

        let mut name_l2 = format!("host_{}", chain[chain.len() - 3]);
        let mut node_ix_l2 = vcluster.get_node_index(&name_l2);
        let mut upbandwidth_l2 = vcluster[vcluster.get_uplink(node_ix_l2)].bandwidth.val();
        let mut new_chain_rate = std::cmp::min(upbandwidth_l2, min_rx);


        while upbandwidth < min_rx {
            let mut found = false;
            for i in 0..chain.len() - 3 {
                let name = format!("host_{}", chain[i]);
                let node_ix = vcluster.get_node_index(&name);
                let tx = vcluster[vcluster.get_reverse_link(vcluster.get_uplink(node_ix))]
                    .bandwidth
                    .val();

                let mut oldrate = 0;
                if m.contains_key(&chain[i]) {
                    oldrate = m[&chain[i]];
                }
                    log::trace!("{} - {} > {}", tx, new_chain_rate, old_chain_rate);

                if tx - new_chain_rate > old_chain_rate + oldrate {
                    side_chain.push((chain[i], chain[chain.len() - 1]));
                    chain.pop();
                    // log::error!("{} - {} > {}", tx, new_chain_rate, old_chain_rate);
                    if !m.contains_key(&chain[i]) {
                        m.insert(chain[i], 0);
                    }
                    *m.get_mut(&chain[i]).unwrap() += new_chain_rate;
                    found = true;
                    break;
                }
            }
            if !found {
                break;
            }

            if m.contains_key(&chain[chain.len() - 1]) {
                break;
            }

            if chain.len() <= 2 {
                break;
            }

            name = format!("host_{}", chain[chain.len() - 2]);
            node_ix = vcluster.get_node_index(&name);
            upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();

            old_chain_rate = std::cmp::min(upbandwidth, min_rx);

            name_l2 = format!("host_{}", chain[chain.len() - 3]);
            node_ix_l2 = vcluster.get_node_index(&name_l2);
            upbandwidth_l2 = vcluster[vcluster.get_uplink(node_ix_l2)].bandwidth.val();
            new_chain_rate = std::cmp::min(upbandwidth_l2, min_rx);
        }
    }

    let name = format!("host_{}", chain[0]);
    let node_ix = vcluster.get_node_index(&name);
    let first_tx = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();
    let last_tx = if chain.len() >= 2 {
        let name = format!("host_{}", chain[chain.len()-2]);
        let node_ix = vcluster.get_node_index(&name);
        let last_tx = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();
        last_tx
    } else {
        u64::MAX
    };
    let chain_rate = std::cmp::min(std::cmp::min(first_tx, last_tx), min_rx);

    let mut max_tx = 0;
    let mut max_node = 0;
    for i in 0..chain.len() {
        let name = format!("host_{}", chain[i]);
        let node_ix = vcluster.get_node_index(&name);
        let mut upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();
        let mut oldrate = 0;
        if m.contains_key(&chain[i]) {
            oldrate = m[&chain[i]];
        }
        upbandwidth -= oldrate;
        if upbandwidth > max_tx {
            if i != chain.len() - 1 {
            max_tx = upbandwidth - chain_rate;
            } else {
                max_tx = upbandwidth;
            }
            max_node = chain[i].clone();
        }
    }

    for (_x, y) in side_chain {
        let name = format!("host_{}", y);
        let node_ix = vcluster.get_node_index(&name);
        let upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();
        if upbandwidth > max_tx {
            max_tx = upbandwidth;
            max_node = y.clone();
        }
    }

    (max_node, max_tx, min_rx)
}

impl RatTree {
    fn run_rl_traffic(
        &mut self,
        root_index: usize,
        size: u64,
        vcluster: &dyn Topology,
    ) -> Vec<Flow> {
        let mut ringlets = Vec::new();
        let mut root_ringlet = (Vec::new(), 0, 0 ,0);
        let mut sidechains = Vec::new();
        for i in 0..vcluster.num_switches() - 1 {
            let mut ringlet = Vec::new();
            let tor = format!("tor_{}", i);

            let tor_upbandwidth = vcluster[vcluster.get_uplink(vcluster.get_node_index(&tor))]
                .bandwidth
                .val();

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

            let pos = ringlet.iter().position(|x| *x == root_index);

            if pos == None {
                ringlet.sort_by_key(|x| {
                    let name = format!("host_{}", x);
                    let node_ix = vcluster.get_node_index(&name);
                    vcluster[vcluster.get_uplink(node_ix)].bandwidth.val()
                });
            
                ringlet.reverse();

                let mut side_chain = Vec::new();
                let result = compact_chain(&mut ringlet, &mut side_chain, vcluster);
                sidechains.append(&mut side_chain);
                ringlets.push((ringlet, result.0, std::cmp::min(tor_upbandwidth,result.1), result.2));

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
                let mut side_chain = Vec::new();
                let result = compact_chain(&mut ringlet, &mut side_chain, vcluster);
                sidechains.append(&mut side_chain);
                root_ringlet = (ringlet, result.0, std::cmp::min(tor_upbandwidth,result.1), result.2);
            }
        }

        ringlets.sort_by_key(|x| x.2);
        ringlets.reverse();
        ringlets.insert(0, root_ringlet);

        let mut min_rx = ringlets[0].2;
        for (_ringlet, _out_node, _upbandwidth, rx) in &ringlets {
            if rx < &min_rx {
                min_rx = *rx;
            }
        }

        let mut siderack = Vec::new();
        let mut m = std::collections::HashMap::<usize, u64>::new();

        if ringlets.len() > 2 {
            let mut upbandwidth = ringlets[ringlets.len() - 2].2;
            let mut current_rate = std::cmp::min(min_rx, upbandwidth);
            let mut new_rate = std::cmp::min(min_rx, ringlets[ringlets.len() - 3].2);
            while upbandwidth < min_rx {
                let mut found = false;
                for i in 0..ringlets.len() - 3 {
                    let mut oldrate = 0;
                    if m.contains_key(&i) {
                        oldrate = m[&i];
                    }
                    if ringlets[i].2 - current_rate > new_rate + oldrate {
                        siderack.push((i, ringlets[ringlets.len() - 1].clone()));
                        ringlets.pop();

                        if !m.contains_key(&i) {
                            m.insert(i, 0);
                        }
                        *m.get_mut(&i).unwrap() += new_rate;
                        found = true;
                        break;
                    }
                }
                if !found {
                    break;
                }

                if ringlets.len() < 3 {
                    break;
                }

                if m.contains_key(&(ringlets.len() - 1)) {
                    break;
                }
                upbandwidth = ringlets[ringlets.len() - 2].2;
                current_rate = std::cmp::min(min_rx, upbandwidth);
                new_rate = std::cmp::min(min_rx, ringlets[ringlets.len() - 3].2);
            }
            log::info!("new_rate: {}", new_rate);
        }

        log::info!("min_rx: {}", min_rx);

        let mut flows = Vec::new();

        // let n = vcluster.num_hosts();

        log::trace!("root_index: {}", root_index);
        log::trace!("{:?}", ringlets);
        log::trace!("{:?}", siderack);
        log::trace!("{:?}", sidechains);

        for i in 0..ringlets[0].0.len()-1 {
            let sender = format!("host_{}", ringlets[0].0[i]);
            let receiver = format!("host_{}", ringlets[0].0[i+1]);
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
        }
        let mut last_out = ringlets[0].1;
        let mut j = 0;
        for (ringlet, out_id, _upbandwidth, _min_rx) in &ringlets {
            j += 1;
            if j == 1 {
                continue;
            }
            let sender = format!("host_{}", last_out);
            let receiver = format!("host_{}", ringlet[0]);
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
            for i in 0..ringlet.len()-1 {
                let sender = format!("host_{}", ringlet[i]);
                let receiver = format!("host_{}", ringlet[i+1]);
                let flow = Flow::new(size as usize, &sender, &receiver, None);
                flows.push(flow);
            }
            last_out = *out_id;
        }

        for (i, (ringlet, _out_id, _upbandwidth, _min_rx)) in siderack {
            let sender = format!("host_{}", ringlets[i].1);
            let receiver = format!("host_{}", ringlet[0]);
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
            for x in 0..ringlet.len() - 1 {
                let sender = format!("host_{}", ringlet[x]);
                let receiver = format!("host_{}", ringlet[x + 1]);
                let flow = Flow::new(size as usize, &sender, &receiver, None);
                flows.push(flow);
            }
        }
        
        for (i, j) in sidechains {
            let sender = format!("host_{}", i);
            let receiver = format!("host_{}", j);
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
        }

        // if sidechains.len() >= 1 {
        //     if let Ok(path) = std::env::var("NETHINT_RL_LOG_FILE") {
        //         use std::io::{Seek, Write};
        //         let mut f = std::fs::OpenOptions::new()
        //             .write(true)
        //             .create(true)
        //             .open(path)
        //             .unwrap();
        //         f.seek(std::io::SeekFrom::End(0)).unwrap();
        //         writeln!(f, "{:?}", sidechains).unwrap();
        //     }
        // }
        log::trace!("{:?}", flows);
        flows
    }
}
