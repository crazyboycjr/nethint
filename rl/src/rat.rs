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
    fn run_rl_traffic(
        &mut self,
        root_index: usize,
        size: u64,
        vcluster: &dyn Topology,
    ) -> Vec<Flow> {
        let mut ringlets = Vec::new();
        let mut sidechains = Vec::new();
        for i in 0..vcluster.num_switches() - 1 {
            let mut ringlet = Vec::new();
            let tor = format!("tor_{}", i);

            let mut tor_upbandwidth = vcluster[vcluster.get_uplink(vcluster.get_node_index(&tor))]
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
                let name = format!("host_{}", ringlet[0]);
                let node_ix = vcluster.get_node_index(&name);
                let mut min_rx = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();
                for x in &ringlet {
                    let name = format!("host_{}", x);
                    let node_ix = vcluster.get_node_index(&name);
                    let rx = vcluster[vcluster.get_reverse_link(vcluster.get_uplink(node_ix))]
                        .bandwidth
                        .val();
                    if rx < min_rx {
                        min_rx = rx;
                    }
                }

                ringlet.sort_by_key(|x| {
                    let name = format!("host_{}", x);
                    let node_ix = vcluster.get_node_index(&name);
                    vcluster[vcluster.get_uplink(node_ix)].bandwidth.val()
                });

                ringlet.reverse();

                let mut name = format!("host_{}", ringlet[ringlet.len() - 1]);
                let mut node_ix = vcluster.get_node_index(&name);
                let mut upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();

                let mut old_chain_rate = std::cmp::min(upbandwidth, min_rx);

                let mut name_l2 = format!("host_{}", ringlet[ringlet.len() - 2]);
                let mut node_ix_l2 = vcluster.get_node_index(&name_l2);
                let mut upbandwidth_l2 = vcluster[vcluster.get_uplink(node_ix_l2)].bandwidth.val();
                let mut new_chain_rate = std::cmp::min(upbandwidth_l2, min_rx);

                let mut m = std::collections::HashMap::<usize, u64>::new();

                while upbandwidth < min_rx {
                    let mut found = false;
                    for i in 0..ringlet.len() - 1 {
                        let name = format!("host_{}", ringlet[i]);
                        let node_ix = vcluster.get_node_index(&name);
                        let tx = vcluster[vcluster.get_reverse_link(vcluster.get_uplink(node_ix))]
                            .bandwidth
                            .val();

                        let mut oldrate = 0;
                        if m.contains_key(&ringlet[i]) {
                            oldrate += m[&ringlet[i]];
                        }

                        if tx - new_chain_rate > old_chain_rate + oldrate {
                            sidechains.push((ringlet[i], ringlet[ringlet.len() - 1]));
                            ringlet.pop();
                            // log::error!("{} - {} > {}", tx, new_chain_rate, old_chain_rate);
                            if !m.contains_key(&ringlet[i]) {
                                m.insert(ringlet[i], 0);
                            }
                            *m.get_mut(&ringlet[i]).unwrap() += new_chain_rate;
                            // m[&ringlet[i]] += new_chain_rate;
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        break;
                    }

                    if m.contains_key(&ringlet[ringlet.len() - 1]) {
                        break;
                    }

                    name = format!("host_{}", ringlet[ringlet.len() - 1]);
                    node_ix = vcluster.get_node_index(&name);
                    upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();

                    old_chain_rate = std::cmp::min(upbandwidth, min_rx);

                    name_l2 = format!("host_{}", ringlet[ringlet.len() - 2]);
                    node_ix_l2 = vcluster.get_node_index(&name_l2);
                    upbandwidth_l2 = vcluster[vcluster.get_uplink(node_ix_l2)].bandwidth.val();
                    new_chain_rate = std::cmp::min(upbandwidth_l2, min_rx);
                }

                let name = format!("host_{}", ringlet[ringlet.len() - 1]);
                let node_ix = vcluster.get_node_index(&name);
                let upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();

                if tor_upbandwidth > upbandwidth {
                    tor_upbandwidth = upbandwidth;
                }

                ringlets.push((ringlet, tor_upbandwidth, min_rx));
            } else {
                let pos = pos.unwrap();
                ringlet.remove(pos);

                let name = format!("host_{}", ringlet[0]);
                let node_ix = vcluster.get_node_index(&name);
                let mut min_rx = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();
                for x in &ringlet {
                    let name = format!("host_{}", x);
                    let node_ix = vcluster.get_node_index(&name);
                    let rx = vcluster[vcluster.get_reverse_link(vcluster.get_uplink(node_ix))]
                        .bandwidth
                        .val();
                    if rx < min_rx {
                        min_rx = rx;
                    }
                }

                ringlet.sort_by_key(|x| {
                    let name = format!("host_{}", x);
                    let node_ix = vcluster.get_node_index(&name);
                    vcluster[vcluster.get_uplink(node_ix)].bandwidth.val()
                });

                ringlet.reverse();

                let mut name = format!("host_{}", ringlet[ringlet.len() - 1]);
                let mut node_ix = vcluster.get_node_index(&name);
                let mut upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();

                let mut old_chain_rate = std::cmp::min(upbandwidth, min_rx);

                if ringlet.len() >= 2 {
                    let mut name_l2 = format!("host_{}", ringlet[ringlet.len() - 2]);
                    let mut node_ix_l2 = vcluster.get_node_index(&name_l2);
                    let mut upbandwidth_l2 =
                        vcluster[vcluster.get_uplink(node_ix_l2)].bandwidth.val();
                    let mut new_chain_rate = std::cmp::min(upbandwidth_l2, min_rx);

                    let mut m = std::collections::HashMap::<usize, u64>::new();

                    while upbandwidth < min_rx {
                        let mut found = false;
                        for i in 0..ringlet.len() - 1 {
                            let name = format!("host_{}", ringlet[i]);
                            let node_ix = vcluster.get_node_index(&name);
                            let tx = vcluster
                                [vcluster.get_reverse_link(vcluster.get_uplink(node_ix))]
                            .bandwidth
                            .val();

                            let mut oldrate = 0;
                            if m.contains_key(&ringlet[i]) {
                                oldrate += m[&ringlet[i]];
                            }

                            if tx - new_chain_rate > old_chain_rate + oldrate {
                                sidechains.push((ringlet[i], ringlet[ringlet.len() - 1]));
                                ringlet.pop();
                                log::error!("{} - {} > {} + {}", tx, new_chain_rate, old_chain_rate, oldrate);
                                if !m.contains_key(&ringlet[i]) {
                                    m.insert(ringlet[i], 0);
                                }
                                *m.get_mut(&ringlet[i]).unwrap() += new_chain_rate;
                                // m[&ringlet[i]] += new_chain_rate;
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            break;
                        }

                        if m.contains_key(&ringlet[ringlet.len() - 1]) {
                            break;
                        }

                        name = format!("host_{}", ringlet[ringlet.len() - 1]);
                        node_ix = vcluster.get_node_index(&name);
                        upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();

                        old_chain_rate = std::cmp::min(upbandwidth, min_rx);

                        name_l2 = format!("host_{}", ringlet[ringlet.len() - 2]);
                        node_ix_l2 = vcluster.get_node_index(&name_l2);
                        upbandwidth_l2 = vcluster[vcluster.get_uplink(node_ix_l2)].bandwidth.val();
                        new_chain_rate = std::cmp::min(upbandwidth_l2, min_rx);
                    }
                }

                ringlet.insert(0, root_index);
                ringlets.push((ringlet, u64::MAX, min_rx));
            }
        }

        ringlets.sort_by_key(|x| x.1);
        ringlets.reverse();

        let name = format!("host_{}", ringlets[0].0[ringlets[0].0.len() - 1]);
        let node_ix = vcluster.get_node_index(&name);
        let upbandwidth = vcluster[vcluster.get_uplink(node_ix)].bandwidth.val();

        ringlets[0].1 = upbandwidth;

        let mut min_rx = ringlets[0].2;
        for (_ringlet, _upbandwidth, rx) in &ringlets {
            if rx < &min_rx {
                min_rx = *rx;
            }
        }

        let mut siderack = Vec::new();
        let mut m = std::collections::HashMap::<usize, u64>::new();

        if ringlets.len() >= 3 {
            let mut upbandwidth = ringlets[ringlets.len() - 2].1;
            let mut current_rate = std::cmp::min(min_rx, upbandwidth);
            let new_rate = std::cmp::min(min_rx, ringlets[ringlets.len() - 3].1);
            while upbandwidth < min_rx {
                let mut found = false;
                for i in 0..ringlets.len() - 2 {
                    let mut oldrate = 0;
                    if m.contains_key(&i) {
                        oldrate += m[&i];
                    }
                    if ringlets[i].1 - current_rate > new_rate + oldrate {
                        siderack.push((i, ringlets[ringlets.len() - 1].0.clone()));
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
                upbandwidth = ringlets[ringlets.len() - 2].1;
                current_rate = std::cmp::min(min_rx, upbandwidth);
            }
        }

        let mut ring = Vec::new();
        for (ringlet, _upbandwidth, _min_rx) in ringlets {
            for ele in ringlet {
                ring.push(ele);
            }
        }

        let mut flows = Vec::new();

        let n = vcluster.num_hosts();

        log::error!("root_index: {}", root_index);
        log::error!("{:?}", ring);
        log::error!("{:?}", siderack);
        log::error!("{:?}", sidechains);

        for i in 0..ring.len() - 1 {
            let sender = format!("host_{}", ring[i]);
            let receiver = format!("host_{}", ring[(i + 1) % n]);
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

        for (i, j) in siderack {
            let sender = format!("host_{}", i);
            let receiver = format!("host_{}", j[0]);
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
            for x in 0..j.len() - 1 {
                let sender = format!("host_{}", j[x]);
                let receiver = format!("host_{}", j[x + 1]);
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

        flows
    }
}
