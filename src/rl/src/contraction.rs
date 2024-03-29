use crate::RLAlgorithm;
use nethint::{cluster::{Topology, helpers::*}, Flow, bandwidth::Bandwidth};
use std::collections::HashMap;
use utils::algo::group_by_key;
use std::rc::Rc;

#[derive(Debug, Default)]
pub struct Contraction {
    seed: u64,
}

#[deprecated(
    since = "0.1.0",
    note = "Group broadcast not supported, please do not use it."
)]
impl Contraction {
    pub fn new(seed: u64) -> Self {
        Contraction { seed }
    }
}

impl RLAlgorithm for Contraction {
    fn run_rl_traffic(
        &mut self,
        root_index: usize,
        _group: Option<Vec<usize>>,
        size: u64,
        vcluster: Rc<dyn Topology>,
    ) -> Vec<Flow> {
        self.run_rl_traffic(root_index, size, &*vcluster)
    }
}

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
        let name = format!("host_{}", chain[chain.len() - 2]);
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

impl Contraction {
    fn run_rl_traffic(
        &mut self,
        root_index: usize,
        size: u64,
        vcluster: &dyn Topology,
    ) -> Vec<Flow> {
        let mut ringlets = Vec::new();
        let mut root_ringlet = (Vec::new(), 0, 0, 0);
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
                ringlets.push((
                    ringlet,
                    result.0,
                    std::cmp::min(tor_upbandwidth, result.1),
                    result.2,
                ));
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
                root_ringlet = (
                    ringlet,
                    result.0,
                    std::cmp::min(tor_upbandwidth, result.1),
                    result.2,
                );
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

        let mut flows = Vec::new();

        // let n = vcluster.num_hosts();

        log::trace!("root_index: {}", root_index);
        log::trace!("{:?}", ringlets);
        log::trace!("{:?}", siderack);
        log::trace!("{:?}", sidechains);

        let mut width: HashMap<String, usize> = HashMap::new();
        let mut parent: HashMap<String, String> = HashMap::new();

        for i in 0..ringlets[0].0.len() - 1 {
            let sender = format!("host_{}", ringlets[0].0[i]);
            let receiver = format!("host_{}", ringlets[0].0[i + 1]);
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
            for i in 0..ringlet.len() - 1 {
                let sender = format!("host_{}", ringlet[i]);
                let receiver = format!("host_{}", ringlet[i + 1]);
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

        for &(i, j) in &sidechains {
            let sender = format!("host_{}", i);
            let receiver = format!("host_{}", j);
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
        }

        if !std::env::var("RL_USE_NEW_ALGO")
            .unwrap_or("false".into())
            .to_lowercase()
            .parse()
            .unwrap_or(false)
        {
            return flows;
        }

        // build trees
        for f in &flows {
            let sender = f.src.clone();
            let receiver = f.dst.clone();
            *width.entry(sender.clone()).or_insert(0) += 1;
            parent.insert(receiver, sender).ok_or(()).unwrap_err();
        }

        // change original flow to new flows by `contraction`
        let mut new_flows = flows.clone();
        let mut all_to_all_flows = Vec::new();
        if ringlets.len() > 1 {
            // do it with the last ringlet
            let ringlet = ringlets.last().unwrap().0.clone();
            let group_size = ringlet.len();
            let rate_sum: u64 = ringlet.iter().map(|&i| {
                get_fwd_rate(vcluster, i, group_size).val()
            }).sum();

            log::info!("multiracks, all_to_all group size: {}, all_to_all rate: {}", group_size, rate_sum / (group_size - 1) as u64);
            let last_sender = format!("host_{}", ringlets[ringlets.len() - 2].1);
            for &i in &ringlet {
                assert_ne!(i, root_index);

                let sender = format!("host_{}", i);
                let fwd_rate_i = get_fwd_rate(vcluster, i, group_size).val();

                let size_i = (size as f64 * fwd_rate_i as f64 / rate_sum as f64) as usize;
                let flow1 = Flow::new(size_i, &last_sender, &sender, None);
                all_to_all_flows.push(flow1);

                for &j in &ringlet {
                    if j == i { continue; }
                    let receiver = format!("host_{}", j);
                    let flow2 = Flow::new(size_i, &sender, &receiver, None);
                    all_to_all_flows.push(flow2);
                }
            }

            let tx_to_last_rack = get_up_bw(vcluster, ringlets[ringlets.len() - 2].1).val();
            if tx_to_last_rack > rate_sum / (group_size - 1) as u64 {
                log::error!("{} vs {}", tx_to_last_rack, rate_sum / (group_size - 1) as u64);
            }
        } else {
            let n = vcluster.num_hosts();
            let group_size = n - 1;
            let rate_sum: u64 = (0..n).filter(|&i| i != root_index).map(|i| {
                get_fwd_rate(vcluster, i, group_size).val()
            }).sum();

            log::info!("all_to_all group size: {}, all_to_all rate: {}", group_size, rate_sum / (group_size - 1) as u64);
            let root = format!("host_{}", root_index);
            for i in 0..n {
                if i == root_index { continue; }

                let sender = format!("host_{}", i);
                let fwd_rate_i = get_fwd_rate(vcluster, i, group_size).val();

                let size_i = (size as f64 * fwd_rate_i as f64 / rate_sum as f64) as usize;
                let flow1 = Flow::new(size_i, &root, &sender, None);
                all_to_all_flows.push(flow1);

                for j in 0..n {
                    if j == i || j == root_index { continue; }
                    let receiver = format!("host_{}", j);
                    let flow2 = Flow::new(size_i, &sender, &receiver, None);
                    all_to_all_flows.push(flow2);
                }
            }

            let root_tx = get_up_bw(vcluster, root_index).val();
            if root_tx > rate_sum / (group_size - 1) as u64 {
                // it's not an error here, just want it to be highlighted
                log::error!("{} vs {}", root_tx, rate_sum / (group_size - 1) as u64);
            }
        }

        for af in all_to_all_flows {
            let mut found = false;
            for f in &mut new_flows {
                if f.src == af.src && f.dst == af.dst {
                    f.bytes = af.bytes;
                    found = true;
                }
            }
            if !found {
                new_flows.push(af);
            }
        }

        // for flow in &flows {
        //     let sender = flow.src.clone();
        //     let receiver = flow.dst.clone();

        //     if *width.get(&sender).unwrap_or(&0) == 1 && *width.get(&receiver).unwrap_or(&0) == 0 {
        //         // combine the two nodes
        //         let bw1 = vcluster[vcluster.get_uplink(vcluster.get_node_index(&sender))]
        //             .bandwidth
        //             .val();
        //         let bw2 = vcluster[vcluster.get_uplink(vcluster.get_node_index(&receiver))]
        //             .bandwidth
        //             .val();
        //         let k = bw1 as f64 / bw2 as f64;
        //         assert!(k >= 1.0, "bw1: {}, bw2: {}", bw1, bw2);

        //         let p = parent[&sender].clone();
        //         // modify the flow from the parent to sender
        //         let flow1 = Flow::new((size as f64 / (k + 1.) * k) as usize, &p, &sender, None);
        //         let flow2 = Flow::new((size as f64 / (k + 1.) * 1.) as usize, &p, &receiver, None);
        //         let flow3 = Flow::new(
        //             (size as f64 / (k + 1.) * k) as usize,
        //             &sender,
        //             &receiver,
        //             None,
        //         );
        //         let flow4 = Flow::new(
        //             (size as f64 / (k + 1.) * 1.) as usize,
        //             &receiver,
        //             &sender,
        //             None,
        //         );

        //         let mut found = 0;
        //         for f in &mut new_flows {
        //             if f.src == flow1.src && f.dst == flow1.dst {
        //                 f.bytes = flow1.bytes;
        //                 found += 1;
        //             }
        //         }

        //         assert_eq!(found, 1);

        //         let mut found = 0;
        //         for f in &mut new_flows {
        //             if f.src == flow3.src && f.dst == flow3.dst {
        //                 f.bytes = flow3.bytes;
        //                 found += 1;
        //             }
        //         }

        //         assert_eq!(found, 1);

        //         log::warn!("old: {:?}", flow);
        //         log::warn!("new 1: {:?}", flow1);
        //         log::warn!("new 2: {:?}", flow2);
        //         log::warn!("new 3: {:?}", flow3);
        //         log::warn!("new 4: {:?}", flow4);

        //         new_flows.push(flow2);
        //         new_flows.push(flow4);
        //     }
        // }

        // calculate min(min_rx, source_tx)
        let min_rx = (0..vcluster.num_hosts()).filter(|&x| x != root_index).map(|i| {
            get_down_bw(vcluster, i)
        }).min().unwrap_or(nethint::bandwidth::MAX);

        let source_tx = get_up_bw(vcluster, root_index);

        log::info!("min_rx: {}, source_tx: {}", min_rx, source_tx);

        let mut speed_bound = min_rx.val().min(source_tx.val());
        if ringlets.len() > 1 {
            let rack_sender = ringlets[ringlets.len() - 2].1;
            let tx_to_last_rack = get_up_bw(vcluster, rack_sender);
            speed_bound = speed_bound.min(tx_to_last_rack.val());
        }
        let sender = format!("host_{}", root_index);
        let receiver = format!("host_{}", (root_index + 1) % vcluster.num_hosts());
        let rx = get_down_bw(vcluster, (root_index + 1) % vcluster.num_hosts());
        let flow_rate = source_tx.val().min(rx.val());
        let single_flow_of_optimal_bound = Flow::new((size as f64 * flow_rate as f64 / speed_bound as f64) as usize, &sender, &receiver, None);
        let single_flow = vec![single_flow_of_optimal_bound];

        if std::env::var("RL_BOUND")
            .unwrap_or("false".into())
            .to_lowercase()
            .parse()
            .unwrap_or(false)
        {
            return single_flow;
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
        log::info!("{:?}", new_flows);
        new_flows


    }
}

#[inline]
fn get_fwd_rate(vc: &dyn Topology, host_id: usize, group_size: usize) -> Bandwidth {
    let tx = get_up_bw(vc, host_id);
    let rx = get_down_bw(vc, host_id);
    let fwd_rate = std::cmp::min(tx, rx * (group_size - 1));
    fwd_rate
}

#[inline]
fn get_fwd_rate2(vc: &dyn Topology, host_id: usize) -> Bandwidth {
    let tx = get_up_bw(vc, host_id);
    let rx = get_down_bw(vc, host_id);
    let fwd_rate = std::cmp::min(tx, rx);
    fwd_rate
}

impl Contraction {
    #[allow(dead_code)]
    fn __run_rl_traffic(
        &mut self,
        root_index: usize,
        size: u64,
        vc: &dyn Topology,
    ) -> Vec<Flow> {

        let n = vc.num_hosts();
        let mut groups = group_by_key(0..n, |&i| get_rack_ix(vc, i));
        let root_group_pos = groups.iter().position(|g| g.contains(&root_index)).unwrap();
        groups.swap(0, root_group_pos);

        // ranks of subroot
        let sub_roots: Vec<usize> = groups.iter().map(|g| {
            if g.contains(&root_index) {
                root_index
            } else {
                g.iter().copied().max_by_key(|&rank| get_fwd_rate2(vc, rank)).unwrap()
            }
        }).collect();

        let mut flows = Vec::new();
        for (g, &sub_root) in groups.iter().zip(&sub_roots) {
            let rate_sum: u64 = g.iter().filter(|&&i| i != sub_root).map(|&i| {
                get_fwd_rate(vc, i, g.len() - 1).val()
            }).sum();
            let sub_root_s = format!("host_{}", sub_root);

            for &i in g {
                if i == sub_root {
                    continue;
                }

                let sender = format!("host_{}", i);
                let size_i = if g.len() == 2 {
                    size as usize
                } else {
                    let fwd_rate_i = get_fwd_rate(vc, i, g.len() - 1).val();
                    let size_i = (size as f64 * fwd_rate_i as f64 / rate_sum as f64) as usize;
                    size_i
                };

                let flow1 = Flow::new(size_i, &sub_root_s, &sender, None);
                flows.push(flow1);

                for &j in g {
                    if j == sub_root || j == i {
                        continue;
                    }

                    let receiver = format!("host_{}", j);
                    let flow2 = Flow::new(size_i, &sender, &receiver, None);
                    flows.push(flow2);
                }
            }
        }

        let rate_sum: u64 = sub_roots.iter().filter(|&&i| i != root_index).map(|&i| {
            get_fwd_rate2(vc, i).val()
        }).sum();

        let root = format!("host_{}", root_index);
        for &i in &sub_roots {
            if i == root_index {
                continue;
            }

            let sender = format!("host_{}", i);
            let fwd_rate_i = get_fwd_rate2(vc, i).val();
            let size_i = (size as f64 * fwd_rate_i as f64 / rate_sum as f64) as usize;

            let flow1 = Flow::new(size_i, &root, &sender, None);
            flows.push(flow1);

            for &j in &sub_roots {
                if j == root_index || j == i {
                    continue;
                }

                let receiver = format!("host_{}", j);
                let flow2 = Flow::new(size_i, &sender, &receiver, None);
                flows.push(flow2);
            }
        }

        log::info!("flows: {:?}", flows);
        flows
    }
}
