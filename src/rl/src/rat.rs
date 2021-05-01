use crate::RLAlgorithm;
use nethint::{
    cluster::{NodeIx, Topology},
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
        self._run_rl_traffic(root_index, size, vcluster)
        // mytest::run_rl_traffic(root_index, size, vcluster)
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
    fn _run_rl_traffic(
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
        }

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

mod mytest {
    use super::*;
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct TreeIdx(usize);
    #[derive(Debug, Clone, Default)]
    pub struct Node {
        rank: usize,
        parent: Option<TreeIdx>,
        children: Vec<TreeIdx>,
    }

    impl Node {
        fn new(rank: usize) -> Self {
            Node {
                rank,
                ..Default::default()
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    pub struct Tree {
        nodes: Vec<Node>,
    }

    impl Tree {
        fn push(&mut self, n: Node) -> TreeIdx {
            let ix = TreeIdx(self.nodes.len());
            self.nodes.push(n);
            ix
        }

        fn connect(&mut self, p: TreeIdx, c: TreeIdx) {
            self[p].children.push(c);
            self[c].parent.replace(p).unwrap_none();
        }

        fn root(&self) -> TreeIdx {
            assert!(!self.nodes.is_empty());
            TreeIdx(0)
        }

        fn cut(&mut self, c: TreeIdx) {
            if let Some(p) = self[c].parent {
                let pos = self[p].children.iter().position(|&x| x == c).unwrap();
                self[p].children.remove(pos);
            }
            self[c].parent = None;
        }

        fn iter(&self) -> std::slice::Iter<Node> {
            self.nodes.iter()
        }

        fn iter_mut(&mut self) -> std::slice::IterMut<Node> {
            self.nodes.iter_mut()
        }
    }

    impl std::ops::Index<TreeIdx> for Tree {
        type Output = Node;
        fn index(&self, index: TreeIdx) -> &Self::Output {
            assert!(index.0 < self.nodes.len());
            &self.nodes[index.0]
        }
    }

    impl std::ops::IndexMut<TreeIdx> for Tree {
        fn index_mut(&mut self, index: TreeIdx) -> &mut Self::Output {
            assert!(index.0 < self.nodes.len());
            &mut self.nodes[index.0]
        }
    }

    pub fn solve(
        mut tx: Vec<u64>,
        rx: Vec<u64>,
        src_rate: u64,
        out_rate: u64,
        src_is_root: bool,
    ) -> (Tree, TreeIdx, u64, u64) {
        assert_eq!(tx.len(), rx.len());
        let n = tx.len();
        let mut ranks: Vec<usize> = (0..n).collect();
        // sort nodes by tx rate
        ranks.sort_by_key(|&i| tx[i]);
        ranks.reverse();
        // get min_rx rate
        let min_rx = rx.iter().copied().chain(std::iter::once(src_rate)).min().unwrap();
        if src_is_root {
            tx.push(src_rate);
        }
        // chaining all the nodes by decending tx_rate
        let mut tree = Tree::default();
        let mut last_node_ix = if src_is_root {
            Some(tree.push(Node::new(n))) // give root_rank: n
        } else {
            None
        };
        for &r in ranks.iter() {
            let new_node_ix = tree.push(Node::new(r));
            if last_node_ix.is_some() {
                tree.connect(last_node_ix.unwrap(), new_node_ix);
            }
            last_node_ix.replace(new_node_ix);
        }
        let mut last_node_ix = last_node_ix.unwrap();
        // rearrange the tree
        let adjust = |node_ix: TreeIdx, tree: &mut Tree| -> bool {
            // cut the last node
            log::info!("solve, 2");
            let orig_parent = tree[node_ix].parent;
            let rate_before = calculate_flow_rate(tree.root(), &tx, min_rx, &tree);
            tree.cut(node_ix);
            log::info!("solve, 3");
            let rate_after = calculate_flow_rate(tree.root(), &tx, min_rx, &tree);
            // the main chain is composed by the first child of each node
            let mut now = tree.root();
            let mut found = false;
            while !tree[now].children.is_empty() {
                log::info!(
                    "rate_before: {}, rate_after: {}, {} - {} > {}",
                    rate_before,
                    rate_after,
                    tx[tree[now].rank],
                    tree[now].children.len() as u64 * rate_after,
                    rate_before
                );
                if tx[tree[now].rank] - tree[now].children.len() as u64 * rate_after > rate_before
                    && orig_parent.map(|p| p != now).unwrap_or(false)
                {
                    found = true;
                    break;
                }
                // go down
                if let Some(&first_child) = tree[now].children.first() {
                    now = first_child;
                } else {
                    break;
                }
            }
            if now != node_ix {
                log::info!("connecting p: {:?}, c: {:?}", now, node_ix);
                tree.connect(now, node_ix);
            }
            found
        };
        log::info!(
            "solve, 0, last_node_ix: {:?}, tree: {:?}",
            last_node_ix,
            tree
        );
        loop {
            let parent = tree[last_node_ix].parent;
            let found = adjust(last_node_ix, &mut tree);
            if !found {
                break;
            }
            if let Some(p) = parent {
                last_node_ix = p;
            } else {
                break;
            }
        }
        log::info!("solve, 1, tree: {:?}", tree);
        let final_rx_rate = calculate_flow_rate(tree.root(), &tx, min_rx, &tree);
        // now that we have the tree, check if we still need to go outside the tree
        // append the extra virtual node to the node with the maximal remaining sending rate
        // the node with maximal remaining sending rate must reside on the main chain
        let out_node = tree.push(Node::new(n + 1));
        tree.connect(last_node_ix, out_node);
        adjust(out_node, &mut tree);
        let p = tree[out_node].parent.unwrap();
        tree.cut(out_node);
        tree.nodes.pop();
        let final_tx_rate =
            out_rate.min(tx[tree[p].rank] - tree[p].children.len() as u64 * final_rx_rate);
        (tree, p, final_rx_rate, final_tx_rate)
    }

    fn calculate_flow_rate(p: TreeIdx, tx: &[u64], min_rx: u64, tree: &Tree) -> u64 {
        let mut ret = min_rx;
        if !tree[p].children.is_empty() {
            ret = ret.min(tx[tree[p].rank] / tree[p].children.len() as u64);
        }
        for &c in &tree[p].children {
            ret = ret.min(calculate_flow_rate(c, tx, min_rx, tree));
        }
        ret
    }

    fn get_node_id(ix: NodeIx, vc: &dyn Topology) -> usize {
        vc[ix]
            .name
            .strip_prefix("host_")
            .unwrap()
            .parse::<usize>()
            .unwrap()
    }

    #[allow(dead_code)]
    pub fn run_rl_traffic(root_index: usize, size: u64, vc: &dyn Topology) -> Vec<Flow> {
        let root_host_name = format!("host_{}", root_index);
        let root_host_ix = vc.get_node_index(&root_host_name);

        log::info!("run_rl_traffic 0");
        let mut chunks = Vec::new();
        let num_racks = vc.num_switches() - 1;
        let mut start_chunk_idx = 0;
        for i in 0..num_racks {
            log::info!("run_rl_traffic 0.0.{}/{}", i, num_racks);
            let tor_name = format!("tor_{}", i);
            let tor_ix = vc.get_node_index(&tor_name);
            let mut node_ids: Vec<_> = vc
                .get_downlinks(tor_ix)
                .map(|&l| get_node_id(vc.get_target(l), vc))
                .collect();
            let (mut tx, mut rx): (Vec<_>, Vec<_>) = vc
                .get_downlinks(tor_ix)
                .map(|&link_ix| {
                    (
                        vc[vc.get_reverse_link(link_ix)].bandwidth.val(),
                        vc[link_ix].bandwidth.val(),
                    )
                })
                .unzip();
            let root_pos = vc
                .get_downlinks(tor_ix)
                .map(|&l| vc.get_target(l))
                .position(|x| x == root_host_ix);
            log::info!("run_rl_traffic 0.1.{}", i);
            if let Some(pos) = root_pos {
                // the root node is the real node which is the root_index
                log::info!("run_rl_traffic 0.2.{}", i);
                start_chunk_idx = chunks.len();
                node_ids.remove(pos);
                rx.remove(pos);
                let src_rate = tx.remove(pos);
                let out_rate = vc[vc.get_uplink(tor_ix)].bandwidth.val();
                let (mut tree, out_id, rx_rate, tx_rate) = solve(tx, rx, src_rate, out_rate, true);
                tree.iter_mut().for_each(|n| {
                    if n.rank == node_ids.len() {
                        n.rank = root_index;
                    } else {
                        n.rank = node_ids[n.rank];
                    }
                });
                chunks.push((tree, out_id, rx_rate, tx_rate));
            } else {
                // the root node is a virtual node
                log::info!("run_rl_traffic 0.3.{}", i);
                let src_rate = vc[vc.get_reverse_link(vc.get_uplink(tor_ix))]
                    .bandwidth
                    .val();
                let out_rate = vc[vc.get_uplink(tor_ix)].bandwidth.val();
                let (mut tree, out_id, rx_rate, tx_rate) = solve(tx, rx, src_rate, out_rate, false);
                tree.iter_mut().for_each(|n| {
                    assert!(n.rank < node_ids.len());
                    n.rank = node_ids[n.rank];
                });
                chunks.push((tree, out_id, rx_rate, tx_rate));
            };
        }

        log::info!("run_rl_traffic 1");
        let (mut rack_tx, mut rack_rx): (Vec<_>, Vec<_>) =
            chunks.iter().map(|x| (x.2, x.3)).unzip();
        let root_chunk = chunks.remove(start_chunk_idx);
        let src_rate = rack_tx.remove(start_chunk_idx);
        rack_rx.remove(start_chunk_idx);
        let out_rate = u64::MAX;
        let (rack_tree, _, _ans, _) = solve(rack_tx, rack_rx, src_rate, out_rate, true);

        log::info!("run_rl_traffic 2");
        let mut flows = Vec::new();

        let mut emit_flow = |a, b| {
            let sender = format!("host_{}", a);
            let receiver = format!("host_{}", b);
            let flow = Flow::new(size as usize, &sender, &receiver, None);
            flows.push(flow);
        };

        let mut last_out_idx = None;
        rack_tree.iter().for_each(|n| {
            let chunk = if n.rank == chunks.len() {
                &root_chunk
            } else {
                &chunks[n.rank]
            };
            let tree = &chunk.0;
            tree.iter().for_each(|p| {
                for &c in &p.children {
                    emit_flow(p.rank, tree[c].rank);
                }
            });

            if let Some(out_idx) = last_out_idx {
                emit_flow(out_idx, tree[tree.root()].rank);
            }

            last_out_idx.replace(tree[chunk.1].rank);
        });

        log::info!("run_rl_traffic 3");

        flows
    }
}
