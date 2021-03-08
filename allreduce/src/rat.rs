use lpsolve;
use std::{
    collections::{BTreeMap, HashMap},
    str::from_utf8,
};

use crate::AllReduceAlgorithm;
use nethint::{
    bandwidth::BandwidthTrait,
    cluster::{Link, LinkIx, NodeIx, Topology},
    Flow,
};

#[derive(Debug, Default)]
pub struct RatAllReduce {
    // cache the result, if nethint hasn't been changed, no need to run LP again
    last_size: Option<u64>,
    last_hint: Option<HashMap<LinkIx, Link>>,
    last_result: Vec<Flow>,
}

impl RatAllReduce {
    pub fn new() -> Self {
        Default::default()
    }

    fn dump_vcluster(vcluster: &dyn Topology) -> HashMap<LinkIx, Link> {
        vcluster
            .all_links()
            .map(|link_ix| (link_ix, vcluster[link_ix].clone()))
            .collect()
    }

    pub fn check_cache(&self, (size, vcluster): (u64, &dyn Topology)) -> bool {
        if self.last_size.is_none() || self.last_hint.is_none() {
            return false;
        }

        if self.last_size.unwrap() != size {
            return false;
        }

        let last_hint = self.last_hint.as_ref().unwrap();
        vcluster.all_links().all(|link_ix| {
            let l1 = vcluster[link_ix].clone();
            if let Some(l2) = last_hint.get(&link_ix) {
                let f = l1.bandwidth + 1.gbps() >= l2.bandwidth
                    && l2.bandwidth + 1.gbps() >= l1.bandwidth;
                if !f {
                    log::debug!("l1: {}, l2: {}", l1, l2);
                }
                f
            } else {
                false
            }
        })
    }

    pub fn update_cache(&mut self, (size, vcluster): (u64, &dyn Topology), flows: Vec<Flow>) {
        self.last_size.replace(size);
        self.last_hint.replace(Self::dump_vcluster(vcluster));
        self.last_result = flows;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TreeIdx(usize);
#[derive(Debug, Clone, Default)]
struct Node {
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
struct Tree {
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

fn group_by_key<T, K, F>(iter: impl Iterator<Item = T>, mut f: F) -> Vec<Vec<T>>
where
    F: FnMut(&T) -> K,
    K: Ord,
{
    let mut groups: BTreeMap<K, Vec<T>> = Default::default();
    for i in iter {
        let key = f(&i);
        groups.entry(key).or_default().push(i);
    }
    groups.into_values().collect()
}

fn construct_tree_offset(groups: &Vec<Vec<usize>>, offset: usize) -> Tree {
    let mut tree = Tree::default();

    let ranks: Vec<_> = groups.iter().flatten().copied().collect();
    assert!(offset < ranks.len());

    let root_rank = ranks[offset];
    let root = tree.push(Node::new(root_rank));
    for group in groups {
        assert!(!group.is_empty());
        // COMMENT(cjr): there's a case where group[offset % group.len()] != ranks[offset]
        // e.g. the sub-tree sizes are (2, 3, 3), and offset = 5
        // in this case, 5 is the root, but group[offset % group.len()] = 7, a mismatch
        if group.iter().find(|&&x| x == root_rank).is_some() {
            for &r in group {
                if r != root_rank {
                    let tn = tree.push(Node::new(r));
                    tree.connect(root, tn);
                }
            }
        } else {
            let first_rank = group[offset % group.len()];
            assert_ne!(first_rank, root_rank);
            let sub_root = {
                let sub_root = tree.push(Node::new(first_rank));
                tree.connect(root, sub_root);
                sub_root
            };
            for i in 1..group.len() {
                let r = group[(offset + i) % group.len()];
                assert_ne!(r, root_rank);
                let tn = tree.push(Node::new(r));
                tree.connect(sub_root, tn);
            }
        }
    }

    tree
}

fn get_rack_ix(vcluster: &dyn Topology, rank: usize) -> NodeIx {
    let host_name = format!("host_{}", rank);
    let host_ix = vcluster.get_node_index(&host_name);
    let rack_ix = vcluster.get_target(vcluster.get_uplink(host_ix));
    rack_ix
}

fn generate_rats(vcluster: &dyn Topology) -> Vec<Tree> {
    let n = vcluster.num_hosts();

    let groups = group_by_key(0..n, |&i| get_rack_ix(vcluster, i));

    let mut tree_set = Vec::with_capacity(n);
    for i in 0..n {
        let tree_i = construct_tree_offset(&groups, i);
        tree_set.push(tree_i);
    }
    tree_set
}

// min: +C17;
//
// /* Constraints */
// +8 C1 +C2 +C3 +C4 +C5 +C6 +C7 +C8 +8 C9 +C10 +C11 +C12 +C13 +C14 +C15 +C16 -25000000000 C17 <= 0;
// +8 C1 +C2 +C3 +C4 +C5 +C6 +C7 +C8 +8 C9 +C10 +C11 +C12 +C13 +C14 +C15 +C16 -17127014614 C17 <= 0;
// +C1 +8 C2 +C3 +C4 +C5 +C6 +C7 +C8 +C9 +8 C10 +C11 +C12 +C13 +C14 +C15 +C16 -69365801680 C17 <= 0;
// +C1 +C2 +8 C3 +C4 +C5 +C6 +C7 +C8 +C9 +C10 +8 C11 +C12 +C13 +C14 +C15 +C16 -100000000000 C17 <= 0;
// +C1 +C2 +C3 +8 C4 +C5 +C6 +C7 +C8 +C9 +C10 +C11 +8 C12 +C13 +C14 +C15 +C16 -12898901680 C17 <= 0;
// +C1 +8 C2 +C3 +C4 +C5 +C6 +C7 +C8 +C9 +8 C10 +C11 +C12 +C13 +C14 +C15 +C16 -33333333333 C17 <= 0;
// +C1 +C2 +C3 +C4 +8 C5 +C6 +C7 +C8 +C9 +C10 +C11 +C12 +8 C13 +C14 +C15 +C16 -25000000000 C17 <= 0;
// +C1 +C2 +C3 +C4 +C5 +8 C6 +C7 +C8 +C9 +C10 +C11 +C12 +C13 +8 C14 +C15 +C16 -87931984080 C17 <= 0;
// +C1 +C2 +8 C3 +C4 +C5 +C6 +C7 +C8 +C9 +C10 +8 C11 +C12 +C13 +C14 +C15 +C16 -20000000000 C17 <= 0;
// +C1 +C2 +C3 +8 C4 +C5 +C6 +C7 +C8 +C9 +C10 +C11 +8 C12 +C13 +C14 +C15 +C16 -12895425360 C17 <= 0;
// +C1 +C2 +C3 +C4 +8 C5 +C6 +C7 +C8 +C9 +C10 +C11 +C12 +8 C13 +C14 +C15 +C16 -50000000000 C17 <= 0;
// +C1 +C2 +C3 +C4 +C5 +8 C6 +C7 +C8 +C9 +C10 +C11 +C12 +C13 +8 C14 +C15 +C16 -12890072400 C17 <= 0;
// +C1 +C2 +C3 +C4 +C5 +C6 +8 C7 +C8 +C9 +C10 +C11 +C12 +C13 +C14 +8 C15 +C16 -12890072400 C17 <= 0;
// +C1 +C2 +C3 +C4 +C5 +C6 +8 C7 +C8 +C9 +C10 +C11 +C12 +C13 +C14 +8 C15 +C16 -21248039653 C17 <= 0;
// +C1 +C2 +C3 +C4 +C5 +C6 +C7 +8 C8 +C9 +C10 +C11 +C12 +C13 +C14 +C15 +8 C16 -20000000000 C17 <= 0;
// +C1 +C2 +C3 +C4 +C5 +C6 +C7 +8 C8 +C9 +C10 +C11 +C12 +C13 +C14 +C15 +8 C16 -20000000000 C17 <= 0;
// +C1 +C2 +C3 +C4 +C5 +C6 +C7 +C8 +C9 +C10 +C11 +C12 +C13 +C14 +C15 +C16 = 1;
// R18: +C1 >= 0;
// R19: +C2 >= 0;
// R20: +C3 >= 0;
// R21: +C4 >= 0;
// R22: +C5 >= 0;
// R23: +C6 >= 0;
// R24: +C7 >= 0;
// R25: +C8 >= 0;
// R26: +C9 >= 0;
// R27: +C10 >= 0;
// R28: +C11 >= 0;
// R29: +C12 >= 0;
// R30: +C13 >= 0;
// R31: +C14 >= 0;
// R32: +C15 >= 0;
// R33: +C16 >= 0;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_lp() {
        let n = 16;
        let mut lp = lpsolve::Problem::new(0, n as i32 + 1).unwrap();

        let bw = vec![
            25000000000.0,
            17127014614.0,
            69365801680.0,
            100000000000.0,
            12898901680.0,
            33333333333.0,
            25000000000.0,
            87931984080.0,
            20000000000.0,
            12895425360.0,
            50000000000.0,
            12890072400.0,
            12890072400.0,
            21248039653.0,
            20000000000.0,
            20000000000.0,
        ];

        let coeff = [
            [
                8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
            ],
            [
                8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
            ],
            [
                1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
            ],
            [
                1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0,
            ],
            [
                1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0,
            ],
            [
                1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
            ],
            [
                1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0,
            ],
            [
                1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0,
            ],
            [
                1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0,
            ],
            [
                1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0,
            ],
            [
                1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0,
            ],
            [
                1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0,
            ],
            [
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0,
            ],
            [
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0,
            ],
            [
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0,
            ],
            [
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 8.0,
            ],
        ];

        let mut obj_func = vec![0.; n + 1];
        obj_func.push(1.);
        lp.set_objective_function(&obj_func);

        for (i, co) in coeff.iter().enumerate() {
            let mut constraint: Vec<f64> = co.clone().into();
            constraint.insert(0, 0.);
            // COMMENT(cjr): precision issue here, if we do not divide 10_000_000.0, the lpsolve will just refuse to work
            constraint.push(-1.0 * bw[i] / 10_000_000.0);
            lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Le);
        }

        let mut constraint = vec![1.; n + 1];
        constraint.push(0.);
        lp.add_constraint(&constraint, 1., lpsolve::ConstraintType::Eq);

        // \vec{w} >= 0
        for i in 0..n {
            let mut constraint = vec![0.; n + 2];
            constraint[i + 1] = 1.;
            lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Ge);
        }

        let mut buffer = Vec::new();
        lp.write_lp(&mut buffer);
        let problem_str = from_utf8(&buffer).unwrap();
        println!("{}", problem_str);

        let status = lp.solve();
        assert_eq!(status, lpsolve::SolveStatus::Optimal);
        println!("status: {:?}", status);

        let mut w = vec![0.; n + 1];
        lp.get_solution_variables(&mut w);
        println!("weights: {:?}", w);
    }
}

fn linear_programming(vcluster: &dyn Topology, tree_set: &[Tree], size: u64) -> Vec<f64> {
    let n = vcluster.num_hosts();
    let mut lp = lpsolve::Problem::new(0, n as i32 + 1).unwrap();

    // set verbosity
    unsafe {
        lpsolve_sys::set_verbose(lp.to_lprec(), lpsolve::Verbosity::Critical as i32);
    }

    // host level constraints
    let bw: Vec<_> = (0..n)
        .map(|i| {
            let host_name = format!("host_{}", i);
            let host_ix = vcluster.get_node_index(&host_name);
            std::cmp::min(
                vcluster[vcluster.get_uplink(host_ix)].bandwidth.val(),
                vcluster[vcluster.get_reverse_link(vcluster.get_uplink(host_ix))]
                    .bandwidth
                    .val(),
            ) as f64
        })
        .collect();

    let mut deg = vec![vec![0.; tree_set.len() + 1]; n];
    for (k, tree) in tree_set.iter().enumerate() {
        for node in &tree.nodes {
            let r = node.rank;
            deg[r][k + 1] = (node.children.len() + node.parent.iter().len()) as f64;
        }
    }

    // rack level constraints
    let r = vcluster.num_switches() - 1;
    let rack_bw: Vec<_> = (0..r)
        .map(|i| {
            let tor_name = format!("tor_{}", i);
            let tor_ix = vcluster.get_node_index(&tor_name);
            // allreduce tasks has symmetric bi-directional traffic
            std::cmp::min(
                vcluster[vcluster.get_uplink(tor_ix)].bandwidth.val(),
                vcluster[vcluster.get_reverse_link(vcluster.get_uplink(tor_ix))]
                    .bandwidth
                    .val(),
            ) as f64
        })
        .collect();
    let mut rack_deg = vec![vec![0.; tree_set.len() + 1]; r];

    let groups = group_by_key(0..n, |&i| get_rack_ix(vcluster, i));
    let mut rank_to_rack_id: BTreeMap<usize, usize> = Default::default();
    for (_i, group) in groups.into_iter().enumerate() {
        let host_ix = vcluster.get_node_index(&format!("host_{}", group[0]));
        let tor_ix = vcluster.get_target(vcluster.get_uplink(host_ix));
        let rack_id: usize = vcluster[tor_ix]
            .name
            .strip_prefix("tor_")
            .unwrap()
            .parse()
            .unwrap();
        for rank in group {
            rank_to_rack_id.insert(rank, rack_id).unwrap_none();
        }
    }
    for (k, tree) in tree_set.iter().enumerate() {
        let root = tree.root();
        let root_rank = tree[root].rank;
        let root_rack_id = rank_to_rack_id[&root_rank];
        let mut degs = 0;
        for &child in &tree[root].children {
            let child_rank = tree[child].rank;
            if rank_to_rack_id[&child_rank] != root_rack_id {
                degs += 1;
            }
        }
        rack_deg[root_rack_id][k + 1] += degs as f64;
    }

    // minimize y
    let mut obj_func = vec![0.; n + 1];
    obj_func.push(1.);
    lp.set_objective_function(&obj_func);

    // D is a degree matrix. D[i][k] is the degree of node i in kth aggregation tree.
    // Bw is a diagonal bandwidth matrix. Bw[i][i] = bw[i];
    // Bw^{-1} \cdot D \cdot \vec{w} <= \vec{y}
    for (i, deg) in deg.iter().enumerate() {
        let mut constraint = deg.clone();
        constraint.push(-1.0 * bw[i] / size as f64);
        lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Le);
    }

    // Dr is a degree matrix. Dr[i][k] is the degree of ToR i in kth aggregation tree.
    // Bwr is a diagonal bandwidth matrix. Bwr[i][i] = bw_rack[i];
    // Bwr^{-1} \cdot Dr \cdot \vec{w} <= \vec{y}
    for (i, deg) in rack_deg.iter().enumerate() {
        let mut constraint = deg.clone();
        constraint.push(-1.0 * rack_bw[i] / size as f64);
        lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Le);
    }

    // w1 + w2 + ... wn = 1.0
    let mut constraint = vec![1.; n + 1];
    constraint.push(0.);
    lp.add_constraint(&constraint, 1., lpsolve::ConstraintType::Eq);

    // \vec{w} >= 0
    for i in 0..n {
        let mut constraint = vec![0.; n + 2];
        constraint[i + 1] = 1.;
        lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Ge);
    }

    let mut buffer = Vec::new();
    lp.write_lp(&mut buffer);
    let problem_str = from_utf8(&buffer).unwrap();
    log::debug!("{}", problem_str);

    let status = lp.solve();
    assert_eq!(status, lpsolve::SolveStatus::Optimal);
    log::debug!("status: {:?}", status);

    let mut w = vec![0.; n + 1];
    lp.get_solution_variables(&mut w);
    log::debug!("weights: {:?}", w);

    // adjust if w[i] < 0
    w.into_iter().map(|x| x.max(0.)).collect()
}

impl AllReduceAlgorithm for RatAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
        if self.check_cache((size, vcluster)) {
            return self.last_result.clone();
        }

        let tree_set = generate_rats(vcluster);
        // log::info!("tree_set: {:#?}", tree_set);
        let weights = linear_programming(vcluster, &tree_set, size);
        let mut flows = Vec::new();
        for (w, tree) in weights.into_iter().zip(tree_set) {
            // traverse all edges in the tree
            if w.abs() < 1e-10 {
                continue;
            }
            for c in &tree.nodes {
                if let Some(p) = c.parent {
                    let x = tree[p].rank;
                    let y = c.rank;
                    let ep1 = format!("host_{}", x);
                    let ep2 = format!("host_{}", y);
                    let [flow1, flow2] = [
                        Flow::new((size as f64 * w).floor() as usize, &ep1, &ep2, None),
                        Flow::new((size as f64 * w).floor() as usize, &ep2, &ep1, None),
                    ];
                    flows.push(flow1);
                    flows.push(flow2);
                }
            }
        }

        self.update_cache((size, vcluster), flows.clone());
        self.last_result = flows.clone();

        flows
    }
}
