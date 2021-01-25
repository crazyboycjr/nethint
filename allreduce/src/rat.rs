use lpsolve;
use std::{collections::BTreeMap, str::from_utf8};

use crate::AllReduceAlgorithm;
use nethint::{cluster::Topology, Flow};

#[derive(Debug, Default)]
pub struct RatAllReduce {}

impl RatAllReduce {
    pub fn new() -> Self {
        Default::default()
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

    let root = tree.push(Node::new(ranks[offset]));
    for group in groups {
        assert!(!group.is_empty());
        let first_rank = group[offset % group.len()];
        let sub_root = if tree[root].rank != first_rank {
            let sub_root = tree.push(Node::new(first_rank));
            tree.connect(root, sub_root);
            sub_root
        } else {
            root
        };
        for i in 1..group.len() {
            let r = group[(offset + i) % group.len()];
            let tn = tree.push(Node::new(r));
            tree.connect(sub_root, tn);
        }
    }

    tree
}

fn generate_rats(vcluster: &dyn Topology) -> Vec<Tree> {
    let n = vcluster.num_hosts();

    let groups = group_by_key(0..n, |i| {
        let host_name = format!("host_{}", i);
        let host_ix = vcluster.get_node_index(&host_name);
        let rack_ix = vcluster.get_target(vcluster.get_uplink(host_ix));
        rack_ix
    });

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

    let bw: Vec<_> = (0..n)
        .map(|i| {
            let host_name = format!("host_{}", i);
            let host_ix = vcluster.get_node_index(&host_name);
            vcluster[vcluster.get_uplink(host_ix)].bandwidth.val() as f64
        })
        .collect();

    let mut deg = vec![vec![0.; tree_set.len() + 1]; n];
    for (k, tree) in tree_set.iter().enumerate() {
        for node in &tree.nodes {
            let r = node.rank;
            deg[r][k + 1] = (node.children.len() + node.parent.iter().len()) as f64;
        }
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

        flows
    }
}
