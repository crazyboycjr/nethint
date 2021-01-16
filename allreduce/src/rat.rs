use lpsolve;
use std::{collections::BTreeMap, str::from_utf8};

use crate::AllReduceAlgorithm;
use nethint::{
    cluster::{NodeIx, Topology},
    Flow,
};

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

fn linear_programming(vcluster: &dyn Topology, tree_set: &[Tree]) -> Vec<f64> {
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
        constraint.push(-1.0 * bw[i]);
        lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Le);
    }

    // w1 + w2 + ... wn = 1.0
    let mut constraint = vec![1.; n + 1];
    constraint.push(0.);
    lp.add_constraint(&constraint, 1., lpsolve::ConstraintType::Eq);

    // \vec{w} >= 0
    for _ in 0..n {
        let mut constraint = vec![0.; n + 1];
        constraint.push(1.);
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
    w
}

impl AllReduceAlgorithm for RatAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
        let tree_set = generate_rats(vcluster);
        // log::info!("tree_set: {:#?}", tree_set);
        let weights = linear_programming(vcluster, &tree_set);
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
