use std::collections::BTreeMap;

use crate::AllReduceAlgorithm;
use nethint::{Flow, cluster::{NodeIx, Topology}};

#[derive(Debug, Default)]
pub struct RatAllReduce { }

impl RatAllReduce {
    pub fn new(seed: u64) -> Self {
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
        ix
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

fn construct_tree_0(vcluster: &dyn Topology) -> Tree {
    let n = vcluster.num_hosts();
    let mut tree = Tree::default();
    let mut groups: BTreeMap<NodeIx, Vec<usize>> = Default::default();
    for i in 0..n {
        let host_name = format!("host_{}", i);
        let host_ix = vcluster.get_node_index(&host_name);
        let rack_ix = vcluster.get_target(vcluster.get_uplink(host_ix));
        groups.entry(rack_ix).or_default().push(i);
    }
    for (rack_ix, group) in groups {
        assert!(!group.is_empty());
        let sub_root = tree.nodes.len();
        tree.nodes.push(Node::new(*group.first().unwrap()));
        for &r in group.iter().skip(1) {
            tree.nodes.push(Node::new(r));
            tree.
        }
    }
    // let groups: Vec<Vec<_>> = groups.into_values().collect();
    
    tree
}

fn generate_magic_tree(vcluster: &dyn Topology) -> Vec<Tree> {
    let tree0 = construct_tree_0(vcluster);
    Vec::new()
}

impl AllReduceAlgorithm for RatAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
        let tree_set = generate_magic_tree(vcluster);
        let mut flows = Vec::new();
        flows
    }
}