use crate::RLAlgorithm;
use nethint::{
    bandwidth::Bandwidth,
    cluster::{helpers::*, Topology},
    Flow,
};
use rat_solver::{CachedSolver, RatSolver, Solver, TopologyWrapper};
use std::rc::Rc;
use utils::algo::*;

#[derive(Default)]
pub struct RatTree<S, I, O> {
    rat_solver: Option<CachedSolver<S, I, O>>,
}

impl<S, I, O> RatTree<S, I, O> {
    pub fn new() -> Self {
        RatTree { rat_solver: None }
    }
}

type RlInput = (u64, TopologyWrapper, usize, std::option::Option<Vec<usize>>);

impl RLAlgorithm for RatTree<RatSolver<RlInput>, RlInput, Vec<Flow>> {
    fn run_rl_traffic(
        &mut self,
        root_index: usize,
        mut group: Option<Vec<usize>>,
        size: u64,
        vc: Rc<dyn Topology>,
    ) -> Vec<Flow> {
        if group.is_some() {
            group.as_mut().unwrap().sort();
            group.as_mut().unwrap().insert(0, root_index);
        }

        let generate_func = || -> Vec<Tree> {
            vec![
                // generate_embeddings(vc, root_index, &group, vc.num_hosts(), construct_rat_offset),
                generate_embeddings2(
                    &*vc,
                    root_index,
                    &group,
                    vc.num_hosts(),
                    construct_rat_offset,
                ),
            ]
            .concat()
        };

        let embeddings = generate_func();

        let rat_solver = self
            .rat_solver
            .get_or_insert_with(|| CachedSolver::new(embeddings));

        // let mut rat_solver = RatSolver::new(generate_func);
        rat_solver.solve(&(size, TopologyWrapper::new(vc), root_index, group))
    }
}

#[inline]
fn get_fwd_rate2(vc: &dyn Topology, host_id: usize) -> Bandwidth {
    let tx = get_up_bw(vc, host_id);
    let rx = get_down_bw(vc, host_id);
    let fwd_rate = std::cmp::min(tx, rx);
    fwd_rate
}

fn construct_rat_offset(
    vc: &dyn Topology,
    groups: &Vec<Vec<usize>>,
    root_index: usize,
    offset: usize,
) -> Tree {
    // the starting position of each group
    let begin_pos: Vec<usize> = groups
        .iter()
        .scan(0, |state, g| {
            let ret = *state;
            *state += g.len();
            Some(ret)
        })
        .collect();

    let ranks: Vec<_> = groups.iter().flatten().copied().collect();
    assert!(offset < ranks.len());
    let root_rank = root_index;

    // let mut tree = Tree::default();
    let mut tree = Tree::new_directed();

    // always ensure the root as the first node
    let root = tree.push(Node::new(root_rank));

    let mut sub_roots = Vec::new();

    for (group, pos) in groups.iter().zip(begin_pos) {
        assert!(!group.is_empty());
        let first_rank = group[(offset + ranks.len() - pos) % group.len()];
        // log::debug!("offset: {}, first_rank: {}, group: {:?}", offset, first_rank, group);

        let sub_root = tree.push(Node::new(first_rank));
        sub_roots.push(sub_root);
        for i in 1..group.len() {
            let r = group[(i + offset + ranks.len() - pos) % group.len()];
            assert_ne!(r, first_rank);
            assert_ne!(r, root_rank);
            let tn = tree.push(Node::new(r));
            tree.connect(sub_root, tn);
        }
    }

    // chain all first ranks from highest forwarding rate to lowest forwarding rate
    if !sub_roots.is_empty() {
        let mut sub_root0 = sub_roots[0];
        sub_roots.remove(0);
        tree.connect(root, sub_root0);

        sub_roots.sort_by_key(|&n| {
            let r = tree[n].rank();
            let fwdrate = get_fwd_rate2(vc, r);
            std::cmp::Reverse(fwdrate)
        });

        for sr in sub_roots {
            tree.connect(sub_root0, sr);
            sub_root0 = sr;
        }
    }

    tree
}

fn prune_tree(x: TreeIdx, tree: &mut Tree, group: &[usize]) -> bool {
    let mut children = Vec::new();
    for &e in tree.neighbor_edges(x) {
        let y = tree[e].to();
        children.push(y);
    }

    let mut can_cut = !group.contains(&tree[x].rank());
    for y in children {
        let can_cut_br = prune_tree(y, tree, group);
        if can_cut_br {
            tree.cut(x, y);
        }
        can_cut &= can_cut_br;
    }

    can_cut
}

fn prune(tree_set: &mut Vec<Tree>, group: &[usize]) {
    // prune all leaves that are not in the broadcast group
    for tree in tree_set {
        prune_tree(tree.root(), tree, group);
    }
}

fn generate_embeddings2<F>(
    vc: &dyn Topology,
    root_index: usize,
    group: &Option<Vec<usize>>,
    num_trees_bound: usize,
    construct_embedding_offset: F,
) -> Vec<Tree>
where
    F: Fn(&dyn Topology, &Vec<Vec<usize>>, usize, usize) -> Tree,
{
    let n = vc.num_hosts();

    let groups = {
        // each group consists of the nodes within a rack
        let mut groups = if group.is_some() {
            group_by_key(group.clone().unwrap().into_iter(), |&i| get_rack_ix(vc, i))
        } else {
            group_by_key(0..n, |&i| get_rack_ix(vc, i))
        };
        // make the group contains the root_index the first group
        let root_group_pos = groups.iter().position(|g| g.contains(&root_index)).unwrap();
        groups.swap(0, root_group_pos);
        // remove the root node from the first group for convenience later
        groups[0].retain(|&x| x != root_index);
        // in case that the root group becomes empty
        if groups[0].is_empty() {
            groups.remove(0);
        }
        groups
    };

    let n = if group.is_some() {
        group.as_ref().unwrap().len() - 1
    } else {
        n - 1
    };
    let m = n.min(num_trees_bound);
    let mut base = 0;
    let mut tree_set = Vec::with_capacity(m);
    for i in 0..m {
        let off = if m < n {
            (base + i / groups.len()) % n
        } else {
            i
        };
        let tree_i = construct_embedding_offset(vc, &groups, root_index, off);
        tree_set.push(tree_i);
        base += groups[i % groups.len()].len();
    }

    tree_set
}

#[allow(unused)]
fn generate_embeddings<F>(
    vc: &dyn Topology,
    root_index: usize,
    group: &Option<Vec<usize>>,
    num_trees_bound: usize,
    construct_embedding_offset: F,
) -> Vec<Tree>
where
    F: Fn(&dyn Topology, &Vec<Vec<usize>>, usize, usize) -> Tree,
{
    let n = vc.num_hosts();

    let groups = {
        // each group consists of the nodes within a rack
        let mut groups = group_by_key(0..n, |&i| get_rack_ix(vc, i));
        // make the group contains the root_index the first group
        let root_group_pos = groups.iter().position(|g| g.contains(&root_index)).unwrap();
        groups.swap(0, root_group_pos);
        // remove the root node from the first group for convenience later
        groups[0].retain(|&x| x != root_index);
        // in case that the root group becomes empty
        if groups[0].is_empty() {
            groups.remove(0);
        }
        groups
    };

    let n = n - 1;
    let m = n.min(num_trees_bound);
    let mut base = 0;
    let mut tree_set = Vec::with_capacity(m);
    for i in 0..m {
        let off = if m < n {
            (base + i / groups.len()) % n
        } else {
            i
        };
        let tree_i = construct_embedding_offset(vc, &groups, root_index, off);
        tree_set.push(tree_i);
        base += groups[i % groups.len()].len();
    }

    if group.is_some() {
        prune(&mut tree_set, group.as_ref().unwrap())
    }

    tree_set
}
