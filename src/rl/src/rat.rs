use crate::RLAlgorithm;
use nethint::{
    bandwidth::Bandwidth,
    cluster::{helpers::*, Topology},
    Flow,
};
use utils::algo::*;
use rat_solver::{Solver, RatSolver, CachedSolver};

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

fn generate_embeddings<F>(
    vc: &dyn Topology,
    root_index: usize,
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

    tree_set
}

impl RatTree {
    fn run_rl_traffic(&mut self, root_index: usize, size: u64, vc: &dyn Topology) -> Vec<Flow> {
        let generate_func = || -> Vec<Tree> {
            vec![generate_embeddings(
                vc,
                root_index,
                vc.num_hosts(),
                construct_rat_offset,
            )]
            .concat()
        };

        // let mut rat_solver = RatSolver::new(generate_func);
        let mut rat_solver: CachedSolver<RatSolver<_>, _, _> = CachedSolver::new(generate_func);
        rat_solver.solve(&(root_index, size, vc))
    }
}


