use std::collections::HashMap;

use crate::AllReduceAlgorithm;
use nethint::{
    bandwidth::BandwidthTrait,
    cluster::{helpers::*, Link, LinkIx, Topology},
    Flow,
};

use rat_solver::{CachedSolver, RatSolver, Solver};
use utils::algo::*;

#[derive(Debug, Default)]
pub struct RatAllReduce {
    // cache the result, if nethint hasn't been changed, no need to run LP again
    num_trees: usize,
    last_size: Option<u64>,
    last_hint: Option<HashMap<LinkIx, Link>>,
    last_result: Vec<Flow>,
}

impl RatAllReduce {
    pub fn new(num_trees: usize) -> Self {
        RatAllReduce {
            num_trees,
            ..Default::default()
        }
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

#[allow(dead_code)]
fn construct_rat_full_set(groups: &mut Vec<Vec<usize>>, mut offset: usize) -> Tree {
    let mut m = groups.iter().map(|x| x.len()).product::<usize>();
    // decide root rack
    let root_rack = offset / m;
    offset %= m;
    let swap_groups = |groups: &mut Vec<Vec<usize>>| {
        let tmp = groups[0].clone();
        groups[0] = groups[root_rack].clone();
        groups[root_rack] = tmp.clone();
    };
    swap_groups(groups);
    // decide root rank
    // let mut tree = Tree::default();
    let mut tree = Tree::new_undirected();
    let mut root_rank = None;
    let mut root = None;

    // decide the subroots by a modified cantor expansion
    for group in groups.iter() {
        assert!(!group.is_empty());
        m /= group.len();
        let first_rank = group[offset / m];

        let sub_root = if root_rank.is_none() {
            root_rank = Some(first_rank);
            root = Some(tree.push(Node::new(first_rank)));
            root.unwrap()
        } else {
            let sub_root = tree.push(Node::new(first_rank));
            tree.connect(root.unwrap(), sub_root);
            sub_root
        };

        for i in 0..group.len() {
            if i != offset / m {
                let r = group[i];
                assert_ne!(r, root_rank.unwrap());
                let tn = tree.push(Node::new(r));
                tree.connect(sub_root, tn);
            }
        }

        offset %= m;
    }

    swap_groups(groups);
    tree
}

#[allow(dead_code)]
fn construct_chain_offset(groups: &Vec<Vec<usize>>, offset: usize) -> Tree {
    let ranks: Vec<_> = groups.iter().flatten().copied().collect();
    assert!(offset < ranks.len());
    let root_rank = ranks[offset];

    // let mut tree = Tree::default();
    let mut tree = Tree::new_undirected();
    let root = tree.push(Node::new(root_rank));
    let mut last_node_ix = root;
    for i in 1..ranks.len() {
        let r = ranks[(i + offset) % ranks.len()];
        assert_ne!(r, root_rank);
        let tn = tree.push(Node::new(r));
        tree.connect(last_node_ix, tn);
        last_node_ix = tn;
    }

    tree
}

#[allow(dead_code)]
fn construct_ps_offset(groups: &Vec<Vec<usize>>, offset: usize) -> Tree {
    let ranks: Vec<_> = groups.iter().flatten().copied().collect();
    assert!(offset < ranks.len());
    let root_rank = ranks[offset];

    // let mut tree = Tree::default();
    let mut tree = Tree::new_undirected();
    let root = tree.push(Node::new(root_rank));
    for r in ranks {
        if r != root_rank {
            let tn = tree.push(Node::new(r));
            tree.connect(root, tn);
        }
    }

    tree
}

fn construct_rat_offset(groups: &Vec<Vec<usize>>, offset: usize) -> Tree {
    let ranks: Vec<_> = groups.iter().flatten().copied().collect();
    assert!(offset < ranks.len());
    let root_rank = ranks[offset];

    // let mut tree = Tree::default();
    let mut tree = Tree::new_undirected();

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

#[allow(dead_code)]
fn generate_rats_full_set(vcluster: &dyn Topology, _num_trees_bound: usize) -> Vec<Tree> {
    let n = vcluster.num_hosts();
    let mut groups = group_by_key(0..n, |&i| get_rack_ix(vcluster, i));
    assert_eq!(groups.len(), 2);
    // to construct full set of rats, the number is: nracks * \PI groups[i].len()
    // 2 * groups[0].len() * groups[1].len()
    let m = groups.len() * groups.iter().map(|x| x.len()).product::<usize>();
    let mut tree_set = Vec::with_capacity(m);
    for i in 0..m {
        let tree_i = construct_rat_full_set(&mut groups, i);
        tree_set.push(tree_i);
    }

    tree_set
}

fn generate_embeddings<F>(
    vcluster: &dyn Topology,
    num_trees_bound: usize,
    construct_embedding_offset: F,
) -> Vec<Tree>
where
    F: Fn(&Vec<Vec<usize>>, usize) -> Tree,
{
    let n = vcluster.num_hosts();

    let groups = group_by_key(0..n, |&i| get_rack_ix(vcluster, i));

    let m = n.min(num_trees_bound);
    let mut base = 0;
    let mut tree_set = Vec::with_capacity(m);
    for i in 0..m {
        let off = if m < n {
            (base + i / groups.len()) % n
        } else {
            i
        };
        let tree_i = construct_embedding_offset(&groups, off);
        tree_set.push(tree_i);
        base += groups[i % groups.len()].len();
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
        let problem_str = std::str::from_utf8(&buffer).unwrap();
        println!("{}", problem_str);

        let status = lp.solve();
        assert_eq!(status, lpsolve::SolveStatus::Optimal);
        println!("status: {:?}", status);

        let mut w = vec![0.; n + 1];
        lp.get_solution_variables(&mut w);
        println!("weights: {:?}", w);
    }
}

impl AllReduceAlgorithm for RatAllReduce {
    fn allreduce(&mut self, size: u64, vcluster: &dyn Topology) -> Vec<Flow> {
        let generate_func = || -> Vec<Tree> {
            vec![
                generate_embeddings(vcluster, self.num_trees, construct_rat_offset),
                generate_embeddings(vcluster, self.num_trees, construct_ps_offset),
                // generate_embeddings(vcluster, self.num_trees, construct_chain_offset),
            ]
            .concat()
        };

        let mut rat_solver: CachedSolver<RatSolver<_, _>, _, _> = CachedSolver::new(generate_func);
        // NOTE(cjr): use non-cached solver when testing controller overhead
        // let mut rat_solver = RatSolver::new(generate_func);
        rat_solver.solve(&(size, vcluster))
    }
}
