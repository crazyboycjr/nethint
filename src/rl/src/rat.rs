use crate::RLAlgorithm;
use nethint::{
    bandwidth::Bandwidth,
    cluster::{helpers::*, Topology},
    Flow,
};
use utils::algo::*;

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

// fn linear_programming(vcluster: &dyn Topology, tree_set: &[Tree], size: u64) -> Vec<f64> {
//     let mut lp = lpsolve::Problem::new(0, tree_set.len() as i32 + 1).unwrap();
// 
//     // set verbosity
//     unsafe {
//         lpsolve_sys::set_verbose(lp.to_lprec(), lpsolve::Verbosity::Critical as i32);
//     }
// 
//     // host level constraints
//     let n = vcluster.num_hosts();
// 
//     // in/out_deg[i][j] means the in/out-degree of node i in the jth tree.
//     let mut in_deg = vec![vec![0.; tree_set.len() + 1]; n];
//     let mut out_deg = vec![vec![0.; tree_set.len() + 1]; n];
//     for (k, tree) in tree_set.iter().enumerate() {
//         for e in tree.all_edges() {
//             let p = tree[e].from();
//             let n = tree[e].to();
//             in_deg[tree[n].rank()][k + 1] += 1.0;
//             out_deg[tree[p].rank()][k + 1] += 1.0;
//         }
//     }
// 
//     // rack level constraints
//     let r = vcluster.num_switches() - 1; // total number of ToR switches
//     let mut rack_in_deg = vec![vec![0.; tree_set.len() + 1]; r];
//     let mut rack_out_deg = vec![vec![0.; tree_set.len() + 1]; r];
// 
//     for (k, tree) in tree_set.iter().enumerate() {
//         for e in tree.all_edges() {
//             // p -> p_rack_id -> cloud -> n_rack_id -> n
//             let p = tree[e].from();
//             let n = tree[e].to();
//             let n_rack_id = get_rack_id(vcluster, get_rack_ix(vcluster, tree[n].rank()));
//             let p_rack_id = get_rack_id(vcluster, get_rack_ix(vcluster, tree[p].rank()));
//             if n_rack_id != p_rack_id {
//                 // cross rack
//                 rack_in_deg[n_rack_id][k + 1] += 1.0;
//                 rack_out_deg[p_rack_id][k + 1] += 1.0;
//             }
//         }
//     }
// 
//     // minimize y
//     let mut obj_func = vec![0.; tree_set.len() + 1];
//     obj_func.push(1.);
//     lp.set_objective_function(&obj_func);
// 
//     // D is a degree matrix. D[i][k] is the degree of node i in kth aggregation tree.
//     // Bw is a diagonal bandwidth matrix. Bw[i][i] = bw[i];
//     // Bw^{-1} \cdot D \cdot \vec{w} <= \vec{y}
//     macro_rules! add_flow_constraint {
//         ($mdeg:expr, $get_bw_func:expr) => {
//             for (i, deg) in $mdeg.iter().enumerate() {
//                 let mut constraint = deg.clone();
//                 let bw = $get_bw_func(vcluster, i).val() as f64;
//                 // be careful about the precision issues of the underlying LP solver
//                 constraint.push(-1.0 * bw / size as f64);
//                 lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Le);
//             }
//         };
//     }
// 
//     add_flow_constraint!(in_deg, get_down_bw);
//     add_flow_constraint!(out_deg, get_up_bw);
//     add_flow_constraint!(rack_in_deg, get_rack_down_bw);
//     add_flow_constraint!(rack_out_deg, get_rack_up_bw);
// 
//     // w1 + w2 + ... wn = 1.0
//     let mut constraint = vec![1.; tree_set.len() + 1];
//     constraint.push(0.);
//     lp.add_constraint(&constraint, 1., lpsolve::ConstraintType::Eq);
// 
//     // \vec{w} >= 0
//     for i in 0..tree_set.len() {
//         let mut constraint = vec![0.; tree_set.len() + 2];
//         constraint[i + 1] = 1.;
//         lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Ge);
//     }
// 
//     let mut buffer = Vec::new();
//     lp.write_lp(&mut buffer);
//     let problem_str = std::str::from_utf8(&buffer).unwrap();
//     log::debug!("{}", problem_str);
// 
//     let status = lp.solve();
//     assert_eq!(status, lpsolve::SolveStatus::Optimal);
//     log::debug!("status: {:?}", status);
// 
//     let mut w = vec![0.; tree_set.len() + 1];
//     lp.get_solution_variables(&mut w);
//     log::debug!("weights: {:?}", w);
// 
//     // adjust if w[i] < 0
//     w.into_iter().map(|x| x.max(0.)).collect()
// }
// 
// fn construct_flows(tree_set: &[Tree], weights: &[f64], size: u64) -> Vec<Flow> {
//     let mut flows = Vec::new();
// 
//     for (w, tree) in weights.into_iter().zip(tree_set) {
//         // traverse all edges in the tree
//         if w.abs() < 1e-10 {
//             continue;
//         }
// 
//         for e in tree.all_edges() {
//             let p = tree[e].from();
//             let n = tree[e].to();
//             let sender = format!("host_{}", tree[p].rank());
//             let receiver = format!("host_{}", tree[n].rank());
//             let flow = Flow::new((size as f64 * w).floor() as usize, &sender, &receiver, None);
//             flows.push(flow);
//         }
//     }
// 
//     flows
// }

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

        let mut rat_solver: CachedSolver<RatSolver<_>, _, _> = CachedSolver::new(generate_func);
        // let mut rat_solver = RatSolver::new(generate_func);
        rat_solver.solve(&(root_index, size, vc))

        // let tree_set = vec![
        //     generate_embeddings(vc, root_index, vc.num_hosts(), construct_rat_offset),
        //     // generate_embeddings(vc, self.num_trees, construct_chain_offset),
        // ]
        // .concat();
        // // log::info!("tree_set: {:?}", tree_set);
        // let weights = linear_programming(vc, &tree_set, size);

        // let flows = construct_flows(&tree_set, &weights, size);
        // // log::info!("root_index: {}, flows: {:?}", root_index, flows);
        // flows
    }
}

pub trait Cached<I, O> {
    fn check_cache(&self, input: &I) -> bool;

    fn update_cache(&mut self, input: &I, output: O);

    fn get(&self, input: &I) -> O;
}

#[derive(Debug)]
pub struct SingleEntryCache<I, O> {
    last_input: Option<I>,
    last_output: Option<O>,
}

impl<I, O> SingleEntryCache<I, O> {
    pub fn new() -> Self {
        Self {
            last_input: None,
            last_output: None,
        }
    }
}

impl<I: Clone + PartialEq, O: Clone> Cached<I, O> for SingleEntryCache<I, O> {
    #[inline]
    fn check_cache(&self, input: &I) -> bool {
        self.last_input.as_ref().map_or(false, |i| i == input)
    }

    #[inline]
    fn update_cache(&mut self, input: &I, output: O) {
        self.last_input.replace(input.clone());
        self.last_output.replace(output.clone());
    }

    #[inline]
    fn get(&self, _input: &I) -> O {
        self.last_output.as_ref().unwrap().clone()
    }
}

pub struct CachedSolver<S, I, O> {
    solver: S,
    cache: SingleEntryCache<I, O>,
}

impl<'a, State, S, I, O> Solver<'a, State> for CachedSolver<S, I, O>
where
    S: Solver<'a, State, Input = I, Output = O>,
    I: Clone + PartialEq,
    O: Clone,
{
    type Input = I;
    type Output = O;

    fn new(init_state: State) -> Self {
        CachedSolver {
            solver: S::new(init_state),
            cache: SingleEntryCache::new(),
        }
    }

    fn solve(&'a mut self, input: &Self::Input) -> Self::Output {
        if self.cache.check_cache(input) {
            return self.cache.get(input);
        }
        let output = self.solver.solve(input);
        self.cache.update_cache(input, output.clone());
        output
    }
}

/// A solver with initial state `State` takes an input `I`, and returns an output `O`
/// by calling `solve`.
pub trait Solver<'a, State> {
    type Input;
    type Output;
    fn new(init_state: State) -> Self;
    fn solve(&'a mut self, input: &Self::Input) -> Self::Output;
}

pub struct RatSolver<F> {
    generate_embeddings: F,
}

impl<'a, F> Solver<'a, F> for RatSolver<F>
where
    F: Fn() -> Vec<Tree>,
{
    type Input = (usize, u64, &'a dyn Topology);
    type Output = Vec<Flow>;

    fn new(generate_embeddings: F) -> Self {
        RatSolver {
            generate_embeddings,
        }
    }

    fn solve(&mut self, &(_root_index, size, vcluster): &Self::Input) -> Self::Output {
        // 1. tree/embeddings generation
        let tree_set = (self.generate_embeddings)();

        // 2. run linear programming
        let weights = Self::linear_programming(vcluster, &tree_set, size);

        // 3. construct flows
        let flows = Self::construct_flows(&tree_set, &weights, size);

        flows
    }
}

impl<F> RatSolver<F> {
    fn linear_programming(vcluster: &dyn Topology, tree_set: &[Tree], size: u64) -> Vec<f64> {
        let mut lp = lpsolve::Problem::new(0, tree_set.len() as i32 + 1).unwrap();

        // set verbosity
        unsafe {
            lpsolve_sys::set_verbose(lp.to_lprec(), lpsolve::Verbosity::Critical as i32);
        }

        // host level constraints
        let n = vcluster.num_hosts();

        // in/out_deg[i][j] means the in/out-degree of node i in the jth tree.
        let mut in_deg = vec![vec![0.; tree_set.len() + 1]; n];
        let mut out_deg = vec![vec![0.; tree_set.len() + 1]; n];
        for (k, tree) in tree_set.iter().enumerate() {
            for e in tree.all_edges() {
                let p = tree[e].from();
                let n = tree[e].to();
                in_deg[tree[n].rank()][k + 1] += 1.0;
                out_deg[tree[p].rank()][k + 1] += 1.0;
            }
        }

        // rack level constraints
        let r = vcluster.num_switches() - 1; // total number of ToR switches
        let mut rack_in_deg = vec![vec![0.; tree_set.len() + 1]; r];
        let mut rack_out_deg = vec![vec![0.; tree_set.len() + 1]; r];

        for (k, tree) in tree_set.iter().enumerate() {
            for e in tree.all_edges() {
                // p -> p_rack_id -> cloud -> n_rack_id -> n
                let p = tree[e].from();
                let n = tree[e].to();
                let n_rack_id = get_rack_id(vcluster, get_rack_ix(vcluster, tree[n].rank()));
                let p_rack_id = get_rack_id(vcluster, get_rack_ix(vcluster, tree[p].rank()));
                if n_rack_id != p_rack_id {
                    // cross rack
                    rack_in_deg[n_rack_id][k + 1] += 1.0;
                    rack_out_deg[p_rack_id][k + 1] += 1.0;
                }
            }
        }

        // minimize y
        let mut obj_func = vec![0.; tree_set.len() + 1];
        obj_func.push(1.);
        lp.set_objective_function(&obj_func);

        // D is a degree matrix. D[i][k] is the degree of node i in kth aggregation tree.
        // Bw is a diagonal bandwidth matrix. Bw[i][i] = bw[i];
        // Bw^{-1} \cdot D \cdot \vec{w} <= \vec{y}
        macro_rules! add_flow_constraint {
            ($mdeg:expr, $get_bw_func:expr) => {
                for (i, deg) in $mdeg.iter().enumerate() {
                    let mut constraint = deg.clone();
                    let bw = $get_bw_func(vcluster, i).val() as f64;
                    // be careful about the precision issues of the underlying LP solver
                    constraint.push(-1.0 * bw / size as f64);
                    lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Le);
                }
            };
        }

        add_flow_constraint!(in_deg, get_down_bw);
        add_flow_constraint!(out_deg, get_up_bw);
        add_flow_constraint!(rack_in_deg, get_rack_down_bw);
        add_flow_constraint!(rack_out_deg, get_rack_up_bw);

        // w1 + w2 + ... wn = 1.0
        let mut constraint = vec![1.; tree_set.len() + 1];
        constraint.push(0.);
        lp.add_constraint(&constraint, 1., lpsolve::ConstraintType::Eq);

        // \vec{w} >= 0
        for i in 0..tree_set.len() {
            let mut constraint = vec![0.; tree_set.len() + 2];
            constraint[i + 1] = 1.;
            lp.add_constraint(&constraint, 0., lpsolve::ConstraintType::Ge);
        }

        let mut buffer = Vec::new();
        lp.write_lp(&mut buffer);
        let problem_str = std::str::from_utf8(&buffer).unwrap();
        log::debug!("{}", problem_str);

        let status = lp.solve();
        assert_eq!(status, lpsolve::SolveStatus::Optimal);
        log::debug!("status: {:?}", status);

        let mut w = vec![0.; tree_set.len() + 1];
        lp.get_solution_variables(&mut w);
        log::debug!("weights: {:?}", w);

        // adjust if w[i] < 0
        w.into_iter().map(|x| x.max(0.)).collect()
    }

    fn construct_flows(tree_set: &[Tree], weights: &[f64], size: u64) -> Vec<Flow> {
        let mut flows = Vec::new();

        for (w, tree) in weights.into_iter().zip(tree_set) {
            // traverse all edges in the tree
            if w.abs() < 1e-10 {
                continue;
            }

            for e in tree.all_edges() {
                let p = tree[e].from();
                let n = tree[e].to();
                let sender = format!("host_{}", tree[p].rank());
                let receiver = format!("host_{}", tree[n].rank());
                let flow = Flow::new((size as f64 * w).floor() as usize, &sender, &receiver, None);
                flows.push(flow);
            }
        }

        flows
    }
}

// fn run_rl_traffic(root_index: usize, size: u64, vc: &dyn Topology) -> Vec<Flow> {
//     let generate_func = || -> Vec<Tree> {
//         vec![generate_embeddings(
//             vc,
//             root_index,
//             vc.num_hosts(),
//             construct_rat_offset,
//         )]
//         .concat()
//     };
// 
//     let mut rat_solver: CachedSolver<RatSolver<_>, _, _> = CachedSolver::new(generate_func);
//     rat_solver.solve(&(root_index, size, vc))
// }
