use nethint::{
    bandwidth::Bandwidth,
    cluster::{helpers::*, Topology},
    Flow,
};
use utils::algo::*;
use std::marker::PhantomData;

/// A solver with initial state `State` takes an input `I`, and returns an output `O`
/// by calling `solve`.
pub trait Solver<'a, State> {
    type Input;
    type Output;
    fn new(init_state: State) -> Self;
    fn solve(&'a mut self, input: &Self::Input) -> Self::Output;
}

// allreduce's input
pub trait RatInput {
    fn size(&self) -> u64;
    fn vcluster(&self) -> &dyn Topology;
}

impl RatInput for (u64, &dyn Topology) {
    #[inline]
    fn size(&self) -> u64 {
        self.0
    }

    #[inline]
    fn vcluster(&self) -> &dyn Topology {
        self.1
    }
}

// rl's input
impl<T> RatInput for (u64, &dyn Topology, T) {
    #[inline]
    fn size(&self) -> u64 {
        self.0
    }

    #[inline]
    fn vcluster(&self) -> &dyn Topology {
        self.1
    }
}

impl<T1, T2> RatInput for (u64, &dyn Topology, T1, T2) {
    #[inline]
    fn size(&self) -> u64 {
        self.0
    }

    #[inline]
    fn vcluster(&self) -> &dyn Topology {
        self.1
    }
}

pub struct RatSolver<F, I> {
    generate_embeddings: F,
    _marker: PhantomData<I>,
}

impl<'a, F, I> Solver<'a, F> for RatSolver<F, I>
where
    F: Fn() -> Vec<Tree>,
    I: RatInput,
{
    type Input = I;
    type Output = Vec<Flow>;

    fn new(generate_embeddings: F) -> Self {
        RatSolver {
            generate_embeddings,
            _marker: PhantomData,
        }
    }

    fn solve(&mut self, input: &Self::Input) -> Self::Output {
        // 1. trees/embeddings generation
        let tree_set = (self.generate_embeddings)();

        // 2. run linear programming
        let weights = Self::linear_programming(input.vcluster(), &tree_set, input.size());

        // 3. construct flows
        let flows = Self::construct_flows(&tree_set, &weights, input.size());

        flows
    }
}

impl<F, I> RatSolver<F, I> {
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

        if tree_set.iter().all(|t| !t.is_directed()) {
            fn get_bw(vc: &dyn Topology, host_id: usize) -> Bandwidth {
                std::cmp::min(get_down_bw(vc, host_id), get_up_bw(vc, host_id))
            }
            fn get_rack_bw(vc: &dyn Topology, rack_id: usize) -> Bandwidth {
                std::cmp::min(get_rack_down_bw(vc, rack_id), get_rack_up_bw(vc, rack_id))
            }
            // in_deg and out_deg should be identical
            add_flow_constraint!(in_deg, get_bw);
            add_flow_constraint!(rack_in_deg, get_rack_bw);
        } else {
            add_flow_constraint!(in_deg, get_down_bw);
            add_flow_constraint!(out_deg, get_up_bw);
            add_flow_constraint!(rack_in_deg, get_rack_down_bw);
            add_flow_constraint!(rack_out_deg, get_rack_up_bw);
        }

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
