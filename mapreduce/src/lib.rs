use std::cell::RefCell;
use std::rc::Rc;

use nethint::cluster::{Cluster, Topology};
use rand::{self, rngs::StdRng, SeedableRng};

pub mod random;
pub use random::RandomReducerScheduler;

pub mod genetic;
pub use genetic::GeneticReducerScheduler;

pub mod greedy;
pub use greedy::GreedyReducerScheduler;

pub mod plot;

const RAND_SEED: u64 = 0;
thread_local! {
    pub static RNG: Rc<RefCell<StdRng>> = Rc::new(RefCell::new(StdRng::seed_from_u64(RAND_SEED)));
}

pub trait PlaceReducer {
    // from a high-level view, the inputs should be:
    // input1: a cluster of hosts with slots
    // input2: job/reduce task specification
    // input3: mapper placement
    // input4: shuffle flows
    fn place(
        &mut self,
        cluster: &Cluster,
        job_spec: &JobSpec,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
    ) -> Placement;
}

pub trait PlaceMapper {
    // from a high-level view, the inputs should be:
    // input1: a cluster of hosts with slots
    // input2: job specification
    fn place(&mut self, cluster: &Cluster, job_spec: &JobSpec) -> Placement;
}

#[derive(Debug, Clone, Copy)]
pub enum ReducerPlacementPolicy {
    Random,
    GeneticAlgorithm,
    HierarchicalGreedy,
}

#[derive(Debug, Clone)]
pub struct Placement(pub Vec<String>);

#[derive(Debug, Clone, Default)]
pub struct JobSpec {
    pub num_map: usize,
    pub num_reduce: usize,
    // cpu_slots: usize,
    // mem_slots: usize,
}

impl JobSpec {
    pub fn new(num_map: usize, num_reduce: usize) -> Self {
        JobSpec {
            num_map,
            num_reduce,
        }
    }
}

#[derive(Debug)]
pub struct Shuffle(pub Vec<Vec<usize>>);

pub fn get_rack_id(cluster: &Cluster, h: &str) -> usize {
    let host_ix = cluster.get_node_index(h);
    let tor_ix = cluster.get_target(cluster.get_uplink(host_ix));
    let rack_id: usize = cluster[tor_ix]
        .name
        .strip_prefix("tor_")
        .unwrap()
        .parse()
        .unwrap();
    rack_id
}
