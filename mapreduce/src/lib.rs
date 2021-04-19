#![feature(str_split_once)]

use std::cell::RefCell;
use std::rc::Rc;

use nethint::cluster::Topology;
use rand::{self, rngs::StdRng, SeedableRng};

use serde::{Serialize, Deserialize};

pub mod random;
pub use random::RandomReducerScheduler;

pub mod genetic;
pub use genetic::GeneticReducerScheduler;

pub mod greedy;
pub use greedy::GreedyReducerScheduler;
pub use greedy::ImprovedGreedyReducerScheduler;
pub use greedy::GreedyReducerLevel1Scheduler;
pub use greedy::GreedyReducerSchedulerPaper;

pub mod plot;

pub mod trace;

pub mod argument;

pub mod mapper;

pub mod app;

pub mod plink;

pub mod inspect;

pub mod config;

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
    // input5: whether to collocate mapper and reducer
    fn place(
        &mut self,
        vcluster: &dyn Topology,
        job_spec: &JobSpec,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
        collocate: bool,
    ) -> Placement;
}

pub trait PlaceMapper {
    // from a high-level view, the inputs should be:
    // input1: a cluster of hosts with slots
    // input2: job specification
    fn place(&mut self, vcluster: &dyn Topology, job_spec: &JobSpec) -> Placement;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReducerPlacementPolicy {
    Random,
    GeneticAlgorithm,
    HierarchicalGreedy,
    HierarchicalGreedyLevel1,
    HierarchicalGreedyPaper,
}

#[derive(Debug, Clone)]
pub struct Placement(pub Vec<String>);

#[derive(Debug, Clone)]
pub struct JobSpec {
    pub num_map: usize,
    pub num_reduce: usize,
    pub shuffle_pat: ShufflePattern,
    // cpu_slots: usize,
    // mem_slots: usize,
}

impl JobSpec {
    pub fn new(num_map: usize, num_reduce: usize, pat: ShufflePattern) -> Self {
        JobSpec {
            num_map,
            num_reduce,
            shuffle_pat: pat,
        }
    }
}

impl std::fmt::Display for JobSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "m{}_r{}", self.num_map, self.num_reduce)
    }
}

#[derive(Debug)]
pub struct Shuffle(pub Vec<Vec<usize>>);

pub fn get_rack_id(cluster: &dyn Topology, h: &str) -> usize {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "args")]
pub enum ShufflePattern {
    Uniform(u64),
    Zipf(u64, f64),
    #[serde(skip)]
    FromTrace(Box<trace::Record>),
}

#[derive(Debug)]
pub struct ParseDistributionError;

impl std::fmt::Display for ParseDistributionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::str::FromStr for ShufflePattern {
    type Err = ParseDistributionError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // uniform_{}
        // zipf_{}_{}

        s.split_once("_")
            .and_then(|(name, args)| match name {
                "uniform" => args.parse::<u64>().ok().map(ShufflePattern::Uniform),
                "zipf" => args.split_once("_").and_then(|(n, s)| {
                    n.parse::<u64>()
                        .ok()
                        .zip(s.parse::<f64>().ok())
                        .map(|(n, s)| ShufflePattern::Zipf(n, s))
                }),
                _ => None,
            })
            .ok_or(ParseDistributionError)
    }
}
