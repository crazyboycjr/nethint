use serde::{Deserialize, Serialize};

pub mod argument;

pub mod app;

pub mod contraction;
pub mod random_ring;
pub mod rat;
pub mod topology_aware;

pub mod config;

use nethint::{cluster::Topology, Flow};

#[derive(Debug, Clone)]
pub struct JobSpec {
    pub num_workers: usize,
    pub buffer_size: usize,
    pub num_iterations: usize,
    pub root_index: usize,
}

impl JobSpec {
    pub fn new(
        num_workers: usize,
        buffer_size: usize,
        num_iterations: usize,
        root_index: usize,
    ) -> Self {
        JobSpec {
            num_workers,
            buffer_size,
            num_iterations,
            root_index,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RLPolicy {
    Random,
    TopologyAware,
    Contraction,
    /// Resilient Aggregation Tree
    RAT,
}

pub trait RLAlgorithm {
    fn run_rl_traffic(
        &mut self,
        root_index: usize,
        group: Option<Vec<usize>>, // worker group
        size: u64,
        vcluster: &dyn Topology,
    ) -> Vec<Flow>;
}
