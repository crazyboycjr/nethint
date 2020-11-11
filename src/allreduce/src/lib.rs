use serde::{Deserialize, Serialize};

pub mod argument;

pub mod app;

pub mod random_ring;

pub mod topology_aware;

pub mod rat;

pub mod config;

use nethint::{cluster::Topology, Flow};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct JobSpec {
    pub num_workers: usize,
    pub buffer_size: usize,
    pub num_iterations: usize,
}

impl JobSpec {
    pub fn new(num_workers: usize, buffer_size: usize, num_iterations: usize) -> Self {
        JobSpec {
            num_workers,
            buffer_size,
            num_iterations,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AllReducePolicy {
    Random,
    TopologyAware,
    /// Resilient Aggregation Tree
    RAT,
}

pub trait AllReduceAlgorithm {
    fn allreduce(&mut self, size: u64, vcluster: Rc<dyn Topology>) -> Vec<Flow>;
}
