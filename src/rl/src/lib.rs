#![feature(option_unwrap_none)]
#![feature(map_into_keys_values)]
use serde::{Serialize, Deserialize};

pub mod argument;

pub mod app;

pub mod random_ring;

pub mod topology_aware;

pub mod rat;
pub mod config;

use nethint:: {
  cluster::Topology,
  Flow,
};

#[derive(Debug, Clone)]
pub struct JobSpec {
    pub num_workers: usize,
    pub buffer_size: usize,
    pub num_iterations: usize,
    pub root_index: usize,
}

impl JobSpec {
    pub fn new(num_workers: usize, buffer_size: usize, num_iterations: usize, root_index: usize) -> Self {
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
    /// Resilient Aggregation Tree
    RAT,
}

pub trait RLAlgorithm {
  fn run_rl_traffic(
      &mut self,
      root_index : usize,
      size: u64,
      vcluster: &dyn Topology,
  ) -> Vec<Flow>;
}