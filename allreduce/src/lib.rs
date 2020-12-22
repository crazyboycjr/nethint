pub mod argument;

pub mod app;

pub mod random_ring;

pub mod topology_aware;

use nethint:: {
  cluster::Topology,
  Flow,
};

#[derive(Debug, Clone)]
pub struct JobSpec {
    pub num_workers: usize,
}

impl JobSpec {
    pub fn new(num_workers: usize) -> Self {
        JobSpec {
          num_workers,
        }
    }
}

#[derive(Debug, Clone)]
pub enum AllReducePolicy {
    Random,
    TopologyAware,
}

pub trait AllReduceAlgorithm {
  fn allreduce(
      &mut self,
      size: u64,
      vcluster: &dyn Topology,
  ) -> Vec<Flow>;
}