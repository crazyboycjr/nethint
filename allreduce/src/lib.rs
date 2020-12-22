pub mod argument;

pub mod app;

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