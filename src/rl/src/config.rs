use serde::{Serialize, Deserialize};
use crate::RLPolicy;
use nethint::brain::{self, BrainSetting};
use nethint::simulator::SimulatorSetting;
use rand::{Rng, rngs::StdRng};
use rand_distr::{Distribution, Poisson};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeConfig {
    pub enable: bool,
    #[serde(default)]
    pub round_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// allreduce policy
    pub policy: RLPolicy,
    /// whether to use plink
    pub probe: ProbeConfig,
    /// Nethint level.
    pub nethint_level: usize,
    /// Whether to auto tune after a certain iterations
    #[serde(default)]
    pub auto_tune: Option<usize>,
    /// Number of broadcast trees
    #[serde(default)]
    pub num_trees: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentConfig {
    /// Number of jobs
    pub ncases: usize,

    /// Number of workers
    pub job_size_distribution: Vec<(usize, usize)>,

    /// Buffer size of all jobs, in bytes
    pub buffer_size: usize,

    /// Number of iterations for all jobs
    pub num_iterations: usize,

    /// Lambda of the poisson arrival
    pub poisson_lambda: f64,

    /// akin to AWS Placement Group
    pub placement_strategy: brain::PlacementStrategy,

    /// whether to allow delay scheduling, default to false, in simulation, it must be false
    pub allow_delay: Option<bool>,

    /// global seed
    pub seed: u64,

    /// Number of repeats for each batch of experiments
    pub batch_repeat: usize,

    #[serde(rename = "batch")]
    pub batches: Vec<BatchConfig>,

    /// Output path of the figure
    #[serde(default)]
    pub directory: Option<std::path::PathBuf>,

    /// Simulator settings
    pub simulator: SimulatorSetting,

    /// Brain settings
    pub brain: BrainSetting,
}

pub fn read_config<T: serde::de::DeserializeOwned, P: AsRef<std::path::Path>>(path: P) -> T {
    use std::io::Read;
    let mut file = std::fs::File::open(path).expect("fail to open file");
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();
    toml::from_str(&content).expect("parse failed")
}

pub fn get_random_job_size(job_size_dist: &[(usize, usize)], rng: &mut StdRng) -> usize {
    // let job_sizes = [[40, 4], [80, 8], [90, 16], [25, 32], [5, 64]];
    let total: usize = job_size_dist.iter().map(|x| x.0).sum();
    assert_ne!(total, 0);

    let mut n = rng.gen_range(0..total);
    let mut i = 0;
    while i < job_size_dist.len() {
        if n < job_size_dist[i].0 {
            return job_size_dist[i].1;
        }
        n -= job_size_dist[i].0;
        i += 1;
    }

    // default
    32
}

pub fn get_random_arrival_time(lambda: f64, rng: &mut StdRng) -> u64 {
    let poi = Poisson::new(lambda).unwrap();
    poi.sample(rng) as u64
}
