use crate::{ShufflePattern, mapper::MapperPlacementPolicy, ReducerPlacementPolicy};
use nethint::brain::{self, BrainSetting};
use nethint::simulator::SimulatorSetting;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeConfig {
    pub enable: bool,
    #[serde(default)]
    pub round_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Reducer placement policy
    pub reducer_policy: ReducerPlacementPolicy,
    /// whether to use plink
    pub probe: ProbeConfig,
    /// Nethint level.
    pub nethint_level: usize,
    /// automatically choose which solution to use, BW or TO
    #[serde(default)]
    pub auto_fallback: Option<bool>,
    /// the alpha, details in paper
    #[serde(default)]
    pub alpha: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentConfig {
    /// Run experiments from trace file
    #[serde(default)]
    pub trace: Option<std::path::PathBuf>,

    /// How to generate the shuffle
    pub shuffle: Option<ShufflePattern>,

    /// Number of testcases
    pub ncases: usize,

    /// Number of map tasks. When using trace, this parameter means map scale factor
    pub num_map: usize,

    /// Number of reduce tasks. When using trace, this parameter means reduce scale factor
    pub num_reduce: usize,

    /// The map scale used only in testbed setting to support scale down; default 1.0
    pub map_scale: Option<f64>,

    /// The reduce scale used only in testbed setting to support scale down; default 1.0
    pub reduce_scale: Option<f64>,

    /// Traffic scale, multiply the traffic size by a number to allow job overlaps
    pub traffic_scale: f64,

    /// Scale the time of job arrival; default 1.0
    pub time_scale: Option<f64>,

    /// Computation time switch
    pub enable_computation_time: bool,

    /// Mapper placement policy
    pub mapper_policy: MapperPlacementPolicy,

    /// akin to AWS Placement Group
    pub placement_strategy: brain::PlacementStrategy,

    /// Whether to allow delay scheduling, default to false, in simulation, it must be false
    pub allow_delay: Option<bool>,

    /// Whether to skip trivial jobs; default false
    pub skip_trivial: Option<bool>,

    /// Collocate or De-collocate
    pub collocate: bool,

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

    /// Environment variables
    #[serde(default)]
    pub envs: toml::value::Table,
}

pub fn read_config<P: AsRef<std::path::Path>>(path: P) -> ExperimentConfig {
    use std::io::Read;
    let mut file = std::fs::File::open(path).expect("fail to open file");
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();
    toml::from_str(&content).expect("parse failed")
}
