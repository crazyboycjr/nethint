use crate::{mapper::MapperPlacementPolicy, ReducerPlacementPolicy};
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentConfig {
    /// Run experiments from trace file
    #[serde(default)]
    pub trace: Option<std::path::PathBuf>,

    /// Number of testcases
    pub ncases: usize,

    /// Number of map tasks. When using trace, this parameter means map scale factor
    pub num_map: usize,

    /// Number of reduce tasks. When using trace, this parameter means reduce scale factor
    pub num_reduce: usize,

    /// Traffic scale, multiply the traffic size by a number to allow job overlaps
    pub traffic_scale: f64,

    /// Mapper placement policy
    pub mapper_policy: MapperPlacementPolicy,

    /// akin to AWS Placement Group
    pub placement_strategy: brain::PlacementStrategy,

    /// whether to allow delay scheduling, default to false, in simulation, it must be false
    pub allow_delay: Option<bool>,

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
