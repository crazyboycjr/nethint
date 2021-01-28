use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use nethint::{
    brain::{self, Brain, BrainSetting},
    cluster::Topology,
    simulator::SimulatorSetting,
};

use mapreduce::{mapper::MapperPlacementPolicy, ReducerPlacementPolicy};
#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "MapReduce Experiment", about = "MapReduce Experiment")]
pub struct Opt {
    /// The configure file
    #[structopt(short = "c", long = "config")]
    pub config: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ProbeConfig {
    enable: bool,
    #[serde(default)]
    round_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchConfig {
    /// Reducer placement policy
    reducer_policy: ReducerPlacementPolicy,
    /// whether to use plink
    probe: ProbeConfig,
    /// Nethint level.
    nethint_level: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExperimentConfig {
    /// Run experiments from trace file
    #[serde(default)]
    trace: Option<std::path::PathBuf>,

    /// Number of testcases
    ncases: usize,

    /// Number of map tasks. When using trace, this parameter means map scale factor
    num_map: usize,

    /// Number of reduce tasks. When using trace, this parameter means reduce scale factor
    num_reduce: usize,

    /// Traffic scale, multiply the traffic size by a number to allow job overlaps
    traffic_scale: f64,

    /// Mapper placement policy
    mapper_policy: MapperPlacementPolicy,

    /// akin to AWS Placement Group
    placement_strategy: brain::PlacementStrategy,

    /// Collocate or De-collocate
    collocate: bool,

    #[serde(rename = "batch")]
    batches: Vec<BatchConfig>,

    /// Output path of the figure
    #[serde(default)]
    directory: Option<std::path::PathBuf>,

    /// Simulator settings
    simulator: SimulatorSetting,

    /// Brain settings
    brain: BrainSetting,
}

fn read_config<P: AsRef<std::path::Path>>(path: P) -> ExperimentConfig {
    use std::io::Read;
    let mut file = std::fs::File::open(path).expect("fail to open file");
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();
    toml::from_str(&content).expect("parse failed")
}

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    log::info!("Opts: {:#?}", opt);

    let config = if let Some(path) = opt.config {
        log::info!("parsing experiment configuration from file: {:?}", path);
        read_config(path)
    } else {
        panic!("config file is not specified");
    };

    log::info!("config: {:#?}", config);

    let brain = Brain::build_cloud(config.brain);

    log::info!("cluster:\n{}", brain.borrow().cluster().to_dot());

    // let mut jobs = Vec::new();
}
