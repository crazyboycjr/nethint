use std::cell::RefCell;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use nethint::{
    app::{AppGroup, Application},
    brain::{self, Brain, BrainSetting},
    cluster::Topology,
    multitenant::Tenant,
    simulator::{Executor, SimulatorBuilder, SimulatorSetting},
    architecture::TopoArgs,
};

use mapreduce::{
    app::MapReduceApp, mapper::MapperPlacementPolicy, plink::PlinkApp, trace::JobTrace, JobSpec,
    ReducerPlacementPolicy, ShufflePattern, app::ReducerMeta,
};

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "MapReduce Experiment", about = "MapReduce Experiment")]
pub struct Opt {
    /// The configure file
    #[structopt(short = "c", long = "config")]
    pub config: Option<std::path::PathBuf>,

    /// The maximal concurrency to run the batches
    #[structopt(short = "P", long = "parallel")]
    pub parallel: usize,
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

    /// Computation time switch
    enable_computation_time: bool,

    /// Mapper placement policy
    mapper_policy: MapperPlacementPolicy,

    /// akin to AWS Placement Group
    placement_strategy: brain::PlacementStrategy,

    /// Collocate or De-collocate
    collocate: bool,

    /// Number of repeats for each batch of experiments
    batch_repeat: usize,

    #[serde(rename = "batch")]
    batches: Vec<BatchConfig>,

    /// Output path of the figure
    #[serde(default)]
    directory: Option<std::path::PathBuf>,

    /// Simulator settings
    simulator: SimulatorSetting,

    /// Brain settings
    brain: BrainSetting,

    /// Environment variables
    #[serde(default)]
    envs: toml::value::Table,
}

fn read_config<P: AsRef<std::path::Path>>(path: P) -> ExperimentConfig {
    use std::io::Read;
    let mut file = std::fs::File::open(path).expect("fail to open file");
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();
    toml::from_str(&content).expect("parse failed")
}

fn set_env_vars(config: &ExperimentConfig) {
    for (k, v) in config.envs.iter() {
        let v = v.as_str().expect("expect String type");
        log::debug!("setting environment {}={}", k, v);
        std::env::set_var(k, v);
    }
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

    set_env_vars(&config);

    // set rayon max concurrency
    log::info!("using {} threads", opt.parallel);
    rayon::ThreadPoolBuilder::new().num_threads(opt.parallel).build_global().unwrap();

    let brain = Brain::build_cloud(config.brain.clone());
    
    log::info!("cluster:\n{}", brain.borrow().cluster().to_dot());

    // create the output directory if it does not exist
    if let Some(path) = &config.directory {
        std::fs::create_dir_all(path).expect("fail to create directory");
        let mut file = path.clone();
        file.push("result.txt");
        // remove the previous result file
        if file.exists() {
            std::fs::remove_file(file.clone()).unwrap();
        }

        // then write parsed configuration to it
        std::fs::write(file, format!("{:#?}\n", config)).unwrap();
    };

    // let mut estimator = nethint::runtime_est::RunningTimeEstimator::new();
    // estimator.set_total_trials(config.batches.len() * config.batch_repeat);
    for i in 0..config.batches.len() {
        // for trial_id in 0..config.batch_repeat {
        //     estimator.bench_single_start();
        //     run_batch(&config, i, trial_id, Rc::clone(&brain));
        // }

        let batch_repeat = config.batch_repeat;
        let config_clone = config.clone();
        let brain_clone = brain.borrow().replicate_for_multithread();
        (0..batch_repeat).into_par_iter().for_each(move |trial_id| {
            let brain_clone = brain_clone.replicate_for_multithread();
            run_batch(&config_clone, i, trial_id, Rc::new(RefCell::new(brain_clone)));
        });
    }
}

fn run_batch(config: &ExperimentConfig, batch_id: usize, trial_id: usize, brain: Rc<RefCell<Brain>>) {

println!("topology {:?}", (config.brain.topology ));
let mut host_bandwidth = 0.0;
match config.brain.topology {
    TopoArgs::Arbitrary {
        nracks,
        rack_size,
        host_bw,
        rack_bw,
    } => {
        host_bandwidth = host_bw;
    },
    _ => (),
}
println!("badwidth {:?}", (host_bandwidth ));

    let job_trace = config
        .trace
        .as_ref()
        .map(|p| {
            JobTrace::from_path(p)
                .unwrap_or_else(|e| panic!("failed to load from file: {:?}, error: {}", p, e))
        })
        .unwrap();

    let ncases = std::cmp::min(config.ncases, job_trace.count);
    
    // Read job information (start_ts, job_spec) from file
    let mut job = Vec::new();
    let mut app_group = AppGroup::new();
    for i in 0..ncases {
        let id = i;
        let (start_ts, job_spec) = {
            let mut record = job_trace.records[id].clone();
            // mutiply traffic by a number
            record.reducers = record
                .reducers
                .into_iter()
                .map(|(a, b)| (a, b * config.traffic_scale))
                .collect();
            let start_ts = record.ts * 1_000_000;
            
            log::debug!("record: {:?}", record);
            let job_spec = JobSpec::new(
                record.num_map * config.num_map,
                record.num_reduce * config.num_reduce,
                ShufflePattern::FromTrace(Box::new(record)),
            );
            (start_ts, job_spec)
        };

        job.push((start_ts, job_spec));
    }

    // Build the application by combination
    // AppGroup[Tenant[PlinkApp[MapReduceApp]]]
    let batch = config.batches[batch_id].clone();
    for i in 0..ncases {
        let seed = (ncases * trial_id + i) as _;
        let tenant_id = i;
        let (start_ts, job_spec) = job.get(i).unwrap();
        let mapper_policy = {
            use MapperPlacementPolicy::*;
            match &config.mapper_policy {
                Random(base) => Random(base + seed),
                Greedy => Greedy,
                RandomSkew(base, s) => RandomSkew(base + seed, *s),
                FromTrace(r) => FromTrace(r.clone()),
            }
        };

        let mapreduce_app = Box::new(MapReduceApp::new(
            seed,
            job_spec,
            None,
            mapper_policy.clone(),
            batch.reducer_policy,
            batch.nethint_level,
            config.collocate,
            host_bandwidth,
            config.enable_computation_time,
            ReducerMeta::default(),
        ));

        let nhosts_to_acquire = if config.collocate {
            job_spec.num_map.max(job_spec.num_reduce)
        } else {
            job_spec.num_map + job_spec.num_reduce
        };

        let app: Box<dyn Application<Output = _>> = if batch.probe.enable {
            Box::new(PlinkApp::new(
                nhosts_to_acquire,
                batch.probe.round_ms,
                mapreduce_app,
            ))
        } else {
            mapreduce_app
        };

        let virtualized_app = Box::new(Tenant::new(
            app,
            tenant_id,
            nhosts_to_acquire,
            Rc::clone(&brain),
            config.placement_strategy,
        ));

        app_group.add(*start_ts, virtualized_app);
    }
    
    
    log::debug!("app_group: {:?}", app_group);

    // setup simulator
    let mut simulator = SimulatorBuilder::new()
        .brain(Rc::clone(&brain))
        .with_setting(config.simulator)
        .build()
        .unwrap_or_else(|e| panic!("{}", e));

    // run application in simulator
    let app_jct = simulator.run_with_application(Box::new(app_group));
    
    let app_stats: Vec<_> = app_jct
        .iter()
        .map(|(i, jct)| (*i, job[*i].0, jct.unwrap()))
        .collect();

    // filter out all trival jobs
    let app_stats: Vec<_> = app_stats
        .into_iter()
        .filter(|&(id, _start, _dura)| {
            let record = &job_trace.records[id];
            let weights: Vec<_> = record.reducers.iter().map(|(_x, y)| *y as u64).collect();
            !(record.num_map == 1 || weights.iter().copied().max() == weights.iter().copied().min())
        })
        .collect();

    // remember to garbage collect remaining jobs
    brain.borrow_mut().reset();

    println!("{:?}", app_stats);

    // save result to config.directory
    if let Some(path) = config.directory.clone() {
        save_result(path, app_stats);
    }
}

use rayon::prelude::*;

use std::sync::Mutex;
lazy_static::lazy_static! {
    static ref MUTEX: Mutex<()> = Mutex::new(());
}

fn save_result(mut path: std::path::PathBuf, app_stats: Vec<(usize, u64, u64)>) {
    let _lk = MUTEX.lock().unwrap();
    use std::io::Write;
    path.push("result.txt");
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .expect("fail to open or create file");
    writeln!(f, "{:?}", app_stats).unwrap();
}