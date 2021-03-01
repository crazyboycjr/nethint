use std::cell::RefCell;
use std::rc::Rc;

use rand::{Rng, rngs::StdRng, SeedableRng};
use rand_distr::{Distribution, Poisson};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use nethint::{
    app::{AppGroup, Application},
    brain::{self, Brain, BrainSetting},
    cluster::Topology,
    multitenant::Tenant,
    simulator::{Executor, SimulatorBuilder, SimulatorSetting},
};

use mapreduce::plink::PlinkApp;

extern crate allreduce;
use allreduce::{app::AllReduceApp, AllReducePolicy, JobSpec};

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "Allreduce Experiment", about = "Allreduce Experiment")]
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
    /// allreduce policy
    policy: AllReducePolicy,
    /// whether to use plink
    probe: ProbeConfig,
    /// Nethint level.
    nethint_level: usize,
    /// Whether to auto tune after a certain iterations
    #[serde(default)]
    auto_tune: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExperimentConfig {
    /// Number of jobs
    ncases: usize,

    /// Number of workers
    job_size_distribution: Vec<(usize, usize)>,

    /// Buffer size of all jobs, in bytes
    buffer_size: usize,

    /// Number of iterations for all jobs
    num_iterations: usize,

    /// Lambda of the poisson arrival
    poisson_lambda: f64,

    /// akin to AWS Placement Group
    placement_strategy: brain::PlacementStrategy,

    /// global seed
    seed: u64,

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
        log::info!(
            "parsing allreduce experiment configuration from file: {:?}",
            path
        );
        read_config(path)
    } else {
        panic!("config file is not specified");
    };

    log::info!("config: {:#?}", config);

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

    // we don't have to be having the exact result as the last run
    // since the results are relatively good, we can accept any seeds
    // let seed = std::time::SystemTime::now()
    //     .duration_since(std::time::UNIX_EPOCH)
    //     .unwrap()
    //     .as_secs();
    // log::info!("seed = {}", seed);
    let seed = config.seed;

    // Generate job information, arrival time and job size
    let mut jobs = Vec::new();

    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut t = 0;
    for i in 0..config.ncases {
        let job_spec = JobSpec::new(
            get_random_job_size(&config.job_size_distribution, &mut rng),
            config.buffer_size,
            config.num_iterations,
        );
        let next = get_random_arrival_time(config.poisson_lambda, &mut rng);
        t += next;
        log::info!("job {}: {:?}", i, job_spec);
        jobs.push((t, job_spec));
    }

    // start to run batches
    let mut estimator = nethint::runtime_est::RunningTimeEstimator::new();
    estimator.set_total_trials(config.batches.len() * config.batch_repeat);
    for i in 0..config.batches.len() {
        for trial_id in 0..config.batch_repeat {
            estimator.bench_single_start();
            run_batch(&config, i, trial_id, seed, &jobs, Rc::clone(&brain));
        }
    }
}

fn get_random_job_size(job_size_dist: &[(usize, usize)], rng: &mut StdRng) -> usize {
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

fn get_random_arrival_time(lambda: f64, rng: &mut StdRng) -> u64 {
    let poi = Poisson::new(lambda).unwrap();
    poi.sample(rng) as u64
}

fn run_batch(
    config: &ExperimentConfig,
    batch_id: usize,
    _trial_id: usize,
    seed: u64,
    jobs: &[(u64, JobSpec)],
    brain: Rc<RefCell<Brain>>,
) {
    // remember to garbage collect remaining jobs
    brain.borrow_mut().reset();

    let ncases = config.ncases;

    // Build the application by composition
    // AppGroup[Tenant[PlinkApp[MapReduceApp]]]
    let mut app_group = AppGroup::new();
    let batch = config.batches[batch_id].clone();
    for i in 0..ncases {
        let tenant_id = i;
        let (start_ts, job_spec) = jobs.get(i).unwrap();

        let allreduce_app = Box::new(AllReduceApp::new(
            job_spec,
            None,
            seed,
            batch.policy,
            batch.nethint_level,
            batch.auto_tune,
        ));

        let nhosts_to_acquire = job_spec.num_workers;

        let app: Box<dyn Application<Output = _>> = if batch.probe.enable {
            Box::new(PlinkApp::new(
                nhosts_to_acquire,
                batch.probe.round_ms,
                allreduce_app,
            ))
        } else {
            allreduce_app
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
        .map(|(i, jct)| (*i, jobs[*i].0, jct.unwrap()))
        .collect();

    println!("{:?}", app_stats);

    // save result to config.directory
    if let Some(mut path) = config.directory.clone() {
        use std::io::Write;
        path.push("result.txt");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .expect("fail to open or create file");
        writeln!(f, "{:?}", app_stats).unwrap();
    }
}
