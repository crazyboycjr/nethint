use std::cell::RefCell;
use std::rc::Rc;

use rand::{rngs::StdRng, SeedableRng};
use structopt::StructOpt;

use nethint::{
    app::AppGroup,
    brain::Brain,
    cluster::Topology,
    multitenant::Tenant,
    simulator::{Executor, SimulatorBuilder},
};

use allreduce::{app::AllReduceApp, JobSpec};

use allreduce::config::{self, read_config, ExperimentConfig};

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "Allreduce Experiment", about = "Allreduce Experiment")]
pub struct Opt {
    /// The configure file
    #[structopt(short = "c", long = "config")]
    pub config: Option<std::path::PathBuf>,

    /// The maximal concurrency to run the batches
    #[structopt(short = "P", long = "parallel")]
    pub parallel: usize,
}

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    log::info!("Opts: {:#?}", opt);

    let config: ExperimentConfig = if let Some(path) = opt.config {
        log::info!(
            "parsing allreduce experiment configuration from file: {:?}",
            path
        );
        read_config(path)
    } else {
        panic!("config file is not specified");
    };

    log::info!("config: {:#?}", config);

    // set rayon max concurrency
    log::info!("using {} threads", opt.parallel);
    rayon::ThreadPoolBuilder::new()
        .num_threads(opt.parallel)
        .build_global()
        .unwrap();

    let brain = Brain::build_cloud(config.brain.clone());

    log::info!("cluster:\n{}", brain.borrow().cluster().to_dot());

    // create the output directory if it does not exist
    if let Some(path) = &config.directory {
        std::fs::create_dir_all(path).expect("fail to create directory");
        let file = path.join("result.txt");
        // remove the previous result file
        if file.exists() {
            std::fs::remove_file(&file).unwrap();
        }

        // then write parsed configuration to it
        std::fs::write(file, format!("{:#?}\n", config)).unwrap();
    };

    let seed = config.seed;

    // start to run batches
    // let mut estimator = nethint::runtime_est::RunningTimeEstimator::new();
    // estimator.set_total_trials(config.batches.len() * config.batch_repeat);
    for i in 0..config.batches.len() {
        // for trial_id in 0..config.batch_repeat {
        //     estimator.bench_single_start();
        //     run_batch(&config, i, trial_id, seed, Rc::clone(&brain));
        // }
        let batch_repeat = config.batch_repeat;
        let config_clone = config.clone();
        let brain_clone = brain.borrow().replicate_for_multithread();
        (0..batch_repeat).into_par_iter().for_each(move |trial_id| {
            let mut brain_clone = brain_clone.replicate_for_multithread();
            brain_clone.set_seed(brain_clone.setting().seed + trial_id as u64);
            run_batch(
                &config_clone,
                i,
                trial_id,
                seed,
                Rc::new(RefCell::new(brain_clone)),
            );
        });
    }
}

fn run_batch(
    config: &ExperimentConfig,
    batch_id: usize,
    trial_id: usize,
    seed: u64,
    brain: Rc<RefCell<Brain>>,
) {
    // remember to garbage collect remaining jobs
    brain.borrow_mut().reset();

    let ncases = config.ncases;

    // Generate job information, arrival time and job size
    let mut jobs = Vec::new();

    let mut rng = StdRng::seed_from_u64(config.seed + trial_id as u64);
    let mut t = 0;
    for i in 0..ncases {
        let job_spec = JobSpec::new(
            config::get_random_job_size(&config.job_size_distribution, &mut rng),
            config.buffer_size,
            config.num_iterations,
        );
        let next = config::get_random_arrival_time(config.poisson_lambda, &mut rng);
        t += next;
        log::info!("job {}: {:?}", i, job_spec);
        jobs.push((t, job_spec));
    }

    // Build the application by composition
    // AppGroup[Tenant[PlinkApp[MapReduceApp]]]
    let mut app_group = AppGroup::new();
    let batch = config.batches[batch_id].clone();
    for i in 0..ncases {
        let tenant_id = i;
        let (start_ts, job_spec) = jobs.get(i).unwrap();

        let nhosts_to_acquire = job_spec.num_workers;

        let allreduce_app = Box::new(AllReduceApp::new(
            job_spec,
            config.computation_speed,
            None,
            seed,
            batch.policy,
            batch.nethint_level,
            batch.auto_tune,
            batch.probe,
            batch.auto_fallback.unwrap_or_default(),
            batch.alpha,
            nhosts_to_acquire,
        ));

        // let app: Box<dyn Application<Output = _>> = if batch.probe.enable {
        //     Box::new(PlinkApp::new(
        //         nhosts_to_acquire,
        //         batch.probe.round_ms,
        //         allreduce_app,
        //     ))
        // } else {
        //     allreduce_app
        // };

        let virtualized_app = Box::new(Tenant::new(
            allreduce_app,
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
    if let Some(path) = config.directory.clone() {
        save_result(path, app_stats);
    }
}

use rayon::prelude::*;

use std::sync::Mutex;
lazy_static::lazy_static! {
    static ref MUTEX: Mutex<()> = Mutex::new(());
}

fn save_result(path: std::path::PathBuf, app_stats: Vec<(usize, u64, u64)>) {
    let _lk = MUTEX.lock().unwrap();
    use std::io::Write;
    let mut f = utils::fs::open_with_create_append(path.join("result.txt"));
    writeln!(f, "{:?}", app_stats).unwrap();
}
