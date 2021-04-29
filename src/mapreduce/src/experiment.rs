use std::cell::RefCell;
use std::rc::Rc;

use structopt::StructOpt;

use nethint::{
    app::{AppGroup, Application},
    brain::Brain,
    cluster::Topology,
    multitenant::Tenant,
    simulator::{Executor, SimulatorBuilder},
};

use mapreduce::{
    app::MapReduceApp, mapper::MapperPlacementPolicy, plink::PlinkApp, trace::JobTrace, JobSpec,
    ShufflePattern,
};

use mapreduce::config::{read_config, ExperimentConfig};

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
            run_batch(
                &config_clone,
                i,
                trial_id,
                Rc::new(RefCell::new(brain_clone)),
            );
        });
    }
}

fn run_batch(
    config: &ExperimentConfig,
    batch_id: usize,
    trial_id: usize,
    brain: Rc<RefCell<Brain>>,
) {
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
            // let start_ts = record.ts * 1_000_000;
            let time_scale = config.time_scale.unwrap_or(1.0);
            let start_ts = ((record.ts * 1_000_000) as f64 * time_scale) as _;
            
            log::debug!("record: {:?}", record);
            // let job_spec = JobSpec::new(
            //     record.num_map * config.num_map,
            //     record.num_reduce * config.num_reduce,
            //     ShufflePattern::FromTrace(Box::new(record)),
            // );
            let map_scale = config.map_scale.unwrap_or(1.0);
            let reduce_scale = config.reduce_scale.unwrap_or(1.0);
            let job_spec = JobSpec::new(
                std::cmp::max(1, (record.num_map as f64 * map_scale) as usize),
                std::cmp::max(
                    1,
                    (record.num_reduce as f64 * reduce_scale) as usize,
                ),
                ShufflePattern::FromTrace(Box::new(record)),
            );
            (start_ts, job_spec)
        };

        log::info!("is_trivial: {} {}", i, is_job_trivial(&job_spec));
        job.push((start_ts, job_spec));
    }

    // Build the application by combination
    // AppGroup[Tenant[PlinkApp[MapReduceApp]]]
    let mut start_ts_vec = Vec::new();
    let batch = config.batches[batch_id].clone();
    for i in 0..ncases {
        let seed = (ncases * trial_id + i) as _;
        let tenant_id = i;
        let (start_ts, job_spec) = job.get(i).unwrap();

        if config.skip_trivial.unwrap_or(false) && is_job_trivial(&job_spec) {
            continue;
        }

        start_ts_vec.push(*start_ts);

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
            config.brain.topology.clone().host_bw(),
            config.enable_computation_time,
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
    
    let mut app_stats: Vec<_> = app_jct
        .iter()
        .map(|(i, jct)| (*i, start_ts_vec[*i], jct.unwrap()))
        .collect();

    // filter out all trival jobs
    if !config.skip_trivial.unwrap_or(false) {
        app_stats = app_stats
            .into_iter()
            .filter(|&(id, _start, _dura)| {
                let record = &job_trace.records[id];
                let weights: Vec<_> = record.reducers.iter().map(|(_x, y)| *y as u64).collect();
                !(record.num_map == 1 || weights.iter().copied().max() == weights.iter().copied().min())
            })
            .collect();
    }

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

fn save_result(path: std::path::PathBuf, app_stats: Vec<(usize, u64, u64)>) {
    let _lk = MUTEX.lock().unwrap();
    use std::io::Write;
    let mut f = utils::fs::open_with_create_append(path.join("result.txt"));
    writeln!(f, "{:?}", app_stats).unwrap();
}


// this code snippet is copied from scheduler.rs
fn is_job_trivial(job_spec: &JobSpec) -> bool {
    match job_spec.shuffle_pat {
        ShufflePattern::FromTrace(ref record) => {
            let mut weights: Vec<u64> = vec![0u64; job_spec.num_reduce];
            for (i, (_x, y)) in record.reducers.iter().enumerate() {
                weights[i % job_spec.num_reduce] += *y as u64;
            }
            job_spec.num_map == 1
                || weights.iter().copied().max().unwrap() as f64
                    <= 1.05 * weights.iter().copied().min().unwrap() as f64
        }
        _ => {
            unimplemented!();
        }
    }
}