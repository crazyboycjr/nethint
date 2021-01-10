#![feature(box_patterns)]
use rand::Rng;
use std::cell::RefCell;
use std::rc::Rc;
use rand::distributions::{Poisson, Distribution};

use log::info;
use structopt::StructOpt;

use nethint::{
    app::AppGroup,
    brain::Brain,
    multitenant::Tenant,
    simulator::{Executor, SimulatorBuilder},
    FairnessModel,
};

extern crate allreduce;
use allreduce::{app::AllReduceApp, argument::Opt, AllReducePolicy, JobSpec};

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    // info!("Opts: {:#?}", opt);

    let brain = Brain::build_cloud(opt.topo.clone());

    // info!("cluster:\n{}", brain.borrow().cluster().to_dot());

    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    info!("seed = {}", seed);
    run_experiments(&opt, brain, seed, false);
}

fn get_random_job_size() -> usize {
    let job_sizes = [[40, 2], [80, 4], [90, 8], [25, 16], [5, 32]];
    let mut rng = rand::thread_rng();
    let mut n = rng.gen_range(0, 240);
    let mut i = 0;
    while i < 5 {
        if n < job_sizes[i][0] {
            return job_sizes[i][1];
        }
        n -= job_sizes[i][0];
        i += 1;
    }
    32
}

fn get_random_arrival_time(lambda: f64) -> u64 {
    let poi = Poisson::new(lambda);
    poi.sample(&mut rand::thread_rng())
}

fn run_experiments(opt: &Opt, brain: Rc<RefCell<Brain>>, seed: u64, use_plink: bool) {
    let mut jobs = Vec::new();
    let mut app_group = AppGroup::new();

    let all_reduce_policy = match opt.nethint_level {
        1 => AllReducePolicy::TopologyAware,
        _ => AllReducePolicy::Random,
    };

    let mut t = 0;
    for _i in 0..opt.ncases {
        let job_spec = JobSpec::new(get_random_job_size(), opt.buffer_size, opt.num_iterations);
        let next = get_random_arrival_time(opt.poisson_lambda);
        t += next;
        jobs.push((t, job_spec));
    }

    for i in 0..opt.ncases {
        let tenant_id = i;
        let (start_ts, job_spec) = jobs.get(i).unwrap();

        let allreduce_app =
            Box::new(AllReduceApp::new(job_spec, None, seed, &all_reduce_policy, opt.nethint_level));

        let nhosts_to_acquire = job_spec.num_workers;

        let virtualized_app = Box::new(Tenant::new(
            allreduce_app,
            tenant_id,
            nhosts_to_acquire,
            Rc::clone(&brain),
        ));

        app_group.add(*start_ts, virtualized_app);
    }

    let mut simulator = SimulatorBuilder::new()
        .enable_nethint(true)
        .brain(Rc::clone(&brain))
        .fairness(FairnessModel::TenantFlowMinMax)
        .sample_interval_ns(100_000_000)
        .build()
        .unwrap_or_else(|e| panic!("{}", e));
    let app_jct = simulator.run_with_appliation(Box::new(app_group));
    let app_stats: Vec<_> = app_jct
        .iter()
        .map(|(i, jct)| (*i, jobs[*i].0, jct.unwrap()))
        .collect();

    println!("{:?}", app_stats);
    // let mut simulator = Simulator::new((**brain.borrow().cluster()).clone());
    // let app_jct = simulator.run_with_appliation(Box::new(app_group));
    // info!("{:?}", app_jct);
    // println!("{:?}", app_jct);
}
