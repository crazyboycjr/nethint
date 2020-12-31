#![feature(box_patterns)]
use std::rc::Rc;
use std::cell::RefCell;

use log::info;
use structopt::StructOpt;

use nethint::{
    app::AppGroup,
    brain::{Brain, PlacementStrategy},
    simulator::{Executor, Simulator},
    // cluster::Topology,
};

extern crate allreduce;
use allreduce::{app::AllReduceApp, argument::Opt, JobSpec, AllReducePolicy};

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    // info!("Opts: {:#?}", opt);

    let brain = Brain::build_cloud(opt.topo.clone());

    // info!("cluster:\n{}", brain.borrow().cluster().to_dot());

    let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    info!("seed = {}", seed);
    run_experiments(&opt, brain, seed);
}

fn run_experiments(opt: &Opt, brain: Rc<RefCell<Brain>>, seed: u64) {
    let mut vc_container = Vec::new();
    let mut jobs = Vec::new();
    let mut app_group = AppGroup::new();

    let all_reduce_policy = match opt.nethint_level {
        1 => AllReducePolicy::TopologyAware,
        _ => AllReducePolicy::Random,
    };

    for i in 0..opt.ncases {
        let tenant_id = i;
        // let seed = i as _;
        let job_spec = JobSpec::new(opt.num_workers, opt.buffer_size, opt.num_iterations);
        let vcluster = brain
            .borrow_mut()
            .provision(tenant_id, job_spec.num_workers, PlacementStrategy::Compact)
            .unwrap();
        vc_container.push(vcluster);
        jobs.push(job_spec);
    }

    for i in 0..opt.ncases {
        let mut app = Box::new(AllReduceApp::new(
            jobs.get(i).unwrap(),
            vc_container.get(i).unwrap(),
            seed,
            &all_reduce_policy,
        ));
        app.start();
        app_group.add(0, app);
    }

    let mut simulator = Simulator::new((**brain.borrow().cluster()).clone());
    let app_jct = simulator.run_with_appliation(Box::new(app_group));
    // let all_jct = app_jct.iter().map(|(_, jct)| jct.unwrap()).max();
    // info!("all job completion time: {:?}", all_jct.unwrap());
    info!("{:?}", app_jct);
    println!("{:?}", app_jct);
}
