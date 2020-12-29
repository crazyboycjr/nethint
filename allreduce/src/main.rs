#![feature(box_patterns)]
use std::rc::Rc;
use std::cell::RefCell;

use log::info;
use structopt::StructOpt;

use nethint::{
    app::AppGroup,
    cluster::Topology,
    brain::{Brain, PlacementStrategy},
    simulator::{Executor, Simulator},
};

extern crate allreduce;
use allreduce::{app::AllReduceApp, argument::Opt, JobSpec, AllReducePolicy};

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    info!("Opts: {:#?}", opt);

    let brain = Brain::build_cloud(opt.topo.clone());

    info!("cluster:\n{}", brain.borrow().cluster().to_dot());

    let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    info!("seed = {}", seed);
    run_experiments(&opt, brain, seed);
}

fn run_experiments(opt: &Opt, brain: Rc<RefCell<Brain>>, seed: u64) {
    let mut vc_container = Vec::new();
    let mut job = Vec::new();
    let mut app_group = AppGroup::new();

    for i in 0..opt.ncases {
        let tenant_id = i;
        let seed = i as _;
        let job_spec = JobSpec::new(opt.num_workers);
        let vcluster = brain
            .borrow_mut()
            .provision(tenant_id, job_spec.num_workers, PlacementStrategy::Random(seed))
            .unwrap();
        vc_container.push(vcluster);
        job.push(job_spec);
    }

    for i in 0..opt.ncases {
        let mut app = Box::new(AllReduceApp::new(
            vc_container.get(i).unwrap(),
            seed,
            AllReducePolicy::TopologyAware,
        ));
        app.start();
        app_group.add(0, app);
    }

    let mut simulator = Simulator::new((**brain.borrow().cluster()).clone());
    let app_jct = simulator.run_with_appliation(Box::new(app_group));
    let all_jct = app_jct.iter().map(|(_, jct)| jct.unwrap()).max();
    info!("all job completion time: {:?}", all_jct.unwrap());
}
