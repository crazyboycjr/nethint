#![feature(box_patterns)]
use std::sync::Arc;

use log::{debug, info};
use structopt::StructOpt;

use nethint::{
    app::AppGroup,
    brain::{Brain, PlacementStrategy},
    cluster::Cluster,
};

extern crate allreduce;
use allreduce::{
    app::{run_allreduce, AllReduceApp},
    argument::Opt,
};

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    info!("Opts: {:#?}", opt);

    let mut brain = Brain::build_cloud(opt.topo.clone());

    info!("cluster:\n{}", brain.cluster().to_dot());

    run_experiments(&opt, Arc::clone(&brain.cluster()));
}

fn run_experiments(
    opt: &Opt,
    cluster: Arc<Cluster>,
) {
    let jct = run_allreduce(&cluster);
    info!(
        "job_finish_time: {:?}", jct.unwrap()
    );
}