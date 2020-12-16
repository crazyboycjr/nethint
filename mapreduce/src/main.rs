#![feature(box_patterns)]
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_std::task;
use futures::stream::StreamExt;
use log::{debug, info};
use structopt::StructOpt;

use nethint::{brain::Brain, cluster::Cluster, ToStdDuration};

extern crate mapreduce;
use mapreduce::{
    app::run_map_reduce, argument::Opt, inspect, plot, trace::JobTrace, JobSpec,
    ReducerPlacementPolicy, ShuffleDist,
};

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    info!("Opts: {:#?}", opt);

    let mut brain = Brain::build_cloud(opt.topo.clone());

    if opt.asym {
        brain.make_asymmetric(0);
    }

    info!("cluster:\n{}", brain.cluster().to_dot());

    let policies = &[
        ReducerPlacementPolicy::Random,
        ReducerPlacementPolicy::GeneticAlgorithm,
        ReducerPlacementPolicy::HierarchicalGreedy,
    ];

    if opt.inspect {
        let results = inspect::run_experiments(&opt, Arc::clone(&brain.cluster()));
        let mut segments = results.unwrap();
        segments.sort_by_key(|x| x.0);
        info!("inspect results: {:?}", segments);

        let data = segments.into_iter().map(|x| x.1).collect();
        use plot::plot_segments;
        let mut fg = plot_segments(&data);
        fg.show().unwrap();
        return;
    }

    let results = run_experiments(&opt, Arc::clone(&brain.cluster()), policies);

    visualize(&opt, results).unwrap();
}

fn run_experiments(
    opt: &Opt,
    cluster: Arc<Cluster>,
    policies: &[ReducerPlacementPolicy],
) -> Option<Vec<(usize, u64)>> {
    let num_cpus = opt.parallel.unwrap_or(num_cpus::get());

    let ngroups = policies.len();

    let job_trace = opt.trace.as_ref().map(|p| {
        JobTrace::from_path(p)
            .unwrap_or_else(|e| panic!("failed to load from file: {:?}, error: {}", p, e))
    });

    task::block_on(async {
        let experiments = futures::stream::iter({
            let ncases = std::cmp::min(
                opt.ncases,
                job_trace.as_ref().map(|v| v.count).unwrap_or(usize::MAX),
            );
            (0..ncases * ngroups).map(|i| {
                let id = i / ngroups;
                let cluster = Arc::clone(&cluster);

                let job_spec = if let Some(job_trace) = job_trace.as_ref() {
                    let record = job_trace.records[id].clone();
                    debug!("record: {:?}", record);
                    JobSpec::new(
                        record.num_map * opt.num_map,
                        record.num_reduce * opt.num_reduce,
                        ShuffleDist::FromTrace(Box::new(record)),
                    )
                } else {
                    JobSpec::new(opt.num_map, opt.num_reduce, opt.shuffle.clone())
                };

                let policy = policies[i % ngroups];

                task::spawn(async move {
                    info!("testcase: {}", id);
                    let output = run_map_reduce(&cluster, &job_spec, policy, id as _);
                    let time = output.recs.into_iter().map(|r| r.dura.unwrap()).max();
                    info!(
                        "{:?}, job_finish_time: {:?}",
                        policy,
                        time.unwrap().to_dura()
                    );
                    Some((i, time.unwrap()))
                })
            })
        })
        .buffer_unordered(num_cpus)
        .collect::<Vec<Option<(usize, u64)>>>();
        experiments.await.into_iter().collect()
    })
}

fn visualize(opt: &Opt, experiments: Option<Vec<(usize, u64)>>) -> Result<()> {
    let data: Option<Vec<u64>> = experiments.map(|mut a| {
        a.sort();
        a.into_iter().map(|t| t.1).collect()
    });

    let data = Arc::new(data.ok_or(anyhow!("Empty experiment data"))?);

    macro_rules! async_plot {
        ($fig_prefix:expr, $func:path) => {{
            let output_path = opt.directory.as_ref().map(|directory| {
                let mut path = directory.clone();
                let fname = opt.to_filename($fig_prefix);
                path.push(fname);
                path
            });

            let data1 = Arc::clone(&data);

            let norm = opt.normalize;
            let future: task::JoinHandle<Result<()>> = task::spawn(async move {
                let mut fg = $func(&data1, norm);

                if let Some(path) = output_path {
                    fg.save_to_pdf(&path, 12, 8).map_err(|e| anyhow!("{}", e))?;
                    info!("save figure to {:?}", path);
                }

                fg.show().map_err(|e| anyhow!("{}", e))?;
                Ok(())
            });

            future
        }};
    }

    use plot::plot;
    let future1 = async_plot!("mapreduce", plot);

    use plot::plot_cdf;
    let future2 = async_plot!("mapreduce_cdf", plot_cdf);

    let _ = task::block_on(async { futures::join!(future1, future2) });

    Ok(())
}
