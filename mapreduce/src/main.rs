#![feature(box_patterns)]
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_std::task;
use futures::stream::StreamExt;
use log::{debug, info};
use structopt::StructOpt;

use nethint::{
    app::{AppGroup, Application},
    bandwidth::BandwidthTrait,
    brain::Brain,
    cluster::{Cluster, Topology},
    multitenant::Tenant,
    simulator::{Executor, SimulatorBuilder},
    FairnessModel, ToStdDuration,
};

extern crate mapreduce;
use mapreduce::{
    app::{run_map_reduce, MapReduceApp},
    argument::Opt,
    inspect,
    mapper::MapperPlacementPolicy,
    plink::PlinkApp,
    plot,
    trace::JobTrace,
    JobSpec, ReducerPlacementPolicy, ShufflePattern,
};

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    info!("Opts: {:#?}", opt);

    let brain = Brain::build_cloud(opt.topo.clone());

    if opt.asym {
        brain.borrow_mut().make_asymmetric(1);
    }

    if opt.broken {
        brain.borrow_mut().mark_broken(1, 0.1);
    }

    info!("cluster:\n{}", brain.borrow().cluster().to_dot());

    let policies = &[
        ReducerPlacementPolicy::Random,
        ReducerPlacementPolicy::GeneticAlgorithm,
        ReducerPlacementPolicy::HierarchicalGreedy,
    ];

    if opt.inspect {
        let results = inspect::run_experiments(&opt, Arc::clone(&brain.borrow().cluster()));
        let mut segments = results.unwrap();
        segments.sort_by_key(|x| x.0);
        info!("inspect results: {:?}", segments);

        let data: Vec<_> = segments.into_iter().map(|x| x.1).collect();
        use plot::plot_segments;
        let mut fg = plot_segments(&data);
        fg.show().unwrap();
        return;
    }

    if opt.multitenant {
        let seed_base = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (app_stats1, max_jct1) = run_experiments_multitenant(
            &opt,
            ReducerPlacementPolicy::Random,
            Rc::clone(&brain),
            seed_base,
            false,
            1,
        );

        let (app_stats2, max_jct2) = run_experiments_multitenant(
            &opt,
            ReducerPlacementPolicy::HierarchicalGreedy,
            Rc::clone(&brain),
            seed_base,
            true,
            1,
        );

        let (app_stats3, max_jct3) = run_experiments_multitenant(
            &opt,
            ReducerPlacementPolicy::HierarchicalGreedy,
            Rc::clone(&brain),
            seed_base,
            false,
            1,
        );

        let (app_stats4, max_jct4) = run_experiments_multitenant(
            &opt,
            ReducerPlacementPolicy::HierarchicalGreedy,
            Rc::clone(&brain),
            seed_base,
            false,
            2,
        );

        println!("Random:");
        println!("app_stats: {:?}", app_stats1);
        println!("max job completion time: {:?}", max_jct1.to_dura());

        println!("Plink:");
        println!("app_stats: {:?}", app_stats2);
        println!("max job completion time: {:?}", max_jct2.to_dura());

        println!("Greedy NetHint Level 1:");
        println!("app_stats: {:?}", app_stats3);
        println!("max job completion time: {:?}", max_jct3.to_dura());

        println!("Greedy NetHint Level 2:");
        println!("app_stats: {:?}", app_stats4);
        println!("max job completion time: {:?}", max_jct4.to_dura());

        let results = app_stats1
            .into_iter()
            .zip(app_stats2)
            .zip(app_stats3)
            .zip(app_stats4)
            .flat_map(|(((x, y), z), w)| {
                vec![
                    (x.0 * 4, x.2),
                    (y.0 * 4 + 1, y.2),
                    (z.0 * 4 + 2, z.2),
                    (w.0 * 4 + 3, w.2),
                ]
            })
            .collect();
        visualize(&opt, Some(results)).unwrap();
        return;
    }

    let results = run_experiments(&opt, Arc::clone(&brain.borrow().cluster()), policies);

    visualize(&opt, results).unwrap();
}

fn run_experiments_multitenant(
    opt: &Opt,
    policy: ReducerPlacementPolicy,
    brain: Rc<RefCell<Brain>>,
    seed_base: u64,
    use_plink: bool,
    nethint_level: usize,
) -> (Vec<(usize, u64, u64)>, u64) {
    let job_trace = opt.trace.as_ref().map(|p| {
        JobTrace::from_path(p)
            .unwrap_or_else(|e| panic!("failed to load from file: {:?}, error: {}", p, e))
    });

    let ncases = std::cmp::min(
        opt.ncases,
        job_trace.as_ref().map(|v| v.count).unwrap_or(usize::MAX),
    );

    // values in a scope are dropped in the opposite order they are defined
    let mut job = Vec::new();
    let mut app_group = AppGroup::new();
    for i in 0..ncases {
        let id = i;
        let (start_ts, job_spec) = job_trace
            .as_ref()
            .map(|job_trace| {
                let mut record = job_trace.records[id].clone();
                // mutiple traffic by a number
                record.reducers = record
                    .reducers
                    .into_iter()
                    .map(|(a, b)| (a, b * opt.traffic_scale))
                    .collect();
                let start_ts = record.ts * 1_000_000;
                debug!("record: {:?}", record);
                let job_spec = JobSpec::new(
                    record.num_map * opt.num_map,
                    record.num_reduce * opt.num_reduce,
                    ShufflePattern::FromTrace(Box::new(record)),
                );
                (start_ts, job_spec)
            })
            .unwrap();

        job.push((start_ts, job_spec));
    }

    for i in 0..ncases {
        let seed = i as _;
        let tenant_id = i;
        let (start_ts, job_spec) = job.get(i).unwrap();
        let mapreduce_app = Box::new(MapReduceApp::new(
            seed,
            job_spec,
            None,
            // MapperPlacementPolicy::Random(seed_base + seed),
            // MapperPlacementPolicy::Greedy,
            MapperPlacementPolicy::RandomSkew(seed_base + seed, 0.2),
            policy,
            nethint_level,
            opt.collocate,
        ));

        let nhosts_to_acquire = if opt.collocate {
            job_spec.num_map.max(job_spec.num_reduce)
        } else {
            job_spec.num_map + job_spec.num_reduce
        };

        let app: Box<dyn Application<Output = _>> = if use_plink {
            Box::new(PlinkApp::new(nhosts_to_acquire, mapreduce_app))
        } else {
            mapreduce_app
        };

        let virtualized_app = Box::new(Tenant::new(
            app,
            tenant_id,
            nhosts_to_acquire,
            Rc::clone(&brain),
        ));

        app_group.add(*start_ts, virtualized_app);
    }

    debug!("app_group: {:?}", app_group);

    // let mut simulator = Simulator::new((**brain.cluster()).clone());
    // let mut simulator = Simulator::with_brain(Rc::clone(&brain));
    let mut simulator = SimulatorBuilder::new()
        .enable_nethint(true)
        .brain(Rc::clone(&brain))
        // .fairness(FairnessModel::PerFlowMinMax)
        .fairness(FairnessModel::TenantFlowMinMax)
        .sample_interval_ns(100_000_000)
        .loopback_speed(400.gbps())
        .build()
        .unwrap_or_else(|e| panic!("{}", e));
    let app_jct = simulator.run_with_appliation(Box::new(app_group));
    let max_jct = app_jct.iter().map(|(_, jct)| jct.unwrap()).max();
    let app_stats: Vec<_> = app_jct
        .iter()
        .map(|(i, jct)| (*i, job[*i].0, jct.unwrap()))
        .collect();

    let app_stats: Vec<_> = app_stats
        .into_iter()
        .filter(|&(id, _start, _dura)| {
            if let Some(job_trace) = job_trace.as_ref() {
                let record = &job_trace.records[id];
                let weights: Vec<_> = record.reducers.iter().map(|(_x, y)| *y as u64).collect();
                !(record.num_map == 1
                    || weights.iter().copied().max() == weights.iter().copied().min())
            } else {
                true
            }
        })
        .collect();

    brain.borrow_mut().garbage_collect(ncases);
    (app_stats, max_jct.unwrap())
}

fn run_experiments(
    opt: &Opt,
    cluster: Arc<Cluster>,
    policies: &[ReducerPlacementPolicy],
) -> Option<Vec<(usize, u64)>> {
    let num_cpus = opt.parallel.unwrap_or_else(num_cpus::get);

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
                        ShufflePattern::FromTrace(Box::new(record)),
                    )
                } else {
                    JobSpec::new(opt.num_map, opt.num_reduce, opt.shuffle.clone())
                };

                let policy = policies[i % ngroups];

                task::spawn(async move {
                    info!("testcase: {}", id);
                    let jct = run_map_reduce(&cluster, &job_spec, policy, id as _);
                    info!(
                        "{:?}, job_finish_time: {:?}",
                        policy,
                        jct.unwrap().to_dura()
                    );
                    Some((i, jct.unwrap()))
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
        a.sort_unstable();
        a.into_iter().map(|t| t.1).collect()
    });

    let data = Arc::new(data.ok_or_else(|| anyhow!("Empty experiment data"))?);

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

    use plot::plot4;
    let future1 = async_plot!("mapreduce", plot4);

    use plot::plot_cdf4;
    let future2 = async_plot!("mapreduce_cdf", plot_cdf4);

    let _ = task::block_on(async { futures::join!(future1, future2) });

    Ok(())
}
