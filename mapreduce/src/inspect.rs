use crate::{
    app::run_map_reduce, argument::Opt, trace::JobTrace, JobSpec, ReducerPlacementPolicy,
    ShufflePattern,
};
use async_std::task;
use futures::stream::StreamExt;
use log::{debug, info};
use nethint::{cluster::Cluster, Duration, Timestamp, ToStdDuration};
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct JobLifetime {
    // start time (ns) of the job, grabbed from trace
    pub start: Timestamp,
    // duration of the job, simulated
    pub dura: Duration,
}

pub fn run_experiments(opt: &Opt, cluster: Arc<Cluster>) -> Option<Vec<(usize, JobLifetime)>> {
    assert!(opt.trace.is_some(), "need to specify a trace file");

    let num_cpus = opt.parallel.unwrap_or(num_cpus::get());

    let job_trace = opt.trace.as_ref().map(|p| {
        JobTrace::from_path(p)
            .unwrap_or_else(|e| panic!("failed to load from file: {:?}, error: {}", p, e))
    });

    assert!(job_trace.is_some());

    task::block_on(async {
        let experiments = futures::stream::iter({
            let ncases = std::cmp::min(
                opt.ncases,
                job_trace.as_ref().map(|v| v.count).unwrap_or(usize::MAX),
            );
            (0..ncases).map(|i| {
                let id = i;
                let cluster = Arc::clone(&cluster);

                let (start_ts, job_spec) = job_trace
                    .as_ref()
                    .map(|job_trace| {
                        let record = job_trace.records[id].clone();
                        debug!("record: {:?}", record);
                        let ts = record.ts;
                        let job_spec = JobSpec::new(
                            record.num_map * opt.num_map,
                            record.num_reduce * opt.num_reduce,
                            ShufflePattern::FromTrace(Box::new(record)),
                        );
                        (ts, job_spec)
                    })
                    .unwrap();

                let policy = ReducerPlacementPolicy::HierarchicalGreedy;

                task::spawn(async move {
                    info!("testcase: {}", id);
                    let output = run_map_reduce(&cluster, &job_spec, policy, id as _);
                    // let time = output.recs.into_iter().map(|r| r.dura.unwrap()).max();
                    let time = Some(0);
                    info!(
                        "{:?}, job_finish_time: {:?}",
                        policy,
                        time.unwrap().to_dura()
                    );
                    Some((
                        i,
                        JobLifetime {
                            start: start_ts * 1_000_000,
                            dura: time.unwrap(),
                        },
                    ))
                })
            })
        })
        .buffer_unordered(num_cpus)
        .collect::<Vec<Option<(usize, JobLifetime)>>>();
        experiments.await.into_iter().collect()
    })
}
