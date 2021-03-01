use std::rc::Rc;
use nethint::{
    app::{AppEvent, AppEventKind, Application, Replayer},
    cluster::Topology,
    simulator::{Event, Events},
    Duration, Timestamp, Trace, TraceRecord,
    hint::NetHintVersion,
};

use crate::{
    random_ring::RandomRingAllReduce, topology_aware::TopologyAwareRingAllReduce, rat::RatAllReduce,
    AllReduceAlgorithm, AllReducePolicy, JobSpec,
};

pub struct AllReduceApp<'c> {
    job_spec: &'c JobSpec,
    cluster: Option<Rc<dyn Topology>>,
    replayer: Replayer,
    jct: Option<Duration>,
    seed: u64,
    allreduce_policy: AllReducePolicy,
    remaining_iterations: usize,
    remaining_flows: usize,
    nethint_level: usize,
    autotune: Option<usize>,
}

impl<'c> std::fmt::Debug for AllReduceApp<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AllReduceApp")
    }
}

impl<'c> AllReduceApp<'c> {
    pub fn new(
        job_spec: &'c JobSpec,
        cluster: Option<Rc<dyn Topology>>,
        seed: u64,
        allreduce_policy: AllReducePolicy,
        nethint_level: usize,
        autotune: Option<usize>,
    ) -> Self {
        let trace = Trace::new();
        AllReduceApp {
            job_spec,
            cluster,
            replayer: Replayer::new(trace),
            jct: None,
            seed,
            allreduce_policy,
            remaining_iterations: job_spec.num_iterations,
            remaining_flows: 0,
            nethint_level,
            autotune,
        }
    }

    pub fn start(&mut self) {
        self.remaining_iterations = self.job_spec.num_iterations;
        self.allreduce(0);
    }

    pub fn allreduce(&mut self, start_time: Timestamp) {
        let mut trace = Trace::new();

        let mut allreduce_algorithm: Box<dyn AllReduceAlgorithm> = match self.allreduce_policy {
            AllReducePolicy::Random => Box::new(RandomRingAllReduce::new(self.seed)),
            AllReducePolicy::TopologyAware => Box::new(TopologyAwareRingAllReduce::new(self.seed)),
            AllReducePolicy::RAT => Box::new(RatAllReduce::new()),
        };

        let flows =
            allreduce_algorithm.allreduce(self.job_spec.buffer_size as u64, &**self.cluster.as_ref().unwrap());

        for flow in flows {
            let rec = TraceRecord::new(start_time, flow, None);
            trace.add_record(rec);
            self.remaining_flows += 1;
        }

        self.remaining_iterations -= 1;

        self.replayer = Replayer::new(trace);
    }

    fn request_nethint(&self) -> Events {
        match self.nethint_level {
            0 => Event::NetHintRequest(0, 0, NetHintVersion::V1).into(),
            1 => Event::NetHintRequest(0, 0, NetHintVersion::V1).into(),
            2 => Event::NetHintRequest(0, 0, NetHintVersion::V2).into(),
            _ => panic!("unexpected nethint_level: {}", self.nethint_level),
        }
    }
}

impl<'c> Application for AllReduceApp<'c> {
    type Output = Option<Duration>;
    fn on_event(&mut self, event: AppEvent) -> Events {
        if self.cluster.is_none() {
            // ask simulator for the NetHint
            match event.event {
                AppEventKind::AppStart => {
                    // app_id should be tagged by AppGroup, so leave 0 here
                    return self.request_nethint();
                }
                AppEventKind::NetHintResponse(_, _tenant_id, ref vc) => {
                    self.cluster = Some(Rc::new(vc.clone()));
                    // info!(
                    //     "nethint response: {}",
                    //     self.cluster.as_ref().unwrap().to_dot()
                    // );
                    // since we have the cluster, start and schedule the app again
                    self.allreduce(event.ts);
                    return self
                        .replayer
                        .on_event(AppEvent::new(event.ts, AppEventKind::AppStart));
                }
                _ => unreachable!(),
            }
        }

        if let AppEventKind::FlowComplete(ref flows) = &event.event {
            let fct_cur = flows.iter().map(|f| f.ts + f.dura.unwrap()).max();
            self.jct = self.jct.iter().cloned().chain(fct_cur).max();
            self.remaining_flows -= flows.len();

            if self.remaining_flows == 0 && self.remaining_iterations > 0 {
                if self.autotune.is_some() && self.autotune.unwrap() > 0 && self.remaining_iterations % self.autotune.unwrap() == 0 {
                    self.cluster = None;
                    return self.request_nethint();
                }
                self.allreduce(self.jct.unwrap());
                return self
                    .replayer
                    .on_event(AppEvent::new(event.ts, AppEventKind::AppStart));
            }
        }
        self.replayer.on_event(event)
    }
    fn answer(&mut self) -> Self::Output {
        self.jct
    }
}
