use std::rc::Rc;
use nethint::{
    app::{AppEvent, AppEventKind, Application, Replayer},
    cluster::Topology,
    simulator::{Event, Events},
    Duration, Timestamp, Trace, TraceRecord,
    hint::NetHintVersion,
};

use crate::{
    random_ring::RandomTree, topology_aware::TopologyAwareTree, rat::RatTree,
    RLAlgorithm, RLPolicy, JobSpec,
};

pub struct RLApp<'c> {
    job_spec: &'c JobSpec,
    cluster: Option<Rc<dyn Topology>>,
    replayer: Replayer,
    jct: Option<Duration>,
    seed: u64,
    allreduce_policy: RLPolicy,
    remaining_iterations: usize,
    remaining_flows: usize,
    nethint_level: usize,
    autotune: Option<usize>,
}

impl<'c> std::fmt::Debug for RLApp<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RLApp")
    }
}

impl<'c> RLApp<'c> {
    pub fn new(
        job_spec: &'c JobSpec,
        cluster: Option<Rc<dyn Topology>>,
        seed: u64,
        allreduce_policy: RLPolicy,
        nethint_level: usize,
        autotune: Option<usize>,
    ) -> Self {
        let trace = Trace::new();
        RLApp {
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
        self.rl_traffic(0);
    }

    pub fn rl_traffic(&mut self, start_time: Timestamp) {
        let mut trace = Trace::new();

        let mut allreduce_algorithm: Box<dyn RLAlgorithm> = match self.allreduce_policy {
            RLPolicy::Random => Box::new(RandomTree::new(self.seed)),
            RLPolicy::TopologyAware => Box::new(TopologyAwareTree::new(self.seed)),
            RLPolicy::RAT => Box::new(RatTree::new(self.seed)),
        };

        let flows =
            allreduce_algorithm.run_rl_traffic(self.job_spec.root_index, self.job_spec.buffer_size as u64, &**self.cluster.as_ref().unwrap());

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

impl<'c> Application for RLApp<'c> {
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
                    log::info!(
                        "nethint response: {}",
                        self.cluster.as_ref().unwrap().to_dot()
                    );
                    // since we have the cluster, start and schedule the app again
                    self.rl_traffic(event.ts);
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
                self.rl_traffic(self.jct.unwrap());
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
