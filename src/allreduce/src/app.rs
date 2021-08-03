use nethint::{
    app::{AppEvent, AppEventKind, Application, Replayer},
    background_flow::{BackgroundFlowApp, BackgroundFlowPattern},
    cluster::Topology,
    hint::NetHintVersion,
    simulator::{Event, Events},
    Duration, Timestamp, Trace, TraceRecord,
};
use std::convert::TryInto;
use std::rc::Rc;

use crate::{
    config::ProbeConfig, random_ring::RandomRingAllReduce, rat::RatAllReduce,
    topology_aware::TopologyAwareRingAllReduce, AllReduceAlgorithm, AllReducePolicy, JobSpec,
};

use utils::collector::OverheadCollector;

pub struct AllReduceApp<'c> {
    job_spec: &'c JobSpec,
    cluster: Option<Rc<dyn Topology>>,
    replayer: Replayer,
    computation_time: Duration,
    iteration_start: Timestamp,
    jct: Option<Duration>,
    seed: u64,
    allreduce_policy: AllReducePolicy,
    remaining_iterations: usize,
    remaining_flows: usize,
    nethint_level: usize,
    autotune: Option<usize>,
    allreduce_algorithm: Option<Box<dyn AllReduceAlgorithm>>,
    overhead_collector: OverheadCollector,
    // dynamic probing
    probe: ProbeConfig,
    nhosts_to_acquire: usize,
    in_probing: bool,
    probing_start_time: Timestamp,
    background_flow_app: Option<BackgroundFlowApp<Option<Duration>>>,
}

impl<'c> std::fmt::Debug for AllReduceApp<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AllReduceApp")
    }
}

impl<'c> AllReduceApp<'c> {
    pub fn new(
        job_spec: &'c JobSpec,
        computation_speed: Option<f64>,
        cluster: Option<Rc<dyn Topology>>,
        seed: u64,
        allreduce_policy: AllReducePolicy,
        nethint_level: usize,
        autotune: Option<usize>,
        probe: ProbeConfig,
        nhosts_to_acquire: usize,
    ) -> Self {
        let trace = Trace::new();
        let computation_time =
            calc_job_computation_time(job_spec.buffer_size, computation_speed.unwrap_or(0.));
        AllReduceApp {
            job_spec,
            cluster,
            replayer: Replayer::new(trace),
            computation_time,
            iteration_start: 0,
            jct: None,
            seed,
            allreduce_policy,
            remaining_iterations: job_spec.num_iterations,
            remaining_flows: 0,
            nethint_level,
            autotune,
            allreduce_algorithm: None,
            overhead_collector: Default::default(),
            probe,
            nhosts_to_acquire,
            in_probing: false,
            probing_start_time: 0,
            background_flow_app: None,
        }
    }

    pub fn start(&mut self) {
        self.remaining_iterations = self.job_spec.num_iterations;

        self.allreduce_algorithm = Some(self.new_allreduce_algorithm());

        self.allreduce(0);
    }

    fn new_allreduce_algorithm(&self) -> Box<dyn AllReduceAlgorithm> {
        match self.allreduce_policy {
            AllReducePolicy::Random => Box::new(RandomRingAllReduce::new(self.seed, 1)),
            AllReducePolicy::TopologyAware => {
                Box::new(TopologyAwareRingAllReduce::new(self.seed, 1))
            }
            AllReducePolicy::RAT => Box::new(RatAllReduce::new(self.job_spec.num_workers)),
        }
    }

    pub fn allreduce(&mut self, start_time: Timestamp) {
        self.iteration_start = start_time;

        let mut trace = Trace::new();

        if self.allreduce_algorithm.is_none() {
            self.allreduce_algorithm = Some(self.new_allreduce_algorithm());
        }

        let flows = self.allreduce_algorithm.as_mut().unwrap().allreduce(
            self.job_spec.buffer_size as u64,
            Rc::clone(self.cluster.as_ref().unwrap()),
        );

        for flow in flows {
            let rec = TraceRecord::new(start_time, flow, None);
            trace.add_record(rec);
            self.remaining_flows += 1;
        }

        self.remaining_iterations -= 1;

        self.replayer = Replayer::new(trace);
    }

    fn start_probe(&mut self, now: Timestamp) -> Events {
        assert!(self.probe.enable);
        assert!(!self.in_probing);

        self.in_probing = true;
        self.probing_start_time = now;

        let dur_ms = (self.nhosts_to_acquire as u64 * self.probe.round_ms) as _;
        self.background_flow_app = Some(BackgroundFlowApp::new(
            self.nhosts_to_acquire,
            dur_ms,
            BackgroundFlowPattern::PlinkProbe,
            Some(100_000_000), // 8ms on 100G
            None,
        ));

        let app_event = AppEvent {
            ts: 0,
            event: AppEventKind::AppStart,
        };
        self.background_flow_app
            .as_mut()
            .unwrap()
            .on_event(app_event)
    }

    fn request_nethint(&self) -> Events {
        match self.nethint_level {
            0 => Event::NetHintRequest(0, 0, NetHintVersion::V1, 1).into(),
            1 => Event::NetHintRequest(0, 0, NetHintVersion::V1, 1).into(),
            2 => Event::NetHintRequest(0, 0, NetHintVersion::V2, 1).into(),
            _ => panic!("unexpected nethint_level: {}", self.nethint_level),
        }
    }
}

//computation time for allreuce job
//para: job_size and constant: k
fn calc_job_computation_time(buffer_size: usize, k: f64) -> u64 {
    let buffer_size_f64 = buffer_size as f64;
    let res = ((k) * (buffer_size_f64)) as u64;
    res
}

impl<'c> Application for AllReduceApp<'c> {
    type Output = Option<Duration>;
    fn on_event(&mut self, event: AppEvent) -> Events {
        if self.in_probing {
            let mut event2 = event.clone();
            event2.ts -= self.probing_start_time;

            let events = self.background_flow_app.as_mut().unwrap().on_event(event2);
            log::trace!("AllreduceApp, background flow events: {:?}", events);

            let app_events: Events = events
                .into_iter()
                .flat_map(|sim_event| match sim_event {
                    Event::AppFinish => {
                        self.in_probing = false;
                        self.probing_start_time = 0;
                        Events::new()
                    }
                    Event::FlowArrive(mut flows) => {
                        for f in &mut flows {
                            f.ts += self.probing_start_time;
                        }
                        Event::FlowArrive(flows).into()
                    }
                    Event::NetHintRequest(..) | Event::UserRegisterTimer(..) => sim_event.into(),
                    Event::AdapterRegisterTimer(..) => panic!("now allowed"),
                })
                .collect();

            if !self.in_probing {
                assert!(app_events.is_empty());
                // request nethint
                self.cluster = None;
                return self.request_nethint();
            } else {
                return app_events;
            }
        }

        if self.cluster.is_none() {
            // ask simulator for the NetHint
            match event.event {
                AppEventKind::AppStart => {
                    // app_id should be tagged by AppGroup, so leave 0 here
                    if self.probe.enable {
                        return self.start_probe(event.ts);
                    } else {
                        return self.request_nethint();
                    }
                }
                AppEventKind::NetHintResponse(_, _tenant_id, ref vc) => {
                    self.cluster = Some(Rc::new(vc.clone()));
                    log::info!(
                        "nethint response: {}",
                        self.cluster.as_ref().unwrap().to_dot()
                    );
                    // since we have the cluster, start and schedule the app again
                    let start_time = self.iteration_start
                        + self.computation_time.max(event.ts - self.iteration_start);

                    let start = std::time::Instant::now();
                    self.allreduce(start_time);
                    let end = std::time::Instant::now();
                    let computing_delay = end - start;
                    // print controller overhead to job scale
                    self.overhead_collector
                        .collect(computing_delay, self.job_spec.num_workers);

                    log::info!("computing_delay: {:?}", computing_delay);
                    return Event::UserRegisterTimer(
                        computing_delay.as_nanos().try_into().unwrap(),
                        None,
                    )
                    .into();
                }
                _ => unreachable!(),
            }
        }

        if let AppEventKind::UserNotification(ref token) = &event.event {
            assert!(token.is_none());

            return self
                .replayer
                .on_event(AppEvent::new(event.ts, AppEventKind::AppStart));
        }

        if let AppEventKind::FlowComplete(ref flows) = &event.event {
            let fct_cur = flows.iter().map(|f| f.ts + f.dura.unwrap()).max();
            self.jct = self.jct.iter().cloned().chain(fct_cur).max();

            self.remaining_flows -= flows.len();

            if self.remaining_flows == 0 && self.remaining_iterations > 0 {
                if self.autotune.is_some()
                    && self.autotune.unwrap() > 0
                    && self.remaining_iterations % self.autotune.unwrap() == 0
                {
                    if self.probe.enable {
                        return self.start_probe(event.ts);
                    } else {
                        self.cluster = None;
                        return self.request_nethint();
                    }
                }
                self.allreduce(
                    self.iteration_start
                        + self
                            .computation_time
                            .max(self.jct.unwrap_or(0) - self.iteration_start),
                );
                return self
                    .replayer
                    .on_event(AppEvent::new(event.ts, AppEventKind::AppStart));
            }
        }
        self.replayer.on_event(event)
    }

    fn answer(&mut self) -> Self::Output {
        self.jct
            .map(|x| self.iteration_start + self.computation_time.max(x - self.iteration_start))
    }
}
