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
    config::ProbeConfig, random_ring::RandomChain, rat::RatTree, topology_aware::TopologyAwareTree,
    JobSpec, RLAlgorithm, RLPolicy,
};

use utils::collector::OverheadCollector;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

pub struct RLApp {
    root_index_modifed: bool,
    job_spec: JobSpec,
    cluster: Option<Rc<dyn Topology>>,
    replayer: Replayer,
    jct: Option<Duration>,
    seed: u64,
    rl_policy: RLPolicy,
    remaining_iterations: usize,
    remaining_flows: usize,
    nethint_level: usize,
    autotune: Option<usize>,
    overhead_collector: OverheadCollector,
    // dynamic probing
    probe: ProbeConfig,
    auto_fallback: bool,
    alpha: Option<f64>,
    nhosts_to_acquire: usize,
    in_probing: bool,
    probing_start_time: Timestamp,
    background_flow_app: Option<BackgroundFlowApp<Option<Duration>>>,
    rl_algorithm: Option<Box<dyn RLAlgorithm>>,
    // partially sync
    partially_sync: bool,
    group_rng: Option<StdRng>,
}

impl std::fmt::Debug for RLApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RLApp")
    }
}

impl RLApp {
    pub fn new(
        job_spec: &JobSpec,
        cluster: Option<Rc<dyn Topology>>,
        seed: u64,
        rl_policy: RLPolicy,
        nethint_level: usize,
        autotune: Option<usize>,
        probe: ProbeConfig,
        auto_fallback: bool,
        alpha: Option<f64>,
        nhosts_to_acquire: usize,
        partially_sync: bool,
    ) -> Self {
        let trace = Trace::new();
        RLApp {
            root_index_modifed: false,
            job_spec: job_spec.clone(),
            cluster,
            replayer: Replayer::new(trace),
            jct: None,
            seed,
            rl_policy,
            remaining_iterations: job_spec.num_iterations,
            remaining_flows: 0,
            nethint_level,
            autotune,
            overhead_collector: Default::default(),
            probe,
            auto_fallback,
            alpha,
            nhosts_to_acquire,
            in_probing: false,
            probing_start_time: 0,
            background_flow_app: None,
            rl_algorithm: None,
            partially_sync,
            group_rng: if partially_sync {
                Some(SeedableRng::seed_from_u64(seed))
            } else {
                None
            },
        }
    }

    pub fn start(&mut self) {
        self.remaining_iterations = self.job_spec.num_iterations;
        self.rl_traffic(0);
    }

    fn estimate_iter(&self, group: Option<Vec<usize>>) -> f64 {
        let mut algorithm = RatTree::new();
        algorithm.estimate_iter(
            self.job_spec.root_index,
            group,
            self.job_spec.buffer_size as u64,
            Rc::clone(self.cluster.as_ref().unwrap()),
        )
    }

    fn estimate_adapt(&self) -> f64 {
        let n = self.job_spec.num_workers;
        if n <= 16 {
            1.0 * 1e-3
        } else if n <= 32 {
            10.0 * 1e-3
        } else if n <= 64 {
            30.0 * 1e-3
        } else {
            50.0 * 1e-3
        }
    }

    pub fn rl_traffic(&mut self, start_time: Timestamp) {
        let mut trace = Trace::new();

        let group = if self.partially_sync {
            // generate group members of (n-1)/2+1
            let n = self.job_spec.num_workers;
            let mut members: Vec<usize> = (0..n).collect();
            members.remove(self.job_spec.root_index);

            let (group, _) =
                members.partial_shuffle(self.group_rng.as_mut().unwrap(), (n - 1) / 2 + 1);
            let g: Vec<usize> = group.into();
            log::debug!("group members: {:?}", g);
            Some(g)
        } else {
            None
        };

        if self.rl_algorithm.is_none() {
            self.rl_algorithm = Some(match self.rl_policy {
                RLPolicy::Random => Box::new(RandomChain::new(self.seed, 1)),
                RLPolicy::TopologyAware => Box::new(TopologyAwareTree::new(self.seed, 1)),
                RLPolicy::Contraction => panic!("do not use Contraction algorithm"),
                RLPolicy::RAT => {
                    if self.auto_fallback {
                        let t_background = std::env::var("NETHINT_BACKGROUND_FLOW_PERIOD")
                            .expect("it should be set in simulator.rs, something is wrong")
                            .parse::<u64>()
                            .unwrap() as f64
                            / 1e9;
                        let alpha = self.alpha.unwrap();
                        let t_iter = self.estimate_iter(group.clone());
                        let t_adapt = self.estimate_adapt();
                        // let p = 0.1;
                        // let k = (t_adapt as f64 * (1.0 - p) / p / t_iter as f64).ceil();
                        let k = self.autotune.unwrap();
                        let t_period = t_adapt + k as f64 * t_iter + 150.0 * 1e-3; // + 150ms
                        log::error!(
                            "num_workers:{}, t_adapt: {}, t_iter: {}, k: {}, alpha: {}, t_background: {}",
                            self.job_spec.num_workers, t_adapt, t_iter, k, alpha, t_background
                        );
                        if t_period < alpha * t_background {
                            Box::new(RatTree::new())
                        } else {
                            Box::new(TopologyAwareTree::new(self.seed, 1))
                        }
                    } else {
                        Box::new(RatTree::new())
                    }
                }
            });
        }

        let flows = self.rl_algorithm.as_mut().unwrap().run_rl_traffic(
            self.job_spec.root_index,
            group,
            self.job_spec.buffer_size as u64,
            Rc::clone(self.cluster.as_ref().unwrap()),
        );

        for flow in flows {
            let rec = TraceRecord::new(start_time, flow, None);
            trace.add_record(rec);
            self.remaining_flows += 1;
        }

        // log::warn!(
        //     "{:?}, seed: {} remaining/total: {}/{}",
        //     self.rl_policy,
        //     self.seed,
        //     self.remaining_iterations,
        //     self.job_spec.num_iterations
        // );
        self.remaining_iterations -= 1;

        self.replayer = Replayer::new(trace);
    }

    // copied from src/allreduce/src/app.rs
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
        // COMMENT(cjr): here in simulation, we request hintv2 in any case so as to pick the root_index
        // with the node of maximal sending rate
        match self.nethint_level {
            0 => Event::NetHintRequest(0, 0, NetHintVersion::V2, 2).into(),
            1 => Event::NetHintRequest(0, 0, NetHintVersion::V2, 2).into(),
            2 => Event::NetHintRequest(0, 0, NetHintVersion::V2, 2).into(),
            _ => panic!("unexpected nethint_level: {}", self.nethint_level),
        }
    }

    fn adjust_root_index(&mut self, vc: Rc<dyn Topology>) {
        use nethint::cluster::helpers::get_up_bw;
        if !self.root_index_modifed {
            let max_node = (0..vc.num_hosts())
                .map(|i| (i, get_up_bw(&*vc, i)))
                .max_by_key(|x| x.1);
            let orig = self.job_spec.root_index;
            if let Some(max_node) = max_node {
                self.job_spec.root_index = max_node.0;
            }
            self.root_index_modifed = true;
            log::info!(
                "root_index original: {}, modified to: {}, outbound_bw: {}",
                orig,
                self.job_spec.root_index,
                get_up_bw(&*vc, self.job_spec.root_index),
            );
        }
    }
}

impl Application for RLApp {
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
                    // reset the root_index
                    let vc = Rc::clone(self.cluster.as_ref().unwrap());
                    self.adjust_root_index(vc);

                    let start = std::time::Instant::now();
                    // since we have the cluster, start and schedule the app again
                    self.rl_traffic(event.ts);
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

            // add computation time here
            // self.jct += self.computation_time

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
