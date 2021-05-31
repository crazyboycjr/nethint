use nethint::{
    app::{AppEvent, AppEventKind, Application, Replayer},
    cluster::Topology,
    hint::NetHintVersion,
    simulator::{Event, Events},
    Duration, Timestamp, Trace, TraceRecord,
};
use std::rc::Rc;

use crate::{
    random_ring::RandomTree, rat::RatTree, topology_aware::TopologyAwareTree, JobSpec, RLAlgorithm,
    RLPolicy,
};

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
        }
    }

    pub fn start(&mut self) {
        self.remaining_iterations = self.job_spec.num_iterations;
        self.rl_traffic(0);
    }

    pub fn rl_traffic(&mut self, start_time: Timestamp) {
        let mut trace = Trace::new();

        let mut rl_algorithm: Box<dyn RLAlgorithm> = match self.rl_policy {
            RLPolicy::Random => Box::new(RandomTree::new(self.seed, 1)),
            RLPolicy::TopologyAware => Box::new(TopologyAwareTree::new(self.seed, 1)),
            RLPolicy::RAT => Box::new(RatTree::new(self.seed)),
        };

        let flows = rl_algorithm.run_rl_traffic(
            self.job_spec.root_index,
            self.job_spec.buffer_size as u64,
            &**self.cluster.as_ref().unwrap(),
        );

        for flow in flows {
            let rec = TraceRecord::new(start_time, flow, None);
            trace.add_record(rec);
            self.remaining_flows += 1;
        }

        self.remaining_iterations -= 1;

        self.replayer = Replayer::new(trace);
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

    fn modify_root_index(&mut self, vc: Rc<dyn Topology>) {
        if !self.root_index_modifed {
            let max_node = (0..vc.num_hosts())
                .map(|i| {
                    let hostname = format!("host_{}", i);
                    let host_ix = vc.get_node_index(&hostname);
                    (i, vc[vc.get_uplink(host_ix)].bandwidth)
                })
                .max_by_key(|x| x.1);
            let orig = self.job_spec.root_index;
            if let Some(max_node) = max_node {
                self.job_spec.root_index = max_node.0;
            }
            self.root_index_modifed = true;
            log::info!(
                "root_index original: {}, modified to: {}",
                orig,
                self.job_spec.root_index
            );
        }
    }
}

impl Application for RLApp {
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
                    // reset the root_index
                    let vc = Rc::clone(self.cluster.as_ref().unwrap());
                    self.modify_root_index(vc);
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

            // add computation time here
            // self.jct += self.computation_time

            if self.remaining_flows == 0 && self.remaining_iterations > 0 {
                if self.autotune.is_some()
                    && self.autotune.unwrap() > 0
                    && self.remaining_iterations % self.autotune.unwrap() == 0
                {
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
