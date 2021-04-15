use nethint::{
    app::{AppEvent, AppEventKind, Application, Replayer},
    cluster::Topology,
    hint::NetHintVersion,
    simulator::{Event, Events},
    Duration, Timestamp, Trace, TraceRecord,
};
use std::rc::Rc;
use std::cmp;

use crate::{
    random_ring::RandomRingAllReduce, rat::RatAllReduce,
    topology_aware::TopologyAwareRingAllReduce, AllReduceAlgorithm, AllReducePolicy, JobSpec,
};

pub struct AllReduceApp<'c> {
    job_spec: &'c JobSpec,
    computation_speed: f64,
    cluster: Option<Rc<dyn Topology>>,
    replayer: Replayer,
    jct: Option<Duration>,
    fct: Option<Duration>,
    seed: u64,
    allreduce_policy: AllReducePolicy,
    remaining_iterations: usize,
    remaining_flows: usize,
    nethint_level: usize,
    autotune: Option<usize>,
    allreduce_algorithm: Option<Box<dyn AllReduceAlgorithm>>,
}

impl<'c> std::fmt::Debug for AllReduceApp<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AllReduceApp")
    }
}

impl<'c> AllReduceApp<'c> {
    pub fn new(
        job_spec: &'c JobSpec,
        computation_speed: f64,
        cluster: Option<Rc<dyn Topology>>,
        seed: u64,
        allreduce_policy: AllReducePolicy,
        nethint_level: usize,
        autotune: Option<usize>,
    ) -> Self {
        let trace = Trace::new();
        AllReduceApp {
            job_spec,
            computation_speed,
            cluster,
            replayer: Replayer::new(trace),
            jct: None,
            fct: None,
            seed,
            allreduce_policy,
            remaining_iterations: job_spec.num_iterations,
            remaining_flows: 0,
            nethint_level,
            autotune,
            allreduce_algorithm: None,
        }
    }

    pub fn start(&mut self) {
        self.remaining_iterations = self.job_spec.num_iterations;

        self.allreduce_algorithm = Some(self.new_allreduce_algorithm());

        self.allreduce(0);
    }

    fn new_allreduce_algorithm(&self) -> Box<dyn AllReduceAlgorithm> {
        match self.allreduce_policy {
            AllReducePolicy::Random => Box::new(RandomRingAllReduce::new(self.seed)),
            AllReducePolicy::TopologyAware => Box::new(TopologyAwareRingAllReduce::new(self.seed)),
            AllReducePolicy::RAT => Box::new(RatAllReduce::new()),
        }
    }

    pub fn allreduce(&mut self, start_time: Timestamp) {
        let mut trace = Trace::new();

        if self.allreduce_algorithm.is_none() {
            self.allreduce_algorithm = Some(self.new_allreduce_algorithm());
        }

        let flows = self.allreduce_algorithm.as_mut().unwrap().allreduce(
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
fn calc_job_computation_time(buffer_size: usize, k: f64) -> u64{
    let buffer_size_f64 = buffer_size as f64;
    let res = ((k)*(buffer_size_f64))  as u64;
    res
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
                    log::info!(
                        "nethint response: {}",
                        self.cluster.as_ref().unwrap().to_dot()
                    );
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
            self.fct = self.fct.iter().cloned().chain(fct_cur).max();
            self.jct = self.jct.iter().cloned().chain(fct_cur).max();
            // get max jct and coputation time
            let computation_time: u64 = calc_job_computation_time(self.job_spec.buffer_size, self.computation_speed);
            self.jct = Some(cmp::max(self.jct.unwrap(), self.fct.unwrap() + computation_time));
            
            self.remaining_flows -= flows.len();

            if self.remaining_flows == 0 && self.remaining_iterations > 0 {
                if self.autotune.is_some()
                    && self.autotune.unwrap() > 0
                    && self.remaining_iterations % self.autotune.unwrap() == 0
                {
                    self.cluster = None;
                    return self.request_nethint();
                }
                self.allreduce(self.jct.unwrap());
                return self
                    .replayer
                    .on_event(AppEvent::new(event.ts+self.jct.unwrap()-self.fct.unwrap(), AppEventKind::AppStart));
            }
        }
        self.replayer.on_event(event)
    }
    fn answer(&mut self) -> Self::Output {
        self.jct
    }
}