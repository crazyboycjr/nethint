use nethint::{
    app::{AppEvent, Application, Replayer},
    cluster::Topology,
    simulator::Events,
    Duration, Trace, TraceRecord, Timestamp,
};

use crate::{
  AllReducePolicy, AllReduceAlgorithm,
  random_ring::RandomRingAllReduce,
  topology_aware::TopologyAwareRingAllReduce,
  JobSpec,
};

pub struct AllReduceApp<'c> {
    job_spec: &'c JobSpec,
    cluster: &'c dyn Topology,
    replayer: Replayer,
    jct: Option<Duration>,
    seed: u64,
    allreduce_policy: &'c AllReducePolicy,
    remaining_iterations: usize,
    remaining_flows: usize,
}

impl<'c> AllReduceApp<'c> {
    pub fn new(job_spec: &'c JobSpec, cluster: &'c dyn Topology, seed: u64, allreduce_policy: &'c AllReducePolicy) -> Self {
        let trace = Trace::new();
        AllReduceApp {
            job_spec,
            cluster,
            replayer: Replayer::new(trace),
            jct: None,
            seed,
            allreduce_policy,
            remaining_iterations: 0,
            remaining_flows: 0,
        }
    }

    pub fn start(&mut self) {
        self.remaining_iterations = self.job_spec.num_iterations;
        self.allreduce(0);
    }

    pub fn allreduce(&mut self, start_time : Timestamp) {
        let mut trace = Trace::new();

        let mut allreduce_algorithm: Box<dyn AllReduceAlgorithm> = match self.allreduce_policy {
          AllReducePolicy::Random => Box::new(RandomRingAllReduce::new(self.seed)),
          AllReducePolicy::TopologyAware => Box::new(TopologyAwareRingAllReduce::new(self.seed)),
        };

        let flows = allreduce_algorithm.allreduce(self.job_spec.buffer_size as u64, self.cluster);

        for flow in flows {
            let rec = TraceRecord::new(start_time, flow, None);
            trace.add_record(rec);
            self.remaining_flows += 1;
        }

        self.remaining_iterations -= 1;

        self.replayer = Replayer::new(trace);
    }
}

impl<'c> Application for AllReduceApp<'c> {
    type Output = Option<Duration>;
    fn on_event(&mut self, event: AppEvent) -> Events {
        if let AppEvent::FlowComplete(ref flows) = &event {
            let fct_cur = flows.iter().map(|f| f.ts + f.dura.unwrap()).max();
            self.jct = self.jct.iter().cloned().chain(fct_cur).max();
            self.remaining_flows -= flows.len();

            if self.remaining_flows == 0 && self.remaining_iterations > 0 {
                self.allreduce(self.jct.unwrap());
                return self.replayer.on_event(AppEvent::AppStart);
            }
        }
        self.replayer.on_event(event)
    }
    fn answer(&mut self) -> Self::Output {
        self.jct
    }
}
