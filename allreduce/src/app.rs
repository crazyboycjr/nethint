use log::info;

use nethint::{
    app::{AppEvent, Application, Replayer},
    cluster::Topology,
    simulator::Event,
    Duration, Flow, Trace, TraceRecord,
};

use crate::{
  AllReducePolicy, AllReduceAlgorithm,
  random_ring::RandomRingAllReduce,
};

pub struct AllReduceApp<'c> {
    cluster: &'c dyn Topology,
    replayer: Replayer,
    jct: Option<Duration>,
    seed: u64,
    allreduce_policy: AllReducePolicy,
}

impl<'c> AllReduceApp<'c> {
    pub fn new(cluster: &'c dyn Topology, seed: u64, allreduce_policy: AllReducePolicy) -> Self {
        let trace = Trace::new();
        AllReduceApp {
            cluster,
            replayer: Replayer::new(trace),
            jct: None,
            seed,
            allreduce_policy,
        }
    }

    pub fn start(&mut self) {
        self.allreduce();
    }

    pub fn allreduce(&mut self) {
        let mut trace = Trace::new();

        let mut allreduce_algorithm: Box<dyn AllReduceAlgorithm> = match self.allreduce_policy {
          AllReducePolicy::Random => Box::new(RandomRingAllReduce::new(self.seed)),
          AllReducePolicy::TopologyAware => Box::new(RandomRingAllReduce::new(self.seed)),
        };

        let flows = allreduce_algorithm.allreduce(1000000, self.cluster);

        for flow in flows {
            let rec = TraceRecord::new(0, flow, None);
            trace.add_record(rec);
        }

        self.replayer = Replayer::new(trace);
    }
}

impl<'c> Application for AllReduceApp<'c> {
    type Output = Option<Duration>;
    fn on_event(&mut self, event: AppEvent) -> Event {
        if let AppEvent::FlowComplete(ref flows) = &event {
            let fct_cur = flows.iter().map(|f| f.ts + f.dura.unwrap()).max();
            self.jct = self.jct.iter().cloned().chain(fct_cur).max();
        }
        self.replayer.on_event(event)
    }
    fn answer(&mut self) -> Self::Output {
        self.jct
    }
}
