use log::{debug, info};

use crate::{
  JobSpec,
};

use nethint::{
  app::{AppEvent, Application, Replayer},
  cluster::{Cluster, Topology},
  simulator::{Event, Executor, Simulator},
  Duration, Flow, Trace, TraceRecord,
};

pub struct AllReduceApp<'c> {
  job_spec: &'c JobSpec,
  cluster: &'c dyn Topology,
  replayer: Replayer,
  jct: Option<Duration>,
}

impl<'c> AllReduceApp<'c> {
  pub fn new(
      job_spec: &'c JobSpec,
      cluster: &'c dyn Topology,
  ) -> Self {
      let trace = Trace::new();
      AllReduceApp {
          job_spec,
          cluster,
          replayer: Replayer::new(trace),
          jct: None,
      }
  }

  pub fn start(&mut self) {
    self.ringallreduce();
  }

  pub fn ringallreduce(&mut self) {
    let mut trace = Trace::new();
    use rand::prelude::SliceRandom;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(0);

    let mut alloced_hosts:Vec<usize> = (0..self.cluster.num_hosts())
          .into_iter().collect();
    alloced_hosts.shuffle(&mut rng);

    info! ("{:?}", alloced_hosts);

    for i in 0..self.cluster.num_hosts(){
        let sender = format!("host_{}", alloced_hosts.get(i).unwrap());
        let receiver = format!("host_{}", alloced_hosts.get((i + 1) % self.cluster.num_hosts()).unwrap());
        let flow = Flow::new(1000000, &sender, &receiver, None);
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
