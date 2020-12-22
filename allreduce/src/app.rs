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
    
    // for simplicity, let's construct a ring based on server id
    let num_hosts = self.cluster.num_hosts();
    for i in 0..num_hosts {
        let sender = format!("host_{}", i);
        let receiver = format!("host_{}", (i + 1) % num_hosts);
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
