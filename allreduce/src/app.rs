use nethint::{
  app::{AppEvent, Application, Replayer},
  cluster::{Cluster, Topology},
  simulator::{Event, Executor, Simulator},
  Duration, Flow, Trace, TraceRecord,
};

pub struct AllReduceApp<'c> {
  cluster: &'c dyn Topology,
  replayer: Replayer,
  jct: Option<Duration>,
}

impl<'c> AllReduceApp<'c> {
  pub fn new(
      cluster: &'c dyn Topology,
  ) -> Self {
      let trace = Trace::new();
      AllReduceApp {
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
    let server1 = format!("host_{}", 0);
    let server2 = format!("host_{}", 1);
    let flow = Flow::new(1000000, &server1, &server2, None);
    let rec = TraceRecord::new(0, flow, None);
    trace.add_record(rec);

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

pub fn run_allreduce(
  cluster: &Cluster,
) -> Option<Duration> {
  let mut simulator = Simulator::new(cluster.clone());

  let mut app = Box::new(AllReduceApp::new(
      cluster,
  ));
  app.start();

  simulator.run_with_appliation(app)
}
