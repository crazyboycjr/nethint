use log::info;

use crate::JobSpec;

use nethint::{
    app::{AppEvent, Application, Replayer},
    cluster::Topology,
    simulator::Event,
    Duration, Flow, Trace, TraceRecord,
};

pub struct AllReduceApp<'c> {
    cluster: &'c dyn Topology,
    replayer: Replayer,
    jct: Option<Duration>,
    seed: u64,
}

impl<'c> AllReduceApp<'c> {
    pub fn new(cluster: &'c dyn Topology, seed: u64) -> Self {
        let trace = Trace::new();
        AllReduceApp {
            cluster,
            replayer: Replayer::new(trace),
            jct: None,
            seed,
        }
    }

    pub fn start(&mut self) {
        self.ringallreduce();
    }

    pub fn ringallreduce(&mut self) {
        let mut trace = Trace::new();
        use rand::prelude::SliceRandom;
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(self.seed);

        let mut alloced_hosts: Vec<usize> = (0..self.cluster.num_hosts()).into_iter().collect();
        alloced_hosts.shuffle(&mut rng);

        info!("{:?}", alloced_hosts);

        for i in 0..self.cluster.num_hosts() {
            let sender = format!("host_{}", alloced_hosts.get(i).unwrap());
            let receiver = format!(
                "host_{}",
                alloced_hosts
                    .get((i + 1) % self.cluster.num_hosts())
                    .unwrap()
            );
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
