use std::rc::Rc;

use log::warn;

use crate::{
    app::{AppEvent, AppEventKind, Application},
    cluster::Topology,
    simulator::{Event, Events},
    Duration, Flow, Trace, TraceRecord,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundFlowPattern {
    Alltoall,
    PlinkProbe,
}

/// BackgroundFlow App creates a certain traffic pattern that will last for a certain time in a cluster.
/// It keeps generating flows with `msg_size` and stops when dur_ms has passed.
/// It also has a generic type T as the output type. The output is just a stub to fit in AppGroup<T>.
pub struct BackgroundFlowApp<T> {
    cluster: Rc<dyn Topology>,
    /// running for dur_ms milliseconds
    dur_ms: Duration,
    pattern: BackgroundFlowPattern,
    /// default msg_size is 1MB
    msg_size: usize,
    output_stub: T,

    // state
    stopped: bool,
    remaining_flows: usize,
}

impl<T> BackgroundFlowApp<T> {
    pub fn new(
        cluster: Rc<dyn Topology>,
        dur_ms: Duration,
        pattern: BackgroundFlowPattern,
        msg_size: Option<usize>,
        output_stub: T,
    ) -> Self {
        BackgroundFlowApp {
            cluster,
            dur_ms,
            pattern,
            msg_size: msg_size.unwrap_or(1_000_000),
            output_stub,
            stopped: false,
            remaining_flows: 0,
        }
    }

    fn on_event_alltoall(&mut self, event: AppEvent) -> Events {
        match event.event {
            AppEventKind::AppStart => {
                let mut trace = Trace::new();
                let n = self.cluster.num_hosts();
                let pnames: Vec<_> = (0..n)
                    .map(|i| {
                        let vname = format!("host_{}", i);
                        self.cluster.translate(&vname)
                    })
                    .collect();

                for i in 0..n {
                    let sname = &pnames[i];
                    for j in 0..n {
                        if i == j {
                            continue;
                        }
                        let dname = &pnames[j];
                        let flow = Flow::new(self.msg_size, sname, dname, None);
                        let rec = TraceRecord::new(0, flow, None);
                        trace.add_record(rec);
                    }
                }

                self.remaining_flows = trace.recs.len();
                Event::FlowArrive(trace.recs).into()
            }
            AppEventKind::FlowComplete(recs) => {
                self.remaining_flows -= recs.len();

                let cur_ts = recs
                    .iter()
                    .map(|r| r.ts + r.dura.unwrap())
                    .max()
                    .expect("no flow actually completed");

                if cur_ts > self.dur_ms * 2 * 1_000_000 {
                    warn!("background flow was expect to finish in {} ms, however it has been running for {} ms", self.dur_ms, cur_ts / 1_000_000);
                }
                assert!(!self.stopped || self.stopped && cur_ts >= self.dur_ms * 1_000_000);

                if cur_ts < self.dur_ms * 1_000_000 {
                    // start new flows
                    let mut new_flows = Trace::new();
                    for r in &recs {
                        let flow = r.flow.clone();
                        let rec = TraceRecord::new(cur_ts, flow, None);
                        new_flows.add_record(rec);
                    }

                    self.remaining_flows += recs.len();
                    Event::FlowArrive(new_flows.recs).into()
                } else {
                    self.stopped = true;
                    if self.remaining_flows == 0 {
                        (Event::AppFinish).into()
                    } else {
                        Events::new()
                    }
                }
            }
            AppEventKind::NetHintResponse(..) | AppEventKind::Notification(_) => {
                unreachable!();
            }
        }
    }

    fn on_event_plink_probe(&mut self, _event: AppEvent) -> Events {
        unimplemented!();
    }
}

impl<T: Clone> Application for BackgroundFlowApp<T> {
    type Output = T;

    fn on_event(&mut self, event: AppEvent) -> Events {
        match self.pattern {
            BackgroundFlowPattern::Alltoall => self.on_event_alltoall(event),
            BackgroundFlowPattern::PlinkProbe => self.on_event_plink_probe(event),
        }
    }

    fn answer(&mut self) -> Self::Output {
        self.output_stub.clone()
    }
}
