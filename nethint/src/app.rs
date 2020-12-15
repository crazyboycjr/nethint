use crate::simulator::Event;
use crate::{Trace, TraceRecord};

#[derive(Debug, Clone)]
pub enum AppEvent {
    /// On start, an application returns all flows it can start, whatever the timestamps are.
    AppStart,
    FlowComplete(Vec<TraceRecord>),
}
/// An Applicatoin can interact with the simulator based on the flow completion event it received.
/// It dynamically start new flows according to the finish and arrive event of some flows.
pub trait Application {
    fn on_event(&mut self, event: AppEvent) -> Event;
}

/// A Replayer is an application that takes a trace and replay the trace.
pub struct Replayer {
    trace: Trace,
    num_flows: usize,
    completed: usize,
}

impl Application for Replayer {
    fn on_event(&mut self, event: AppEvent) -> Event {
        match event {
            AppEvent::AppStart => Event::FlowArrive(self.trace.recs.clone()),
            AppEvent::FlowComplete(flows) => {
                self.completed += flows.len();
                if self.completed == self.num_flows {
                    Event::AppFinish
                } else {
                    Event::Continue
                }
            }
        }
    }
}

impl Replayer {
    pub fn new(trace: Trace) -> Replayer {
        let num_flows = trace.recs.len();
        Replayer {
            trace,
            num_flows,
            completed: 0,
        }
    }
}
