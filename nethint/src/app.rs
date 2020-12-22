use crate::{hint::NetHintV2, simulator::Event};
use crate::{Timestamp, Trace, TraceRecord};
use crate::brain::TenantId;

#[derive(Debug, Clone)]
pub enum AppEvent {
    /// On start, an application returns all flows it can start, whatever the timestamps are.
    AppStart,
    NetHintResponse(usize, TenantId, NetHintV2),
    FlowComplete(Vec<TraceRecord>),
}

/// An Applicatoin can interact with the simulator based on the flow completion event it received.
/// It dynamically start new flows according to the finish and arrive event of some flows.
pub trait Application {
    type Output;
    fn on_event(&mut self, event: AppEvent) -> Event;
    fn answer(&mut self) -> Self::Output;
}

/// A Replayer is an application that takes a trace and replay the trace.
#[derive(Debug)]
pub struct Replayer {
    trace: Trace,
    num_flows: usize,
    completed: Trace,
}

impl Application for Replayer {
    type Output = Trace;
    fn on_event(&mut self, event: AppEvent) -> Event {
        match event {
            AppEvent::AppStart => Event::FlowArrive(self.trace.recs.clone()),
            AppEvent::FlowComplete(mut flows) => {
                self.completed.recs.append(&mut flows);
                if self.completed.recs.len() == self.num_flows {
                    Event::AppFinish
                } else {
                    Event::Continue
                }
            }
            AppEvent::NetHintResponse(_, _, _) => Event::Continue,
        }
    }
    fn answer(&mut self) -> Self::Output {
        self.completed.clone()
    }
}

impl Replayer {
    pub fn new(trace: Trace) -> Self {
        let num_flows = trace.recs.len();
        Replayer {
            trace,
            num_flows,
            completed: Default::default(),
        }
    }
}

/// AppGroup is an application created by combining multiple applications started at different timestamps.
/// It marks flows from each individual application.
/// It works like a proxy, intercepting, modifying, and forwarding simulator events to and from corresponding apps.
#[derive(Default)]
pub struct AppGroup<'a, T> {
    // Vec<(start time offset, application)>
    apps: Vec<(Timestamp, Box<dyn Application<Output = T> + 'a>)>,
    output: Vec<(usize, T)>,
}

impl<'a, T> Application for AppGroup<'a, T>
where
    T: Clone,
{
    // a list of (app_id, App::Output)
    type Output = Vec<(usize, T)>;

    fn on_event(&mut self, event: AppEvent) -> Event {
        if self.output.len() == self.apps.len() {
            return Event::AppFinish;
        }

        let mut new_flows = Vec::new();
        match event {
            AppEvent::AppStart => {
                // start all apps, modify the start time of flow, gather all flows
                // xs >>= f = concat $ map f xs
                // (>>=) :: m a -> (a -> m b) -> m b
                // and_then
                // concat a set of Event::FlowArraive 
                for app_id in 0..self.apps.len() {
                    self.forward(app_id, AppEvent::AppStart, &mut new_flows);
                }
            }
            AppEvent::FlowComplete(recs) => {
                // dispatch flows to different apps by looking at the token
                let mut flows = vec![vec![]; self.apps.len()];
                for r in recs {
                    let app_id: usize = r.flow.token.unwrap().into();
                    assert!(app_id < self.apps.len());
                    let mut f = r.clone();
                    f.ts -= self.apps[app_id].0; // f.ts -= start_off;
                    flows[app_id].push(f);
                }
                for (app_id, recs) in flows.into_iter().enumerate() {
                    if !recs.is_empty() {
                        self.forward(app_id, AppEvent::FlowComplete(recs), &mut new_flows);
                    }
                }
            }
            AppEvent::NetHintResponse(app_id, tenant_id, vc) => {
                let app_event = AppEvent::NetHintResponse(app_id, tenant_id, vc);
                self.forward(app_id, app_event, &mut new_flows);
            }
        }

        if self.apps.len() == self.output.len() {
            Event::AppFinish
        } else if new_flows.is_empty() {
            Event::Continue
        } else {
            Event::FlowArrive(new_flows)
        }
    }

    fn answer(&mut self) -> Self::Output {
        // it may not be in its original order
        self.output.clone()
    }
}

impl<'a, T> AppGroup<'a, T>
where
    T: Clone,
{
    pub fn new() -> Self {
        AppGroup {
            apps: Default::default(),
            output: Default::default(),
        }
    }

    pub fn add(
        &mut self,
        start_ts: Timestamp,
        app: Box<dyn Application<Output = T> + 'a>,
    ) -> usize {
        let app_id = self.apps.len();
        self.apps.push((start_ts, app));
        app_id
    }

    fn forward(&mut self, app_id: usize, app_event: AppEvent, new_flows: &mut Vec<TraceRecord>) {
        let (start_off, app) = &mut self.apps[app_id];
        match app.on_event(app_event) {
            Event::FlowArrive(mut recs) => {
                recs.iter_mut().for_each(|r| {
                    r.ts += *start_off;
                    assert!(
                        r.flow.token.is_none(),
                        "Currently AppGroup assumes the flow token is not used"
                    );
                    r.flow.token = Some(app_id.into());
                    // TODO(cjr): we can save the token and restore it transparently
                });
                new_flows.append(&mut recs);
            }
            Event::AppFinish => {
                self.output.push((app_id, app.answer()));
            }
            Event::Continue => {}
            Event::NetHintRequest(app_id, tenant_id) => {
                assert_eq!(app_id, 0);
                Event::NetHintRequest(app_id, tenant_id)
            }
        }
    }
}
