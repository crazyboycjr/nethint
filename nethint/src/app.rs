use log::{debug, trace};

use crate::brain::TenantId;
use crate::{
    hint::NetHintV2,
    simulator::{Event, Events},
    Token,
};
use crate::{Timestamp, Trace, TraceRecord};

#[derive(Debug, Clone)]
pub enum AppEvent {
    /// On start, an application returns all flows it can start, whatever the timestamps are.
    AppStart,
    NetHintResponse(usize, TenantId, NetHintV2),
    FlowComplete(Vec<TraceRecord>),
    // Time up event triggered
    Notification(Token),
}

/// An Applicatoin can interact with the simulator based on the flow completion event it received.
/// It dynamically start new flows according to the finish and arrive event of some flows.
pub trait Application {
    type Output;
    fn on_event(&mut self, event: AppEvent) -> Events;
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
    fn on_event(&mut self, event: AppEvent) -> Events {
        match event {
            AppEvent::AppStart => Event::FlowArrive(self.trace.recs.clone()).into(),
            AppEvent::FlowComplete(mut flows) => {
                self.completed.recs.append(&mut flows);
                if self.completed.recs.len() == self.num_flows {
                    (Event::AppFinish).into()
                } else {
                    Events::new()
                }
            }
            AppEvent::NetHintResponse(..) | AppEvent::Notification(_) => Events::new(), // Replayer does not handle these events
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

pub trait AppGroupTokenCoding {
    fn encode(tenant_id: TenantId, app_id: usize) -> Token;
    fn decode(&self) -> (TenantId, usize);
}

impl AppGroupTokenCoding for Token {
    fn encode(tenant_id: TenantId, app_id: usize) -> Token {
        assert_eq!(
            tenant_id, app_id,
            "currently, AppGroupTokenCoding assumes tenant_id and app_id are the same"
        );
        assert!(
            tenant_id < 0x1 << 32,
            format!("invalid tenant_id {}", tenant_id)
        );
        Token(tenant_id << 32 | app_id)
    }
    fn decode(&self) -> (TenantId, usize) {
        (self.0 >> 32, self.0 & 0xffffffff)
    }
}

impl<'a, T> Application for AppGroup<'a, T>
where
    T: Clone,
{
    // a list of (app_id, App::Output)
    type Output = Vec<(usize, T)>;

    fn on_event(&mut self, event: AppEvent) -> Events {
        if self.output.len() == self.apps.len() {
            return (Event::AppFinish).into();
        }

        trace!("AppGroup receive an app_event {:?}", event);
        let events = match event {
            AppEvent::AppStart => {
                // start all apps, modify the start time of flow, gather all flows
                // register notification to start apps
                (0..self.apps.len())
                    .map(|app_id| {
                        let start_off = self.apps[app_id].0;
                        let token = Token::encode(app_id, app_id);
                        Event::RegisterTimer(start_off, token)
                    })
                    .collect()
            }
            AppEvent::FlowComplete(recs) => {
                // dispatch flows to different apps by looking at the token
                let mut flows = vec![vec![]; self.apps.len()];
                for r in recs {
                    let (_, app_id) = r.flow.token.unwrap().decode();
                    assert!(app_id < self.apps.len());
                    let mut f = r.clone();
                    f.ts -= self.apps[app_id].0; // f.ts -= start_off;
                    flows[app_id].push(f);
                }

                flows
                    .into_iter()
                    .enumerate()
                    .map(|(app_id, recs)| {
                        if !recs.is_empty() {
                            self.forward(app_id, AppEvent::FlowComplete(recs))
                        } else {
                            Events::new()
                        }
                    })
                    .flatten()
                    .collect()
            }
            AppEvent::NetHintResponse(app_id, tenant_id, vc) => {
                let app_event = AppEvent::NetHintResponse(app_id, tenant_id, vc);
                self.forward(app_id, app_event)
            }
            AppEvent::Notification(token) => {
                // now the timestamp is at self.apps[app_id].start_off
                let (_, app_id) = token.decode();
                self.forward(app_id, AppEvent::AppStart)
            }
        };

        if self.output.len() == self.apps.len() {
            (Event::AppFinish).into()
        } else {
            events
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

    fn forward(&mut self, app_id: usize, app_event: AppEvent) -> Events {
        let start_off = self.apps[app_id].0;

        self.apps[app_id]
            .1
            .on_event(app_event)
            .into_iter()
            .flat_map(|event| {
                match event {
                    Event::FlowArrive(mut recs) => {
                        recs.iter_mut().for_each(|r| {
                            r.ts += start_off;
                            assert!(
                                r.flow.token.is_none(),
                                "Currently AppGroup assumes the flow token is not used"
                            );
                            r.flow.token = Some(Token::encode(app_id, app_id));
                            // TODO(cjr): we can save the token and restore it transparently
                        });
                        Event::FlowArrive(recs).into()
                    }
                    Event::AppFinish => {
                        debug!("AppGroup, user app notifies AppFinish, app_id: {}", app_id);
                        let app = &mut self.apps[app_id].1;
                        self.output.push((app_id, app.answer()));
                        Events::new()
                    }
                    Event::NetHintRequest(inner_app_id, tenant_id) => {
                        assert_eq!(inner_app_id, 0);
                        Event::NetHintRequest(app_id, tenant_id).into()
                    }
                    Event::RegisterTimer(_dura, token) => {
                        panic!(
                            "impossible to receive timer registration from user app, token: {:?}",
                            token
                        );
                    }
                }
            })
            .collect()
    }
}
