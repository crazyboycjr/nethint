use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::rc::Rc;

use log::{trace, debug};

pub mod cluster;
use crate::cluster::{Cluster, Link, Route, Topology};

// nanoseconds
pub type Timestamp = u64;
pub type Duration = u64;

#[derive(Debug, Clone, Copy)]
pub enum Bandwidth {
    Kbps(f64),
    Mbps(f64),
    Gbps(f64),
}

impl Bandwidth {
    fn value(self) -> f64 {
        // to bit/s
        use Bandwidth::*;
        match self {
            Kbps(x) => x * 1e3,
            Mbps(x) => x * 1e6,
            Gbps(x) => x * 1e9,
        }
    }
}

impl std::cmp::PartialEq for Bandwidth {
    fn eq(&self, other: &Self) -> bool {
        self.value().eq(&other.value())
    }
}

impl Eq for Bandwidth {}

impl std::cmp::PartialOrd for Bandwidth {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value().partial_cmp(&other.value())
    }
}

impl std::cmp::Ord for Bandwidth {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value().partial_cmp(&other.value()).unwrap()
    }
}

impl std::ops::Div<f64> for Bandwidth {
    type Output = Self;
    fn div(self, rhs: f64) -> Self::Output {
        use Bandwidth::*;
        match self {
            Kbps(x) => Kbps(x / rhs),
            Mbps(x) => Mbps(x / rhs),
            Gbps(x) => Gbps(x / rhs),
        }
    }
}

impl std::ops::Sub for Bandwidth {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        use Bandwidth::*;
        let x = self.value() - rhs.value();
        match self {
            Kbps(_) => Kbps(x / 1e3),
            Mbps(_) => Mbps(x / 1e6),
            Gbps(_) => Gbps(x / 1e9),
        }
    }
}

/// The simulator driver API
pub trait Executor {
    fn run_with_trace(&mut self, trace: Trace) -> Trace;
    fn run_with_appliation(&mut self, app: Box<dyn Application>) -> Trace;
}

/// The flow-level simulator.
pub struct Simulator {
    cluster: Cluster,
    ts: Timestamp,
    state: NetState,
}

impl Simulator {
    pub fn new(cluster: Cluster) -> Self {
        Simulator {
            cluster,
            ts: 0,
            state: Default::default(),
        }
    }

    pub fn suspend(&mut self, path: &std::path::Path) {
        // dump all running states of the simulator
    }

    pub fn resume(&mut self, path: &std::path::Path) {
        // resume from the previous saved state
    }
}

impl Simulator {
    #[inline]
    fn calc_delta(l: &Link, fs: &Vec<FlowStateRef>) -> Bandwidth {
        let (num_active, consumed_bw) = fs.iter().fold((0, 0.0), |acc, f| {
            let f = f.borrow();
            (acc.0 + !f.converged as usize, acc.1 + f.speed)
        });
        (l.bandwidth - Bandwidth::Gbps(consumed_bw * 1e-9)) / num_active as f64
    }

    fn proceed(&mut self, ts_inc: Duration) -> Vec<TraceRecord> {
        // complete some flows
        let comp_flows = self.state.complete_flows(self.ts, ts_inc);

        // start new ready flows
        self.ts += ts_inc;
        self.state.emit_flows(self.ts, &self.cluster);

        comp_flows
    }

    fn min_max_fairness_converge(&mut self) {
        let mut converged = 0;
        let active_flows = self.state.running_flows.len();
        self.state.running_flows.iter().for_each(|f| {
            f.borrow_mut().converged = false;
            f.borrow_mut().speed = 0.0;
        });
        while converged < active_flows {
            let res = self.state.flows.iter().min_by_key(|(l, fs)| {
                assert!(!fs.is_empty());
                Self::calc_delta(l, fs)
            });

            let (l, fs) = res.expect("impossible");
            let speed_inc = Self::calc_delta(l, fs).value();

            for f in fs {
                if !f.borrow().converged {
                    f.borrow_mut().speed += speed_inc;
                    f.borrow_mut().converged = true;
                    converged += 1;
                }
            }

            for f in &self.state.running_flows {
                if !f.borrow().converged {
                    f.borrow_mut().speed += speed_inc;
                    // check if the link is converged
                }
            }
        }
    }

    fn min_max_fairness(&mut self) -> Vec<TraceRecord> {
        loop {
            self.min_max_fairness_converge();
            // all FlowStates are converged

            // find the first event
            let first_complete_flow = self
                .state
                .running_flows
                .iter()
                .min_by_key(|f| f.borrow().time_to_complete())
                .expect("");

            let ts_inc = {
                let first_complete_flow_time =
                    self.ts + first_complete_flow.borrow().time_to_complete();

                if !self.state.flow_bufs.is_empty() {
                    let first_ready_flow = self.state.flow_bufs.peek().unwrap();
                    let first_ready_flow_time = first_ready_flow.0.borrow().ts;
                    assert!(first_ready_flow_time > self.ts);
                    first_ready_flow_time.min(first_complete_flow_time)
                } else {
                    assert!(first_complete_flow_time > self.ts);
                    first_complete_flow_time
                }
            };

            let comp_flows = self.proceed(ts_inc);
            if !comp_flows.is_empty() {
                break comp_flows;
            }
        }
    }
}

impl Executor for Simulator {
    fn run_with_trace(&mut self, trace: Trace) -> Trace {
        let app = Box::new(Replayer::new(trace));
        self.run_with_appliation(app)
    }

    fn run_with_appliation(&mut self, mut app: Box<dyn Application>) -> Trace {
        // let's write some conceptual code
        let mut output = Trace::new();
        let mut event = app.on_event(AppEvent::AppStart);
        assert!(matches!(event, Event::FlowArrive(_)));
        loop {
            trace!("simulator: on event {:?}", event);
            event = match event {
                Event::FlowArrive(recs) => {
                    assert!(!recs.is_empty(), "No flow arrives.");
                    // 1. find path for each flow and add to current net state
                    for r in recs {
                        self.state.add_flow(r, &self.cluster, self.ts);
                    }
                    // 2. run min-max fairness to find the next completed flow
                    let comp_flows = self.min_max_fairness();
                    // 3. add completed flows to trace output
                    output.recs.append(&mut comp_flows.clone());

                    // 4. nofity the application with this flow
                    app.on_event(AppEvent::FlowComplete(comp_flows))
                }
                Event::AppFinish => {
                    break;
                }
                Event::Continue => {
                    let comp_flows = self.min_max_fairness();
                    app.on_event(AppEvent::FlowComplete(comp_flows))
                }
            }
        }

        output
    }
}

/// A flow Trace is a table of flow record.
#[derive(Debug, Clone)]
pub struct Trace {
    pub recs: Vec<TraceRecord>,
}

impl Trace {
    pub fn new() -> Self {
        Trace { recs: Vec::new() }
    }

    #[inline]
    pub fn add_record(&mut self, rec: TraceRecord) {
        self.recs.push(rec);
    }
}

#[derive(Debug, Clone)]
pub struct TraceRecord {
    /// The start timestamp of the flow.
    pub ts: Timestamp,
    pub flow: Flow,
    pub dura: Option<Duration>, // this is calculated by the simulator
}

impl TraceRecord {
    #[inline]
    pub fn new(ts: Timestamp, flow: Flow, dura: Option<Duration>) -> Self {
        TraceRecord { ts, flow, dura }
    }
}

#[derive(Debug, Clone)]
pub struct Flow {
    bytes: usize,
    src: String,
    dst: String,
    /// a optional tag for application use (e.g. identify the flow in application)
    token: Option<Token>,
}

impl Flow {
    #[inline]
    pub fn new(bytes: usize, src: &str, dst: &str, token: Option<Token>) -> Self {
        Flow {
            bytes,
            src: src.to_owned(),
            dst: dst.to_owned(),
            token,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(pub usize);

impl From<usize> for Token {
    fn from(val: usize) -> Token {
        Token(val)
    }
}

impl From<Token> for usize {
    fn from(val: Token) -> usize {
        val.0
    }
}

#[derive(Default)]
struct NetState {
    /// buffered flows, flow.ts > sim.ts
    flow_bufs: BinaryHeap<Reverse<FlowStateRef>>,
    // emitted flows
    running_flows: Vec<FlowStateRef>,
    // TODO(cjr): maybe change to BTreeMap later?
    flows: HashMap<Link, Vec<FlowStateRef>>,
}

impl NetState {
    #[inline]
    fn add_flow(&mut self, r: TraceRecord, cluster: &Cluster, sim_ts: Timestamp) {
        let route = cluster.resolve_route(&r.flow.src, &r.flow.dst);
        let fs = FlowState::new(r.ts, r.flow, route.clone());
        if r.ts > sim_ts {
            // add to buffered flows
            self.flow_bufs.push(Reverse(fs));
        } else {
            // add to current flow states, an invereted index
            self.running_flows.push(Rc::clone(&fs));
            for l in &route.path {
                self.flows
                    .entry(l.borrow().clone())
                    .or_insert(Vec::new())
                    .push(Rc::clone(&fs));
            }
        }
    }

    fn emit_flows(&mut self, sim_ts: Timestamp, cluster: &Cluster) {
        while let Some(f) = self.flow_bufs.pop() {
            let f = Rc::clone(&f.0);
            if sim_ts < f.borrow().ts {
                break;
            }
            assert_eq!(sim_ts, f.borrow().ts);
            self.add_flow(
                TraceRecord::new(f.borrow().ts, f.borrow().flow.clone(), None),
                cluster,
                sim_ts,
            );
        }
    }

    fn complete_flows(&mut self, sim_ts: Timestamp, ts_inc: Duration) -> Vec<TraceRecord> {
        let mut comp_flows = Vec::new();

        self.running_flows.iter_mut().for_each(|f| {
            let speed = f.borrow().speed;
            f.borrow_mut().bytes_sent += (speed * ts_inc as f64).round() as usize / 8;

            if f.borrow().completed() {
                comp_flows.push(TraceRecord::new(
                    f.borrow().ts,
                    f.borrow().flow.clone(),
                    Some(sim_ts + ts_inc - f.borrow().ts),
                ));
            }
        });

        self.running_flows.retain(|f| f.borrow().completed());

        for (_, fs) in self.flows.iter_mut() {
            fs.retain(|f| f.borrow().completed());
        }
        // filter all empty links
        self.flows.retain(|_, fs| !fs.is_empty());

        comp_flows
    }
}

type FlowStateRef = Rc<RefCell<FlowState>>;

/// A running flow
struct FlowState {
    /// flow start time, could be greater than sim_ts, read only
    ts: Timestamp,
    /// read only flow property
    flow: Flow,
    /// states, mutated by the simulator
    converged: bool,
    bytes_sent: usize,
    speed: f64, // bits/s
    route: Route,
}

impl FlowState {
    #[inline]
    fn new(ts: Timestamp, flow: Flow, route: Route) -> FlowStateRef {
        Rc::new(RefCell::new(FlowState {
            ts,
            flow,
            converged: false,
            bytes_sent: 0,
            speed: 0.0,
            route,
        }))
    }

    #[inline]
    fn time_to_complete(&self) -> Duration {
        assert!(self.speed > 0.0);
        let time_sec = (self.flow.bytes - self.bytes_sent) as f64 * 8.0 / self.speed;
        (time_sec * 1e9) as Duration
    }

    #[inline]
    fn completed(&self) -> bool {
        // TODO(cjr): check the precision of this condition
        self.bytes_sent >= self.flow.bytes
    }
}

impl std::cmp::PartialEq for FlowState {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts
    }
}

impl Eq for FlowState {}

impl std::cmp::PartialOrd for FlowState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.ts.partial_cmp(&other.ts)
    }
}

impl std::cmp::Ord for FlowState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts.cmp(&other.ts)
    }
}

#[derive(Debug, Clone)]
pub enum AppEvent {
    /// On start, an application returns all flows it can start, whatever the timestamps are.
    AppStart,
    FlowComplete(Vec<TraceRecord>),
}

#[derive(Debug, Clone)]
pub enum Event {
    /// Application notifies the simulator with the arrival of a set of flows.
    FlowArrive(Vec<TraceRecord>),
    /// The application has completed all flows and has no more flows to send. This event should
    /// appear after certain FlowComplete event.
    AppFinish,
    /// Application does not take any action.
    Continue,
}

/// An Applicatoin can interact with the simulator based on the flow completion event it received.
/// It dynamically start new flows according to the finish and arrive event of some flows.
pub trait Application {
    fn on_event(&mut self, event: AppEvent) -> Event;
}

/// A Replayer is an application that takes a trace and replay the trace.
struct Replayer {
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
    fn new(trace: Trace) -> Replayer {
        let num_flows = trace.recs.len();
        Replayer {
            trace,
            num_flows,
            completed: 0,
        }
    }
}
