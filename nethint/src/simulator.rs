use fnv::FnvBuildHasher;
use indexmap::IndexMap;
use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::rc::Rc;

use log::{debug, trace};

use crate::app::{AppEvent, Application, Replayer};
use crate::bandwidth::{Bandwidth, BandwidthTrait};
use crate::cluster::{Cluster, Link, Route, Topology};
use crate::{Duration, Flow, Timestamp, ToStdDuration, Trace, TraceRecord};

type HashMap<K, V> = IndexMap<K, V, FnvBuildHasher>;

/// The simulator driver API
pub trait Executor<'a> {
    fn run_with_trace(&mut self, trace: Trace) -> Trace;
    fn run_with_appliation<T>(&mut self, app: Box<dyn Application<Output = T> + 'a>) -> T;
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

    pub fn suspend(&mut self, _path: &std::path::Path) {
        // dump all running states of the simulator
    }

    pub fn resume(&mut self, _path: &std::path::Path) {
        // resume from the previous saved state
    }

    #[inline]
    fn calc_delta(l: &Link, fs: &[FlowStateRef]) -> Bandwidth {
        let (num_active, consumed_bw) = fs.iter().fold((0, 0.0), |acc, f| {
            let f = f.borrow();
            (acc.0 + !f.converged as usize, acc.1 + f.speed)
        });
        (l.bandwidth - (consumed_bw / 1e9).gbps()) / num_active as f64
    }

    fn proceed(&mut self, ts_inc: Duration) -> Vec<TraceRecord> {
        // complete some flows
        let comp_flows = self.state.complete_flows(self.ts, ts_inc);

        // start new ready flows
        self.ts += ts_inc;
        self.state.emit_ready_flows(self.ts);

        comp_flows
    }

    fn max_min_fairness_converge(&mut self) {
        let mut converged = 0;
        let active_flows = self.state.running_flows.len();
        self.state.running_flows.iter().for_each(|f| {
            f.borrow_mut().converged = false;
            f.borrow_mut().speed = 0.0;
        });
        while converged < active_flows {
            // find the bottleneck link
            let res = self
                .state
                .flows
                .iter()
                .filter(|(_, fs)| fs.iter().any(|f| !f.borrow().converged))
                .min_by_key(|(l, fs)| {
                    assert!(!fs.is_empty());
                    Self::calc_delta(l, fs)
                });

            let (l, fs) = res.expect("impossible");
            let speed_inc = Self::calc_delta(l, fs).val() as f64;

            // update and converge flows pass through the bottleneck link
            for f in fs {
                if !f.borrow().converged {
                    f.borrow_mut().speed += speed_inc;
                    f.borrow_mut().converged = true;
                    converged += 1;
                }
            }

            // increase the speed of all active flows
            for f in &self.state.running_flows {
                if !f.borrow().converged {
                    f.borrow_mut().speed += speed_inc;
                }
            }
        }
    }

    fn max_min_fairness(&mut self) -> Vec<TraceRecord> {
        loop {
            // compute a fair share of bandwidth allocation
            self.max_min_fairness_converge();
            trace!(
                "after max_min_fairness converged, ts: {:?}, running flows: {:#?}\nnumber of ready flows: {}",
                self.ts.to_dura(),
                self.state.running_flows,
                self.state.flow_bufs.len(),
            );
            // all FlowStates are converged

            // find the next flow to complete
            let first_complete_time = self
                .state
                .running_flows
                .iter()
                .map(|f| f.borrow().time_to_complete() + self.ts)
                .min();

            // find the next flow to start
            let first_ready_time = self.state.flow_bufs.peek().map(|f| f.0.borrow().ts);

            // get min from first_complete_time and first_ready_time, both could be None
            let ts_inc = first_complete_time
                .into_iter()
                .chain(first_ready_time)
                .min()
                .expect("running flows and ready flows are both empty")
                - self.ts;

            assert!(ts_inc > 0);

            // modify the network state to the time at ts + ts_inc
            let comp_flows = self.proceed(ts_inc);
            if !comp_flows.is_empty() {
                break comp_flows;
            }
        }
    }
}

impl<'a> Executor<'a> for Simulator {
    fn run_with_trace(&mut self, trace: Trace) -> Trace {
        let app = Box::new(Replayer::new(trace));
        self.run_with_appliation(app)
    }

    fn run_with_appliation<T>(&mut self, mut app: Box<dyn Application<Output = T> + 'a>) -> T {
        // let's write some conceptual code
        let start = std::time::Instant::now();
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
                    // 2. run max-min fairness to find the next completed flow
                    let comp_flows = self.max_min_fairness();
                    trace!(
                        "ts: {:?}, completed flows: {:?}",
                        self.ts.to_dura(),
                        comp_flows
                    );

                    // 3. nofity the application with this flow
                    app.on_event(AppEvent::FlowComplete(comp_flows))
                }
                Event::AppFinish => {
                    break;
                }
                Event::Continue => {
                    let comp_flows = self.max_min_fairness();
                    trace!(
                        "ts: {:?}, completed flows: {:?}",
                        self.ts.to_dura(),
                        comp_flows
                    );

                    app.on_event(AppEvent::FlowComplete(comp_flows))
                }
            }
        }

        let end = std::time::Instant::now();

        debug!("sim_time: {:?}", end - start);
        // output
        app.answer()
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
    resolve_route_time: std::time::Duration,
}

impl NetState {
    #[inline]
    fn emit_flow(&mut self, fs: FlowStateRef) {
        self.running_flows.push(Rc::clone(&fs));
        for l in &fs.borrow().route.path {
            self.flows
                .entry(l.clone())
                .or_insert_with(Vec::new)
                .push(Rc::clone(&fs));
        }
    }

    fn add_flow(&mut self, r: TraceRecord, cluster: &Cluster, sim_ts: Timestamp) {
        let start = std::time::Instant::now();
        let route = cluster.resolve_route(&r.flow.src, &r.flow.dst, None);
        let end = std::time::Instant::now();
        self.resolve_route_time += end - start;

        let fs = FlowState::new(r.ts, r.flow, route);
        if r.ts > sim_ts {
            // add to buffered flows
            self.flow_bufs.push(Reverse(fs));
        } else {
            // add to current flow states, an invereted index
            self.emit_flow(fs);
        }
    }

    fn emit_ready_flows(&mut self, sim_ts: Timestamp) {
        while let Some(f) = self.flow_bufs.peek() {
            let f = Rc::clone(&f.0);
            if sim_ts < f.borrow().ts {
                break;
            }
            self.flow_bufs.pop();
            assert_eq!(sim_ts, f.borrow().ts);
            self.emit_flow(f);
        }
    }

    fn complete_flows(&mut self, sim_ts: Timestamp, ts_inc: Duration) -> Vec<TraceRecord> {
        trace!(
            "complete_flows: sim_ts: {:?}, ts_inc: {:?}",
            sim_ts.to_dura(),
            ts_inc.to_dura()
        );
        let mut comp_flows = Vec::new();

        self.running_flows.iter_mut().for_each(|f| {
            let speed = f.borrow().speed;
            f.borrow_mut().bytes_sent += (speed / 1e9 * ts_inc as f64).round() as usize / 8;

            if f.borrow().completed() {
                comp_flows.push(TraceRecord::new(
                    f.borrow().ts,
                    f.borrow().flow.clone(),
                    Some(sim_ts + ts_inc - f.borrow().ts),
                ));
            }
        });

        self.running_flows.retain(|f| !f.borrow().completed());

        for (_, fs) in self.flows.iter_mut() {
            fs.retain(|f| !f.borrow().completed());
        }
        // filter all empty links
        self.flows.retain(|_, fs| !fs.is_empty());

        comp_flows
    }
}

type FlowStateRef = Rc<RefCell<FlowState>>;

/// A running flow
#[derive(Debug)]
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
        (time_sec * 1e9).ceil() as Duration
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
pub enum Event {
    /// Application notifies the simulator with the arrival of a set of flows.
    FlowArrive(Vec<TraceRecord>),
    /// The application has completed all flows and has no more flows to send. This event should
    /// appear after certain FlowComplete event.
    AppFinish,
    /// Application does not take any action.
    Continue,
}
