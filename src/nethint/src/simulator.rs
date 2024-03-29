use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::rc::Rc;
use std::{cmp::Reverse, fmt::Debug};

use fnv::FnvBuildHasher;
use indexmap::IndexMap;
use log::{debug, trace};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::{smallvec, SmallVec};
use thiserror::Error;

use crate::{
    app::{AppEvent, AppEventKind, Application, Replayer},
    background_flow_hard::{search_zipf_exp, BackgroundFlowHard},
    bandwidth::{self, Bandwidth, BandwidthTrait},
    brain::{Brain, TenantId},
    cluster::{Cluster, Link, LinkIx, Route, RouteHint, Topology},
    hint::{Estimator, NetHintVersion, SimpleEstimator},
    timer::{OnceTimer, PoissonTimer, RepeatTimer, Timer, TimerKind},
};
use crate::{
    Duration, FairnessModel, Flow, SharingMode, Timestamp, ToStdDuration, Token, Trace,
    TraceRecord, TIMER_ID,
};

type HashMap<K, V> = IndexMap<K, V, FnvBuildHasher>;
type HashMapValues<'a, K, V> = indexmap::map::Values<'a, K, V>;
type HashMapValuesMut<'a, K, V> = indexmap::map::ValuesMut<'a, K, V>;

pub const LOOPBACK_SPEED_GBPS: u64 = 400; // 400Gbps
pub const SAMPLE_INTERVAL_NS: u64 = 100_000_000; // 100ms

/// The simulator driver API
pub trait Executor<'a> {
    fn run_with_trace(&mut self, trace: Trace) -> Trace;
    fn run_with_application<T>(&mut self, app: Box<dyn Application<Output = T> + 'a>) -> T;
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SimulatorSetting {
    #[serde(rename = "nethint")]
    pub enable_nethint: bool,
    pub sample_interval_ns: Duration,
    #[serde(serialize_with = "serialize_bandwidth")]
    #[serde(deserialize_with = "deserialize_bandwidth")]
    pub loopback_speed: Bandwidth,
    pub fairness: FairnessModel,
    /// emulate background flow by subtracting a bandwidth to each link
    /// note that the remaining bandwidth must not smaller than link_bw / current_tenants
    pub background_flow_hard: BackgroundFlowHard,
}

impl Default for SimulatorSetting {
    fn default() -> Self {
        Self {
            fairness: FairnessModel::default(),
            enable_nethint: false,
            sample_interval_ns: SAMPLE_INTERVAL_NS,
            loopback_speed: LOOPBACK_SPEED_GBPS.gbps(),
            background_flow_hard: Default::default(),
        }
    }
}

fn serialize_bandwidth<S>(bw: &Bandwidth, se: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = bw.to_string();
    s.serialize(se)
}

fn deserialize_bandwidth<'de, D>(de: D) -> Result<Bandwidth, D::Error>
where
    D: Deserializer<'de>,
{
    let f: f64 = Deserialize::deserialize(de)?;
    Ok(f.gbps())
}

#[derive(Debug, Clone)]
pub struct SimulatorBuilder {
    cluster: Option<Cluster>,
    brain: Option<Rc<RefCell<Brain>>>,
    setting: SimulatorSetting,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("you must also set brain to enable nethint")]
    EmptyBrain,
    #[error("at least one of cluster of brain must be set")]
    EmptyClusterOrBrain,
}

impl Default for SimulatorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SimulatorBuilder {
    pub fn new() -> Self {
        SimulatorBuilder {
            cluster: None,
            brain: None,
            setting: Default::default(),
        }
    }

    pub fn with_setting(&mut self, setting: SimulatorSetting) -> &mut Self {
        self.setting = setting;
        self
    }

    pub fn cluster(&mut self, cluster: Cluster) -> &mut Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn enable_nethint(&mut self, enable: bool) -> &mut Self {
        self.setting.enable_nethint = enable;
        self
    }

    pub fn brain(&mut self, brain: Rc<RefCell<Brain>>) -> &mut Self {
        self.brain = Some(brain);
        self
    }

    pub fn fairness(&mut self, fairness: FairnessModel) -> &mut Self {
        self.setting.fairness = fairness;
        self
    }

    pub fn sample_interval_ns(&mut self, sample_interval_ns: Duration) -> &mut Self {
        self.setting.sample_interval_ns = sample_interval_ns;
        self
    }

    pub fn loopback_speed(&mut self, loopback_speed: Bandwidth) -> &mut Self {
        self.setting.loopback_speed = loopback_speed;
        self
    }

    pub fn build(&mut self) -> Result<Simulator, Error> {
        if self.setting.enable_nethint && self.brain.is_none() {
            return Err(Error::EmptyBrain);
        }
        if self.cluster.is_none() && self.brain.is_none() {
            return Err(Error::EmptyClusterOrBrain);
        }

        let mut timers = BinaryHeap::<Box<dyn Timer>>::new();
        if self.setting.background_flow_hard.enable {
            // This is a dirty hack to let the simulated apps can get background_flow changing period easily
            let freq = self.setting.background_flow_hard.frequency_ns;
            std::env::set_var("NETHINT_BACKGROUND_FLOW_PERIOD", freq.to_string());
            timers.push(Box::new(PoissonTimer::new(
                freq,
                freq as f64,
            )));
        }

        if self.setting.background_flow_hard.enable {
            self.setting.background_flow_hard.zipf_exp = search_zipf_exp(
                self.setting.background_flow_hard.amplitude,
                self.setting.background_flow_hard.average_load,
            );
        }

        let simulator = if self.setting.enable_nethint {
            let brain = self.brain.as_ref().unwrap();
            let estimator = Box::new(SimpleEstimator::new(
                Rc::clone(brain),
                self.setting.sample_interval_ns,
            ));
            timers.push(Box::new(RepeatTimer::new(
                self.setting.sample_interval_ns,
                self.setting.sample_interval_ns,
            )) as Box<dyn Timer>);
            let mut state = NetState::default();
            state.brain = Some(Rc::clone(brain));
            state.fairness = self.setting.fairness;
            Simulator {
                cluster: None,
                ts: 0,
                state,
                timers,
                setting: self.setting,
                estimator: Some(estimator),
            }
        } else {
            Simulator {
                cluster: None,
                ts: 0,
                state: NetState::default(),
                timers,
                setting: self.setting,
                estimator: None,
            }
        };

        Ok(simulator)
    }
}

/// The flow-level simulator.
pub struct Simulator {
    cluster: Option<Cluster>,
    ts: Timestamp,
    state: NetState,
    timers: BinaryHeap<Box<dyn Timer>>,
    // setting
    setting: SimulatorSetting,
    estimator: Option<Box<dyn Estimator>>,
}

macro_rules! calc_delta_on_group {
    ($total_bw:expr, $m:expr) => {{
        let mut consumed_bw = 0.0;
        let mut num_active_objects = 0;
        let mut num_active_flows = Vec::new();
        let mut min_inc_to_max_rate = f64::MAX;
        for fs in $m.values() {
            let mut active_flows_per_object = 0;
            for f in fs {
                let f = f.borrow();
                consumed_bw += f.speed;
                if !f.converged {
                    active_flows_per_object += 1;
                    assert!(f.speed < f.max_rate.val() as f64 + 10.0, "flow: {:?}", f);
                    min_inc_to_max_rate =
                        min_inc_to_max_rate.min(f.max_rate.val() as f64 - f.speed);
                }
            }

            num_active_objects += (active_flows_per_object > 0) as usize;
            num_active_flows.push(active_flows_per_object);
        }

        assert_ne!(num_active_objects, 0);

        let bw_inc_per_object = if $total_bw < (consumed_bw / 1e9).gbps() {
            0.gbps()
        } else {
            ($total_bw - (consumed_bw / 1e9).gbps()) / num_active_objects as f64
        };

        let mut min_inc = bw_inc_per_object.min((min_inc_to_max_rate / 1e9).gbps());

        // set when a flow will converge
        for (i, fs) in $m.values_mut().enumerate() {
            if num_active_flows[i] == 0 {
                continue;
            }
            let speed_inc_per_object = bw_inc_per_object / num_active_flows[i] as f64;
            min_inc = min_inc.min(speed_inc_per_object);

            let speed_inc_per_object_f64 = speed_inc_per_object.val() as f64;
            for f in fs {
                let mut f = f.borrow_mut();
                f.speed_bound = f.speed_bound.min(f.speed + speed_inc_per_object_f64);
            }
        }

        min_inc
    }};
}

impl Simulator {
    pub fn new(cluster: Cluster) -> Self {
        Simulator {
            cluster: Some(cluster),
            ts: 0,
            state: Default::default(),
            timers: BinaryHeap::new(),
            setting: SimulatorSetting::default(),
            estimator: None,
        }
    }

    pub fn with_brain(brain: Rc<RefCell<Brain>>) -> Self {
        let cluster = None;
        let interval = SAMPLE_INTERVAL_NS;
        let estimator = Box::new(SimpleEstimator::new(Rc::clone(&brain), interval));
        let timers =
            std::iter::once(Box::new(RepeatTimer::new(interval, interval)) as Box<dyn Timer>)
                .collect();
        let mut state: NetState = Default::default();
        state.brain = Some(Rc::clone(&brain));
        Simulator {
            cluster,
            ts: 0,
            state,
            timers,
            setting: SimulatorSetting::default(),
            estimator: Some(estimator),
        }
    }

    pub fn suspend(&mut self, _path: &std::path::Path) {
        // dump all running states of the simulator
        unimplemented!();
    }

    pub fn resume(&mut self, _path: &std::path::Path) {
        // resume from the previous saved state
        unimplemented!();
    }

    fn register_once(
        &mut self,
        next_ready: Timestamp,
        token: Option<Token>,
        timer_id: Option<TimerId>,
    ) {
        self.timers
            .push(Box::new(OnceTimer::new(next_ready, token, timer_id)));
    }

    #[inline]
    fn calc_delta_per_flow(total_bw: Bandwidth, fs: &mut FlowSet) -> Bandwidth {
        let (num_active, consumed_bw, min_inc) = fs.iter().fold((0, 0.0, f64::MAX), |acc, f| {
            let f = f.borrow();
            assert!(f.speed <= f.max_rate.val() as f64 + 10.0, "f: {:?}", f);
            (
                acc.0 + !f.converged as usize,
                acc.1 + f.speed,
                acc.2.min(if f.converged {
                    f.max_rate.val() as f64 - f.speed
                } else {
                    f64::MAX
                }),
            )
        });
        // COMMENT(cjr): due to precision issue, here consumed_bw can be a little bigger than bw
        let mut bw_inc = if total_bw < (consumed_bw / 1e9).gbps() {
            0.gbps()
        } else {
            (total_bw - (consumed_bw / 1e9).gbps()) / num_active as f64
        };

        // some flows may reach its max_rate earlier
        bw_inc = bw_inc.min((min_inc / 1e9).gbps());

        // set when a flow will converge
        fs.iter_mut()
            .map(|f| f.borrow_mut())
            .for_each(|mut f| f.speed_bound = f.speed_bound.min(f.speed + bw_inc.val() as f64));
        bw_inc
    }

    #[inline]
    fn calc_delta_groupped(total_bw: Bandwidth, fs: &mut FlowSet) -> Bandwidth {
        match fs {
            FlowSet::GroupByVmPair(m) => {
                calc_delta_on_group!(total_bw, m)
            }
            FlowSet::GroupByTenant(m) => {
                calc_delta_on_group!(total_bw, m)
            }
            _ => panic!("flows must be groupped by some label"),
        }
    }

    #[inline]
    fn calc_delta(fairness: FairnessModel, l: &Link, fs: &mut FlowSet) -> Bandwidth {
        match fairness {
            FairnessModel::PerFlowMaxMin => Self::calc_delta_per_flow(l.bandwidth, fs),
            FairnessModel::PerVmPairMaxMin => Self::calc_delta_groupped(l.bandwidth, fs),
            FairnessModel::TenantFlowMaxMin => Self::calc_delta_groupped(l.bandwidth, fs),
        }
    }

    fn proceed(&mut self, ts_inc: Duration) -> Vec<TraceRecord> {
        // complete some flows
        let comp_flows = self.state.complete_flows(self.ts, ts_inc);

        // start new ready flows
        self.ts += ts_inc;
        self.state.emit_ready_flows(self.ts);

        // nethint sampling
        if self.setting.enable_nethint {
            if let Some(timer) = self.timers.peek() {
                if timer.next_alert() <= self.ts && timer.kind() == TimerKind::Repeat {
                    let timer = self.timers.pop().unwrap();
                    match timer.as_box_any().downcast::<RepeatTimer>() {
                        Ok(mut repeat_timer) => {
                            // estimator samples
                            self.estimator.as_mut().unwrap().sample(self.ts);

                            repeat_timer.reset();
                            self.timers.push(repeat_timer);
                        }
                        Err(any_timer) => {
                            match any_timer.downcast::<PoissonTimer>() {
                                Ok(mut poisson_timer) => {
                                    assert!(self.setting.background_flow_hard.enable);
                                    // ask brain to update background flow
                                    let brain = self.state.brain.as_ref().unwrap().clone();
                                    brain.borrow_mut().update_background_flow_hard(
                                        self.setting.background_flow_hard.probability,
                                        self.setting.background_flow_hard.amplitude,
                                        self.setting.background_flow_hard.zipf_exp,
                                    );
                                    poisson_timer.reset();
                                    self.timers.push(poisson_timer);
                                }
                                Err(_) => panic!("fail to downcast to RepeatTimer or PoissonTimer"),
                            }
                        }
                    }
                }
            }
        }

        comp_flows
    }

    fn max_min_fairness_converge(&mut self) {
        let mut converged = 0;
        let active_flows = self.state.running_flows.len();
        self.state.running_flows.iter().for_each(|f| {
            let mut f = f.borrow_mut();
            if f.is_loopback() {
                f.speed_bound = self.setting.loopback_speed.val() as f64;
                f.converged = true;
                f.speed = self.setting.loopback_speed.val() as f64;
                converged += 1;
            } else {
                f.speed_bound = f.max_rate.val() as f64;
                f.converged = false;
                f.speed = 0.0;
            }
        });

        let fairness = self.setting.fairness;
        while converged < active_flows {
            // find the bottleneck link
            let brain = &self.state.brain.as_ref().unwrap().borrow();
            let mut res = self
                .state
                .link_flows
                .iter_mut()
                .filter(|(_, fs)| fs.iter().any(|f| !f.borrow().converged)) // this seems to be redundant
                .map(|(l, fs)| {
                    assert!(!fs.is_empty());
                    let link = &brain.cluster()[*l];
                    Self::calc_delta(fairness, link, fs)
                })
                .min();

            // in rate limited mode, some flows share the same rate limiter, so they may reach the limit together earlier
            if brain.setting().sharing_mode == crate::SharingMode::RateLimited {
                for i in 0..=1 {
                    let tmp = self.state.vm_flows[i]
                        .iter_mut()
                        .filter(|(_, fs)| fs.iter().any(|f| !f.borrow().converged)) // this seems to be redundant
                        .map(|(_vm, fs)| {
                            assert!(!fs.is_empty());
                            let limited_bw = fs.iter().next().unwrap().borrow().max_rate;
                            assert!(limited_bw > 0.gbps());
                            // log::trace!("vm: {:?}, limited_bw: {}, count: {}", vm, limited_bw, fs.iter().count());
                            Self::calc_delta_per_flow(limited_bw, fs)
                        })
                        .min();

                    res = res.into_iter().chain(tmp).min();
                }
            }

            let bw = res.expect("impossible");
            let speed_inc = bw.val() as f64;

            // increase the speed of all active flows
            for f in &self.state.running_flows {
                let mut f = f.borrow_mut();
                if !f.converged {
                    f.speed += speed_inc;
                    // TODO(cjr): be careful about this
                    if f.speed + 1e-10 >= f.speed_bound {
                        f.converged = true;
                        converged += 1;
                    } else {
                        f.speed_bound = f.max_rate.val() as f64;
                    }
                }
            }
        }
    }

    fn max_min_fairness(&mut self) -> AppEventKind {
        loop {
            // TODO(cjr): Optimization: if netstate hasn't been changed
            // (i.e. new newly added or completed flows), then skip max_min_fairness_converge.
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
                .chain(self.timers.peek().map(|x| x.next_alert()))
                .min()
                .expect("running flows, ready flows, and timers are all empty")
                - self.ts;

            trace!("self.ts: {}, ts_inc: {}", self.ts, ts_inc);
            assert!(
                !(first_complete_time.is_none()
                    && first_ready_time.is_none()
                    && (self.timers.is_empty()
                        || self.timers.len() == 1 && self.setting.enable_nethint))
            );
            assert!(
                ts_inc <= self.setting.sample_interval_ns,
                "ts_inc: {}, now: {}, next timer: {:?}",
                ts_inc,
                self.ts,
                self.timers.peek().map(|x| x.next_alert())
            );

            // it must be the timer
            if ts_inc == 0 {
                // the next event should be the timer event
                if let Some(timer) = self.timers.peek() {
                    if timer.next_alert() != self.ts {
                        log::warn!("ts_inc = 0, while next alert ts: {}, self.ts: {}", timer.next_alert(), self.ts);
                    }
                    if timer.kind() == TimerKind::Once {
                        let timer = self.timers.pop().unwrap();
                        let once_timer = timer.as_any().downcast_ref::<OnceTimer>().unwrap();
                        log::trace!("{:?}", once_timer);
                        let token = once_timer.token;
                        let timer_id = once_timer.timer_id;
                        if timer_id.is_some() {
                            break AppEventKind::AdapterNotification(token, timer_id.unwrap());
                        } else {
                            break AppEventKind::UserNotification(token);
                        }
                    }
                }
            }

            assert!(
                ts_inc > 0
                    || (ts_inc == 0
                        && self
                            .timers
                            .peek()
                            .and_then(|timer| if timer.kind() == TimerKind::Repeat {
                                Some(())
                            } else {
                                None
                            })
                            .is_some()),
                "only Nethint timers can cause ts_inc == 0"
            );

            // modify the network state to the time at ts + ts_inc
            let comp_flows = self.proceed(ts_inc);
            if !comp_flows.is_empty() {
                trace!(
                    "ts: {:?}, completed flows: {:?}",
                    self.ts.to_dura(),
                    comp_flows
                );
                break AppEventKind::FlowComplete(comp_flows);
            }
        }
    }
}

impl<'a> Executor<'a> for Simulator {
    fn run_with_trace(&mut self, trace: Trace) -> Trace {
        let app = Box::new(Replayer::new(trace));
        self.run_with_application(app)
    }

    fn run_with_application<T>(&mut self, mut app: Box<dyn Application<Output = T> + 'a>) -> T {
        macro_rules! app_event {
            ($kind:expr) => {{
                let kind = $kind;
                AppEvent::new(self.ts, kind)
            }};
        }
        // set background flow hard at the very beginning
        if self.setting.background_flow_hard.enable {
            // ask brain to update background flow
            let brain = Rc::clone(&self.state.brain.as_ref().unwrap());
            brain.borrow_mut().update_background_flow_hard(
                self.setting.background_flow_hard.probability,
                self.setting.background_flow_hard.amplitude,
                self.setting.background_flow_hard.zipf_exp,
            );
        }

        let start = std::time::Instant::now();
        let mut events = app.on_event(app_event!(AppEventKind::AppStart));
        let mut new_events = Events::new();
        loop {
            let mut finished = false;
            events.reverse();
            trace!("simulator: events.len: {:?}", events.len());
            while let Some(event) = events.pop() {
                trace!("simulator: on event {:?}", event);
                match event {
                    Event::FlowArrive(recs) => {
                        assert!(!recs.is_empty(), "No flow arrives.");
                        // 1. find path for each flow and add to current net state
                        for r in recs {
                            if let Some(cluster) = self.cluster.as_ref() {
                                self.state.add_flow(r, Some(cluster), self.ts);
                            } else {
                                self.state.add_flow(r, None, self.ts);
                            }
                        }
                    }
                    Event::AppFinish => {
                        finished = true;
                    }
                    Event::NetHintRequest(app_id, tenant_id, version, app_hint) => {
                        assert!(self.setting.enable_nethint, "Nethint not enabled.");
                        let response = AppEventKind::NetHintResponse(
                            app_id,
                            tenant_id,
                            match version {
                                NetHintVersion::V1 => {
                                    self.estimator.as_ref().unwrap().estimate_v1(tenant_id)
                                }
                                NetHintVersion::V2 => self.estimator.as_mut().unwrap().estimate_v2(
                                    tenant_id,
                                    self.setting.fairness,
                                    &self.state.link_flows,
                                    app_hint, // 0 for mapreduce, 1 for allreduce
                                ),
                            },
                        );
                        new_events.append(app.on_event(app_event!(response)));
                    }
                    Event::AdapterRegisterTimer(after_dura, token, timer_id) => {
                        self.register_once(self.ts + after_dura, token, Some(timer_id));
                    }
                    Event::UserRegisterTimer(after_dura, token) => {
                        // panic!("currently we do not support user app directly register this
                        //         kind of timer, considering save and restore the token
                        //         just like stored_flow_token");
                        self.register_once(self.ts + after_dura, token, None);
                    }
                }
            }

            if finished {
                break;
            }

            if !new_events.is_empty() {
                // NetHintResponse sent, new flows may arrive
                // must handle them first before computing max_min_fairness
                std::mem::swap(&mut events, &mut new_events);
                continue;
            }

            // 2. run max-min fairness to find the next completed flow
            let app_event_kind = self.max_min_fairness();

            // 3. nofity the application with this flow
            new_events.append(app.on_event(app_event!(app_event_kind)));
            std::mem::swap(&mut events, &mut new_events);
        }

        let end = std::time::Instant::now();

        debug!("sim_time: {:?}", end - start);
        // output
        app.answer()
    }
}

#[inline]
fn hash_vm_pair(f: &Flow) -> usize {
    let vsrc_id: usize = f.vsrc.as_ref().unwrap()
            .strip_prefix("host_")
            .unwrap()
            .parse()
            .unwrap();
    let vdst_id: usize = f.vsrc.as_ref().unwrap()
            .strip_prefix("host_")
            .unwrap()
            .parse()
            .unwrap();
    (vsrc_id << 32) | vdst_id
}

#[derive(Debug)]
pub(crate) enum FlowSet {
    Flat(Vec<FlowStateRef>),
    // src, dst -> vector of flows
    GroupByVmPair(HashMap<usize, Vec<FlowStateRef>>),
    GroupByTenant(HashMap<TenantId, Vec<FlowStateRef>>),
}

impl FlowSet {
    fn new(fairness: FairnessModel) -> FlowSet {
        match fairness {
            FairnessModel::PerFlowMaxMin => FlowSet::Flat(Vec::new()),
            FairnessModel::PerVmPairMaxMin => FlowSet::GroupByVmPair(HashMap::default()),
            FairnessModel::TenantFlowMaxMin => FlowSet::GroupByTenant(HashMap::default()),
        }
    }

    fn push(&mut self, fs: FlowStateRef) {
        match self {
            Self::Flat(v) => v.push(fs),
            Self::GroupByVmPair(m) => {
                // we can just unwrap here because it must have valid vsrc and vdst fields,
                // otherwise, it does not make sense to use PerVmPairMaxMin
                let key = hash_vm_pair(&fs.borrow().flow);
                m.entry(key).or_insert_with(Vec::new).push(fs);
            }
            Self::GroupByTenant(m) => {
                let key = fs
                    .borrow()
                    .flow
                    .tenant_id
                    .unwrap_or_else(|| panic!("flow: {:?}", fs.borrow().flow));
                m.entry(key).or_insert_with(Vec::new).push(fs);
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Flat(v) => v.is_empty(),
            Self::GroupByVmPair(m) => m.is_empty(),
            Self::GroupByTenant(m) => m.is_empty(),
        }
    }

    fn retain<F>(&mut self, f: F)
    where
        F: Clone + FnMut(&FlowStateRef) -> bool,
    {
        match self {
            Self::Flat(v) => v.retain(f),
            Self::GroupByVmPair(m) => {
                m.values_mut().for_each(|v| v.retain(f.clone()));
                m.retain(|_, v| !v.is_empty());
            }
            Self::GroupByTenant(m) => {
                m.values_mut().for_each(|v| v.retain(f.clone()));
                m.retain(|_, v| !v.is_empty());
            }
        }
    }

    pub(crate) fn iter(&self) -> FlowSetIter {
        match self {
            Self::Flat(v) => FlowSetIter::FlatIter(v.iter()),
            Self::GroupByVmPair(m) => {
                let mut iter1 = m.values();
                let iter2 = iter1.next().map(|v| v.iter());
                FlowSetIter::GroupByVmPairIter(iter1, iter2)
            }
            Self::GroupByTenant(m) => {
                let mut iter1 = m.values();
                let iter2 = iter1.next().map(|v| v.iter());
                FlowSetIter::GroupByTenantIter(iter1, iter2)
            }
        }
    }

    fn iter_mut(&mut self) -> FlowSetIterMut {
        match self {
            Self::Flat(v) => FlowSetIterMut::FlatIter(v.iter_mut()),
            Self::GroupByVmPair(m) => {
                let mut iter1 = m.values_mut();
                let iter2 = iter1.next().map(|v| v.iter_mut());
                FlowSetIterMut::GroupByVmPairIter(iter1, iter2)
            }
            Self::GroupByTenant(m) => {
                let mut iter1 = m.values_mut();
                let iter2 = iter1.next().map(|v| v.iter_mut());
                FlowSetIterMut::GroupByTenantIter(iter1, iter2)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum FlowSetIter<'a> {
    FlatIter(std::slice::Iter<'a, FlowStateRef>),
    GroupByVmPairIter(
        HashMapValues<'a, usize, Vec<FlowStateRef>>,
        Option<std::slice::Iter<'a, FlowStateRef>>,
    ),
    GroupByTenantIter(
        HashMapValues<'a, TenantId, Vec<FlowStateRef>>,
        Option<std::slice::Iter<'a, FlowStateRef>>,
    ),
}

enum FlowSetIterMut<'a> {
    FlatIter(std::slice::IterMut<'a, FlowStateRef>),
    GroupByVmPairIter(
        HashMapValuesMut<'a, usize, Vec<FlowStateRef>>,
        Option<std::slice::IterMut<'a, FlowStateRef>>,
    ),
    GroupByTenantIter(
        HashMapValuesMut<'a, TenantId, Vec<FlowStateRef>>,
        Option<std::slice::IterMut<'a, FlowStateRef>>,
    ),
}

macro_rules! impl_groupped_iter {
    ($iter1:expr, $iter2:expr, $iter_func:ident) => {
        if $iter2.is_none() {
            None
        } else {
            match $iter2.as_mut().unwrap().next() {
                Some(ret) => Some(ret),
                None => {
                    *$iter2 = $iter1.next().map(|v| v.$iter_func());
                    if $iter2.is_none() {
                        None
                    } else {
                        $iter2.as_mut().unwrap().next()
                    }
                }
            }
        }
    };
}

macro_rules! impl_iter_for {
    ($name:ident, $item:ty, $iter_func:ident) => {
        impl<'a> Iterator for $name<'a> {
            type Item = $item;
            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    Self::FlatIter(iter) => iter.next(),

                    Self::GroupByVmPairIter(iter1, iter2) => {
                        impl_groupped_iter!(iter1, iter2, $iter_func)
                    }

                    Self::GroupByTenantIter(iter1, iter2) => {
                        impl_groupped_iter!(iter1, iter2, $iter_func)
                    }
                }
            }
        }
    };
}

impl_iter_for!(FlowSetIter, &'a FlowStateRef, iter);
impl_iter_for!(FlowSetIterMut, &'a mut FlowStateRef, iter_mut);

#[derive(Default)]
struct NetState {
    /// buffered flows, flow.ts > sim.ts
    flow_bufs: BinaryHeap<Reverse<FlowStateRef>>,
    // emitted flows
    running_flows: Vec<FlowStateRef>,
    link_flows: HashMap<LinkIx, FlowSet>,
    vm_flows: [HashMap<(TenantId, String), FlowSet>; 2], // used only in rate limited mode, 0 tx, 1 rx
    loopback_flows: Vec<FlowStateRef>,
    resolve_route_time: std::time::Duration,
    // for nethint use
    brain: Option<Rc<RefCell<Brain>>>,
    fairness: FairnessModel,
}

impl NetState {
    #[inline]
    fn emit_flow(&mut self, fs: FlowStateRef) {
        self.running_flows.push(Rc::clone(&fs));
        let fairness = self.fairness;
        for &l in &fs.borrow().route.path {
            self.link_flows
                .entry(l)
                .or_insert_with(|| FlowSet::new(fairness))
                .push(Rc::clone(&fs));
        }
        if fs.borrow().is_loopback() {
            self.loopback_flows.push(Rc::clone(&fs));
        } else {
            if let Some(brain) = self.brain.as_ref() {
                if brain.borrow().setting().sharing_mode == SharingMode::RateLimited {
                    let tenant_id = fs.borrow().flow.tenant_id.unwrap();
                    let vsrc = fs.borrow().flow.vsrc.as_ref().unwrap().clone();
                    let vdst = fs.borrow().flow.vdst.as_ref().unwrap().clone();
                    self.vm_flows[0]
                        .entry((tenant_id, vsrc))
                        .or_insert_with(|| FlowSet::new(FairnessModel::PerFlowMaxMin))
                        .push(Rc::clone(&fs));
                    self.vm_flows[1]
                        .entry((tenant_id, vdst))
                        .or_insert_with(|| FlowSet::new(FairnessModel::PerFlowMaxMin))
                        .push(Rc::clone(&fs));
                }
            }
        }
    }

    fn add_flow(&mut self, r: TraceRecord, cluster: Option<&Cluster>, sim_ts: Timestamp) {
        let start = std::time::Instant::now();
        let hint = RouteHint::VirtAddr(r.flow.vsrc.as_deref(), r.flow.vdst.as_deref());
        let (max_rate, route) = if let Some(cluster) = cluster {
            // only the physical cluster is what we have, no virtualization
            let max_rate = bandwidth::MAX;
            (
                max_rate,
                cluster.resolve_route(&r.flow.src, &r.flow.dst, &hint, None),
            )
        } else {
            let brain = self.brain.as_ref().unwrap().borrow();
            let max_rate = if let Some(tenant_id) = r.flow.tenant_id {
                let vcluster = brain.vclusters[&tenant_id].borrow();
                let src_vhost_ix = vcluster.get_node_index(r.flow.vsrc.as_ref().unwrap());
                let dst_vhost_ix = vcluster.get_node_index(r.flow.vdst.as_ref().unwrap());
                if src_vhost_ix == dst_vhost_ix {
                    bandwidth::MAX
                } else {
                    vcluster[vcluster.get_uplink(src_vhost_ix)].bandwidth.min(
                        vcluster[vcluster.get_reverse_link(vcluster.get_uplink(dst_vhost_ix))]
                            .bandwidth,
                    )
                }
            } else {
                let cluster = brain.cluster();
                let src_host_ix = cluster.get_node_index(&r.flow.src);
                let dst_host_ix = cluster.get_node_index(&r.flow.dst);
                if src_host_ix == dst_host_ix {
                    bandwidth::MAX
                } else {
                    cluster[cluster.get_uplink(src_host_ix)].bandwidth.min(
                        cluster[cluster.get_reverse_link(cluster.get_uplink(dst_host_ix))]
                            .bandwidth,
                    )
                }
            };
            assert!(max_rate > 0.gbps());

            (
                max_rate,
                self.brain
                    .as_ref()
                    .unwrap()
                    .borrow()
                    .cluster()
                    .resolve_route(&r.flow.src, &r.flow.dst, &hint, None),
            )
        };

        let end = std::time::Instant::now();
        self.resolve_route_time += end - start;

        let fs = FlowState::new(r.ts, r.flow, max_rate, route);
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

        self.running_flows.iter().for_each(|f| {
            let mut f = f.borrow_mut();
            let speed = f.speed;
            assert!(
                f.speed <= f.max_rate.val() as f64 + 10.0,
                "flowstate: {:?}",
                f
            );
            let delta = (speed / 1e9 * ts_inc as f64).round() as usize / 8;
            f.bytes_sent += delta;

            // update counters for nethint
            if !f.is_loopback() {
                self.update_counters(&*f, delta);
            }

            if f.completed() {
                // log::info!("ts: {}, comp f: {:?}", sim_ts + ts_inc, f);
                comp_flows.push(TraceRecord::new(
                    f.ts,
                    f.flow.clone(),
                    Some(sim_ts + ts_inc - f.ts),
                ));
            }
        });

        self.running_flows.retain(|f| !f.borrow().completed());

        // finish and filter link flows
        for (_, fs) in self.link_flows.iter_mut() {
            fs.retain(|f| !f.borrow().completed());
        }

        // filter all empty links
        self.link_flows.retain(|_, fs| !fs.is_empty());

        // finish and filter loopback_flows
        self.loopback_flows.retain(|f| !f.borrow().completed());

        // only in rate limited mode
        if let Some(brain) = self.brain.as_ref() {
            if brain.borrow().setting().sharing_mode == SharingMode::RateLimited {
                for i in 0..=1 {
                    for (_, fs) in self.vm_flows[i].iter_mut() {
                        fs.retain(|f| !f.borrow().completed());
                    }
                    self.vm_flows[i].retain(|_, fs| !fs.is_empty());
                }
            }
        }

        comp_flows
    }

    fn update_counters(&self, f: &FlowState, delta: usize) {
        // by looking at flow.token, we can extract the tenant_id of the flow
        // that directs us to the corresponding VirtCluster by looking up
        // in Brain.
        // The sampler is to copy all nodes's latest data in all virt clusters.
        if let Some(ref brain) = self.brain {
            let tenant_id = f.flow.tenant_id.unwrap();
            if let Some(vcluster) = brain.borrow().vclusters.get(&tenant_id) {
                assert!(f.flow.vsrc.is_some() && f.flow.vdst.is_some());
                let vsrc_ix = vcluster
                    .borrow()
                    .get_node_index(f.flow.vsrc.as_deref().unwrap());
                let vdst_ix = vcluster
                    .borrow()
                    .get_node_index(f.flow.vdst.as_deref().unwrap());
                vcluster.borrow_mut()[vsrc_ix].counters.update_tx(f, delta);
                vcluster.borrow_mut()[vdst_ix].counters.update_rx(f, delta);
                // log::info!("src side counter: {:?}", vcluster.borrow_mut()[vsrc_ix].counters);
                // log::info!("dst side counter: {:?}", vcluster.borrow_mut()[vdst_ix].counters);
            }
        }
    }
}

type FlowStateRef = Rc<RefCell<FlowState>>;

/// A running flow
#[derive(Debug)]
pub(crate) struct FlowState {
    /// flow start time, could be greater than sim_ts, read only
    ts: Timestamp,
    /// read only flow property
    pub(crate) flow: Flow,
    /// maximal rate of this flow. Some flows are rate limited while others are limited by the host's speed
    max_rate: Bandwidth,
    /// below are states, mutated by the simulator
    ///
    /// upper bound to decide if a flow should converge
    speed_bound: f64,
    converged: bool,
    bytes_sent: usize,
    speed: f64, // bits/s
    pub(crate) route: Route,
}

impl FlowState {
    #[inline]
    fn new(ts: Timestamp, flow: Flow, max_rate: Bandwidth, route: Route) -> FlowStateRef {
        Rc::new(RefCell::new(FlowState {
            ts,
            flow,
            max_rate,
            speed_bound: 0.0,
            converged: false,
            bytes_sent: 0,
            speed: 0.0,
            route,
        }))
    }

    #[inline]
    fn time_to_complete(&self) -> Duration {
        assert!(
            self.speed > 0.0,
            "speed: {}, speed_bound: {}",
            self.speed,
            self.speed_bound
        );
        let time_sec = (self.flow.bytes - self.bytes_sent) as f64 * 8.0 / self.speed;
        (time_sec * 1e9).ceil() as Duration
    }

    #[inline]
    fn completed(&self) -> bool {
        // TODO(cjr): check the precision of this condition
        // self.bytes_sent >= self.flow.bytes
        self.bytes_sent >= self.flow.bytes || self.time_to_complete() == 0
    }

    #[inline]
    fn is_loopback(&self) -> bool {
        self.route.path.is_empty()
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct TimerId(pub usize);

impl TimerId {
    pub fn new() -> TimerId {
        TimerId(TIMER_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

impl From<usize> for TimerId {
    fn from(val: usize) -> TimerId {
        TimerId(val)
    }
}

impl From<TimerId> for usize {
    fn from(val: TimerId) -> usize {
        val.0
    }
}

#[derive(Debug, Clone)]
pub enum Event {
    /// Application notifies the simulator with the arrival of a set of flows.
    FlowArrive(Vec<TraceRecord>),
    /// The application has completed all flows and has no more flows to send. This event should
    /// appear after certain FlowComplete event.
    AppFinish,
    /// Request NetHint information, (app_id, tenant_id, version, app_hint)
    NetHintRequest(usize, TenantId, NetHintVersion, usize),
    /// A Timer event is registered by Application. It notifies the application after duration ns.
    /// Token is used to identify the timer.
    // RegisterTimer(Duration, Option<Token>, TimerId),
    AdapterRegisterTimer(Duration, Option<Token>, TimerId),
    /// This kind of events only faces to the user apps, and it will be
    /// translated into and from `AdapterRegisterTimer`.
    UserRegisterTimer(Duration, Option<Token>),
}

/// Iterator of Event
#[derive(Debug, Clone, Default)]
pub struct Events(SmallVec<[Event; 8]>);

impl Events {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn last(&self) -> Option<&Event> {
        self.0.last()
    }

    pub fn add(&mut self, e: Event) {
        self.0.push(e);
    }

    pub fn append(&mut self, mut e: Events) {
        self.0.append(&mut e.0);
    }

    pub fn reverse(&mut self) {
        self.0.reverse();
    }

    pub fn pop(&mut self) -> Option<Event> {
        self.0.pop()
    }
}

impl IntoIterator for Events {
    type Item = Event;
    type IntoIter = smallvec::IntoIter<[Self::Item; 8]>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl std::iter::FromIterator<Event> for Events {
    fn from_iter<T: IntoIterator<Item = Event>>(iter: T) -> Self {
        Events(iter.into_iter().collect())
    }
}

impl From<Event> for Events {
    fn from(e: Event) -> Self {
        Events(smallvec![e])
    }
}
