#![feature(option_unwrap_none)]
#![feature(option_zip)]
#![feature(concat_idents)]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use lazy_static::lazy_static;
use rand::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};

const RAND_SEED: u64 = 0;
thread_local! {
    pub static RNG: Rc<RefCell<StdRng>> = Rc::new(RefCell::new(StdRng::seed_from_u64(RAND_SEED)));
}

lazy_static! {
    static ref FLOW_ID: AtomicUsize = AtomicUsize::new(0);
}

pub mod bandwidth;

pub mod cluster;
use cluster::Route;

pub mod app;

pub mod multitenant;

pub mod simulator;
pub mod timer;

pub mod architecture;
pub mod brain;
pub use brain::TenantId;

pub mod hint;

pub mod background_flow;

pub mod runtime_est;
pub mod counterunit;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FairnessModel {
    PerFlowMaxMin,
    PerVmPairMaxMin,
    TenantFlowMaxMin,
}

impl std::default::Default for FairnessModel {
    fn default() -> Self {
        Self::PerFlowMaxMin
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SharingMode {
    RateLimited,
    Guaranteed,
}

impl std::default::Default for SharingMode {
    fn default() -> Self {
        Self::Guaranteed
    }
}

// nanoseconds
pub type Timestamp = u64;
pub type Duration = u64;

pub trait ToStdDuration {
    fn to_dura(self) -> std::time::Duration;
}

impl ToStdDuration for u64 {
    #[inline]
    fn to_dura(self) -> std::time::Duration {
        std::time::Duration::new(self / 1_000_000_000, (self % 1_000_000_000) as u32)
    }
}

/// A flow Trace is a table of flow record.
#[derive(Debug, Clone)]
pub struct Trace {
    pub recs: Vec<TraceRecord>,
}

impl Default for Trace {
    fn default() -> Self {
        Self::new()
    }
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

#[derive(Clone)]
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

impl std::fmt::Debug for TraceRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let dura = self.dura.map(|x| x.to_dura());
        f.debug_struct("TraceRecord")
            .field("ts", &self.ts)
            .field("flow", &self.flow)
            .field("dura", &dura)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct Flow {
    /// an identifier
    id: usize,
    pub bytes: usize,
    pub src: String,
    pub dst: String,
    /// an optional tag for application use (e.g. identify the flow in application)
    token: Option<Token>,
    /// this field is to explicitly support tenant based fairness
    tenant_id: Option<TenantId>,
    /// TODO(cjr): This is a hack to let the simulator be able to
    /// be aware of the virtual address.
    /// A better way to do this is to encapsulate the inner header
    /// inside the new header instead of replacing fields in place.
    vsrc: Option<String>,
    vdst: Option<String>,
}

impl Flow {
    #[inline]
    pub fn new(bytes: usize, src: &str, dst: &str, token: Option<Token>) -> Self {
        Flow {
            id: FLOW_ID.fetch_add(1, SeqCst),
            bytes,
            src: src.to_owned(),
            dst: dst.to_owned(),
            tenant_id: None,
            token,
            vsrc: None,
            vdst: None,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
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

pub trait LoadBalancer {
    fn load_balance(&mut self, routes: &[Route]) -> Route;
}
