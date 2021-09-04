use nethint::hint::NetHintVersion;
use nethint::{Duration, TenantId};
use std::collections::HashMap;

use crate::controller::app::Application;
use crate::message;
use crate::Flow;
use crate::Node;
use litemsg::endpoint::Endpoint;

use rand::{rngs::StdRng, SeedableRng};
use std::cell::RefCell;
use std::rc::Rc;
const RAND_SEED: u64 = 0;
thread_local! {
    pub static RNG: Rc<RefCell<StdRng>> = Rc::new(RefCell::new(StdRng::seed_from_u64(RAND_SEED)));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundFlowPattern {
    Alltoall,
    PlinkProbe,
}

pub struct BackgroundFlowApp {
    workers: HashMap<Node, Endpoint>,
    brain: Endpoint,
    hostname_to_node: HashMap<String, Node>,
    tenant_id: TenantId,

    nhosts: usize,
    /// running for dur_ms milliseconds
    dur_ms: std::time::Duration,
    pattern: BackgroundFlowPattern,
    /// default msg_size is 1MB
    msg_size: usize,

    // state
    stopped: bool,
    remaining_flows: usize,
    start_time: std::time::Instant,
    pub vname_to_hostname: HashMap<String, String>,
}

impl BackgroundFlowApp {
    pub fn new(
        workers: HashMap<Node, Endpoint>,
        brain: Endpoint,
        hostname_to_node: HashMap<String, Node>,
        tenant_id: TenantId,
        nhosts: usize,
        dur_ms: Duration,
        pattern: BackgroundFlowPattern,
        msg_size: Option<usize>,
    ) -> Self {
        BackgroundFlowApp {
            workers,
            brain,
            hostname_to_node,
            tenant_id,
            nhosts,
            dur_ms: std::time::Duration::from_millis(dur_ms),
            pattern,
            msg_size: msg_size.unwrap_or(1_000_000),
            stopped: false,
            remaining_flows: 0,
            start_time: std::time::Instant::now(),
            vname_to_hostname: Default::default(),
        }
    }

    fn start_alltoall(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    fn on_event_alltoall(&mut self, _cmd: message::Command) -> anyhow::Result<bool> {
        unimplemented!()
    }

    fn plink_probe_round(&mut self) -> anyhow::Result<()> {
        use rand::seq::SliceRandom;

        let mut n = self.nhosts;
        let mut ids = RNG.with(|rng| {
            let mut rng = rng.borrow_mut();
            let mut ids: Vec<_> = (0..n).collect();
            ids.shuffle(&mut *rng);
            ids
        });

        if n % 2 == 1 {
            ids.push(*ids.last().unwrap());
            n += 1;
        }

        let vnames: Vec<_> = ids
            .into_iter()
            .map(|i| {
                let vname = format!("host_{}", i);
                vname
            })
            .collect();

        let keys = self.workers.keys().cloned().collect::<Vec<_>>();
        for (i, j) in (0..n).step_by(2).zip((1..n).step_by(2)) {
            let sname = &vnames[i];
            let dname = &vnames[j];
            // log::info!("nhosts: {}, n: {}, sname: {}", self.nhosts, n, sname);
            let src_hostname = &self.vname_to_hostname[sname];
            let dst_hostname = &self.vname_to_hostname[dname];
            let src_node = &self.hostname_to_node[src_hostname];
            let dst_node = &self.hostname_to_node[dst_hostname];
            let flow = Flow::new(self.msg_size, src_node.clone(), dst_node.clone(), None);
            let cmd = message::Command::EmitFlow(flow);
            log::debug!("plink_probe_round, cmd: {:?}", cmd);
            let endpoint = self.workers.get_mut(src_node).unwrap_or_else(|| {
                panic!(
                    "sname: {}, src_hostname: {}, src_node: {:?}, workers: {:?}",
                    sname, src_hostname, src_node, keys
                )
            });
            endpoint.post(cmd, None)?;

            self.remaining_flows += 1;
        }

        Ok(())
    }

    fn start_plink_probe(&mut self) -> anyhow::Result<()> {
        self.plink_probe_round()?;
        Ok(())
    }

    fn on_event_plink_probe(&mut self, cmd: message::Command) -> anyhow::Result<bool> {
        use message::Command::*;
        match cmd {
            FlowComplete(_flow) => {
                self.remaining_flows -= 1;
                let cur_ts = std::time::Instant::now();
                let dura = cur_ts - self.start_time;
                if dura > self.dur_ms * 2 {
                    log::warn!("background flow was expect to finish in {:?}, however it has been running for {:?}", self.dur_ms, dura);
                }

                assert!(!self.stopped || self.stopped && dura >= self.dur_ms);

                if dura < self.dur_ms {
                    // start new flows
                    if self.remaining_flows == 0 {
                        self.plink_probe_round()?;
                    }
                    Ok(false)
                } else {
                    self.stopped = true;
                    Ok(self.remaining_flows == 0)
                }
            }
            _ => {
                panic!("unexpected cmd: {:?}", cmd);
            }
        }
    }
}

impl Application for BackgroundFlowApp {
    fn workers(&self) -> &HashMap<Node, Endpoint> {
        &self.workers
    }

    fn workers_mut(&mut self) -> &mut HashMap<Node, Endpoint> {
        &mut self.workers
    }

    fn brain(&self) -> &Endpoint {
        &self.brain
    }

    fn brain_mut(&mut self) -> &mut Endpoint {
        &mut self.brain
    }

    fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn hostname_to_node(&self) -> &HashMap<String, Node> {
        &self.hostname_to_node
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        panic!("impossible");
    }

    fn request_nethint(&mut self, _version: NetHintVersion) -> anyhow::Result<()> {
        panic!("impossible");
    }

    fn start(&mut self) -> anyhow::Result<()> {
        match self.pattern {
            BackgroundFlowPattern::Alltoall => self.start_alltoall(),
            BackgroundFlowPattern::PlinkProbe => self.start_plink_probe(),
        }
    }

    fn on_event(&mut self, cmd: message::Command) -> anyhow::Result<bool> {
        match self.pattern {
            BackgroundFlowPattern::Alltoall => self.on_event_alltoall(cmd),
            BackgroundFlowPattern::PlinkProbe => self.on_event_plink_probe(cmd),
        }
    }
}
