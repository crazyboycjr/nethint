use crate::controller::app::Application;
use crate::{message, Flow, Node};
use litemsg::endpoint::Endpoint;
use rand::{rngs::StdRng, SeedableRng};
use std::collections::HashMap;
use std::rc::Rc;

use crate::controller::background_flow::{BackgroundFlowApp, BackgroundFlowPattern};
use allreduce::{
    config::ProbeConfig, random_ring::RandomRingAllReduce, rat::RatAllReduce,
    topology_aware::TopologyAwareRingAllReduce, AllReduceAlgorithm, AllReducePolicy, JobSpec,
};

use nethint::{
    bandwidth::{Bandwidth, BandwidthTrait},
    cluster::{LinkIx, Topology, VirtCluster},
    counterunit::{CounterType, CounterUnit},
    hint::{NetHintV2Real, NetHintVersion},
    TenantId,
};
use serde::{Deserialize, Serialize};

/// see allreduce/config.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllreduceSetting {
    pub job_id: usize, // which item in trace
    pub job_size_distribution: Vec<(usize, usize)>,
    pub buffer_size: usize,
    pub num_iterations: usize,
    pub poisson_lambda: f64,
    pub seed_base: u64,
    pub traffic_scale: f64,
    pub allreduce_policy: AllReducePolicy,
    pub probe: ProbeConfig,
    pub nethint_level: usize,
    #[serde(default)]
    pub auto_tune: Option<usize>,
    #[serde(default)]
    pub num_rings: Option<usize>,
}

pub struct AllreduceAppBuilder {
    config_path: std::path::PathBuf,
    workers: HashMap<Node, Endpoint>,
    brain: Endpoint,
    hostname_to_node: HashMap<String, Node>,
}

impl AllreduceAppBuilder {
    pub fn new(
        config_path: std::path::PathBuf,
        workers: HashMap<Node, Endpoint>,
        brain: Endpoint,
        hostname_to_node: HashMap<String, Node>,
    ) -> Self {
        AllreduceAppBuilder {
            config_path,
            workers,
            brain,
            hostname_to_node,
        }
    }

    fn get_job_spec(setting: &AllreduceSetting) -> JobSpec {
        let mut rng = StdRng::seed_from_u64(setting.seed_base - setting.job_id as u64);
        let mut t = 0;
        let mut jobs = Vec::new();
        for i in 0..setting.job_id + 1 {
            let job_spec = JobSpec::new(
                allreduce::config::get_random_job_size(&setting.job_size_distribution, &mut rng),
                setting.buffer_size,
                setting.num_iterations,
            );
            let next = allreduce::config::get_random_arrival_time(setting.poisson_lambda, &mut rng);
            t += next;
            log::info!("job {}: {:?}", i, job_spec);
            jobs.push((t, job_spec));
        }

        jobs.last().unwrap().1.clone()
    }

    pub fn build(self) -> Box<dyn Application> {
        let mut setting = allreduce::config::read_config(&self.config_path);
        log::info!("allreduce setting: {:?}", setting);
        let job_spec = Self::get_job_spec(&setting);
        let seed = setting.seed_base;
        // no need to auto tune in case of n == 2
        if job_spec.num_workers == 2 {
            setting.auto_tune = None;
        }
        let app: Box<dyn Application> = Box::new(AllreduceApp {
            workers: self.workers,
            brain: self.brain,
            hostname_to_node: self.hostname_to_node,
            remaining_iterations: 0,
            num_remaining_flows: 0,
            setting: setting.clone(),
            seed,
            job_spec: job_spec.clone(),
            allreduce_algorithm: None,
            vname_to_hostname: Default::default(),
            cluster: None,
            flow_iters: Default::default(),
            in_probing: false,
            background_flow_app: None,
        });

        // if setting.probe.enable {
        //     app = Box::new(PlinkApp::new(
        //         job_spec.num_workers,
        //         setting.probe.round_ms,
        //         app,
        //     ));
        // }

        app
    }
}

pub struct AllreduceApp {
    workers: HashMap<Node, Endpoint>,
    brain: Endpoint,
    hostname_to_node: HashMap<String, Node>,

    remaining_iterations: usize,
    num_remaining_flows: usize,

    setting: AllreduceSetting,
    seed: u64,
    job_spec: JobSpec,
    allreduce_algorithm: Option<Box<dyn AllReduceAlgorithm>>,
    vname_to_hostname: HashMap<String, String>,
    cluster: Option<Rc<dyn Topology>>, // use Rc so we don't have deal with ugly lifetime specifiers
    flow_iters: HashMap<(Node, Node), usize>,
    // flow_iters: HashMap<Token, usize>,
    // dynamic probe
    in_probing: bool,
    background_flow_app: Option<BackgroundFlowApp>,
}

impl Application for AllreduceApp {
    fn workers(&self) -> &HashMap<Node, Endpoint> {
        if self.in_probing {
            self.background_flow_app.as_ref().unwrap().workers()
        } else {
            &self.workers
        }
    }

    fn workers_mut(&mut self) -> &mut HashMap<Node, Endpoint> {
        if self.in_probing {
            self.background_flow_app.as_mut().unwrap().workers_mut()
        } else {
            &mut self.workers
        }
    }

    fn brain(&self) -> &Endpoint {
        &self.brain
    }

    fn brain_mut(&mut self) -> &mut Endpoint {
        &mut self.brain
    }

    fn tenant_id(&self) -> TenantId {
        self.setting.job_id
    }

    fn hostname_to_node(&self) -> &HashMap<String, Node> {
        &self.hostname_to_node
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.remaining_iterations = self.job_spec.num_iterations;
        self.allreduce_algorithm = Some(self.new_allreduce_algorithm());
        self.allreduce()?;

        Ok(())
    }

    fn on_event(&mut self, cmd: message::Command) -> anyhow::Result<bool> {
        if self.in_probing {
            log::info!("AllreduceApp on_event, in_probing, cmd: {:?}", cmd);

            // forward the cmd to background_flow_app
            let fin = self.background_flow_app.as_mut().unwrap().on_event(cmd)?;
            if fin {
                self.in_probing = false;
                let mut temp: HashMap<Node, Endpoint> = HashMap::new();
                std::mem::swap(
                    &mut temp,
                    self.background_flow_app.as_mut().unwrap().workers_mut(),
                );
                std::mem::swap(self.workers_mut(), &mut temp);
                self.background_flow_app = None;

                log::info!("worker_mut.len: {}", self.workers_mut().len());

                self.allreduce()?;
            }
            return Ok(false);
        }

        // wait for all flows to finish
        use message::Command::*;
        match cmd {
            FlowComplete(flow) => {
                log::info!(
                    "remaining iter: {}, flow complete: {:?}",
                    self.remaining_iterations,
                    flow
                );
                // self.num_remaining_flows -= 1;
                let mut no_more_flow = false;
                self.flow_iters
                    .entry((flow.src.clone(), flow.dst.clone()))
                    // .entry(flow.token.unwrap())
                    .and_modify(|e| {
                        assert!(*e > 0);
                        *e -= 1;
                        if *e == 0 {
                            no_more_flow = true;
                        }
                    });
                self.num_remaining_flows -= no_more_flow as usize;
                if !no_more_flow {
                    // re-emit the flow
                    let endpoint = self.workers.get_mut(&flow.src).unwrap();
                    let cmd = message::Command::EmitFlow(flow);
                    log::debug!("allreduce::run, cmd: {:?}", cmd);
                    endpoint.post(cmd, None).unwrap();
                }

                if self.num_remaining_flows == 0 && self.remaining_iterations > 0 {
                    if self.setting.auto_tune.is_some() && self.setting.auto_tune.unwrap() > 0
                    // && self.remaining_iterations % self.setting.auto_tune.unwrap() == 0
                    {
                        self.cluster = None;
                        assert_eq!(self.setting.nethint_level, 2);
                        self.request_nethint(NetHintVersion::V2)?;
                        return Ok(false);
                    }
                    unreachable!();
                    // self.allreduce()?;
                } else if self.num_remaining_flows == 0 && self.remaining_iterations == 0 {
                    self.finish()?;
                    return Ok(true);
                }
            }
            BrainResponse(msg) => {
                self.handle_brain_response_event(msg)?;
            }
            _ => {
                panic!("unexpected cmd: {:?}", cmd);
            }
        }
        Ok(false)
    }
}

impl AllreduceApp {
    fn start_probe(&mut self) -> anyhow::Result<()> {
        log::info!("start probe");
        assert!(self.setting.probe.enable);
        assert!(!self.in_probing);

        let nhosts = self.job_spec.num_workers;
        let round_ms = self.setting.probe.round_ms;

        let dur_ms = (nhosts as u64 * round_ms) as _;
        self.background_flow_app = Some(BackgroundFlowApp::new(
            std::mem::take(self.workers_mut()),
            self.brain().clone(),
            self.hostname_to_node().clone(),
            self.tenant_id(),
            nhosts,
            dur_ms,
            BackgroundFlowPattern::PlinkProbe,
            Some(10_000_000), // 8ms on 10G
        ));

        self.in_probing = true;

        self.background_flow_app.as_mut().unwrap().vname_to_hostname = self.vname_to_hostname.clone();
        self.background_flow_app.as_mut().unwrap().start()?;

        Ok(())
    }

    fn new_allreduce_algorithm(&self) -> Box<dyn AllReduceAlgorithm> {
        let num_rings = self.setting.num_rings.unwrap_or(1);
        let num_trees = self.setting.num_rings.unwrap_or(self.job_spec.num_workers);
        match self.setting.allreduce_policy {
            AllReducePolicy::Random => Box::new(RandomRingAllReduce::new(self.seed, num_rings)),
            AllReducePolicy::TopologyAware => {
                Box::new(TopologyAwareRingAllReduce::new(self.seed, num_rings))
            }
            AllReducePolicy::RAT => Box::new(RatAllReduce::new(num_trees)),
        }
    }

    pub fn allreduce(&mut self) -> anyhow::Result<()> {
        log::info!("allreduce");

        if self.cluster.is_none() {
            // self.request_provision()?;
            let version = match self.setting.nethint_level {
                0 => NetHintVersion::V1,
                1 => NetHintVersion::V1,
                2 => NetHintVersion::V2,
                _ => panic!("unexpected nethint_level: {}", self.setting.nethint_level),
            };
            self.request_nethint(version)?;
            return Ok(());
        }

        let start = std::time::Instant::now();

        // then we have the hint, self.cluster is set
        if self.allreduce_algorithm.is_none() {
            self.allreduce_algorithm = Some(self.new_allreduce_algorithm());
        }

        log::debug!("hint: {}", self.cluster.as_ref().unwrap().to_dot());
        let flows = self.allreduce_algorithm.as_mut().unwrap().allreduce(
            self.job_spec.buffer_size as u64,
            Rc::clone(self.cluster.as_ref().unwrap()),
        );

        let end = std::time::Instant::now();
        log::debug!("it takes {:?} to run the allreduce algorithm", end - start);
        log::debug!("flows from result of allreduce algorithm: {:?}", flows);

        // merge the flows with the same src and dst pair
        log::debug!("merging the flows with the same src and dst pair");
        let mut matrix: HashMap<(String, String), usize> = Default::default();
        for flow in flows {
            let size = flow.bytes;
            *matrix
                .entry((flow.src.clone(), flow.dst.clone()))
                .or_default() += size;
        }

        // for (i, f) in &mut flows.iter_mut().enumerate() {
        //     f.token = Some(Token(i));
        // }

        self.flow_iters.clear();
        let niters = self
            .setting
            .auto_tune
            .unwrap_or(self.job_spec.num_iterations)
            .min(self.remaining_iterations);
        for (k, size) in matrix {
            // for flow in flows {
            // let size = flow.bytes;
            // let src_hostname = &self.vname_to_hostname[&flow.src];
            // let dst_hostname = &self.vname_to_hostname[&flow.dst];
            let src_hostname = &self.vname_to_hostname[&k.0];
            let dst_hostname = &self.vname_to_hostname[&k.1];
            let src_node = &self.hostname_to_node[src_hostname];
            let dst_node = &self.hostname_to_node[dst_hostname];

            let e = self
                .flow_iters
                // .entry(flow.token.unwrap())
                .entry((src_node.clone(), dst_node.clone()))
                .or_insert(0);
            *e += niters;
            let flow = Flow::new(size, src_node.clone(), dst_node.clone(), None);
            let cmd = message::Command::EmitFlow(flow);
            log::debug!("allreduce::run, cmd: {:?}", cmd);
            let endpoint = self.workers.get_mut(src_node).unwrap();
            endpoint.post(cmd, None)?;

            self.num_remaining_flows += 1;
        }

        self.remaining_iterations -= niters;
        Ok(())
    }

    // write the estimation result to self.cluster
    fn estimate_hintv2(&mut self, hintv2: NetHintV2Real) {
        // unimplemented!("the problem is how to estimate nethint v2 from link traffic information. save to self.cluster");

        // the estimation refers to nethint/src/hint.rs:estimate_v2
        let mut vc = hintv2.hintv1.vc.clone();
        log::debug!(
            "estimating, hintv2: vc: {}, vname_to_hostname: {:?}, interval_ms: {}, traffic: {:?}",
            vc.to_dot(),
            hintv2.hintv1.vname_to_hostname,
            hintv2.interval_ms,
            hintv2.traffic
        );

        let empty_traffic = &Vec::new();
        for vlink_ix in vc.all_links() {
            // decide the direction
            let n1 = &vc[vc.get_source(vlink_ix)];
            let n2 = &vc[vc.get_target(vlink_ix)];
            assert_ne!(n1.depth, n2.depth);
            let (traffic, link_ix, direction) = if n1.depth > n2.depth {
                // tx
                (
                    hintv2.traffic.get(&vlink_ix).unwrap_or(empty_traffic),
                    vlink_ix,
                    CounterType::Tx,
                )
            } else {
                // rx
                let link_ix = vc.get_reverse_link(vlink_ix);
                (
                    hintv2.traffic.get(&link_ix).unwrap_or(empty_traffic),
                    link_ix,
                    CounterType::Rx,
                )
            };
            let plink_capacity = vc[vlink_ix].bandwidth;
            // we are using per flow fairness here
            let bw = Self::compute_fair_share_per_flow(
                &vc,
                link_ix,
                hintv2.interval_ms,
                traffic,
                plink_capacity,
                direction,
            );
            vc[vlink_ix].bandwidth = bw;
        }

        self.cluster = Some(Rc::new(vc));
    }

    fn compute_fair_share_per_flow(
        vc: &VirtCluster,
        link_ix: LinkIx,
        interval_ms: u64,
        traffic: &Vec<CounterUnit>,
        plink_capacity: Bandwidth,
        direction: CounterType,
    ) -> Bandwidth {
        // XXX(cjr): remember to subtract traffic from this tenant itself from all traffic
        // assume it has already been subtracted from the return value
        let demand_sum = traffic.iter().map(|c| c.data[direction].bytes).sum::<u64>() as usize;
        let num_flows = traffic
            .iter()
            .map(|c| c.data[direction].num_competitors)
            .sum::<u32>() as usize;

        // num_nwe_flows will depend on the app
        let mut high_node = vc[vc.get_source(link_ix)].clone();
        let mut low_node = vc[vc.get_target(link_ix)].clone();
        if high_node.depth > low_node.depth {
            std::mem::swap(&mut high_node, &mut low_node);
        }
        let num_new_flows = if high_node.depth == 1 {
            // for allreduce
            let a = vc.get_downlinks(vc.get_node_index("virtual_cloud")).count();
            let n = vc.num_hosts();
            let b = vc.get_downlinks(vc.get_node_index(&low_node.name)).count();
            std::cmp::max(1, b * (a - 1) + n - b)
        } else {
            // for allreduce
            let a = vc.get_downlinks(vc.get_node_index("virtual_cloud")).count();
            let n = vc.num_hosts();
            let b = vc.get_downlinks(vc.get_node_index(&high_node.name)).count();
            a + n + n * (b - 1) / b
        };
        let mut demand_sum_bw =
            (8.0 * demand_sum as f64 / (interval_ms as f64 / 1000.0) / 1e9).gbps();
        if demand_sum_bw > plink_capacity {
            demand_sum_bw = plink_capacity;
        }
        std::cmp::max(
            plink_capacity - demand_sum_bw,
            plink_capacity / (num_flows + num_new_flows) * num_new_flows,
        )
    }

    pub fn handle_brain_response_event(
        &mut self,
        msg: nhagent_v2::message::Message,
    ) -> anyhow::Result<()> {
        use nhagent_v2::message::Message::*;
        let my_tenant_id = self.tenant_id();
        match msg {
            NetHintResponseV1(tenant_id, hintv1) => {
                assert_eq!(my_tenant_id, tenant_id);
                self.vname_to_hostname = hintv1.vname_to_hostname;
                self.cluster = Some(Rc::new(hintv1.vc));

                if self.setting.probe.enable {
                    self.start_probe()?;
                } else {
                    self.allreduce()?;
                }
            }
            NetHintResponseV2(tenant_id, hintv2) => {
                assert_eq!(my_tenant_id, tenant_id);
                self.vname_to_hostname = hintv2.hintv1.vname_to_hostname.clone();
                self.estimate_hintv2(hintv2);
                if self.setting.probe.enable {
                    self.start_probe()?;
                } else {
                    self.allreduce()?;
                }
            }
            _ => {
                panic!("unexpected brain response: {:?}", msg);
            }
        }
        Ok(())
    }
}
