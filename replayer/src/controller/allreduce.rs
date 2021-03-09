use crate::controller::ProbeConfig;
use crate::{message, Flow, Node};
use litemsg::endpoint::Endpoint;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::HashMap;
use std::rc::Rc;
use crate::controller::app::Application;

use allreduce::{
    random_ring::RandomRingAllReduce, rat::RatAllReduce,
    topology_aware::TopologyAwareRingAllReduce, AllReduceAlgorithm, AllReducePolicy, JobSpec,
};

use nethint::{
    cluster::Topology,
    hint::{NetHintV1Real, NetHintV2Real, NetHintVersion},
    TenantId,
};
use serde::{Deserialize, Serialize};

use rand_distr::{Distribution, Poisson};

fn get_random_job_size(job_size_dist: &[(usize, usize)], rng: &mut StdRng) -> usize {
    // let job_sizes = [[40, 4], [80, 8], [90, 16], [25, 32], [5, 64]];
    let total: usize = job_size_dist.iter().map(|x| x.0).sum();
    assert_ne!(total, 0);

    let mut n = rng.gen_range(0..total);
    let mut i = 0;
    while i < job_size_dist.len() {
        if n < job_size_dist[i].0 {
            return job_size_dist[i].1;
        }
        n -= job_size_dist[i].0;
        i += 1;
    }

    // default
    32
}

fn get_random_arrival_time(lambda: f64, rng: &mut StdRng) -> u64 {
    let poi = Poisson::new(lambda).unwrap();
    poi.sample(rng) as u64
}

/// see allreduce/experiment.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllreduceSetting {
    job_id: usize, // which item in trace
    job_size_distribution: Vec<(usize, usize)>,
    buffer_size: usize,
    num_iterations: usize,
    poisson_lambda: f64,
    seed_base: u64,
    traffic_scale: f64,
    allreduce_policy: AllReducePolicy,
    probe: ProbeConfig,
    nethint_level: usize,
    #[serde(default)]
    auto_tune: Option<usize>,
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

    fn load_config_from_file(&self) -> AllreduceSetting {
        use std::io::Read;
        let mut file = std::fs::File::open(&self.config_path).expect("fail to open file");
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        toml::from_str(&content).expect("parse failed")
    }

    fn get_job_spec(setting: &AllreduceSetting) -> JobSpec {
        let mut rng = StdRng::seed_from_u64(setting.seed_base);
        let mut t = 0;
        let mut jobs = Vec::new();
        for i in 0..setting.job_id + 1 {
            let job_spec = JobSpec::new(
                get_random_job_size(&setting.job_size_distribution, &mut rng),
                setting.buffer_size,
                setting.num_iterations,
            );
            let next = get_random_arrival_time(setting.poisson_lambda, &mut rng);
            t += next;
            log::info!("job {}: {:?}", i, job_spec);
            jobs.push((t, job_spec));
        }

        jobs.last().unwrap().1.clone()
    }

    pub fn build(self) -> AllreduceApp {
        let setting = self.load_config_from_file();
        log::info!("allreduce setting: {:?}", setting);
        let job_spec = Self::get_job_spec(&setting);
        let seed = setting.seed_base;
        AllreduceApp {
            workers: self.workers,
            brain: self.brain,
            hostname_to_node: self.hostname_to_node,
            remaining_iterations: 0,
            num_remaining_flows: 0,
            setting,
            seed,
            job_spec,
            allreduce_algorithm: None,
            vname_to_hostname: Default::default(),
            cluster: None,
        }
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
}

impl Application for AllreduceApp {
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
        self.setting.job_id
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.remaining_iterations = self.job_spec.num_iterations;
        self.allreduce_algorithm = Some(self.new_allreduce_algorithm());
        self.allreduce()?;

        Ok(())
    }

    fn on_event(&mut self, cmd: message::Command) -> anyhow::Result<()> {
        // wait for all flows to finish
        use message::Command::*;
        match cmd {
            FlowComplete(_flow) => {
                self.num_remaining_flows -= 1;
                if self.num_remaining_flows == 0 && self.remaining_iterations > 0 {
                    if self.setting.auto_tune.is_some()
                        && self.setting.auto_tune.unwrap() > 0
                        && self.remaining_iterations % self.setting.auto_tune.unwrap() == 0
                    {
                        self.cluster = None;
                        assert_eq!(self.setting.nethint_level, 2);
                        return self.request_nethint(NetHintVersion::V2);
                    }
                    self.allreduce()?;
                } else if self.num_remaining_flows == 0 && self.remaining_iterations == 0 {
                    self.finish()?;
                }
            }
            BrainResponse(msg) => {
                self.handle_brain_response_event(msg)?;
            }
            _ => {
                panic!("unexpected cmd: {:?}", cmd);
            }
        }
        Ok(())
    }
}

impl AllreduceApp {
    fn new_allreduce_algorithm(&self) -> Box<dyn AllReduceAlgorithm> {
        match self.setting.allreduce_policy {
            AllReducePolicy::Random => Box::new(RandomRingAllReduce::new(self.seed)),
            AllReducePolicy::TopologyAware => Box::new(TopologyAwareRingAllReduce::new(self.seed)),
            AllReducePolicy::RAT => Box::new(RatAllReduce::new()),
        }
    }

    pub fn allreduce(&mut self) -> anyhow::Result<()> {
        if self.cluster.is_none() {
            // self.request_provision()?;
            let version = match self.setting.nethint_level {
                1 => NetHintVersion::V1,
                2 => NetHintVersion::V2,
                _ => panic!("unexpected nethint_level: {}", self.setting.nethint_level),
            };
            self.request_nethint(version)?;
            return Ok(());
        }

        // then we have the hint, self.cluster is set
        if self.allreduce_algorithm.is_none() {
            self.allreduce_algorithm = Some(self.new_allreduce_algorithm());
        }

        let flows = self.allreduce_algorithm.as_mut().unwrap().allreduce(
            self.job_spec.buffer_size as u64,
            &**self.cluster.as_ref().unwrap(),
        );

        for flow in flows {
            let size = flow.bytes;
            let src_hostname = &self.vname_to_hostname[&flow.src];
            let dst_hostname = &self.vname_to_hostname[&flow.dst];
            let src_node = &self.hostname_to_node[src_hostname];
            let dst_node = &self.hostname_to_node[dst_hostname];

            let flow = Flow::new(size, src_node.clone(), dst_node.clone(), None);
            let cmd = message::Command::EmitFlow(flow);
            log::debug!("mapreduce::run, cmd: {:?}", cmd);
            let endpoint = self.workers.get_mut(src_node).unwrap();
            endpoint.post(cmd, None)?;

            self.num_remaining_flows += 1;
        }

        self.remaining_iterations -= 1;
        Ok(())
    }

    fn estimate_hintv2(&mut self, hintv2: NetHintV2Real) {
        unimplemented!("the problem is how to estimate nethint v2 from link traffic information. save to self.cluster");
    }

    fn handle_brain_response_event(
        &mut self,
        msg: nhagent::message::Message,
    ) -> anyhow::Result<()> {
        use nhagent::message::Message::*;
        let my_tenant_id = self.tenant_id();
        match msg {
            NetHintResponseV1(tenant_id, hintv1) => {
                assert_eq!(my_tenant_id, tenant_id);
                self.vname_to_hostname = hintv1.vname_to_hostname;
                self.cluster = Some(Rc::new(hintv1.vc));
                self.allreduce()?;
            }
            NetHintResponseV2(tenant_id, hintv2) => {
                assert_eq!(my_tenant_id, tenant_id);
                self.vname_to_hostname = hintv2.hintv1.vname_to_hostname.clone();
                self.estimate_hintv2(hintv2);
                self.allreduce()?;
            }
            _ => {
                panic!("unexpected brain response: {:?}", msg);
            }
        }
        Ok(())
    }

}
