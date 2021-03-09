use crate::{message, Flow, Node};
use litemsg::endpoint::Endpoint;
use std::collections::HashMap;
use std::rc::Rc;

use mapreduce::{
    mapper::MapperPlacementPolicy, trace::JobTrace, JobSpec, ReducerPlacementPolicy, ShufflePattern,
    GreedyReducerLevel1Scheduler, GreedyReducerScheduler, GreedyReducerSchedulerPaper,
    PlaceMapper, PlaceReducer, Placement, RandomReducerScheduler, Shuffle,
};
use nethint::{
    cluster::{Cluster, Topology},
    hint::{NetHintV1Real, NetHintV2Real, NetHintVersion},
    Duration, Trace, TraceRecord, TenantId,
};
use rand::{self, distributions::Distribution, rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ProbeConfig {
    enable: bool,
    #[serde(default)]
    round_ms: u64,
}

/// see mapreduce/experiment.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapReduceSetting {
    trace: std::path::PathBuf,
    job_id: usize, // which item in trace
    seed_base: u64,
    map_scale: f64,
    reduce_scale: f64, // scale down from the real trace to fit our testbed
    traffic_scale: f64,
    collocate: bool,
    mapper_policy: MapperPlacementPolicy,
    reducer_policy: ReducerPlacementPolicy,
    probe: ProbeConfig,
    nethint_level: usize,
}

pub struct MapReduceAppBuilder {
    config_path: std::path::PathBuf,
    workers: HashMap<Node, Endpoint>,
    brain: Endpoint,
}

impl MapReduceAppBuilder {
    pub fn new(config_path: std::path::PathBuf, workers: HashMap<Node, Endpoint>, brain: Endpoint) -> Self {
        MapReduceAppBuilder {
            config_path,
            workers,
            brain,
        }
    }

    fn load_config_from_file(&self) -> MapReduceSetting {
        use std::io::Read;
        let mut file = std::fs::File::open(&self.config_path).expect("fail to open file");
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        toml::from_str(&content).expect("parse failed")
    }

    fn get_job_spec(setting: &MapReduceSetting) -> JobSpec {
        let job_trace = JobTrace::from_path(&setting.trace).unwrap_or_else(|e| {
            panic!(
                "failed to load from file: {:?}, error: {}",
                setting.trace, e
            )
        });

        // Read job information (start_ts, job_spec) from file
        let (_start_ts, job_spec) = {
            let mut record = job_trace.records[setting.job_id].clone();
            // mutiply traffic by a number
            record.reducers = record
                .reducers
                .into_iter()
                .map(|(a, b)| (a, b * setting.traffic_scale))
                .collect();
            let start_ts = record.ts * 1_000_000;
            log::debug!("record: {:?}", record);
            let job_spec = JobSpec::new(
                std::cmp::max(1, (record.num_map as f64 * setting.map_scale) as usize),
                std::cmp::max(
                    1,
                    (record.num_reduce as f64 * setting.reduce_scale) as usize,
                ),
                ShufflePattern::FromTrace(Box::new(record)),
            );
            (start_ts, job_spec)
        };

        job_spec
    }

    pub fn build(self) -> MapReduceApp {
        let setting = self.load_config_from_file();
        log::info!("mapreduce setting: {:?}", setting);
        let job_spec = Self::get_job_spec(&setting);
        let seed = setting.seed_base + setting.job_id as u64;
        MapReduceApp {
            workers: self.workers,
            brain: self.brain,
            num_remaining_flows: 0,
            setting,
            seed,
            job_spec,
            // hint1: None,
            // hint2: None,
            cluster: None,
        }
    }
}

pub struct MapReduceApp {
    workers: HashMap<Node, Endpoint>,
    brain: Endpoint,
    num_remaining_flows: usize,

    setting: MapReduceSetting,
    seed: u64,
    job_spec: JobSpec,
    // hint1: Option<NetHintV1Real>,
    // hint2: Option<NetHintV2Real>,
    cluster: Option<Rc<dyn Topology>>, // use Rc so we don't have deal with ugly lifetime specifiers
}

impl MapReduceApp {
    pub fn workers(&self) -> &HashMap<Node, Endpoint> {
        &self.workers
    }

    pub fn workers_mut(&mut self) -> &mut HashMap<Node, Endpoint> {
        &mut self.workers
    }

    pub fn brain(&self) -> &Endpoint {
        &self.brain
    }

    pub fn brain_mut(&mut self) -> &mut Endpoint {
        &mut self.brain
    }

    pub fn tenant_id(&self) -> TenantId {
        self.setting.job_id
    }

    fn generate_shuffle(&mut self, seed: u64) -> Shuffle {
        let mut rng = StdRng::seed_from_u64(seed);
        let n = self.job_spec.num_map;
        let m = self.job_spec.num_reduce;
        let mut pairs = vec![vec![0; m]; n];
        match &self.job_spec.shuffle_pat {
            &ShufflePattern::Uniform(n) => {
                pairs.iter_mut().for_each(|v| {
                    v.iter_mut().for_each(|i| *i = rng.gen_range(0..n as usize));
                });
            }
            &ShufflePattern::Zipf(n, s) => {
                let zipf = zipf::ZipfDistribution::new(1000, s).unwrap();
                pairs.iter_mut().for_each(|v| {
                    v.iter_mut()
                        .for_each(|i| *i = n as usize * zipf.sample(&mut rng) / 10);
                });
            }
            ShufflePattern::FromTrace(record) => {
                assert_eq!(m % record.num_reduce, 0);
                let k = m / record.num_reduce;
                #[allow(clippy::needless_range_loop)]
                for i in 0..n {
                    for j in 0..m {
                        pairs[i][j] = (record.reducers[j / k].1 / n as f64 * 1e6) as usize;
                    }
                }
            }
        }
        Shuffle(pairs)
    }

    fn request_provision(&mut self) -> anyhow::Result<()> {
        let nhosts_to_acquire = std::cmp::max(self.job_spec.num_map, self.job_spec.num_reduce);
        let msg = nhagent::message::Message::ProvisionRequest(self.tenant_id(), nhosts_to_acquire);
        self.brain.post(msg, None)?;
        Ok(())
    }

    fn request_nethint(&mut self) -> anyhow::Result<()> {
        let msg = nhagent::message::Message::NetHintRequest(self.tenant_id(), NetHintVersion::V2);
        self.brain.post(msg, None)?;
        Ok(())
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        if self.cluster.is_none() {
            self.request_provision()?;
        } else {

        }

        let workers: Vec<_> = self.workers.keys().cloned().collect();
        let n = workers.len();
        let mappers = &workers[..n / 2];
        let reducers = &workers[n / 2..];

        log::info!("mappers: {:?}", mappers);
        log::info!("reducers: {:?}", reducers);

        // emit flows
        for m in mappers {
            for r in reducers {
                let flow = Flow::new(1_000_000_000, m.clone(), r.clone(), None);
                let cmd = message::Command::EmitFlow(flow);
                log::trace!("mapreduce::run, cmd: {:?}", cmd);
                let endpoint = self.workers.get_mut(&m).unwrap();
                endpoint.post(cmd, None)?;
                self.num_remaining_flows += 1;
            }
        }

        Ok(())
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        for worker in self.workers.values_mut() {
            worker.post(message::Command::AppFinish, None)?;
        }
        Ok(())
    }

    fn estimate_hintv2(&mut self, hintv2: NetHintV2Real) {
        unimplemented!("the problem is how to estimate nethint v2 from link traffic information. save to self.cluster");
    }

    fn handle_brain_response_event(&mut self, msg: nhagent::message::Message) -> anyhow::Result<()> {
        use nhagent::message::Message::*;
        let my_tenant_id = self.tenant_id();
        match msg {
            ProvisionResponse(tenant_id, hintv1) => {
                assert_eq!(my_tenant_id, tenant_id);
                // self.hint1 = Some(hintv1);
                self.cluster = Some(Rc::new(hintv1.vc));
            }
            NetHintResponse(tenant_id, hintv2) => {
                assert_eq!(my_tenant_id, tenant_id);
                self.estimate_hintv2(hintv2);
            }
            _ => {
                panic!("unexpected brain response: {:?}", msg);
            }
        }
        Ok(())
    }

    pub fn on_event(&mut self, cmd: message::Command) -> anyhow::Result<()> {
        // wait for all flows to finish
        use message::Command::*;
        match cmd {
            FlowComplete(_flow) => {
                self.num_remaining_flows -= 1;
                if self.num_remaining_flows == 0 {
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
