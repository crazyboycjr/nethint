use crate::{message, Flow, Node};
use litemsg::endpoint::Endpoint;
use std::collections::HashMap;
use std::rc::Rc;

use crate::controller::app::Application;
use mapreduce::{
    config::ProbeConfig,
    mapper::{
        GreedyMapperScheduler, MapperPlacementPolicy, RandomMapperScheduler,
        RandomSkewMapperScheduler, TraceMapperScheduler,
    },
    trace::JobTrace,
    GreedyReducerLevel1Scheduler, GreedyReducerScheduler, GreedyReducerSchedulerPaper, JobSpec,
    PlaceMapper, PlaceReducer, Placement, RandomReducerScheduler, ReducerPlacementPolicy, Shuffle,
    ShufflePattern,
};
use nethint::{
    cluster::Topology,
    hint::{NetHintV2Real, NetHintVersion},
    TenantId,
};
use rand::{self, distributions::Distribution, rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

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
    hostname_to_node: HashMap<String, Node>,
}

impl MapReduceAppBuilder {
    pub fn new(
        config_path: std::path::PathBuf,
        workers: HashMap<Node, Endpoint>,
        brain: Endpoint,
        hostname_to_node: HashMap<String, Node>,
    ) -> Self {
        MapReduceAppBuilder {
            config_path,
            workers,
            brain,
            hostname_to_node,
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
            hostname_to_node: self.hostname_to_node,
            num_remaining_flows: 0,
            setting,
            seed,
            job_spec,
            // hint1: None,
            // hint2: None,
            vname_to_hostname: Default::default(),
            cluster: None,
        }
    }
}

pub struct MapReduceApp {
    workers: HashMap<Node, Endpoint>,
    brain: Endpoint,
    hostname_to_node: HashMap<String, Node>,
    num_remaining_flows: usize,

    setting: MapReduceSetting,
    seed: u64,
    job_spec: JobSpec,
    // hint1: Option<NetHintV1Real>,
    // hint2: Option<NetHintV2Real>,
    vname_to_hostname: HashMap<String, String>,
    cluster: Option<Rc<dyn Topology>>, // use Rc so we don't have deal with ugly lifetime specifiers
}

impl Application for MapReduceApp {
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

    fn hostname_to_node(&self) -> &HashMap<String, Node> {
        &self.hostname_to_node
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.cluster.is_none() {
            // self.request_provision()?;
            let version = match self.setting.nethint_level {
                1 => NetHintVersion::V1,
                2 => NetHintVersion::V2,
                _ => panic!("unexpected nethint_level: {}", self.setting.nethint_level),
            };
            self.request_nethint(version)?;
        } else {
            let shuffle = self.generate_shuffle(self.seed);
            log::info!("shuffle: {:?}", shuffle);

            let mappers = self.place_map();
            let reducers = self.place_reduce(&mappers, &shuffle);
            // in shuffle, we do submit the flows
            self.shuffle(shuffle, mappers, reducers)?;
        }

        Ok(())
    }

    fn on_event(&mut self, cmd: message::Command) -> anyhow::Result<bool> {
        // wait for all flows to finish
        use message::Command::*;
        match cmd {
            FlowComplete(_flow) => {
                self.num_remaining_flows -= 1;
                if self.num_remaining_flows == 0 {
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

impl MapReduceApp {
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

    fn place_map(&self) -> Placement {
        let mut map_scheduler: Box<dyn PlaceMapper> = match self.setting.mapper_policy {
            MapperPlacementPolicy::Random(seed) => Box::new(RandomMapperScheduler::new(seed)),
            MapperPlacementPolicy::FromTrace(ref record) => {
                Box::new(TraceMapperScheduler::new(record.clone()))
            }
            MapperPlacementPolicy::Greedy => Box::new(GreedyMapperScheduler::new()),
            MapperPlacementPolicy::RandomSkew(seed, s) => {
                Box::new(RandomSkewMapperScheduler::new(seed, s))
            }
        };
        let mappers = map_scheduler.place(&**self.cluster.as_ref().unwrap(), &self.job_spec);
        log::info!("mappers: {:?}", mappers);
        mappers
    }

    fn place_reduce(&self, mappers: &Placement, shuffle: &Shuffle) -> Placement {
        let mut reduce_scheduler: Box<dyn PlaceReducer> = match self.setting.reducer_policy {
            ReducerPlacementPolicy::Random => Box::new(RandomReducerScheduler::new()),
            ReducerPlacementPolicy::GeneticAlgorithm => {
                panic!("do not use genetic algorithm");
                // Box::new(GeneticReducerScheduler::new())
            }
            ReducerPlacementPolicy::HierarchicalGreedy => Box::new(GreedyReducerScheduler::new()),
            ReducerPlacementPolicy::HierarchicalGreedyLevel1 => {
                Box::new(GreedyReducerLevel1Scheduler::new())
            }
            ReducerPlacementPolicy::HierarchicalGreedyPaper => {
                Box::new(GreedyReducerSchedulerPaper::new())
            }
        };

        let reducers = reduce_scheduler.place(
            &**self.cluster.as_ref().unwrap(),
            &self.job_spec,
            &mappers,
            &shuffle,
            self.setting.collocate,
        );
        log::info!("reducers: {:?}", reducers);
        reducers
    }

    fn shuffle(
        &mut self,
        shuffle: Shuffle,
        mappers: Placement,
        reducers: Placement,
    ) -> anyhow::Result<()> {
        for i in 0..self.job_spec.num_map {
            for j in 0..self.job_spec.num_reduce {
                // no translation
                let m_vm_hostname = &self.vname_to_hostname[&mappers.0[i]];
                let r_vm_hostname = &self.vname_to_hostname[&reducers.0[i]];
                let m = &self.hostname_to_node[m_vm_hostname];
                let r = &self.hostname_to_node[r_vm_hostname];
                let flow = Flow::new(shuffle.0[i][j], m.clone(), r.clone(), None);
                let cmd = message::Command::EmitFlow(flow);
                log::debug!("mapreduce::run, cmd: {:?}", cmd);
                let endpoint = self.workers.get_mut(m).unwrap();
                endpoint.post(cmd, None)?;
                self.num_remaining_flows += 1;
            }
        }
        Ok(())
    }

    // fn request_provision(&mut self) -> anyhow::Result<()> {
    //     let nhosts_to_acquire = std::cmp::max(self.job_spec.num_map, self.job_spec.num_reduce);
    //     let msg = nhagent::message::Message::ProvisionRequest(self.tenant_id(), nhosts_to_acquire);
    //     self.brain.post(msg, None)?;
    //     Ok(())
    // }

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
            // ProvisionResponse(tenant_id, hintv1) => {
            //     assert_eq!(my_tenant_id, tenant_id);
            //     // self.hint1 = Some(hintv1);
            //     self.vname_to_hostname = hintv1.vname_to_hostname;
            //     self.cluster = Some(Rc::new(hintv1.vc));
            // }
            NetHintResponseV1(tenant_id, hintv1) => {
                assert_eq!(my_tenant_id, tenant_id);
                self.vname_to_hostname = hintv1.vname_to_hostname;
                self.cluster = Some(Rc::new(hintv1.vc));
                self.start()?;
            }
            NetHintResponseV2(tenant_id, hintv2) => {
                assert_eq!(my_tenant_id, tenant_id);
                self.vname_to_hostname = hintv2.hintv1.vname_to_hostname.clone();
                self.estimate_hintv2(hintv2);
                self.start()?;
            }
            _ => {
                panic!("unexpected brain response: {:?}", msg);
            }
        }
        Ok(())
    }
}
