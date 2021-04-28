use crate::{message, Flow, Node};
use litemsg::endpoint::Endpoint;
use std::collections::HashMap;
use std::rc::Rc;

use crate::controller::app::Application;
use crate::controller::plink::PlinkApp;
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
    cluster::{Topology, VirtCluster, LinkIx},
    hint::{NetHintV2Real, NetHintVersion},
    counterunit::{CounterType, CounterUnit},
    bandwidth::{Bandwidth, BandwidthTrait},
    TenantId, Timestamp,
};
use rand::{self, distributions::Distribution, rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

/// see mapreduce/experiment.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapReduceSetting {
    pub trace: std::path::PathBuf,
    pub job_id: usize, // which item in trace
    pub seed_base: u64,
    pub map_scale: f64,
    pub reduce_scale: f64, // scale down from the real trace to fit our testbed
    pub traffic_scale: f64,
    pub time_scale: f64,
    pub collocate: bool,
    pub mapper_policy: MapperPlacementPolicy,
    pub reducer_policy: ReducerPlacementPolicy,
    pub probe: ProbeConfig,
    pub nethint_level: usize,
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

    pub fn get_job_spec(setting: &MapReduceSetting) -> (Timestamp, JobSpec) {
        let job_trace = JobTrace::from_path(&setting.trace).unwrap_or_else(|e| {
            panic!(
                "failed to load from file: {:?}, error: {}",
                setting.trace, e
            )
        });

        // Read job information (start_ts, job_spec) from file
        let mut record = job_trace.records[setting.job_id].clone();
        // mutiply traffic by a number
        record.reducers = record
            .reducers
            .into_iter()
            .map(|(a, b)| (a, b * setting.traffic_scale))
            .collect();
        let start_ts = ((record.ts * 1_000_000) as f64 * setting.time_scale) as Timestamp;
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
    }

    pub fn build(self) -> Box<dyn Application> {
        let setting = self.load_config_from_file();
        log::info!("mapreduce setting: {:?}", setting);
        let job_spec = Self::get_job_spec(&setting).1;
        let seed = setting.seed_base + setting.job_id as u64;
        let mut app: Box<dyn Application> = Box::new(MapReduceApp {
            workers: self.workers,
            brain: self.brain,
            hostname_to_node: self.hostname_to_node,
            num_remaining_flows: 0,
            setting: setting.clone(),
            seed,
            job_spec: job_spec.clone(),
            vname_to_hostname: Default::default(),
            cluster: None,
        });

        if setting.probe.enable {
            let num_hosts = if setting.collocate {
                job_spec.num_map.max(job_spec.num_reduce)
            } else {
                job_spec.num_map + job_spec.num_reduce
            };
            app = Box::new(PlinkApp::new(num_hosts, setting.probe.round_ms, app));
        }

        app
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
                if m >= record.num_reduce {
                    // scale up
                    assert_eq!(m % record.num_reduce, 0);
                    let k = m / record.num_reduce;
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..n {
                        for j in 0..m {
                            pairs[i][j] = (record.reducers[j / k].1 / n as f64 * 1e6) as usize;
                        }
                    }
                } else {
                    // scale down
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..n {
                        for j in 0..record.num_reduce {
                            pairs[i][j % m] += (record.reducers[j].1 / n as f64 * 1e6) as usize;
                        }
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
                let r_vm_hostname = &self.vname_to_hostname[&reducers.0[j]];
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

    fn estimate_hintv2(&mut self, hintv2: NetHintV2Real) {

        // the implementation referes to allreduce.rs:estimate_hintv2 and nethint/src/hint.rs:estimate_v2
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
        // unimplemented!("the problem is how to estimate nethint v2 from link traffic information. save to self.cluster");
    }

    fn compute_fair_share_per_flow(
        vc: &VirtCluster,
        link_ix: LinkIx,
        interval_ms: u64,
        traffic: &Vec<CounterUnit>,
        plink_capacity: Bandwidth,
        direction: CounterType,
    ) -> Bandwidth {
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
            // for mapreduce
            let n = vc.num_hosts();
            let a = vc.get_downlinks(vc.get_node_index(&low_node.name)).count();
            n * a
        } else {
            // for mapreduce
            vc.num_hosts()
        };
        let mut demand_sum_bw = (8.0 * demand_sum as f64 / (interval_ms as f64 / 1000.0) / 1e9).gbps();
        if demand_sum_bw > plink_capacity {
            demand_sum_bw = plink_capacity;
        }
        std::cmp::max(
            plink_capacity - demand_sum_bw,
            plink_capacity / (num_flows + num_new_flows) * num_new_flows,
        )
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
