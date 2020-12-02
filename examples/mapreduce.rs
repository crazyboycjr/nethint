use log::{debug, info};
use rand::{self, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use nethint::bandwidth::{Bandwidth, BandwidthTrait};
use nethint::cluster::{Cluster, Node, NodeIx, NodeType, Topology};
use nethint::{
    AppEvent, Application, Event, Executor, Flow, Simulator, ToStdDuration, Trace, TraceRecord,
};

use spiril::population::Population;
use spiril::unit::Unit;

mod logging;

const RAND_SEED: u64 = 0;
thread_local! {
    pub static RNG: Rc<RefCell<StdRng>> = Rc::new(RefCell::new(StdRng::seed_from_u64(RAND_SEED)));
}

fn build_fatree_fake(nports: usize, bw: Bandwidth, oversub_ratio: f64) -> Cluster {
    assert!(
        nports % 2 == 0,
        "the number of ports of a switch is required to be even"
    );
    let k = nports;
    let num_pods = k;
    let _num_cores = k * k / 4;
    let num_aggs_in_pod = k / 2;
    let _num_aggs = num_pods * num_aggs_in_pod;
    let num_edges_in_pod = k / 2;
    let num_edges = num_pods * num_edges_in_pod;
    let num_hosts_under_edge = k / 2;
    let _num_hosts = num_edges * num_hosts_under_edge;

    let mut cluster = Cluster::new();
    let cloud = Node::new(&format!("cloud"), 1, NodeType::Switch);
    cluster.add_node(cloud);

    let mut host_id = 0;
    for i in 0..num_edges {
        let tor_name = format!("tor_{}", i);
        let tor = Node::new(&tor_name, 2, NodeType::Switch);
        cluster.add_node(tor);
        cluster.add_link_by_name(
            "cloud",
            &tor_name,
            bw * num_hosts_under_edge / oversub_ratio,
        );

        for j in host_id..host_id + num_hosts_under_edge {
            let host_name = format!("host_{}", j);
            let host = Node::new(&host_name, 3, NodeType::Host);
            cluster.add_node(host);
            cluster.add_link_by_name(&tor_name, &host_name, bw);
        }

        host_id += num_hosts_under_edge;
    }

    cluster
}

trait PlaceReducer {
    // from a high-level view, the inputs should be:
    // input1: a cluster of hosts with slots
    // input2: job/reduce task specification
    // input3: mapper placement
    // input4: shuffle flows
    fn place(
        &mut self,
        cluster: &Cluster,
        job_spec: &JobSpec,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
    ) -> Placement;
}

trait PlaceMapper {
    // from a high-level view, the inputs should be:
    // input1: a cluster of hosts with slots
    // input2: job specification
    fn place(&mut self, cluster: &Cluster, job_spec: &JobSpec) -> Placement;
}

#[derive(Debug)]
enum ReducerPlacementPolicy {
    Random,
    GeneticAlgorithm,
    HierarchicalGreedy,
}

#[derive(Debug, Default)]
struct RandomReducerScheduler {}

impl RandomReducerScheduler {
    fn new() -> Self {
        Default::default()
    }
}

impl PlaceReducer for RandomReducerScheduler {
    fn place(
        &mut self,
        cluster: &Cluster,
        job_spec: &JobSpec,
        mapper: &Placement,
        _shuffle_pairs: &Shuffle,
    ) -> Placement {
        RNG.with(|rng| {
            let mut rng = rng.borrow_mut();
            let num_hosts = cluster.num_hosts();
            let mut hosts: Vec<String> = (0..num_hosts).map(|x| format!("host_{}", x)).collect();
            hosts.retain(|h| mapper.0.iter().find(|&m| m.eq(h)).is_none());
            let hosts = hosts
                .choose_multiple(&mut *rng, job_spec.num_reduce)
                .cloned()
                .collect();
            Placement(hosts)
        })
    }
}

#[derive(Debug, Default)]
struct GeneticReducerScheduler {}

impl GeneticReducerScheduler {
    fn new() -> Self {
        Default::default()
    }
}

impl PlaceReducer for GeneticReducerScheduler {
    fn place(
        &mut self,
        cluster: &Cluster,
        job_spec: &JobSpec,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
    ) -> Placement {
        let units = RNG.with(|rng| {
            let mut rng = rng.borrow_mut();
            let num_hosts = cluster.num_hosts();
            let mut hosts: Vec<String> = (0..num_hosts).map(|x| format!("host_{}", x)).collect();
            hosts.retain(|h| mapper.0.iter().find(|&m| m.eq(h)).is_none());

            let units: Vec<JobPlacement> = (0..1000)
                .map(|_| JobPlacement {
                    mapper: mapper.clone(),
                    reducer: Placement(
                        hosts
                            .choose_multiple(&mut *rng, job_spec.num_reduce)
                            .cloned()
                            .collect(),
                    ),
                    shuffle: shuffle_pairs,
                    cluster,
                })
                .collect();
            units
        });

        let solutions = Population::new(units)
            .set_size(1000)
            .set_breed_factor(0.5)
            .set_survival_factor(0.8)
            .epochs_parallel(50, 1) // 1 CPU cores
            .finish();

        let job_placement = solutions.first().unwrap();
        debug!("fitness: {}", job_placement.fitness());
        // let hosts = vec![1, 7, 12, 13].into_iter().map(|x| format!("host_{}", x)).collect();
        // Placement(hosts)
        job_placement.reducer.clone()
    }
}

#[derive(Debug, Default)]
struct GreedyReducerScheduler {}

impl GreedyReducerScheduler {
    fn new() -> Self {
        Default::default()
    }
}

fn get_rack_id(cluster: &Cluster, h: &str) -> usize {
    let host_ix = cluster.get_node_index(h);
    let tor_ix = cluster.get_target(cluster.get_uplink(host_ix));
    let rack_id: usize = cluster[tor_ix].name.strip_prefix("tor_").unwrap().parse().unwrap();
    rack_id
}

impl PlaceReducer for GreedyReducerScheduler {
    fn place(
        &mut self,
        cluster: &Cluster,
        job_spec: &JobSpec,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
    ) -> Placement {
        // two-level allocation
        // first, find a best rack to place the reducer
        // second, find the best node in that rack
        let mut placement = Vec::new();
        let num_racks = cluster.num_switches() - 1;
        let mut rack_taken = vec![0; num_racks];
        let mut rack = vec![0; num_racks];

        let mut taken: HashSet<NodeIx> =
            mapper.0.iter().map(|m| cluster.get_node_index(m)).collect();
        mapper.0.iter().for_each(|m| {
            let rack_id = get_rack_id(cluster, m);
            rack_taken[rack_id] += 1;
        });

        for j in 0..job_spec.num_reduce {
            let mut min_est = f64::MAX;
            let mut best_rack = 0;

            for i in 0..num_racks {
                let mut est = 0.;
                let tor_ix = cluster.get_node_index(&format!("tor_{}", i));
                let link_ix = cluster.get_uplink(tor_ix);
                let rack_bw = cluster[link_ix].bandwidth;

                // exclude the case that the rack is full
                // debug!("rack_taken[{}]: {} {}", i, rack_taken[i], cluster.get_downlinks(tor_ix).len());
                if rack_taken[i] == cluster.get_downlinks(tor_ix).len() {
                    continue;
                }

                for (mi, m) in mapper.0.iter().enumerate() {
                    let m = cluster.get_node_index(&m);
                    // let m_bw = cluster[cluster.get_uplink(m)].bandwidth;
                    let rack_m = cluster.get_target(cluster.get_uplink(m));
                    if rack_m != tor_ix {
                        est +=
                            (shuffle_pairs.0[mi][j] * (rack[i] + 1)) as f64 / rack_bw.val() as f64;
                    }
                }

                if min_est > est {
                    min_est = est;
                    best_rack = i;
                }
            }

            // get the best_rack
            rack[best_rack] += 1;
            rack_taken[best_rack] += 1;
            // debug!("best_rack: {}", best_rack);

            // fix the rack, find the best node in the rack
            let tor_ix = cluster.get_node_index(&format!("tor_{}", best_rack));
            let downlink = cluster
                .get_downlinks(tor_ix)
                .filter(|&&downlink| !taken.contains(&cluster.get_target(downlink)))
                .max_by_key(|&&downlink| cluster[downlink].bandwidth)
                .unwrap();
            let best_node_ix = cluster.get_target(*downlink);
            let best_node = cluster[best_node_ix].name.clone();

            // here we get the best node
            taken.insert(best_node_ix);
            placement.push(best_node);
        }

        Placement(placement)
    }
}

#[derive(Debug, Default)]
struct MapperScheduler {
    seed: u64,
}

impl MapperScheduler {
    fn new(seed: u64) -> Self {
        MapperScheduler { seed }
    }
}

impl PlaceMapper for MapperScheduler {
    fn place(&mut self, cluster: &Cluster, job_spec: &JobSpec) -> Placement {
        // here we just consider the case where there is only one single job
        let mut rng = StdRng::seed_from_u64(self.seed);
        let num_hosts = cluster.num_hosts();
        let hosts = (0..num_hosts)
            .collect::<Vec<_>>()
            .choose_multiple(&mut rng, job_spec.num_map)
            .map(|x| format!("host_{}", x))
            .collect();
        Placement(hosts)
    }
}

#[derive(Debug, Clone)]
struct Placement(Vec<String>);

#[derive(Debug, Clone)]
struct JobPlacement<'s> {
    mapper: Placement,
    reducer: Placement,
    shuffle: &'s Shuffle,
    cluster: &'s Cluster,
}

impl<'s> Unit for JobPlacement<'s> {
    fn fitness(&self) -> f64 {
        let num_racks = self.cluster.num_switches() - 1;
        let mut rack = vec![0; num_racks];
        let mut traffic = vec![vec![0; num_racks]; self.mapper.0.len()];

        self.reducer.0.iter().enumerate().for_each(|(j, r)| {
            let id = get_rack_id(self.cluster, r);
            self.mapper.0.iter().enumerate().for_each(|(i, m)| {
                let rack_m = get_rack_id(self.cluster, m);
                if id != rack_m {
                    traffic[i][id] += self.shuffle.0[i][j];
                }
            });
        });

        self.reducer.0.iter().for_each(|r| {
            let id = get_rack_id(self.cluster, r);
            rack[id] += 1;
        });

        let mut res: f64 = 0.;
        for (j, r) in self.reducer.0.iter().enumerate() {
            let mut inner_est = 0.;
            let mut outer_est = 0.;
            let rack_r = get_rack_id(self.cluster, r);

            let r_ix = self.cluster.get_node_index(r);
            let tor_ix = self.cluster.get_target(self.cluster.get_uplink(r_ix));
            let rack_bw = self.cluster[self.cluster.get_uplink(tor_ix)].bandwidth;

            for (i, m) in self.mapper.0.iter().enumerate() {
                let m_ix = self.cluster.get_node_index(m);
                let tor_m_ix = self.cluster.get_target(self.cluster.get_uplink(m_ix));
                let m_bw = self.cluster[self.cluster.get_uplink(m_ix)].bandwidth;

                if tor_ix == tor_m_ix {
                    inner_est += self.shuffle.0[i][j] as f64 / m_bw.val() as f64;
                } else {
                    outer_est += traffic[i][rack_r] as f64 / rack_bw.val() as f64;
                }
            }

            assert!(rack[rack_r] > 0);
            let acc_time = inner_est.max(outer_est);
            res = res.max(acc_time);
        }

        // 1 - sigmoid(res)
        1.0 - 1.0 / (1.0 + (-res).exp())
    }

    fn breed_with(&self, other: &JobPlacement) -> Self {
        RNG.with(|rng| {
            let mut rng = rng.borrow_mut();
            let num_intersect = rng.gen_range(0, self.reducer.0.len() / 2 + 1);
            let indices: Vec<usize> = (0..self.reducer.0.len())
                .collect::<Vec<usize>>()
                .choose_multiple(&mut *rng, num_intersect)
                .map(|&x| x)
                .collect();
            let mut result = self.clone();
            for i in indices {
                result.reducer.0[i] = other.reducer.0[i].clone();
            }
            result.reducer.0.dedup();
            if result.reducer.0.len() < self.reducer.0.len() {
                result.reducer = self.reducer.clone();
            }
            result
        })
    }
}

#[derive(Debug, Clone, Default)]
struct JobSpec {
    num_map: usize,
    num_reduce: usize,
    // cpu_slots: usize,
    // mem_slots: usize,
}

impl JobSpec {
    fn new(num_map: usize, num_reduce: usize) -> Self {
        JobSpec {
            num_map,
            num_reduce,
        }
    }
}

#[derive(Debug)]
struct Shuffle(Vec<Vec<usize>>);

struct MapReduceApp<'c> {
    job_spec: JobSpec,
    cluster: &'c Cluster,
    reducer_place_policy: ReducerPlacementPolicy,
    replayer: nethint::Replayer,
}

impl<'c> MapReduceApp<'c> {
    fn new(
        job_spec: JobSpec,
        cluster: &'c Cluster,
        reducer_place_policy: ReducerPlacementPolicy,
    ) -> Self {
        let trace = Trace::new();
        MapReduceApp {
            job_spec,
            cluster,
            reducer_place_policy,
            replayer: nethint::Replayer::new(trace),
        }
    }

    fn finish_map_stage(&mut self, seed: u64) {
        let shuffle = self.generate_shuffle_flows(seed);
        info!("shuffle: {:?}", shuffle);

        let mut map_scheduler = MapperScheduler::new(seed);
        let mappers = map_scheduler.place(self.cluster, &self.job_spec);
        info!("mappers: {:?}", mappers);

        let mut reduce_scheduler: Box<dyn PlaceReducer> = match self.reducer_place_policy {
            ReducerPlacementPolicy::Random => Box::new(RandomReducerScheduler::new()),
            ReducerPlacementPolicy::GeneticAlgorithm => Box::new(GeneticReducerScheduler::new()),
            ReducerPlacementPolicy::HierarchicalGreedy => Box::new(GreedyReducerScheduler::new()),
        };
        let reducers = reduce_scheduler.place(self.cluster, &self.job_spec, &mappers, &shuffle);
        info!("reducers: {:?}", reducers);

        // reinitialize replayer with new trace
        let mut trace = Trace::new();
        for i in 0..self.job_spec.num_map {
            for j in 0..self.job_spec.num_reduce {
                let flow = Flow::new(shuffle.0[i][j], &mappers.0[i], &reducers.0[j], None);
                let rec = TraceRecord::new(0, flow, None);
                trace.add_record(rec);
            }
        }

        self.replayer = nethint::Replayer::new(trace);
    }

    fn generate_shuffle_flows(&mut self, seed: u64) -> Shuffle {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut pairs = Vec::new();
        for _i in 0..self.job_spec.num_map {
            let data_size = 1_000_000 * rng.gen_range(1, 5);
            let nums: Vec<usize> = std::iter::repeat_with(|| rng.gen_range(1, 5))
                .take(self.job_spec.num_reduce)
                .collect();
            let sum: usize = nums.iter().map(|x| *x).sum();
            let spreads: Vec<usize> = nums.into_iter().map(|x| x * data_size / sum).collect();
            pairs.push(spreads);
        }

        Shuffle(pairs)
    }
}

impl<'c> Application for MapReduceApp<'c> {
    fn on_event(&mut self, event: AppEvent) -> Event {
        self.replayer.on_event(event)
    }
}

fn run_map_reduce(
    cluster: &Cluster,
    reduce_place_policy: ReducerPlacementPolicy,
    seed: u64,
) -> Trace {
    let mut simulator = Simulator::new(cluster.clone());

    let job_spec = JobSpec::new(4, 5);
    let mut app = Box::new(MapReduceApp::new(job_spec, cluster, reduce_place_policy));
    app.finish_map_stage(seed);

    let output = simulator.run_with_appliation(app);

    // println!("{:#?}", output);
    output
}

fn main() {
    logging::init_log();

    let nports = 4;
    let oversub_ratio = 2.0;
    let cluster = build_fatree_fake(nports, 100.gbps(), oversub_ratio);
    assert_eq!(cluster.num_hosts(), nports * nports * nports / 4);

    for i in 0..100 {
        let output = run_map_reduce(&cluster, ReducerPlacementPolicy::Random, i);
        let time1 = output.recs.into_iter().map(|r| r.dura.unwrap()).max();

        let output = run_map_reduce(&cluster, ReducerPlacementPolicy::GeneticAlgorithm, i);
        let time2 = output.recs.into_iter().map(|r| r.dura.unwrap()).max();

        let output = run_map_reduce(&cluster, ReducerPlacementPolicy::HierarchicalGreedy, i);
        let time3 = output.recs.into_iter().map(|r| r.dura.unwrap()).max();

        info!(
            "{:?}, job_finish_time: {:?}",
            ReducerPlacementPolicy::Random,
            time1.unwrap().to_dura()
        );
        info!(
            "{:?}, job_finish_time: {:?}",
            ReducerPlacementPolicy::GeneticAlgorithm,
            time2.unwrap().to_dura()
        );
        info!(
            "{:?}, job_finish_time: {:?}",
            ReducerPlacementPolicy::HierarchicalGreedy,
            time3.unwrap().to_dura()
        );
    }
}
