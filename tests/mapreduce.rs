#[cfg(test)]
mod logging;

use log::{debug, info};
use rand::{self, Rng, SeedableRng};

use nethint::bandwidth::{Bandwidth, BandwidthTrait};
use nethint::cluster::{Cluster, Node};
use nethint::{
    AppEvent, Application, Event, Executor, Flow, Simulator, ToStdDuration, Trace, TraceRecord,
};

use spiril::population::Population;
use spiril::unit::Unit;

const RAND_SEED: u64 = 0;

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
    let cloud = Node::new(&format!("cloud"), 1);
    cluster.add_node(cloud);

    let mut host_id = 0;
    for i in 0..num_edges {
        let tor_name = format!("tor_{}", i);
        let tor = Node::new(&tor_name, 2);
        cluster.add_node(tor);
        cluster.add_link_by_name(
            "cloud",
            &tor_name,
            bw * num_hosts_under_edge / oversub_ratio,
        );

        for j in host_id..host_id + num_hosts_under_edge {
            let host_name = format!("host_{}", j);
            let host = Node::new(&host_name, 3);
            cluster.add_node(host);
            cluster.add_link_by_name(&tor_name, &host_name, bw);
        }

        host_id += num_hosts_under_edge;
    }

    cluster
}

trait ReducerPlacementPolicy {
    // from a high-level view, the inputs should be:
    // input1: a cluster of hosts with slots
    // input2: job/reduce task specification
    // input3: mapper placement
    // input4: shuffle flows
    fn place(
        &mut self,
        num_hosts: usize,
        num_reduce: usize,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
    ) -> Placement;
}

trait MapperPlacementPolicy {
    // from a high-level view, the inputs should be:
    // input1: a cluster of hosts with slots
    // input2: job specification
    fn place(&mut self, num_hosts: usize, num_map: usize) -> Placement;
}

#[derive(Debug, Default)]
struct RandomReducerScheduler {}

impl RandomReducerScheduler {
    fn new() -> Self {
        Default::default()
    }
}

impl ReducerPlacementPolicy for RandomReducerScheduler {
    fn place(
        &mut self,
        num_hosts: usize,
        num_reduce: usize,
        mapper: &Placement,
        _shuffle_pairs: &Shuffle,
    ) -> Placement {
        use rand::seq::SliceRandom;
        let mut rng2 = rand::rngs::StdRng::seed_from_u64(RAND_SEED);
        let mut hosts: Vec<String> = (0..num_hosts).map(|x| format!("host_{}", x)).collect();
        hosts.retain(|h| mapper.0.iter().find(|&m| m.eq(h)).is_none());
        let hosts = hosts
            .choose_multiple(&mut rng2, num_reduce)
            .cloned()
            .collect();
        Placement(hosts)
    }
}

#[derive(Debug, Default)]
struct HeuristicReducerScheduler {}

impl HeuristicReducerScheduler {
    fn new() -> Self {
        Default::default()
    }
}

impl ReducerPlacementPolicy for HeuristicReducerScheduler {
    fn place(
        &mut self,
        num_hosts: usize,
        num_reduce: usize,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
    ) -> Placement {
        use rand::seq::SliceRandom;
        let mut rng2 = rand::rngs::StdRng::seed_from_u64(RAND_SEED);
        let mut hosts: Vec<String> = (0..num_hosts).map(|x| format!("host_{}", x)).collect();
        hosts.retain(|h| mapper.0.iter().find(|&m| m.eq(h)).is_none());

        let units: Vec<JobPlacement> = (0..1000)
            .map(|_| JobPlacement {
                mapper: mapper.clone(),
                reducer: Placement(
                    hosts
                        .choose_multiple(&mut rng2, num_reduce)
                        .cloned()
                        .collect(),
                ),
                shuffle: shuffle_pairs,
            })
            .collect();

        let solutions = Population::new(units)
            .set_size(1000)
            .set_breed_factor(0.3)
            .set_survival_factor(0.5)
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
struct MapperScheduler {}

impl MapperScheduler {
    fn new() -> Self {
        Default::default()
    }
}

impl MapperPlacementPolicy for MapperScheduler {
    fn place(&mut self, num_hosts: usize, num_map: usize) -> Placement {
        // here we just consider the case where there is only one single job
        use rand::seq::SliceRandom;
        let mut rng2 = rand::rngs::StdRng::seed_from_u64(RAND_SEED);
        let hosts = (0..num_hosts)
            .collect::<Vec<_>>()
            .choose_multiple(&mut rng2, num_map)
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
}

impl<'s> Unit for JobPlacement<'s> {
    fn fitness(&self) -> f64 {
        let mut rack = vec![0; 16];
        for m in self.mapper.0.iter().chain(self.reducer.0.iter()) {
            let idm: usize = m.strip_prefix("host_").unwrap().parse().unwrap();
            rack[idm] += 1;
        }
        let mut res: f64 = 0.;
        for (i, m) in self.mapper.0.iter().enumerate() {
            let mut acc_time = 0f64;
            for (j, r) in self.reducer.0.iter().enumerate() {
                let idm: usize = m.strip_prefix("host_").unwrap().parse().unwrap();
                let idr: usize = r.strip_prefix("host_").unwrap().parse().unwrap();
                assert_ne!(idm, idr);
                if idm / 2 == idr / 2 {
                    // same rack
                    acc_time += self.shuffle.0[i][j] as f64;
                } else {
                    // different racks
                    acc_time += self.shuffle.0[i][j] as f64 / rack[idr] as f64;
                }
            }
            res = res.max(acc_time);
        }
        res
    }

    fn breed_with(&self, other: &JobPlacement) -> Self {
        use rand::seq::SliceRandom;
        let mut rng = rand::rngs::StdRng::seed_from_u64(RAND_SEED);
        let num_intersect = rng.gen_range(0, self.reducer.0.len() / 2 + 1);
        let indices: Vec<usize> = (0..self.reducer.0.len())
            .collect::<Vec<usize>>()
            .choose_multiple(&mut rng, num_intersect)
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
    }
}

#[derive(Debug)]
struct Shuffle(Vec<Vec<usize>>);

struct MapReduceApp {
    num_map: usize,
    num_reduce: usize,
    num_hosts: usize,
    replayer: nethint::Replayer,
}

impl MapReduceApp {
    fn new(num_map: usize, num_reduce: usize, num_hosts: usize) -> Self {
        let trace = Trace::new();
        MapReduceApp {
            num_map,
            num_reduce,
            num_hosts,
            replayer: nethint::Replayer::new(trace),
        }
    }

    fn finish_map_stage(&mut self) {
        let shuffle = self.generate_shuffle_flows();
        info!("shuffle: {:?}", shuffle);

        let mut map_scheduler = MapperScheduler::new();
        let mappers = map_scheduler.place(self.num_hosts, self.num_map);
        info!("mappers: {:?}", mappers);

        let mut reduce_scheduler = RandomReducerScheduler::new();
        // let mut reduce_scheduler = HeuristicReducerScheduler::new();
        let reducers = reduce_scheduler.place(self.num_hosts, self.num_reduce, &mappers, &shuffle);
        info!("reducers: {:?}", reducers);

        // reinitialize replayer with new trace
        let mut trace = Trace::new();
        for i in 0..self.num_map {
            for j in 0..self.num_reduce {
                let flow = Flow::new(shuffle.0[i][j], &mappers.0[i], &reducers.0[j], None);
                let rec = TraceRecord::new(0, flow, None);
                trace.add_record(rec);
            }
        }

        self.replayer = nethint::Replayer::new(trace);
    }

    fn generate_shuffle_flows(&mut self) -> Shuffle {
        let mut rng2 = rand::rngs::StdRng::seed_from_u64(RAND_SEED);

        let mut pairs = Vec::new();
        for _i in 0..self.num_map {
            let data_size = 1_000_000 * rng2.gen_range(1, 5);
            let nums: Vec<usize> = std::iter::repeat_with(|| rng2.gen_range(1, 5))
                .take(self.num_reduce)
                .collect();
            let sum: usize = nums.iter().map(|x| *x).sum();
            let spreads: Vec<usize> = nums.into_iter().map(|x| x * data_size / sum).collect();
            pairs.push(spreads);
        }

        Shuffle(pairs)
    }
}

impl Application for MapReduceApp {
    fn on_event(&mut self, event: AppEvent) -> Event {
        self.replayer.on_event(event)
    }
}

#[test]
fn main() {
    // env_logger::init();
    logging::init_log();

    let nports = 4;
    let oversub_ratio = 2.0;
    let cluster = build_fatree_fake(nports, 100.gbps(), oversub_ratio);

    let nhosts = nports * nports * nports / 4;
    // let trace = alltoall_trace(nhosts, 1_000_000);

    let mut simulator = Simulator::new(cluster);

    let mut app = Box::new(MapReduceApp::new(4, 4, nhosts));
    app.finish_map_stage();

    let output = simulator.run_with_appliation(app);
    println!("{:#?}", output);

    let job_finish_time = output.recs.into_iter().map(|r| r.dura.unwrap()).max();
    println!("job_finish_time: {:?}", job_finish_time.unwrap().to_dura());
}
