use log::info;
use rand::{self, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use structopt::StructOpt;

use nethint::bandwidth::{Bandwidth, BandwidthTrait};
use nethint::cluster::{Cluster, Node, NodeType, Topology};
use nethint::{
    AppEvent, Application, Event, Executor, Flow, Simulator, ToStdDuration, Trace, TraceRecord,
};

extern crate mapreduce;
use mapreduce::{
    plot, GeneticReducerScheduler, GreedyReducerScheduler, JobSpec, PlaceMapper, PlaceReducer,
    Placement, RandomReducerScheduler, ReducerPlacementPolicy, Shuffle,
};

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
    let cloud = Node::new("cloud", 1, NodeType::Switch);
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

struct MapReduceApp<'c> {
    job_spec: &'c JobSpec,
    cluster: &'c Cluster,
    reducer_place_policy: ReducerPlacementPolicy,
    replayer: nethint::Replayer,
}

impl<'c> MapReduceApp<'c> {
    fn new(
        job_spec: &'c JobSpec,
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
            let sum: usize = nums.iter().cloned().sum();
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
    job_spec: &JobSpec,
    reduce_place_policy: ReducerPlacementPolicy,
    seed: u64,
) -> Trace {
    let mut simulator = Simulator::new(cluster.clone());

    let mut app = Box::new(MapReduceApp::new(job_spec, cluster, reduce_place_policy));
    app.finish_map_stage(seed);

    simulator.run_with_appliation(app)
}

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "MapReduce", about = "MapReduce Application")]
struct Opt {
    /// Set the buffer size for tx/rx
    #[structopt(short = "p", long = "nports", default_value = "4")]
    nports: usize,

    /// Oversubscription ratio
    #[structopt(short = "o", long = "oversub", default_value = "1")]
    oversub_ratio: f64,

    /// Bandwidth of host, in Gbps
    #[structopt(short = "b", long = "bandwidth", default_value = "100")]
    bandwidth: f64,

    /// Number of map tasks
    #[structopt(short = "m", long = "map", default_value = "4")]
    num_map: usize,

    /// Number of reduce tasks
    #[structopt(short = "r", long = "reduce", default_value = "4")]
    num_reduce: usize,

    /// Number of testcases
    #[structopt(short = "n", long = "ncases", default_value = "10")]
    ncases: usize,

    /// Output path of the figure
    #[structopt(short = "d", long = "directory", default_value = "figure/")]
    directory: std::path::PathBuf,
}

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    info!("Opts: {:#?}", opt);

    let nports = opt.nports;
    let oversub_ratio = opt.oversub_ratio;
    let cluster = build_fatree_fake(nports, opt.bandwidth.gbps(), oversub_ratio);
    assert_eq!(cluster.num_hosts(), nports * nports * nports / 4);
    info!("cluster:\n{}", cluster.to_dot());

    let job_spec = JobSpec::new(opt.num_map, opt.num_reduce);

    let mut data = Vec::new();
    for i in 0..opt.ncases as u64 {
        info!("testcase: {}", i);

        let output = run_map_reduce(&cluster, &job_spec, ReducerPlacementPolicy::Random, i);
        let time1 = output.recs.into_iter().map(|r| r.dura.unwrap()).max();

        let output = run_map_reduce(
            &cluster,
            &job_spec,
            ReducerPlacementPolicy::GeneticAlgorithm,
            i,
        );
        let time2 = output.recs.into_iter().map(|r| r.dura.unwrap()).max();

        let output = run_map_reduce(
            &cluster,
            &job_spec,
            ReducerPlacementPolicy::HierarchicalGreedy,
            i,
        );
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

        data.push(time1);
        data.push(time2);
        data.push(time3);
    }

    let data: Option<Vec<u64>> = data.into_iter().collect();

    let data = data.unwrap();
    let mut fg = plot::plot(&data);

    let fname = format!(
        "mapreduce_{}_{}_{}_{}.pdf",
        cluster.num_hosts(),
        oversub_ratio,
        job_spec.num_map,
        job_spec.num_reduce,
    );

    let mut output_path = opt.directory.clone();
    output_path.push(fname);

    fg.save_to_pdf(&output_path, 12, 8).unwrap();
    fg.show().unwrap();
}
