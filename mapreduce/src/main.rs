use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_std::task;
use futures::stream::StreamExt;
use log::info;
use rand::{self, rngs::StdRng, seq::SliceRandom, Rng, SeedableRng, distributions::Distribution};
use structopt::StructOpt;

use nethint::bandwidth::BandwidthTrait;
use nethint::cluster::{Cluster, Topology};
use nethint::{
    AppEvent, Application, Event, Executor, Flow, Simulator, ToStdDuration, Trace, TraceRecord,
};

extern crate mapreduce;
use mapreduce::{
    plot, topology, GeneticReducerScheduler, GreedyReducerScheduler, JobSpec, PlaceMapper,
    PlaceReducer, Placement, RandomReducerScheduler, ReducerPlacementPolicy, Shuffle, ShuffleDist
};
use topology::make_asymmetric;

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
        // let shuffle = self._generate_shuffle_flows_2(seed);
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
        let mut rng = StdRng::seed_from_u64(seed);
        let n = self.job_spec.num_map;
        let m = self.job_spec.num_reduce;
        let mut pairs = vec![vec![0; m]; n];
        match self.job_spec.shuffle_dist {
            ShuffleDist::Uniform(n) => {
                pairs.iter_mut().for_each(|v| {
                    v.iter_mut().for_each(|i| *i = rng.gen_range(0, n as usize));
                });
            }
            ShuffleDist::Zipf(n, s) => {
                let zipf = zipf::ZipfDistribution::new(1000, s).unwrap();
                pairs.iter_mut().for_each(|v| {
                    v.iter_mut().for_each(|i| *i = n as usize * zipf.sample(&mut rng) / 10);
                });
            }
        }
        Shuffle(pairs)
    }

    fn _generate_shuffle_flows_2(&mut self, seed: u64) -> Shuffle {
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
    /// Specify the topology for simulation
    #[structopt(subcommand)]
    topo: Topo,

    /// Asymmetric bandwidth
    #[structopt(short = "a", long = "asymmetric")]
    asym: bool,

    /// Probability distribution of shuffle flows
    #[structopt(
        short = "s",
        long = "shuffle",
        name = "distribution",
        default_value = "uniform_1000000",
    )]
    shuffle: ShuffleDist,

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
    #[structopt(short = "d", long = "directory")]
    directory: Option<std::path::PathBuf>,

    /// Run simulation experiments in parallel, default using the hardware concurrency
    #[structopt(short = "P", long = "parallel", name = "nthreads")]
    parallel: Option<usize>,
}

#[derive(Debug, Clone, StructOpt)]
enum Topo {
    /// FatTree, parameters include the number of ports of each switch, bandwidth, and oversubscription ratio
    FatTree {
        /// Set the the number of ports
        nports: usize,
        /// Bandwidth of a host, in Gbps
        bandwidth: f64,
        /// Oversubscription ratio
        oversub_ratio: f64,
    },

    /// Virtual cluster, parameters include the number of racks and rack_size, host_bw, and rack_bw
    Virtual {
        /// Specify the number of racks
        nracks: usize,
        /// Specify the number of hosts under one rack
        rack_size: usize,
        /// Bandwidth of a host, in Gbps
        host_bw: f64,
        /// Bandwidth of a ToR switch, in Gbps
        rack_bw: f64,
    },
}

impl std::fmt::Display for Topo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Topo::FatTree {
                nports,
                bandwidth,
                oversub_ratio,
            } => write!(f, "fattree_{}_{}g_{:.2}", nports, bandwidth, oversub_ratio),
            Topo::Virtual {
                nracks,
                rack_size,
                host_bw,
                rack_bw,
            } => write!(
                f,
                "virtual_{}_{}_{}g_{}g",
                nracks, rack_size, host_bw, rack_bw,
            ),
        }
    }
}

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    info!("Opts: {:#?}", opt);

    let cluster = match opt.topo {
        Topo::FatTree {
            nports,
            bandwidth,
            oversub_ratio,
        } => {
            let cluster = topology::build_fatree_fake(nports, bandwidth.gbps(), oversub_ratio);
            assert_eq!(cluster.num_hosts(), nports * nports * nports / 4);
            cluster
        }
        Topo::Virtual {
            nracks,
            rack_size,
            host_bw,
            rack_bw,
        } => topology::build_virtual_cluster(nracks, rack_size, host_bw.gbps(), rack_bw.gbps()),
    };

    let cluster = Arc::new(if opt.asym {
        make_asymmetric(cluster)
    } else {
        cluster
    });

    info!("cluster:\n{}", cluster.to_dot());

    let job_spec = Arc::new(JobSpec::new(opt.num_map, opt.num_reduce, opt.shuffle));

    let policies = &[
        ReducerPlacementPolicy::Random,
        ReducerPlacementPolicy::GeneticAlgorithm,
        ReducerPlacementPolicy::HierarchicalGreedy,
    ];

    let num_cpus = opt.parallel.unwrap_or(num_cpus::get());

    let experiments: Option<Vec<(usize, u64)>> = task::block_on(async {
        let experiments = futures::stream::iter({
            (0..opt.ncases * 3).map(|i| {
                let cluster = Arc::clone(&cluster);
                let job_spec = Arc::clone(&job_spec);
                let policy = policies[i % 3];
                task::spawn(async move {
                    info!("testcase: {}", i / 3);
                    let output = run_map_reduce(&cluster, &job_spec, policy, (i / 3) as _);
                    let time = output.recs.into_iter().map(|r| r.dura.unwrap()).max();
                    info!(
                        "{:?}, job_finish_time: {:?}",
                        policy,
                        time.unwrap().to_dura()
                    );
                    Some((i, time.unwrap()))
                })
            })
        })
        .buffer_unordered(num_cpus)
        .collect::<Vec<Option<(usize, u64)>>>();
        experiments.await.into_iter().collect()
    });

    visualize(&opt, experiments, &cluster, &job_spec).unwrap();
}

fn visualize(
    opt: &Opt,
    experiments: Option<Vec<(usize, u64)>>,
    _cluster: &Cluster,
    job_spec: &JobSpec,
) -> Result<()> {
    let data: Option<Vec<u64>> = experiments.map(|mut a| {
        a.sort();
        a.into_iter().map(|t| t.1).collect()
    });

    let data = Arc::new(data.ok_or(anyhow!("Empty experiment data"))?);

    let output_path = opt.directory.as_ref().map(|directory| {
        let mut path = directory.clone();
        let fname = format!("mapreduce_{}_{}.pdf", opt.topo, job_spec);
        path.push(fname);
        path
    });

    let data1 = Arc::clone(&data);

    let future1: task::JoinHandle<Result<()>> = task::spawn(async move {
        let mut fg = plot::plot(&data1);

        if let Some(path) = output_path {
            fg.save_to_pdf(&path, 12, 8).map_err(|e| anyhow!("{}", e))?;
            info!("save figure to {:?}", path);
        }

        fg.show().map_err(|e| anyhow!("{}", e))?;
        Ok(())
    });

    let output_path = opt.directory.as_ref().map(|directory| {
        let mut path = directory.clone();
        let fname = format!("mapreduce_cdf_{}_{}.pdf", opt.topo, job_spec);
        path.push(fname);
        path
    });

    let data2 = Arc::clone(&data);

    let future2: task::JoinHandle<Result<()>> = task::spawn(async move {
        let mut fg = plot::plot_cdf(&data2);

        if let Some(path) = output_path {
            fg.save_to_pdf(&path, 12, 8).map_err(|e| anyhow!("{}", e))?;
            info!("save figure to {:?}", path);
        }

        fg.show().map_err(|e| anyhow!("{}", e))?;
        Ok(())
    });

    let _ = task::block_on(async { futures::join!(future1, future2) });

    Ok(())
}
