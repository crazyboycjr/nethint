use crate::{
    mapper::{MapperPlacementPolicy, RandomMapperScheduler, TraceMapperScheduler},
    GeneticReducerScheduler, GreedyReducerScheduler, JobSpec, PlaceMapper, PlaceReducer, Placement,
    RandomReducerScheduler, ReducerPlacementPolicy, Shuffle, ShufflePattern,
};
use log::info;
use nethint::{
    app::{AppEvent, Application, Replayer},
    cluster::{Cluster, Topology},
    simulator::{Event, Executor, Simulator},
    Duration, Flow, Trace, TraceRecord,
};
use rand::{self, distributions::Distribution, rngs::StdRng, Rng, SeedableRng};

pub struct MapReduceApp<'c> {
    job_spec: &'c JobSpec,
    cluster: &'c dyn Topology,
    mapper_place_policy: MapperPlacementPolicy,
    reducer_place_policy: ReducerPlacementPolicy,
    replayer: Replayer,
    jct: Option<Duration>,
}

impl<'c> MapReduceApp<'c> {
    pub fn new(
        job_spec: &'c JobSpec,
        cluster: &'c dyn Topology,
        mapper_place_policy: MapperPlacementPolicy,
        reducer_place_policy: ReducerPlacementPolicy,
    ) -> Self {
        let trace = Trace::new();
        MapReduceApp {
            job_spec,
            cluster,
            mapper_place_policy,
            reducer_place_policy,
            replayer: Replayer::new(trace),
            jct: None,
        }
    }

    pub fn start(&mut self, seed: u64) {
        let shuffle = self.generate_shuffle(seed);
        info!("shuffle: {:?}", shuffle);
        let mappers = self.place_map();
        let reducers = self.place_reduce(&mappers, &shuffle);
        self.shuffle(shuffle, mappers, reducers);
    }

    fn place_map(&self) -> Placement {
        let mut map_scheduler: Box<dyn PlaceMapper> = match self.mapper_place_policy {
            MapperPlacementPolicy::Random(seed) => Box::new(RandomMapperScheduler::new(seed)),
            MapperPlacementPolicy::FromTrace(ref record) => {
                Box::new(TraceMapperScheduler::new(record.clone()))
            }
            MapperPlacementPolicy::Greedy => unimplemented!(),
        };
        let mappers = map_scheduler.place(self.cluster, &self.job_spec);
        info!("mappers: {:?}", mappers);
        mappers
    }

    fn place_reduce(&self, mappers: &Placement, shuffle: &Shuffle) -> Placement {
        let mut reduce_scheduler: Box<dyn PlaceReducer> = match self.reducer_place_policy {
            ReducerPlacementPolicy::Random => Box::new(RandomReducerScheduler::new()),
            ReducerPlacementPolicy::GeneticAlgorithm => Box::new(GeneticReducerScheduler::new()),
            ReducerPlacementPolicy::HierarchicalGreedy => Box::new(GreedyReducerScheduler::new()),
        };

        let reducers = reduce_scheduler.place(self.cluster, &self.job_spec, &mappers, &shuffle);
        info!("reducers: {:?}", reducers);
        reducers
    }

    fn shuffle(&mut self, shuffle: Shuffle, mappers: Placement, reducers: Placement) {
        // reinitialize replayer with new trace
        let mut trace = Trace::new();
        for i in 0..self.job_spec.num_map {
            for j in 0..self.job_spec.num_reduce {
                let flow = Flow::new(shuffle.0[i][j], &mappers.0[i], &reducers.0[j], None);
                let rec = TraceRecord::new(0, flow, None);
                trace.add_record(rec);
            }
        }

        self.replayer = Replayer::new(trace);
    }

    fn generate_shuffle(&mut self, seed: u64) -> Shuffle {
        let mut rng = StdRng::seed_from_u64(seed);
        let n = self.job_spec.num_map;
        let m = self.job_spec.num_reduce;
        let mut pairs = vec![vec![0; m]; n];
        match &self.job_spec.shuffle_pat {
            &ShufflePattern::Uniform(n) => {
                pairs.iter_mut().for_each(|v| {
                    v.iter_mut().for_each(|i| *i = rng.gen_range(0, n as usize));
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
                for i in 0..n {
                    for j in 0..m {
                        pairs[i][j] = (record.reducers[j / k].1 / n as f64 * 1e6) as usize;
                    }
                }
            }
        }
        Shuffle(pairs)
    }
}

impl<'c> Application for MapReduceApp<'c> {
    type Output = Option<Duration>;
    fn on_event(&mut self, event: AppEvent) -> Event {
        if let AppEvent::FlowComplete(ref flows) = &event {
            let fct_cur = flows.iter().map(|f| f.ts + f.dura.unwrap()).max();
            self.jct = self.jct.iter().cloned().chain(fct_cur).max();
        }
        self.replayer.on_event(event)
    }
    fn answer(&mut self) -> Self::Output {
        self.jct
    }
}

pub fn run_map_reduce(
    cluster: &Cluster,
    job_spec: &JobSpec,
    reduce_place_policy: ReducerPlacementPolicy,
    seed: u64,
) -> Option<Duration> {
    let mut simulator = Simulator::new(cluster.clone());

    let map_place_policy = if let ShufflePattern::FromTrace(ref record) = job_spec.shuffle_pat {
        MapperPlacementPolicy::FromTrace((**record).clone())
    } else {
        MapperPlacementPolicy::Random(seed)
    };
    let mut app = Box::new(MapReduceApp::new(
        job_spec,
        cluster,
        map_place_policy,
        reduce_place_policy,
    ));
    app.start(seed);

    simulator.run_with_appliation(app)
}
