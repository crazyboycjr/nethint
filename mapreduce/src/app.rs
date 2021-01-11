use std::rc::Rc;

use crate::{
    mapper::{
        GreedyMapperScheduler, MapperPlacementPolicy, RandomMapperScheduler,
        RandomSkewMapperScheduler, TraceMapperScheduler,
    },
    ImprovedGreedyReducerScheduler, JobSpec, PlaceMapper, PlaceReducer, Placement,
    RandomReducerScheduler, ReducerPlacementPolicy, Shuffle, ShufflePattern,
};
use log::info;
use nethint::{
    app::{AppEvent, AppEventKind, Application, Replayer},
    cluster::{Cluster, Topology},
    hint::NetHintVersion,
    simulator::{Event, Events, Executor, Simulator},
    Duration, Flow, Trace, TraceRecord,
};
use rand::{self, distributions::Distribution, rngs::StdRng, Rng, SeedableRng};

pub struct MapReduceApp<'c> {
    seed: u64,
    job_spec: &'c JobSpec,
    cluster: Option<Rc<dyn Topology>>,
    mapper_place_policy: MapperPlacementPolicy,
    reducer_place_policy: ReducerPlacementPolicy,
    nethint_level: usize,
    collocate: bool,
    replayer: Replayer,
    jct: Option<Duration>,
}

impl<'c> std::fmt::Debug for MapReduceApp<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MapReduceApp")
    }
}

impl<'c> MapReduceApp<'c> {
    pub fn new(
        seed: u64,
        job_spec: &'c JobSpec,
        cluster: Option<Rc<dyn Topology>>,
        mapper_place_policy: MapperPlacementPolicy,
        reducer_place_policy: ReducerPlacementPolicy,
        nethint_level: usize,
        collocate: bool,
    ) -> Self {
        assert!(nethint_level == 1 || nethint_level == 2);
        MapReduceApp {
            seed,
            job_spec,
            cluster,
            mapper_place_policy,
            reducer_place_policy,
            nethint_level,
            collocate,
            replayer: Replayer::new(Trace::new()),
            jct: None,
        }
    }

    fn start(&mut self) {
        let shuffle = self.generate_shuffle(self.seed);
        info!("shuffle: {:?}", shuffle);

        // if we know the physical cluster
        if self.cluster.is_some() {
            let mappers = self.place_map();
            let reducers = self.place_reduce(&mappers, &shuffle);
            self.shuffle(shuffle, mappers, reducers);
        }
    }

    fn place_map(&self) -> Placement {
        let mut map_scheduler: Box<dyn PlaceMapper> = match self.mapper_place_policy {
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
        info!("mappers: {:?}", mappers);
        mappers
    }

    fn place_reduce(&self, mappers: &Placement, shuffle: &Shuffle) -> Placement {
        let mut reduce_scheduler: Box<dyn PlaceReducer> = match self.reducer_place_policy {
            ReducerPlacementPolicy::Random => Box::new(RandomReducerScheduler::new()),
            ReducerPlacementPolicy::GeneticAlgorithm => {
                panic!("do not use genetic algorithm");
                // Box::new(GeneticReducerScheduler::new())
            }
            ReducerPlacementPolicy::HierarchicalGreedy => Box::new(ImprovedGreedyReducerScheduler::new()),
        };

        let reducers = reduce_scheduler.place(
            &**self.cluster.as_ref().unwrap(),
            &self.job_spec,
            &mappers,
            &shuffle,
            self.collocate,
        );
        info!("reducers: {:?}", reducers);
        reducers
    }

    fn shuffle(&mut self, shuffle: Shuffle, mappers: Placement, reducers: Placement) {
        // reinitialize replayer with new trace
        let mut trace = Trace::new();
        for i in 0..self.job_spec.num_map {
            for j in 0..self.job_spec.num_reduce {
                // no translation
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
}

impl<'c> Application for MapReduceApp<'c> {
    type Output = Option<Duration>;
    fn on_event(&mut self, event: AppEvent) -> Events {
        if self.cluster.is_none() {
            // ask simulator for the NetHint
            match event.event {
                AppEventKind::AppStart => {
                    // app_id should be tagged by AppGroup, so leave 0 here
                    return match self.nethint_level {
                        1 => Event::NetHintRequest(0, 0, NetHintVersion::V1).into(),
                        2 => Event::NetHintRequest(0, 0, NetHintVersion::V2).into(),
                        _ => panic!("unexpected nethint_level: {}", self.nethint_level),
                    };
                }
                AppEventKind::NetHintResponse(_, _tenant_id, ref vc) => {
                    self.cluster = Some(Rc::new(vc.clone()));
                    info!(
                        "nethint response: {}",
                        self.cluster.as_ref().unwrap().to_dot()
                    );
                    // since we have the cluster, start and schedule the app again
                    self.start();
                    return self
                        .replayer
                        .on_event(AppEvent::new(event.ts, AppEventKind::AppStart));
                }
                _ => unreachable!(),
            }
        }

        assert!(self.cluster.is_some());

        let now = event.ts;
        let events = self.replayer.on_event(event);

        if let Some(sim_ev) = events.last() {
            if matches!(sim_ev, Event::AppFinish) {
                self.jct = Some(now);
            }
        }

        events
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
        seed,
        job_spec,
        Some(Rc::new(cluster.clone())),
        map_place_policy,
        reduce_place_policy,
        2,
        false,
    ));
    app.start();

    simulator.run_with_appliation(app)
}
