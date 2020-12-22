use std::rc::Rc;

use crate::{
    mapper::{
        GreedyMapperScheduler, MapperPlacementPolicy, RandomMapperScheduler, TraceMapperScheduler,
    },
    GeneticReducerScheduler, GreedyReducerScheduler, JobSpec, PlaceMapper, PlaceReducer, Placement,
    RandomReducerScheduler, ReducerPlacementPolicy, Shuffle, ShufflePattern,
};
use log::info;
use nethint::{
    app::{AppEvent, Application, Replayer},
    cluster::{Cluster, Topology},
    simulator::{Event, Executor, Simulator},
    Duration, Flow, Trace, TraceRecord,
    brain::TenantId,
};
use rand::{self, distributions::Distribution, rngs::StdRng, Rng, SeedableRng};

pub struct MapReduceApp<'c> {
    tenant_id: TenantId,
    seed: u64,
    job_spec: &'c JobSpec,
    cluster: Option<Rc<dyn Topology>>,
    mapper_place_policy: MapperPlacementPolicy,
    reducer_place_policy: ReducerPlacementPolicy,
    replayer: Replayer,
    jct: Option<Duration>,
}

impl<'c> MapReduceApp<'c> {
    pub fn new(
        tenant_id: TenantId,
        seed: u64,
        job_spec: &'c JobSpec,
        cluster: Option<Rc<dyn Topology>>,
        mapper_place_policy: MapperPlacementPolicy,
        reducer_place_policy: ReducerPlacementPolicy,
    ) -> Self {
        let trace = Trace::new();
        MapReduceApp {
            tenant_id,
            seed,
            job_spec,
            cluster,
            mapper_place_policy,
            reducer_place_policy,
            replayer: Replayer::new(trace),
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
        };
        let mappers = map_scheduler.place(&**self.cluster.as_ref().unwrap(), &self.job_spec);
        info!("mappers: {:?}", mappers);
        mappers
    }

    fn place_reduce(&self, mappers: &Placement, shuffle: &Shuffle) -> Placement {
        let mut reduce_scheduler: Box<dyn PlaceReducer> = match self.reducer_place_policy {
            ReducerPlacementPolicy::Random => Box::new(RandomReducerScheduler::new()),
            ReducerPlacementPolicy::GeneticAlgorithm => Box::new(GeneticReducerScheduler::new()),
            ReducerPlacementPolicy::HierarchicalGreedy => Box::new(GreedyReducerScheduler::new()),
        };

        let reducers = reduce_scheduler.place(&**self.cluster.as_ref().unwrap(), &self.job_spec, &mappers, &shuffle);
        info!("reducers: {:?}", reducers);
        reducers
    }

    fn shuffle(&mut self, shuffle: Shuffle, mappers: Placement, reducers: Placement) {
        // reinitialize replayer with new trace
        let mut trace = Trace::new();
        for i in 0..self.job_spec.num_map {
            for j in 0..self.job_spec.num_reduce {
                let phys_map = self.cluster.as_ref().unwrap().translate(&mappers.0[i]);
                let phys_reduce = self.cluster.as_ref().unwrap().translate(&reducers.0[j]);
                let flow = Flow::new(shuffle.0[i][j], &phys_map, &phys_reduce, None);
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
        if self.cluster.is_none() {
            // ask simulator for the NetHint
            match event {
                AppEvent::AppStart => {
                    // app_id should be tagged by AppGroup, so leave 0 here
                    return Event::NetHintRequest(0, self.tenant_id);
                }
                AppEvent::NetHintResponse(_, tenant_id, vc) => {
                    assert_eq!(tenant_id, self.tenant_id);
                    self.cluster = Some(Rc::new(vc));
                    // since we have the cluster, start and schedule the app again
                    self.start();
                }
                _ => unreachable!(),
            }
        }

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
        0,
        seed,
        job_spec,
        Some(Rc::new(cluster.clone())),
        map_place_policy,
        reduce_place_policy,
    ));
    app.start();

    simulator.run_with_appliation(app)
}
