use std::rc::Rc;

use crate::{
    mapper::{
        GreedyMapperScheduler, MapperPlacementPolicy, RandomMapperScheduler,
        RandomSkewMapperScheduler, TraceMapperScheduler,
    },
    GreedyReducerLevel1Scheduler, GreedyReducerScheduler, GreedyReducerSchedulerPaper, JobSpec,
    PlaceMapper, PlaceReducer, Placement, RandomReducerScheduler, ReducerPlacementPolicy, Shuffle,
    ShufflePattern,
};
use std::convert::TryInto;
use std::cmp;
use log::info;
use nethint::{
    Duration, Flow, Trace, TraceRecord,
    app::{AppEvent, AppEventKind, Application, Replayer}, 
    cluster::{Cluster, Topology}, 
    hint::NetHintVersion, 
    simulator::{Event, Events, Executor, Simulator}
};
use rand::{self, distributions::Distribution, rngs::StdRng, Rng, SeedableRng, prelude::*};

use utils::collector::OverheadCollector;

#[derive(Debug, Clone, Default)]
struct ReducerMeta {
    unit_estimation_time: f64,
    remaining_flows: Vec<usize>,
    max_timestamp: u64,
    placements: Vec<String>,
    sizes: Vec<usize>,
}
impl ReducerMeta {
    pub fn new(
        job_spec: &JobSpec
    ) -> Self {
        ReducerMeta{
            unit_estimation_time: 0.0,
            remaining_flows: vec![job_spec.num_map; job_spec.num_reduce],
            max_timestamp: 0,
            placements: vec!["".to_string(); job_spec.num_reduce],
            sizes: vec![0; job_spec.num_reduce],
        }

    }
}


pub struct MapReduceApp<'c> {
    seed: u64,
    rng: StdRng,
    job_spec: &'c JobSpec,
    cluster: Option<Rc<dyn Topology>>,
    mapper_place_policy: MapperPlacementPolicy,
    reducer_place_policy: ReducerPlacementPolicy,
    nethint_level: usize,
    collocate: bool,
    replayer: Replayer,
    jct: Option<Duration>,
    host_bandwidth: f64,
    enable_computation_time: bool,
    reducer_meta: ReducerMeta,
    overhead_collector: OverheadCollector,
}


impl<'c> std::fmt::Debug for MapReduceApp<'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MapReduceApp")
    }
}

/// get shuffle duration 
fn get_shuffle_dur(rng: &mut StdRng)->usize{
    let choices = [(24, 61), (37, 13), (62, 14), (85, 12)];
    
    let val = choices.choose_weighted( rng, |item| item.1).unwrap().0;

    val
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
        host_bandwidth: f64,
        enable_computation_time: bool,
    ) -> Self {
        assert!(nethint_level == 1 || nethint_level == 2);
        MapReduceApp {
            seed,
            rng: StdRng::seed_from_u64(seed),
            job_spec,
            cluster,
            mapper_place_policy,
            reducer_place_policy,
            nethint_level,
            collocate,
            replayer: Replayer::new(Trace::new()),
            jct: None,
            host_bandwidth,
            enable_computation_time,
            reducer_meta: ReducerMeta::default(),
            overhead_collector: Default::default(),
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
            self.collocate,
        );
        info!("reducers: {:?}", reducers);
        reducers
    }

    fn shuffle(&mut self, shuffle: Shuffle, mappers: Placement, reducers: Placement) {
        // reinitialize replayer with new trace
        let mut trace = Trace::new();

        if self.enable_computation_time {
            //init reducer_meta
            self.reducer_meta = ReducerMeta::new(&self.job_spec);
            
            // compute the entire job size
            let mut job_size = 0;
            for i in 0..self.job_spec.num_map {
                for j in 0..self.job_spec.num_reduce {
                    job_size += shuffle.0[i][j];
                }
            }
            // total time(mapper, reducer, shuffle)
            let shuffle_estimate_time = job_size as f64 *8.0 / ((self.host_bandwidth) * cmp::min(self.job_spec.num_map, self.job_spec.num_reduce) as f64);
            let job_estimate_time =  shuffle_estimate_time / (get_shuffle_dur(&mut self.rng) as f64 /100.0);
            // println!("{:?}", get_shuffle_dur(&mut self.rng));
            let mut mappers_size = vec![0; self.job_spec.num_map];
            let mut reducers_size = vec![0; self.job_spec.num_reduce];

            
            for i in 0..self.job_spec.num_map {
                for j in 0..self.job_spec.num_reduce {
                    mappers_size[i] += shuffle.0[i][j];
                    reducers_size[j] += shuffle.0[i][j];
                }
            }
            
            //todo
            let max_mapper_size = *mappers_size.iter().max().unwrap_or_else(|| panic!("unknow value in mapper size"));
            
            let max_reducer_size = *reducers_size.iter().max().unwrap_or_else(|| panic!("unknow value in reducer size"));
            

            // calculate k1
            let k1 = (job_estimate_time-shuffle_estimate_time)/(max_mapper_size+max_reducer_size) as f64;
            // assume k1 = k2
            let k2 = k1;
            
            for i in 0..self.job_spec.num_map {
                for j in 0..self.job_spec.num_reduce {
                    let flow = Flow::new(shuffle.0[i][j], &mappers.0[i], &reducers.0[j], None);
                    // let rec = TraceRecord::new(0, flow, None);
                    let rec = TraceRecord::new((k1 * max_mapper_size as f64) as u64, flow, None);
                    trace.add_record(rec);
                }
            }

            self.reducer_meta.sizes = reducers_size;
            self.reducer_meta.unit_estimation_time = k2;
            self.reducer_meta.placements = reducers.0;

        }else{

            for i in 0..self.job_spec.num_map {
                for j in 0..self.job_spec.num_reduce {
                    let flow = Flow::new(shuffle.0[i][j], &mappers.0[i], &reducers.0[j], None);
                    let rec = TraceRecord::new(0, flow, None);
                    trace.add_record(rec);
                }
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
                        1 => Event::NetHintRequest(0, 0, NetHintVersion::V1, 0).into(),
                        2 => Event::NetHintRequest(0, 0, NetHintVersion::V2, 0).into(),
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
                    let start = std::time::Instant::now();
                    // since we have the cluster, start and schedule the app again
                    self.start();
                    let end = std::time::Instant::now();
                    let computing_delay = end - start;
                    // print controller overhead to job scale
                    self.overhead_collector.collect(computing_delay, self.job_spec.num_map.max(self.job_spec.num_reduce));

                    log::info!("computing_delay: {:?}", computing_delay);
                    return Event::UserRegisterTimer(
                        computing_delay.as_nanos().try_into().unwrap(),
                        None,
                    )
                    .into();
                }
                _ => unreachable!(),
            }
        }

        assert!(self.cluster.is_some());

        if let AppEventKind::UserNotification(ref token) = &event.event {
            assert!(token.is_none());

            return self
                .replayer
                .on_event(AppEvent::new(event.ts, AppEventKind::AppStart));
        }

        let now = event.ts;

        if self.enable_computation_time{
            if let AppEventKind::FlowComplete(ref flows) = &event.event {

                //update remaining flows on each reducer
                for trace in flows.iter(){
                    let dst = &trace.flow.dst;
                    // println!("reduccer: {:?}", &reducers.0[0]);
                    let index = self.reducer_meta.placements.iter().position(|r| r == dst).unwrap();
                    self.reducer_meta.remaining_flows[index] -= 1;
                    //find any completed reducer and update max_reducer_timestamp
                    if self.reducer_meta.remaining_flows[index]==0{
                        let single_reducer_complete_timestamp = now + (self.reducer_meta.sizes[index] as u64 * self.reducer_meta.unit_estimation_time as u64);
                        self.reducer_meta.max_timestamp = cmp::max(self.reducer_meta.max_timestamp, single_reducer_complete_timestamp)
                    }
                }
            }

        }else{
            self.reducer_meta.max_timestamp = now;
        }


        let events = self.replayer.on_event(event);
        

        if let Some(sim_ev) = events.last() {
            if matches!(sim_ev, Event::AppFinish) { 
                self.jct = Some(self.reducer_meta.max_timestamp as u64);
                // self.jct = Some(now);
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
        0.0,
        false,
    ));
    app.start();

    simulator.run_with_application(app)
}
