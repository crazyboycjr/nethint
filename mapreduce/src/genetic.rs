use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;

use log::debug;
use rand::{self, seq::SliceRandom, Rng};

#[allow(clippy::all)]
use spiril::population::Population;
use spiril::unit::Unit;

use nethint::cluster::{Cluster, Topology};

use crate::{get_rack_id, JobSpec, PlaceReducer, Placement, Shuffle, RNG};

#[derive(Debug, Default)]
pub struct GeneticReducerScheduler {}

impl GeneticReducerScheduler {
    pub fn new() -> Self {
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
        let ga_size = 1000;
        let ga_breed_factor = 0.5;
        let ga_survival_factor = 0.2;
        let ga_epochs = 50;
        let ga_mutate_rate = 0.1;

        let mapper_id: Vec<usize> = mapper
            .0
            .iter()
            .map(|x| x.strip_prefix("host_").unwrap().parse().unwrap())
            .collect();

        let ctx = &GAContext {
            mapper: mapper.clone(),
            mapper_id,
            shuffle: shuffle_pairs,
            cluster,
            mutate_rate: ga_mutate_rate,
            succ: AtomicUsize::new(0),
            mutation: AtomicUsize::new(0),
            cross: AtomicUsize::new(0),
        };

        let units = RNG.with(|rng| {
            let mut rng = rng.borrow_mut();
            let num_hosts = cluster.num_hosts();

            let hosts: Vec<usize> = (0..num_hosts)
                .filter(|&h| ctx.mapper_id.iter().find(|&&m| m == h).is_none())
                .collect();

            (0..ga_size)
                .map(|_| GAUnit {
                    reducer: hosts
                        .choose_multiple(&mut *rng, job_spec.num_reduce)
                        .cloned()
                        .collect(),
                    ctx: &ctx,
                })
                .collect()
        });

        let solutions = Population::new(units)
            .set_size(ga_size)
            .set_breed_factor(ga_breed_factor)
            .set_survival_factor(ga_survival_factor)
            .epochs_parallel(ga_epochs, 4) // 1 CPU cores
            .finish();

        let job_placement = solutions.first().unwrap();
        debug!("fitness: {}", job_placement.fitness());
        debug!(
            "success/mutation/crossover: {:?}/{:?}/{:?}",
            ctx.succ.load(SeqCst),
            ctx.mutation.load(SeqCst),
            ctx.cross.load(SeqCst),
        );

        Placement(
            job_placement
                .reducer
                .iter()
                .map(|x| format!("host_{}", *x))
                .collect(),
        )
    }
}

#[derive(Debug)]
struct GAContext<'a> {
    mapper: Placement,
    mapper_id: Vec<usize>,
    shuffle: &'a Shuffle,
    cluster: &'a Cluster,
    mutate_rate: f64,
    succ: AtomicUsize,
    mutation: AtomicUsize,
    cross: AtomicUsize,
}

#[derive(Debug, Clone)]
struct GAUnit<'a, 'ctx> {
    reducer: Vec<usize>,
    ctx: &'ctx GAContext<'a>,
}

impl<'a, 'ctx> Unit for GAUnit<'a, 'ctx> {
    fn fitness(&self) -> f64 {
        let GAContext {
            mapper,
            mapper_id: _,
            shuffle,
            cluster,
            ..
        } = self.ctx;

        let num_racks = cluster.num_switches() - 1;
        let mut rack = vec![0; num_racks];
        let mut traffic = vec![vec![0; num_racks]; mapper.0.len()];

        let reducer_name: Vec<String> = self
            .reducer
            .iter()
            .map(|x| format!("host_{}", *x))
            .collect();

        self.reducer.iter().enumerate().for_each(|(j, _r)| {
            let id = get_rack_id(cluster, &reducer_name[j]);
            mapper.0.iter().enumerate().for_each(|(i, m)| {
                let rack_m = get_rack_id(cluster, m);
                if id != rack_m {
                    traffic[i][id] += shuffle.0[i][j];
                }
            });
        });

        self.reducer.iter().enumerate().for_each(|(j, _)| {
            let id = get_rack_id(cluster, &reducer_name[j]);
            rack[id] += 1;
        });

        let mut res: f64 = 0.;
        for (j, _) in self.reducer.iter().enumerate() {
            let mut inner_est: f64 = 0.;
            let mut outer_est: f64 = 0.;
            let rack_r = get_rack_id(cluster, &reducer_name[j]);

            let r_ix = cluster.get_node_index(&reducer_name[j]);
            let tor_ix = cluster.get_target(cluster.get_uplink(r_ix));
            let rack_bw = cluster[cluster.get_uplink(tor_ix)].bandwidth;

            for (i, m) in mapper.0.iter().enumerate() {
                let m_ix = cluster.get_node_index(m);
                let tor_m_ix = cluster.get_target(cluster.get_uplink(m_ix));
                let m_bw = cluster[cluster.get_uplink(m_ix)].bandwidth;

                if tor_ix == tor_m_ix {
                    inner_est += shuffle.0[i][j] as f64 / m_bw.val() as f64;
                } else {
                    outer_est += traffic[i][rack_r] as f64 / rack_bw.val() as f64;
                }
            }

            assert!(rack[rack_r] > 0);
            let acc_time = inner_est.max(outer_est);
            res = res.max(acc_time);
        }

        // 2 * (1 - sigmoid(res)) -> (0, 1)
        2.0 - 2.0 / (1.0 + (-res).exp())
    }

    fn breed_with(&self, other: &GAUnit) -> Self {
        let GAContext { succ, cross, .. } = self.ctx;

        // crossover
        let mut result = self.crossover(other);

        // probabilitisc mutation
        self.mutate(&mut result);

        // prune invalid result
        result.dedup();
        if result.len() < other.reducer.len() {
            result = other.reducer.clone();
        } else {
            succ.fetch_add(1, SeqCst);
        }
        cross.fetch_add(1, SeqCst);

        GAUnit {
            reducer: result,
            ctx: self.ctx,
        }
    }
}

impl<'a, 'ctx> GAUnit<'a, 'ctx> {
    fn crossover(&self, other: &GAUnit) -> Vec<usize> {
        RNG.with(|rng| {
            let mut rng = rng.borrow_mut();

            let num_intersect = rng.gen_range(0, other.reducer.len() / 2 + 1);
            let indices: Vec<usize> = (0..other.reducer.len())
                .collect::<Vec<usize>>()
                .choose_multiple(&mut *rng, num_intersect)
                .cloned()
                .collect();

            // crossover
            let mut result = self.reducer.clone();
            let set: HashSet<_> = result.iter().cloned().collect();
            for i in indices {
                if !set.contains(&other.reducer[i]) {
                    result[i] = other.reducer[i];
                }
            }

            result
        })
    }

    fn mutate(&self, result: &mut Vec<usize>) {
        let GAContext {
            mapper_id,
            cluster,
            mutate_rate,
            mutation,
            ..
        } = self.ctx;

        RNG.with(|rng| {
            let mut rng = rng.borrow_mut();
            let p = rng.gen_range(0., 1.);
            if p < *mutate_rate {
                let i = rng.gen_range(0, self.reducer.len());
                if let Some(y) = (0..cluster.num_hosts())
                    .map(|_| rng.gen_range(0, cluster.num_hosts()))
                    .filter(|y| mapper_id.iter().find(|&x| x.eq(&y)).is_none())
                    .filter(|y| result.iter().find(|&x| x.eq(&y)).is_none())
                    .take(1)
                    .next()
                {
                    result[i] = y;
                    mutation.fetch_add(1, SeqCst);
                }
            }
        });
    }
}
