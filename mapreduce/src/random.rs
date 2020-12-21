use rand::{self, seq::SliceRandom};

use nethint::cluster::Topology;

use crate::{JobSpec, PlaceReducer, Placement, Shuffle, RNG};

#[derive(Debug, Default)]
pub struct RandomReducerScheduler {}

impl RandomReducerScheduler {
    pub fn new() -> Self {
        Default::default()
    }
}

impl PlaceReducer for RandomReducerScheduler {
    fn place(
        &mut self,
        cluster: &dyn Topology,
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
