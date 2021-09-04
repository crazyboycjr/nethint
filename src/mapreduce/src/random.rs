use rand::{self, seq::SliceRandom};
use std::collections::HashMap;

use nethint::cluster::{Topology, LinkIx, RouteHint};

use crate::{JobSpec, PlaceReducer, Placement, Shuffle, RNG};

#[derive(Debug, Default)]
pub struct RandomReducerScheduler {}

impl RandomReducerScheduler {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn estimate_jct(
        &mut self,
        cluster: &dyn Topology,
        job_spec: &JobSpec,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
        collocate: bool,
    ) -> f64 {
        let reducers = self.place(cluster, job_spec, mapper, shuffle_pairs, collocate);
        let mut traffic: HashMap<LinkIx, usize> = Default::default();

        for (mi, m) in mapper.0.iter().enumerate() {
            let m_ix = cluster.get_node_index(m);
            for (ri, r) in reducers.0.iter().enumerate() {
                let s = shuffle_pairs.0[mi][ri];
                let r_ix = cluster.get_node_index(r);
                if m_ix != r_ix {
                    let route = cluster.resolve_route(m, r, &RouteHint::default(), None);
                    for link_ix in route.path {
                        *traffic.entry(link_ix).or_insert(0) += s;
                    }
                }
            }
        }

        let mut est: f64 = 0.0;
        for (&link_ix, &tr) in traffic.iter() {
            let bw = cluster[link_ix].bandwidth;
            est = est.max(tr as f64 * 8.0 / bw.val() as f64);
        }

        // unit in seconds
        est
    }
}

impl PlaceReducer for RandomReducerScheduler {
    fn place(
        &mut self,
        cluster: &dyn Topology,
        job_spec: &JobSpec,
        mapper: &Placement,
        _shuffle_pairs: &Shuffle,
        collocate: bool,
    ) -> Placement {
        RNG.with(|rng| {
            let mut rng = rng.borrow_mut();
            let num_hosts = cluster.num_hosts();
            let mut hosts: Vec<String> = (0..num_hosts).map(|x| format!("host_{}", x)).collect();
            if !collocate {
                hosts.retain(|h| mapper.0.iter().find(|&m| m.eq(h)).is_none());
            }
            let mut hosts: Vec<String> = hosts
                .choose_multiple(&mut *rng, job_spec.num_reduce)
                .cloned()
                .collect();
            hosts.shuffle(&mut *rng);
            Placement(hosts)
        })
    }
}
