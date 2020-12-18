use crate::{GreedyReducerScheduler, JobSpec, PlaceMapper, Placement, ShufflePattern, trace::Record};
use nethint::cluster::Topology;
use rand::{self, rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub enum MapperPlacementPolicy {
    Random(u64),
    FromTrace(Record),
    Greedy,
}

#[derive(Debug, Default)]
pub struct RandomMapperScheduler {
    seed: u64,
}

impl RandomMapperScheduler {
    pub fn new(seed: u64) -> Self {
        RandomMapperScheduler { seed }
    }
}

impl PlaceMapper for RandomMapperScheduler {
    fn place(&mut self, cluster: &dyn Topology, job_spec: &JobSpec) -> Placement {
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

#[derive(Debug)]
pub struct TraceMapperScheduler {
    record: Record,
}

impl TraceMapperScheduler {
    pub fn new(record: Record) -> Self {
        TraceMapperScheduler { record }
    }
}

impl PlaceMapper for TraceMapperScheduler {
    fn place(&mut self, cluster: &dyn Topology, job_spec: &JobSpec) -> Placement {
        assert!(matches!(job_spec.shuffle_pat, ShufflePattern::FromTrace(_)));
        let mut used: HashSet<String> = HashSet::new();
        let mut hosts = Vec::new();
        assert_eq!(job_spec.num_map % self.record.num_map, 0);
        let k = job_spec.num_map / self.record.num_map;
        self.record.mappers.iter().for_each(|&rack_id| {
            assert!(
                rack_id < cluster.num_switches(),
                format!(
                    "rack_id: {}, number of switches: {}",
                    rack_id,
                    cluster.num_switches()
                )
            );
            let mut selected = 0;
            let tor_ix = cluster.get_node_index(&format!("tor_{}", rack_id));
            for &link_ix in cluster.get_downlinks(tor_ix) {
                let host_ix = cluster.get_target(link_ix);
                let name = cluster[host_ix].name.clone();
                if !used.contains(&name) {
                    hosts.push(name.clone());
                    used.insert(name);
                    selected += 1;
                    if selected == k {
                        break;
                    }
                }
            }
            assert!(
                selected == k,
                "please increase the number of hosts within a rack"
            );
        });

        Placement(hosts)
    }
}

pub struct GreedyMapperScheduler {
}

impl PlaceMapper for GreedyReducerScheduler {
    fn place(&mut self, vcluster: &dyn Topology, job_spec: &JobSpec) -> Placement {
        Placement(Vec::new())
    }
}