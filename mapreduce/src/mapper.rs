use crate::{JobSpec, PlaceMapper, Placement, ShuffleDist};
use nethint::cluster::{Cluster, Topology};
use rand::{self, rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::collections::HashSet;

#[derive(Debug, Default)]
pub struct MapperScheduler {
    seed: u64,
}

impl MapperScheduler {
    pub fn new(seed: u64) -> Self {
        MapperScheduler { seed }
    }
}

impl PlaceMapper for MapperScheduler {
    fn place(&mut self, cluster: &Cluster, job_spec: &JobSpec) -> Placement {
        // here we just consider the case where there is only one single job
        let hosts = match &job_spec.shuffle_dist {
            ShuffleDist::FromTrace(record) => {
                let mut used: HashSet<String> = HashSet::new();
                let mut hosts = Vec::new();
                assert_eq!(job_spec.num_map % record.num_map, 0);
                let k = job_spec.num_map / record.num_map;
                record.mappers.iter().for_each(|&rack_id| {
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
                hosts
            }
            _ => {
                let mut rng = StdRng::seed_from_u64(self.seed);
                let num_hosts = cluster.num_hosts();
                (0..num_hosts)
                    .collect::<Vec<_>>()
                    .choose_multiple(&mut rng, job_spec.num_map)
                    .map(|x| format!("host_{}", x))
                    .collect()
            }
        };
        Placement(hosts)
    }
}
