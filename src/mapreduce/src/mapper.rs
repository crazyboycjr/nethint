use crate::{trace::Record, JobSpec, PlaceMapper, Placement, ShufflePattern};
use nethint::{bandwidth::Bandwidth, cluster::Topology};
use rand::{self, rngs::StdRng, seq::SliceRandom, SeedableRng};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "args")]
pub enum MapperPlacementPolicy {
    Random(u64),
    Greedy,
    RandomSkew(u64, f64), // seed, s
    #[serde(skip)]
    FromTrace(Record),
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
        let mut hosts: Vec<String> = (0..num_hosts)
            .collect::<Vec<_>>()
            .choose_multiple(&mut rng, job_spec.num_map)
            .map(|x| format!("host_{}", x))
            .collect();
        hosts.shuffle(&mut rng);
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
                "rack_id: {}, number of switches: {}",
                rack_id,
                cluster.num_switches()
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

#[derive(Debug, Default)]
pub struct GreedyMapperScheduler {}

impl GreedyMapperScheduler {
    pub fn new() -> Self {
        Default::default()
    }
}

impl PlaceMapper for GreedyMapperScheduler {
    fn place(&mut self, vcluster: &dyn Topology, job_spec: &JobSpec) -> Placement {
        let mut bw_host: Vec<(Bandwidth, String)> = (0..vcluster.num_hosts())
            .map(|i| {
                let host_name = format!("host_{}", i);
                let host_ix = vcluster.get_node_index(&host_name);
                let bw = vcluster[vcluster.get_uplink(host_ix)].bandwidth;
                (bw, host_name)
            })
            .collect();

        bw_host.sort_by_key(|x| std::cmp::Reverse(x.0));

        let hosts: Vec<_> = bw_host
            .into_iter()
            .map(|x| x.1)
            .take(job_spec.num_map)
            .collect();

        Placement(hosts)
    }
}
#[derive(Debug, Default)]
pub struct RandomSkewMapperScheduler {
    seed: u64,
    s: f64,
}

impl RandomSkewMapperScheduler {
    pub fn new(seed: u64, s: f64) -> Self {
        Self { seed, s }
    }
}

impl PlaceMapper for RandomSkewMapperScheduler {
    fn place(&mut self, vcluster: &dyn Topology, job_spec: &JobSpec) -> Placement {
        let mut rng = StdRng::seed_from_u64(self.seed);
        let num_racks = vcluster.num_switches() - 1;

        use rand::distributions::Distribution;
        let zipf = zipf::ZipfDistribution::new(num_racks, self.s).unwrap();

        let mut used = HashSet::new();
        let mut hosts = Vec::new();

        for _i in 0..job_spec.num_map {
            let mut found = None;
            while found.is_none() {
                let rack_id = zipf.sample(&mut rng) - 1;
                assert!(rack_id < num_racks, "{} vs {}", rack_id, num_racks);

                let tor_name = format!("tor_{}", rack_id);
                let tor_ix = vcluster.get_node_index(&tor_name);
                for &link_ix in vcluster.get_downlinks(tor_ix) {
                    let host_ix = vcluster.get_target(link_ix);
                    if !used.contains(&host_ix) {
                        found = Some(host_ix);
                        break;
                    }
                }

                if found.is_none() {
                    log::warn!("rack {} is full, we don't want this", rack_id);
                }
            }

            used.insert(found.unwrap());
            let host_name = vcluster[found.unwrap()].name.clone();
            hosts.push(host_name);
        }

        hosts.shuffle(&mut rng);

        Placement(hosts)
    }
}
