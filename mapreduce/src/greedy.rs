use std::collections::HashSet;

use log::debug;

use nethint::cluster::{Topology, NodeIx};

use crate::{get_rack_id, JobSpec, PlaceReducer, Placement, Shuffle};

#[derive(Debug, Default)]
pub struct GreedyReducerScheduler {}

impl GreedyReducerScheduler {
    pub fn new() -> Self {
        Default::default()
    }
}

fn find_best_node(cluster: &dyn Topology, denylist: &HashSet<NodeIx>, rack: usize) -> NodeIx {
    let tor_ix = cluster.get_node_index(&format!("tor_{}", rack));
    let downlink = cluster
        .get_downlinks(tor_ix)
        .filter(|&&downlink| !denylist.contains(&cluster.get_target(downlink)))
        .max_by_key(|&&downlink| cluster[downlink].bandwidth)
        .unwrap();

    let best_node_ix = cluster.get_target(*downlink);
    best_node_ix
}

impl PlaceReducer for GreedyReducerScheduler {
    fn place(
        &mut self,
        cluster: &dyn Topology,
        job_spec: &JobSpec,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
    ) -> Placement {
        // two-level allocation
        // first, find a best rack to place the reducer
        // second, find the best node in that rack
        let mut placement = vec![String::new(); job_spec.num_reduce];
        let num_racks = cluster.num_switches() - 1;

        // number of nodes has been taken in a rack
        let mut rack_taken = vec![0; num_racks];

        // number of reducers in the rack
        let mut rack = vec![0; num_racks];

        // existing ingress traffic to rack_i during greedy
        let mut ingress = vec![0; num_racks];

        let mut taken: HashSet<_> = mapper.0.iter().map(|m| cluster.get_node_index(m)).collect();
        mapper.0.iter().for_each(|m| {
            let rack_id = get_rack_id(cluster, m);
            rack_taken[rack_id] += 1;
        });

        // reducer rank sorted by weight
        let mut reducer_weight = vec![0; job_spec.num_reduce];
        debug_assert!(job_spec.num_map <= shuffle_pairs.0.len());
        for i in 0..job_spec.num_map {
            for (j, w) in reducer_weight.iter_mut().enumerate() {
                *w += shuffle_pairs.0[i][j];
            }
        }

        use std::cmp::Reverse;
        let mut rank: Vec<_> = (0..job_spec.num_reduce).collect();
        rank.sort_by_key(|&r| Reverse(reducer_weight[r]));

        for j in rank {
            let mut min_est = f64::MAX;
            let mut min_traffic = usize::MAX;
            let mut best_rack = 0;

            // Step 1. find the best rack
            for i in 0..num_racks {
                let mut traffic = 0;
                let mut est = 0.;
                let tor_ix = cluster.get_node_index(&format!("tor_{}", i));
                let rack_bw = cluster[cluster.get_uplink(tor_ix)].bandwidth;

                // exclude the case that the rack is full
                // debug!("rack_taken[{}]: {} {}", i, rack_taken[i], cluster.get_downlinks(tor_ix).len());
                if rack_taken[i] == cluster.get_downlinks(tor_ix).len() {
                    continue;
                }

                // Step 2. fix the rack, find the best node in the rack
                let best_node_ix = find_best_node(cluster, &taken, i);
                let r_bw = cluster[cluster.get_uplink(best_node_ix)].bandwidth;

                for (mi, m) in mapper.0.iter().enumerate() {
                    let m = cluster.get_node_index(&m);
                    // let m_bw = cluster[cluster.get_uplink(m)].bandwidth;
                    let rack_m = cluster.get_target(cluster.get_uplink(m));
                    if rack_m != tor_ix {
                        // est += (shuffle_pairs.0[mi][j] * (rack[i] + 1)) as f64 / rack_bw.val() as f64;
                        // The same case as it happens in GeneticAlgorithm, we should also take
                        // the case that the host becomes bottleneck into consideration
                        // est += (shuffle_pairs.0[mi][j] + ingress[i]) as f64 / rack_bw.val() as f64;
                        let rack_bottleneck = (shuffle_pairs.0[mi][j] + ingress[i]) as f64 / rack_bw.val() as f64;
                        let host_bottleneck = shuffle_pairs.0[mi][j] as f64 / r_bw.val() as f64;
                        est += rack_bottleneck.max(host_bottleneck);
                        traffic += shuffle_pairs.0[mi][j];
                    } else {
                        let host_bottleneck = shuffle_pairs.0[mi][j] as f64 / r_bw.val() as f64;
                        est += host_bottleneck;
                    }
                }

                if min_est > est {
                    min_est = est;
                    min_traffic = traffic;
                    best_rack = i;
                }
            }

            // get the best_rack
            rack[best_rack] += 1;
            ingress[best_rack] += min_traffic;
            rack_taken[best_rack] += 1;
            debug!("best_rack: {}, traffic: {}", best_rack, ingress[best_rack]);

            // Step 2. fix the rack, find the best node in the rack
            let best_node_ix = find_best_node(cluster, &taken, best_rack);
            let best_node = cluster[best_node_ix].name.clone();

            // here we get the best node
            taken.insert(best_node_ix);
            placement[j] = best_node;
        }

        Placement(placement)
    }
}
