use std::collections::HashMap;

use log::debug;

use nethint::bandwidth::BandwidthTrait;
use nethint::cluster::{NodeIx, Topology};

use crate::{get_rack_id, JobSpec, PlaceReducer, Placement, Shuffle};

#[derive(Debug, Default)]
pub struct GreedyReducerScheduler {}

impl GreedyReducerScheduler {
    pub fn new() -> Self {
        Default::default()
    }
}

fn find_best_node(
    cluster: &dyn Topology,
    denylist: &mut HashMap<NodeIx, usize>,
    rack: usize,
    collocate: bool,
) -> NodeIx {
    let tor_ix = cluster.get_node_index(&format!("tor_{}", rack));
    let downlink = cluster
        .get_downlinks(tor_ix)
        .filter(|&&downlink| {
            *denylist.entry(cluster.get_target(downlink)).or_default() < collocate as usize + 1
        })
        .max_by_key(|&&downlink| cluster[downlink].bandwidth)
        .unwrap();

    #[allow(clippy::let_and_return)]
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
        collocate: bool,
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

        let mut taken: HashMap<_, _> = mapper
            .0
            .iter()
            .map(|m| (cluster.get_node_index(m), 1))
            .collect();
        if !collocate {
            mapper.0.iter().for_each(|m| {
                let rack_id = get_rack_id(cluster, m);
                rack_taken[rack_id] += 1;
            });
        }

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
                let rack_bw =
                    cluster[cluster.get_reverse_link(cluster.get_uplink(tor_ix))].bandwidth;

                // exclude the case that the rack is full
                // debug!("rack_taken[{}]: {} {}", i, rack_taken[i], cluster.get_downlinks(tor_ix).len());
                if rack_taken[i] == cluster.get_downlinks(tor_ix).len() {
                    continue;
                }

                // Step 2. fix the rack, find the best node in the rack
                let best_node_ix = find_best_node(cluster, &mut taken, i, collocate);
                let r_bw =
                    cluster[cluster.get_reverse_link(cluster.get_uplink(best_node_ix))].bandwidth;

                for (mi, m) in mapper.0.iter().enumerate() {
                    let m = cluster.get_node_index(&m);
                    // let m_bw = cluster[cluster.get_uplink(m)].bandwidth;
                    let rack_m = cluster.get_target(cluster.get_uplink(m));

                    let host_bottleneck = if m == best_node_ix {
                        // collocate
                        assert!(collocate);
                        // TODO(cjr): make this configurable
                        shuffle_pairs.0[mi][j] as f64 / 400.gbps().val() as f64
                    } else {
                        shuffle_pairs.0[mi][j] as f64 / r_bw.val() as f64
                    };

                    if rack_m != tor_ix {
                        // est += (shuffle_pairs.0[mi][j] * (rack[i] + 1)) as f64 / rack_bw.val() as f64;
                        // The same case as it happens in GeneticAlgorithm, we should also take
                        // the case that the host becomes bottleneck into consideration
                        // est += (shuffle_pairs.0[mi][j] + ingress[i]) as f64 / rack_bw.val() as f64;
                        let rack_bottleneck =
                            (shuffle_pairs.0[mi][j] + ingress[i]) as f64 / rack_bw.val() as f64;
                        est += rack_bottleneck.max(host_bottleneck);
                        traffic += shuffle_pairs.0[mi][j];
                    } else {
                        est += host_bottleneck;
                    }
                }

                if min_est > est || min_est + 1e-10 > est && min_traffic > traffic {
                    min_est = est;
                    min_traffic = traffic;
                    best_rack = i;
                }
            }

            // get the best_rack
            rack[best_rack] += 1;
            ingress[best_rack] += min_traffic;
            // rack_taken[best_rack] += 1;
            debug!("best_rack: {}, traffic: {}", best_rack, ingress[best_rack]);

            // Step 2. fix the rack, find the best node in the rack
            let best_node_ix = find_best_node(cluster, &mut taken, best_rack, collocate);
            let best_node = cluster[best_node_ix].name.clone();

            // here we get the best node
            *taken.entry(best_node_ix).or_default() += 1;
            placement[j] = best_node;

            if taken[&best_node_ix] == collocate as usize + 1 {
                rack_taken[best_rack] += 1;
            }
        }

        Placement(placement)
    }
}

#[derive(Debug, Default)]
pub struct ImprovedGreedyReducerScheduler {}

impl ImprovedGreedyReducerScheduler {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn evaluate(
        &self,
        reducer_rank: usize,
        host_id: usize,
        ingress: &[usize],
        egress: &[usize],
        cluster: &dyn Topology,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
        collocate: bool,
    ) -> (f64, usize) {
        let mut cross = 0;
        let mut est = 0.;

        let host_name = format!("host_{}", host_id);
        let r_ix = cluster.get_node_index(&host_name);
        let r_rack_id = get_rack_id(cluster, &host_name);

        let r_uplink_ix = cluster.get_uplink(r_ix);
        let r_tor_ix = cluster.get_target(r_uplink_ix);
        let r_bw = cluster[cluster.get_reverse_link(r_uplink_ix)].bandwidth;
        let r_rack_bw = cluster[cluster.get_reverse_link(cluster.get_uplink(r_tor_ix))].bandwidth;

        for (mi, m) in mapper.0.iter().enumerate() {
            let flow_size = shuffle_pairs.0[mi][reducer_rank];

            let m_ix = cluster.get_node_index(m);
            let m_bw = cluster[cluster.get_uplink(m_ix)].bandwidth;
            let rack_m = cluster.get_target(cluster.get_uplink(m_ix));
            let m_rack_id = get_rack_id(cluster, m);
            let m_rack_bw = cluster[cluster.get_uplink(rack_m)].bandwidth;
            // so we have four bottlenecks here, r_bw, r_rack_bw, m_rack_bw, m_bw
            // consider all of them, as well as minimize cross rack bw
            let (r_host_bottleneck, m_host_bottleneck) = if m_ix == r_ix {
                assert!(collocate);
                (
                    flow_size as f64 / 400.gbps().val() as f64,
                    flow_size as f64 / 400.gbps().val() as f64,
                )
            } else {
                (
                    flow_size as f64 / r_bw.val() as f64,
                    flow_size as f64 / m_bw.val() as f64,
                )
            };

            if rack_m != r_tor_ix {
                let r_rack_bottleneck =
                    (flow_size + ingress[r_rack_id]) as f64 / r_rack_bw.val() as f64;
                let m_rack_bottleneck =
                    (flow_size + egress[m_rack_id]) as f64 / m_rack_bw.val() as f64;
                est += r_host_bottleneck
                    .max(r_rack_bottleneck)
                    .max(m_rack_bottleneck)
                    .max(m_host_bottleneck);
                cross += flow_size;
            } else {
                est += r_host_bottleneck.max(m_host_bottleneck);
            }
        }

        (est, cross)
    }
}

impl PlaceReducer for ImprovedGreedyReducerScheduler {
    fn place(
        &mut self,
        cluster: &dyn Topology,
        job_spec: &JobSpec,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
        collocate: bool,
    ) -> Placement {
        // two-level allocation
        // first, find a best rack to place the reducer
        // second, find the best node in that rack
        let mut placement = vec![String::new(); job_spec.num_reduce];
        let num_racks = cluster.num_switches() - 1;

        // existing ingress traffic to rack_i during greedy
        let mut ingress = vec![0; num_racks];

        // existing egress traffic to rack_i during greedy
        let mut egress = vec![0; num_racks];

        let mut taken: HashMap<_, _> = mapper
            .0
            .iter()
            .map(|m| (cluster.get_node_index(m), 1))
            .collect();

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
            let mut min_cross = usize::MAX;
            let mut best_node_id = None;

            for i in 0..cluster.num_hosts() {
                let host_name = format!("host_{}", i);
                let host_ix = cluster.get_node_index(&host_name);

                if *taken.entry(host_ix).or_default() < collocate as usize + 1 {
                    let (est, cross) = self.evaluate(
                        j,
                        i,
                        &ingress,
                        &egress,
                        cluster,
                        mapper,
                        shuffle_pairs,
                        collocate,
                    );

                    if min_est > est || min_est + 1e-10 > est && min_cross > cross {
                        min_est = est;
                        min_cross = cross;
                        best_node_id = Some(i);
                    }
                }
            }

            assert!(best_node_id.is_some());
            // get the best_node and its rack
            let best_node_id = best_node_id.unwrap();
            let best_node_name = format!("host_{}", best_node_id);
            let best_node_ix = cluster.get_node_index(&best_node_name);
            let best_rack = get_rack_id(cluster, &best_node_name);

            // update ingress and egress
            ingress[best_rack] += min_cross;
            for (mi, m) in mapper.0.iter().enumerate() {
                let flow_size = shuffle_pairs.0[mi][j];
                let m_rack_id = get_rack_id(cluster, m);
                if m_rack_id != best_rack {
                    egress[m_rack_id] += flow_size;
                }
            }

            debug!("best_rack: {}, ingress: {}", best_rack, ingress[best_rack]);

            // Step 2. fix the rack, find the best node in the rack
            let best_node = cluster[best_node_ix].name.clone();

            // here we get the best node
            *taken.entry(best_node_ix).or_default() += 2;
            placement[j] = best_node;
        }

        Placement(placement)
    }
}

/// The only optimization goal of this scheduler is to minimize inter-rack traffic.
/// This optimization goal makes sense because we do not know the difference of bandwidth among hosts.
/// That means we will not put a reducer in rack A instead of rack B because of the rack B has a host with more abundant bandwidth.
#[derive(Debug, Default)]
pub struct GreedyReducerLevel1Scheduler {}

impl GreedyReducerLevel1Scheduler {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn evaluate(
        &self,
        reducer_rank: usize,
        host_id: usize,
        cluster: &dyn Topology,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
    ) -> usize {
        let mut cross = 0;

        let host_name = format!("host_{}", host_id);
        let r_ix = cluster.get_node_index(&host_name);

        let r_uplink_ix = cluster.get_uplink(r_ix);
        let r_tor_ix = cluster.get_target(r_uplink_ix);

        for (mi, m) in mapper.0.iter().enumerate() {
            let flow_size = shuffle_pairs.0[mi][reducer_rank];

            let m_ix = cluster.get_node_index(m);
            let rack_m = cluster.get_target(cluster.get_uplink(m_ix));
            // minimize cross rack bw

            if rack_m != r_tor_ix {
                cross += flow_size;
            }
        }

        cross
    }
}

impl PlaceReducer for GreedyReducerLevel1Scheduler {
    fn place(
        &mut self,
        cluster: &dyn Topology,
        job_spec: &JobSpec,
        mapper: &Placement,
        shuffle_pairs: &Shuffle,
        collocate: bool,
    ) -> Placement {
        assert!(collocate, "assume collocation here");
        let mut placement = vec![String::new(); job_spec.num_reduce];

        let mut taken: HashMap<_, _> = mapper
            .0
            .iter()
            .map(|m| (cluster.get_node_index(m), 1))
            .collect();

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
            let mut min_cross = usize::MAX;
            let mut best_node_id = None;

            for i in 0..cluster.num_hosts() {
                let host_name = format!("host_{}", i);
                let host_ix = cluster.get_node_index(&host_name);

                if *taken.entry(host_ix).or_default() < collocate as usize + 1 {
                    let cross = self.evaluate(
                        j,
                        i,
                        cluster,
                        mapper,
                        shuffle_pairs,
                    );

                    if min_cross > cross {
                        min_cross = cross;
                        best_node_id = Some(i);
                    }
                }
            }

            assert!(best_node_id.is_some());
            // get the best_node and its rack
            let best_node_id = best_node_id.unwrap();
            let best_node_name = format!("host_{}", best_node_id);
            let best_node_ix = cluster.get_node_index(&best_node_name);
            let best_rack = get_rack_id(cluster, &best_node_name);

            debug!("best_rack: {}", best_rack);

            // Step 2. fix the rack, find the best node in the rack
            let best_node = cluster[best_node_ix].name.clone();

            // here we get the best node
            *taken.entry(best_node_ix).or_default() += 2;  // one host only one reducer, so just disable the host
            placement[j] = best_node;
        }

        Placement(placement)
    }
}