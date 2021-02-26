use std::collections::VecDeque;
use std::rc::Rc;
use std::{cell::RefCell, unimplemented};

// use fnv::FnvHashMap as HashMap;
use fnv::FnvBuildHasher;
use indexmap::IndexMap;
type HashMap<K, V> = IndexMap<K, V, FnvBuildHasher>;

use crate::{
    bandwidth::{Bandwidth, BandwidthTrait},
    brain::{Brain, TenantId},
    cluster::{Counters, LinkIx, NodeIx, NodeType, Topology, VirtCluster},
    simulator::FlowSet,
    Duration, FairnessModel, SharingMode, Timestamp,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetHintVersion {
    V1,
    V2,
}

/// Only topology information, all bandwidth are set to 0.gbps()
pub type NetHintV1 = VirtCluster;

/// With network metrics, either collected by measurement or telemetry.
/// Currently, we are using bandwidth as the level 2 hint.
pub type NetHintV2 = VirtCluster;

/// Sample point
#[derive(Debug, Clone)]
struct Point<V: Copy> {
    ts: Timestamp,
    val: V,
}

impl<V: Copy> Point<V> {
    #[inline]
    fn new(ts: Timestamp, val: V) -> Self {
        Point { ts, val }
    }
}

/// VM historical statictics for each virtual node
#[derive(Debug, Clone, Default)]
struct VmStats {
    tx_total: VecDeque<Point<u64>>,
    /// tx traffic within the rack
    tx_in: VecDeque<Point<u64>>,
    rx_total: VecDeque<Point<u64>>,
    rx_in: VecDeque<Point<u64>>,
}

const VM_STATS_MEMORY_LEN: usize = 100;

macro_rules! impl_last_delta_for {
    ($(($member:ident, $func_name:ident)),+ $(,)?) => (
        $(
            #[inline]
            fn $func_name(&self) -> Option<u64> {
                if self.$member.is_empty() {
                    None
                } else if self.$member.len() == 1 {
                    Some(self.$member[0].val)
                } else {
                    let n = self.$member.len();
                    Some(self.$member[n - 1].val - self.$member[n - 2].val)
                }
            }
        )+
    )
}

impl VmStats {
    impl_last_delta_for!(
        (tx_total, last_delta_tx_total),
        (tx_in, last_delta_tx_in),
        (rx_total, last_delta_rx_total),
        (rx_in, last_delta_rx_in)
    );

    fn add_sample(&mut self, ts: Timestamp, counter: &Counters) {
        macro_rules! stmt_add_counter_for {
            ($($member:ident),+ $(,)?) => (
                $(
                    self.$member.push_back(Point::new(ts, counter.$member));
                    if self.$member.len() > VM_STATS_MEMORY_LEN {
                        self.$member.pop_front();
                    }
                )+
            )
        }
        stmt_add_counter_for!(tx_total, tx_in, rx_total, rx_in);
    }
}

struct Sampler {
    interval_ns: Duration,
    // vmstats: Vec<VmStats>,
    vmstats: HashMap<NodeIx, VmStats>,
}

impl Sampler {
    fn new(interval_ns: Duration) -> Self {
        Sampler {
            interval_ns,
            vmstats: HashMap::default(),
        }
    }

    fn sample(&mut self, vcluster: &VirtCluster, ts: Timestamp) {
        // enumerate each node
        let num_hosts = vcluster.num_hosts();
        let num_racks = vcluster.num_switches() - 1;
        let mut agg = vec![Counters::new(); num_racks];

        for i in 0..num_hosts {
            // TODO(cjr): this conversion can be cached in Sampler
            let host_name = format!("host_{}", i);
            let host_ix = vcluster.get_node_index(&host_name);
            let counters = &vcluster[host_ix].counters;

            self.vmstats
                .entry(host_ix)
                .or_default()
                .add_sample(ts, counters);

            let tor_ix = vcluster.get_target(vcluster.get_uplink(host_ix));
            let rack_id: usize = vcluster[tor_ix]
                .name
                .strip_prefix("tor_")
                .unwrap()
                .parse()
                .unwrap();
            assert!(
                rack_id < num_racks,
                "rack_id: {}, num_racks: {}",
                rack_id,
                num_racks
            );
            agg[rack_id].tx_total += counters.tx_total;
            agg[rack_id].tx_in += counters.tx_in;
            agg[rack_id].rx_total += counters.rx_total;
            agg[rack_id].rx_in += counters.rx_in;
        }

        debug_assert_eq!(agg.len(), num_racks);

        #[allow(clippy::needless_range_loop)]
        for i in 0..num_racks {
            let tor_name = format!("tor_{}", i);
            let tor_ix = vcluster.get_node_index(&tor_name);
            self.vmstats
                .entry(tor_ix)
                .or_default()
                .add_sample(ts, &agg[i]);
        }
    }
}

pub(crate) trait Estimator {
    fn estimate_v1(&self, tenant_id: TenantId) -> NetHintV1;
    // link flows tells which flows are on each link, and it also groups flows with the same fairness property together,
    // which will be useful for estimating available bandwidth.
    fn estimate_v2(
        &self,
        tenant_id: TenantId,
        fairness: FairnessModel,
        link_flows: &HashMap<LinkIx, FlowSet>,
    ) -> NetHintV2;
    fn sample(&mut self, ts: Timestamp);
}

pub struct SimpleEstimator {
    sample_interval_ns: Duration,
    sampler: HashMap<TenantId, Sampler>,
    brain: Rc<RefCell<Brain>>,
}

impl SimpleEstimator {
    pub fn new(brain: Rc<RefCell<Brain>>, sample_interval_ns: Duration) -> Self {
        SimpleEstimator {
            sample_interval_ns,
            sampler: HashMap::default(),
            brain,
        }
    }
}

fn get_phys_link(brain: &Brain, tenant_id: TenantId, vlink_ix: LinkIx) -> LinkIx {
    brain.vlink_to_plink[&(tenant_id, vlink_ix)]
}

fn get_all_virtual_links(
    brain: &Brain,
    plink_ix: LinkIx,
) -> impl Iterator<Item = (TenantId, LinkIx)> + '_ {
    brain.plink_to_vlinks[&plink_ix]
        .iter()
        .map(|&(tenant_id, vlink_ix)| (tenant_id, vlink_ix))
}

impl SimpleEstimator {
    fn compute_fair_share(
        &self,
        phys_link: LinkIx,
        demand: Bandwidth,
        plink_capacity: Bandwidth,
        fairness: FairnessModel,
        link_flows: &HashMap<LinkIx, FlowSet>,
    ) -> Bandwidth {
        match fairness {
            FairnessModel::PerFlowMaxMin => {
                self.compute_fair_share_per_flow(phys_link, demand, plink_capacity, link_flows)
            }
            FairnessModel::PerVmPairMaxMin => unimplemented!(),
            FairnessModel::TenantFlowMaxMin => {
                self.compute_fair_share_per_tenant(phys_link, demand, plink_capacity)
            }
        }
    }

    fn calculate_demand_sum(&self, phys_link: LinkIx) -> Bandwidth {
        let brain = self.brain.borrow();
        let all_virtual_links = get_all_virtual_links(&*brain, phys_link);
        let plink_capacity = brain.cluster()[phys_link].bandwidth;

        let mut demand_sum = 0.gbps();
        let mut cnt = 0;
        for (tenant_id, vlink_ix) in all_virtual_links {
            let demand_i = if let Some(sampler) = self.sampler.get(&tenant_id) {
                let vcluster = brain.vclusters[&tenant_id].borrow();
                let source_ix = vcluster.get_source(vlink_ix);
                let target_ix = vcluster.get_target(vlink_ix);

                if vcluster[source_ix].depth > vcluster[target_ix].depth {
                    // tx
                    match vcluster[source_ix].node_type {
                        NodeType::Switch => sampler.vmstats[&source_ix]
                            .last_delta_tx_total()
                            .zip_with(sampler.vmstats[&source_ix].last_delta_tx_in(), |x, y| {
                                (8.0 * (x - y) as f64 / sampler.interval_ns as f64).gbps()
                            }),
                        NodeType::Host => {
                            sampler.vmstats[&source_ix].last_delta_tx_total().map(|x| {
                                if x > 0 {
                                    append_log_file(&format!(
                                        "host link, last_delta_tx_total: {}",
                                        x
                                    ));
                                }
                                (8.0 * x as f64 / sampler.interval_ns as f64).gbps()
                            })
                        }
                    }
                } else {
                    // rx
                    match vcluster[target_ix].node_type {
                        NodeType::Switch => sampler.vmstats[&target_ix]
                            .last_delta_rx_total()
                            .zip_with(sampler.vmstats[&target_ix].last_delta_rx_in(), |x, y| {
                                (8.0 * (x - y) as f64 / sampler.interval_ns as f64).gbps()
                            }),
                        NodeType::Host => {
                            sampler.vmstats[&target_ix].last_delta_rx_total().map(|x| {
                                if x > 0 {
                                    append_log_file(&format!(
                                        "host link, last_delta_rx_total: {}",
                                        x
                                    ));
                                }
                                (8.0 * x as f64 / sampler.interval_ns as f64).gbps()
                            })
                        }
                    }
                }
            } else {
                None
            };

            demand_sum = demand_sum + demand_i.unwrap_or_else(|| 0.gbps());
            cnt += 1;
        }

        // clamp
        if demand_sum > plink_capacity {
            demand_sum = plink_capacity;
        }

        assert!(
            demand_sum <= plink_capacity,
            "{} vs {}",
            demand_sum,
            plink_capacity
        );

        if demand_sum > 0.gbps() {
            append_log_file(&format!(
                "demand_sum: {}, cnt = {}, pink_capacity/cnt = {}",
                demand_sum,
                cnt,
                plink_capacity / cnt
            ));
        }

        demand_sum
    }

    fn compute_fair_share_per_flow(
        &self,
        phys_link: LinkIx,
        demand: Bandwidth,
        plink_capacity: Bandwidth,
        link_flows: &HashMap<LinkIx, FlowSet>,
    ) -> Bandwidth {
        let demand_sum = self.calculate_demand_sum(phys_link);
        let cnt = link_flows.iter().count();

        demand.min(std::cmp::max(
            plink_capacity - demand_sum,
            plink_capacity / cnt,
        ))
    }

    fn compute_fair_share_per_tenant(
        &self,
        phys_link: LinkIx,
        demand: Bandwidth,
        plink_capacity: Bandwidth,
    ) -> Bandwidth {
        let demand_sum = self.calculate_demand_sum(phys_link);
        let brain = self.brain.borrow();
        let all_virtual_links = get_all_virtual_links(&*brain, phys_link);
        let cnt = all_virtual_links.count();

        demand.min(std::cmp::max(
            plink_capacity - demand_sum,
            plink_capacity / cnt,
        ))
    }
}

impl Estimator for SimpleEstimator {
    fn estimate_v1(&self, tenant_id: TenantId) -> NetHintV1 {
        let mut vcluster = (*self.brain.borrow().vclusters[&tenant_id].borrow()).clone();

        for link_ix in vcluster.all_links() {
            // just some non-zero constant value will be OK
            vcluster[link_ix].bandwidth = 100.gbps();
        }

        vcluster
    }

    fn estimate_v2(&self, tenant_id: TenantId, fairness: FairnessModel, link_flows: &HashMap<LinkIx, FlowSet>) -> NetHintV2 {
        let mut vcluster = (*self.brain.borrow().vclusters[&tenant_id].borrow()).clone();

        let brain = self.brain.borrow();
        for link_ix in vcluster.all_links() {
            let phys_link = get_phys_link(&*brain, tenant_id, link_ix);

            let bw = self.compute_fair_share(
                phys_link,
                brain.cluster()[phys_link].bandwidth,
                brain.cluster()[phys_link].bandwidth,
                fairness,
                link_flows,
            );
            let count = get_all_virtual_links(&*brain, phys_link).count();
            log::info!(
                "phys_link: {}, estimated: bw: {}, vlinks count: {}",
                brain.cluster()[phys_link].bandwidth,
                bw,
                count
            );
            if brain.cluster()[phys_link].bandwidth > bw {
                append_log_file(&format!(
                    "phys_link: {}, estimated: bw: {}, vlinks count: {}",
                    brain.cluster()[phys_link].bandwidth,
                    bw,
                    count
                ));
            }

            // whether the link is limited should also be considered when giving nethint
            vcluster[link_ix].bandwidth = if brain.setting().sharing_mode
                == SharingMode::RateLimited
                && (vcluster[vcluster.get_target(link_ix)].depth > 1 // the link a host link
                    && vcluster[vcluster.get_source(link_ix)].depth > 1)
            {
                let limited_rate = vcluster[link_ix].bandwidth;
                assert!(limited_rate > 0.gbps());
                limited_rate.min(bw)
            } else {
                bw
            }
        }

        vcluster
    }

    fn sample(&mut self, ts: Timestamp) {
        let brain = self.brain.borrow();
        let interval_ns = self.sample_interval_ns;
        for (&tenant_id, vcluster) in brain.vclusters.iter() {
            let vcluster = &*vcluster.borrow();
            self.sampler
                .entry(tenant_id)
                .or_insert_with(|| Sampler::new(interval_ns))
                .sample(vcluster, ts);
        }
    }
}

fn append_log_file(str: &str) {
    use std::io::{Seek, Write};
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open("/tmp/ff")
        .unwrap();
    f.seek(std::io::SeekFrom::End(0)).unwrap();
    writeln!(f, "{}", str).unwrap();
}
