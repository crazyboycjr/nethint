use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use rand::{rngs::StdRng, SeedableRng};
use rand::Rng;

use serde::{Deserialize, Serialize};
// use fnv::FnvHashMap as HashMap;
use fnv::FnvBuildHasher;
use indexmap::IndexMap;
type HashMap<K, V> = IndexMap<K, V, FnvBuildHasher>;

use crate::{
    bandwidth::{self, Bandwidth, BandwidthTrait},
    brain::{Brain, TenantId},
    cluster::{Counters, LinkIx, NodeIx, NodeType, Topology, VirtCluster},
    simulator::FlowSet,
    Duration, FairnessModel, SharingMode, Timestamp,
};

use crate::counterunit::CounterUnit;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetHintVersion {
    V1,
    V2,
}

// TODO(cjr): Migrate NetHintV2 to NetHint_V2

/// Only topology information, all bandwidth are set to 0.gbps()
pub type NetHintV1 = VirtCluster;

/// With network metrics, either collected by measurement or telemetry.
/// Currently, we are using bandwidth as the level 2 hint.
pub type NetHintV2 = VirtCluster;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetHintV1Real {
    pub vc: VirtCluster,
    // this is a dirty hack, we need to know the mapping between virtual name host_{} to a vm hostname, cpu{}
    pub vname_to_hostname: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetHintV2Real {
    // not in simulation
    pub hintv1: NetHintV1Real,
    pub interval_ms: u64,
    pub traffic: std::collections::HashMap<LinkIx, Vec<CounterUnit>>,
}

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
        &mut self,
        tenant_id: TenantId,
        fairness: FairnessModel,
        link_flows: &HashMap<LinkIx, FlowSet>,
        app_hint: usize,
    ) -> NetHintV2;
    fn sample(&mut self, ts: Timestamp);
}

pub struct SimpleEstimator {
    sample_interval_ns: Duration,
    sampler: HashMap<TenantId, Sampler>,
    brain: Rc<RefCell<Brain>>,
    rng: StdRng,
}

impl SimpleEstimator {
    pub fn new(brain: Rc<RefCell<Brain>>, sample_interval_ns: Duration) -> Self {
        SimpleEstimator {
            sample_interval_ns,
            sampler: HashMap::default(),
            brain,
            rng: StdRng::seed_from_u64(222 as u64),
        }
    }
}

pub fn get_phys_link(brain: &Brain, tenant_id: TenantId, vlink_ix: LinkIx) -> LinkIx {
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
        tenant_id: TenantId,
        phys_link: LinkIx,
        demand: Bandwidth,
        plink_capacity: Bandwidth,
        fairness: FairnessModel,
        link_flows: &HashMap<LinkIx, FlowSet>,
        // num_new_flows: usize,
        num_new_objects: usize,
    ) -> Bandwidth {
        match fairness {
            FairnessModel::PerFlowMaxMin => self.compute_fair_share_per_flow(
                tenant_id,
                phys_link,
                demand,
                plink_capacity,
                link_flows,
                num_new_objects, // num_new_flows affects the bandwidth will be shared by this app/tenant
            ),
            FairnessModel::PerVmPairMaxMin => self.compute_fair_share_per_vm_pair(
                tenant_id,
                phys_link,
                demand,
                plink_capacity,
                link_flows,
                num_new_objects,
            ),
            FairnessModel::TenantFlowMaxMin => {
                self.compute_fair_share_per_tenant(tenant_id, phys_link, demand, plink_capacity)
            }
        }
    }

    fn calculate_demand_vec(&self, phys_link: LinkIx, my_tenant_id: TenantId) -> Vec<Bandwidth> {
        let brain = self.brain.borrow();
        let all_virtual_links = get_all_virtual_links(&*brain, phys_link);

        let mut demand_vec = Vec::new();
        for (tenant_id, vlink_ix) in all_virtual_links {
            if tenant_id == my_tenant_id {
                continue;
            }

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

            demand_vec.push(demand_i.unwrap_or_else(|| 0.gbps()));
        }

        demand_vec
    }

    fn calculate_demand_sum(&self, phys_link: LinkIx, my_tenant_id: TenantId) -> Bandwidth {
        let brain = self.brain.borrow();
        let plink_capacity = brain.cluster()[phys_link].bandwidth;

        let demand_vec = self.calculate_demand_vec(phys_link, my_tenant_id);
        let mut demand_sum: Bandwidth = demand_vec.iter().copied().sum();

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
            let cnt = demand_vec.len() + 1;
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
        tenant_id: TenantId,
        phys_link: LinkIx,
        demand: Bandwidth,
        plink_capacity: Bandwidth,
        link_flows: &HashMap<LinkIx, FlowSet>,
        num_new_flows: usize,
    ) -> Bandwidth {
        let demand_sum = self.calculate_demand_sum(phys_link, tenant_id);
        let cnt = link_flows
            .get(&phys_link)
            .map(|fs| fs.iter().count())
            .unwrap_or(0);
        assert!(num_new_flows > 0);
        // assert!(
        //     cnt < 10,
        //     "cnt: {}, link: {:?}, link_flows: {:?}",
        //     cnt,
        //     phys_link,
        //     link_flows[&phys_link]
        // );

        assert!(num_new_flows > 0);

        demand.min(std::cmp::max(
            plink_capacity - demand_sum,
            plink_capacity / (cnt + num_new_flows) * num_new_flows,
        ))
    }

    fn compute_fair_share_per_vm_pair(
        &self,
        tenant_id: TenantId,
        phys_link: LinkIx,
        demand: Bandwidth,
        plink_capacity: Bandwidth,
        link_flows: &HashMap<LinkIx, FlowSet>,
        num_new_vm_pairs: usize,
    ) -> Bandwidth {
        let demand_sum = self.calculate_demand_sum(phys_link, tenant_id);

        // count the number of vm pairs passed on this physical link
        use std::collections::HashSet;
        let cnt = link_flows
            .get(&phys_link)
            .map(|fs| {
                let set = fs
                    .iter()
                    .map(|f| {
                        (
                            f.borrow().flow.vsrc.clone().unwrap(),
                            f.borrow().flow.vdst.clone().unwrap(),
                        )
                    })
                    .collect::<HashSet<(String, String)>>();
                set.len()
            })
            .unwrap_or(0);

        assert!(num_new_vm_pairs > 0);

        // because like in perflow where we do not count the actual bytes of each flow, we do not count
        // the number of bytes of each vm pair. So we only use the number of vm pairs to roughly
        // estimate the demand.

        demand.min(std::cmp::max(
            plink_capacity - demand_sum,
            plink_capacity / (cnt + num_new_vm_pairs) * num_new_vm_pairs,
        ))
    }

    fn compute_fair_share_per_tenant(
        &self,
        my_tenant_id: TenantId,
        phys_link: LinkIx,
        demand: Bandwidth,
        plink_capacity: Bandwidth,
    ) -> Bandwidth {
        let mut demand_vec = self.calculate_demand_vec(phys_link, my_tenant_id);
        // let demand_sum = self.calculate_demand_sum(phys_link, my_tenant_id);

        // return demand.min(std::cmp::max(
        //     plink_capacity - demand_sum,
        //     plink_capacity / demand_vec.len(),
        // ));

        // calculate max min fair share

        // append my_tenant_id
        demand_vec.push(demand);

        let mut num_converged = 0;
        let mut converged = vec![false; demand_vec.len()];
        let mut share = vec![0.gbps(); demand_vec.len()];

        // compute max min fair share
        while num_converged < demand_vec.len() {
            let mut bound = demand_vec.clone();

            let share_sum: Bandwidth = share.iter().cloned().sum();
            assert!(share_sum < plink_capacity);
            let alloced_share =
                (plink_capacity - share_sum) / (demand_vec.len() - num_converged) as f64;

            let mut min_inc = bandwidth::MAX;
            for i in 0..demand_vec.len() {
                if !converged[i] {
                    bound[i] = bound[i].min(share[i] + alloced_share);
                    min_inc = min_inc.min(bound[i] - share[i]);
                }
            }

            assert!(min_inc < bandwidth::MAX);

            let mut tmp = 0;
            for i in 0..demand_vec.len() {
                if !converged[i] {
                    share[i] = share[i] + min_inc;
                    if share[i] + 1.kbps() >= bound[i] {
                        tmp += 1;
                        converged[i] = true;
                    }
                }
            }

            assert!(
                tmp > 0,
                "min_inc: {}, share: {:?}, bound: {:?}",
                min_inc,
                share,
                bound
            );
            num_converged += tmp;
        }

        *share.last().unwrap()
    }

    fn calc_num_new_vm_pairs(&self, vc: &dyn Topology, vlink_ix: LinkIx, app_hint: usize) -> usize {
        // for now, we just assume the nubmer of new flows equals to the number of new vm pairs
        // in our algorithms
        self.calc_num_new_flows(vc, vlink_ix, app_hint)
    }

    fn calc_num_new_flows(&self, vc: &dyn Topology, vlink_ix: LinkIx, app_hint: usize) -> usize {
        // accord to the position of the link, decide how many flows are going to be added to this link
        // basically, a link break the tree into two parts A and B, we roughly estimate the number of newly added flow
        // on this link as size(A) * size(B). This estimation makes sense in MapReduce. For allreduce, it may
        // not be that case. For allreduce, we use RAT. Each node on average will have a + n + (n / b) * (b - 1), a is num_racks
        // b is rack_size of that node. Each cross rack link will have max(1, b * (a - 1) + n - b)
        let mut high_node = vc[vc.get_source(vlink_ix)].clone();
        let mut low_node = vc[vc.get_target(vlink_ix)].clone();
        if high_node.depth > low_node.depth {
            std::mem::swap(&mut high_node, &mut low_node);
        }

        let num_new_flows = if high_node.depth == 1 {
            match app_hint {
                0 => {
                    // for mapreduce
                    let n = vc.num_hosts();
                    let a = vc.get_downlinks(vc.get_node_index(&low_node.name)).count();
                    n * a
                }
                1 => {
                    // for allreduce
                    let a = vc.get_downlinks(vc.get_node_index("virtual_cloud")).count();
                    let n = vc.num_hosts();
                    let b = vc.get_downlinks(vc.get_node_index(&low_node.name)).count();
                    std::cmp::max(1, b * (a - 1) + n - b)
                }
                2 => {
                    // for rl broadcast
                    1
                }
                _ => panic!("unexpected app_hint: {}", app_hint),
            }
        } else {
            match app_hint {
                0 => {
                    // for mapreduce
                    vc.num_hosts()
                }
                1 => {
                    // for allreduce
                    let a = vc.get_downlinks(vc.get_node_index("virtual_cloud")).count();
                    let n = vc.num_hosts();
                    let b = vc.get_downlinks(vc.get_node_index(&high_node.name)).count();
                    a + n + n * (b - 1) / b
                }
                2 => {
                    // for rl broadcast
                    1
                }
                _ => panic!("unexpected app_hint: {}", app_hint),
            }
        };

        num_new_flows
    }

    fn calc_num_new_objects(
        &self,
        vc: &dyn Topology,
        vlink_ix: LinkIx,
        app_hint: usize,
        fairness: FairnessModel,
    ) -> usize {
        match fairness {
            FairnessModel::PerFlowMaxMin => self.calc_num_new_flows(vc, vlink_ix, app_hint),
            FairnessModel::PerVmPairMaxMin => self.calc_num_new_vm_pairs(vc, vlink_ix, app_hint),
            FairnessModel::TenantFlowMaxMin => 1,
        }
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

    fn estimate_v2(
        &mut self,
        tenant_id: TenantId,
        fairness: FairnessModel,
        link_flows: &HashMap<LinkIx, FlowSet>,
        app_hint: usize, // hint from app, 0 for mapreduce, 1 for allreduce, it's a dirty hack
    ) -> NetHintV2 {
        let mut vcluster = (*self.brain.borrow().vclusters[&tenant_id].borrow()).clone();
        log::info!("estimate_v2: {}", vcluster.to_dot());

        let brain = self.brain.borrow();
        for link_ix in vcluster.all_links() {
            let phys_link = get_phys_link(&*brain, tenant_id, link_ix);

            let num_new_objects = self.calc_num_new_objects(&vcluster, link_ix, app_hint, fairness);

            let factor = self.rng.gen_range(0.1..1.9);
            
            let bw = self.compute_fair_share(
                tenant_id,
                phys_link,
                brain.cluster()[phys_link].bandwidth,
                brain.cluster()[phys_link].bandwidth*factor,
                fairness,
                link_flows,
                num_new_objects,
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
    // use std::io::{Seek, Write};
    // let mut f = utils::fs::open_with_create_append("/tmp/ff");
    // f.seek(std::io::SeekFrom::End(0)).unwrap();
    // writeln!(f, "{}", str).unwrap();
    utils::fs::append_to_file("/tmp/ff", str);
}
