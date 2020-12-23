use std::cell::RefCell;
use std::rc::Rc;

use log::warn;

use crate::{
    bandwidth::BandwidthTrait,
    brain::{Brain, TenantId},
    cluster::{Topology, VirtCluster},
    Duration, Timestamp,
};

/// Only topology information, all bandwidth are set to 0.gbps()
pub type NetHintV1 = VirtCluster;

/// With network metrics, either collected by measurement or telemetry.
/// Currently, we are using bandwidth as the level 2 hint.
pub type NetHintV2 = VirtCluster;

/// Sample point
struct Point<V> {
    ts: Timestamp,
    val: V,
}

struct VmStats {
    tx_total: Vec<Point<u64>>,
    /// tx traffic within the rack
    tx_in: Vec<Point<u64>>,
}

struct Sampler {
    interval_ns: Duration,
    vmstats: Vec<VmStats>,
}

impl Sampler {
    fn new(interval_ns: Duration) -> Self {
        Sampler {
            interval_ns,
            vmstats: Default::default(),
        }
    }
}

pub trait Estimator {
    fn estimate(&self, tenant_id: TenantId) -> NetHintV2;
    fn sample(&mut self);
}

pub struct SimpleEstimator {
    sampler: Sampler,
    brain: Rc<RefCell<Brain>>,
}

impl SimpleEstimator {
    pub fn new(brain: Rc<RefCell<Brain>>, sample_interval_ns: Duration) -> Self {
        SimpleEstimator {
            sampler: Sampler::new(sample_interval_ns),
            brain,
        }
    }
}

impl Estimator for SimpleEstimator {
    fn estimate(&self, tenant_id: TenantId) -> NetHintV2 {
        // let vcluster = Rc::clone(&self.brain.borrow().vclusters[&tenant_id]);
        // let mut ret = (*vcluster.borrow()).clone();
        let mut vcluster = (*self.brain.borrow().vclusters[&tenant_id].borrow()).clone();
        // fill the link with some arbitrary bandwidth
        for link_ix in vcluster.all_links() {
            vcluster[link_ix].bandwidth = 100.gbps();
        }
        vcluster
    }

    fn sample(&mut self) {
        //
    }
}
