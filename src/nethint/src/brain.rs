use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

use fnv::FnvHashMap as HashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::bandwidth::{Bandwidth, BandwidthTrait};
use crate::cluster::{Cluster, Link, LinkIx, Node, NodeIx, NodeType, Topology, VirtCluster};
use crate::{
    architecture::{build_arbitrary_cluster, build_fatree_fake, TopoArgs},
    SharingMode,
};

pub type TenantId = usize;

pub const MAX_SLOTS: usize = 4;

/// High frequency changed background flows.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackgroundFlowHighFreq {
    enable: bool,
    #[serde(default)]
    probability: f64,
    // the amplitude range in [1, 9], it cut down the original bandwidth of a link by up to amplitude/10.
    // for example, if amplitude = 9, it means that up to 90Gbps (90%) can be taken from a 100Gbps link.
    #[serde(default)]
    amplitude: usize,
}

impl Default for BackgroundFlowHighFreq {
    fn default() -> Self {
        Self {
            enable: false,
            probability: 0.0,
            amplitude: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrainSetting {
    /// Random seed for multiple uses
    pub seed: u64,
    /// Asymmetric bandwidth
    pub asymmetric: bool,
    /// Mark some nodes as broken to be more realistic
    pub broken: f64,
    /// The slots of each physical machine
    pub max_slots: usize,
    /// how bandwidth is partitioned among multiple VMs in the same physical server, possible values are "RateLimited", "Guaranteed"
    pub sharing_mode: SharingMode,
    /// guaranteed bandwidth, in Gbps
    pub guaranteed_bandwidth: Option<f64>,
    /// The parameters of the cluster's physical topology
    pub topology: TopoArgs,

    pub background_flow_high_freq: BackgroundFlowHighFreq,
    /// GC period
    pub gc_period: usize,

    /// bandwidth inaccuracy
    pub inaccuracy: Option<f64>,
}

/// can only send by replicating (deep copy) the object, user should be very careful about this
unsafe impl Send for Brain {}
unsafe impl Sync for Brain {}

/// Brain is the cloud controller. It knows the underlay physical
/// topology and decides how to provision VMs for tenants.
#[derive(Debug)]
pub struct Brain {
    /// Brain settings
    setting: BrainSetting,
    /// physical clsuter
    cluster: Arc<Cluster>,
    /// original bandwidth
    orig_bw: HashMap<LinkIx, Bandwidth>,
    /// used set of nodes in phycisal cluster, the value
    /// represents which VMs have been allocated on this physical node
    /// the VM are numbered from 0 to max_slots - 1.
    used: HashMap<NodeIx, Vec<usize>>,
    /// now we only support each tenant owns a single VirtCluster
    pub(crate) vclusters: HashMap<TenantId, Rc<RefCell<VirtCluster>>>,
    /// map from physical link to virtual links
    pub(crate) plink_to_vlinks: HashMap<LinkIx, Vec<(TenantId, LinkIx)>>,
    /// virtual link to physical link
    pub(crate) vlink_to_plink: HashMap<(TenantId, LinkIx), LinkIx>,
    /// a queue to enforce FIFO order to destroy VMs
    gc_queue: Vec<TenantId>,
    /// save the state of updating background flow traffic
    background_flow_update_cnt: u64,
}

/// Similar to AWS Placement Groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "args")]
pub enum PlacementStrategy {
    /// Prefer to choose hosts within the same rack.
    Compact,
    /// Prefer to choose hosts within the same rack, and also prefer to choose empty racks.
    CompactLoadBalanced,
    /// Prefer to spread instances to different racks.
    Spread,
    /// Random uniformally choose some hosts to allocate.
    Random(u64),
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("insufficient empty host available, available: {0}, requested: {1}")]
    NoHost(usize, usize),
    #[error("invalid host name: {0}")]
    InvalidHostName(String),
}

impl Brain {
    pub fn replicate_for_multithread(&self) -> Self {
        let new_cluster = (*self.cluster).clone();
        let vclusters = self
            .vclusters
            .iter()
            .map(|(&k, v)| {
                let new_vc = (*v.borrow()).clone();
                (k, Rc::new(RefCell::new(new_vc)))
            })
            .collect();
        Brain {
            setting: self.setting.clone(),
            cluster: Arc::new(new_cluster),
            orig_bw: self.orig_bw.clone(),
            used: self.used.clone(),
            vclusters,
            plink_to_vlinks: self.plink_to_vlinks.clone(),
            vlink_to_plink: self.vlink_to_plink.clone(),
            gc_queue: self.gc_queue.clone(),
            background_flow_update_cnt: self.background_flow_update_cnt,
        }
    }

    pub fn set_seed(&mut self, seed: u64) {
        self.setting.seed = seed;
    }

    pub fn build_cloud(setting: BrainSetting) -> Rc<RefCell<Self>> {
        let cluster = match setting.topology {
            TopoArgs::FatTree {
                nports,
                bandwidth,
                oversub_ratio,
            } => {
                let cluster = build_fatree_fake(nports, bandwidth.gbps(), oversub_ratio);
                assert_eq!(cluster.num_hosts(), nports * nports * nports / 4);
                cluster
            }
            TopoArgs::Arbitrary {
                nracks,
                rack_size,
                host_bw,
                rack_bw,
            } => build_arbitrary_cluster(nracks, rack_size, host_bw.gbps(), rack_bw.gbps()),
        };

        let used = (0..cluster.num_hosts())
            .map(|i| {
                let host_ix = cluster.get_node_index(&format!("host_{}", i));
                (host_ix, Vec::new())
            })
            .collect();

        let orig_bw = cluster
            .all_links()
            .map(|link_ix| (link_ix, cluster[link_ix].bandwidth))
            .collect();

        let mut brain = Brain {
            setting,
            cluster: Arc::new(cluster),
            orig_bw,
            used,
            vclusters: Default::default(),
            // phys_to_virt: Default::default(),
            plink_to_vlinks: Default::default(),
            vlink_to_plink: Default::default(),
            gc_queue: Default::default(),
            background_flow_update_cnt: 0,
        };

        if brain.setting.asymmetric {
            brain.make_asymmetric(1.0, 50);
        }

        // set high frequency part of background flow
        if brain.setting.background_flow_high_freq.enable {
            brain.make_asymmetric(
                brain.setting.background_flow_high_freq.probability,
                brain.setting.background_flow_high_freq.amplitude,
            );
        }

        brain.mark_broken(brain.setting.seed, brain.setting.broken);

        Rc::new(RefCell::new(brain))
    }

    pub fn cluster(&self) -> &Arc<Cluster> {
        &self.cluster
    }

    pub fn setting(&self) -> &BrainSetting {
        &self.setting
    }

    pub fn vclusters(&self) -> &HashMap<TenantId, Rc<RefCell<VirtCluster>>> {
        &self.vclusters
    }

    pub fn make_asymmetric(&mut self, probability: f64, amplitude: usize) {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(self.setting.seed);

        let cluster = Arc::get_mut(&mut self.cluster)
            .expect("there should be no other reference to physical cluster");

        for link_ix in cluster.all_links() {
            if link_ix.index() & 1 == 1 {
                if rng.gen_range(0.0..1.0) < probability {
                    log::info!("old_bw: {}", cluster[link_ix].bandwidth);
                    let new_bw = cluster[link_ix].bandwidth
                        * (1.0 - rng.gen_range(1..=amplitude) as f64 / 100.0);
                    cluster[link_ix] = Link::new(new_bw);
                    let reverse_link_ix = cluster.get_reverse_link(link_ix);
                    cluster[reverse_link_ix] = Link::new(new_bw);
                    log::info!("new_bw: {}", new_bw);
                }
            }
        }

        // also update orig_bw
        self.orig_bw = cluster
            .all_links()
            .map(|link_ix| (link_ix, cluster[link_ix].bandwidth))
            .collect();
    }

    fn mark_broken(&mut self, seed: u64, ratio: f64) {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(seed + 10000);

        let cluster = Arc::get_mut(&mut self.cluster)
            .expect("there should be no other reference to physical cluster");

        let max_slots = self.setting.max_slots;
        for i in 0..cluster.num_hosts() {
            let node_ix = cluster.get_node_index(&format!("host_{}", i));
            if rng.gen_range(0.0..1.0) < ratio {
                self.used
                    .entry(node_ix)
                    .and_modify(|e| *e = (0..max_slots).collect());
            }
        }
    }

    fn reset_link_bandwidth(&mut self) {
        let cluster = Arc::get_mut(&mut self.cluster)
            .expect("there should be no other reference to physical cluster");

        for link_ix in cluster.all_links() {
            // recover the link's capacity
            let bw = self.orig_bw[&link_ix];
            cluster[link_ix] = Link::new(bw);
        }
    }

    pub fn clear_background_flow_update_cnt(&mut self) {
        self.background_flow_update_cnt = 0;
    }

    pub fn update_background_flow_hard(
        &mut self,
        probability: f64,
        amplitude: usize,
        zipf_exp: f64,
    ) {
        self.background_flow_update_cnt += 1;

        use rand::{distributions::Distribution, rngs::StdRng, Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(self.setting.seed + self.background_flow_update_cnt);

        self.reset_link_bandwidth();

        let cluster = Arc::get_mut(&mut self.cluster)
            .expect("there should be no other reference to physical cluster");

        log::debug!("zipf_exp: {}", zipf_exp);
        let zipf = zipf::ZipfDistribution::new(amplitude * 10, zipf_exp).unwrap();

        for link_ix in cluster.all_links() {
            if rng.gen_range(0.0..1.0) < probability {
                // if link_ix.index() % 2 == 0 {
                //     continue;
                // }

                // integer overflow will only be checked in debug mode, so I should have detected an error here.
                let current_tenants = self.plink_to_vlinks.entry(link_ix).or_default().len();
                let total_slots = self.setting.max_slots;
                let new_bw: Bandwidth = if cluster[cluster.get_target(link_ix)].depth == 1
                    || cluster[cluster.get_source(link_ix)].depth == 1
                {
                    assert!(
                        amplitude < 10,
                        "amplitude must be range in [1, 9], got {}",
                        amplitude
                    );
                    let new_bw =
                        self.orig_bw[&link_ix] * (1.0 - zipf.sample(&mut rng) as f64 / 100.0);
                    // let new_bw = self.orig_bw[&link_ix];
                    new_bw
                } else {
                    match self.setting.sharing_mode {
                        SharingMode::RateLimited => {
                            // if the sharing mode is "RateLimited", then we are good here
                            self.orig_bw[&link_ix] * (1.0 - zipf.sample(&mut rng) as f64 / 100.0)
                        }
                        SharingMode::Guaranteed => {
                            // if the sharing mode is "Guaranteed", then we cannot take too much bandwidth from that host
                            let orig_bw = self.orig_bw[&link_ix];
                            let guaranteed_bw = self
                                .setting
                                .guaranteed_bandwidth
                                .expect("Expect a bandwidth lower bound.");
                            let coeff = if amplitude as f64 * 10.0 / 100.0
                                < ((orig_bw.val() as f64 - guaranteed_bw.gbps().val() as f64)
                                    / orig_bw.val() as f64)
                            {
                                zipf.sample(&mut rng) as f64 / 100.0
                            } else {
                                zipf.sample(&mut rng) as f64 / 100.0
                                    * ((orig_bw.val() as f64 - guaranteed_bw.gbps().val() as f64)
                                        / orig_bw.val() as f64)
                            };
                            let new_bw = orig_bw * (1.0 - coeff);
                            assert!(
                                new_bw >= guaranteed_bw.gbps(),
                                "new_bw: {}, guaranteed_bw: {}",
                                new_bw,
                                guaranteed_bw.gbps()
                            );
                            new_bw
                        }
                    }
                };

                assert!(
                    new_bw.val() > 0,
                    "current_tenants: {}, total_slots: {}",
                    current_tenants,
                    total_slots
                );

                cluster[link_ix] = Link::new(new_bw);
                // let reverse_link_ix = cluster.get_reverse_link(link_ix);
                // cluster[reverse_link_ix] = Link::new(new_bw);
            }
        }
    }

    pub fn provision(
        &mut self,
        tenant_id: TenantId,
        nhosts: usize,
        strategy: PlacementStrategy,
    ) -> Result<VirtCluster, Error> {
        if self.setting.gc_period == 0 || tenant_id > 0 && tenant_id % self.setting.gc_period == 0 {
            self.garbage_collect(tenant_id - self.setting.gc_period);
        }

        let taken = self.used.values().map(|x| x.len()).sum::<usize>();
        if taken + nhosts > self.cluster.num_hosts() * self.setting.max_slots {
            return Err(Error::NoHost(
                self.cluster.num_hosts() * self.setting.max_slots - taken,
                nhosts,
            ));
        }

        let (inner, virt_to_phys, virt_to_vmno) = match strategy {
            PlacementStrategy::Compact => self.place_compact(nhosts),
            PlacementStrategy::CompactLoadBalanced => self.place_compact_load_balanced(nhosts),
            PlacementStrategy::Spread => unimplemented!(),
            PlacementStrategy::Random(seed) => self.place_random(nhosts, seed),
        };

        if let Ok(path) = std::env::var("NETHINT_PROVISION_LOG_FILE") {
            use std::io::{Seek, Write};
            let mut f = utils::fs::open_with_create_append(path);
            f.seek(std::io::SeekFrom::End(0)).unwrap();
            writeln!(f, "{:?}", virt_to_phys).unwrap();
        }

        let vcluster = VirtCluster {
            inner,
            virt_to_phys,
            virt_to_vmno,
            tenant_id,
        };

        let ret = vcluster.clone();

        // update link mappings
        for vlink_ix in vcluster.all_links() {
            let vsrc = vcluster.get_source(vlink_ix);
            let vdst = vcluster.get_target(vlink_ix);
            let psrc = self
                .cluster
                .get_node_index(&vcluster.translate(&vcluster[vsrc].name));
            let pdst = self
                .cluster
                .get_node_index(&vcluster.translate(&vcluster[vdst].name));
            let plink_ix = self.cluster.find_link(psrc, pdst).unwrap_or_else(|| {
                panic!(
                    "cannot find link from vname:{} to vname:{} in physical cluster",
                    vcluster[vsrc].name, vcluster[vdst].name
                )
            });

            self.vlink_to_plink
                .insert((tenant_id, vlink_ix), plink_ix)
                .ok_or(())
                .unwrap_err();
            self.plink_to_vlinks
                .entry(plink_ix)
                .or_insert_with(Vec::new)
                .push((tenant_id, vlink_ix));
        }

        // update tenant_id -> VirtCluster
        self.vclusters
            .insert(tenant_id, Rc::new(RefCell::new(vcluster)))
            .ok_or(())
            .unwrap_err();

        Ok(ret)
    }

    pub fn garbage_collect(&mut self, until: TenantId) {
        #[allow(clippy::stable_sort_primitive)]
        self.gc_queue.sort();
        self.gc_queue.reverse();
        while let Some(tenant_id) = self.gc_queue.pop() {
            if tenant_id >= until {
                self.gc_queue.push(tenant_id);
                break;
            }

            let vcluster = self
                .vclusters
                .remove(&tenant_id)
                .unwrap_or_else(|| panic!("tenant_id: {}", tenant_id));

            vcluster.borrow().all_links().for_each(|vlink_ix| {
                let plink_ix = self.vlink_to_plink[&(tenant_id, vlink_ix)];
                let v = self.plink_to_vlinks.get_mut(&plink_ix).unwrap();
                v.remove(v.iter().position(|&x| x == (tenant_id, vlink_ix)).unwrap());
            });

            vcluster.borrow().all_links().for_each(|vlink_ix| {
                self.vlink_to_plink.remove(&(tenant_id, vlink_ix));
            });

            vcluster
                .borrow()
                .virt_to_vmno()
                .iter()
                .for_each(|(name, &vmno)| {
                    let phys_name = vcluster.borrow().virt_to_phys()[name].clone();
                    let pnode_ix = self.cluster.get_node_index(&phys_name);
                    self.used.entry(pnode_ix).and_modify(|e| {
                        let pos = e.iter().position(|&x| x == vmno).unwrap();
                        e.remove(pos);
                    });
                });
        }
    }

    pub fn reset(&mut self) {
        self.background_flow_update_cnt = 0;
        self.reset_link_bandwidth();
        self.garbage_collect(TenantId::MAX);
    }

    pub fn destroy(&mut self, tenant_id: TenantId) {
        // lazily destroy
        // NOTE that this lazily destroy mechanism still cannot guarantee that
        // all jobs from different strategies to see the same environment (allocation status).
        log::info!("destroy: {}", tenant_id);
        self.gc_queue.push(tenant_id);
    }

    fn place_specified(
        &mut self,
        hosts_spec: &[NodeIx],
    ) -> (Cluster, HashMap<String, String>, HashMap<String, usize>) {
        if hosts_spec.iter().cloned().collect::<HashSet<_>>().len() < hosts_spec.len() {
            log::warn!(
                "multiple VMs will be placed on the same host, hosts_spec: {:?}",
                hosts_spec
            );
        }
        let mut tor_alloc: HashMap<NodeIx, String> = HashMap::default();
        let mut host_alloc: Vec<(NodeIx, String)> = Default::default();
        let mut virt_to_vmno: HashMap<String, usize> = HashMap::default();

        let mut inner = Cluster::new();
        let root = "virtual_cloud";
        inner.add_node(Node::new(root, 1, NodeType::Switch));

        for &host_ix in hosts_spec {
            let uplink_ix = self.cluster.get_uplink(host_ix);
            let tor_ix = self.cluster.get_target(uplink_ix);

            #[allow(clippy::map_entry)]
            if !tor_alloc.contains_key(&tor_ix) {
                // new ToR in virtual cluster
                let tor_name = format!("tor_{}", tor_alloc.len());
                inner.add_node(Node::new(&tor_name, 2, NodeType::Switch));
                // despite we have sharing mode, we intentionally leave 0.gbps() here.
                // Then if anywhere tries to use this information, it will probably yields some errors during computation.
                inner.add_link_by_name(root, &tor_name, 0.gbps());
                tor_alloc.insert(tor_ix, tor_name);
            }

            let host_len = host_alloc.len();
            let vhost_name = &format!("host_{}", host_len);
            host_alloc.push((host_ix, vhost_name.clone()));

            // connect the host to the ToR
            inner.add_node(Node::new(vhost_name, 3, NodeType::Host));

            // here we compute the limited rate of a virtual machine
            let phys_link = self.cluster().get_uplink(host_ix);
            let phys_link_bw = self.orig_bw[&phys_link];
            let allocated_bw = match self.setting.sharing_mode {
                SharingMode::RateLimited => phys_link_bw / self.setting.max_slots, // also the rate limit works on both direction of the link
                SharingMode::Guaranteed => phys_link_bw,
            };
            inner.add_link_by_name(&tor_alloc[&tor_ix], vhost_name, allocated_bw);

            let max_slots = self.setting.max_slots;
            self.used.entry(host_ix).and_modify(|e| {
                let next = Self::find_next_slot(e, max_slots)
                    .unwrap_or_else(|| panic!("host_ix: {:?}, used: {:?}", host_ix, e));
                e.push(next);
                virt_to_vmno.insert(vhost_name.clone(), next).ok_or(()).unwrap_err();
            });
        }

        tor_alloc
            .insert(
                self.cluster().get_node_index("cloud"),
                "virtual_cloud".to_owned(),
            )
            .ok_or(())
            .unwrap_err();

        let virt_to_phys = host_alloc
            .into_iter()
            .chain(tor_alloc)
            .map(|(k, v)| (v, self.cluster[k].name.clone()))
            .collect();

        (inner, virt_to_phys, virt_to_vmno)
    }

    fn place_random(
        &mut self,
        nhosts: usize,
        seed: u64,
    ) -> (Cluster, HashMap<String, String>, HashMap<String, usize>) {
        use rand::seq::{IteratorRandom, SliceRandom};
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(seed);

        let alloced_hosts = {
            let avail_hosts = (0..self.cluster.num_hosts())
                .into_iter()
                .map(|i| self.cluster.get_node_index(&format!("host_{}", i)))
                .filter(|node_ix| self.used[node_ix].len() < self.setting.max_slots);
            let mut choosed: Vec<NodeIx> = avail_hosts.choose_multiple(&mut rng, nhosts);
            choosed.shuffle(&mut rng);
            choosed
        };

        assert_eq!(alloced_hosts.len(), nhosts);

        self.place_specified(&alloced_hosts)
    }

    fn place_compact(
        &mut self,
        nhosts: usize,
    ) -> (Cluster, HashMap<String, String>, HashMap<String, usize>) {
        let mut allocated = 0;
        let mut alloced_hosts = Vec::new();

        // place VMs at the next empty slot, and try to avoid collocation
        let num_hosts = self.cluster.num_hosts();
        let mut used: HashMap<NodeIx, usize> =
            self.used.iter().map(|(&k, v)| (k, v.len())).collect();
        while allocated < nhosts {
            for i in 0..num_hosts {
                let host_name = format!("host_{}", i);
                let host_ix = self.cluster.get_node_index(&host_name);
                if used[&host_ix] < self.setting.max_slots {
                    let n = &self.cluster[host_ix];
                    assert_eq!(n.depth, 3); // this doesn't have to be true

                    allocated += 1;
                    alloced_hosts.push(host_ix);
                    used.entry(host_ix).and_modify(|e| *e += 1);

                    if allocated >= nhosts {
                        break;
                    }
                }
            }
        }

        use rand::seq::SliceRandom;
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(0); // use a constant seed
        alloced_hosts.shuffle(&mut rng);

        self.place_specified(&alloced_hosts)
    }

    fn place_compact_load_balanced(
        &mut self,
        nhosts: usize,
    ) -> (Cluster, HashMap<String, String>, HashMap<String, usize>) {
        let mut allocated = 0;
        let mut alloced_hosts = Vec::new();

        let mut used: HashMap<NodeIx, usize> =
            self.used.iter().map(|(&k, v)| (k, v.len())).collect();

        while allocated < nhosts {
            let mut tors: Vec<(usize, NodeIx)> = (0..self.cluster.num_switches() - 1)
                .map(|i| {
                    let name = format!("tor_{}", i);
                    let tor_ix = self.cluster.get_node_index(&name);
                    let cap: usize = self
                        .cluster
                        .get_downlinks(tor_ix)
                        .map(|&link_ix| {
                            let host_ix = self.cluster.get_target(link_ix);
                            self.setting.max_slots - used[&host_ix]
                        })
                        .sum();
                    (cap, tor_ix)
                })
                .collect();
            tors.sort_by_key(|x| x.0);

            for (w, tor_ix) in tors.into_iter().rev() {
                let mut to_pick = w.min(nhosts - allocated);

                // traverse the tor and pick up w.min(nhosts) nodes
                let mut avail_host_ixs: Vec<_> = self
                    .cluster
                    .get_downlinks(tor_ix)
                    .map(|&link_ix| self.cluster.get_target(link_ix))
                    .collect();
                avail_host_ixs.sort_by_key(|host_ix| used[host_ix]);

                for host_ix in avail_host_ixs {
                    if to_pick == 0 {
                        break;
                    }
                    if used[&host_ix] < self.setting.max_slots {
                        let n = &self.cluster[host_ix];
                        assert_eq!(n.depth, 3); // this doesn't have to be true

                        to_pick -= 1;
                        allocated += 1;
                        alloced_hosts.push(host_ix);
                        used.entry(host_ix).and_modify(|e| *e += 1);
                    }
                }

                if allocated >= nhosts {
                    break;
                }
            }
        }

        use rand::seq::SliceRandom;
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(0); // use a constant seed
        alloced_hosts.shuffle(&mut rng);

        self.place_specified(&alloced_hosts)
    }

    fn find_next_slot(v: &[usize], max_slots: usize) -> Option<usize> {
        for i in 0..max_slots {
            if v.iter().find(|&&x| x == i).is_none() {
                return Some(i);
            }
        }
        None
    }
}
