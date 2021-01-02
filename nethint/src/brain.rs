use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use thiserror::Error;

use crate::architecture::{build_arbitrary_cluster, build_fatree_fake, TopoArgs};
use crate::bandwidth::BandwidthTrait;
use crate::cluster::{Cluster, Link, LinkIx, Node, NodeIx, NodeType, Topology, VirtCluster};

pub type TenantId = usize;

/// Brain is the cloud controller. It knows the underlay physical
/// topology and decides how to provision VMs for tenants.
#[derive(Debug)]
pub struct Brain {
    /// physical clsuter
    cluster: Arc<Cluster>,
    /// used set of nodes in phycisal cluster
    used: HashSet<NodeIx>,
    /// now we only support each tenant owns a single VirtCluster
    pub(crate) vclusters: HashMap<TenantId, Rc<RefCell<VirtCluster>>>,
    /// physical node index + tenant_id to virtual node index
    phys_to_virt: HashMap<(NodeIx, TenantId), NodeIx>,
    /// map from physical link to virtual links
    pub(crate) plink_to_vlinks: HashMap<LinkIx, Vec<(TenantId, LinkIx)>>,
    /// virtual link to physical link
    pub(crate) vlink_to_plink: HashMap<(TenantId, LinkIx), LinkIx>,
}

/// Similar to AWS Placement Groups.
#[derive(Debug, Clone, Copy)]
pub enum PlacementStrategy {
    /// Prefer to choose hosts within the same rack.
    Compact,
    /// Prefer to spread instances to different racks.
    Spread,
    /// Random uniformally choose some hosts to allocate.
    Random(u64),
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("not enough empty host available, available: {0}, requested: {1}")]
    NoHost(usize, usize),
    #[error("invalid host name: {0}")]
    InvalidHostName(String),
}

impl Brain {
    pub fn build_cloud(topo: TopoArgs) -> Rc<RefCell<Self>> {
        let cluster = match topo {
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

        Rc::new(RefCell::new(Brain {
            cluster: Arc::new(cluster),
            used: Default::default(),
            vclusters: Default::default(),
            phys_to_virt: Default::default(),
            plink_to_vlinks: Default::default(),
            vlink_to_plink: Default::default(),
        }))
    }

    pub fn cluster(&self) -> &Arc<Cluster> {
        &self.cluster
    }

    pub(crate) fn phys_to_virt(&self, node_ix: NodeIx, tenant_id: TenantId) -> NodeIx {
        *self
            .phys_to_virt
            .get(&(node_ix, tenant_id))
            .unwrap_or_else(|| {
                panic!(
                    "node_ix: {:?}, teannt_id: {} not found in physical cluster.",
                    node_ix, tenant_id
                )
            })
    }

    pub fn make_asymmetric(&mut self, seed: u64) {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(seed);

        let cluster = Arc::get_mut(&mut self.cluster)
            .expect("there should be no other reference to physical cluster");

        for link_ix in cluster.all_links() {
            let new_bw = cluster[link_ix].bandwidth * rng.gen_range(1, 11) / 10;
            cluster[link_ix] = Link::new(new_bw);
            cluster[LinkIx::new(link_ix.index() ^ 1)] = Link::new(new_bw);
        }
    }

    pub fn provision(
        &mut self,
        tenant_id: TenantId,
        nhosts: usize,
        strategy: PlacementStrategy,
    ) -> Result<VirtCluster, Error> {
        if self.used.len() + nhosts > self.cluster.num_hosts() {
            return Err(Error::NoHost(self.cluster.num_hosts(), nhosts));
        }

        let (inner, virt_to_phys) = match strategy {
            PlacementStrategy::Compact => self.place_compact(nhosts),
            PlacementStrategy::Spread => unimplemented!(),
            PlacementStrategy::Random(seed) => self.place_random(nhosts, seed),
        };

        let vcluster = VirtCluster {
            inner,
            virt_to_phys,
            tenant_id,
        };

        let ret = vcluster.clone();

        // update mapping from physical node to virtual node
        vcluster
            .virt_to_phys
            .iter()
            .for_each(|(virt_name, phys_name)| {
                let vnode_ix = vcluster.get_node_index(&virt_name);
                let pnode_ix = self.cluster.get_node_index(&phys_name);
                self.phys_to_virt
                    .insert((pnode_ix, tenant_id), vnode_ix)
                    .unwrap_none();
            });

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
                .unwrap_none();
            self.plink_to_vlinks
                .entry(plink_ix)
                .or_insert_with(Vec::new)
                .push((tenant_id, vlink_ix));
        }

        // update tenant_id -> VirtCluster
        self.vclusters
            .insert(tenant_id, Rc::new(RefCell::new(vcluster)))
            .unwrap_none();

        Ok(ret)
    }

    pub fn destroy(&mut self, tenant_id: TenantId) {
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
            .virt_to_phys
            .iter()
            .for_each(|(_, phys_name)| {
                let pnode_ix = self.cluster.get_node_index(&phys_name);
                self.used.remove(&pnode_ix);
                let flag = self.phys_to_virt.remove(&(pnode_ix, tenant_id)).is_some();
                assert!(flag);
            });
    }

    fn place_random(&mut self, nhosts: usize, seed: u64) -> (Cluster, HashMap<String, String>) {
        use rand::seq::{IteratorRandom, SliceRandom};
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(seed);

        let mut tor_alloc: HashMap<NodeIx, String> = HashMap::default();
        let mut host_alloc: HashMap<NodeIx, String> = HashMap::default();

        let mut inner = Cluster::new();
        let root = "virtual_cloud";
        inner.add_node(Node::new(root, 1, NodeType::Switch));

        let alloced_hosts = {
            let avail_hosts = (0..self.cluster.num_hosts())
                .into_iter()
                .map(|i| self.cluster.get_node_index(&format!("host_{}", i)))
                .filter(|node_ix| !self.used.contains(&node_ix));
            let mut choosed: Vec<NodeIx> = avail_hosts.choose_multiple(&mut rng, nhosts);
            choosed.shuffle(&mut rng);
            choosed
        };

        assert_eq!(alloced_hosts.len(), nhosts);

        for host_ix in alloced_hosts {
            let uplink_ix = self.cluster.get_uplink(host_ix);
            let tor_ix = self.cluster.get_target(uplink_ix);

            #[allow(clippy::map_entry)]
            if !tor_alloc.contains_key(&tor_ix) {
                // new ToR in virtual cluster
                let tor_name = format!("tor_{}", tor_alloc.len());
                inner.add_node(Node::new(&tor_name, 2, NodeType::Switch));
                inner.add_link_by_name(root, &tor_name, 0.gbps());
                tor_alloc.insert(tor_ix, tor_name);
            }

            let host_len = host_alloc.len();
            let host_name = host_alloc
                .entry(host_ix)
                .or_insert(format!("host_{}", host_len));

            // connect the host to the ToR
            inner.add_node(Node::new(host_name, 3, NodeType::Host));
            inner.add_link_by_name(&tor_alloc[&tor_ix], host_name, 0.gbps());
            self.used.insert(host_ix);
        }

        tor_alloc
            .insert(
                self.cluster().get_node_index("cloud"),
                "virtual_cloud".to_owned(),
            )
            .unwrap_none();

        let virt_to_phys = host_alloc
            .into_iter()
            .chain(tor_alloc)
            .map(|(k, v)| (v, self.cluster[k].name.clone()))
            .collect();

        (inner, virt_to_phys)
    }

    fn place_compact(&mut self, nhosts: usize) -> (Cluster, HashMap<String, String>) {
        // Vec<(slots, tor_ix)>
        let root = "virtual_cloud";

        let mut tors: Vec<(usize, NodeIx)> = (0..self.cluster.num_switches() - 1)
            .map(|i| {
                let name = format!("tor_{}", i);
                let tor_ix = self.cluster.get_node_index(&name);
                let cap = self
                    .cluster
                    .get_downlinks(tor_ix)
                    .filter(|&link_ix| {
                        let host_ix = self.cluster.get_target(*link_ix);
                        !self.used.contains(&host_ix)
                    })
                    .count();
                (cap, tor_ix)
            })
            .collect();
        tors.sort_by_key(|x| x.0);

        let mut tor_alloc: HashMap<NodeIx, String> = HashMap::default();
        let mut host_alloc: HashMap<NodeIx, String> = HashMap::default();

        let mut allocated = 0;
        let mut inner = Cluster::new();
        inner.add_node(Node::new(root, 1, NodeType::Switch));

        for (w, tor_ix) in tors.into_iter().rev() {
            let tor_len = tor_alloc.len();
            let tor_name = tor_alloc
                .entry(tor_ix)
                .or_insert(format!("tor_{}", tor_len));

            inner.add_node(Node::new(tor_name, 2, NodeType::Switch));
            inner.add_link_by_name(root, tor_name, 0.gbps());
            let mut to_pick = w.min(nhosts - allocated);

            // traverse the tor and pick up w.min(nhosts) nodes
            for &link_ix in self.cluster.get_downlinks(tor_ix) {
                if to_pick == 0 {
                    break;
                }
                let host_ix = self.cluster.get_target(link_ix);
                if !self.used.contains(&host_ix) {
                    let n = &self.cluster[host_ix];
                    assert_eq!(n.depth, 3); // this doesn't have to be true

                    // allocate a new name, make sure host_id is continuous and start from 0
                    let host_len = host_alloc.len();
                    let host_name = host_alloc
                        .entry(host_ix)
                        .or_insert(format!("host_{}", host_len));

                    inner.add_node(Node::new(host_name, 3, NodeType::Host));
                    inner.add_link_by_name(&tor_alloc[&tor_ix], host_name, 0.gbps());
                    to_pick -= 1;
                    allocated += 1;
                    self.used.insert(host_ix);
                }
            }

            if allocated >= nhosts {
                break;
            }
        }

        tor_alloc
            .insert(
                self.cluster().get_node_index("cloud"),
                "virtual_cloud".to_owned(),
            )
            .unwrap_none();

        let virt_to_phys = host_alloc
            .into_iter()
            .chain(tor_alloc)
            .map(|(k, v)| (v, self.cluster[k].name.clone()))
            .collect();

        (inner, virt_to_phys)
    }
}
