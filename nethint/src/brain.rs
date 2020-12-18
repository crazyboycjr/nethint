use std::sync::Arc;

use crate::architecture::{build_arbitrary_cluster, build_fatree_fake, TopoArgs};
use crate::bandwidth::BandwidthTrait;
use crate::cluster::{Cluster, Node, NodeIx, NodeType, Topology, VirtCluster};
use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use thiserror::Error;

/// Brain is the cloud controller. It knows the underlay physical
/// topology and decides how to provision VMs for tenants.
#[derive(Debug)]
pub struct Brain {
    /// physical clsuter
    cluster: Arc<Cluster>,
    /// used list
    used: HashSet<NodeIx>,
}

/// Similar to AWS Placement Groups.
#[derive(Debug, Clone, Copy)]
pub enum PlacementStrategy {
    /// Prefer to choose hosts within the same rack.
    Compact,
    /// Prefer to spread instances to different racks.
    Spread,
    /// Random uniformally choose some hosts to allocate.
    Random,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("not enough empty host available, available: {0}, requested: {1}")]
    NoHost(usize, usize),
    #[error("invalid host name: {0}")]
    InvalidHostName(String),
}

impl Brain {
    pub fn build_cloud(topo: TopoArgs) -> Self {
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

        Brain {
            cluster: Arc::new(cluster),
            used: Default::default(),
        }
    }

    pub fn cluster(&self) -> &Arc<Cluster> {
        &self.cluster
    }

    pub fn make_asymmetric(&mut self, seed: u64) {
        use crate::cluster::{Link, LinkIx};
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
        nhosts: usize,
        strategy: PlacementStrategy,
    ) -> Result<VirtCluster, Error> {
        if self.used.len() + nhosts >= self.cluster.num_hosts() {
            return Err(Error::NoHost(self.cluster.num_hosts(), nhosts));
        }

        let (inner, virt_to_phys) = match strategy {
            PlacementStrategy::Compact => self.place_compact(nhosts),
            PlacementStrategy::Spread => unimplemented!(),
            PlacementStrategy::Random => self.place_random(nhosts),
        };

        Ok(VirtCluster {
            inner,
            virt_to_phys,
        })
    }

    fn place_random(&self, nhosts: usize) -> (Cluster, HashMap<String, String>) {
        use crate::RNG;
        use rand::seq::IteratorRandom;

        let mut tor_alloc: HashMap<NodeIx, String> = HashMap::default();
        let mut host_alloc: HashMap<NodeIx, String> = HashMap::default();

        let mut inner = Cluster::new();
        let root = "virtual_cloud";
        inner.add_node(Node::new(root, 1, NodeType::Switch));

        let alloced_hosts = RNG.with(|rng| {
            let mut rng = rng.borrow_mut();
            let avail_hosts = (0..self.cluster.num_hosts())
                .into_iter()
                .map(|i| self.cluster.get_node_index(&format!("host_{}", i)))
                .filter(|node_ix| !self.used.contains(&node_ix));
            avail_hosts.choose_multiple(&mut *rng, nhosts)
        });

        assert_eq!(alloced_hosts.len(), nhosts);

        // let mut selected_tor: HashSet<NodeIx> = HashSet::default();
        for host_ix in alloced_hosts {
            let uplink_ix = self.cluster.get_uplink(host_ix);
            let tor_ix = self.cluster.get_target(uplink_ix);

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
        }

        let virt_to_phys = host_alloc
            .into_iter()
            .chain(tor_alloc)
            .map(|(k, v)| (v, self.cluster[k].name.clone()))
            .collect();

        (inner, virt_to_phys)
    }

    fn place_compact(&self, nhosts: usize) -> (Cluster, HashMap<String, String>) {
        // Vec<(slots, tor_ix)>
        let root = "virtual_cloud";

        let mut tors: Vec<(usize, NodeIx)> = (0..self.cluster.num_switches())
            .map(|i| {
                let name = format!("tor_{}", i);
                let tor_ix = self.cluster.get_node_index(&name);
                let cap = self
                    .cluster
                    .get_downlinks(tor_ix)
                    .filter(|&link_ix| {
                        let host_ix = self.cluster.get_target(*link_ix);
                        self.used.contains(&host_ix)
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
                }
            }

            if allocated >= nhosts {
                break;
            }
        }

        let virt_to_phys = host_alloc
            .into_iter()
            .chain(tor_alloc)
            .map(|(k, v)| (v, self.cluster[k].name.clone()))
            .collect();

        (inner, virt_to_phys)
    }
}
