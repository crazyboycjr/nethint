use fnv::FnvHashMap as HashMap;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use lazy_static::lazy_static;
use log::debug;
use petgraph::{
    dot::Dot,
    graph::{EdgeIndex, EdgeIndices, Graph, NodeIndex},
};

use crate::bandwidth::Bandwidth;
use crate::LoadBalancer;

lazy_static! {
    static ref LINK_ID: AtomicUsize = AtomicUsize::new(0);
}

pub type LinkIx = EdgeIndex;
pub type NodeIx = NodeIndex;
pub type LinkIxIter = EdgeIndices;

use std::ops::{Index, IndexMut};

pub trait Topology:
    Index<NodeIx, Output = Node> + Index<LinkIx, Output = Link> + IndexMut<LinkIx>
{
    fn get_node_index(&self, name: &str) -> NodeIx;
    fn get_target(&self, ix: LinkIx) -> NodeIx;
    fn get_uplink(&self, ix: NodeIx) -> LinkIx;
    fn get_downlinks(&self, ix: NodeIx) -> std::slice::Iter<LinkIx>;
    fn all_links(&self) -> EdgeIndices;
    fn resolve_route(
        &self,
        src: &str,
        dst: &str,
        load_balancer: Option<Box<dyn LoadBalancer>>,
    ) -> Route;
    fn num_hosts(&self) -> usize;
    fn num_switches(&self) -> usize;
}

/// A VirtCluster is a subgraph of the original physical cluster.
/// It works just as a Cluster.
/// Ideally, it should be able to translate the virtual node to a physical node in the physical cluster.
/// In our implementation, we just maintain the mapping of node name for the translation.
#[derive(Debug, Clone)]
pub struct VirtCluster {
    pub(crate) inner: Cluster,
    pub(crate) virt_to_phys: HashMap<String, String>,
}

impl Index<LinkIx> for VirtCluster {
    type Output = Link;
    fn index(&self, index: LinkIx) -> &Self::Output {
        &self.inner[index]
    }
}

impl Index<NodeIx> for VirtCluster {
    type Output = Node;
    fn index(&self, index: NodeIx) -> &Self::Output {
        &self.inner[index]
    }
}

impl IndexMut<LinkIx> for VirtCluster {
    fn index_mut(&mut self, index: LinkIx) -> &mut Self::Output {
        &mut self.inner[index]
    }
}

impl Topology for VirtCluster {
    #[inline]
    fn get_node_index(&self, name: &str) -> NodeIx {
        self.inner.get_node_index(name)
    }

    #[inline]
    fn get_target(&self, ix: LinkIx) -> NodeIx {
        self.inner.get_target(ix)
    }

    #[inline]
    fn get_uplink(&self, ix: NodeIx) -> LinkIx {
        self.inner.get_uplink(ix)
    }

    #[inline]
    fn get_downlinks(&self, ix: NodeIx) -> std::slice::Iter<LinkIx> {
        self.inner.get_downlinks(ix)
    }

    fn all_links(&self) -> EdgeIndices {
        self.inner.all_links()
    }

    fn resolve_route(
        &self,
        src: &str,
        dst: &str,
        load_balancer: Option<Box<dyn LoadBalancer>>,
    ) -> Route {
        self.inner.resolve_route(src, dst, load_balancer)
    }

    #[inline]
    fn num_hosts(&self) -> usize {
        self.inner.num_hosts()
    }

    #[inline]
    fn num_switches(&self) -> usize {
        self.inner.num_switches()
    }
}

/// The network topology and hardware configuration of the cluster.
#[derive(Debug, Default, Clone)]
pub struct Cluster {
    graph: Graph<Node, Link>,
    node_map: HashMap<String, NodeIndex>,
    num_hosts: usize,
}

impl Cluster {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn from_nodes(nodes: Vec<Node>) -> Self {
        let mut g = Graph::new();
        let mut node_map = HashMap::default();
        let num_hosts = nodes.iter().filter(|n| n.is_host()).count();
        nodes.into_iter().for_each(|n| {
            let name = n.name.clone();
            let node_idx = g.add_node(n);
            let old = node_map.insert(name.clone(), node_idx);
            assert!(old.is_none(), format!("repeated key: {}", name));
        });
        Cluster {
            graph: g,
            node_map,
            num_hosts,
        }
    }

    #[inline]
    pub fn add_node(&mut self, node: Node) -> NodeIndex {
        if node.is_host() {
            self.num_hosts += 1;
        }
        let name = node.name.clone();
        let node_idx = self.graph.add_node(node);
        let old = self.node_map.insert(name.clone(), node_idx);
        assert!(old.is_none(), format!("repeated key: {}", name));
        node_idx
    }

    #[inline]
    pub fn add_link_by_name(&mut self, parent: &str, child: &str, bw: Bandwidth) {
        let &pnode = self
            .node_map
            .get(parent)
            .unwrap_or_else(|| panic!("cannot find node with name: {}", parent));
        let &cnode = self
            .node_map
            .get(child)
            .unwrap_or_else(|| panic!("cannot find node with name: {}", child));

        let l1 = self.graph.add_edge(pnode, cnode, Link::new(bw));
        let l2 = self.graph.add_edge(cnode, pnode, Link::new(bw));

        self.graph[pnode].children.push(l1);
        assert!(self.graph[cnode].parent.is_none());
        self.graph[cnode].parent = Some(l2);
    }

    pub fn to_dot(&self) -> Dot<&Graph<Node, Link>> {
        Dot::with_config(&self.graph, &[])
    }
}

impl std::ops::Index<NodeIx> for Cluster {
    type Output = Node;
    fn index(&self, index: NodeIx) -> &Self::Output {
        &self.graph[index]
    }
}

impl std::ops::Index<LinkIx> for Cluster {
    type Output = Link;
    fn index(&self, index: LinkIx) -> &Self::Output {
        &self.graph[index]
    }
}

impl std::ops::IndexMut<LinkIx> for Cluster {
    fn index_mut(&mut self, index: LinkIx) -> &mut Self::Output {
        &mut self.graph[index]
    }
}

impl Topology for Cluster {
    #[inline]
    fn get_node_index(&self, name: &str) -> NodeIx {
        let &id = self
            .node_map
            .get(name)
            .unwrap_or_else(|| panic!("cannot find node with name: {}", name));
        id
    }

    #[inline]
    fn get_target(&self, ix: LinkIx) -> NodeIx {
        self.graph.raw_edges()[ix.index()].target()
    }

    #[inline]
    fn get_uplink(&self, ix: NodeIx) -> LinkIx {
        self.graph[ix]
            .parent
            .unwrap_or_else(|| panic!("invalid index: {:?}", ix))
    }

    #[inline]
    fn get_downlinks(&self, ix: NodeIx) -> std::slice::Iter<LinkIx> {
        self.graph[ix].children.iter()
    }

    fn all_links(&self) -> EdgeIndices {
        self.graph.edge_indices()
    }

    fn resolve_route(
        &self,
        src: &str,
        dst: &str,
        load_balancer: Option<Box<dyn LoadBalancer>>,
    ) -> Route {
        let g = &self.graph;
        let src_id = self.node_map[src];
        let dst_id = self.node_map[dst];

        debug!("searching route from {} to {}", src, dst);
        debug!("src_node: {:?}, dst_node: {:?}", g[src_id], g[dst_id]);
        assert_eq!(g[src_id].depth, g[dst_id].depth);
        let mut depth = g[src_id].depth;

        let mut path1 = Vec::new();
        let mut path2 = Vec::new();

        let mut x = self.node_map[src];
        let mut y = self.node_map[dst];
        while x != y && depth > 1 {
            let parx = g[x].parent.unwrap();
            let pary = g[y].parent.unwrap();
            x = g.raw_edges()[parx.index()].target();
            y = g.raw_edges()[pary.index()].target();
            depth -= 1;
            path1.push(g[parx].clone());
            path2.push(g[EdgeIndex::new(pary.index() ^ 1)].clone());
        }

        assert!(x == y, format!("route from {} to {} not found", src, dst));
        path2.reverse();
        path1.append(&mut path2);
        let route = Route {
            from: src_id,
            to: dst_id,
            path: path1,
        };

        debug!("find a route from {} to {}, route; {:#?}", src, dst, route);

        assert!(load_balancer.is_none(), "unimplemented");
        route
    }

    #[inline]
    fn num_hosts(&self) -> usize {
        self.num_hosts
    }

    #[inline]
    fn num_switches(&self) -> usize {
        self.graph.node_count() - self.num_hosts
    }
}

#[derive(Debug, Clone)]
pub struct Link {
    id: usize,
    pub bandwidth: Bandwidth,
    // latency: Duration,
    // drop_rate: f64,
}

impl Link {
    #[inline]
    pub fn new(bw: Bandwidth) -> Link {
        Link {
            id: LINK_ID.fetch_add(1, SeqCst),
            bandwidth: bw,
        }
    }
}

impl std::fmt::Display for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.bandwidth)
    }
}

impl std::cmp::PartialEq for Link {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Link {}

impl std::hash::Hash for Link {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NodeType {
    Host,
    Switch,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub name: String,
    pub depth: usize, // 1: core, 2: agg, 3: edge, 4: host
    pub node_type: NodeType,
    // this gives us faster route search
    parent: Option<EdgeIndex>,
    children: Vec<EdgeIndex>,
}

impl Node {
    #[inline]
    pub fn new(name: &str, depth: usize, node_type: NodeType) -> Node {
        Node {
            name: name.to_owned(),
            depth,
            node_type,
            parent: None,
            children: Vec::new(),
        }
    }

    #[inline]
    pub fn is_host(&self) -> bool {
        matches!(self.node_type, NodeType::Host)
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// A route is a network path that a flow goes through
#[derive(Debug, Clone)]
pub struct Route {
    pub(crate) from: NodeIndex,
    pub(crate) to: NodeIndex,
    pub(crate) path: Vec<Link>,
}
