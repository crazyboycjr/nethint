use lazy_static::lazy_static;
use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use log::debug;

use crate::bandwidth::Bandwidth;
use crate::LoadBalancer;

pub type NodeRef = Rc<RefCell<Node>>;
pub type LinkRef = Rc<RefCell<Link>>;

lazy_static! {
    static ref NODE_ID: AtomicUsize = AtomicUsize::new(0);
    static ref LINK_ID: AtomicUsize = AtomicUsize::new(0);
}

pub trait Topology {
    fn get_node(&self, name: &str) -> NodeRef;
    fn resolve_route(&self, src: &str, dst: &str, load_balancer: Option<Box<dyn LoadBalancer>>) -> Route;
}

/// The network topology and hardware configuration of the cluster.
#[derive(Debug, Default)]
pub struct Cluster {
    nodes: Vec<NodeRef>,
    links: Vec<LinkRef>,
}

impl Cluster {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn from_nodes(nodes: Vec<NodeRef>) -> Self {
        Cluster {
            nodes,
            links: Vec::new(),
        }
    }

    #[inline]
    pub fn add_node(&mut self, node: NodeRef) {
        self.nodes.push(node);
    }

    #[inline]
    pub fn add_link(&mut self, link: LinkRef) {
        let from_node = link
            .borrow()
            .from
            .upgrade()
            .unwrap_or_else(|| panic!("link: {:?}", link));
        let to_node = link
            .borrow()
            .to
            .upgrade()
            .unwrap_or_else(|| panic!("link: {:?}", link));

        match from_node.borrow().depth.cmp(&to_node.borrow().depth) {
            std::cmp::Ordering::Less => {
                assert!(from_node.borrow().parent.is_none());
                from_node.borrow_mut().parent = Some(Rc::clone(&link));
            }
            std::cmp::Ordering::Greater => from_node.borrow_mut().children.push(Rc::clone(&link)),
            _ => panic!("from_node: {:?}, to_node: {:?}", from_node, to_node),
        }

        self.links.push(link);
    }

    #[inline]
    pub fn add_link_by_name(&mut self, parent: &str, child: &str, bw: Bandwidth) {
        let pnode = self.get_node(parent);
        let cnode = self.get_node(child);

        let l1 = Link::new(Rc::clone(&pnode), Rc::clone(&cnode), bw);
        let l2 = Link::new(Rc::clone(&cnode), Rc::clone(&pnode), bw);

        pnode.borrow_mut().children.push(Rc::clone(&l1));
        assert!(cnode.borrow().parent.is_none());
        cnode.borrow_mut().parent = Some(Rc::clone(&l2));

        self.links.push(l1);
        self.links.push(l2);
    }
}

impl Topology for Cluster {
    fn get_node(&self, name: &str) -> NodeRef {
        Rc::clone(
            self.nodes
                .iter()
                .find(|n| n.borrow().name == name)
                .unwrap_or_else(|| panic!("cannot find node with name: {}", name)),
        )
    }

    fn resolve_route(&self, src: &str, dst: &str, load_balancer: Option<Box<dyn LoadBalancer>>) -> Route {
        let src_node = self.get_node(src);
        let dst_node = self.get_node(dst);

        debug!("searching route from {} to {}", src, dst);
        debug!("src_node: {:?}, dst_node: {:?}", src_node, dst_node);
        assert_eq!(src_node.borrow().depth, dst_node.borrow().depth);
        let mut depth= src_node.borrow().depth;

        let mut path1 = Vec::new();
        let mut path2 = Vec::new();

        let mut x = Rc::clone(&src_node);
        let mut y = Rc::clone(&dst_node);
        while x != y && depth > 1 {
            let parx = Rc::clone(x.borrow().parent.as_ref().unwrap());
            let pary = Rc::clone(y.borrow().parent.as_ref().unwrap());
            x = Weak::upgrade(&parx.borrow().to).unwrap();
            y = Weak::upgrade(&pary.borrow().to).unwrap();
            depth -= 1;
            path1.push(parx);
            path2.push(Rc::clone(&self.links[pary.borrow().id ^ 1]));
        }

        assert!(x == y, format!("route from {} to {} not found", src, dst));
        path2.reverse();
        path1.append(&mut path2);
        let route = Route {
            from: src_node,
            to: dst_node,
            path: path1,
        };

        debug!("find a route from {} to {}, route; {:#?}", src, dst, route);

        assert!(load_balancer.is_none(), "unimplemented");
        route
    }
}

#[derive(Debug, Clone)]
pub struct Link {
    pub(crate) id: usize,
    pub(crate) from: Weak<RefCell<Node>>,
    pub(crate) to: Weak<RefCell<Node>>,
    pub(crate) bandwidth: Bandwidth,
    // latency: Duration,
    // drop_rate: f64,
}

impl Link {
    #[inline]
    pub fn new(from: NodeRef, to: NodeRef, bw: Bandwidth) -> LinkRef {
        Rc::new(RefCell::new(Link {
            id: LINK_ID.fetch_add(1, SeqCst),
            from: Rc::downgrade(&from),
            to: Rc::downgrade(&to),
            bandwidth: bw,
        }))
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

#[derive(Debug, Clone)]
pub struct Node {
    id: usize,
    name: String,
    depth: usize, // 1: core, 2: agg, 3: edge, 4: host
    parent: Option<LinkRef>,
    children: Vec<LinkRef>,
}
impl std::cmp::PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Node {}

impl std::cmp::PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl std::cmp::Ord for Node {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl Node {
    #[inline]
    pub fn new(name: &str, depth: usize) -> NodeRef {
        Rc::new(RefCell::new(Node {
            id: NODE_ID.fetch_add(1, SeqCst),
            name: name.to_string(),
            depth,
            parent: None,
            children: Vec::new(),
        }))
    }

    // fn get_parent_nodes(&self) -> Vec<NodeRef> {
    //     self.parents.iter().map(|l| Weak::upgrade(&l.borrow().to).unwrap()).collect()
    // }
}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
/// A route is a network path that a flow goes through
#[derive(Debug, Clone)]
pub struct Route {
    pub(crate) from: NodeRef,
    pub(crate) to: NodeRef,
    pub(crate) path: Vec<LinkRef>,
}
