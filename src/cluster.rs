use lazy_static::lazy_static;
use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use log::{debug};

use crate::bandwidth::Bandwidth;

pub type NodeRef = Rc<RefCell<Node>>;
pub type LinkRef = Rc<RefCell<Link>>;

lazy_static! {
    static ref NODE_ID: AtomicUsize = AtomicUsize::new(0);
    static ref LINK_ID: AtomicUsize = AtomicUsize::new(0);
}

pub trait Topology {
    fn get_node(&self, name: &str) -> NodeRef;
    fn resolve_route(&self, src: &str, dst: &str) -> Route;
}

/// The network topology and hardware configuration of the cluster.
pub struct Cluster {
    nodes: Vec<NodeRef>,
    links: Vec<LinkRef>,
}

impl Cluster {
    pub fn from_nodes(nodes: Vec<NodeRef>) -> Self
    {
        Cluster {
            nodes,
            links: Vec::new(),
        }
    }

    #[inline]
    pub fn add_edge(&mut self, src: &str, dst: &str, bw: Bandwidth) {
        let src_node = self.get_node(src);
        let dst_node = self.get_node(dst);

        let l1 = Link::new(Rc::clone(&src_node), Rc::clone(&dst_node), bw);
        let l2 = Link::new(Rc::clone(&dst_node), Rc::clone(&src_node), bw);

        src_node.borrow_mut().links.push(Rc::clone(&l1));
        dst_node.borrow_mut().links.push(Rc::clone(&l2));

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
                .unwrap_or_else(|| panic!("cannot find node with name: {}", name))
        )
    }

    fn resolve_route(&self, src: &str, dst: &str) -> Route {
        // it would be better to find the least common ancestor, and connect the two paths
        let src_node = self.get_node(src);
        let dst_node = self.get_node(dst);

        let mut vis = vec![false; self.nodes.len()];
        fn dfs(x: NodeRef, goal: NodeRef, vis: &mut Vec<bool>) -> Option<Vec<LinkRef>> {
            let x = x.borrow();
            if vis[x.id] {
                return None;
            }
            if x.id == goal.borrow().id {
                return Some(vec![]);
            }
            vis[x.id] = true;
            for l in &x.links {
                let y = Weak::upgrade(&l.borrow().to).unwrap();
                match dfs(y, Rc::clone(&goal), vis) {
                    None => continue,
                    Some(mut path) => {
                        path.push(Rc::clone(l));
                        return Some(path);
                    }
                }
            }
            None
        }

        debug!("searching route from {} to {}", src, dst);
        debug!("src_node: {:?}, dst_node: {:?}", src_node, dst_node);
        let path = dfs(Rc::clone(&src_node), Rc::clone(&dst_node), &mut vis);
        match path {
            None => panic!("route from {} to {} not found", src, dst),
            Some(mut path) => {
                path.reverse();
                let route = Route {
                    from: src_node,
                    to: dst_node,
                    path,
                };
                debug!("find a route from {} to {}\n{:#?}", src, dst, route);
                route
            }
        }
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
    links: Vec<LinkRef>,
}

impl Node {
    #[inline]
    pub fn new(name: &str) -> NodeRef {
        Rc::new(RefCell::new(Node {
            id: NODE_ID.fetch_add(1, SeqCst),
            name: name.to_string(),
            links: Vec::new(),
        }))
    }
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
