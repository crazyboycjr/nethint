use std::marker::PhantomData;

pub fn group_by_key<T, K, F>(iter: impl Iterator<Item = T>, mut f: F) -> Vec<Vec<T>>
where
    F: FnMut(&T) -> K,
    K: Ord,
{
    use std::collections::BTreeMap;
    let mut groups: BTreeMap<K, Vec<T>> = Default::default();
    for i in iter {
        let key = f(&i);
        groups.entry(key).or_default().push(i);
    }
    groups.into_values().collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TreeIdx(usize);

#[derive(Debug, Clone)]
pub struct Node {
    rank: usize,
    parent: Option<TreeIdx>,
    // children: Vec<TreeIdx>,
    neighbor_edges: Vec<EdgeIdx>, // can potentially contains an edge directed to its parent
}

impl Node {
    #[inline]
    pub fn new(rank: usize) -> Self {
        Node {
            rank,
            parent: None,
            // children: Vec::new(),
            neighbor_edges: Vec::new(),
        }
    }

    #[inline]
    pub fn rank(&self) -> usize {
        self.rank
    }

    #[inline]
    pub fn parent(&self) -> Option<TreeIdx> {
        self.parent
    }

    #[inline]
    pub fn neighbor_edges(&self) -> impl Iterator<Item = &EdgeIdx> {
        self.neighbor_edges.iter()
    }

    // #[inline]
    // pub fn children(&self) -> impl Iterator<Item = &TreeIdx> {
    //     self.children.iter()
    // }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeIdx(usize);

#[derive(Debug, Clone)]
pub struct Edge {
    from: TreeIdx,
    to: TreeIdx,
}

impl Edge {
    #[inline]
    pub fn new(from: TreeIdx, to: TreeIdx) -> Self {
        Edge { from, to }
    }

    #[inline]
    pub fn from(&self) -> TreeIdx {
        self.from
    }

    #[inline]
    pub fn to(&self) -> TreeIdx {
        self.to
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Directed,
    Undirected,
}

#[derive(Debug, Clone)]
pub struct Tree {
    mode: Mode,
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl Tree {
    #[inline]
    pub fn new_directed() -> Tree {
        Tree {
            mode: Mode::Directed,
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    #[inline]
    pub fn new_undirected() -> Tree {
        Tree {
            mode: Mode::Undirected,
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    #[inline]
    pub fn is_directed(&self) -> bool {
        self.mode == Mode::Directed
    }

    #[inline]
    pub fn push(&mut self, n: Node) -> TreeIdx {
        let ix = TreeIdx(self.nodes.len());
        self.nodes.push(n);
        ix
    }

    #[inline]
    pub fn add_edge(&mut self, a: TreeIdx, b: TreeIdx) -> EdgeIdx {
        let ix = EdgeIdx(self.edges.len());
        self.edges.push(Edge::new(a, b));
        ix
    }

    #[inline]
    pub fn connect(&mut self, p: TreeIdx, c: TreeIdx) {
        let e1 = self.add_edge(p, c);
        self[p].neighbor_edges.push(e1);
        self[c].parent.replace(p).ok_or(()).unwrap_err();

        if !self.is_directed() {
            let e2 = self.add_edge(c, p);
            self[c].neighbor_edges.push(e2);
        }

        // self[p].children.push(c);
        // self[c].parent.replace(p).ok_or(()).unwrap_err();
        // self.edges.push(Edge::new(p, c));
        // if !self.is_directed() {
        //     self.edges.push(Edge::new(c, p));
        // }
    }

    #[inline]
    pub fn root(&self) -> TreeIdx {
        assert!(!self.nodes.is_empty());
        TreeIdx(0)
    }

    #[inline]
    pub fn all_nodes(&self) -> impl Iterator<Item = TreeIdx> {
        (0..self.nodes.len()).map(TreeIdx)
    }

    #[inline]
    pub fn all_edges(&self) -> impl Iterator<Item = EdgeIdx> {
        (0..self.edges.len()).map(EdgeIdx)
    }

    #[inline]
    pub fn neighbor_edges(&self, n: TreeIdx) -> impl Iterator<Item = &EdgeIdx> {
        self[n].neighbor_edges()
    }

    #[inline]
    pub fn reverse_edge(&self, e: EdgeIdx) -> EdgeIdx {
        assert_eq!(self.mode, Mode::Undirected);
        EdgeIdx(e.0 ^ 1)
    }

    #[inline]
    pub fn rank(&self, n: TreeIdx) -> usize {
        self[n].rank()
    }

    #[inline]
    pub fn parent(&self, n: TreeIdx) -> Option<TreeIdx> {
        self[n].parent()
    }

    /// Return an iterator of all nodes with an edge starting from `a`.
    ///
    /// - `Directed`: Outgoing edges from `a`.
    /// - `Undirected`: All edges from or to `a`.
    ///
    /// Produces an empty iterator if the node doesn't exist.<br>
    #[inline]
    pub fn children<'a>(
        &'a self,
        n: TreeIdx,
    ) -> NodeIter<'a, impl Iterator<Item = &'a TreeIdx>> {
        NodeIter {
            iter: Some(
                self[n]
                    .neighbor_edges()
                    .filter(move |&&e| Some(self[e].to()) != self[n].parent())
                    .map(move |&e| &self[e].to),
            ),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn siblings<'a>(
        &'a self,
        n: TreeIdx,
    ) -> NodeIter<'a, impl Iterator<Item = &'a TreeIdx>> {
        if let Some(p) = self[n].parent {
            NodeIter {
                iter: Some(self.children(p).filter(move |&&x| x != n)),
                _marker: PhantomData,
            }
        } else {
            NodeIter {
                iter: None,
                _marker: PhantomData,
            }
        }
    }
}

pub struct NodeIter<'a, I> {
    iter: Option<I>,
    _marker: PhantomData<&'a Tree>,
}

impl<'a, I: Iterator<Item = &'a TreeIdx>> Iterator for NodeIter<'a, I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.iter.as_mut() {
            iter.next()
        } else {
            None
        }
    }
}

impl std::ops::Index<TreeIdx> for Tree {
    type Output = Node;
    fn index(&self, index: TreeIdx) -> &Self::Output {
        &self.nodes[index.0]
    }
}

impl std::ops::IndexMut<TreeIdx> for Tree {
    fn index_mut(&mut self, index: TreeIdx) -> &mut Self::Output {
        &mut self.nodes[index.0]
    }
}

impl std::ops::Index<EdgeIdx> for Tree {
    type Output = Edge;
    fn index(&self, index: EdgeIdx) -> &Self::Output {
        &self.edges[index.0]
    }
}
