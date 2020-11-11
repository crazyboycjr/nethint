use serde::{Deserialize, Serialize};
use crate::Node;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// (node, hostname), send by worker, processed by controller
    AddNode(Node, String),
    /// send by worker, processed by worker
    AddNodePeer(Node),
    /// send by controller, processed by worker
    BroadcastNodes(Vec<Node>),
    /// send by worker, processed by controller
    LeaveNode(Node),
}
