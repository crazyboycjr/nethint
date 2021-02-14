use serde::{Deserialize, Serialize};
use crate::{Node, Flow};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    /// (node, hostname), send by worker, processed by controller
    AddNode(Node, String),
    /// send by worker, processed by worker
    AddNodePeer(Node),
    /// send by controller, processed by worker
    BroadcastNodes(Vec<Node>),
    /// emit a flow, send by controller, processed by worker
    EmitFlow(Flow),
    /// a flow has completed, send by worker, processed by controller
    FlowComplete(Flow),
    /// send by controller, processed by worker
    AppFinish,
    /// send by worker, processed by controller
    LeaveNode(Node),
    /// send by worker, processed by worker
    Data(Flow, Vec<u8>)
}
