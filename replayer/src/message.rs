use serde::{Deserialize, Serialize};
use crate::{Node, Flow};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    /// send by worker, processed by controller
    AddNode(Node),
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
    Data(Vec<u8>)
}
