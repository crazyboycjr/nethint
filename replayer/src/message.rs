use serde::{Deserialize, Serialize};
use crate::{Flow, Node};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    /// emit a flow, send by controller, processed by worker
    EmitFlow(Flow),
    /// a flow has completed, send by worker, processed by controller
    FlowComplete(Flow),
    /// send by controller, processed by worker
    AppFinish,
    /// send by worker, processed by controller
    LeaveNode(Node),
    /// send by worker, processed by worker
    Data(Flow),
}