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
    DataChunk(Flow),
    /// send by worker, processed by worker
    Data(Flow),
    /// send by nhagent global leader, processed by controller
    BrainResponse(nhagent_v2::message::Message),
}
