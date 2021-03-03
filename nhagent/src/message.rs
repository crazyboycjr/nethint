use serde::{Deserialize, Serialize};
use crate::{Node, sampler::CounterUnit};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// send by leader, processed by worker
    AppFinish,
    /// send by worker, processed by leader
    LeaveNode(Node),
    /// send by worker, processed by rack leader
    ServerChunk(Vec<CounterUnit>),
    /// send by rack leader, processed by rack leader
    RackChunk(Vec<CounterUnit>),
}
