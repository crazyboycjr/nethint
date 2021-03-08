use crate::{sampler::CounterUnit, sampler::EthAddr};
use nethint::cluster::SLinkIx;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    // do not handle these messages for now
    // /// send by worker, processed by leader
    // LeaveNode(Node),
    /// send by leader, processed by worker
    AppFinish,

    /// send by non global leader, processed by global leader
    /// barrier ID
    SyncRequest(u64),

    /// send by global leader, processed by non global leader
    /// barrier ID
    SyncResponse(u64),

    /// send by worker, procesed by worker
    /// declare the table to map ethaddr to hostname collected locally
    DeclareEthHostTable(HashMap<EthAddr, String>),

    /// send by worker, processed by worker
    DeclareHostname(String),

    /// send by worker, processed by rack leader
    ServerChunk(Vec<CounterUnit>),
    /// A potential problem here is that SLinkIx from different machines may not be compatible
    /// send by rack leader, processed by rack leader
    RackChunk(HashMap<SLinkIx, Vec<CounterUnit>>),
    /// send by rack leader, processed by worker
    AllHints(HashMap<SLinkIx, Vec<CounterUnit>>),
}