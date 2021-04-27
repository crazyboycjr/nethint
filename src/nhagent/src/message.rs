use crate::sampler::EthAddr;

use nethint::{TenantId, hint::{NetHintV1Real, NetHintV2Real, NetHintVersion}};
use nethint::counterunit::CounterUnit;
use nethint::cluster::LinkIx;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;

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

    /// send by worker, procesed by worker
    /// declare the table to map ethaddr to hostname collected locally
    DeclareIpHostTable(HashMap<IpAddr, String>),

    /// send by worker, processed by worker
    DeclareHostname(String),

    /// send by worker, processed by rack leader
    ServerChunk(Vec<CounterUnit>),
    /// A potential problem here is that LinkIx from different machines may not be compatible
    /// send by rack leader, processed by rack leader
    RackChunk(HashMap<LinkIx, Vec<CounterUnit>>),
    /// send by rack leader, processed by worker
    AllHints(HashMap<LinkIx, Vec<CounterUnit>>),

    /// send by experiment scheduler, processed by rack leader, 
    /// forward by rack leader, processed by global leader
    /// in practice, we skip the forwarding pass
    /// tenant_id, nhosts, allow_delay
    ProvisionRequest(TenantId, usize, bool),
    /// send by global leader, processed by rack leader
    /// forward by rack leader, processed by experiment scheduler
    /// in practice, we skip the forwarding pass
    /// tenant_id, hintv1
    ProvisionResponse(TenantId, NetHintV1Real),
    /// send by app, processed by global leader
    DestroyRequest(TenantId),
    /// send by global leader, processed by app
    DestroyResponse(TenantId),
    /// send by app, processed by rack/global leader leader
    NetHintRequest(TenantId, NetHintVersion),
    /// send by rack/global leader, processed by app
    NetHintResponseV1(TenantId, NetHintV1Real),
    /// send by rack/global leader, processed by app
    NetHintResponseV2(TenantId, NetHintV2Real),
    /// send by global leader, processed by all
    UpdateRateLimit(usize),
}
