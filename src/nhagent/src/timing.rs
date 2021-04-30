use serde::{Serialize, Deserialize};
use std::time::SystemTime;

pub const ON_COLLECTED: &str = "OnCollected";
pub const ON_SAMPLED: &str = "OnSampled";
pub const ON_CHUNK_SENT: &str = "OnChunkSent";
pub const ON_ALL_RECEIVED: &str = "OnAllReceived";
pub const ON_TENANT_REQ: &str = "OnTenantRequested";
pub const ON_TENANT_RES: &str = "OnTenantResponsed";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRecord {
    stage: String,
    ts: SystemTime,
}

impl TimeRecord {
    pub fn new(stage: &str) -> Self {
        TimeRecord {
            stage: stage.to_owned(),
            ts: SystemTime::now(),
        }
    }

    pub fn with_ts(stage: &str, ts: SystemTime) -> Self {
        TimeRecord {
            stage: stage.to_owned(),
            ts,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimeList {
    recs: Vec<TimeRecord>,
}

impl TimeList {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn clear(&mut self) {
        self.recs.clear()
    }

    pub fn push(&mut self, stage: &str, ts: SystemTime) {
        self.recs.push(TimeRecord::with_ts(stage, ts))
    }

    pub fn push_now(&mut self, stage: &str) {
        self.recs.push(TimeRecord::with_ts(stage, SystemTime::now()))
    }

    /// sync the latest corresponding element in `self` with `other`,
    /// if not exists, append that element to `self`.
    pub fn update(&mut self, other: &TimeList) {
        for o in &other.recs {
            let e = self.recs.iter_mut().rfind(|x| x.stage == o.stage);
            if let Some(x) = e {
                x.ts = o.ts;
            } else {
                self.recs.push(o.clone());
            }
        }
    }

    // /// similar to update, but remove the element from head
    // pub fn remove(&mut self, other: &TimeList) {
    //     for o in &other.recs {
    //         let pos = self.recs.iter().position(|x| x.stage == o.stage);
    //         assert!(pos.is_some(), "self: {:?}, to remove: {:?}, other: {:?}", self, o, other);
    //         self.recs.remove(pos.unwrap());
    //     }
    // }
}