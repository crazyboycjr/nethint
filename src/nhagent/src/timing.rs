use serde::{Serialize, Deserialize};
use std::time::SystemTime;

pub const ON_COLLECTED: &str = "OnCollected";
pub const ON_SAMPLED: &str = "OnSampled";
pub const ON_CHUNK_SENT: &str = "OnChunkSent";
pub const ON_ALL_RECEIVED: &str = "OnAllReceived";
pub const ON_TENANT_SENT_REQ: &str = "OnTenantSentRequest";
pub const ON_RECV_TENANT_REQ: &str = "OnRecvTenantRequest";
pub const ON_TENANT_RECV_RES: &str = "OnTenantRecvResponse";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRecord {
    pub stage: String,
    pub ts: SystemTime,
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

    pub fn get(&self, stage: &str) -> Option<TimeRecord> {
        self.recs.iter().find(|x| x.stage == stage).cloned()
    }

    pub fn push(&mut self, stage: &str, ts: SystemTime) {
        self.recs.push(TimeRecord::with_ts(stage, ts))
    }

    pub fn push_now(&mut self, stage: &str) {
        self.recs.push(TimeRecord::new(stage));
    }

    pub fn update(&mut self, stage: &str, ts: SystemTime) {
        let e = self.recs.iter_mut().rfind(|x| x.stage == stage);
        if let Some(x) = e {
            x.ts = ts.max(x.ts);
        } else {
            self.recs.push(TimeRecord::with_ts(stage, ts));
        }
    }

    pub fn update_now(&mut self, stage: &str) {
        self.update(stage, SystemTime::now());
    }

    /// sync the latest corresponding element in `self` with `other`,
    /// if not exists, append that element to `self`.
    pub fn update_time_list(&mut self, other: &TimeList) {
        other.recs.iter().for_each(|o| self.update(&o.stage, o.ts));
    }

    pub fn update_min(&mut self, stage: &str, other: &TimeList) {
        if let Some(o) = other.get(stage) {
            if let Some(e) = self.recs.iter_mut().rfind(|x| x.stage == stage) {
                e.ts = o.ts.min(e.ts);
            } else {
                self.recs.push(TimeRecord::with_ts(&o.stage, o.ts));
            }
        }
    }
}

impl std::fmt::Display for TimeList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut rs = self.recs.clone();
        rs.sort_by_key(|x| x.ts);
        if !rs.is_empty() {
            let eariest = rs[0].ts;
            for r in rs {
                writeln!(f, "{} {}", r.stage, r.ts.duration_since(eariest).unwrap().as_micros())?;
            }
        }
        writeln!(f, "\n")
    }
}
