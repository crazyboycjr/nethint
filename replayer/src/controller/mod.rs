use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProbeConfig {
    pub(crate) enable: bool,
    #[serde(default)]
    pub(crate) round_ms: u64,
}

pub mod app;
pub mod mapreduce;
pub mod allreduce;
