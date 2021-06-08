use std::time::Duration;
use crate::fs::append_to_file;

#[derive(Debug, Clone, Default)]
pub struct OverheadCollector {
    // controller overhead, job scale
    data: Vec<(Duration, usize)>,
}

impl OverheadCollector {
    pub fn collect(&mut self, duration: Duration, scale: usize) {
        self.data.push((duration, scale));

        if let Ok(path) = std::env::var("NETHINT_COLLECT_CONTROLLER_OVERHEAD") {
            append_to_file(path, &format!("{} {}", scale, duration.as_nanos()));
        }
    }
}
