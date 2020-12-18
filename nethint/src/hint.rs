use crate::cluster::VirtCluster;

/// Only topology information, all bandwidth are set to 0.gbps()
pub type NetHintV1 = VirtCluster;

/// With network metrics, either collected by measurement or telemetry.
/// Currently, we are using bandwidth as the level 2 hint.
pub type NetHintV2 = VirtCluster;
