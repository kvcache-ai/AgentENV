/*
 * Firecracker API
 *
 * Generated-model-compatible fallback. Regenerate with `cargo adev codegen firecracker` when the pinned OpenAPI generator is available.
 */

use crate::models;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreFaultMemoryStats {
    #[serde(rename = "range_count")]
    pub range_count: i64,
    #[serde(rename = "requested_bytes")]
    pub requested_bytes: i64,
    #[serde(rename = "completed_bytes")]
    pub completed_bytes: i64,
    #[serde(rename = "remaining_bytes")]
    pub remaining_bytes: i64,
    #[serde(rename = "ioctl_count")]
    pub ioctl_count: i64,
    #[serde(rename = "wall_time_us")]
    pub wall_time_us: i64,
    #[serde(rename = "workers")]
    pub workers: Vec<models::PreFaultMemoryWorkerStats>,
}
