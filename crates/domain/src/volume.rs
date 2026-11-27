//! Persistent volume records and mount ownership values.

use serde::{Deserialize, Serialize};

use crate::{impl_str_enum, LayerRef};

/// Whether `value` is a valid volume ID, name, or owner component.
pub fn is_valid_volume_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum VolumeMode {
    ReadOnly,
    #[default]
    Exclusive,
}

impl_str_enum!(VolumeMode, "volume mode" {
    Exclusive => "exclusive",
    ReadOnly => "read_only",
});

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeStatus {
    #[default]
    Ready,
    Uploading,
    Failed,
}

impl_str_enum!(VolumeStatus, "volume status" {
    Ready => "ready",
    Uploading => "uploading",
    Failed => "failed",
});

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeRecord {
    pub id: String,
    pub name: String,
    pub mode: VolumeMode,
    pub size_mb: u64,
    pub status: VolumeStatus,
    pub reserved_by_sandbox_id: Option<String>,
    /// Repository-owned layers materialized lazily on each node.
    pub backing_layers: Vec<LayerRef>,
    pub read_only_mounts: Vec<String>,
}

/// Lease mutation applied atomically to a volume row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum VolumeLeaseOp {
    Acquire { owner: String },
    Transfer { from: String, to: String },
    Release { owner: String },
}
