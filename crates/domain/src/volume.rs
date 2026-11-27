//! Persistent volume records, build-cache purpose states, and mount ownership values.

use std::fmt;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::{LayerRef, VolumeMode};

/// Whether `value` is a valid volume ID, name, or owner component.
pub fn is_valid_volume_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// Distinguishes public user volumes from internal BuildKit cache volumes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumePurpose {
    #[default]
    User,
    BuildCacheWork,
    BuildCacheSeed,
    BuildCacheRetired,
}

impl VolumePurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::BuildCacheWork => "build_cache_work",
            Self::BuildCacheSeed => "build_cache_seed",
            Self::BuildCacheRetired => "build_cache_retired",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "user" => Ok(Self::User),
            "build_cache_work" => Ok(Self::BuildCacheWork),
            "build_cache_seed" => Ok(Self::BuildCacheSeed),
            "build_cache_retired" => Ok(Self::BuildCacheRetired),
            other => bail!("unknown volume purpose '{other}'"),
        }
    }
}

impl fmt::Display for VolumePurpose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeStatus {
    #[default]
    Ready,
    Uploading,
    Failed,
}

impl VolumeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Uploading => "uploading",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "ready" => Ok(Self::Ready),
            "uploading" => Ok(Self::Uploading),
            "failed" => Ok(Self::Failed),
            other => bail!("unknown volume status '{other}'"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeRecord {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub purpose: VolumePurpose,
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
