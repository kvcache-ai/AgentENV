//! Persistent volume records, build-cache purpose states, and mount ownership values.

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

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
    #[serde(skip)]
    pub backing_image_config: Option<PathBuf>,
    /// Repository-owned layers; the image config path is only a node-local cache.
    pub backing_layers: Vec<LayerRef>,
    pub read_only_mounts: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deleting: bool,
}

impl VolumeRecord {
    pub fn mounted_by(&self, owner: &str) -> bool {
        self.reserved_by_sandbox_id.as_deref() == Some(owner)
            || self.read_only_mounts.iter().any(|entry| entry == owner)
    }

    pub fn replace_owner(&mut self, from: &str, to: Option<&str>) -> bool {
        let mut changed = false;
        if self.reserved_by_sandbox_id.as_deref() == Some(from) {
            self.reserved_by_sandbox_id = to.map(str::to_owned);
            changed = true;
        }
        if self.read_only_mounts.iter().any(|owner| owner == from) {
            self.read_only_mounts.retain(|owner| owner != from);
            if let Some(to) = to {
                if !self.read_only_mounts.iter().any(|owner| owner == to) {
                    self.read_only_mounts.push(to.to_owned());
                }
            }
            changed = true;
        }
        changed
    }

    pub fn validate_catalog_update(&self, next: &Self) -> Result<(), String> {
        if self.deleting {
            return Err(format!("volume '{}' is being deleted", self.id));
        }
        if self.name != next.name || self.mode != next.mode || self.size_mb != next.size_mb {
            return Err("volume identity fields cannot be changed".to_owned());
        }
        if self.backing_layers != next.backing_layers && self.status != VolumeStatus::Uploading {
            return Err(format!(
                "volume '{}' must enter uploading state before replacing its backing",
                self.id
            ));
        }
        if self.status == VolumeStatus::Failed && next.status == VolumeStatus::Ready {
            return Err(format!(
                "volume '{}' must enter uploading state before becoming ready",
                self.id
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct VolumeRecordPage {
    pub records: Vec<VolumeRecord>,
    /// The last returned volume ID for keyset pagination.
    pub next_volume_id: Option<String>,
}

/// Active build-cache seed and its retired seeds awaiting cleanup.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildCacheState {
    pub current: Option<String>,
    pub retired: BTreeSet<String>,
}
