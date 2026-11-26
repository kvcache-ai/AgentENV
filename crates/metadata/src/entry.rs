//! Authoritative SQL snapshot directory entry and lifecycle state.

use std::fmt;

use agentenv_domain::{
    SnapshotAlias, SnapshotConfig, SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotRecord,
    SnapshotSource, SnapshotSourceKind, SnapshotVisibility,
};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::{MetaError, MetaResult};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotState {
    #[default]
    Active,
    Retired,
    Reaped,
    Reserved,
}

impl SnapshotState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Retired => "retired",
            Self::Reaped => "reaped",
            Self::Reserved => "reserved",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "retired" => Ok(Self::Retired),
            "reaped" => Ok(Self::Reaped),
            "reserved" => Ok(Self::Reserved),
            other => bail!("unknown snapshot state '{other}'"),
        }
    }
}

impl fmt::Display for SnapshotState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Directory state; complete snapshot configuration and manifest live in the artifact repository.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub id: SnapshotId,
    pub source_kind: SnapshotSourceKind,
    pub visibility: SnapshotVisibility,
    pub alias: Option<SnapshotAlias>,
    pub state: SnapshotState,
    pub placement: Option<SnapshotPlacement>,
    pub owner_node_id: Option<String>,
    pub source_sandbox_id: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

impl SnapshotEntry {
    pub fn from_record<M>(record: &SnapshotRecord<M>) -> Self {
        let source_kind = record.config.source.source_kind();
        let visibility = record.config.source.visibility();
        let source_sandbox_id = match &record.config.source {
            SnapshotSource::Sandbox {
                source_sandbox_id, ..
            } => Some(source_sandbox_id.clone()),
            SnapshotSource::Template | SnapshotSource::Builder => None,
        };
        Self {
            id: record.id.clone(),
            source_kind,
            visibility,
            alias: record.alias.clone(),
            state: SnapshotState::Active,
            placement: Some(record.snapshot_type),
            owner_node_id: record.owner_node_id.clone(),
            source_sandbox_id,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
        }
    }

    pub fn id(&self) -> &SnapshotId {
        &self.id
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_ref().map(AsRef::as_ref)
    }

    pub fn owner_node_id(&self) -> Option<&str> {
        self.owner_node_id.as_deref()
    }

    pub fn source_sandbox_id(&self) -> Option<&str> {
        self.source_sandbox_id.as_deref()
    }

    pub fn is_visible(&self) -> bool {
        self.state == SnapshotState::Active && self.placement.is_some()
    }

    pub fn snapshot_placement(&self) -> MetaResult<SnapshotPlacement> {
        self.placement.ok_or_else(|| MetaError::Unavailable {
            reason: "snapshot has no public placement".into(),
        })
    }

    /// Apply authoritative directory state to a snapshot's stored content.
    pub fn into_record(
        self,
        config: SnapshotConfig,
        manifest: Option<SnapshotManifest>,
    ) -> MetaResult<SnapshotRecord> {
        if !self.is_visible() {
            return Err(MetaError::Unavailable {
                reason: format!("snapshot '{}' is {}", self.id, self.state),
            });
        }
        let snapshot_type = self.snapshot_placement()?;
        // A promotion can prepare its published inventory before the SQL commit.
        let manifest = match snapshot_type {
            SnapshotPlacement::Local => None,
            SnapshotPlacement::Distributed => {
                Some(manifest.ok_or_else(|| MetaError::Unavailable {
                    reason: format!("published snapshot '{}' has no manifest", self.id),
                })?)
            }
        };
        Ok(SnapshotRecord {
            id: self.id,
            snapshot_type,
            owner_node_id: self.owner_node_id,
            alias: self.alias,
            config,
            created_at_unix_ms: self.created_at_unix_ms,
            updated_at_unix_ms: self.updated_at_unix_ms,
            manifest,
        })
    }

    pub(crate) fn validate_for_publication(&self) -> Result<(), String> {
        if self.state != SnapshotState::Active {
            return Err("cannot publish non-active snapshot entry".into());
        }
        match (self.source_kind, self.visibility) {
            (SnapshotSourceKind::Builder, SnapshotVisibility::Internal)
            | (
                SnapshotSourceKind::Sandbox | SnapshotSourceKind::Template,
                SnapshotVisibility::Public,
            ) => {}
            _ => {
                return Err("snapshot source kind and visibility do not match".into());
            }
        }
        match self.placement {
            Some(SnapshotPlacement::Local) => {
                if self.source_kind != SnapshotSourceKind::Sandbox {
                    return Err("Local snapshots must originate from a sandbox".into());
                }
                if self.alias.is_some() {
                    return Err("Local snapshots do not support aliases".into());
                }
                if self
                    .owner_node_id
                    .as_deref()
                    .is_none_or(|owner| owner.trim().is_empty())
                {
                    return Err("Local snapshots require owner_node_id".into());
                }
            }
            Some(SnapshotPlacement::Distributed) => {
                if self.owner_node_id.is_some() {
                    return Err("Distributed snapshots must not carry owner_node_id".into());
                }
            }
            None => return Err("publication requires snapshot placement".into()),
        }
        if let Some(alias) = &self.alias {
            SnapshotAlias::parse(alias.as_ref()).map_err(|error| error.to_string())?;
        }
        if self
            .source_sandbox_id
            .as_deref()
            .is_some_and(|source| source.is_empty() || source.contains('\0'))
            || (self.source_kind == SnapshotSourceKind::Sandbox && self.source_sandbox_id.is_none())
            || (self.source_kind != SnapshotSourceKind::Sandbox && self.source_sandbox_id.is_some())
        {
            return Err(
                "source sandbox ID must be nonempty text without NUL for sandbox snapshots only"
                    .into(),
            );
        }
        Ok(())
    }
}
