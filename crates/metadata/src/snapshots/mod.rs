//! Snapshot directory entry types, queries, and GC pin coordination.

mod pins;
mod queries;

pub(crate) use queries::tombstone_sql;

use std::fmt;

use agentenv_domain::{
    SnapshotAlias, SnapshotConfig, SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotRecord,
    SnapshotSource, SnapshotSourceKind,
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
        let source_sandbox_id = match &record.config.source {
            SnapshotSource::Sandbox {
                source_sandbox_id, ..
            } => Some(source_sandbox_id.clone()),
            SnapshotSource::Template | SnapshotSource::Builder => None,
        };
        Self {
            id: record.id.clone(),
            source_kind: record.config.source.source_kind(),
            alias: record.alias.clone(),
            state: SnapshotState::Active,
            placement: Some(record.snapshot_type),
            owner_node_id: record.owner_node_id.clone(),
            source_sandbox_id,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.state == SnapshotState::Active && self.placement.is_some()
    }

    pub fn snapshot_placement(&self) -> MetaResult<SnapshotPlacement> {
        self.placement
            .ok_or_else(|| MetaError::unavailable("snapshot has no public placement"))
    }

    /// Apply authoritative directory state to a snapshot's stored content.
    pub fn into_record(
        self,
        config: SnapshotConfig,
        manifest: Option<SnapshotManifest>,
    ) -> MetaResult<SnapshotRecord> {
        if !self.is_visible() {
            return Err(MetaError::unavailable(format!(
                "snapshot '{}' is {}",
                self.id, self.state
            )));
        }
        let snapshot_type = self.snapshot_placement()?;
        // A promotion can prepare its published inventory before the SQL commit.
        let manifest = match snapshot_type {
            SnapshotPlacement::Local => None,
            SnapshotPlacement::Distributed => Some(manifest.ok_or_else(|| {
                MetaError::unavailable(format!("published snapshot '{}' has no manifest", self.id))
            })?),
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
}
