use agentenv_domain::{
    SnapshotAlias, SnapshotConfig, SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotRecord,
    SnapshotSource,
};
use serde::{Deserialize, Serialize};

use crate::{MetaError, MetaResult};

/// Directory state and captured configuration; published artifacts are read from the repository.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub(crate) snapshot_id: SnapshotId,
    pub(crate) alias: Option<SnapshotAlias>,
    pub(crate) snapshot_type: Option<SnapshotPlacement>,
    pub(crate) owner_node_id: Option<String>,
    pub(crate) config: Option<SnapshotConfig>,
    pub(crate) retired: bool,
    pub(crate) created_at: i64,
    pub(crate) updated_at: i64,
}

impl SnapshotEntry {
    pub(crate) fn from_record(record: &SnapshotRecord) -> Self {
        Self {
            snapshot_id: record.id.clone(),
            alias: record.alias.clone(),
            snapshot_type: Some(record.snapshot_type),
            owner_node_id: record.owner_node_id.clone(),
            config: Some(record.config.clone()),
            retired: false,
            created_at: record.created_at_unix_ms,
            updated_at: record.updated_at_unix_ms,
        }
    }

    pub fn id(&self) -> &SnapshotId {
        &self.snapshot_id
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_ref().map(AsRef::as_ref)
    }

    pub fn is_visible(&self) -> bool {
        !self.retired && self.snapshot_type.is_some()
    }

    pub fn snapshot_placement(&self) -> MetaResult<SnapshotPlacement> {
        self.snapshot_type
            .ok_or_else(|| unavailable("snapshot has no public placement"))
    }

    pub fn source(&self) -> MetaResult<SnapshotSource> {
        self.config
            .as_ref()
            .map(|config| config.source.clone())
            .ok_or_else(|| unavailable("snapshot has no public metadata"))
    }

    /// Local records have captured configuration but no published manifest.
    pub fn into_local_record(self) -> MetaResult<SnapshotRecord> {
        if !self.is_visible() || self.snapshot_placement()? != SnapshotPlacement::Local {
            return Err(unavailable("snapshot is not an available Local snapshot"));
        }
        self.assemble_record(None)
    }

    /// Assemble a Distributed record with the caller's published artifact manifest.
    pub fn into_record(self, manifest: SnapshotManifest) -> MetaResult<SnapshotRecord> {
        if !self.is_visible() || self.snapshot_placement()? != SnapshotPlacement::Distributed {
            return Err(unavailable(
                "snapshot is not an available Distributed snapshot",
            ));
        }
        self.assemble_record(Some(manifest))
    }

    fn assemble_record(self, manifest: Option<SnapshotManifest>) -> MetaResult<SnapshotRecord> {
        let snapshot_type = self.snapshot_placement()?;
        Ok(SnapshotRecord {
            id: self.snapshot_id,
            snapshot_type,
            owner_node_id: self.owner_node_id,
            alias: self.alias,
            config: self
                .config
                .ok_or_else(|| unavailable("snapshot has no public metadata"))?,
            created_at_unix_ms: self.created_at,
            updated_at_unix_ms: self.updated_at,
            manifest,
            deleting: false,
        })
    }
}

fn unavailable(reason: impl Into<String>) -> MetaError {
    MetaError::Unavailable {
        reason: reason.into(),
    }
}
