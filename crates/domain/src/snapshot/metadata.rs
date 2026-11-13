use serde::{Deserialize, Serialize};

use super::{SnapshotAlias, SnapshotId, SnapshotPlacement, SnapshotRecord, SnapshotSource};

/// Snapshot identity and lifecycle state shared by the directory and its callers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub id: SnapshotId,
    pub alias: Option<SnapshotAlias>,
    pub snapshot_type: Option<SnapshotPlacement>,
    pub owner_node_id: Option<String>,
    pub source_sandbox_id: Option<String>,
    pub retired: bool,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

impl SnapshotMetadata {
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
        !self.retired && self.snapshot_type.is_some()
    }

    pub fn snapshot_placement(&self) -> anyhow::Result<SnapshotPlacement> {
        self.snapshot_type
            .ok_or_else(|| anyhow::anyhow!("snapshot has no public placement"))
    }

    pub fn validate_for_publication(&self) -> Result<(), String> {
        if self.retired {
            return Err("cannot publish retired snapshot metadata".into());
        }
        match self.snapshot_type {
            Some(SnapshotPlacement::Local) => {
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
            || (self.snapshot_type == Some(SnapshotPlacement::Local)
                && self.source_sandbox_id.is_none())
        {
            return Err("source sandbox ID must be nonempty text without NUL".into());
        }
        Ok(())
    }
}

impl<M> From<&SnapshotRecord<M>> for SnapshotMetadata {
    fn from(record: &SnapshotRecord<M>) -> Self {
        let source_sandbox_id = match &record.config.source {
            SnapshotSource::Sandbox {
                source_sandbox_id, ..
            } => Some(source_sandbox_id.clone()),
            SnapshotSource::Template { .. } => None,
        };
        Self {
            id: record.id.clone(),
            alias: record.alias.clone(),
            snapshot_type: Some(record.snapshot_type),
            owner_node_id: record.owner_node_id.clone(),
            source_sandbox_id,
            retired: record.deleting,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
        }
    }
}
