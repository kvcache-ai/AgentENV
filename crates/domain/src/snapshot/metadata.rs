use serde::{Deserialize, Serialize};

use super::{
    SnapshotAlias, SnapshotId, SnapshotPlacement, SnapshotRecord, SnapshotSource,
    SnapshotSourceKind, SnapshotState, SnapshotVisibility,
};

/// Snapshot identity and lifecycle state shared by the directory and its callers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
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
        self.state == SnapshotState::Active && self.placement.is_some()
    }

    pub fn is_retired(&self) -> bool {
        self.state != SnapshotState::Active
    }

    pub fn snapshot_placement(&self) -> anyhow::Result<SnapshotPlacement> {
        self.placement
            .ok_or_else(|| anyhow::anyhow!("snapshot has no public placement"))
    }

    pub fn validate_for_publication(&self) -> Result<(), String> {
        if self.state != SnapshotState::Active {
            return Err("cannot publish non-active snapshot metadata".into());
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

impl<M> From<&SnapshotRecord<M>> for SnapshotMetadata {
    fn from(record: &SnapshotRecord<M>) -> Self {
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
            state: if record.deleting {
                SnapshotState::Retired
            } else {
                SnapshotState::Active
            },
            placement: Some(record.snapshot_type),
            owner_node_id: record.owner_node_id.clone(),
            source_sandbox_id,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
        }
    }
}
