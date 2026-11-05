use agentenv_domain::{
    CommittedSnapshot, SandboxResources, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotPlacement, SnapshotRecord, SnapshotSource,
};
use serde::{Deserialize, Serialize};

use crate::{MetaError, MetaResult};

/// Directory metadata; the caller reads Distributed content from its repository.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub(crate) snapshot_id: SnapshotId,
    pub(crate) alias: Option<SnapshotAlias>,
    pub(crate) snapshot_type: Option<SnapshotPlacement>,
    pub(crate) committed_digest: Option<String>,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) retired: bool,
    pub(crate) created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub(crate) source: SnapshotSource,
    pub(crate) resources: SandboxResources,
    pub(crate) updated_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) owner_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) local_committed: Option<CommittedSnapshot>,
}

impl Metadata {
    pub(crate) fn from_record(record: &SnapshotRecord) -> Self {
        let local = record.snapshot_type == SnapshotPlacement::Local;
        Self {
            source: record.source.clone(),
            resources: record.resources,
            updated_at_unix_ms: record.updated_at_unix_ms,
            owner_node_id: record.owner_node_id.clone(),
            local_committed: local.then(|| record.committed.clone()).flatten(),
        }
    }
}

impl SnapshotEntry {
    pub(crate) fn from_record(record: &SnapshotRecord, committed_digest: Option<String>) -> Self {
        Self {
            snapshot_id: record.id.clone(),
            alias: record.alias.clone(),
            snapshot_type: Some(record.snapshot_type),
            committed_digest,
            metadata: Some(Metadata::from_record(record)),
            retired: false,
            created_at: record.created_at_unix_ms,
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
        self.metadata
            .as_ref()
            .map(|metadata| metadata.source.clone())
            .ok_or_else(|| unavailable("snapshot has no public metadata"))
    }

    /// Reconstruct Local metadata; usable files still belong to the owner Node.
    pub fn into_local_record(self) -> MetaResult<SnapshotRecord> {
        if !self.is_visible() || self.snapshot_placement()? != SnapshotPlacement::Local {
            return Err(unavailable("snapshot is not an available Local snapshot"));
        }
        let metadata = self
            .metadata
            .ok_or_else(|| unavailable("snapshot has no public metadata"))?;
        let committed = metadata
            .local_committed
            .ok_or_else(|| unavailable("Local snapshot has no committed metadata"))?;
        Ok(SnapshotRecord {
            id: self.snapshot_id,
            snapshot_type: SnapshotPlacement::Local,
            owner_node_id: metadata.owner_node_id,
            alias: self.alias,
            source: metadata.source,
            resources: metadata.resources,
            created_at_unix_ms: self.created_at,
            updated_at_unix_ms: metadata.updated_at_unix_ms,
            committed: Some(committed),
            deleting: false,
        })
    }

    /// Assemble a Distributed record using content read by the caller.
    /// This directory does not fetch or verify the external manifest.
    pub fn into_record(self, committed: CommittedSnapshot) -> MetaResult<SnapshotRecord> {
        if !self.is_visible()
            || self.snapshot_placement()? != SnapshotPlacement::Distributed
            || self
                .committed_digest
                .as_deref()
                .is_none_or(|digest| digest.trim().is_empty())
        {
            return Err(unavailable(
                "snapshot has no available Distributed manifest",
            ));
        }
        let metadata = self
            .metadata
            .ok_or_else(|| unavailable("snapshot has no public metadata"))?;
        Ok(SnapshotRecord {
            id: self.snapshot_id,
            snapshot_type: SnapshotPlacement::Distributed,
            owner_node_id: None,
            alias: self.alias,
            source: metadata.source,
            resources: metadata.resources,
            created_at_unix_ms: self.created_at,
            updated_at_unix_ms: metadata.updated_at_unix_ms,
            committed: Some(committed),
            deleting: false,
        })
    }

    pub(crate) fn matches(&self, filter: &SnapshotListFilter) -> bool {
        if !self.is_visible() {
            return false;
        }
        let Some(metadata) = &self.metadata else {
            return false;
        };
        filter.matches_fields(&self.snapshot_id, self.alias.as_ref(), &metadata.source)
    }
}

fn unavailable(reason: impl Into<String>) -> MetaError {
    MetaError::Unavailable {
        reason: reason.into(),
    }
}
