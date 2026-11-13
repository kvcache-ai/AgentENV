use agentenv_domain::{SnapshotId, SnapshotListFilter, SnapshotManifest, SnapshotPlacement};

use crate::database::{DatabaseConfig, DatabasePool};
use crate::{
    snapshot_directory, DeletableLayer, LayerCheck, MetaError, MetaResult, SnapshotMetadata,
};

/// Snapshot directory and the database connections it owns.
#[derive(Clone)]
pub struct MetaServer {
    database: DatabasePool,
}

impl MetaServer {
    pub async fn open(config: &DatabaseConfig) -> anyhow::Result<Self> {
        let database = config.open().await?;
        let ready = database
            .migration_status()
            .await
            .and_then(|status| status.ensure_ready());
        if let Err(error) = ready {
            database.close().await;
            return Err(error);
        }
        Ok(Self { database })
    }

    pub async fn close(&self) {
        self.database.close().await;
    }

    /// Register a snapshot and, for Distributed placement, its managed layer relationships.
    pub async fn register_snapshot(
        &self,
        metadata: SnapshotMetadata,
        manifest: SnapshotManifest,
        restored_layers: &[String],
    ) -> MetaResult<SnapshotMetadata> {
        metadata
            .validate_for_publication()
            .map_err(|reason| MetaError::InvalidRequest { reason })?;
        snapshot_directory::register(&self.database, &metadata, &manifest, restored_layers).await
    }

    /// Promote a Local directory entry after the caller has published immutable content.
    pub async fn promote_snapshot(
        &self,
        metadata: SnapshotMetadata,
        manifest: SnapshotManifest,
        restored_layers: &[String],
    ) -> MetaResult<Option<SnapshotMetadata>> {
        metadata
            .validate_for_publication()
            .map_err(|reason| MetaError::InvalidRequest { reason })?;
        if metadata.snapshot_type != Some(SnapshotPlacement::Distributed) {
            return Err(MetaError::InvalidRequest {
                reason: "promotion requires Distributed placement".into(),
            });
        }
        snapshot_directory::promote(&self.database, &metadata, &manifest, restored_layers).await
    }

    /// Retain the identity and its alias even if retirement precedes registration.
    pub async fn retire_snapshot(&self, id: SnapshotId) -> MetaResult<()> {
        snapshot_directory::retire(&self.database, &id).await
    }

    pub async fn get_snapshot(&self, reference: &str) -> MetaResult<Option<SnapshotMetadata>> {
        snapshot_directory::get(&self.database, reference).await
    }

    pub async fn list_snapshots(
        &self,
        filter: SnapshotListFilter,
    ) -> MetaResult<Vec<SnapshotMetadata>> {
        snapshot_directory::list(&self.database, &filter).await
    }

    /// Pin a visible snapshot for node-local usage protection. A previously
    /// released `pin_id` is never reopened.
    pub async fn pin_snapshot(
        &self,
        id: SnapshotId,
        pin_id: &str,
    ) -> MetaResult<Option<SnapshotMetadata>> {
        snapshot_directory::pin(&self.database, &id, pin_id).await
    }

    /// Release a pin or record a tombstone so a late `pin_snapshot` cannot open it.
    pub async fn release_pin(&self, id: SnapshotId, pin_id: &str) -> MetaResult<()> {
        snapshot_directory::release_pin(&self.database, &id, pin_id).await
    }

    pub async fn check_layers(&self, uuids: &[String]) -> MetaResult<Vec<LayerCheck>> {
        snapshot_directory::check_layers(&self.database, uuids).await
    }

    pub async fn revert_layer_deletions(&self, uuids: &[String]) -> MetaResult<()> {
        snapshot_directory::finish_layer_deletions(&self.database, uuids, "available").await
    }

    pub async fn reap_retired_snapshots(&self, limit: u32) -> MetaResult<Vec<SnapshotId>> {
        snapshot_directory::reap_retired_snapshots(&self.database, limit).await
    }

    pub async fn complete_reaped_snapshot(&self, id: SnapshotId) -> MetaResult<bool> {
        snapshot_directory::complete_reaped_snapshot(&self.database, &id).await
    }

    pub async fn claim_deletable_layers(&self, limit: u32) -> MetaResult<Vec<DeletableLayer>> {
        snapshot_directory::claim_deletable_layers(&self.database, limit).await
    }

    pub async fn complete_deleted_layers(&self, uuids: &[String]) -> MetaResult<()> {
        snapshot_directory::finish_layer_deletions(&self.database, uuids, "deleted").await
    }
}
