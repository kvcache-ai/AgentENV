use agentenv_domain::{SnapshotId, SnapshotListFilter, SnapshotManifest, SnapshotMetadata};
use tarpc::context::Context;

use crate::{DeletableLayer, LayerCheck, MetaResult, MetaServer};

#[tarpc::service]
pub trait Meta {
    async fn health() -> MetaResult<()>;
    async fn get_snapshot(reference: String) -> MetaResult<Option<SnapshotMetadata>>;
    async fn list_snapshots(filter: SnapshotListFilter) -> MetaResult<Vec<SnapshotMetadata>>;
    async fn register_snapshot(
        metadata: SnapshotMetadata,
        manifest: SnapshotManifest,
        restored_layers: Vec<String>,
    ) -> MetaResult<SnapshotMetadata>;
    async fn promote_snapshot(
        metadata: SnapshotMetadata,
        manifest: SnapshotManifest,
        restored_layers: Vec<String>,
    ) -> MetaResult<Option<SnapshotMetadata>>;
    async fn retire_snapshot(id: SnapshotId) -> MetaResult<()>;
    async fn pin_snapshot(id: SnapshotId, pin_id: String) -> MetaResult<Option<SnapshotMetadata>>;
    async fn release_pin(id: SnapshotId, pin_id: String) -> MetaResult<()>;
    async fn check_layers(uuids: Vec<String>) -> MetaResult<Vec<LayerCheck>>;
    async fn revert_layer_deletions(uuids: Vec<String>) -> MetaResult<()>;
    async fn reap_retired_snapshots(limit: u32) -> MetaResult<Vec<SnapshotId>>;
    async fn complete_reaped_snapshot(id: SnapshotId) -> MetaResult<bool>;
    async fn claim_deletable_layers(limit: u32) -> MetaResult<Vec<DeletableLayer>>;
    async fn complete_deleted_layers(uuids: Vec<String>) -> MetaResult<()>;
}

impl Meta for MetaServer {
    async fn health(self, _: Context) -> MetaResult<()> {
        Ok(())
    }

    async fn get_snapshot(
        self,
        _: Context,
        reference: String,
    ) -> MetaResult<Option<SnapshotMetadata>> {
        Box::pin(MetaServer::get_snapshot(&self, &reference)).await
    }

    async fn list_snapshots(
        self,
        _: Context,
        filter: SnapshotListFilter,
    ) -> MetaResult<Vec<SnapshotMetadata>> {
        Box::pin(MetaServer::list_snapshots(&self, filter)).await
    }

    async fn register_snapshot(
        self,
        _: Context,
        metadata: SnapshotMetadata,
        manifest: SnapshotManifest,
        restored_layers: Vec<String>,
    ) -> MetaResult<SnapshotMetadata> {
        Box::pin(MetaServer::register_snapshot(
            &self,
            metadata,
            manifest,
            &restored_layers,
        ))
        .await
    }

    async fn promote_snapshot(
        self,
        _: Context,
        metadata: SnapshotMetadata,
        manifest: SnapshotManifest,
        restored_layers: Vec<String>,
    ) -> MetaResult<Option<SnapshotMetadata>> {
        Box::pin(MetaServer::promote_snapshot(
            &self,
            metadata,
            manifest,
            &restored_layers,
        ))
        .await
    }

    async fn retire_snapshot(self, _: Context, id: SnapshotId) -> MetaResult<()> {
        Box::pin(MetaServer::retire_snapshot(&self, id)).await
    }

    async fn pin_snapshot(
        self,
        _: Context,
        id: SnapshotId,
        pin_id: String,
    ) -> MetaResult<Option<SnapshotMetadata>> {
        Box::pin(MetaServer::pin_snapshot(&self, id, &pin_id)).await
    }

    async fn release_pin(self, _: Context, id: SnapshotId, pin_id: String) -> MetaResult<()> {
        Box::pin(MetaServer::release_pin(&self, id, &pin_id)).await
    }

    async fn check_layers(self, _: Context, uuids: Vec<String>) -> MetaResult<Vec<LayerCheck>> {
        Box::pin(MetaServer::check_layers(&self, &uuids)).await
    }

    async fn revert_layer_deletions(self, _: Context, uuids: Vec<String>) -> MetaResult<()> {
        Box::pin(MetaServer::revert_layer_deletions(&self, &uuids)).await
    }

    async fn reap_retired_snapshots(self, _: Context, limit: u32) -> MetaResult<Vec<SnapshotId>> {
        Box::pin(MetaServer::reap_retired_snapshots(&self, limit)).await
    }

    async fn complete_reaped_snapshot(self, _: Context, id: SnapshotId) -> MetaResult<bool> {
        Box::pin(MetaServer::complete_reaped_snapshot(&self, id)).await
    }

    async fn claim_deletable_layers(
        self,
        _: Context,
        limit: u32,
    ) -> MetaResult<Vec<DeletableLayer>> {
        Box::pin(MetaServer::claim_deletable_layers(&self, limit)).await
    }

    async fn complete_deleted_layers(self, _: Context, uuids: Vec<String>) -> MetaResult<()> {
        Box::pin(MetaServer::complete_deleted_layers(&self, &uuids)).await
    }
}
