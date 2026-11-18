use agentenv_domain::{SnapshotId, SnapshotListFilter, SnapshotManifest, SnapshotMetadata};
use tarpc::context::Context;

use crate::{MetaResult, MetaServer};

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
    async fn collect_garbage(limit: u32) -> MetaResult<crate::GcReport>;
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

    async fn collect_garbage(self, _: Context, limit: u32) -> MetaResult<crate::GcReport> {
        // Keep claimed layer deletions running when an operator disconnects.
        let active = self.active_collections.clone().read_owned().await;
        tokio::spawn(async move {
            let _active = active;
            MetaServer::collect_garbage(&self, limit).await
        })
        .await
        .map_err(|error| crate::MetaError::backend("finish collection", error))?
    }
}
