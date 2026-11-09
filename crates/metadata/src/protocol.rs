use agentenv_domain::{SnapshotId, SnapshotListFilter, SnapshotRecord};
use tarpc::context::Context;

use crate::{MetaResult, MetaServer, SnapshotEntry};

#[tarpc::service]
pub trait Meta {
    async fn health() -> MetaResult<()>;
    async fn get_snapshot(reference: String) -> MetaResult<Option<SnapshotEntry>>;
    async fn list_snapshots(filter: SnapshotListFilter) -> MetaResult<Vec<SnapshotEntry>>;
    async fn register_snapshot(record: SnapshotRecord) -> MetaResult<SnapshotEntry>;
    async fn promote_snapshot(record: SnapshotRecord) -> MetaResult<Option<SnapshotEntry>>;
    async fn retire_snapshot(id: SnapshotId) -> MetaResult<()>;
}

impl Meta for MetaServer {
    async fn health(self, _: Context) -> MetaResult<()> {
        Ok(())
    }

    async fn get_snapshot(
        self,
        _: Context,
        reference: String,
    ) -> MetaResult<Option<SnapshotEntry>> {
        MetaServer::get_snapshot(&self, &reference).await
    }

    async fn list_snapshots(
        self,
        _: Context,
        filter: SnapshotListFilter,
    ) -> MetaResult<Vec<SnapshotEntry>> {
        MetaServer::list_snapshots(&self, filter).await
    }

    async fn register_snapshot(
        self,
        _: Context,
        record: SnapshotRecord,
    ) -> MetaResult<SnapshotEntry> {
        MetaServer::register_snapshot(&self, record).await
    }

    async fn promote_snapshot(
        self,
        _: Context,
        record: SnapshotRecord,
    ) -> MetaResult<Option<SnapshotEntry>> {
        MetaServer::promote_snapshot(&self, record).await
    }

    async fn retire_snapshot(self, _: Context, id: SnapshotId) -> MetaResult<()> {
        MetaServer::retire_snapshot(&self, id).await
    }
}
