mod sweep;
#[cfg(test)]
mod tests;

use agentenv_domain::SnapshotId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    pub reaped_snapshots: usize,
    pub reaped_heads: usize,
    pub deleted_layers: usize,
}

#[async_trait]
pub trait GcStorage: Send + Sync {
    async fn delete_snapshot(&self, id: &SnapshotId) -> anyhow::Result<()>;

    /// Delete a batch of layer digests, returning one result per input digest in order.
    async fn delete_layers(&self, digests: &[String]) -> Vec<anyhow::Result<()>>;
}

/// Worker-local keyset cursors preserved across sweeps so persistent head failures
/// cannot starve later healthy items while backoff pauses the stalled phase.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub(crate) struct GcCursor {
    after_layer: Option<Uuid>,
    after_snapshot: Option<Uuid>,
    after_head: Option<(String, String, Uuid)>,
}
