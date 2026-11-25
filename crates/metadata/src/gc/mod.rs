mod sweep;
#[cfg(test)]
mod tests;

use agentenv_domain::SnapshotId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    pub reaped_snapshots: usize,
    pub deleted_layers: usize,
}

#[async_trait]
pub trait GcStorage: Send + Sync {
    async fn delete_snapshot(&self, id: &SnapshotId) -> anyhow::Result<()>;
    async fn delete_layer(&self, digest: &str) -> anyhow::Result<()>;

    /// Delete a batch of layer digests, returning one result per input digest in order.
    async fn delete_layers(&self, digests: &[String]) -> Vec<anyhow::Result<()>> {
        let mut results = Vec::with_capacity(digests.len());
        for digest in digests {
            results.push(self.delete_layer(digest).await);
        }
        results
    }
}

/// Worker-local keyset cursors preserved across sweeps so persistent head failures
/// cannot starve later healthy items while backoff pauses the stalled phase.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub(crate) struct GcCursor {
    after_layer: Option<String>,
    after_snapshot: Option<String>,
}
