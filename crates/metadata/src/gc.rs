use std::sync::Arc;

use agentenv_domain::SnapshotId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{MetaError, MetaResult, MetaServer};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    pub reaped_snapshots: usize,
    pub deleted_layers: usize,
}

pub enum LayerDeleteError {
    StillPresent(anyhow::Error),
    UnknownOutcome(anyhow::Error),
}

#[async_trait]
pub trait GcStorage: Send + Sync {
    async fn delete_snapshot(&self, id: &SnapshotId) -> anyhow::Result<()>;
    async fn delete_layer(&self, digest: &str) -> Result<(), LayerDeleteError>;
}

impl MetaServer {
    pub fn with_gc_storage(mut self, storage: Arc<dyn GcStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub async fn collect_garbage(&self, limit: u32) -> MetaResult<GcReport> {
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetaError::Unavailable {
                reason: "snapshot repository is not configured for collection".into(),
            })?;
        let mut first_error = None;
        let mut reaped_snapshots = 0;
        for id in self.reap_retired_snapshots(limit).await? {
            match storage.delete_snapshot(&id).await {
                Ok(()) => reaped_snapshots += usize::from(self.complete_reaped_snapshot(id).await?),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }

        let mut deleted = Vec::new();
        let mut still_present = Vec::new();
        for layer in self.claim_deletable_layers(limit).await? {
            match storage.delete_layer(&layer.digest).await {
                Ok(()) => deleted.push(layer.uuid),
                Err(LayerDeleteError::StillPresent(error)) => {
                    still_present.push(layer.uuid);
                    first_error.get_or_insert(error);
                }
                Err(LayerDeleteError::UnknownOutcome(error)) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        self.complete_deleted_layers(&deleted).await?;
        self.revert_layer_deletions(&still_present).await?;
        if let Some(error) = first_error {
            return Err(MetaError::backend("collect snapshot storage", error));
        }
        Ok(GcReport {
            reaped_snapshots,
            deleted_layers: deleted.len(),
        })
    }
}
