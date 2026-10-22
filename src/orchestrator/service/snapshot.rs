//! Capturing snapshots and volume snapshots of a running sandbox.

use std::sync::Arc;

use tracing::{info, warn};

use crate::sandbox::{SandboxBackendFactory, SandboxCaptureError};
use crate::types::SandboxId;

use super::super::claim::{Claim, Snapshot};
use super::super::persistence::SandboxPersister;
use super::super::store::*;
use super::super::types::{SandboxState, SnapshotCaptureResult};
use super::super::{OrchestratorError, Result, SandboxOperation};

use super::{Orchestrator, SandboxHandle};

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    /// Captures a snapshot of a running sandbox.
    pub async fn capture_snapshot(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("snapshot", sandbox_id, async move {
            this.capture_snapshot_inner(sandbox_id).await
        })
        .await
    }

    async fn begin_snapshot_operation(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<(Claim<Snapshot>, SandboxHandle)> {
        self.ensure_accepting_lifecycle_operations()?;
        let claim = Claim::<Snapshot>::acquire(self.claim_host(), sandbox_id)
            .await
            .map_err(|error| match error {
                StoreError::StateConflict {
                    actual_state: SandboxState::Killing,
                    ..
                } => OrchestratorError::SandboxNotFound(sandbox_id),
                StoreError::StateConflict { actual_state, .. } => {
                    OrchestratorError::InvalidSandboxState {
                        sandbox_id,
                        state: actual_state,
                    }
                }
                error => OrchestratorError::from(error),
            })?;

        if let Some(handle) = self.sandboxes.read().await.get(&sandbox_id).cloned() {
            return Ok((claim, handle));
        }
        warn!("sandbox handle not found while snapshotting, removing from store");
        claim.teardown(None).await?;
        Err(OrchestratorError::SandboxNotFound(sandbox_id))
    }

    async fn fail_snapshot_operation<T>(
        &self,
        claim: Claim<Snapshot>,
        handle: SandboxHandle,
        error: SandboxCaptureError,
        operation: SandboxOperation,
    ) -> Result<T> {
        warn!(?error, ?operation, "sandbox snapshot operation failed");
        let sandbox_id = claim.sandbox_id();
        if error.is_terminal() {
            claim.teardown(Some(handle)).await?;
        } else {
            claim.rollback().await;
        }
        Err(OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation,
            source: error.into(),
        })
    }

    async fn finish_snapshot_operation(&self, claim: Claim<Snapshot>) -> Result<SandboxMetadata> {
        match claim.commit(|_| {}).await {
            Ok(metadata) => Ok(metadata),
            Err((claim, err)) => {
                claim.rollback().await;
                Err(OrchestratorError::from(err))
            }
        }
    }

    /// Seals writable persistent-volume uppers without capturing the VM's
    /// rootfs, memory, or device state so callers can clone them locally.
    pub async fn snapshot_volume_mounts(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("snapshot_volumes", sandbox_id, async move {
            this.snapshot_volume_mounts_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "snapshot_volume_mounts",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]

    async fn snapshot_volume_mounts_inner(self: Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let (claim, handle) = self.begin_snapshot_operation(sandbox_id).await?;
        if let Err(error) = {
            let mut sandbox = handle.lock().await;
            sandbox.snapshot_volumes().await
        } {
            return self
                .fail_snapshot_operation(claim, handle, error, SandboxOperation::SnapshotVolumes)
                .await;
        }
        self.finish_snapshot_operation(claim).await.map(|_| ())
    }

    #[tracing::instrument(
        name = "capture_snapshot",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]

    async fn capture_snapshot_inner(
        self: Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotCaptureResult> {
        info!("capturing sandbox snapshot");
        let (claim, handle) = self.begin_snapshot_operation(sandbox_id).await?;
        let result = {
            let mut sandbox = handle.lock().await;
            sandbox.snapshot().await
        };
        let captured_snapshot = match result {
            Ok(captured_snapshot) => captured_snapshot,
            Err(error) => {
                return self
                    .fail_snapshot_operation(claim, handle, error, SandboxOperation::Snapshot)
                    .await;
            }
        };

        let metadata = self.finish_snapshot_operation(claim).await?;

        info!("snapshot captured");
        Ok(SnapshotCaptureResult {
            metadata,
            captured_snapshot,
        })
    }
}
