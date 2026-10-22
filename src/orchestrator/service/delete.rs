//! Forward-only sandbox deletion, retried through `DeleteProgress`.

use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::image::cache::RuntimeImageOwner;
use crate::sandbox::{SandboxBackendFactory, SandboxCaptureError};
use crate::types::SandboxId;

use super::super::persistence::SandboxPersister;
use super::super::store::*;
use super::super::types::{SandboxLifecycleEventType, SandboxState};
use super::super::{OrchestratorError, Result, SandboxOperation};

use super::Orchestrator;

#[derive(Default)]
pub(super) enum DeleteProgress {
    #[default]
    Capture,
    Stop {
        capture_failed: bool,
    },
    Release {
        capture_failed: bool,
    },
    Done,
}

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    /// Stops and deletes the sandbox with the given ID.
    ///
    /// If the sandbox is currently in a transitional state, this method waits for
    /// the in-progress operation to finish before proceeding with deletion, preventing
    /// races where an ongoing operation might overwrite the `Killing` state.
    pub async fn delete_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("delete", sandbox_id, async move {
            this.delete_sandbox_inner(sandbox_id).await
        })
        .await
    }

    pub(super) async fn deletion_progress(
        &self,
        sandbox_id: SandboxId,
    ) -> Arc<Mutex<DeleteProgress>> {
        self.deletions
            .lock()
            .await
            .entry(sandbox_id)
            .or_default()
            .clone()
    }

    #[tracing::instrument(
        name = "delete_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]

    pub(super) async fn delete_sandbox_inner(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
    ) -> Result<()> {
        info!("deleting sandbox");
        let deletion = self.deletion_progress(sandbox_id).await;
        let mut progress = deletion.lock().await;
        match *progress {
            DeleteProgress::Done => return Ok(()),
            DeleteProgress::Capture => {}
            _ => {
                return self
                    .delete_sandbox_impl(sandbox_id, SandboxState::Killing, &mut progress)
                    .await
            }
        }

        // Attempt to transition to Killing, retrying after waiting whenever we
        // find the sandbox in a transitional state.
        let previous_state = loop {
            match self
                .store
                .update_state_if_state(
                    &sandbox_id,
                    SandboxState::Killing,
                    &[SandboxState::Running, SandboxState::Paused],
                )
                .await
            {
                Ok(previous_state) => break previous_state,
                Err(StoreError::StateConflict { actual_state, .. }) => match actual_state {
                    SandboxState::Killing => {
                        debug!("sandbox already in killing state, waiting for delete to finish");
                        match self
                            .wait_for_transition(sandbox_id, SandboxState::Killing)
                            .await
                        {
                            Ok(_) => {
                                // The in-flight delete rolled back to a stable state.
                                // Retry the Killing CAS rather than letting multiple
                                // deleters run concurrently.
                                continue;
                            }
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                info!("sandbox was deleted by a concurrent delete");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    SandboxState::Creating
                    | SandboxState::Snapshotting
                    | SandboxState::Forking
                    | SandboxState::Pausing
                    | SandboxState::Resuming => {
                        // An in-progress operation is currently holding the sandbox in this
                        // transitional state.  Wait for it to finish so our Killing transition
                        // doesn't race with the final state write from that operation.
                        debug!(
                            state = ?actual_state,
                            "sandbox in transitional state, waiting before deletion"
                        );
                        match self.wait_for_transition(sandbox_id, actual_state).await {
                            Ok(_) => {
                                // Transition finished; retry the Killing CAS.
                                continue;
                            }
                            Err(OrchestratorError::SandboxNotFound(_)) => {
                                // Sandbox was removed while we waited (e.g. by
                                // another concurrent delete).
                                info!("sandbox was deleted while waiting for transitional state");
                                return Ok(());
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    _ => {
                        return Err(OrchestratorError::from(StoreError::StateConflict {
                            sandbox_id,
                            expected_states: vec![SandboxState::Running, SandboxState::Paused],
                            actual_state,
                        }));
                    }
                },
                Err(err) => {
                    if matches!(err, StoreError::SandboxNotFound { .. }) {
                        self.deletions.lock().await.remove(&sandbox_id);
                    }
                    return Err(OrchestratorError::from(err));
                }
            }
        };

        self.delete_sandbox_impl(sandbox_id, previous_state, &mut progress)
            .await
    }

    pub(super) async fn delete_sandbox_impl(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        previous_state: SandboxState,
        progress: &mut DeleteProgress,
    ) -> Result<()> {
        let metadata = self.store.get(&sandbox_id).await?;
        let optional_cache = metadata
            .as_ref()
            .is_some_and(|record| record.template_builder);
        let volume_ids = metadata
            .map(|metadata| metadata.volume_mounts.into_values().collect::<Vec<_>>())
            .unwrap_or_default();
        let (handle, removed_route) = self.detach_sandbox_handle_and_route(&sandbox_id).await;
        let mut capture_error = None;

        if matches!(progress, DeleteProgress::Capture) {
            let mut volumes_frozen = false;
            let capture_result: std::result::Result<(), SandboxCaptureError> = async {
                if let Some(handle) = handle.as_ref() {
                    if previous_state == SandboxState::Running && !volume_ids.is_empty() {
                        handle.lock().await.freeze_and_snapshot_volumes().await?;
                        volumes_frozen = true;
                    }
                }
                self.publish_sandbox_volume_backings(sandbox_id, &volume_ids)
                    .await
                    .map_err(|error| SandboxCaptureError::recoverable(error.into()))
            }
            .await;
            if let Err(mut error) = capture_result {
                if volumes_frozen && !error.is_terminal() && !optional_cache {
                    if let Some(handle) = handle.as_ref() {
                        if let Err(thaw_error) = handle.lock().await.thaw_volumes().await {
                            error = SandboxCaptureError::terminal(anyhow::anyhow!(
                                "delete failed: {error}; thaw failed: {thaw_error:#}"
                            ));
                        }
                    }
                }
                // Builder caches are optional; still stop and release the worker on capture failure.
                if !error.is_terminal() && !optional_cache {
                    if let Some(handle) = handle {
                        self.sandboxes.write().await.insert(sandbox_id, handle);
                    }
                    self.restore_proxy_route(sandbox_id, removed_route).await;
                    self.store
                        .update_state_if_state(
                            &sandbox_id,
                            previous_state,
                            &[SandboxState::Killing],
                        )
                        .await?;
                    return Err(OrchestratorError::SandboxOperationFailed {
                        sandbox_id,
                        operation: SandboxOperation::Stop,
                        source: error.into(),
                    });
                }
                warn!(
                    ?error,
                    "volume capture failed; stopping sandbox and failing its volumes"
                );
                capture_error = Some(error);
            }
            *progress = DeleteProgress::Stop {
                capture_failed: capture_error.is_some(),
            };
        }

        let cleanup: anyhow::Result<()> = async {
            if let DeleteProgress::Stop { capture_failed } = *progress {
                if let Some(handle) = handle.as_ref() {
                    handle.lock().await.stop().await?;
                }
                *progress = DeleteProgress::Release { capture_failed };
            }
            if let DeleteProgress::Release { capture_failed } = *progress {
                if let Some(manager) = self.volume_manager.as_ref() {
                    let owner = sandbox_id.to_string();
                    // An incomplete restack may leave image.json pointing at old
                    // layers. Never publish that backing as a successful capture.
                    if capture_failed {
                        manager.fail_backings(&owner, &volume_ids).await?;
                    }
                    manager.replace_owner_for(&owner, None, &volume_ids).await?;
                }
                self.remove_deleted_sandbox(sandbox_id).await?;
                *progress = DeleteProgress::Done;
            }
            Ok(())
        }
        .await;
        if let Err(error) = cleanup {
            if matches!(progress, DeleteProgress::Stop { .. }) {
                if let Some(handle) = handle {
                    self.sandboxes.write().await.insert(sandbox_id, handle);
                }
            }
            warn!(?error, "sandbox deletion needs a cleanup retry");
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Stop,
                source: error,
            });
        }
        if let Some(error) = capture_error.filter(|_| !optional_cache) {
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Stop,
                source: error.into(),
            });
        }
        Ok(())
    }

    async fn remove_deleted_sandbox(&self, sandbox_id: SandboxId) -> Result<()> {
        let metadata = self.store.remove(&sandbox_id).await?;
        if let Some(metadata) = metadata {
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Delete,
                metadata.id,
                metadata.resources,
            );
        }
        if let Err(err) = self
            .persister
            .delete_record_and_artifacts(&sandbox_id)
            .await
        {
            warn!(error = ?err, "failed to delete persisted sandbox state");
        }
        self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
            .await;
        self.deletions.lock().await.remove(&sandbox_id);
        info!("sandbox deleted");

        Ok(())
    }
}
