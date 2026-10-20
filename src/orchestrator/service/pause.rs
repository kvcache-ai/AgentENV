//! Pausing a running sandbox and persisting it for later resume.

use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::image::cache::RuntimeImageOwner;
use crate::sandbox::{RuntimeArtifactSet, SandboxBackendFactory};
use crate::types::SandboxId;

use super::super::claim::{Claim, Pause};
use super::super::persistence::SandboxPersister;
use super::super::proxy::ProxyRoute;
use super::super::store::*;
use super::super::types::{SandboxLifecycleEventType, SandboxState};
use super::super::{OrchestratorError, Result, SandboxOperation};

use super::{Orchestrator, SandboxHandle};

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    /// Pauses a running sandbox by taking a snapshot and stopping its VM.
    ///
    /// If another `pause_sandbox` call is already in progress for the same
    /// sandbox (`Pausing` state), this call waits for it to complete and then
    /// returns the outcome rather than duplicating the work.
    pub async fn pause_sandbox(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("pause", sandbox_id, async move {
            this.pause_sandbox_inner(sandbox_id).await
        })
        .await
    }

    #[tracing::instrument(
        name = "pause_sandbox",
        skip(self),
        fields(sandbox_id = %sandbox_id)
    )]

    pub(super) async fn pause_sandbox_inner(self: &Arc<Self>, sandbox_id: SandboxId) -> Result<()> {
        info!("pausing sandbox");
        let claim = match Claim::<Pause>::acquire(self.claim_host(), sandbox_id).await {
            Ok(claim) => claim,
            Err(StoreError::StateConflict { actual_state, .. }) => {
                return match actual_state {
                    // Another task is already performing the pause.  Wait for
                    // it to finish and then report the final outcome.
                    SandboxState::Pausing => self.join_concurrent_pause(sandbox_id).await,
                    SandboxState::Paused => Ok(()),
                    SandboxState::Killing => {
                        info!("sandbox is being deleted while pausing");
                        Err(OrchestratorError::SandboxNotFound(sandbox_id))
                    }
                    _ => {
                        info!(state = ?actual_state, "cannot pause sandbox in current state");
                        Err(OrchestratorError::InvalidSandboxState {
                            sandbox_id,
                            state: actual_state,
                        })
                    }
                };
            }
            Err(err) => return Err(OrchestratorError::from(err)),
        };

        self.pause_claimed(claim).await
    }

    pub(super) async fn pause_claimed(self: &Arc<Self>, claim: Claim<Pause>) -> Result<()> {
        let sandbox_id = claim.sandbox_id();
        // Pin paused runtime artifacts before detaching from the running set.
        let runtime_artifacts = {
            let handle = self.sandboxes.read().await.get(&sandbox_id).cloned();
            match handle {
                Some(handle) => {
                    let sandbox = handle.lock().await;
                    sandbox.runtime_info().runtime_artifacts
                }
                None => RuntimeArtifactSet::empty(),
            }
        };
        if let Err(error) = self
            .protect_image_refs(
                RuntimeImageOwner::PausedSandbox(sandbox_id),
                runtime_artifacts,
                "paused sandbox",
            )
            .await
        {
            warn!(error = %error, "failed to protect paused runtime artifacts; keeping sandbox Running");
            claim.rollback().await;
            return Err(error);
        }

        // Allocate persistence space while the running handle and route are
        // still attached. Allocation does not mutate the backend, so failure
        // only needs to restore metadata and release the temporary image refs.
        let artifact_root = match self.persister.allocate_artifact_root(&sandbox_id).await {
            Ok(artifact_root) => artifact_root,
            Err(err) => {
                warn!(error = ?err, "failed to allocate paused sandbox artifact root");
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                claim.rollback().await;
                return Err(OrchestratorError::from(err));
            }
        };

        let (handle, removed_proxy_route) = self.detach_sandbox_handle_and_route(&sandbox_id).await;

        let Some(handle) = handle else {
            warn!("sandbox handle not found while pausing, removing from store");
            self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                .await;
            claim.teardown(None).await?;
            return Err(OrchestratorError::SandboxNotFound(sandbox_id));
        };

        // Pause the sandbox and capture the paused state for resuming later.
        let paused_state_result = {
            let mut sandbox = handle.lock().await;
            sandbox.pause(artifact_root.as_deref()).await
        };

        // If pausing failed, attempt to put the sandbox back and return an error.
        let paused_state = match paused_state_result {
            Ok(s) => s,
            Err(err) => {
                warn!(error = ?err, "failed to pause sandbox");
                if err.is_terminal() {
                    // The handle was already detached from `self.sandboxes`
                    // before `pause()`. Do not reinsert it here: the live
                    // runtime may have been mutated and is no longer safe to
                    // keep serving as a running sandbox.
                    claim.teardown(Some(handle)).await?;
                } else {
                    self.sandboxes.write().await.insert(sandbox_id, handle);
                    self.restore_proxy_route(sandbox_id, removed_proxy_route)
                        .await;
                    claim.rollback().await;
                }
                self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
                    .await;
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id,
                    operation: SandboxOperation::Pause,
                    source: err.into(),
                });
            }
        };

        let mut persisted_metadata = claim.metadata().clone();
        persisted_metadata.state = SandboxState::Paused;
        persisted_metadata.paused_state = Some(paused_state.clone());
        if let Err(err) = self
            .persister
            .persist_paused(
                &persisted_metadata,
                artifact_root.as_deref(),
                paused_state.as_ref(),
            )
            .await
        {
            warn!(error = ?err, "failed to persist paused sandbox state");
            return self
                .recover_failed_pause(
                    claim,
                    handle,
                    removed_proxy_route,
                    format!("failed to persist paused sandbox state: {err:#}"),
                )
                .await;
        }
        let resources = persisted_metadata.resources;
        if let Err((claim, err)) = claim
            .commit(move |metadata| metadata.paused_state = Some(paused_state))
            .await
        {
            warn!(error = ?err, "failed to record paused sandbox state");
            return self
                .recover_failed_pause(
                    claim,
                    handle,
                    removed_proxy_route,
                    format!("failed to record paused sandbox state: {err:#}"),
                )
                .await;
        }

        // Stop the sandbox to free up resources.
        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        if let Err(err) = stop_result {
            warn!(error = ?err, "failed to stop sandbox after pausing");
        }
        self.publish_sandbox_event(SandboxLifecycleEventType::Pause, sandbox_id, resources);
        info!("sandbox paused");

        Ok(())
    }

    /// Resumes a VM that was paused but could not be recorded as paused, and
    /// keeps it running; tears it down if it cannot resume.
    async fn recover_failed_pause(
        &self,
        claim: Claim<Pause>,
        handle: SandboxHandle,
        removed_proxy_route: Option<ProxyRoute>,
        message: String,
    ) -> Result<()> {
        let sandbox_id = claim.sandbox_id();
        let resume_result = {
            let mut sandbox = handle.lock().await;
            sandbox.resume().await
        };
        if let Err(resume_err) = resume_result {
            warn!(error = ?resume_err, "failed to resume sandbox after pause failure");
            if let Err(error) = claim.teardown(Some(handle)).await {
                warn!(error = ?error, "failed to remove sandbox after pause failure");
            }
        } else {
            // Keep the artifact root: the resumed runtime may reference layers
            // captured there. It is removed when the sandbox is deleted.
            self.sandboxes.write().await.insert(sandbox_id, handle);
            self.restore_proxy_route(sandbox_id, removed_proxy_route)
                .await;
            claim.rollback().await;
        }
        self.release_image_refs(RuntimeImageOwner::PausedSandbox(sandbox_id))
            .await;
        Err(OrchestratorError::InternalError(message))
    }

    /// Joins a concurrent pause already in progress for the same sandbox.
    /// Waits for the `Pausing` state to resolve and maps the final state to
    /// the appropriate `Ok(())` / `Err(...)` result.
    pub(super) async fn join_concurrent_pause(&self, sandbox_id: SandboxId) -> Result<()> {
        debug!("concurrent pause in progress, waiting for completion");
        let m = self
            .wait_for_transition(sandbox_id, SandboxState::Pausing)
            .await?;
        match m.state {
            SandboxState::Paused => {
                debug!("concurrent pause succeeded");
                Ok(())
            }
            SandboxState::Running => {
                info!("concurrent pause failed; sandbox returned to running state");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: SandboxState::Running,
                })
            }
            SandboxState::Killing => {
                info!("sandbox is being deleted after concurrent pause attempt");
                Err(OrchestratorError::SandboxNotFound(sandbox_id))
            }
            other => {
                info!(state = ?other, "unexpected state after waiting for concurrent pause");
                Err(OrchestratorError::InvalidSandboxState {
                    sandbox_id,
                    state: other,
                })
            }
        }
    }
}
