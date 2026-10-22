//! Applying the timeout action to expired sandboxes.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, warn};

use crate::sandbox::SandboxBackendFactory;
use crate::types::SandboxId;

use super::super::claim::{Claim, Pause};
use super::super::persistence::SandboxPersister;
use super::super::store::*;
use super::super::types::SandboxState;
use super::super::{OrchestratorError, Result};

use super::Orchestrator;

/// An expired sandbox claimed for its timeout action.
enum ExpiredClaim {
    Pause(Claim<Pause>),
    /// Deletion is forward-only and retried through `DeleteProgress`, so it
    /// holds `Killing` without a claim.
    Delete,
}

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    async fn claim_expired_running_sandbox(
        self: &Arc<Self>,
        sandbox_id: SandboxId,
        action: SandboxTimeoutAction,
        cutoff: SystemTime,
    ) -> Result<Option<ExpiredClaim>> {
        let is_expired = move |metadata: &SandboxMetadata| metadata.is_expired(cutoff);
        let claimed = match action {
            SandboxTimeoutAction::Pause => {
                Claim::<Pause>::acquire_if(self.claim_host(), sandbox_id, is_expired)
                    .await
                    .map(|claim| claim.map(ExpiredClaim::Pause))
            }
            SandboxTimeoutAction::Delete => self
                .store
                .update_if_state(&sandbox_id, &[SandboxState::Running], |metadata| {
                    if is_expired(metadata) {
                        metadata.state = SandboxState::Killing;
                    }
                })
                .await
                .map(|update| {
                    (update.current.state == SandboxState::Killing).then_some(ExpiredClaim::Delete)
                }),
        };
        match claimed {
            Ok(Some(claim)) => Ok(Some(claim)),
            Ok(None) => {
                debug!(
                    ?cutoff,
                    "skipping auto-eviction because sandbox expiry was updated"
                );
                Ok(None)
            }
            Err(StoreError::StateConflict { actual_state, .. }) => {
                debug!(
                    state = ?actual_state,
                    "skipping auto-eviction because sandbox state changed"
                );
                Ok(None)
            }
            Err(StoreError::SandboxNotFound { .. }) => {
                debug!("skipping auto-eviction because sandbox no longer exists");
                Ok(None)
            }
            Err(err) => Err(OrchestratorError::from(err)),
        }
    }

    /// Automatically pauses or stops sandboxes whose timeout has expired.
    pub(super) async fn evict_expired_sandboxes(self: &Arc<Self>) -> Result<Vec<SandboxId>> {
        if self.is_shutting_down() {
            debug!("skipping auto-evict because orchestrator is shutting down");
            return Ok(Vec::new());
        }

        let eviction_cutoff = SystemTime::now();
        let expired = self.store.list_expired(eviction_cutoff).await?;
        let mut evicted_ids = Vec::new();

        for metadata in expired {
            if metadata.state != SandboxState::Running {
                continue;
            }
            // Use the same operation lock as explicit deletion before claiming Killing.
            let deletion = self.deletion_progress(metadata.id).await;
            let mut progress = deletion.lock().await;
            let result = match self
                .claim_expired_running_sandbox(
                    metadata.id,
                    metadata.timeout_action,
                    eviction_cutoff,
                )
                .await
            {
                Ok(Some(ExpiredClaim::Pause(claim))) => {
                    self.pause_claimed(claim).await.map(|_| true)
                }
                Ok(Some(ExpiredClaim::Delete)) => self
                    .delete_sandbox_impl(metadata.id, SandboxState::Running, &mut progress)
                    .await
                    .map(|_| true),
                Ok(None) => Ok(false),
                Err(err) => Err(err),
            };
            match result {
                Ok(true) => evicted_ids.push(metadata.id),
                Ok(false) => continue,
                Err(err) => warn!(
                    sandbox_id = %metadata.id,
                    action = ?metadata.timeout_action,
                    error = ?err,
                    "failed to auto-evict expired sandbox"
                ),
            }
        }

        Ok(evicted_ids)
    }

    /// Starts a background task that periodically evicts expired sandboxes.
    /// The eviction policy is defined by the sandbox's [`timeout_action`](SandboxMetadata::timeout_action).
    pub(super) fn start_auto_evict_task(
        this: Arc<Self>,
        evict_interval: Duration,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Ok(runtime_handle) = tokio::runtime::Handle::try_current() else {
            warn!("auto-evict task not started: no Tokio runtime available");
            return;
        };

        let this = Arc::downgrade(&this);
        runtime_handle.spawn(async move {
            let mut ticker = tokio::time::interval(evict_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            debug!("auto-evict task started with interval {:?}", evict_interval);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("auto-evict task stopping because orchestrator is shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let Some(this) = this.upgrade() else {
                            debug!("auto-evict task stopping because orchestrator was dropped");
                            break;
                        };
                        if let Err(err) = this.evict_expired_sandboxes().await {
                            warn!("auto-evict task failed: {err}");
                        }
                    }
                }
            }
        });
    }
}
