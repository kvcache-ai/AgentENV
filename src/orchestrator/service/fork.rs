//! Forking a running sandbox into child sandboxes.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::sandbox::{SandboxBackend, SandboxBackendFactory, SandboxForkSpec};
use crate::types::SandboxId;

use super::super::claim::{Claim, Fork};
use super::super::persistence::SandboxPersister;
use super::super::store::*;
use super::super::types::{SandboxForkChildSpec, SandboxLifecycleEventType, SandboxState};
use super::super::{OrchestratorError, Result, SandboxForkOutcome, SandboxOperation};

use super::Orchestrator;

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    /// Forks a running sandbox into multiple new sandboxes on the same node.
    pub async fn fork_sandbox(
        self: &Arc<Self>,
        source_sandbox_id: SandboxId,
        count: u32,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        if self
            .store
            .get(&source_sandbox_id)
            .await?
            .is_some_and(|metadata| !metadata.volume_mounts.is_empty())
        {
            return Err(OrchestratorError::InternalError(
                "fork with volume mounts requires volume-aware child specs".to_string(),
            ));
        }
        let child_specs = (0..count)
            .map(|_| SandboxForkChildSpec {
                sandbox_id: SandboxId::new(),
                ..Default::default()
            })
            .collect();
        self.fork_sandbox_with_specs(source_sandbox_id, child_specs, new_timeout)
            .await
    }

    pub async fn fork_sandbox_with_specs(
        self: &Arc<Self>,
        source_sandbox_id: SandboxId,
        child_specs: Vec<SandboxForkChildSpec>,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        let this = Arc::clone(self);
        self.run_cancellation_safe("fork", source_sandbox_id, async move {
            this.fork_sandbox_inner(source_sandbox_id, child_specs, new_timeout)
                .await
        })
        .await
    }

    #[tracing::instrument(
        name = "fork_sandbox",
        skip(self, child_specs),
        fields(source_sandbox_id = %source_sandbox_id, count = child_specs.len())
    )]

    async fn fork_sandbox_inner(
        self: Arc<Self>,
        source_sandbox_id: SandboxId,
        child_specs: Vec<SandboxForkChildSpec>,
        new_timeout: NewTimeout,
    ) -> Result<Vec<SandboxForkOutcome>> {
        self.ensure_accepting_lifecycle_operations()?;

        let count = u32::try_from(child_specs.len())
            .map_err(|_| OrchestratorError::InternalError("too many fork children".to_string()))?;
        let mut child_ids = HashSet::with_capacity(child_specs.len());
        for child in &child_specs {
            if child.sandbox_id == source_sandbox_id || !child_ids.insert(child.sandbox_id) {
                return Err(OrchestratorError::InternalError(
                    "fork child sandbox IDs must be unique and differ from the source".to_string(),
                ));
            }
            if self.store.get(&child.sandbox_id).await?.is_some() {
                return Err(OrchestratorError::InternalError(format!(
                    "fork child sandbox {} already exists",
                    child.sandbox_id
                )));
            }
        }
        info!("forking sandboxes");

        let source_handle = {
            let sandboxes = self.sandboxes.read().await;
            sandboxes.get(&source_sandbox_id).cloned()
        }
        .ok_or(OrchestratorError::SandboxNotFound(source_sandbox_id))?;

        let claim = Claim::<Fork>::acquire(self.claim_host(), source_sandbox_id)
            .await
            .map_err(|err| match err {
                StoreError::StateConflict { actual_state, .. } => match actual_state {
                    SandboxState::Killing => OrchestratorError::SandboxNotFound(source_sandbox_id),
                    _ => OrchestratorError::InvalidSandboxState {
                        sandbox_id: source_sandbox_id,
                        state: actual_state,
                    },
                },
                err => OrchestratorError::from(err),
            })?;
        let source_metadata = claim.metadata().clone();

        let backend_specs = child_specs
            .iter()
            .map(|child| SandboxForkSpec {
                sandbox_id: child.sandbox_id,
                envd_access_token: source_metadata
                    .secure
                    .then(|| self.access_tokens.generate(child.sandbox_id)),
                extra_drives: child.extra_drives.clone(),
                replace_drive_ids: child.replace_drive_ids.clone(),
            })
            .collect::<Vec<_>>();

        // Start to fork the sandbox.
        // This is a single operation that will return a list of results for each child sandbox.
        let fork_result = {
            let mut sandbox = source_handle.lock().await;
            sandbox.fork(&backend_specs).await
        };
        let forked_backends = match fork_result {
            Ok(forked_backends) => forked_backends,
            Err(err) => {
                warn!(error = ?err, "failed to fork sandbox");
                self.counters.record_create_fail(u64::from(count));
                if err.is_terminal() {
                    claim.teardown(Some(source_handle)).await?;
                } else {
                    claim.rollback().await;
                }
                return Err(OrchestratorError::SandboxOperationFailed {
                    sandbox_id: source_sandbox_id,
                    operation: SandboxOperation::Fork,
                    source: err.into(),
                });
            }
        };

        // Restore the source sandbox's state to Running.
        if let Err((claim, err)) = claim.commit(|_| {}).await {
            warn!(error = ?err, "failed to restore source sandbox metadata after fork");
            claim.rollback().await;
        }

        // Register each forked sandbox in the store and runtime, and publish events.
        let mut outcomes = Vec::with_capacity(child_specs.len());
        let mut successes = 0u64;
        let now = SystemTime::now();
        for (child, backend) in child_specs.into_iter().zip(forked_backends) {
            let sandbox_id = child.sandbox_id;
            let backend = match backend {
                Ok(backend) => backend,
                Err(err) => {
                    warn!(%sandbox_id, error = ?err, "failed to start forked sandbox");
                    outcomes.push(Err(Self::fork_child_error(sandbox_id, err)));
                    continue;
                }
            };

            let mut metadata = source_metadata.clone();
            metadata.id = sandbox_id;
            metadata.state = SandboxState::Running;
            metadata.created_at = now;
            metadata.paused_state = None;
            metadata.volume_mounts = child.volume_mounts;
            metadata.update_timeout(new_timeout);

            let proxy_target = match Self::proxy_target_from_sandbox(backend.as_ref()) {
                Ok(proxy_target) => proxy_target,
                Err(err) => {
                    Self::stop_failed_fork(backend, sandbox_id).await;
                    outcomes.push(Err(Self::fork_child_error(
                        sandbox_id,
                        anyhow::Error::new(err),
                    )));
                    continue;
                }
            };
            if let Err(err) = self.store.add(metadata.clone()).await {
                warn!(%sandbox_id, error = ?err, "failed to register forked sandbox");
                Self::stop_failed_fork(backend, sandbox_id).await;
                outcomes.push(Err(Self::fork_child_error(
                    sandbox_id,
                    anyhow::Error::new(err),
                )));
                continue;
            }
            self.sandboxes
                .write()
                .await
                .insert(metadata.id, Arc::new(Mutex::new(backend)));
            self.upsert_proxy_route(metadata.id, proxy_target).await;
            self.publish_sandbox_event(
                SandboxLifecycleEventType::Fork,
                metadata.id,
                metadata.resources,
            );
            successes += 1;
            outcomes.push(Ok(metadata));
        }

        self.counters.record_create_success(successes);
        self.counters
            .record_create_fail(u64::from(count) - successes);
        Ok(outcomes)
    }

    fn fork_child_error(sandbox_id: SandboxId, source: anyhow::Error) -> OrchestratorError {
        OrchestratorError::SandboxOperationFailed {
            sandbox_id,
            operation: SandboxOperation::Fork,
            source,
        }
    }

    async fn stop_failed_fork(mut backend: Box<dyn SandboxBackend>, sandbox_id: SandboxId) {
        if let Err(err) = backend.stop().await {
            warn!(%sandbox_id, error = ?err, "failed to stop unsuccessful fork");
        }
    }
}
