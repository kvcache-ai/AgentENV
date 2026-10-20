//! Starting a sandbox runtime for create and resume, and resolving its claim.

use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::image::cache::RuntimeImageOwner;
use crate::sandbox::{SandboxBackend, SandboxBackendFactory, SandboxRuntimeInfo};
use crate::types::{bytes_to_mib_ceil, SandboxId, SandboxResources};

use super::super::claim::{Claim, Create};
use super::super::launch_plan::{CreateLaunchSource, LaunchClaim, LaunchPlan};
use super::super::persistence::SandboxPersister;
use super::super::proxy::ProxyTarget;
use super::super::store::*;
use super::super::{OrchestratorError, Result, SandboxOperation};

use super::{Orchestrator, SandboxHandle};

fn resources_with_runtime_info(
    mut resources: SandboxResources,
    runtime_info: SandboxRuntimeInfo,
) -> SandboxResources {
    // This API resource field tracks the rootfs block device size. Attached
    // drives are separately configured storage and are not folded into it.
    if let Some(size) = runtime_info.rootfs_virtual_size {
        resources.disk_size_mib = bytes_to_mib_ceil(size);
    }
    resources
}

impl<S, F, P> Orchestrator<S, F, P>
where
    S: MetadataStore + 'static,
    F: SandboxBackendFactory,
    P: SandboxPersister + 'static,
{
    #[tracing::instrument(skip(self, plan))]
    pub(super) async fn launch_sandbox(
        self: &Arc<Self>,
        plan: LaunchPlan,
    ) -> Result<SandboxMetadata> {
        let sandbox_id = plan.sandbox_id();
        let launch_timeout = plan.timeout();

        let (handle, runtime_resources) = match self.start_launch_runtime(&plan).await {
            Ok(started) => started,
            Err(err) => {
                if let LaunchPlan::Resume(plan) = plan {
                    plan.claim.rollback().await;
                }
                return Err(err);
            }
        };

        // A create registers its metadata only once the runtime has started, so
        // a failed build or start leaves nothing persisted to roll back.
        let claim = match plan {
            LaunchPlan::Create(plan) => {
                let mut metadata = plan.metadata;
                metadata.resources = runtime_resources;
                match Claim::<Create>::acquire(self.claim_host(), metadata).await {
                    Ok(claim) => LaunchClaim::Create(claim),
                    Err(err) => {
                        warn!(error = %format_args!("{err:#}"), "failed to persist sandbox metadata; cleaning up");
                        self.abort_launch_runtime(sandbox_id, handle).await;
                        return Err(OrchestratorError::from(err));
                    }
                }
            }
            LaunchPlan::Resume(plan) => LaunchClaim::Resume(plan.claim),
        };
        let resumed = matches!(claim, LaunchClaim::Resume(_));

        let proxy_target = match self.ready_launch_runtime(sandbox_id, &handle).await {
            Ok(proxy_target) => proxy_target,
            Err(err) => {
                self.abort_launch_runtime(sandbox_id, handle).await;
                claim.rollback().await;
                return Err(err);
            }
        };

        let final_metadata = match claim
            .commit(move |metadata| {
                metadata.resources = runtime_resources;
                metadata.update_timeout(launch_timeout);
            })
            .await
        {
            Ok(metadata) => metadata,
            Err((claim, err)) => {
                warn!(error = %format_args!("{err:#}"), "failed to persist final sandbox metadata after launch");
                self.abort_launch_runtime(sandbox_id, handle).await;
                claim.rollback().await;
                return Err(OrchestratorError::from(err));
            }
        };

        if !self
            .upsert_proxy_route_if_current_handle(sandbox_id, &handle, proxy_target)
            .await
        {
            debug!("skipping runtime proxy route publication because sandbox handle is stale");
        }

        if resumed {
            if let Err(err) = self.persister.delete_record(&sandbox_id).await {
                warn!(error = %format_args!("{err:#}"), "failed to delete persisted sandbox record after resume");
            }
        }

        info!("sandbox launch completed");
        Ok(final_metadata)
    }

    /// Builds and starts the runtime, then registers its handle. On failure it
    /// releases everything it acquired; the caller resolves the launch claim.
    async fn start_launch_runtime(
        &self,
        plan: &LaunchPlan,
    ) -> Result<(SandboxHandle, SandboxResources)> {
        self.ensure_accepting_lifecycle_operations()?;

        let sandbox_id = plan.sandbox_id();
        let mut sandbox = self.build_sandbox(plan)?;

        // Protect artifacts before the backend opens them.
        let startup_artifacts = sandbox.startup_artifacts();
        if let Err(err) = self
            .protect_image_refs(
                RuntimeImageOwner::StartingSandbox(sandbox_id),
                startup_artifacts,
                "starting sandbox",
            )
            .await
        {
            warn!(error = %format_args!("{err:#}"), "failed to protect starting runtime artifacts");
            self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                .await;
            return Err(err);
        }
        if let Err(source) = sandbox.start_nowait().await {
            warn!(error = %format_args!("{source:#}"), "failed to start sandbox");
            if let Err(stop_err) = sandbox.stop().await {
                warn!(error = %format_args!("{stop_err:#}"), "failed to stop sandbox after start failure");
            }
            self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                .await;
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::Start,
                source,
            });
        }
        debug!("sandbox start requested");

        // If the orchestrator started shutting down, stop here before we persist any state.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down just after starting the sandbox");
            if let Err(err) = sandbox.stop().await {
                warn!(error = %format_args!("{err:#}"), "failed to stop sandbox");
            }
            self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
                .await;
            return Err(OrchestratorError::ShuttingDown);
        }

        let runtime_resources =
            resources_with_runtime_info(plan.resources(), sandbox.runtime_info());

        // Store the sandbox handle in memory.
        let handle = Arc::new(Mutex::new(sandbox));
        self.sandboxes
            .write()
            .await
            .insert(sandbox_id, handle.clone());

        self.release_image_refs(RuntimeImageOwner::StartingSandbox(sandbox_id))
            .await;
        Ok((handle, runtime_resources))
    }

    /// Waits for a registered runtime to become ready and returns its proxy target.
    async fn ready_launch_runtime(
        &self,
        sandbox_id: SandboxId,
        handle: &SandboxHandle,
    ) -> Result<ProxyTarget> {
        // Check for shutdown again before we wait for the sandbox to become ready.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down before sandbox became ready");
            return Err(OrchestratorError::ShuttingDown);
        }

        // Wait for the sandbox to be ready
        let wait_result = {
            let sandbox = handle.lock().await;
            sandbox.wait_for_ready().await
        };
        if let Err(source) = wait_result {
            warn!(error = %format_args!("{source:#}"), "sandbox failed to become ready");
            return Err(OrchestratorError::SandboxOperationFailed {
                sandbox_id,
                operation: SandboxOperation::WaitReady,
                source,
            });
        }

        // Check for shutdown again before we persist the final state and publish the proxy route.
        if self.is_shutting_down() {
            info!("orchestrator started shutting down while sandbox was becoming ready");
            return Err(OrchestratorError::ShuttingDown);
        }

        let sandbox = handle.lock().await;
        Self::proxy_target_from_sandbox(sandbox.as_ref()).inspect_err(|err| {
            warn!(error = %format_args!("{err:#}"), "sandbox became ready without a proxy target; rolling back launch");
        })
    }

    fn build_sandbox(&self, plan: &LaunchPlan) -> Result<Box<dyn SandboxBackend>> {
        let build_result = match plan {
            LaunchPlan::Create(plan) => match &plan.source {
                CreateLaunchSource::Snapshot { snapshot } => self
                    .factory
                    .build_from_snapshot(snapshot, plan.launch_config.clone()),
                CreateLaunchSource::Fresh { build_spec } => self
                    .factory
                    .build((**build_spec).clone(), plan.launch_config.clone()),
            },
            LaunchPlan::Resume(plan) => self.factory.build_from_paused_state(
                plan.claim.sandbox_id(),
                plan.paused_state.as_ref(),
                plan.envd_access_token.clone(),
            ),
        };
        build_result.map_err(|source| {
            warn!(error = %format_args!("{source:#}"), "failed to build sandbox");
            OrchestratorError::SandboxOperationFailed {
                sandbox_id: plan.sandbox_id(),
                operation: SandboxOperation::Build,
                source,
            }
        })
    }

    /// Stops a launched runtime, unregistering its handle unless it was replaced.
    pub(super) async fn abort_launch_runtime(&self, sandbox_id: SandboxId, handle: SandboxHandle) {
        {
            let mut sandboxes = self.sandboxes.write().await;
            match sandboxes.get(&sandbox_id) {
                Some(current) if Arc::ptr_eq(current, &handle) => {
                    sandboxes.remove(&sandbox_id);
                }
                Some(_) => {
                    warn!("sandbox handle was replaced during failed launch cleanup; keeping replacement");
                }
                None => {}
            }
        }

        let stop_result = {
            let mut sandbox = handle.lock().await;
            sandbox.stop().await
        };
        if let Err(err) = stop_result {
            warn!(error = %format_args!("{err:#}"), "failed to stop sandbox while rolling back launch");
        }
    }
}
