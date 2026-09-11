use agentenv_http_server::models;
use anyhow::{ensure, Context, Result};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, net::SocketAddr, time::Duration};
use tokio::time::Instant;
use tracing::{info, warn};

use super::{ApiImpl, BuildJournal, BuildSession, SessionState};
use crate::{
    cfg::ConfigManager,
    image::buildkit::{BuildkitHistory, BUILDKIT_PORT},
    orchestrator::{
        CreateSandboxRequest, ProxyLookupResult, SandboxLaunchSource, SandboxTimeoutAction,
    },
    sandbox::{Executor, ProcessOpts, SandboxNetworkPolicy},
    snapshot::{
        repository::backends::build_builder_snapshot_backend, CommandContext, RepositoryError,
        RunnableSnapshot, SnapshotManager, SnapshotRecord,
    },
    template::TemplateBuildSpec,
    types::{ImageConfigs, SandboxId},
};

const BUILDER_READY: &str =
    "command -v buildkitd && command -v buildctl && mkdir -p /var/lib/buildkit /run/aenv-buildkit";

impl ApiImpl {
    /// Apply one solve deadline and cancellation boundary across all startup phases.
    pub(super) async fn wait_for_image_build(
        &self,
        record: &SnapshotRecord,
        body: &models::TemplateBuilderRequest,
        session: &BuildSession,
        entry: &BuildJournal,
        deadline: Instant,
    ) -> Result<(SocketAddr, String)> {
        let mut cancellation = session.state.subscribe();
        let solve = async {
            let snapshot = self.builder_template().await?;
            let id = record.id.to_string();
            // Finish attaching the cache even if the caller cancels. Cleanup waits
            // on the same lock before deleting the worker or releasing its volumes.
            let cleanup = session.cleanup.clone().lock_owned().await;
            let api = self.clone();
            let body = body.clone();
            let mut entry = entry.clone();
            let (address, executor) = tokio::spawn(async move {
                let _cleanup = cleanup;
                api.prepare_builder(&id, &body, &mut entry, snapshot).await
            })
            .await??;
            worker_command(&executor, START_BUILDKIT, 90).await?;
            let history = BuildkitHistory::connect(address).await?;
            ensure!(session.ready(address), "build cancelled");
            let digest = history.wait_for_image(&record.id.to_string()).await?;
            session
                .publish()
                .map_err(|error| anyhow::anyhow!(error.message))?;
            Ok((address, digest))
        };
        tokio::select! {
            biased;
            _ = cancellation.wait_for(|state| matches!(state, SessionState::Cancelled)) => {
                anyhow::bail!("build cancelled");
            }
            result = tokio::time::timeout_at(deadline, solve) => {
                result.context("Dockerfile build deadline exceeded")?
            }
        }
    }

    pub(super) async fn builder_template(&self) -> Result<RunnableSnapshot> {
        let api = self.clone();
        // A cancelled request must not cancel initialization shared by other builds.
        tokio::spawn(async move {
            api.build_sessions
                .builder_template
                .get_or_try_init(|| api.initialize_builder_template())
                .await
                .cloned()
        })
        .await?
    }

    async fn initialize_builder_template(&self) -> Result<RunnableSnapshot> {
        let config = ConfigManager::global_config();
        let builder = &config.template_build;
        let inputs = serde_json::to_vec(&(
            1,
            &builder.builder_image,
            builder.builder_cpu_count,
            builder.builder_memory_mb,
            BUILDER_READY,
            config.virtualization_mode,
        ))?;
        let alias = format!("builder-{:x}", Sha256::digest(inputs));
        let (repository, resolver) = build_builder_snapshot_backend()?;
        let manager = SnapshotManager::from_parts(repository, resolver, None);
        if let Some(snapshot) = manager.load_runnable(&alias).await? {
            info!(snapshot_id = %snapshot.record().id, "reusing prepared Dockerfile builder template");
            return Ok(snapshot);
        }

        info!("preparing Dockerfile builder template for the first build");
        let resolved = self.image_resolver.resolve(&builder.builder_image).await?;
        let mut image_configs = ImageConfigs::new();
        if let Some(config) = resolved.raw_config {
            image_configs.add(None::<String>, "/", config);
        }
        let context = CommandContext::from_env_and_workdir(
            resolved.base_context.env_vars,
            Some("/".to_owned()),
        )
        .with_user(Some("root".to_owned()));
        let spec = TemplateBuildSpec::new()
            .alias(&alias)
            .resources(builder.builder_cpu_count, builder.builder_memory_mb)
            .with_startup_shell("/bin/sh")
            .with_resolved_overlaybd_image(resolved.overlaybd_config_path, image_configs)
            .with_base_context(context)
            .ready_cmd(BUILDER_READY);
        let result = self
            .template_builder
            .build_and_publish(&manager, spec)
            .await;
        // Two nodes may prepare the first builder simultaneously. If another
        // node published the alias first, reuse its committed snapshot.
        if let Err(error) = result {
            let error = anyhow::Error::new(error);
            if !error.chain().any(|cause| {
                matches!(
                    cause.downcast_ref::<RepositoryError>(),
                    Some(RepositoryError::AliasConflict { .. })
                )
            }) {
                return Err(error);
            }
        }
        manager
            .load_runnable(&alias)
            .await?
            .context("prepared builder template is missing")
    }
    async fn prepare_builder(
        &self,
        id: &str,
        body: &models::TemplateBuilderRequest,
        entry: &mut BuildJournal,
        snapshot: RunnableSnapshot,
    ) -> Result<(SocketAddr, Executor)> {
        let volume = self.fork_build_cache(id, entry).await?;
        let (drives, mounts) = super::super::volumes::resolve_volume_mounts(
            &self.volume_manager,
            &HashMap::from([("/var/lib/buildkit".to_owned(), volume.id)]),
            id,
        )
        .await
        .map_err(|error| anyhow::anyhow!("{}", error.message))?;
        let network_policy = SandboxNetworkPolicy {
            allow_public_traffic: false,
            ..Default::default()
        };
        let metadata = self
            .orchestrator
            .create_template_builder(
                SandboxId::parse_str(id)?,
                CreateSandboxRequest {
                    source: SandboxLaunchSource::Snapshot(Box::new(snapshot)),
                    extra_drives: drives,
                    extra_drives_in_snapshot: false,
                    timeout: Some(Duration::from_secs(
                        u64::from(body.timeout.unwrap_or(3600)) + 3900,
                    )),
                    timeout_action: SandboxTimeoutAction::Delete,
                    auto_resume: false,
                    user_metadata: None,
                    env_vars: None,
                    network_policy,
                    secure: true,
                    custom_extension_params: None,
                    volume_mounts: mounts,
                },
            )
            .await?;
        let ProxyLookupResult::Ready(target) =
            self.orchestrator.proxy_lookup_for(&metadata.id).await?
        else {
            anyhow::bail!("builder has no route")
        };
        let executor = Executor::for_endpoint(
            format!(
                "http://{}:{}",
                target.ip,
                ConfigManager::global_config().tools.control_plane_port
            ),
            self.orchestrator.get_envd_access_token(&metadata),
        );
        Ok((SocketAddr::new(target.ip.into(), BUILDKIT_PORT), executor))
    }

    pub(super) async fn release_builder(&self, id: &str, cache: &str) -> Result<bool> {
        let sandbox_id = SandboxId::parse_str(id)?;
        let mut cache_ready = false;
        if let Some(metadata) = self.orchestrator.get_sandbox(&sandbox_id).await? {
            if let ProxyLookupResult::Ready(target) =
                self.orchestrator.proxy_lookup_for(&sandbox_id).await?
            {
                let executor = Executor::for_endpoint(
                    format!(
                        "http://{}:{}",
                        target.ip,
                        ConfigManager::global_config().tools.control_plane_port
                    ),
                    self.orchestrator.get_envd_access_token(&metadata),
                );
                let started = std::time::Instant::now();
                match worker_command(&executor, STOP_BUILDKIT, 40).await {
                    Ok(()) => {
                        cache_ready = true;
                        info!(build_id = %id, elapsed_ms = started.elapsed().as_millis(), "BuildKit daemon stopped");
                    }
                    Err(error) => {
                        warn!(build_id = %id, %error, "BuildKit shutdown failed; keeping the previous cache seed");
                    }
                }
            }
            self.orchestrator.delete_sandbox(sandbox_id).await?;
            cache_ready &=
                self.volume_manager.get(cache).await?.status == crate::volume::VolumeStatus::Ready;
        }
        self.release_cache_lease(id, cache).await?;
        Ok(cache_ready)
    }
}

async fn worker_command(executor: &Executor, script: &str, seconds: u64) -> Result<()> {
    let timeout = Duration::from_secs(seconds);
    let output = tokio::time::timeout(
        timeout,
        executor.run_command_with_opts(
            "/bin/sh",
            &["-c", script],
            &ProcessOpts::default().with_timeout(timeout),
        ),
    )
    .await
    .context("builder command timed out")??;
    ensure!(
        output.exit_code == 0,
        "builder command failed: {}",
        output.stderr
    );
    Ok(())
}

const START_BUILDKIT: &str = r#"
set -eu
mkdir -p /run/aenv-buildkit
nohup buildkitd --root /var/lib/buildkit --addr tcp://0.0.0.0:1234 \
  --oci-worker=true --containerd-worker=false --oci-worker-net host \
  >/run/aenv-buildkit/log 2>&1 </dev/null &
echo $! >/run/aenv-buildkit/pid
for attempt in $(seq 1 60); do
  if buildctl --addr tcp://127.0.0.1:1234 debug workers >/dev/null 2>&1; then exit 0; fi
  kill -0 $(cat /run/aenv-buildkit/pid) 2>/dev/null || break
  sleep 1
done
cat /run/aenv-buildkit/log >&2
exit 1
"#;

const STOP_BUILDKIT: &str = r#"
set -eu
if test -f /run/aenv-buildkit/pid; then
  pid=$(cat /run/aenv-buildkit/pid)
  kill -TERM "$pid" 2>/dev/null || true
  for attempt in $(seq 1 300); do
    if ! kill -0 "$pid" 2>/dev/null || grep -q 'State:.*Z' "/proc/$pid/status"; then break; fi
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null && ! grep -q 'State:.*Z' "/proc/$pid/status"; then exit 1; fi
fi
"#;
