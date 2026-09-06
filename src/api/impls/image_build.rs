use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use agentenv_http_server::models;
use anyhow::{ensure, Context, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use dashmap::DashMap;
use futures::{FutureExt, SinkExt, StreamExt};
use http::StatusCode;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{oneshot, watch, Mutex, OnceCell},
    time::Instant,
};
use tracing::{debug, info, warn};

mod cache;
mod cleanup;
mod worker;

use super::{template_helpers::template_build_record_from_v3_request, ApiImpl};
use crate::{
    cfg::ConfigManager,
    image::buildkit::{validate_digest, BuildkitContent, BUILDKIT_PORT},
    local_store::{LocalKvStore, LocalStoreDurability},
    orchestrator::{
        CreateSandboxRequest, ProxyLookupResult, SandboxLaunchSource, SandboxTimeoutAction,
    },
    sandbox::{Executor, ProcessOpts, SandboxNetworkPolicy},
    snapshot::{
        CommandContext, RunnableSnapshot, SnapshotId, SnapshotRecord, TemplateBuildErrorReason,
    },
    template::TemplateBuildSpec,
    types::{ImageConfigs, SandboxId},
};

#[derive(Default)]
pub(crate) struct BuildSessions {
    active: DashMap<String, BuildSession>,
    journal: OnceCell<LocalKvStore>,
    builder_template: OnceCell<RunnableSnapshot>,
}

impl BuildSessions {
    pub(super) fn contains(&self, id: &str) -> bool {
        self.active.contains_key(id)
    }

    pub(super) fn is_finishing(&self, id: &str) -> bool {
        self.active
            .get(id)
            .is_some_and(|session| matches!(*session.state.borrow(), SessionState::Submitted(_)))
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct BuildJournal {
    cache: String,
    parent: Option<String>,
}

impl BuildJournal {
    async fn persist(&self, journal: &LocalKvStore, id: &str) -> Result<()> {
        journal
            .put(format!("build/{id}"), serde_json::to_vec(self)?)
            .await
    }
}

#[derive(Clone)]
struct BuildSession {
    state: watch::Sender<SessionState>,
    cleanup: Arc<Mutex<()>>,
}

#[derive(Clone)]
enum SessionState {
    Starting,
    Ready(SocketAddr),
    Submitted(String),
    Cancelled,
    Finished(Option<TemplateBuildErrorReason>),
}

impl BuildSession {
    fn new() -> Self {
        Self {
            state: watch::channel(SessionState::Starting).0,
            cleanup: Arc::new(Mutex::new(())),
        }
    }

    fn ready(&self, address: SocketAddr) -> bool {
        self.state.send_if_modified(|state| {
            if !matches!(state, SessionState::Starting) {
                return false;
            }
            *state = SessionState::Ready(address);
            true
        })
    }

    fn submit(&self, digest: &str) -> Result<(), models::Error> {
        let accepted = self.state.send_if_modified(|state| {
            if !matches!(state, SessionState::Ready(_)) {
                return false;
            }
            *state = SessionState::Submitted(digest.to_owned());
            true
        });
        if !accepted {
            return Err(ApiImpl::error(
                409,
                "builder is not ready or build was already submitted or cancelled",
            ));
        }
        Ok(())
    }

    fn request_cancel(&self) -> Result<(), models::Error> {
        let mut submitted = false;
        self.state.send_if_modified(|state| {
            submitted = matches!(state, SessionState::Submitted(_));
            if !matches!(state, SessionState::Starting | SessionState::Ready(_)) {
                return false;
            }
            *state = SessionState::Cancelled;
            true
        });
        if submitted {
            return Err(ApiImpl::error(
                409,
                "publication already started; the server will finish it and release the builder",
            ));
        }
        Ok(())
    }
}

impl ApiImpl {
    async fn build_journal(&self) -> Result<&LocalKvStore> {
        self.build_sessions
            .journal
            .get_or_try_init(|| {
                LocalKvStore::open(
                    ConfigManager::global_config()
                        .home_path
                        .join("template-builds"),
                    LocalStoreDurability::Sync,
                )
            })
            .await
    }

    pub(super) async fn start_image_build(
        &self,
        body: &models::TemplateBuildSessionRequest,
    ) -> Result<models::TemplateRequestResponseV3, models::Error> {
        let api = self.clone();
        let body = body.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = api.allocate_image_build(body).await;
            // Complete allocation durably, then cancel if its request disappeared.
            if let Err(Ok(response)) = sender.send(result) {
                if let Err(error) = api
                    .cancel_image_build(&response.template_id, &response.build_id)
                    .await
                {
                    warn!(build_id = %response.build_id, error = %error.message, "disconnected build cleanup will be retried");
                }
            }
        });
        receiver
            .await
            .map_err(|error| Self::error(500, error.to_string()))?
    }

    async fn allocate_image_build(
        &self,
        body: models::TemplateBuildSessionRequest,
    ) -> Result<models::TemplateRequestResponseV3, models::Error> {
        let name = body
            .template
            .name
            .as_deref()
            .ok_or_else(|| Self::error(400, "template name must be provided"))?
            .to_owned();
        if ConfigManager::global_config().template_build.cache_size_mb
            > self.volume_manager.limits().max_size_mb
        {
            return Err(Self::error(
                400,
                "template_build.cache_size_mb exceeds volume.max_size_mb",
            ));
        }
        let id = SnapshotId::generate();
        let record = template_build_record_from_v3_request(&body.template, id.clone(), &name)?;
        let entry = BuildJournal {
            cache: format!("aenv-buildkit-work-{id}"),
            parent: None,
        };
        let journal = self
            .build_journal()
            .await
            .map_err(|err| Self::internal_error(err.as_ref()))?;
        let key = format!("build/{id}");
        // Recovery must see the live session before its journal entry becomes durable.
        let session = BuildSession::new();
        self.build_sessions
            .active
            .insert(id.to_string(), session.clone());
        if let Err(err) = entry.persist(journal, &id.to_string()).await {
            self.build_sessions.active.remove(&id.to_string());
            return Err(Self::internal_error(err.as_ref()));
        }
        if let Err(err) = self.snapshot_manager.create(record.clone()).await {
            let _ = journal.delete(key.into_bytes()).await;
            self.build_sessions.active.remove(&id.to_string());
            return Err(Self::repository_error(&err));
        }
        self.orchestrator
            .register_template_build(
                SandboxId::parse_str(&id.to_string()).expect("build ID is a UUID"),
            )
            .await;
        let api = self.clone();
        tokio::spawn(async move {
            api.run_image_build(record, body, session, entry).await;
        });
        Ok(models::TemplateRequestResponseV3::new(
            id.to_string(),
            id.to_string(),
            true,
            vec![name.clone()],
            vec![name],
            vec![],
        ))
    }

    fn session(&self, template_id: &str, build_id: &str) -> Result<BuildSession, models::Error> {
        if template_id != build_id {
            return Err(Self::error(404, "template build not found"));
        }
        self.build_sessions
            .active
            .get(build_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| Self::error(404, "active template build not found"))
    }

    pub(super) fn submit_image_build(
        &self,
        template_id: &str,
        build_id: &str,
        digest: &str,
    ) -> Result<(), models::Error> {
        validate_digest(digest).map_err(|err| Self::error(400, err.to_string()))?;
        let session = self.session(template_id, build_id)?;
        session.submit(digest)
    }

    pub(super) async fn cancel_image_build(
        &self,
        template_id: &str,
        build_id: &str,
    ) -> Result<(), models::Error> {
        if template_id != build_id {
            return Err(Self::error(404, "template build not found"));
        }
        let session = match self.session(template_id, build_id) {
            Ok(session) => session,
            Err(_) => {
                self.snapshot_manager
                    .get(build_id)
                    .await
                    .map_err(|err| Self::snapshot_manager_error(&err))?
                    .ok_or_else(|| Self::error(404, "template build not found"))?;
                return self
                    .retry_image_build_cleanup(build_id)
                    .await
                    .map_err(|err| Self::error(500, format!("builder cleanup failed: {err:#}")));
            }
        };
        let mut state = session.state.subscribe();
        session.request_cancel()?;
        tokio::time::timeout(Duration::from_secs(60), async {
            state
                .wait_for(|state| matches!(state, SessionState::Finished(_)))
                .await
                .map_err(|_| Self::error(500, "builder cleanup stopped unexpectedly"))?;
            self.retry_image_build_cleanup(build_id)
                .await
                .map_err(|err| Self::error(500, format!("builder cleanup failed: {err:#}")))
        })
        .await
        .map_err(|_| {
            Self::error(
                500,
                "builder cleanup is still running; retry cancellation later",
            )
        })?
    }

    async fn run_image_build(
        &self,
        record: SnapshotRecord,
        body: models::TemplateBuildSessionRequest,
        session: BuildSession,
        entry: BuildJournal,
    ) {
        let id = record.id.to_string();
        let deadline = Instant::now() + Duration::from_secs(body.timeout.unwrap_or(3600).into());
        info!(build_id = %id, "template build starting");
        let work = async {
            let mut state = session.state.subscribe();
            let snapshot = tokio::select! {
                biased;
                _ = state.wait_for(|state| matches!(state, SessionState::Cancelled)) => {
                    anyhow::bail!("build cancelled while preparing the builder template");
                }
                snapshot = tokio::time::timeout_at(deadline, self.builder_template()) => {
                    snapshot.context("builder template preparation deadline exceeded")??
                }
            };
            info!(build_id = %id, cache = %entry.cache, "template builder starting");
            // Creation finishes under the cleanup lock even if its caller times
            // out. Finalization must not delete volumes that are still attaching.
            let cleanup = session.cleanup.clone().lock_owned().await;
            let api = self.clone();
            let prepare_id = id.clone();
            let prepare_body = body.clone();
            let mut prepare_entry = entry.clone();
            let preparation = tokio::spawn(async move {
                let _cleanup = cleanup;
                api.prepare_builder(&prepare_id, &prepare_body, &mut prepare_entry, snapshot)
                    .await
            });
            let (address, executor) = tokio::select! {
                biased;
                _ = state.wait_for(|state| matches!(state, SessionState::Cancelled)) => anyhow::bail!("build cancelled"),
                result = tokio::time::timeout_at(deadline, preparation) => result.context("builder preparation deadline exceeded")???
            };
            tokio::select! {
                biased;
                _ = state.wait_for(|state| matches!(state, SessionState::Cancelled)) => anyhow::bail!("build cancelled"),
                result = tokio::time::timeout_at(deadline, worker_command(&executor, START_BUILDKIT, 90)) => result.context("BuildKit startup deadline exceeded")??
            }
            ensure!(session.ready(address), "build cancelled");
            // Existing template status becomes Building only when the worker can accept a solve.
            self.snapshot_manager.try_start_build(&record.id).await?;
            let command = tokio::time::timeout_at(
                deadline,
                state.wait_for(|state| {
                    matches!(state, SessionState::Submitted(_) | SessionState::Cancelled)
                }),
            )
            .await
            .context("Dockerfile build deadline exceeded")??
            .clone();
            let SessionState::Submitted(digest) = command else {
                anyhow::bail!("build cancelled");
            };
            let content = BuildkitContent::connect(address).await?;
            let resolved = tokio::time::timeout(
                Duration::from_secs(3600),
                self.image_resolver.resolve_buildkit(&content, &digest),
            )
            .await
            .context("image import deadline exceeded")??;
            let cache_ready = self.release_builder(&id, &entry.cache).await?;
            let context = CommandContext::from(resolved.base_context);
            let (start, ready) =
                build_startup_commands(&body, &context, resolved.raw_config.as_ref())?;
            let mut configs = ImageConfigs::new();
            if let Some(config) = resolved.raw_config {
                configs.add(None::<String>, "/", config);
            }
            let mut spec = TemplateBuildSpec::new()
                .alias(
                    record
                        .alias
                        .as_ref()
                        .context("template name missing")?
                        .to_string(),
                )
                .resources(record.resources.cpu_count, record.resources.memory_mib)
                .with_startup_shell("/bin/sh")
                .with_resolved_overlaybd_image(resolved.overlaybd_config_path, configs)
                .with_base_context(context);
            if let Some(start) = start {
                spec = spec.start_cmd(start);
            }
            if let Some(ready) = ready {
                spec = spec.ready_cmd(ready);
            }
            self.template_builder
                .build_and_publish_with_id(self.snapshot_manager.as_ref(), record.id.clone(), spec)
                .await?;
            if cache_ready {
                if let Err(error) = self.publish_build_cache(&id, &entry.cache).await {
                    warn!(build_id = %id, error = %format_args!("{error:#}"), "cache publication failed; keeping the previous cache seed");
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        self.supervise_image_build(&record, &session, work).await;
    }

    async fn supervise_image_build(
        &self,
        record: &SnapshotRecord,
        session: &BuildSession,
        work: impl std::future::Future<Output = Result<()>>,
    ) {
        let result = std::panic::AssertUnwindSafe(work)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("build worker panicked")));
        self.finish_image_build(record, session, result).await;
    }

    async fn finish_image_build(
        &self,
        record: &SnapshotRecord,
        session: &BuildSession,
        result: Result<()>,
    ) {
        let id = record.id.to_string();
        let reason = match result {
            Ok(()) => {
                info!(build_id = %id, "template build completed");
                None
            }
            Err(error) => {
                warn!(build_id = %id, error = %format_args!("{error:#}"), "template build failed");
                Some(TemplateBuildErrorReason::new(format!("{error:#}")))
            }
        };
        session.state.send_replace(SessionState::Finished(reason));
        if let Err(error) = self.retry_image_build_cleanup(&id).await {
            warn!(build_id = %id, error = %format_args!("{error:#}"), "build finalization failed; cleanup will be retried");
        }
    }

    async fn prepare_builder(
        &self,
        id: &str,
        body: &models::TemplateBuildSessionRequest,
        entry: &mut BuildJournal,
        snapshot: RunnableSnapshot,
    ) -> Result<(SocketAddr, Executor)> {
        let volume = self.fork_build_cache(id, entry).await?;
        let (drives, mounts) = super::volumes::resolve_volume_mounts(
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

    async fn release_builder(&self, id: &str, cache: &str) -> Result<bool> {
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
            cache_ready &= self
                .orchestrator
                .delete_sandbox_with_volume_status(sandbox_id)
                .await?;
        }
        self.release_cache_lease(id, cache).await?;
        Ok(cache_ready)
    }
}

fn build_startup_commands(
    request: &models::TemplateBuildSessionRequest,
    context: &CommandContext,
    image_config: Option<&serde_json::Value>,
) -> Result<(Option<String>, Option<String>)> {
    let start = request
        .start_cmd
        .clone()
        .or_else(|| context.effective_start_cmd());
    let ready = match &request.ready_cmd {
        Some(command) => Some(command.clone()),
        None => dockerfile_ready_command(image_config)?,
    };
    Ok((start, ready))
}

fn dockerfile_ready_command(config: Option<&serde_json::Value>) -> Result<Option<String>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let Some(test) = config.pointer("/Healthcheck/Test") else {
        return Ok(None);
    };
    let test: Vec<String> =
        serde_json::from_value(test.clone()).context("parse Dockerfile HEALTHCHECK")?;
    let command = match test.as_slice() {
        [] => return Ok(None),
        [mode] if mode == "NONE" => return Ok(None),
        [mode, command] if mode == "CMD-SHELL" => {
            let mut shell: Vec<String> = match config.get("Shell") {
                Some(value) => {
                    serde_json::from_value(value.clone()).context("parse Dockerfile SHELL")?
                }
                None => vec!["/bin/sh".into(), "-c".into()],
            };
            ensure!(!shell.is_empty(), "Dockerfile SHELL must not be empty");
            shell.push(command.clone());
            shell
        }
        [mode, args @ ..] if mode == "CMD" && !args.is_empty() => args.to_vec(),
        _ => anyhow::bail!("invalid Dockerfile HEALTHCHECK command"),
    };
    Ok(Some(
        command
            .iter()
            .map(|arg| shell_util::shell_quote(arg))
            .collect::<Vec<_>>()
            .join(" "),
    ))
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

pub(crate) fn router<I: AsRef<ApiImpl> + Clone + Send + Sync + 'static>(state: I) -> Router {
    Router::new()
        .route(
            "/templates/{template_id}/builds/{build_id}/builder",
            get(connect::<I>),
        )
        .with_state(state)
}

async fn connect<I: AsRef<ApiImpl>>(
    State(state): State<I>,
    Path((template_id, build_id)): Path<(String, String)>,
    ws: WebSocketUpgrade,
) -> Response {
    let result = async {
        let session = state.as_ref().session(&template_id, &build_id)?;
        let SessionState::Ready(address) = *session.state.borrow() else {
            return Err(ApiImpl::error(409, "builder is not ready"));
        };
        tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(address))
            .await
            .map_err(|_| ApiImpl::error(504, "builder connection timed out"))?
            .map_err(|error| ApiImpl::error(502, format!("builder connection failed: {error}")))
    }
    .await;
    match result {
        Ok(stream) => ws
            .max_message_size(1024 * 1024)
            .max_frame_size(1024 * 1024)
            .on_upgrade(move |socket| async move {
                if let Err(error) = bridge(socket, stream).await {
                    debug!(%build_id, %error, "BuildKit connection closed");
                }
            }),
        Err(error) => (
            StatusCode::from_u16(error.code as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(error),
        )
            .into_response(),
    }
}

async fn bridge(socket: WebSocket, stream: TcpStream) -> Result<()> {
    stream.set_nodelay(true)?;
    let (mut sender, mut receiver) = socket.split();
    let (mut read, mut write) = stream.into_split();
    let upstream = async {
        while let Some(message) = receiver.next().await {
            match message? {
                Message::Binary(bytes) => write.write_all(&bytes).await?,
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Text(_) => anyhow::bail!("expected binary BuildKit stream"),
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    let downstream = async {
        let mut buffer = vec![0u8; 64 * 1024];
        let mut ping = tokio::time::interval(Duration::from_secs(20));
        loop {
            tokio::select! {
                n = read.read(&mut buffer) => { let n = n?; if n == 0 { break; } sender.send(Message::Binary(buffer[..n].to_vec().into())).await?; }
                _ = ping.tick() => sender.send(Message::Ping(Vec::new().into())).await?,
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! { result = upstream => result, result = downstream => result }
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
# Freeze only the attached cache, never the builder root filesystem. Keep it
# frozen until Firecracker exits so journal recovery is unnecessary next time.
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{SnapshotSource, TemplateBuildStatus};
    use crate::volume::{VolumeLimits, VolumeMode, VolumeRecord, VolumeStatus};

    async fn test_api(
        limits: VolumeLimits,
    ) -> Result<(tempfile::TempDir, ApiImpl, SnapshotRecord)> {
        use crate::{
            api_key::ApiKey,
            cfg::AppConfig,
            image::ImageResolver,
            orchestrator::{FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator},
            sandbox::FirecrackerSandboxFactory,
            snapshot::{
                mock::write_mock_built_artifacts,
                repository::backends::{PosixFsBackend, PosixFsBackendConfig},
                SnapshotManager, SnapshotPublishMetadata,
            },
            template::TemplateBuilder,
            volume::VolumeManager,
        };

        let root = tempfile::tempdir()?;
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.path().join("repository"),
            cache_root: Some(root.path().join("cache")),
            runtime_cache_root: None,
        })?;
        let manager = Arc::new(SnapshotManager::from_parts(
            backend.repository(),
            backend.runtime_resolver(),
            None,
        ));
        let (_, _, manifest) = write_mock_built_artifacts(&root.path().join("artifacts"))?;
        let mut metadata = SnapshotPublishMetadata::mock();
        metadata.alias = Some(crate::snapshot::SnapshotAlias::parse("test-template")?);
        let record = manager.publish(metadata, manifest).await?;
        let orchestrator = Orchestrator::new(
            InMemoryMetadataStore::new(),
            FirecrackerSandboxFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().join("sandboxes")),
        )
        .await?;
        let volumes = VolumeManager::open_with_repository_and_limits(
            root.path().join("volumes/catalog.json"),
            backend.repository(),
            limits,
        )
        .await?;
        let api = ApiImpl::new(
            orchestrator,
            manager.clone(),
            Arc::new(TemplateBuilder::new()),
            Arc::new(ImageResolver::new(&AppConfig::default())),
            Arc::new(volumes),
            None,
            Vec::new(),
            ApiKey::new("build-cleanup-test-api-key-0123456789")?,
        );
        let journal =
            LocalKvStore::open(root.path().join("journal"), LocalStoreDurability::Memory).await?;
        api.build_sessions.journal.set(journal).unwrap();
        Ok((root, api, record))
    }

    async fn cache_volume(
        api: &ApiImpl,
        name: &str,
        mode: VolumeMode,
        owner: &str,
    ) -> Result<String> {
        let id = format!("vol_{}", uuid::Uuid::now_v7().simple());
        api.snapshot_manager
            .repository()
            .create_volume(VolumeRecord {
                id: id.clone(),
                name: name.into(),
                mode,
                size_mb: 1024,
                status: VolumeStatus::Ready,
                reserved_by_sandbox_id: (mode == VolumeMode::Exclusive).then(|| owner.into()),
                backing_image_config: None,
                backing_layers: Vec::new(),
                read_only_mounts: if mode == VolumeMode::ReadOnly {
                    vec![owner.into()]
                } else {
                    Vec::new()
                },
                deleting: false,
            })
            .await?;
        Ok(id)
    }

    #[tokio::test]
    async fn buildkit_cleanup_retries_preserve_status_and_release_all_resources() -> Result<()> {
        for (cancel_retry, succeeded) in
            [(false, true), (true, true), (false, false), (true, false)]
        {
            let (root, api, mut record) = test_api(VolumeLimits::default()).await?;
            if !succeeded {
                record = SnapshotRecord::template_waiting(
                    SnapshotId::generate(),
                    None,
                    record.resources,
                );
                api.snapshot_manager.create(record.clone()).await?;
            }
            let id = record.id.to_string();
            let key = format!("build/{id}").into_bytes();
            let entry = BuildJournal {
                cache: cache_volume(&api, "work", VolumeMode::Exclusive, &id).await?,
                parent: Some(cache_volume(&api, "parent", VolumeMode::ReadOnly, &id).await?),
            };
            entry.persist(api.build_journal().await?, &id).await?;
            let session = BuildSession::new();
            session
                .state
                .send_replace(SessionState::Submitted("sha256:result".into()));
            api.build_sessions
                .active
                .insert(id.clone(), session.clone());
            api.orchestrator
                .register_template_build(SandboxId::parse_str(&id)?)
                .await;

            // An unreadable cache-head record fails cleanup after worker and parent leases are released.
            let fault = root
                .path()
                .join("repository/template-build/cache-head.json");
            tokio::fs::create_dir_all(&fault).await?;
            api.finish_image_build(
                &record,
                &session,
                if succeeded {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("original build failure"))
                },
            )
            .await;

            let saved = api.snapshot_manager.get(&id).await?.unwrap();
            let expected_status = if succeeded {
                TemplateBuildStatus::Ready
            } else {
                TemplateBuildStatus::Error
            };
            assert_eq!(
                super::super::template::template_build_status(&saved),
                expected_status
            );
            assert!(api.build_journal().await?.get(key.clone()).await?.is_some());
            assert!(matches!(*session.state.borrow(), SessionState::Finished(_)));
            assert!(api.build_sessions.active.contains_key(&id));
            assert!(api
                .volume_manager
                .get(&entry.cache)
                .await?
                .reserved_by_sandbox_id
                .is_none());
            assert!(api
                .volume_manager
                .get(entry.parent.as_ref().unwrap())
                .await?
                .read_only_mounts
                .is_empty());

            assert!(api.cancel_image_build(&id, &id).await.is_err());
            tokio::fs::remove_dir(&fault).await?;
            if cancel_retry {
                api.cancel_image_build(&id, &id)
                    .await
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            } else {
                api.recover_image_builds().await?;
            }
            assert!(api.build_journal().await?.get(key).await?.is_none());
            assert!(!api.build_sessions.active.contains_key(&id));
            assert!(api
                .volume_manager
                .list_page(None, 100)
                .await?
                .records
                .is_empty());
            assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
            let saved = api.snapshot_manager.get(&id).await?.unwrap();
            assert_eq!(
                super::super::template::template_build_status(&saved),
                expected_status
            );
            let SnapshotSource::Template { build } = saved.source else {
                panic!("expected template")
            };
            assert_eq!(
                build
                    .error_reason
                    .as_ref()
                    .map(|reason| reason.message.as_str()),
                (!succeeded).then_some("original build failure")
            );
            api.cancel_image_build(&id, &id)
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn buildkit_recovery_isolates_bad_entries_and_skips_active_builds() -> Result<()> {
        let (_root, api, record) = test_api(VolumeLimits::default()).await?;
        let journal = api.build_journal().await?;
        let bad_key = b"build/00000000-0000-0000-0000-000000000000".to_vec();
        journal
            .put(bad_key.clone(), b"invalid JSON".to_vec())
            .await?;
        journal.put(b"build/\xff".to_vec(), b"{}".to_vec()).await?;
        let entry = BuildJournal {
            cache: "missing-cache".into(),
            parent: None,
        };
        let live_id = SnapshotId::generate().to_string();
        let live = BuildSession::new();
        api.build_sessions
            .active
            .insert(live_id.clone(), live.clone());
        entry.persist(journal, &live_id).await?;
        entry.persist(journal, &record.id.to_string()).await?;
        let request = serde_json::from_value(serde_json::json!({"name": "interrupted"}))?;
        let interrupted =
            template_build_record_from_v3_request(&request, SnapshotId::generate(), "interrupted")
                .unwrap();
        api.snapshot_manager.create(interrupted.clone()).await?;
        entry.persist(journal, &interrupted.id.to_string()).await?;

        let (first, second) = tokio::join!(api.recover_image_builds(), api.recover_image_builds());
        first?;
        second?;
        assert!(journal.get(bad_key).await?.is_some());
        assert!(journal.get(format!("build/{live_id}")).await?.is_some());
        assert!(matches!(*live.state.borrow(), SessionState::Starting));
        assert!(journal.get(format!("build/{}", record.id)).await?.is_none());
        assert!(!api
            .build_sessions
            .active
            .contains_key(&record.id.to_string()));
        assert!(journal
            .get(format!("build/{}", interrupted.id))
            .await?
            .is_none());
        let saved = api
            .snapshot_manager
            .get(interrupted.id.to_string())
            .await?
            .unwrap();
        assert_eq!(
            super::super::template::template_build_status(&saved),
            TemplateBuildStatus::Error
        );
        Ok(())
    }

    #[tokio::test]
    async fn buildkit_worker_panic_releases_journal_and_scheduler_binding() -> Result<()> {
        let (_root, api, record) = test_api(VolumeLimits::default()).await?;
        let record =
            SnapshotRecord::template_waiting(SnapshotId::generate(), None, record.resources);
        api.snapshot_manager.create(record.clone()).await?;
        let id = record.id.to_string();
        let entry = BuildJournal {
            cache: cache_volume(&api, "work", VolumeMode::Exclusive, &id).await?,
            parent: None,
        };
        entry.persist(api.build_journal().await?, &id).await?;
        let session = BuildSession::new();
        api.build_sessions
            .active
            .insert(id.clone(), session.clone());
        api.orchestrator
            .register_template_build(SandboxId::parse_str(&id)?)
            .await;
        api.supervise_image_build(&record, &session, async {
            panic!("injected worker panic");
        })
        .await;
        assert!(
            matches!(&*session.state.borrow(), SessionState::Finished(Some(reason)) if reason.message == "build worker panicked")
        );
        assert!(!api.build_sessions.active.contains_key(&id));
        assert!(api
            .build_journal()
            .await?
            .get(format!("build/{id}"))
            .await?
            .is_none());
        assert!(api
            .volume_manager
            .list_page(None, 100)
            .await?
            .records
            .is_empty());
        assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
        let saved = api.snapshot_manager.get(&id).await?.unwrap();
        assert_eq!(
            super::super::template::template_build_status(&saved),
            TemplateBuildStatus::Error
        );
        Ok(())
    }

    #[tokio::test]
    async fn active_build_rejects_template_deletion_by_id_and_alias() -> Result<()> {
        use agentenv_http_server::apis::templates::{Templates, TemplatesTemplateIdDeleteResponse};
        use axum_extra::extract::CookieJar;
        use headers::Host;

        let (_root, api, record) = test_api(VolumeLimits::default()).await?;
        let id = record.id.to_string();
        api.build_sessions
            .active
            .insert(id.clone(), BuildSession::new());
        let references = [id.clone(), record.alias.as_ref().unwrap().to_string()];
        for reference in references {
            let response = api
                .templates_template_id_delete(
                    &http::Method::DELETE,
                    &Host::from(http::uri::Authority::from_static("localhost")),
                    &CookieJar::new(),
                    &super::super::Claims,
                    &models::TemplatesTemplateIdDeletePathParams {
                        template_id: reference,
                    },
                )
                .await
                .unwrap();
            assert!(matches!(
                response,
                TemplatesTemplateIdDeleteResponse::Status409_Conflict(_)
            ));
            assert!(api.snapshot_manager.get(&id).await?.is_some());
        }
        api.build_sessions.active.remove(&id);
        let response = api
            .templates_template_id_delete(
                &http::Method::DELETE,
                &Host::from(http::uri::Authority::from_static("localhost")),
                &CookieJar::new(),
                &super::super::Claims,
                &models::TemplatesTemplateIdDeletePathParams {
                    template_id: id.clone(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            response,
            TemplatesTemplateIdDeleteResponse::Status204_TheTemplateWasDeletedSuccessfully
        ));
        assert!(api.snapshot_manager.get(&id).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn buildkit_cache_limit_is_checked_before_allocating_build() -> Result<()> {
        let (_root, api, _) = test_api(VolumeLimits {
            max_size_mb: 1024,
            ..VolumeLimits::default()
        })
        .await?;
        let request =
            serde_json::from_value(serde_json::json!({"template": {"name": "too-large"}}))?;
        let error = api.allocate_image_build(request).await.unwrap_err();
        assert_eq!(error.code, 400);
        assert!(error.message.contains("volume.max_size_mb"));
        assert!(api.build_sessions.active.is_empty());
        assert!(api
            .build_journal()
            .await?
            .scan_prefix(b"build/".to_vec())
            .await?
            .is_empty());
        assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
        Ok(())
    }

    #[test]
    fn buildkit_status_waits_for_cache_publication() -> Result<()> {
        let sessions = BuildSessions::default();
        let session = BuildSession::new();
        sessions.active.insert("build".to_owned(), session.clone());
        assert!(!sessions.is_finishing("build"));
        assert!(session.ready("127.0.0.1:1234".parse()?));
        session.submit("sha256:result").unwrap();
        assert!(sessions.is_finishing("build"));
        session.state.send_replace(SessionState::Finished(None));
        assert!(!sessions.is_finishing("build"));
        assert!(!sessions.is_finishing("missing"));
        Ok(())
    }

    #[test]
    fn buildkit_readiness_comes_from_dockerfile_healthcheck() -> Result<()> {
        use serde_json::json;
        assert_eq!(dockerfile_ready_command(None)?, None);
        assert_eq!(
            dockerfile_ready_command(Some(&json!({"Healthcheck": {"Test": ["NONE"]}})))?,
            None
        );
        let shell = json!({"Healthcheck": {"Test": ["CMD-SHELL", "test -f /started && test -s /result.txt"]}});
        assert_eq!(
            dockerfile_ready_command(Some(&shell))?.unwrap(),
            "/bin/sh -c 'test -f /started && test -s /result.txt'"
        );
        let exec = json!({"Healthcheck": {"Test": ["CMD", "test", "$literal", "=", "$literal"]}});
        assert_eq!(
            dockerfile_ready_command(Some(&exec))?.unwrap(),
            "test '$literal' = '$literal'"
        );
        let bash = json!({"Shell": ["/bin/bash", "-c"], "Healthcheck": {"Test": ["CMD-SHELL", "[[ -f /started ]]"]}});
        assert_eq!(
            dockerfile_ready_command(Some(&bash))?.unwrap(),
            "/bin/bash -c '[[ -f /started ]]'"
        );
        assert!(
            dockerfile_ready_command(Some(&json!({"Healthcheck": {"Test": ["CMD"]}}))).is_err()
        );
        Ok(())
    }

    #[test]
    fn buildkit_startup_overrides_take_precedence_independently() -> Result<()> {
        use serde_json::json;
        let context = CommandContext::default()
            .with_entrypoint(Some(vec!["/server".into()]))
            .with_cmd(Some(vec!["--port".into(), "8080".into()]));
        let image = json!({"Healthcheck": {"Test": ["CMD", "test", "-f", "/ready"]}});
        for (start, ready) in [
            (None, None),
            (Some("exec /other"), None),
            (None, Some("test -f /other-ready")),
            (Some(""), Some("")),
        ] {
            let request = serde_json::from_value(json!({
                "template": {"name": "demo"}, "startCmd": start, "readyCmd": ready,
            }))?;
            let commands = build_startup_commands(&request, &context, Some(&image))?;
            assert_eq!(
                commands.0.as_deref(),
                Some(start.unwrap_or("/server --port 8080"))
            );
            assert_eq!(
                commands.1.as_deref(),
                Some(ready.unwrap_or("test -f /ready"))
            );
        }
        // An explicit readiness command also bypasses unusable image health checks.
        let request =
            serde_json::from_value(json!({"template": {"name": "demo"}, "readyCmd": "true"}))?;
        let invalid = json!({"Healthcheck": {"Test": ["CMD"]}});
        assert_eq!(
            build_startup_commands(&request, &context, Some(&invalid))?
                .1
                .as_deref(),
            Some("true")
        );
        Ok(())
    }

    #[test]
    fn buildkit_submission_and_cancellation_are_mutually_exclusive() {
        let digest = crate::digest::sha256_digest(b"image");
        for cancel_first in [false, true] {
            let session = BuildSession::new();
            assert_eq!(session.submit(&digest).unwrap_err().code, 409);
            assert!(session.ready("127.0.0.1:1234".parse().unwrap()));
            if cancel_first {
                session.request_cancel().unwrap();
                assert_eq!(session.submit(&digest).unwrap_err().code, 409);
                assert!(matches!(*session.state.borrow(), SessionState::Cancelled));
                assert!(!session.ready("127.0.0.1:1234".parse().unwrap()));
                session.request_cancel().unwrap();
            } else {
                session.submit(&digest).unwrap();
                assert!(
                    matches!(&*session.state.borrow(), SessionState::Submitted(value) if value == &digest)
                );
                assert_eq!(session.submit(&digest).unwrap_err().code, 409);
                assert_eq!(session.request_cancel().unwrap_err().code, 409);
            }
        }
    }
}
