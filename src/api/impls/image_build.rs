use std::{collections::HashMap, net::SocketAddr, time::Duration};

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
use futures::{SinkExt, StreamExt};
use http::StatusCode;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{watch, OnceCell},
    time::Instant,
};
use tracing::{debug, info, warn};

mod cache;
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
        TemplateBuildStatus,
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
    pub(super) fn is_finishing(&self, id: &str) -> bool {
        self.active
            .get(id)
            .is_some_and(|session| matches!(*session.state.borrow(), SessionState::Submitted(_)))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
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
}

#[derive(Clone)]
enum SessionState {
    Starting,
    Ready(SocketAddr),
    Submitted(String),
    Cancelled,
    Finished,
}

impl BuildSession {
    fn new() -> Self {
        Self {
            state: watch::channel(SessionState::Starting).0,
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

    /// Recover server-owned builders and unfinished records before accepting API requests.
    pub async fn recover_image_builds(&self) -> Result<()> {
        let journal = self.build_journal().await?;
        for (key, value) in journal.scan_prefix(b"build/".to_vec()).await? {
            let key_text = std::str::from_utf8(&key)?;
            let id = &key_text[6..];
            let entry: BuildJournal = serde_json::from_slice(&value)?;
            self.release_builder(id, &entry.cache).await?;
            self.cleanup_build_cache(id, &entry).await?;
            if let Some(record) = self.snapshot_manager.get(id).await? {
                if matches!(
                    super::template::template_build_status(&record),
                    TemplateBuildStatus::Waiting | TemplateBuildStatus::Building
                ) {
                    self.snapshot_manager
                        .mark_build_error(
                            &record.id,
                            TemplateBuildErrorReason::new("build interrupted by server restart"),
                        )
                        .await?;
                }
            }
            journal.delete(key).await?;
        }
        Ok(())
    }

    pub(super) async fn start_image_build(
        &self,
        body: &models::TemplateBuildSessionRequest,
    ) -> Result<models::TemplateRequestResponseV3, models::Error> {
        let api = self.clone();
        let body = body.clone();
        tokio::spawn(async move { api.allocate_image_build(body).await })
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
        entry
            .persist(journal, &id.to_string())
            .await
            .map_err(|err| Self::internal_error(err.as_ref()))?;
        if let Err(err) = self.snapshot_manager.create(record.clone()).await {
            let _ = journal.delete(key.into_bytes()).await;
            return Err(Self::repository_error(&err));
        }
        self.orchestrator
            .register_template_build(
                SandboxId::parse_str(&id.to_string()).expect("build ID is a UUID"),
            )
            .await;
        let session = BuildSession::new();
        self.build_sessions
            .active
            .insert(id.to_string(), session.clone());
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
                return Ok(());
            }
        };
        let mut state = session.state.subscribe();
        session.request_cancel()?;
        tokio::time::timeout(
            Duration::from_secs(60),
            state.wait_for(|state| matches!(state, SessionState::Finished)),
        )
        .await
        .map_err(|_| {
            Self::error(
                500,
                "builder cleanup is still running; retry cancellation later",
            )
        })?
        .map_err(|_| Self::error(500, "builder cleanup stopped unexpectedly"))?;
        // Finished is sent after successful cleanup removes the active entry.
        // A retained entry means release failed and needs another attempt.
        if self.build_sessions.active.contains_key(build_id) {
            self.release_builder(build_id, &format!("aenv-buildkit-work-{build_id}"))
                .await
                .map_err(|err| Self::error(500, format!("builder cleanup failed: {err:#}")))?;
            self.forget_image_build(build_id).await;
        }
        Ok(())
    }

    async fn run_image_build(
        &self,
        record: SnapshotRecord,
        body: models::TemplateBuildSessionRequest,
        session: BuildSession,
        mut entry: BuildJournal,
    ) {
        let id = record.id.to_string();
        let deadline = Instant::now() + Duration::from_secs(body.timeout.unwrap_or(3600).into());
        info!(build_id = %id, "template build starting");
        let result = async {
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
            let (address, executor) = self.prepare_builder(&id, &body, &mut entry, snapshot).await?;
            ensure!(!matches!(*state.borrow(), SessionState::Cancelled), "build cancelled");
            worker_command(&executor, START_BUILDKIT, 90).await?;
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
            self.release_builder(&id, &entry.cache).await?;
            let context = CommandContext::from(resolved.base_context);
            let (start, ready) = build_startup_commands(&body, &context, resolved.raw_config.as_ref())?;
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
            if let Err(error) = self.publish_build_cache(&id, &entry.cache).await {
                warn!(build_id = %id, error = %format_args!("{error:#}"), "cache publication failed; keeping the previous cache seed");
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        self.finish_image_build(&record, &session, &entry, result)
            .await;
    }

    async fn finish_image_build(
        &self,
        record: &SnapshotRecord,
        session: &BuildSession,
        entry: &BuildJournal,
        result: Result<()>,
    ) {
        let id = record.id.to_string();
        let persisted = match &result {
            Ok(()) => {
                info!(build_id = %id, "template build completed");
                Ok(())
            }
            Err(error) => {
                warn!(build_id = %id, error = %format_args!("{error:#}"), "template build failed");
                self.snapshot_manager
                    .mark_build_error(
                        &record.id,
                        TemplateBuildErrorReason::new(format!("{error:#}")),
                    )
                    .await
            }
        };
        let cleanup = async {
            // Successful publication already released the worker before capture.
            if result.is_err() {
                self.release_builder(&id, &entry.cache).await?;
            }
            self.forget_image_build(&id).await;
            self.cleanup_build_cache(&id, entry).await?;
            persisted?;
            self.build_journal()
                .await?
                .delete(format!("build/{id}"))
                .await
        }
        .await;
        if let Err(error) = cleanup {
            warn!(build_id = %id, %error, "build finalization failed; recovery journal retained for restart");
        }
        session.state.send_replace(SessionState::Finished);
    }

    async fn forget_image_build(&self, id: &str) {
        self.build_sessions.active.remove(id);
        self.orchestrator
            .unregister_template_build(SandboxId::parse_str(id).expect("build ID is a UUID"))
            .await;
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

    async fn release_builder(&self, id: &str, cache: &str) -> Result<()> {
        let sandbox_id = SandboxId::parse_str(id)?;
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
                if let Err(error) = worker_command(&executor, STOP_BUILDKIT, 40).await {
                    warn!(build_id = %id, %error, "builder daemon shutdown failed");
                }
            }
            self.orchestrator.delete_sandbox(sandbox_id).await?;
        }
        self.release_cache_lease(id, cache).await
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
test -f /run/aenv-buildkit/pid || exit 0
pid=$(cat /run/aenv-buildkit/pid)
kill -TERM "$pid" 2>/dev/null || true
for attempt in $(seq 1 30); do
  if ! kill -0 "$pid" 2>/dev/null || grep -q 'State:.*Z' "/proc/$pid/status"; then sync; exit 0; fi
  sleep 1
done
exit 1
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn buildkit_cleanup_failure_preserves_success_and_recovery_journal() -> Result<()> {
        use crate::{
            api_key::ApiKey,
            cfg::AppConfig,
            image::ImageResolver,
            orchestrator::{FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator},
            sandbox::FirecrackerSandboxFactory,
            snapshot::{
                mock::{write_mock_built_artifacts, MockSnapshotRepository},
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
        let record = manager
            .publish(SnapshotPublishMetadata::mock(), manifest)
            .await?;
        let orchestrator = Orchestrator::new(
            InMemoryMetadataStore::new(),
            FirecrackerSandboxFactory::new(),
            FileBackedSandboxPersister::new_for_test(root.path().join("sandboxes")),
        )
        .await?;
        // Every volume operation fails, including a redundant worker release.
        let volumes = VolumeManager::open_with_repository(
            root.path().join("volumes/catalog.json"),
            Arc::new(MockSnapshotRepository),
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
        let id = record.id.to_string();
        let key = format!("build/{id}").into_bytes();
        let entry = BuildJournal {
            cache: "aenv-buildkit-work-test".into(),
            parent: None,
        };
        entry.persist(api.build_journal().await?, &id).await?;
        let session = BuildSession {
            state: watch::channel(SessionState::Submitted("sha256:result".into())).0,
        };
        api.build_sessions
            .active
            .insert(id.clone(), session.clone());
        api.orchestrator
            .register_template_build(SandboxId::parse_str(&id)?)
            .await;

        api.finish_image_build(&record, &session, &entry, Ok(()))
            .await;

        let saved = manager.get(&id).await?.unwrap();
        assert_eq!(
            super::super::template::template_build_status(&saved),
            TemplateBuildStatus::Ready
        );
        assert!(api.build_journal().await?.get(key).await?.is_some());
        assert!(matches!(*session.state.borrow(), SessionState::Finished));
        assert!(!api.build_sessions.active.contains_key(&id));
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
        session.state.send_replace(SessionState::Finished);
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
