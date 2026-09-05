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
use futures::{SinkExt, StreamExt};
use http::StatusCode;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{watch, OnceCell},
};
use tracing::{debug, info, warn};

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
        CommandContext, SnapshotId, SnapshotRecord, TemplateBuildErrorReason, TemplateBuildStatus,
    },
    template::TemplateBuildSpec,
    types::{ImageConfigs, SandboxId, SandboxResources},
    volume::{VolumeError, VolumeMode},
};

#[derive(Default)]
pub(crate) struct BuildSessions {
    active: DashMap<String, Arc<BuildSession>>,
    journal: OnceCell<LocalKvStore>,
}

struct BuildSession {
    state: watch::Sender<SessionState>,
    cache: String,
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
            let cache = std::str::from_utf8(&value)?;
            self.release_builder(id, cache).await?;
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
        tokio::spawn(async move { api.allocate_image_build(&body).await })
            .await
            .map_err(|error| Self::error(500, error.to_string()))?
    }

    async fn allocate_image_build(
        &self,
        body: &models::TemplateBuildSessionRequest,
    ) -> Result<models::TemplateRequestResponseV3, models::Error> {
        let name = body
            .template
            .name
            .as_deref()
            .ok_or_else(|| Self::error(400, "template name must be provided"))?;
        let id = SnapshotId::generate();
        let record = template_build_record_from_v3_request(&body.template, id.clone(), name)?;
        let cache = body.cache_volume.clone().unwrap_or_else(|| {
            format!(
                "aenv-buildkit-{}",
                &crate::digest::sha256_hex(name.as_bytes())[..24]
            )
        });
        let journal = self
            .build_journal()
            .await
            .map_err(|err| Self::internal_error(err.as_ref()))?;
        let key = format!("build/{id}");
        journal
            .put(key.as_bytes().to_vec(), cache.as_bytes().to_vec())
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
        let session = Arc::new(BuildSession {
            state: watch::channel(SessionState::Starting).0,
            cache,
        });
        self.build_sessions
            .active
            .insert(id.to_string(), session.clone());
        let api = self.clone();
        let body = body.clone();
        tokio::spawn(async move {
            api.run_image_build(record, body, session).await;
        });
        Ok(models::TemplateRequestResponseV3::new(
            id.to_string(),
            id.to_string(),
            true,
            vec![name.to_owned()],
            vec![name.to_owned()],
            vec![],
        ))
    }

    fn session(
        &self,
        template_id: &str,
        build_id: &str,
    ) -> Result<Arc<BuildSession>, models::Error> {
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
        loop {
            if matches!(*state.borrow(), SessionState::Finished) {
                self.release_builder(build_id, &session.cache)
                    .await
                    .map_err(|err| Self::error(500, format!("builder cleanup failed: {err:#}")))?;
                self.build_sessions.active.remove(build_id);
                self.orchestrator
                    .unregister_template_build(
                        SandboxId::parse_str(build_id).expect("build ID is a UUID"),
                    )
                    .await;
                return Ok(());
            }
            state
                .changed()
                .await
                .map_err(|_| Self::error(500, "builder cleanup stopped unexpectedly"))?;
        }
    }

    async fn run_image_build(
        &self,
        record: SnapshotRecord,
        body: models::TemplateBuildSessionRequest,
        session: Arc<BuildSession>,
    ) {
        let id = record.id.to_string();
        info!(build_id = %id, "template builder starting");
        let result = async {
            let (address, executor) = self.prepare_builder(&id, &body, &session.cache).await?;
            worker_command(&executor, START_BUILDKIT, 90).await?;
            ensure!(session.ready(address), "build cancelled");
            // Existing template status becomes Building only when the worker can accept a solve.
            self.snapshot_manager.try_start_build(&record.id).await?;
            let mut receiver = session.state.subscribe();
            let command = tokio::time::timeout(
                Duration::from_secs(body.timeout.unwrap_or(3600).into()),
                receiver.wait_for(|state| {
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
            self.release_builder(&id, &session.cache).await?;
            let mut configs = ImageConfigs::new();
            if let Some(config) = resolved.raw_config {
                configs.add(None::<String>, "/", config);
            }
            let base = resolved.base_context;
            let context = CommandContext::from_env_and_workdir(base.env_vars, base.workdir)
                .with_user(base.user)
                .with_exposed_ports(base.exposed_ports)
                .with_entrypoint(base.entrypoint)
                .with_cmd(base.cmd)
                .with_volumes(base.volumes)
                .with_labels(base.labels);
            let start = body.start_cmd.or_else(|| context.effective_start_cmd());
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
            if let Some(ready) = body.ready_cmd {
                spec = spec.ready_cmd(ready);
            }
            self.template_builder
                .build_and_publish_with_id(self.snapshot_manager.as_ref(), record.id.clone(), spec)
                .await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let cleanup = self.release_builder(&id, &session.cache).await;
        let cleanup_ok = cleanup.is_ok();
        if let Err(error) = &cleanup {
            warn!(build_id = %id, %error, "template worker cleanup failed; recovery will retry");
        }
        let result = result.and(cleanup);
        let mut persisted = true;
        match &result {
            Ok(()) => info!(build_id = %id, "template build completed"),
            Err(error) => {
                warn!(build_id = %id, error = %format_args!("{error:#}"), "template build failed");
                if let Err(error) = self
                    .snapshot_manager
                    .mark_build_error(
                        &record.id,
                        TemplateBuildErrorReason::new(format!("{error:#}")),
                    )
                    .await
                {
                    persisted = false;
                    warn!(build_id = %id, %error, "failed to persist template build error");
                }
            }
        }
        // Keep the recovery journal if cleanup failed, so restart can retry it.
        if cleanup_ok && persisted {
            if let Ok(journal) = self.build_journal().await {
                let _ = journal.delete(format!("build/{id}").into_bytes()).await;
            }
        }
        session.state.send_replace(SessionState::Finished);
        if cleanup_ok {
            self.build_sessions.active.remove(&id);
            self.orchestrator
                .unregister_template_build(SandboxId::parse_str(&id).expect("build ID is a UUID"))
                .await;
        }
    }

    async fn prepare_builder(
        &self,
        id: &str,
        body: &models::TemplateBuildSessionRequest,
        cache: &str,
    ) -> Result<(SocketAddr, Executor)> {
        let volume = match self.volume_manager.get(cache).await {
            Ok(volume) => volume,
            Err(VolumeError::NotFound(_)) => match self
                .volume_manager
                .create(
                    cache.to_owned(),
                    VolumeMode::Exclusive,
                    None,
                    None,
                    body.cache_size_mb.unwrap_or(16384),
                )
                .await
            {
                Ok(volume) => volume,
                Err(VolumeError::NameConflict(_)) => self.volume_manager.get(cache).await?,
                Err(error) => return Err(error.into()),
            },
            Err(err) => return Err(err.into()),
        };
        ensure!(
            volume.mode == VolumeMode::Exclusive,
            "build cache must use exclusive mode"
        );
        let resolved = self
            .image_resolver
            .resolve(
                body.builder_image
                    .as_deref()
                    .unwrap_or("docker.io/moby/buildkit:v0.33.0"),
            )
            .await?;
        let (drives, mounts) = super::volumes::resolve_volume_mounts(
            &self.volume_manager,
            &HashMap::from([("/var/lib/buildkit".to_owned(), volume.id)]),
            id,
        )
        .await
        .map_err(|error| {
            if error.code == 409 {
                anyhow::anyhow!(
                    "build cache is reserved or unavailable; select another cache volume"
                )
            } else {
                anyhow::anyhow!("{}", error.message)
            }
        })?;
        let mut image_configs = ImageConfigs::new();
        if let Some(config) = resolved.raw_config {
            image_configs.add(None::<String>, "/", config);
        }
        let network_policy = SandboxNetworkPolicy {
            allow_public_traffic: false,
            ..Default::default()
        };
        let context = CommandContext::from_env_and_workdir(
            resolved.base_context.env_vars,
            Some("/".to_owned()),
        )
        .with_user(Some("root".to_owned()));
        let metadata = self
            .orchestrator
            .create_template_builder(
                SandboxId::parse_str(id)?,
                CreateSandboxRequest {
                    source: SandboxLaunchSource::Image {
                        image_ref: resolved.image_ref,
                        overlaybd_config_path: resolved.overlaybd_config_path,
                        context: Box::new(context),
                        resources: Some(SandboxResources {
                            cpu_count: body.builder_cpu_count.unwrap_or(2),
                            memory_mib: body.builder_memory_mb.unwrap_or(2048),
                            disk_size_mib: 0,
                        }),
                        extra_drives: drives,
                        extra_boot_args: None,
                        image_configs: Box::new(image_configs),
                    },
                    extra_drives: vec![],
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
        match self.volume_manager.get(cache).await {
            Ok(volume) => {
                self.volume_manager
                    .replace_owner_for(id, None, &[volume.id])
                    .await?
            }
            Err(VolumeError::NotFound(_)) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(())
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

    #[test]
    fn buildkit_submission_and_cancellation_are_mutually_exclusive() {
        let digest = crate::digest::sha256_digest(b"image");
        for cancel_first in [false, true] {
            let session = BuildSession {
                state: watch::channel(SessionState::Starting).0,
                cache: "cache".into(),
            };
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
