use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use agentenv_domain::BuildJobTransition;
use agentenv_metadata::MetaClient;
use anyhow::{ensure, Context, Result};
use futures::FutureExt;
use tokio::{
    net::TcpStream,
    sync::{oneshot, watch, Mutex, OnceCell, OwnedRwLockReadGuard, RwLock},
    time::Instant,
};
use tracing::{info, warn};

mod cache;
mod cleanup;
mod worker;

use crate::{
    cfg::ConfigManager,
    image::{
        buildkit::{build_image_name, BuildkitContent},
        ImageResolver, ResolvedBlockImage,
    },
    local_store::{LocalKvStore, LocalStoreDurability},
    orchestrator::Orchestrator,
    snapshot::{
        BuildErrorReason, BuildJobRecord, BuildStatus, BuildTarget, CommandContext, SnapshotHandle,
        SnapshotId, SnapshotManager, SnapshotRecord,
    },
    template::{
        logs::{BuildLogSession, BuildLogs},
        service::{lookup_build, BuildLookup, BuildServiceError},
        TemplateBuildSpec, TemplateBuilder,
    },
    types::{ImageConfigs, ImageName, ImageTag, SandboxId, DEFAULT_IMAGE_TAG},
    volume::VolumeManager,
};

// buildctl uses multiple connections for control and session traffic.
pub const MAX_TUNNEL_CONNECTIONS: u32 = 8;

/// Domain request to register a new `imageOnly` build job.
#[derive(Clone, Debug, Default)]
pub struct CreateImageBuildRequest {
    pub name: Option<String>,
    pub tags: Vec<String>,
    pub has_resource_overrides: bool,
}

/// Created `imageOnly` build job summary.
#[derive(Clone, Debug)]
pub struct CreatedImageBuild {
    pub build_id: SnapshotId,
    pub image_name: String,
    pub tags: Vec<String>,
}

/// Domain request to allocate and start a BuildKit worker session.
#[derive(Clone, Debug, Default)]
pub struct StartImageBuildRequest {
    pub start_cmd: Option<String>,
    pub ready_cmd: Option<String>,
    pub timeout_secs: Option<u32>,
}

/// Active TCP tunnel to a BuildKit worker, guarded against connection over-subscription
/// and carrying the session state watch channel for cancellation/shutdown notification.
pub struct BuilderTunnel {
    pub stream: TcpStream,
    pub state: watch::Receiver<SessionState>,
    pub _connection: OwnedRwLockReadGuard<()>,
}

#[derive(Default)]
pub(crate) struct BuildSessions {
    pub(crate) active: StdMutex<HashMap<String, BuildSession>>,
    v2_active: StdMutex<HashSet<String>>,
    pub(crate) journal: OnceCell<LocalKvStore>,
    pub(crate) builder_template: OnceCell<SnapshotRecord<Arc<SnapshotHandle>>>,
    #[cfg(test)]
    pub(crate) fail_cache_cleanup: std::sync::atomic::AtomicBool,
}

pub(crate) struct V2BuildGuard {
    sessions: Arc<BuildSessions>,
    id: String,
}

impl Drop for V2BuildGuard {
    fn drop(&mut self) {
        self.sessions.v2_active.lock().unwrap().remove(&self.id);
    }
}

impl BuildSessions {
    pub(crate) fn track_v2_build(self: &Arc<Self>, id: String) -> Option<V2BuildGuard> {
        self.v2_active
            .lock()
            .unwrap()
            .insert(id.clone())
            .then(|| V2BuildGuard {
                sessions: Arc::clone(self),
                id,
            })
    }

    pub(crate) fn reserve(&self, id: &str) -> Result<BuildSession, BuildServiceError> {
        let mut active = self.active.lock().unwrap();
        if active.contains_key(id) {
            return Err(BuildServiceError::Conflict(
                "build has already started".to_owned(),
            ));
        }
        if active.len()
            >= ConfigManager::global_config()
                .template_build
                .max_concurrent_builds
        {
            return Err(BuildServiceError::TooManyRequests(
                "node concurrent build limit reached; retry later".to_owned(),
            ));
        }
        let session = BuildSession::new();
        active.insert(id.to_owned(), session.clone());
        Ok(session)
    }

    pub(crate) fn contains(&self, id: &str) -> bool {
        self.active.lock().unwrap().contains_key(id) || self.v2_active.lock().unwrap().contains(id)
    }

    pub(crate) fn is_starting(&self, id: &str) -> bool {
        self.active
            .lock()
            .unwrap()
            .get(id)
            .is_some_and(|session| matches!(*session.state.borrow(), SessionState::Starting))
    }

    pub(crate) fn is_finishing(&self, id: &str) -> bool {
        self.active
            .lock()
            .unwrap()
            .get(id)
            .is_some_and(|session| matches!(*session.state.borrow(), SessionState::Publishing))
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct BuildJournal {
    pub(crate) cache: String,
    pub(crate) parent: Option<String>,
}

impl BuildJournal {
    pub(crate) async fn persist(&self, journal: &LocalKvStore, id: &str) -> Result<()> {
        journal
            .put(format!("build/{id}"), serde_json::to_vec(self)?)
            .await
    }
}

#[derive(Clone)]
pub(crate) struct BuildSession {
    pub(crate) state: watch::Sender<SessionState>,
    pub(crate) cleanup: Arc<Mutex<()>>,
    pub(crate) connections: Arc<RwLock<()>>,
}

#[derive(Clone)]
pub enum SessionState {
    Starting,
    Ready(SocketAddr),
    Publishing,
    Cancelled,
    Finished(Option<BuildErrorReason>),
}

impl SessionState {
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Cancelled | Self::Finished(_))
    }
}

impl BuildSession {
    pub(crate) fn new() -> Self {
        Self {
            state: watch::channel(SessionState::Starting).0,
            cleanup: Arc::new(Mutex::new(())),
            connections: Arc::new(RwLock::with_max_readers((), MAX_TUNNEL_CONNECTIONS)),
        }
    }

    pub(crate) fn ready(&self, address: SocketAddr) -> bool {
        self.state.send_if_modified(|state| {
            if !matches!(state, SessionState::Starting) {
                return false;
            }
            *state = SessionState::Ready(address);
            true
        })
    }

    pub(crate) fn publish(&self) -> Result<(), BuildServiceError> {
        let accepted = self.state.send_if_modified(|state| {
            if !matches!(state, SessionState::Ready(_)) {
                return false;
            }
            *state = SessionState::Publishing;
            true
        });
        if !accepted {
            return Err(BuildServiceError::Conflict(
                "builder is not ready or build is already publishing or cancelled".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn request_cancel(&self) -> Result<(), BuildServiceError> {
        let mut publishing = false;
        self.state.send_if_modified(|state| {
            publishing = matches!(state, SessionState::Publishing);
            if !matches!(state, SessionState::Starting | SessionState::Ready(_)) {
                return false;
            }
            *state = SessionState::Cancelled;
            true
        });
        if publishing {
            return Err(BuildServiceError::Conflict(
                "publication already started; the server will finish it and release the builder"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// Domain service owning BuildKit image/template build sessions, cache volume
/// lifecycle, recovery journal, and background cleanup.
#[derive(Clone)]
pub struct ImageBuildService {
    orchestrator: Arc<Orchestrator>,
    snapshot_manager: Arc<SnapshotManager>,
    template_builder: Arc<TemplateBuilder>,
    image_resolver: Arc<ImageResolver>,
    volume_manager: Arc<VolumeManager>,
    meta: MetaClient,
    build_logs: BuildLogs,
    sessions: Arc<BuildSessions>,
}

impl ImageBuildService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        orchestrator: Arc<Orchestrator>,
        snapshot_manager: Arc<SnapshotManager>,
        template_builder: Arc<TemplateBuilder>,
        image_resolver: Arc<ImageResolver>,
        volume_manager: Arc<VolumeManager>,
        meta: MetaClient,
        build_logs: BuildLogs,
    ) -> Self {
        Self {
            orchestrator,
            snapshot_manager,
            template_builder,
            image_resolver,
            volume_manager,
            meta,
            build_logs,
            sessions: Arc::new(BuildSessions::default()),
        }
    }

    pub fn has_active_session(&self, id: &str) -> bool {
        self.sessions.contains(id)
    }

    pub(crate) fn track_v2_build(&self, id: String) -> Option<V2BuildGuard> {
        self.sessions.track_v2_build(id)
    }

    pub fn is_starting(&self, id: &str) -> bool {
        self.sessions.is_starting(id)
    }

    pub fn is_finishing(&self, id: &str) -> bool {
        self.sessions.is_finishing(id)
    }

    pub(crate) async fn build_journal(&self) -> Result<&LocalKvStore> {
        self.sessions
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

    #[cfg(test)]
    pub(crate) fn sessions(&self) -> &Arc<BuildSessions> {
        &self.sessions
    }

    #[cfg(test)]
    pub(crate) fn with_empty_sessions(&self) -> Self {
        let mut cloned = self.clone();
        cloned.sessions = Arc::new(BuildSessions::default());
        cloned
    }

    #[cfg(test)]
    pub(crate) fn build_logs(&self) -> &BuildLogs {
        &self.build_logs
    }

    pub async fn start_image_build(
        &self,
        template_id: &str,
        build_id: &str,
        request: StartImageBuildRequest,
    ) -> Result<String, BuildServiceError> {
        if template_id != build_id {
            return Err(BuildServiceError::NotFound(
                "template build not found".to_owned(),
            ));
        }
        let id = SnapshotId::parse(build_id)
            .map_err(|_| BuildServiceError::NotFound("template build not found".to_owned()))?;
        // Reserve synchronously before spawning or touching durable state. Failed starts
        // release the slot; admitted builds hold it through publication and cleanup.
        let session = self.sessions.reserve(&id.to_string())?;
        let service = self.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = service
                .allocate_image_build(id.clone(), request, session)
                .await;
            if result.is_err() {
                service
                    .sessions
                    .active
                    .lock()
                    .unwrap()
                    .remove(&id.to_string());
            }
            // Finish allocation even if the request disappears, then release its worker.
            if let Err(Ok(_)) = sender.send(result) {
                let id = id.to_string();
                if let Err(error) = service.cancel_image_build(&id, &id).await {
                    warn!(build_id = %id, error = %error, "disconnected build cleanup will be retried");
                }
            }
        });
        receiver
            .await
            .map_err(|error| BuildServiceError::Internal(error.to_string()))?
    }

    async fn allocate_image_build(
        &self,
        id: SnapshotId,
        request: StartImageBuildRequest,
        session: BuildSession,
    ) -> Result<String, BuildServiceError> {
        if ConfigManager::global_config().template_build.cache_size_mb
            > self.volume_manager.limits().max_size_mb
        {
            return Err(BuildServiceError::InvalidInput(
                "template_build.cache_size_mb exceeds volume.max_size_mb".to_owned(),
            ));
        }
        let id_str = id.to_string();
        let job = match lookup_build(&self.meta, &self.snapshot_manager, &id_str, &id_str).await? {
            BuildLookup::ReadyTemplate(_) => {
                return Err(BuildServiceError::Conflict(
                    "build has already started".to_owned(),
                ));
            }
            BuildLookup::Job(job) => {
                if matches!(job.target, BuildTarget::Image { .. })
                    && (request.start_cmd.is_some() || request.ready_cmd.is_some())
                {
                    return Err(BuildServiceError::InvalidInput(
                        "startCmd and readyCmd do not apply to imageOnly builds".to_owned(),
                    ));
                }
                if job.status != BuildStatus::Waiting {
                    return Err(BuildServiceError::Conflict(
                        "build has already started".to_owned(),
                    ));
                }
                job
            }
        };
        let journal = self
            .build_journal()
            .await
            .map_err(|err| BuildServiceError::internal_from(err.as_ref()))?;
        // The SQL CAS also excludes starts on another node or through the legacy API.
        let started = self
            .meta
            .update_build_job(id.clone(), BuildJobTransition::Start)
            .await
            .map_err(BuildServiceError::from_meta)?
            .is_some();
        if !started {
            let exists = self
                .meta
                .get_build_job(id.clone())
                .await
                .map_err(BuildServiceError::from_meta)?
                .is_some();
            return Err(if exists {
                BuildServiceError::Conflict("build has already started".to_owned())
            } else {
                BuildServiceError::NotFound("template build not found".to_owned())
            });
        }
        let entry = BuildJournal {
            cache: format!("aenv-buildkit-work-{id}"),
            parent: None,
        };
        if let Err(err) = entry.persist(journal, &id_str).await {
            let reason = BuildErrorReason::new("failed to persist builder allocation");
            let _ = self
                .meta
                .update_build_job(id.clone(), BuildJobTransition::Fail { reason })
                .await;
            return Err(BuildServiceError::internal_from(err.as_ref()));
        }
        self.orchestrator
            .register_template_build(SandboxId::parse_str(&id_str).expect("build ID is a UUID"))
            .await;
        let service = self.clone();
        let build_id = id_str;
        tokio::spawn(async move {
            Box::pin(service.run_image_build(id, job, request, session, entry)).await;
        });
        Ok(build_image_name(&build_id))
    }

    /// Creates an imageOnly build. It publishes an image named by `name`
    /// instead of a template, and points the tags at it.
    pub async fn create_image_build(
        &self,
        request: CreateImageBuildRequest,
    ) -> Result<CreatedImageBuild, BuildServiceError> {
        let name = request
            .name
            .as_deref()
            .ok_or_else(|| BuildServiceError::InvalidInput("image name must be provided".into()))?;
        if request.has_resource_overrides {
            return Err(BuildServiceError::InvalidInput(
                "cpuCount and memoryMB do not apply to imageOnly builds".to_owned(),
            ));
        }
        let (name, tag) = match name.split_once(':') {
            Some((name, tag)) => (name, Some(tag)),
            None => (name, None),
        };
        let name = ImageName::parse(name)
            .map_err(|err| BuildServiceError::InvalidInput(err.to_string()))?;
        let mut tags = Vec::new();
        for tag in tag
            .into_iter()
            .chain(request.tags.iter().map(String::as_str))
        {
            let tag = ImageTag::new(&name, tag)
                .map_err(|err| BuildServiceError::InvalidInput(err.to_string()))?;
            if !tags.iter().any(|existing| existing == tag.tag()) {
                tags.push(tag.tag().to_owned());
            }
        }
        if tags.is_empty() {
            tags.push(DEFAULT_IMAGE_TAG.to_owned());
        }
        let id = SnapshotId::generate();
        self.meta
            .create_build_job(
                id.clone(),
                BuildTarget::Image {
                    name: name.to_string(),
                    tags: tags.clone(),
                },
            )
            .await
            .map_err(BuildServiceError::from_meta_build)?;
        info!(build_id = %id, image = %name, ?tags, "image build created");
        Ok(CreatedImageBuild {
            build_id: id,
            image_name: name.to_string(),
            tags,
        })
    }

    fn session(
        &self,
        template_id: &str,
        build_id: &str,
    ) -> Result<BuildSession, BuildServiceError> {
        if template_id != build_id {
            return Err(BuildServiceError::NotFound(
                "template build not found".to_owned(),
            ));
        }
        self.sessions
            .active
            .lock()
            .unwrap()
            .get(build_id)
            .cloned()
            .ok_or_else(|| BuildServiceError::NotFound("active template build not found".into()))
    }

    /// Validates an active ready session, enforces the per-build tunnel connection
    /// cap, and connects to the BuildKit TCP endpoint inside the worker VM.
    pub async fn connect_builder_tunnel(
        &self,
        template_id: &str,
        build_id: &str,
    ) -> Result<BuilderTunnel, BuildServiceError> {
        let session = self.session(template_id, build_id)?;
        let SessionState::Ready(address) = *session.state.borrow() else {
            return Err(BuildServiceError::Conflict(
                "builder is not ready".to_owned(),
            ));
        };
        // Include pending TCP connections and upgrades in the cap; never queue waiters.
        let connection = session.connections.clone().try_read_owned().map_err(|_| {
            BuildServiceError::TooManyRequests(
                "builder tunnel connection limit reached; retry later".to_owned(),
            )
        })?;
        let stream = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(address))
            .await
            .map_err(|_| BuildServiceError::UpstreamTimeout("builder connection timed out".into()))?
            .map_err(|error| {
                BuildServiceError::UpstreamFailed(format!("builder connection failed: {error}"))
            })?;
        Ok(BuilderTunnel {
            stream,
            state: session.state.subscribe(),
            _connection: connection,
        })
    }

    pub async fn cancel_image_build(
        &self,
        template_id: &str,
        build_id: &str,
    ) -> Result<(), BuildServiceError> {
        if template_id != build_id {
            return Err(BuildServiceError::NotFound(
                "template build not found".to_owned(),
            ));
        }
        let session = match self.session(template_id, build_id) {
            Ok(session) => session,
            Err(_) => {
                lookup_build(&self.meta, &self.snapshot_manager, template_id, build_id).await?;
                return self
                    .retry_image_build_cleanup(build_id)
                    .await
                    .map_err(|err| {
                        BuildServiceError::Internal(format!("builder cleanup failed: {err:#}"))
                    });
            }
        };
        let mut state = session.state.subscribe();
        session.request_cancel()?;
        tokio::time::timeout(Duration::from_secs(60), async {
            state
                .wait_for(|state| matches!(state, SessionState::Finished(_)))
                .await
                .map_err(|_| {
                    BuildServiceError::Internal("builder cleanup stopped unexpectedly".to_owned())
                })?;
            self.retry_image_build_cleanup(build_id)
                .await
                .map_err(|err| {
                    BuildServiceError::Internal(format!("builder cleanup failed: {err:#}"))
                })
        })
        .await
        .map_err(|_| {
            BuildServiceError::Internal(
                "builder cleanup is still running; retry cancellation later".to_owned(),
            )
        })?
    }

    #[tracing::instrument(skip_all, fields(build_id = %build_id))]
    async fn run_image_build(
        &self,
        build_id: SnapshotId,
        job: BuildJobRecord,
        request: StartImageBuildRequest,
        session: BuildSession,
        entry: BuildJournal,
    ) {
        let id = build_id.to_string();
        let deadline =
            Instant::now() + Duration::from_secs(request.timeout_secs.unwrap_or(3600).into());
        let logs = self
            .build_logs
            .start(build_id.clone(), self.snapshot_manager.repository());
        info!(build_id = %id, "build starting");
        let logger = logs.logger.clone();
        let image_only = matches!(job.target, BuildTarget::Image { .. });
        let work = async {
            let (address, digest) = Box::pin(
                self.wait_for_image_build(&build_id, &request, &session, &entry, deadline, &logger),
            )
            .await?;
            let resolved = Box::pin(self.import_build_image(address, &digest)).await?;
            // Let buildctl receive its final Solve response before stopping BuildKit.
            // A client that keeps sockets open must not indefinitely delay publication.
            let _connections =
                tokio::time::timeout(Duration::from_secs(10), session.connections.write()).await;
            let cache_ready = Box::pin(self.release_builder(&id, &entry.cache)).await?;
            match job.target {
                BuildTarget::Image { name, tags } => {
                    Box::pin(self.publish_built_image(&build_id, &name, &tags, &resolved)).await?;
                }
                BuildTarget::Template { alias, resources } => {
                    let context = CommandContext::from(resolved.base_context);
                    let (start, ready) =
                        build_startup_commands(&request, &context, resolved.raw_config.as_ref())?;
                    let mut configs = ImageConfigs::new();
                    if let Some(config) = resolved.raw_config {
                        configs.add(None::<String>, "/", config);
                    }
                    let mut spec = TemplateBuildSpec::new()
                        .with_logger(logger.clone())
                        .alias(alias.as_ref().context("template name missing")?.to_string())
                        .resources(resources.cpu_count, resources.memory_mib)
                        .with_startup_shell("/bin/sh")
                        .with_resolved_overlaybd_image(resolved.overlaybd_config_path, configs)
                        .with_base_context(context);
                    if let Some(start) = start {
                        spec = spec.start_cmd(start);
                    }
                    if let Some(ready) = ready {
                        spec = spec.ready_cmd(ready);
                    }
                    Box::pin(self.template_builder.build_and_publish_with_id(
                        self.snapshot_manager.as_ref(),
                        build_id.clone(),
                        spec,
                    ))
                    .await?;
                }
            }
            if cache_ready {
                if let Err(error) = Box::pin(self.publish_build_cache(&id, &entry.cache)).await {
                    warn!(build_id = %id, error = %format_args!("{error:#}"), "cache publication failed; keeping the previous cache seed");
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        let work =
            Box::pin(async { deliver_build_failure(&session, image_only, work.await).await });
        self.supervise_image_build(&build_id, &session, logs, work)
            .await;
    }

    /// Publishes the built image, points the build's tags at it, and marks
    /// the build ready.
    async fn publish_built_image(
        &self,
        build_id: &SnapshotId,
        name: &str,
        tags: &[String],
        resolved: &ResolvedBlockImage,
    ) -> Result<()> {
        let image_name = ImageName::parse(name)?;
        let image_tags = tags
            .iter()
            .map(|tag| ImageTag::new(&image_name, tag))
            .collect::<Result<Vec<_>, _>>()?;
        let digest = self.image_resolver.publish_image(resolved).await?;
        for tag in &image_tags {
            self.meta
                .put_image_tag(tag.clone(), digest.clone(), None)
                .await
                .context("put image tag in metadata service")?;
        }
        self.meta
            .update_build_job(
                build_id.clone(),
                BuildJobTransition::FinishImage {
                    digest: digest.clone(),
                },
            )
            .await
            .context("finish image build job")?;
        info!(%build_id, image_digest = %digest, ?tags, image = %name, "image published");
        Ok(())
    }

    async fn import_build_image(
        &self,
        address: SocketAddr,
        digest: &str,
    ) -> Result<ResolvedBlockImage> {
        let content = BuildkitContent::connect(address).await?;
        tokio::time::timeout(
            Duration::from_secs(3600),
            self.image_resolver.resolve_buildkit(&content, digest),
        )
        .await
        .context("image import deadline exceeded")?
    }

    pub(crate) async fn supervise_image_build(
        &self,
        build_id: &SnapshotId,
        session: &BuildSession,
        logs: BuildLogSession,
        work: impl std::future::Future<Output = Result<()>>,
    ) {
        let result = std::panic::AssertUnwindSafe(work)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("build worker panicked")));
        self.finish_image_build(build_id, session, result).await;
        if let Err(error) = logs.finish().await {
            warn!(build_id = %build_id, %error, "failed to persist final build logs");
        }
    }

    pub(crate) async fn finish_image_build(
        &self,
        build_id: &SnapshotId,
        session: &BuildSession,
        result: Result<()>,
    ) {
        let id = build_id.to_string();
        let reason = match result {
            Ok(()) => {
                info!(build_id = %id, "build completed");
                None
            }
            Err(error) => {
                warn!(build_id = %id, error = %format_args!("{error:#}"), "build failed");
                Some(BuildErrorReason::new(format!("{error:#}")))
            }
        };
        session.state.send_replace(SessionState::Finished(reason));
        if let Err(error) = self.retry_image_build_cleanup(&id).await {
            warn!(build_id = %id, error = %format_args!("{error:#}"), "build finalization failed; cleanup will be retried");
        }
    }
}

/// An image build's registry export reports a failed push in buildctl's final
/// Solve response. Give the client a bounded chance to receive it before the
/// tunnel closes; template builds keep finishing immediately.
async fn deliver_build_failure(
    session: &BuildSession,
    image_only: bool,
    result: Result<()>,
) -> Result<()> {
    if image_only && result.is_err() {
        let _connections =
            tokio::time::timeout(Duration::from_secs(10), session.connections.write()).await;
    }
    result
}

fn build_startup_commands(
    request: &StartImageBuildRequest,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buildkit_status_waits_for_cache_publication() -> Result<()> {
        let sessions = BuildSessions::default();
        let session = BuildSession::new();
        sessions
            .active
            .lock()
            .unwrap()
            .insert("build".to_owned(), session.clone());
        assert!(!sessions.is_finishing("build"));
        assert!(session.ready("127.0.0.1:1234".parse()?));
        session.publish().unwrap();
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
            let request = StartImageBuildRequest {
                start_cmd: start.map(str::to_owned),
                ready_cmd: ready.map(str::to_owned),
                timeout_secs: None,
            };
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
        let request = StartImageBuildRequest {
            start_cmd: None,
            ready_cmd: Some("true".to_owned()),
            timeout_secs: None,
        };
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
    fn buildkit_publication_and_cancellation_are_mutually_exclusive() {
        for cancel_first in [false, true] {
            let session = BuildSession::new();
            assert!(matches!(
                session.publish().unwrap_err(),
                BuildServiceError::Conflict(_)
            ));
            assert!(session.ready("127.0.0.1:1234".parse().unwrap()));
            if cancel_first {
                session.request_cancel().unwrap();
                assert!(matches!(
                    session.publish().unwrap_err(),
                    BuildServiceError::Conflict(_)
                ));
                assert!(matches!(*session.state.borrow(), SessionState::Cancelled));
                assert!(!session.ready("127.0.0.1:1234".parse().unwrap()));
                session.request_cancel().unwrap();
            } else {
                session.publish().unwrap();
                assert!(matches!(&*session.state.borrow(), SessionState::Publishing));
                assert!(matches!(
                    session.publish().unwrap_err(),
                    BuildServiceError::Conflict(_)
                ));
                assert!(matches!(
                    session.request_cancel().unwrap_err(),
                    BuildServiceError::Conflict(_)
                ));
            }
        }
    }

    #[test]
    fn concurrent_build_reservations_never_exceed_node_limit() {
        let sessions = BuildSessions::default();
        let limit = ConfigManager::global_config()
            .template_build
            .max_concurrent_builds;
        let barrier = std::sync::Barrier::new(limit + 8);
        let accepted = std::thread::scope(|scope| {
            let attempts = (0..limit + 8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        match sessions.reserve(&SnapshotId::generate().to_string()) {
                            Ok(_) => true,
                            Err(error) => {
                                assert!(matches!(error, BuildServiceError::TooManyRequests(_)));
                                false
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();
            attempts
                .into_iter()
                .map(|attempt| usize::from(attempt.join().unwrap()))
                .sum::<usize>()
        });
        assert_eq!(accepted, limit);
        assert_eq!(sessions.active.lock().unwrap().len(), limit);
    }

    #[tokio::test(start_paused = true)]
    async fn only_failed_image_builds_wait_for_buildctl_to_receive_the_error() {
        let session = BuildSession::new();
        // An open BuildKit tunnel holds a connection read lock.
        let _tunnel = session.connections.clone().read_owned().await;
        for (image_only, failed, waits) in [
            (true, true, true),
            (false, true, false),
            (true, false, false),
        ] {
            let started = Instant::now();
            let result = if failed {
                Err(anyhow::anyhow!("push failed"))
            } else {
                Ok(())
            };
            let result = deliver_build_failure(&session, image_only, result).await;
            assert_eq!(result.is_err(), failed);
            assert_eq!(
                started.elapsed() >= Duration::from_secs(10),
                waits,
                "image_only={image_only} failed={failed}"
            );
        }
    }
}
