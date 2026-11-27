use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use agentenv_domain::BuildJobTransition;
use agentenv_http_server::models;
use anyhow::{ensure, Context, Result};
use futures::FutureExt;
use tokio::{
    sync::{oneshot, watch, Mutex, OnceCell, RwLock},
    time::Instant,
};
use tracing::{info, warn};

mod cache;
mod cleanup;
mod transport;
mod worker;
pub(crate) use transport::router;

use super::{template::BuildLookup, ApiImpl};
use crate::{
    cfg::ConfigManager,
    image::{
        buildkit::{build_image_name, BuildkitContent},
        ResolvedBlockImage,
    },
    local_store::{LocalKvStore, LocalStoreDurability},
    snapshot::{
        BuildErrorReason, BuildJobRecord, BuildStatus, BuildTarget, CommandContext, SnapshotHandle,
        SnapshotId, SnapshotRecord,
    },
    template::{logs::BuildLogSession, TemplateBuildSpec},
    types::{ImageConfigs, ImageName, ImageTag, SandboxId, DEFAULT_IMAGE_TAG},
};

#[derive(Default)]
pub(crate) struct BuildSessions {
    active: StdMutex<HashMap<String, BuildSession>>,
    v2_active: StdMutex<HashSet<String>>,
    journal: OnceCell<LocalKvStore>,
    builder_template: OnceCell<SnapshotRecord<Arc<SnapshotHandle>>>,
    #[cfg(test)]
    fail_cache_cleanup: std::sync::atomic::AtomicBool,
}

pub(super) struct V2BuildGuard {
    sessions: Arc<BuildSessions>,
    id: String,
}

impl Drop for V2BuildGuard {
    fn drop(&mut self) {
        self.sessions.v2_active.lock().unwrap().remove(&self.id);
    }
}

impl BuildSessions {
    pub(super) fn track_v2_build(self: &Arc<Self>, id: String) -> Option<V2BuildGuard> {
        self.v2_active
            .lock()
            .unwrap()
            .insert(id.clone())
            .then(|| V2BuildGuard {
                sessions: Arc::clone(self),
                id,
            })
    }

    fn reserve(&self, id: &str) -> Result<BuildSession, models::Error> {
        let mut active = self.active.lock().unwrap();
        if active.contains_key(id) {
            return Err(ApiImpl::error(409, "build has already started"));
        }
        if active.len()
            >= ConfigManager::global_config()
                .template_build
                .max_concurrent_builds
        {
            return Err(ApiImpl::error(
                429,
                "node concurrent build limit reached; retry later",
            ));
        }
        let session = BuildSession::new();
        active.insert(id.to_owned(), session.clone());
        Ok(session)
    }

    pub(super) fn contains(&self, id: &str) -> bool {
        self.active.lock().unwrap().contains_key(id) || self.v2_active.lock().unwrap().contains(id)
    }

    pub(super) fn is_starting(&self, id: &str) -> bool {
        self.active
            .lock()
            .unwrap()
            .get(id)
            .is_some_and(|session| matches!(*session.state.borrow(), SessionState::Starting))
    }

    pub(super) fn is_finishing(&self, id: &str) -> bool {
        self.active
            .lock()
            .unwrap()
            .get(id)
            .is_some_and(|session| matches!(*session.state.borrow(), SessionState::Publishing))
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
    connections: Arc<RwLock<()>>,
}

#[derive(Clone)]
enum SessionState {
    Starting,
    Ready(SocketAddr),
    Publishing,
    Cancelled,
    Finished(Option<BuildErrorReason>),
}

impl BuildSession {
    fn new() -> Self {
        Self {
            state: watch::channel(SessionState::Starting).0,
            cleanup: Arc::new(Mutex::new(())),
            connections: Arc::new(RwLock::with_max_readers(
                (),
                transport::MAX_TUNNEL_CONNECTIONS,
            )),
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

    fn publish(&self) -> Result<(), models::Error> {
        let accepted = self.state.send_if_modified(|state| {
            if !matches!(state, SessionState::Ready(_)) {
                return false;
            }
            *state = SessionState::Publishing;
            true
        });
        if !accepted {
            return Err(ApiImpl::error(
                409,
                "builder is not ready or build is already publishing or cancelled",
            ));
        }
        Ok(())
    }

    fn request_cancel(&self) -> Result<(), models::Error> {
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
            return Err(ApiImpl::error(
                409,
                "publication already started; the server will finish it and release the builder",
            ));
        }
        Ok(())
    }
}

impl ApiImpl {
    pub(super) async fn build_journal(&self) -> Result<&LocalKvStore> {
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
        template_id: &str,
        build_id: &str,
        body: &models::TemplateBuilderRequest,
    ) -> Result<models::TemplateBuilder, models::Error> {
        if template_id != build_id {
            return Err(Self::error(404, "template build not found"));
        }
        let id = SnapshotId::parse(build_id)
            .map_err(|_| Self::error(404, "template build not found"))?;
        // Reserve synchronously before spawning or touching durable state. Failed starts
        // release the slot; admitted builds hold it through publication and cleanup.
        let session = self.build_sessions.reserve(&id.to_string())?;
        let api = self.clone();
        let body = body.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = api.allocate_image_build(id.clone(), body, session).await;
            if result.is_err() {
                api.build_sessions
                    .active
                    .lock()
                    .unwrap()
                    .remove(&id.to_string());
            }
            // Finish allocation even if the request disappears, then release its worker.
            if let Err(Ok(_)) = sender.send(result) {
                let id = id.to_string();
                if let Err(error) = api.cancel_image_build(&id, &id).await {
                    warn!(build_id = %id, error = %error.message, "disconnected build cleanup will be retried");
                }
            }
        });
        receiver
            .await
            .map_err(|error| Self::error(500, error.to_string()))?
    }

    async fn allocate_image_build(
        &self,
        id: SnapshotId,
        body: models::TemplateBuilderRequest,
        session: BuildSession,
    ) -> Result<models::TemplateBuilder, models::Error> {
        if ConfigManager::global_config().template_build.cache_size_mb
            > self.volume_manager.limits().max_size_mb
        {
            return Err(Self::error(
                400,
                "template_build.cache_size_mb exceeds volume.max_size_mb",
            ));
        }
        let job = match self.build_lookup(&id.to_string(), &id.to_string()).await? {
            BuildLookup::ReadyTemplate(_) => {
                return Err(Self::error(409, "build has already started"));
            }
            BuildLookup::Job(job) => {
                if matches!(job.target, BuildTarget::Image { .. })
                    && (body.start_cmd.is_some() || body.ready_cmd.is_some())
                {
                    return Err(Self::error(
                        400,
                        "startCmd and readyCmd do not apply to imageOnly builds",
                    ));
                }
                if job.status != BuildStatus::Waiting {
                    return Err(Self::error(409, "build has already started"));
                }
                job
            }
        };
        let journal = self
            .build_journal()
            .await
            .map_err(|err| Self::internal_error(err.as_ref()))?;
        let meta = self.meta();
        // The SQL CAS also excludes starts on another node or through the legacy API.
        let started = meta
            .update_build_job(id.clone(), BuildJobTransition::Start)
            .await
            .map_err(Self::meta_error)?
            .is_some();
        if !started {
            let exists = meta
                .get_build_job(id.clone())
                .await
                .map_err(Self::meta_error)?
                .is_some();
            return Err(if exists {
                Self::error(409, "build has already started")
            } else {
                Self::error(404, "template build not found")
            });
        }
        let entry = BuildJournal {
            cache: format!("aenv-buildkit-work-{id}"),
            parent: None,
        };
        if let Err(err) = entry.persist(journal, &id.to_string()).await {
            let reason = BuildErrorReason::new("failed to persist builder allocation");
            let _ = meta
                .update_build_job(id.clone(), BuildJobTransition::Fail { reason })
                .await;
            return Err(Self::internal_error(err.as_ref()));
        }
        self.orchestrator
            .register_template_build(
                SandboxId::parse_str(&id.to_string()).expect("build ID is a UUID"),
            )
            .await;
        let api = self.clone();
        let build_id = id.to_string();
        tokio::spawn(async move {
            Box::pin(api.run_image_build(id, job, body, session, entry)).await;
        });
        Ok(models::TemplateBuilder::new(build_image_name(&build_id)))
    }

    /// Creates an imageOnly build. It publishes an image named by `name`
    /// instead of a template, and points the tags at it.
    pub(super) async fn create_image_build(
        &self,
        body: &models::TemplateBuildRequestV3,
    ) -> Result<models::TemplateRequestResponseV3, models::Error> {
        let name = body
            .name
            .as_deref()
            .ok_or_else(|| Self::error(400, "image name must be provided"))?;
        if body.cpu_count.is_some() || body.memory_mb.is_some() {
            return Err(Self::error(
                400,
                "cpuCount and memoryMB do not apply to imageOnly builds",
            ));
        }
        let (name, tag) = match name.split_once(':') {
            Some((name, tag)) => (name, Some(tag)),
            None => (name, None),
        };
        let name = ImageName::parse(name).map_err(|err| Self::error(400, err.to_string()))?;
        let mut tags = Vec::new();
        for tag in tag
            .into_iter()
            .chain(body.tags.iter().flatten().map(String::as_str))
        {
            let tag = ImageTag::new(&name, tag).map_err(|err| Self::error(400, err.to_string()))?;
            if !tags.iter().any(|existing| existing == tag.tag()) {
                tags.push(tag.tag().to_owned());
            }
        }
        if tags.is_empty() {
            tags.push(DEFAULT_IMAGE_TAG.to_owned());
        }
        let id = SnapshotId::generate();
        self.meta()
            .create_build_job(
                id.clone(),
                BuildTarget::Image {
                    name: name.to_string(),
                    tags: tags.clone(),
                },
            )
            .await
            .map_err(Self::meta_build_error)?;
        info!(build_id = %id, image = %name, ?tags, "image build created");
        Ok(models::TemplateRequestResponseV3::new(
            id.to_string(),
            id.to_string(),
            true,
            vec![name.to_string()],
            tags,
            Vec::new(),
        ))
    }

    fn session(&self, template_id: &str, build_id: &str) -> Result<BuildSession, models::Error> {
        if template_id != build_id {
            return Err(Self::error(404, "template build not found"));
        }
        self.build_sessions
            .active
            .lock()
            .unwrap()
            .get(build_id)
            .cloned()
            .ok_or_else(|| Self::error(404, "active template build not found"))
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
                self.build_lookup(template_id, build_id).await?;
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

    #[tracing::instrument(skip_all, fields(build_id = %build_id))]
    async fn run_image_build(
        &self,
        build_id: SnapshotId,
        job: BuildJobRecord,
        body: models::TemplateBuilderRequest,
        session: BuildSession,
        entry: BuildJournal,
    ) {
        let id = build_id.to_string();
        let deadline = Instant::now() + Duration::from_secs(body.timeout.unwrap_or(3600).into());
        let logs = self
            .build_logs
            .start(build_id.clone(), self.snapshot_manager.repository());
        info!(build_id = %id, "build starting");
        let logger = logs.logger.clone();
        let image_only = matches!(job.target, BuildTarget::Image { .. });
        let work = async {
            let (address, digest) = Box::pin(
                self.wait_for_image_build(&build_id, &body, &session, &entry, deadline, &logger),
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
                        build_startup_commands(&body, &context, resolved.raw_config.as_ref())?;
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
        let meta = self.meta();
        for tag in &image_tags {
            meta.put_image_tag(tag.clone(), digest.clone(), None)
                .await
                .context("put image tag in metadata service")?;
        }
        meta.update_build_job(
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

    async fn supervise_image_build(
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

    async fn finish_image_build(
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
    request: &models::TemplateBuilderRequest,
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
pub(crate) mod tests;
