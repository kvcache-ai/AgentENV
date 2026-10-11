use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use agentenv_http_server::apis::images::*;
use agentenv_http_server::models;
use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use futures::FutureExt;
use headers::Host;
use http::Method;
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

use super::ApiImpl;
use crate::{
    cfg::ConfigManager,
    image::{
        buildkit::{build_image_name, BuildkitContent},
        ResolvedBlockImage,
    },
    local_store::{LocalKvStore, LocalStoreDurability},
    snapshot::{
        CommandContext, RunnableSnapshot, SnapshotId, SnapshotRecord, TemplateBuildErrorReason,
    },
    template::{logs::BuildLogSession, TemplateBuildSpec},
    types::{ImageConfigs, SandboxId},
};

#[derive(Default)]
pub(crate) struct BuildSessions {
    active: StdMutex<HashMap<String, BuildSession>>,
    journal: OnceCell<LocalKvStore>,
    result_routes: Mutex<()>,
    builder_template: OnceCell<RunnableSnapshot>,
}

impl BuildSessions {
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
        self.active.lock().unwrap().contains_key(id)
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
    #[serde(default)]
    image_only: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ImageBuildResult {
    Waiting,
    Ready { digest: String },
    Error { reason: TemplateBuildErrorReason },
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
    Finished(Option<TemplateBuildErrorReason>),
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
        let record = self
            .snapshot_manager
            .get(id.to_string())
            .await
            .map_err(|err| Self::snapshot_manager_error(&err))?
            .ok_or_else(|| Self::error(404, "template build not found"))?;
        if super::template::template_build_status(&record)
            != crate::snapshot::TemplateBuildStatus::Waiting
        {
            return Err(Self::error(409, "build has already started"));
        }
        let journal = self
            .build_journal()
            .await
            .map_err(|err| Self::internal_error(err.as_ref()))?;
        // The repository CAS also excludes starts on another node or through the legacy API.
        let record = match self.snapshot_manager.try_start_build(&id).await {
            Ok(record) => record,
            Err(err) => {
                return Err(match err {
                    crate::snapshot::RepositoryError::SnapshotNotFound { .. } => {
                        Self::error(404, "template build not found")
                    }
                    crate::snapshot::RepositoryError::InvalidRequest { .. } => {
                        Self::error(409, "build has already started")
                    }
                    _ => Self::repository_error(&err),
                });
            }
        };
        let entry = BuildJournal {
            cache: format!("aenv-buildkit-work-{id}"),
            parent: None,
            image_only: false,
        };
        if let Err(err) = entry.persist(journal, &id.to_string()).await {
            let _ = self
                .snapshot_manager
                .mark_build_error(
                    &id,
                    TemplateBuildErrorReason::new("failed to persist builder allocation"),
                )
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
            api.run_image_build(id, Some(record), body, session, entry)
                .await;
        });
        Ok(models::TemplateBuilder::new(build_image_name(&build_id)))
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
        self.snapshot_manager
            .get(template_id)
            .await
            .map_err(|err| Self::snapshot_manager_error(&err))?
            .ok_or_else(|| Self::error(404, "template build not found"))?;
        self.cancel_build(template_id, build_id).await
    }

    async fn cancel_build(&self, template_id: &str, build_id: &str) -> Result<(), models::Error> {
        if template_id != build_id {
            return Err(Self::error(404, "template build not found"));
        }
        let session = match self.session(template_id, build_id) {
            Ok(session) => session,
            Err(_) => {
                if self
                    .image_build_info(build_id)
                    .await
                    .map_err(|err| Self::internal_error(err.as_ref()))?
                    .is_none()
                {
                    self.snapshot_manager
                        .get(build_id)
                        .await
                        .map_err(|err| Self::snapshot_manager_error(&err))?
                        .ok_or_else(|| Self::error(404, "template build not found"))?;
                }
                self.retry_image_build_cleanup(build_id)
                    .await
                    .map_err(|err| Self::error(500, format!("builder cleanup failed: {err:#}")))?;
                return self.delete_image_build_result(build_id).await;
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
        })??;
        self.delete_image_build_result(build_id).await
    }

    async fn start_published_image_build(
        &self,
        request: &models::ImageBuildRequest,
    ) -> Result<models::ImageBuilder, models::Error> {
        let config = ConfigManager::global_config();
        if config.template_build.cache_size_mb > self.volume_manager.limits().max_size_mb {
            return Err(Self::error(
                400,
                "template_build.cache_size_mb exceeds volume.max_size_mb",
            ));
        }
        let id = SnapshotId::generate();
        let build_id = id.to_string();
        let session = self.build_sessions.reserve(&build_id)?;
        let api = self.clone();
        let request = request.clone();
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = api
                .allocate_published_image_build(id, request, session)
                .await;
            if let Err(Ok(_)) = sender.send(result) {
                if let Err(error) = api.cancel_build(&build_id, &build_id).await {
                    warn!(%build_id, error = %error.message, "disconnected image build cleanup will be retried");
                }
            }
        });
        receiver
            .await
            .map_err(|error| Self::error(500, error.to_string()))?
    }

    async fn allocate_published_image_build(
        &self,
        id: SnapshotId,
        request: models::ImageBuildRequest,
        session: BuildSession,
    ) -> Result<models::ImageBuilder, models::Error> {
        let build_id = id.to_string();
        let mut body = models::TemplateBuilderRequest::new();
        body.timeout = request.timeout;
        let entry = BuildJournal {
            cache: format!("aenv-buildkit-work-{id}"),
            parent: None,
            image_only: true,
        };
        let persist = async {
            let journal = self.build_journal().await?;
            entry.persist(journal, &build_id).await?;
            journal
                .put(
                    format!("image/{id}"),
                    serde_json::to_vec(&ImageBuildResult::Waiting)?,
                )
                .await
        }
        .await;
        if let Err(error) = persist {
            session.state.send_replace(SessionState::Finished(Some(
                TemplateBuildErrorReason::new("builder allocation failed"),
            )));
            let _ = self.retry_image_build_cleanup(&build_id).await;
            self.build_sessions.active.lock().unwrap().remove(&build_id);
            return Err(Self::internal_error(error.as_ref()));
        }
        self.orchestrator
            .register_template_build(SandboxId::parse_str(&build_id).expect("generated UUID"))
            .await;
        let api = self.clone();
        tokio::spawn(async move {
            api.run_image_build(id, None, body, session, entry).await;
        });
        Ok(models::ImageBuilder::new(
            build_id.clone(),
            build_image_name(&build_id),
        ))
    }

    // A build task owns its result and diagnostics, independently of the
    // immutable image it publishes into the shared repository.
    pub(super) async fn image_build_info(
        &self,
        id: &str,
    ) -> Result<Option<models::ImageBuildInfo>> {
        let Some(result) = self
            .build_journal()
            .await?
            .get(format!("image/{id}"))
            .await?
        else {
            return Ok(None);
        };
        let mut info =
            models::ImageBuildInfo::new(id.to_owned(), models::ImageBuildStatus::Waiting);
        match serde_json::from_slice::<ImageBuildResult>(&result)? {
            ImageBuildResult::Waiting => {
                if self
                    .build_sessions
                    .active
                    .lock()
                    .unwrap()
                    .get(id)
                    .is_some_and(|session| {
                        matches!(
                            *session.state.borrow(),
                            SessionState::Ready(_) | SessionState::Publishing
                        )
                    })
                {
                    info.status = models::ImageBuildStatus::Building;
                }
            }
            ImageBuildResult::Ready { digest } => {
                info.status = models::ImageBuildStatus::Ready;
                info.image_digest = Some(digest);
            }
            ImageBuildResult::Error { reason } => {
                info.status = models::ImageBuildStatus::Error;
                info.reason = Some(models::BuildStatusReason {
                    message: reason.message,
                    step: reason.step,
                    log_entries: None,
                });
            }
        }
        Ok(Some(info))
    }

    async fn delete_image_build_result(&self, id: &str) -> Result<(), models::Error> {
        let _routes = self.build_sessions.result_routes.lock().await;
        if self
            .image_build_info(id)
            .await
            .map_err(|err| Self::internal_error(err.as_ref()))?
            .is_some()
        {
            // The repository removes orphaned build logs by ID as well. An
            // image build never creates a snapshot record or owns image layers.
            self.snapshot_manager
                .delete(id)
                .await
                .map_err(|err| Self::snapshot_manager_error(&err))?;
        }
        self.build_journal()
            .await
            .map_err(|err| Self::internal_error(err.as_ref()))?
            .delete(format!("image/{id}"))
            .await
            .map_err(|err| Self::internal_error(err.as_ref()))?;
        self.orchestrator
            .unregister_template_build(
                SandboxId::parse_str(id).map_err(|_| Self::error(404, "invalid build ID"))?,
            )
            .await;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(build_id = %build_id))]
    async fn run_image_build(
        &self,
        build_id: SnapshotId,
        template: Option<SnapshotRecord>,
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
        let work = async {
            let (address, digest) = self
                .wait_for_image_build(&build_id, &body, &session, &entry, deadline, &logger)
                .await?;
            let resolved = self.import_build_image(address, &digest).await?;
            if entry.image_only {
                let digest = self.image_resolver.publish_image(&resolved).await?;
                self.build_journal()
                    .await?
                    .put(
                        format!("image/{id}"),
                        serde_json::to_vec(&ImageBuildResult::Ready { digest })?,
                    )
                    .await?;
            }
            // Let buildctl receive its final Solve response before stopping BuildKit.
            // A client that keeps sockets open must not indefinitely delay publication.
            let _connections =
                tokio::time::timeout(Duration::from_secs(10), session.connections.write()).await;
            let cache_ready = self.release_builder(&id, &entry.cache).await?;
            if let Some(record) = template {
                let context = CommandContext::from(resolved.base_context);
                let (start, ready) =
                    build_startup_commands(&body, &context, resolved.raw_config.as_ref())?;
                let mut configs = ImageConfigs::new();
                if let Some(config) = resolved.raw_config {
                    configs.add(None::<String>, "/", config);
                }
                let mut spec = TemplateBuildSpec::new()
                    .with_logger(logger.clone())
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
                    .build_and_publish_with_id(
                        self.snapshot_manager.as_ref(),
                        record.id.clone(),
                        spec,
                    )
                    .await?;
            }
            if cache_ready {
                if let Err(error) = self.publish_build_cache(&id, &entry.cache).await {
                    warn!(build_id = %id, error = %format_args!("{error:#}"), "cache publication failed; keeping the previous cache seed");
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        self.supervise_image_build(&build_id, &session, logs, work)
            .await;
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
        self.finish_image_build(build_id, session, logs, result)
            .await;
    }

    async fn finish_image_build(
        &self,
        build_id: &SnapshotId,
        session: &BuildSession,
        logs: BuildLogSession,
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
                Some(TemplateBuildErrorReason::new(format!("{error:#}")))
            }
        };
        if reason.is_some() {
            // Failed exporters still have a final Solve error to deliver. Give
            // buildctl a bounded chance to print it before closing its tunnel.
            let _connections =
                tokio::time::timeout(Duration::from_secs(10), session.connections.write()).await;
        }
        // Flush diagnostics before finalizing the task and releasing its worker.
        if let Err(error) = logs.finish().await {
            warn!(build_id = %id, %error, "failed to persist final build logs");
        }
        session.state.send_replace(SessionState::Finished(reason));
        if let Err(error) = self.retry_image_build_cleanup(&id).await {
            warn!(build_id = %id, error = %format_args!("{error:#}"), "build finalization failed; cleanup will be retried");
        }
    }
}

#[async_trait]
impl Images<()> for ApiImpl {
    type Claims = super::Claims;

    async fn images_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query: &models::ImagesGetQueryParams,
    ) -> Result<ImagesGetResponse, ()> {
        use ImagesGetResponse::*;
        let limit = query.limit.unwrap_or(100) as usize;
        if !(1..=100).contains(&limit) {
            return Ok(Status400_BadRequest(Self::error(
                400,
                "limit must be between 1 and 100",
            )));
        }
        if let Some(cursor) = &query.next_token {
            if let Err(error) = crate::image::buildkit::validate_digest(cursor) {
                return Ok(Status400_BadRequest(Self::error(400, error.to_string())));
            }
        }
        Ok(
            match self
                .list_images_page(query.next_token.as_deref(), limit)
                .await
            {
                Ok(page) => Status200_PublishedImagePage(page),
                Err(error) => Status500_ServerError(Self::internal_error(error.as_ref())),
            },
        )
    }

    async fn images_image_digest_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path: &models::ImagesImageDigestGetPathParams,
    ) -> Result<ImagesImageDigestGetResponse, ()> {
        use ImagesImageDigestGetResponse::*;
        if let Err(error) = crate::image::buildkit::validate_digest(&path.image_digest) {
            return Ok(Status400_BadRequest(Self::error(400, error.to_string())));
        }
        let result: Result<Option<models::ImageDetails>> = async {
            let image = self
                .snapshot_manager
                .repository()
                .get_image(&path.image_digest)
                .await?;
            image
                .map(|image| {
                    Ok(models::ImageDetails::new(
                        path.image_digest.clone(),
                        agentenv_http_server::types::Object(serde_json::to_value(image)?),
                    ))
                })
                .transpose()
        }
        .await;
        Ok(match result {
            Ok(Some(image)) => Status200_PublishedImageDetails(image),
            Ok(None) => Status404_NotFound(Self::error(404, "image not found")),
            Err(error) => Status500_ServerError(Self::internal_error(error.as_ref())),
        })
    }

    async fn images_image_digest_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path: &models::ImagesImageDigestDeletePathParams,
    ) -> Result<ImagesImageDigestDeleteResponse, ()> {
        use ImagesImageDigestDeleteResponse::*;
        if let Err(error) = crate::image::buildkit::validate_digest(&path.image_digest) {
            return Ok(Status400_BadRequest(Self::error(400, error.to_string())));
        }
        Ok(
            match self
                .snapshot_manager
                .repository()
                .delete_image(&path.image_digest)
                .await
            {
                Ok(()) => Status204_ImageDeleted,
                Err(error) => Status500_ServerError(Self::internal_error(&error)),
            },
        )
    }

    async fn images_builds_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::ImageBuildRequest,
    ) -> Result<ImagesBuildsPostResponse, ()> {
        use ImagesBuildsPostResponse::*;
        Ok(match self.start_published_image_build(body).await {
            Ok(body) => Status202_BuilderPreparationAccepted {
                x_agentenv_build_id: Some(body.build_id.clone()),
                body,
            },
            Err(error) => match error.code {
                400 => Status400_BadRequest(error),
                429 => Status429_ConcurrentBuildLimitReached(error),
                _ => Status500_ServerError(error),
            },
        })
    }

    async fn images_builds_build_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path: &models::ImagesBuildsBuildIdGetPathParams,
    ) -> Result<ImagesBuildsBuildIdGetResponse, ()> {
        use ImagesBuildsBuildIdGetResponse::*;
        Ok(match self.image_build_info(&path.build_id).await {
            Ok(Some(mut info)) => {
                if info.status == models::ImageBuildStatus::Ready
                    && self.build_sessions.is_finishing(&path.build_id)
                {
                    // Sequential builds must observe the new cache seed, and
                    // their completed task must be ready for explicit deletion.
                    info.status = models::ImageBuildStatus::Building;
                    info.image_digest = None;
                }
                Status200_ImageBuildStatus(info)
            }
            Ok(None) => Status404_NotFound(Self::error(404, "image build not found")),
            Err(error) => Status500_ServerError(Self::internal_error(error.as_ref())),
        })
    }

    async fn images_builds_build_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path: &models::ImagesBuildsBuildIdDeletePathParams,
    ) -> Result<ImagesBuildsBuildIdDeleteResponse, ()> {
        use ImagesBuildsBuildIdDeleteResponse::*;
        let result = async {
            self.require_image_build(&path.build_id).await?;
            self.cancel_build(&path.build_id, &path.build_id).await
        }
        .await;
        Ok(match result {
            Ok(()) => Status204_BuilderReleased,
            Err(error) => match error.code {
                404 => Status404_NotFound(error),
                409 => Status409_Conflict(error),
                _ => Status500_ServerError(error),
            },
        })
    }

    async fn images_builds_build_id_logs_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path: &models::ImagesBuildsBuildIdLogsGetPathParams,
        query: &models::ImagesBuildsBuildIdLogsGetQueryParams,
    ) -> Result<ImagesBuildsBuildIdLogsGetResponse, ()> {
        use ImagesBuildsBuildIdLogsGetResponse::*;
        let result = async {
            let id = self.require_image_build(&path.build_id).await?;
            self.query_build_logs(
                &id,
                query.offset.unwrap_or(0) as usize,
                None,
                query.limit.unwrap_or(100) as usize,
                false,
                None,
                None,
            )
            .await
        }
        .await;
        Ok(match result {
            Ok(logs) => Status200_ImageBuildLogs(logs),
            Err(error) => match error.code {
                400 => Status400_BadRequest(error),
                404 => Status404_NotFound(error),
                _ => Status500_ServerError(error),
            },
        })
    }
}

impl ApiImpl {
    async fn list_images_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<models::ImagePage> {
        let repository = self.snapshot_manager.repository();
        let mut digests = repository.list_image_digests(after, limit + 1).await?;
        let has_more = digests.len() > limit;
        digests.truncate(limit);
        let mut page = models::ImagePage::new(Vec::with_capacity(digests.len()));
        // A concurrent delete can remove a listed record. Advance past scanned
        // keys even when a page becomes empty, so pagination always makes progress.
        page.next_token = has_more.then(|| digests.last().cloned()).flatten();
        for digest in digests {
            if let Some(image) = repository.get_image(&digest).await? {
                page.images.push(models::ImageSummary::new(
                    digest,
                    image.os,
                    image.architecture,
                    image.layers.len() as u32,
                ));
            }
        }
        Ok(page)
    }

    async fn require_image_build(&self, id: &str) -> Result<SnapshotId, models::Error> {
        let parsed =
            SnapshotId::parse(id).map_err(|_| Self::error(404, "image build not found"))?;
        self.image_build_info(id)
            .await
            .map_err(|error| Self::internal_error(error.as_ref()))?
            .ok_or_else(|| Self::error(404, "image build not found"))?;
        Ok(parsed)
    }
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
mod tests;
