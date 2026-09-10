use std::{net::SocketAddr, sync::Arc, time::Duration};

use agentenv_http_server::models;
use anyhow::{ensure, Context, Result};
use dashmap::DashMap;
use futures::FutureExt;
use tokio::{
    sync::{oneshot, watch, Mutex, OnceCell},
    time::Instant,
};
use tracing::{info, warn};

mod cache;
mod cleanup;
mod transport;
mod worker;
pub(crate) use transport::router;

use super::{template_helpers::template_build_record_from_v3_request, ApiImpl};
use crate::{
    cfg::ConfigManager,
    image::buildkit::{validate_digest, BuildkitContent},
    local_store::{LocalKvStore, LocalStoreDurability},
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
            let (address, digest) = self
                .wait_for_image_build(&record, &body, &session, &entry, deadline)
                .await?;
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

#[cfg(test)]
mod tests;
