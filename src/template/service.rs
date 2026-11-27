use std::sync::Arc;

use agentenv_domain::BuildJobTransition;
use agentenv_metadata::{MetaClient, MetaError};
use futures::FutureExt;
use tracing::{info, warn, Instrument};

use crate::image::{oci_image::RegistryAuth, ImageResolver, ResolvedBlockImage};
use crate::logging::LogLevel;
use crate::orchestrator::Orchestrator;
use crate::repository::RepositoryError;
use crate::snapshot::{
    BuildErrorReason, BuildJobRecord, BuildTarget, SnapshotAlias, SnapshotId, SnapshotManager,
    SnapshotRecord, SnapshotSource,
};
use crate::template::{
    logs::{delete_build_logs, read_build_logs, BuildLogEntry, BuildLogs},
    TemplateBuildError, TemplateBuildFailure, TemplateBuildSpec, TemplateBuilder,
    TemplatePipelineError,
};
use crate::types::{ImageConfigs, SandboxId, SandboxResources};

/// Transport-agnostic error returned by template and image build services.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BuildServiceError {
    #[error("{0}")]
    InvalidInput(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    TooManyRequests(String),
    #[error("{0}")]
    UpstreamFailed(String),
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    UpstreamTimeout(String),
    #[error("{0}")]
    Internal(String),
}

impl BuildServiceError {
    pub fn internal_from(err: &dyn std::error::Error) -> Self {
        let mut message = err.to_string();
        let mut current = err.source();
        while let Some(source) = current {
            let cause = source.to_string();
            if cause != message {
                message.push_str(": ");
                message.push_str(&cause);
            }
            current = source.source();
        }
        Self::Internal(message)
    }

    pub fn from_meta(err: MetaError) -> Self {
        match &err {
            MetaError::InvalidRequest { .. } => Self::InvalidInput(err.to_string()),
            MetaError::VolumeNotFound { .. } => Self::NotFound(err.to_string()),
            MetaError::SnapshotConflict { .. }
            | MetaError::AliasConflict { .. }
            | MetaError::VolumeNameConflict { .. }
            | MetaError::VolumeReserved { .. }
            | MetaError::BuildConflict { .. } => Self::Conflict(err.to_string()),
            MetaError::MissingLayers { .. } | MetaError::Unavailable { .. } => {
                Self::Unavailable(err.to_string())
            }
            MetaError::Backend { .. } => Self::internal_from(&err),
        }
    }

    pub fn from_meta_build(err: MetaError) -> Self {
        match &err {
            MetaError::InvalidRequest { .. }
            | MetaError::AliasConflict { .. }
            | MetaError::BuildConflict { .. }
            | MetaError::SnapshotConflict { .. } => Self::InvalidInput(err.to_string()),
            _ => Self::from_meta(err),
        }
    }

    pub fn from_repository(err: &RepositoryError) -> Self {
        match err {
            RepositoryError::InvalidRequest { .. } => Self::InvalidInput(err.to_string()),
            RepositoryError::SnapshotNotFound { .. }
            | RepositoryError::ArtifactNotFound { .. }
            | RepositoryError::ManagedLayerNotFound { .. } => Self::NotFound(err.to_string()),
            RepositoryError::AliasConflict { .. } | RepositoryError::IntegrityMismatch { .. } => {
                Self::Conflict(err.to_string())
            }
            RepositoryError::Unavailable { .. } => Self::Unavailable(err.to_string()),
            RepositoryError::Unsupported { .. } => Self::Internal(err.to_string()),
            RepositoryError::Backend { .. } => Self::internal_from(err),
        }
    }

    pub fn from_reusable_repository(err: &RepositoryError) -> Self {
        match err {
            RepositoryError::Backend { .. } | RepositoryError::Unavailable { .. } => {
                Self::Unavailable(err.to_string())
            }
            _ => Self::from_repository(err),
        }
    }

    pub fn from_snapshot_manager(err: &anyhow::Error) -> Self {
        match err
            .chain()
            .find_map(|cause| cause.downcast_ref::<RepositoryError>())
        {
            Some(error) => Self::from_repository(error),
            None => Self::internal_from(err.as_ref()),
        }
    }

    pub fn from_reusable_snapshot_manager(err: &anyhow::Error) -> Self {
        match err
            .chain()
            .find_map(|cause| cause.downcast_ref::<RepositoryError>())
        {
            Some(error) => Self::from_reusable_repository(error),
            None => Self::internal_from(err.as_ref()),
        }
    }
}

/// Durable lookup result for build status, log, and builder endpoints.
pub enum BuildLookup {
    Job(BuildJobRecord),
    ReadyTemplate(Box<SnapshotRecord>),
}

impl BuildLookup {
    pub fn id(&self) -> &SnapshotId {
        match self {
            Self::Job(job) => &job.build_id,
            Self::ReadyTemplate(record) => &record.id,
        }
    }
}

/// Source selection for build log queries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BuildLogSource {
    #[default]
    Any,
    Temporary,
    Persistent,
}

/// Query parameters for reading build logs.
#[derive(Clone, Debug, Default)]
pub struct BuildLogQuery {
    pub offset: usize,
    pub cursor: Option<i64>,
    pub limit: usize,
    pub backward: bool,
    pub level: Option<LogLevel>,
    pub source: BuildLogSource,
}

/// Base source for a v2 template build.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TemplateBuildBaseSource {
    DefaultImage,
    Image(String),
    Template(SnapshotAlias),
}

/// Shared durable build lookup used by both template and image build flows.
pub(crate) async fn lookup_build(
    meta: &MetaClient,
    snapshot_manager: &SnapshotManager,
    template: &str,
    build: &str,
) -> Result<BuildLookup, BuildServiceError> {
    if template != build {
        return Err(BuildServiceError::NotFound(
            "build not found for template".to_owned(),
        ));
    }
    let id = SnapshotId::parse(build)
        .map_err(|_| BuildServiceError::InvalidInput("invalid buildID".to_owned()))?;
    if let Some(job) = meta
        .get_build_job(id)
        .await
        .map_err(BuildServiceError::from_meta)?
    {
        return Ok(BuildLookup::Job(job));
    }
    if let Some(record) = snapshot_manager
        .get(build)
        .await
        .map_err(|e| BuildServiceError::from_snapshot_manager(&e))?
    {
        if matches!(record.config.source, SnapshotSource::Template) {
            return Ok(BuildLookup::ReadyTemplate(Box::new(record)));
        }
    }
    Err(BuildServiceError::NotFound(
        "template build not found".to_owned(),
    ))
}

/// Domain service owning v2/v3 template build orchestration, deletion sagas,
/// build lookups, and build log queries.
#[derive(Clone)]
pub struct TemplateBuildService {
    orchestrator: Arc<Orchestrator>,
    snapshot_manager: Arc<SnapshotManager>,
    template_builder: Arc<TemplateBuilder>,
    image_resolver: Arc<ImageResolver>,
    meta: MetaClient,
    build_logs: BuildLogs,
}

impl TemplateBuildService {
    pub fn new(
        orchestrator: Arc<Orchestrator>,
        snapshot_manager: Arc<SnapshotManager>,
        template_builder: Arc<TemplateBuilder>,
        image_resolver: Arc<ImageResolver>,
        meta: MetaClient,
        build_logs: BuildLogs,
    ) -> Self {
        Self {
            orchestrator,
            snapshot_manager,
            template_builder,
            image_resolver,
            meta,
            build_logs,
        }
    }

    pub fn meta(&self) -> &MetaClient {
        &self.meta
    }

    pub fn build_logs(&self) -> &BuildLogs {
        &self.build_logs
    }

    #[cfg(test)]
    pub(crate) fn with_build_logs(&self, build_logs: BuildLogs) -> Self {
        let mut cloned = self.clone();
        cloned.build_logs = build_logs;
        cloned
    }

    pub async fn build_lookup(
        &self,
        template: &str,
        build: &str,
    ) -> Result<BuildLookup, BuildServiceError> {
        lookup_build(&self.meta, &self.snapshot_manager, template, build).await
    }

    pub async fn create_template_build(
        &self,
        target: BuildTarget,
    ) -> Result<BuildJobRecord, BuildServiceError> {
        let build_id = SnapshotId::generate();
        self.meta
            .create_build_job(build_id, target)
            .await
            .map_err(BuildServiceError::from_meta_build)
    }

    pub async fn get_template_build_job(
        &self,
        id: SnapshotId,
    ) -> Result<Option<BuildJobRecord>, BuildServiceError> {
        self.meta
            .get_build_job(id)
            .await
            .map_err(BuildServiceError::from_meta)
    }

    pub async fn load_template_build_target(
        &self,
        template_id: &str,
        build_id: &str,
    ) -> Result<(SnapshotId, Option<SnapshotAlias>, SandboxResources), BuildServiceError> {
        if template_id != build_id {
            return Err(BuildServiceError::InvalidInput(
                "templateID and buildID must match in AgentENV compatibility mode".to_owned(),
            ));
        }

        let parsed_id = SnapshotId::parse(build_id)
            .map_err(|_| BuildServiceError::InvalidInput(format!("invalid buildID: {build_id}")))?;
        match self
            .meta
            .get_build_job(parsed_id.clone())
            .await
            .map_err(BuildServiceError::from_meta)?
        {
            Some(BuildJobRecord {
                target: BuildTarget::Template { alias, resources },
                ..
            }) => Ok((parsed_id, alias, resources)),
            _ => Err(BuildServiceError::NotFound(format!(
                "template {template_id} not found"
            ))),
        }
    }

    pub(crate) async fn start_v2_build(
        &self,
        build_id: SnapshotId,
        base_source: TemplateBuildBaseSource,
        registry_auth: Option<RegistryAuth>,
        spec: TemplateBuildSpec,
        image_builds: Arc<super::ImageBuildService>,
    ) -> Result<(), BuildServiceError> {
        let Some(v2_guard) = image_builds.track_v2_build(build_id.to_string()) else {
            return Err(BuildServiceError::InvalidInput(
                "template build has already started".to_owned(),
            ));
        };
        let journal = image_builds
            .build_journal()
            .await
            .map_err(|err| BuildServiceError::internal_from(err.as_ref()))?;
        let started = self
            .meta
            .update_build_job(build_id.clone(), BuildJobTransition::Start)
            .await
            .map_err(BuildServiceError::from_meta)?;
        if started.is_none() {
            return Err(BuildServiceError::InvalidInput(
                "template build has already started".to_owned(),
            ));
        }
        let journal_key = format!("v2build/{build_id}");
        if let Err(err) = journal.put(journal_key.clone(), Vec::new()).await {
            let _ = self
                .meta
                .update_build_job(
                    build_id.clone(),
                    BuildJobTransition::Fail {
                        reason: BuildErrorReason::new("failed to persist template build start"),
                    },
                )
                .await;
            return Err(BuildServiceError::internal_from(err.as_ref()));
        }

        let logs = self
            .build_logs
            .start(build_id.clone(), self.snapshot_manager.repository());
        let spec = spec.with_logger(logs.logger.clone());
        let sandbox_id = SandboxId::parse_str(&build_id.to_string()).expect("build ID is a UUID");
        self.orchestrator.register_template_build(sandbox_id).await;
        let service = self.clone();
        tokio::spawn(async move {
            let span = tracing::info_span!("template_build", build_id = %build_id);
            async move {
                let work = service.run_v2_build(&build_id, base_source, registry_auth, spec);
                let failure_reason = match std::panic::AssertUnwindSafe(work).catch_unwind().await {
                    Ok(reason) => reason,
                    Err(_) => Some(BuildErrorReason::new("template build worker panicked")),
                };
                let terminal_persisted = match failure_reason {
                    None => true,
                    Some(reason) => {
                        if service.mark_v2_build_error(&build_id, reason.clone()).await {
                            true
                        } else {
                            if let (Ok(journal), Ok(bytes)) = (
                                image_builds.build_journal().await,
                                serde_json::to_vec(&reason),
                            ) {
                                let _ = journal.put(journal_key.clone(), bytes).await;
                            }
                            false
                        }
                    }
                };
                if let Err(error) = logs.finish().await {
                    warn!(%build_id, %error, "failed to persist final build logs");
                }
                service
                    .orchestrator
                    .unregister_template_build(sandbox_id)
                    .await;
                if terminal_persisted {
                    match image_builds.build_journal().await {
                        Ok(journal) => {
                            if let Err(error) = journal.delete(journal_key).await {
                                warn!(%build_id, %error, "failed to clear v2 build journal entry");
                            }
                        }
                        Err(error) => {
                            warn!(%build_id, %error, "failed to open v2 build journal for cleanup");
                        }
                    }
                } else {
                    warn!(
                        %build_id,
                        "retaining v2 build journal entry so background recovery retries error persistence"
                    );
                }
                drop(v2_guard);
            }
            .instrument(span)
            .await;
        });

        Ok(())
    }

    async fn run_v2_build(
        &self,
        build_id: &SnapshotId,
        base_source: TemplateBuildBaseSource,
        registry_auth: Option<RegistryAuth>,
        spec: TemplateBuildSpec,
    ) -> Option<BuildErrorReason> {
        info!(build_id = %build_id, "template build started");
        match base_source {
            source
            @ (TemplateBuildBaseSource::DefaultImage | TemplateBuildBaseSource::Image(_)) => {
                let requested_image = match source {
                    TemplateBuildBaseSource::Image(image) => Some(image),
                    _ => None,
                };
                let resolved_rootfs = match self
                    .resolve_template_rootfs_image(
                        requested_image.as_deref(),
                        registry_auth.as_ref(),
                    )
                    .await
                {
                    Ok(resolved) => resolved,
                    Err(err) => {
                        let message = err.to_string();
                        warn!(
                            build_id = %build_id,
                            reason = %message,
                            "template build failed while resolving the base image"
                        );
                        return Some(BuildErrorReason::new(message));
                    }
                };
                info!(
                    build_id = %build_id,
                    requested_image,
                    "template build base image resolved"
                );
                let mut image_configs = ImageConfigs::new();
                if let Some(config) = &resolved_rootfs.raw_config {
                    image_configs.add(None::<String>, "/", config.clone());
                }
                let spec = spec
                    .with_resolved_overlaybd_image(
                        resolved_rootfs.overlaybd_config_path,
                        image_configs,
                    )
                    .with_base_context(resolved_rootfs.base_context.into());
                match self
                    .template_builder
                    .build_and_publish_with_id(
                        self.snapshot_manager.as_ref(),
                        build_id.clone(),
                        spec,
                    )
                    .await
                {
                    Ok(_) => {
                        info!(build_id = %build_id, "template build completed");
                        None
                    }
                    Err(error) => Some(handle_v2_pipeline_error(build_id, None, &error)),
                }
            }
            TemplateBuildBaseSource::Template(alias) => {
                let base_runnable = match self.snapshot_manager.load_runnable(alias.as_ref()).await
                {
                    Ok(Some(runnable)) => runnable,
                    Ok(None) => {
                        warn!(
                            build_id = %build_id,
                            base_template = %alias,
                            reason = "base template alias not found",
                            "template build failed while resolving the base template"
                        );
                        return Some(BuildErrorReason::new(format!(
                            "template alias not found: {alias}"
                        )));
                    }
                    Err(err) => {
                        warn!(
                            build_id = %build_id,
                            base_template = %alias,
                            error = %format_args!("{err:#}"),
                            "template build failed while loading the base template"
                        );
                        let message = BuildServiceError::from_snapshot_manager(&err).to_string();
                        return Some(BuildErrorReason::new(message));
                    }
                };
                info!(
                    build_id = %build_id,
                    base_template = %alias,
                    base_snapshot_id = %base_runnable.id,
                    "template build base template resolved"
                );
                match self
                    .template_builder
                    .build_from_snapshot_and_publish(
                        self.snapshot_manager.as_ref(),
                        spec,
                        build_id.clone(),
                        &base_runnable,
                    )
                    .await
                {
                    Ok(_) => {
                        info!(build_id = %build_id, "template build completed");
                        None
                    }
                    Err(error) => Some(handle_v2_pipeline_error(build_id, Some(&alias), &error)),
                }
            }
        }
    }

    #[tracing::instrument(skip(self, registry_auth))]
    async fn resolve_template_rootfs_image(
        &self,
        requested_image: Option<&str>,
        registry_auth: Option<&RegistryAuth>,
    ) -> Result<ResolvedBlockImage, BuildServiceError> {
        let image_ref = requested_image
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| self.image_resolver.default_image());
        self.image_resolver
            .resolve_with_registry_auth(image_ref, registry_auth)
            .await
            .map_err(|err| {
                if err.is_user_error() {
                    BuildServiceError::InvalidInput(format!("{err:#}"))
                } else {
                    BuildServiceError::Internal(format!("{err:#}"))
                }
            })
    }

    async fn mark_v2_build_error(&self, build_id: &SnapshotId, reason: BuildErrorReason) -> bool {
        if let Err(error) = self
            .meta
            .update_build_job(build_id.clone(), BuildJobTransition::Fail { reason })
            .await
        {
            warn!(
                build_id = %build_id,
                error = %error,
                "failed to persist template build error status"
            );
            return false;
        }
        true
    }

    pub async fn delete_template(
        &self,
        template_ref: &str,
        has_active_build: impl Fn(&str) -> bool,
    ) -> Result<(), BuildServiceError> {
        let parsed_id = SnapshotId::parse(template_ref).ok();
        if let Some(id) = parsed_id.as_ref() {
            if has_active_build(&id.to_string()) {
                return Err(BuildServiceError::Conflict(
                    "template build is active; cancel the builder or wait for it to finish"
                        .to_owned(),
                ));
            }
        }

        let record = match self.snapshot_manager.get_metadata(template_ref).await {
            Ok(Some(record)) if has_active_build(&record.id.to_string()) => {
                return Err(BuildServiceError::Conflict(
                    "template build is active; cancel the builder or wait for it to finish"
                        .to_owned(),
                ));
            }
            Err(err) => return Err(BuildServiceError::from_reusable_snapshot_manager(&err)),
            Ok(Some(record)) => Some(record),
            Ok(None) => None,
        };

        if let Some(record) = record {
            let deleted_id = record.id.clone();
            if let Err(err) = self.snapshot_manager.delete(deleted_id.to_string()).await {
                return Err(BuildServiceError::from_reusable_snapshot_manager(&err));
            }
            self.meta
                .delete_build_job(deleted_id.clone(), false)
                .await
                .map_err(BuildServiceError::from_meta)?;
            self.cleanup_build_logs(&deleted_id).await;
            return Ok(());
        }

        if let Some(id) = parsed_id {
            let job = self
                .meta
                .get_build_job(id.clone())
                .await
                .map_err(BuildServiceError::from_meta)?;
            if let Some(job) = job {
                let reserve_tombstone = matches!(job.target, BuildTarget::Template { .. });
                self.meta
                    .delete_build_job(id.clone(), reserve_tombstone)
                    .await
                    .map_err(BuildServiceError::from_meta)?;
            } else if let Err(err) = self.snapshot_manager.delete(id.to_string()).await {
                return Err(BuildServiceError::from_reusable_snapshot_manager(&err));
            }
            self.cleanup_build_logs(&id).await;
        }

        Ok(())
    }

    pub async fn cleanup_build_logs(&self, id: &SnapshotId) {
        self.build_logs.remove(id);
        let _ = delete_build_logs(self.snapshot_manager.repository().as_ref(), id).await;
    }

    pub async fn query_build_logs(
        &self,
        id: &SnapshotId,
        query: BuildLogQuery,
    ) -> Result<Vec<BuildLogEntry>, BuildServiceError> {
        if query.limit > 100 {
            return Err(BuildServiceError::InvalidInput(
                "limit must be between 0 and 100".to_owned(),
            ));
        }
        if query.limit == 0 {
            return Ok(Vec::new());
        }
        let temporary = if query.source != BuildLogSource::Persistent {
            self.build_logs.temporary(id)
        } else {
            None
        };
        let mut entries = match temporary {
            Some(entries) => entries,
            None if query.source == BuildLogSource::Temporary => Vec::new(),
            None => read_build_logs(self.snapshot_manager.repository().as_ref(), id)
                .await
                .map_err(|e| BuildServiceError::from_repository(&e))?,
        };
        if query.backward {
            entries.reverse();
        }
        Ok(entries
            .into_iter()
            .filter(|entry| {
                query.level.is_none_or(|level| entry.level >= level)
                    && query.cursor.is_none_or(|cursor| {
                        let boundary = chrono::DateTime::from_timestamp_millis(cursor)
                            .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC);
                        if query.backward {
                            entry.timestamp <= boundary
                        } else {
                            entry.timestamp >= boundary
                        }
                    })
            })
            .skip(query.offset)
            .take(query.limit)
            .collect())
    }

    pub async fn query_build_reason_logs(
        &self,
        id: &SnapshotId,
        step: &str,
    ) -> Result<Vec<BuildLogEntry>, BuildServiceError> {
        let entries = match self.build_logs.temporary(id) {
            Some(entries) => entries,
            None => read_build_logs(self.snapshot_manager.repository().as_ref(), id)
                .await
                .map_err(|error| BuildServiceError::from_repository(&error))?,
        };
        Ok(entries
            .into_iter()
            .filter(|entry| entry.step.as_deref() == Some(step) && entry.level >= LogLevel::Warn)
            .take(100)
            .collect())
    }
}

pub(crate) fn pipeline_build_error(err: &TemplatePipelineError) -> BuildServiceError {
    match err {
        TemplatePipelineError::Build(TemplateBuildError::InvalidInput { reason }) => {
            BuildServiceError::InvalidInput(reason.clone())
        }
        TemplatePipelineError::Build(TemplateBuildError::System { reason, .. }) => {
            BuildServiceError::Internal(reason.message.clone())
        }
        TemplatePipelineError::Repository(repository) => match repository {
            RepositoryError::InvalidRequest { .. }
            | RepositoryError::AliasConflict { .. }
            | RepositoryError::IntegrityMismatch { .. } => {
                BuildServiceError::InvalidInput(repository.to_string())
            }
            _ => BuildServiceError::Internal(repository.to_string()),
        },
    }
}

fn pipeline_error_reason(err: &TemplatePipelineError) -> BuildErrorReason {
    match err {
        TemplatePipelineError::Build(TemplateBuildError::InvalidInput { reason }) => {
            BuildErrorReason::new(reason.clone())
        }
        TemplatePipelineError::Build(TemplateBuildError::System { reason, .. }) => reason.clone(),
        TemplatePipelineError::Repository(repository) => {
            BuildErrorReason::new(repository.to_string())
        }
    }
}

fn pipeline_error_chain(err: &TemplatePipelineError) -> Option<String> {
    let client_message = pipeline_error_reason(err).message;
    let outer_message = err.to_string();
    let mut causes = Vec::new();
    let mut current = std::error::Error::source(err);
    while let Some(source) = current {
        let cause = source.to_string();
        if cause != outer_message
            && cause != client_message
            && !cause.starts_with("template build failed:")
            && source.downcast_ref::<TemplateBuildFailure>().is_none()
            && !causes.contains(&cause)
        {
            causes.push(cause);
        }
        current = source.source();
    }
    (!causes.is_empty()).then(|| causes.join(": "))
}

fn pipeline_error_is_internal(err: &TemplatePipelineError) -> bool {
    if matches!(
        err,
        TemplatePipelineError::Build(TemplateBuildError::InvalidInput { .. })
    ) {
        return false;
    }

    let causes = std::iter::successors(std::error::Error::source(err), |cause| cause.source());
    if causes
        .into_iter()
        .any(|cause| cause.downcast_ref::<TemplateBuildFailure>().is_some())
    {
        return false;
    }

    !matches!(
        pipeline_build_error(err),
        BuildServiceError::InvalidInput(_)
    )
}

fn handle_v2_pipeline_error(
    build_id: &SnapshotId,
    base_template: Option<&SnapshotAlias>,
    err: &TemplatePipelineError,
) -> BuildErrorReason {
    let error = pipeline_build_error(err);
    let reason = pipeline_error_reason(err);
    if pipeline_error_is_internal(err) {
        let internal_causes = pipeline_error_chain(err);
        if let Some(base_template) = base_template {
            warn!(
                build_id = %build_id,
                base_template = %base_template,
                reason = %error,
                failed_step = ?reason.step,
                internal_causes = ?internal_causes,
                "snapshot-based template build failed"
            );
        } else {
            warn!(
                build_id = %build_id,
                reason = %error,
                failed_step = ?reason.step,
                internal_causes = ?internal_causes,
                "template build failed"
            );
        }
    }
    reason
}

#[cfg(test)]
mod tests {
    use super::{pipeline_build_error, BuildServiceError};

    #[test]
    fn pipeline_build_error_maps_invalid_input_to_bad_request() {
        let error = pipeline_build_error(&crate::template::TemplatePipelineError::Build(
            crate::template::TemplateBuildError::invalid_input("bad input"),
        ));

        assert_eq!(error, BuildServiceError::InvalidInput("bad input".into()));
    }

    #[test]
    fn pipeline_build_error_maps_repository_conflict_to_bad_request() {
        let error = pipeline_build_error(&crate::template::TemplatePipelineError::Repository(
            crate::repository::RepositoryError::AliasConflict {
                alias: "dup".to_string(),
                existing: crate::snapshot::SnapshotId::generate(),
                new_id: crate::snapshot::SnapshotId::generate(),
            },
        ));

        assert!(matches!(error, BuildServiceError::InvalidInput(msg) if msg.contains("dup")));
    }
}
