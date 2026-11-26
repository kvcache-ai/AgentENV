use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use chrono::TimeZone;
use futures::FutureExt;
use headers::Host;
use http::Method;
use std::time::SystemTime;
use tracing::{info, warn, Instrument};

use agentenv_domain::BuildJobTransition;
use agentenv_http_server::apis::templates::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::pagination::PaginationCursor;
use super::template_helpers::{
    template_build_spec_from_start_request, template_build_start_base_source,
    template_build_target_from_v3_request, template_registry_auth, TemplateBuildStartBaseSource,
};
use super::ApiImpl;
use crate::image::ResolvedBlockImage;
use crate::logging::LogLevel;
use crate::snapshot::{
    BuildErrorReason, BuildJobRecord, BuildStatus, BuildTarget, SnapshotAlias, SnapshotId,
    SnapshotListFilter, SnapshotRecord, SnapshotSource,
};
use crate::template::{
    logs::{read_build_logs, BuildLogEntry},
    TemplateBuildError, TemplateBuildFailure, TemplatePipelineError,
};
use crate::types::ImageConfigs;

fn pipeline_build_error(err: &TemplatePipelineError) -> models::Error {
    match err {
        TemplatePipelineError::Build(TemplateBuildError::InvalidInput { reason }) => {
            models::Error::new(400, reason.clone())
        }
        TemplatePipelineError::Build(TemplateBuildError::System { reason, .. }) => {
            models::Error::new(500, reason.message.clone())
        }
        TemplatePipelineError::Repository(repository) => {
            ApiImpl::bad_request_for_repository_build_error(repository)
                .unwrap_or_else(|| models::Error::new(500, repository.to_string()))
        }
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

    pipeline_build_error(err).code >= 500
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
                reason = %error.message,
                failed_step = ?reason.step,
                internal_causes = ?internal_causes,
                "snapshot-based template build failed"
            );
        } else {
            warn!(
                build_id = %build_id,
                reason = %error.message,
                failed_step = ?reason.step,
                internal_causes = ?internal_causes,
                "template build failed"
            );
        }
    }
    reason
}

#[tracing::instrument(skip(api, registry_auth))]
async fn resolve_template_rootfs_image(
    api: &ApiImpl,
    requested_image: Option<&str>,
    registry_auth: Option<&crate::image::oci_image::RegistryAuth>,
) -> Result<ResolvedBlockImage, models::Error> {
    let image_resolver = api.image_resolver();
    let image_ref = requested_image
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| image_resolver.default_image());
    image_resolver
        .resolve_with_registry_auth(image_ref, registry_auth)
        .await
        .map_err(|err| {
            models::Error::new(
                if err.is_user_error() { 400 } else { 500 },
                format!("{err:#}"),
            )
        })
}

fn build_status_response(status: BuildStatus) -> models::TemplateBuildStatus {
    match status {
        BuildStatus::Waiting => models::TemplateBuildStatus::Waiting,
        BuildStatus::Building => models::TemplateBuildStatus::Building,
        BuildStatus::Ready => models::TemplateBuildStatus::Ready,
        BuildStatus::Error => models::TemplateBuildStatus::Error,
    }
}

fn datetime_from_unix_ms(unix_ms: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc
        .timestamp_millis_opt(unix_ms)
        .single()
        .unwrap_or_else(chrono::Utc::now)
}

fn build_record_names(record: &SnapshotRecord) -> Vec<String> {
    record
        .alias
        .as_ref()
        .map(|alias| vec![alias.to_string()])
        .unwrap_or_else(|| vec![record.id.to_string()])
}

fn build_record_envd_version(record: &SnapshotRecord) -> String {
    record
        .manifest
        .as_ref()
        .map(|_| record.config.runtime_versions.envd_version.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

fn template_from_record(record: SnapshotRecord) -> models::Template {
    let created_at = datetime_from_unix_ms(record.created_at_unix_ms);
    let updated_at = datetime_from_unix_ms(record.updated_at_unix_ms);
    models::Template::new(
        record.id.to_string(),
        record.id.to_string(),
        record.config.resources.cpu_count,
        record.config.resources.memory_mib,
        record.config.resources.disk_size_mib,
        true,
        build_record_names(&record),
        created_at,
        updated_at,
        Nullable::Null,
        0,
        1,
        build_record_envd_version(&record),
        models::TemplateBuildStatus::Ready,
    )
}

fn template_build_from_record(record: &SnapshotRecord) -> models::TemplateBuild {
    let created_at = datetime_from_unix_ms(record.created_at_unix_ms);
    let updated_at = datetime_from_unix_ms(record.updated_at_unix_ms);
    let build_id = *record.id.as_uuid();
    let finished_at = match &record.config.source {
        SnapshotSource::Template => Some(updated_at),
        SnapshotSource::Sandbox { .. } | SnapshotSource::Builder => None,
    };
    models::TemplateBuild {
        build_id,
        status: models::TemplateBuildStatus::Ready,
        created_at,
        updated_at,
        finished_at,
        cpu_count: record.config.resources.cpu_count,
        memory_mb: record.config.resources.memory_mib,
        disk_size_mb: Some(record.config.resources.disk_size_mib),
        envd_version: record
            .manifest
            .as_ref()
            .map(|_| record.config.runtime_versions.envd_version.clone()),
    }
}

fn template_build_from_job(job: &BuildJobRecord) -> models::TemplateBuild {
    let created_at = datetime_from_unix_ms(job.created_at_unix_ms);
    let updated_at = datetime_from_unix_ms(job.updated_at_unix_ms);
    let resources = match &job.target {
        BuildTarget::Template { resources, .. } => *resources,
        BuildTarget::Image { .. } => crate::types::SandboxResources::default(),
    };
    models::TemplateBuild {
        build_id: *job.build_id.as_uuid(),
        status: build_status_response(job.status),
        created_at,
        updated_at,
        finished_at: job.finished_at_unix_ms.map(datetime_from_unix_ms),
        cpu_count: resources.cpu_count,
        memory_mb: resources.memory_mib,
        disk_size_mb: Some(resources.disk_size_mib),
        envd_version: None,
    }
}

fn template_with_builds_from_job(
    job: &BuildJobRecord,
    limit: Option<u32>,
    next_token: Option<&str>,
) -> models::TemplateWithBuilds {
    let mut builds = if next_token.is_some() {
        Vec::new()
    } else {
        vec![template_build_from_job(job)]
    };
    if let Some(limit) = limit {
        builds.truncate(limit as usize);
    }
    let names = match &job.target {
        BuildTarget::Template {
            alias: Some(alias), ..
        } => vec![alias.to_string()],
        _ => vec![job.build_id.to_string()],
    };
    models::TemplateWithBuilds::new(
        job.build_id.to_string(),
        true,
        names,
        datetime_from_unix_ms(job.created_at_unix_ms),
        datetime_from_unix_ms(job.updated_at_unix_ms),
        Nullable::Null,
        0,
        builds,
    )
}

fn template_build_info_from_record(record: SnapshotRecord) -> models::TemplateBuildInfo {
    models::TemplateBuildInfo::new(
        Vec::new(),
        Vec::new(),
        record.id.to_string(),
        record.id.to_string(),
        models::TemplateBuildStatus::Ready,
    )
}

fn template_build_info_from_job(job: BuildJobRecord) -> models::TemplateBuildInfo {
    let id = job.build_id.to_string();
    let mut info = models::TemplateBuildInfo::new(
        Vec::new(),
        Vec::new(),
        id.clone(),
        id,
        build_status_response(job.status),
    );
    if let Some(error_reason) = job.error_reason {
        info.reason = Some(models::BuildStatusReason {
            message: error_reason.message,
            step: error_reason.step,
            log_entries: None,
        });
    }
    if job.status == BuildStatus::Ready {
        info.image_digest = job.result_image_digest;
    }
    info
}

fn v2_start_build_error(err: models::Error) -> V2TemplatesTemplateIdBuildsBuildIdPostResponse {
    match err.code {
        400 => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status400_BadRequest(err),
        404 => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(err),
        _ => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status500_ServerError(err),
    }
}

async fn mark_v2_build_error(api: &ApiImpl, build_id: &SnapshotId, reason: BuildErrorReason) {
    if let Err(error) = api
        .meta()
        .update_build_job(build_id.clone(), BuildJobTransition::Fail { reason })
        .await
    {
        warn!(
            build_id = %build_id,
            error = %error,
            "failed to persist template build error status"
        );
    }
}

impl From<BuildLogEntry> for models::BuildLogEntry {
    fn from(entry: BuildLogEntry) -> Self {
        Self {
            timestamp: entry.timestamp,
            message: entry.message,
            step: entry.step,
            level: entry.level.into(),
        }
    }
}

/// Durable lookup result for build status/logs/builder endpoints.
pub(super) enum BuildLookup {
    Job(BuildJobRecord),
    ReadyTemplate(Box<SnapshotRecord>),
}

impl BuildLookup {
    pub(super) fn id(&self) -> &SnapshotId {
        match self {
            Self::Job(job) => &job.build_id,
            Self::ReadyTemplate(record) => &record.id,
        }
    }
}

impl From<BuildLookup> for models::TemplateBuildInfo {
    fn from(lookup: BuildLookup) -> Self {
        match lookup {
            BuildLookup::Job(job) => template_build_info_from_job(job),
            BuildLookup::ReadyTemplate(record) => template_build_info_from_record(*record),
        }
    }
}

impl ApiImpl {
    async fn cleanup_build_logs(&self, id: &SnapshotId) {
        self.build_logs.remove(id);
        let _ = self
            .snapshot_manager
            .repository()
            .delete_build_log(id)
            .await;
    }

    pub(super) async fn build_lookup(
        &self,
        template: &str,
        build: &str,
    ) -> Result<BuildLookup, models::Error> {
        if template != build {
            return Err(Self::error(404, "build not found for template"));
        }
        let id = SnapshotId::parse(build).map_err(|_| Self::error(400, "invalid buildID"))?;
        if let Some(job) = self
            .meta()
            .get_build_job(id.clone())
            .await
            .map_err(Self::meta_error)?
        {
            return Ok(BuildLookup::Job(job));
        }
        if let Some(record) = self
            .snapshot_manager
            .get(build)
            .await
            .map_err(|e| Self::snapshot_manager_error(&e))?
        {
            if matches!(record.config.source, SnapshotSource::Template) {
                return Ok(BuildLookup::ReadyTemplate(Box::new(record)));
            }
        }
        Err(Self::error(404, "template build not found"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_build_logs(
        &self,
        id: &SnapshotId,
        offset: usize,
        cursor: Option<i64>,
        limit: usize,
        backward: bool,
        level: Option<LogLevel>,
        source: Option<models::LogsSource>,
    ) -> Result<Vec<models::BuildLogEntry>, models::Error> {
        if limit > 100 {
            return Err(Self::error(400, "limit must be between 0 and 100"));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let temporary = if source != Some(models::LogsSource::LogsSourcePersistent) {
            self.build_logs.temporary(id)
        } else {
            None
        };
        let entries = match temporary {
            Some(entries) => entries,
            None if source == Some(models::LogsSource::LogsSourceTemporary) => Vec::new(),
            None => read_build_logs(self.snapshot_manager.repository().as_ref(), id)
                .await
                .map_err(|e| Self::repository_error(&e))?,
        };
        let mut entries = entries;
        if backward {
            entries.reverse();
        }
        Ok(entries
            .into_iter()
            .filter(|entry| {
                level.is_none_or(|level| entry.level >= level)
                    && cursor.is_none_or(|cursor| {
                        if backward {
                            entry.timestamp
                                <= chrono::DateTime::from_timestamp_millis(cursor)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC)
                        } else {
                            entry.timestamp
                                >= chrono::DateTime::from_timestamp_millis(cursor)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC)
                        }
                    })
            })
            .skip(offset)
            .take(limit)
            .map(Into::into)
            .collect())
    }

    async fn query_build_reason_logs(
        &self,
        id: &SnapshotId,
        step: &str,
    ) -> Result<Vec<models::BuildLogEntry>, models::Error> {
        let entries = match self.build_logs.temporary(id) {
            Some(entries) => entries,
            None => read_build_logs(self.snapshot_manager.repository().as_ref(), id)
                .await
                .map_err(|error| Self::repository_error(&error))?,
        };
        Ok(entries
            .into_iter()
            .filter(|entry| entry.step.as_deref() == Some(step) && entry.level >= LogLevel::Warn)
            .take(100)
            .map(Into::into)
            .collect())
    }
}

#[async_trait]
impl Templates<()> for ApiImpl {
    type Claims = super::Claims;

    async fn templates_template_id_builds_build_id_builder_put(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path: &models::TemplatesTemplateIdBuildsBuildIdBuilderPutPathParams,
        body: &models::TemplateBuilderRequest,
    ) -> Result<TemplatesTemplateIdBuildsBuildIdBuilderPutResponse, ()> {
        use TemplatesTemplateIdBuildsBuildIdBuilderPutResponse::*;
        Ok(
            match self
                .start_image_build(&path.template_id, &path.build_id, body)
                .await
            {
                Ok(body) => Status202_BuilderPreparationAccepted {
                    x_agentenv_build_id: Some(path.build_id.clone()),
                    body,
                },
                Err(err) => match err.code {
                    400 => Status400_BadRequest(err),
                    404 => Status404_NotFound(err),
                    409 => Status409_Conflict(err),
                    429 => Status429_ConcurrentBuildLimitReached(err),
                    _ => Status500_ServerError(err),
                },
            },
        )
    }

    async fn templates_template_id_builds_build_id_builder_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path: &models::TemplatesTemplateIdBuildsBuildIdBuilderDeletePathParams,
    ) -> Result<TemplatesTemplateIdBuildsBuildIdBuilderDeleteResponse, ()> {
        use TemplatesTemplateIdBuildsBuildIdBuilderDeleteResponse::*;
        Ok(
            match self
                .cancel_image_build(&path.template_id, &path.build_id)
                .await
            {
                Ok(()) => Status204_BuilderReleased,
                Err(err) => match err.code {
                    404 => Status404_NotFound(err),
                    409 => Status409_Conflict(err),
                    _ => Status500_ServerError(err),
                },
            },
        )
    }

    async fn templates_aliases_alias_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesAliasesAliasGetPathParams,
    ) -> Result<TemplatesAliasesAliasGetResponse, ()> {
        if let Err(err) = SnapshotAlias::parse(&path_params.alias) {
            return Ok(TemplatesAliasesAliasGetResponse::Status400_BadRequest(
                Self::error(400, err.to_string()),
            ));
        }

        match self
            .snapshot_manager
            .resolve_committed_alias(&path_params.alias)
            .await
        {
            Ok(Some(snapshot_id)) => Ok(
                TemplatesAliasesAliasGetResponse::Status200_SuccessfullyQueriedTemplateByAlias(
                    models::TemplateAliasResponse::new(snapshot_id.to_string(), false),
                ),
            ),
            Ok(None) => Ok(TemplatesAliasesAliasGetResponse::Status404_NotFound(
                Self::error(
                    404,
                    format!("template alias not found: {}", path_params.alias),
                ),
            )),
            Err(err) => Ok(TemplatesAliasesAliasGetResponse::Status500_ServerError(
                Self::snapshot_manager_error(&err),
            )),
        }
    }

    async fn templates_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        _query_params: &models::TemplatesGetQueryParams,
    ) -> Result<TemplatesGetResponse, ()> {
        let records = match self
            .snapshot_manager
            .list(SnapshotListFilter::templates())
            .await
        {
            Ok(records) => records,
            Err(err) => {
                return Ok(TemplatesGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        let out = records.into_iter().map(template_from_record).collect();

        Ok(TemplatesGetResponse::Status200_SuccessfullyReturnedAllTemplates(out))
    }

    async fn v2_templates_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::V2TemplatesGetQueryParams,
    ) -> Result<V2TemplatesGetResponse, ()> {
        let cursor = match query_params.next_token.as_deref() {
            Some(token) => match PaginationCursor::<SnapshotId>::parse(token) {
                Ok(cursor) if cursor.is_descending() => cursor,
                Ok(_) => {
                    return Ok(V2TemplatesGetResponse::Status400_BadRequest(Self::error(
                        400,
                        "next token was issued for a different sort order".to_string(),
                    )));
                }
                Err(err) => {
                    return Ok(V2TemplatesGetResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("invalid next token: {err}"),
                    )));
                }
            },
            None => PaginationCursor::new_descending(SystemTime::now(), SnapshotId::max()),
        };

        let summaries = match self
            .snapshot_manager
            .list_metadata(SnapshotListFilter::templates())
            .await
        {
            Ok(summaries) => summaries,
            Err(err) => {
                return Ok(V2TemplatesGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        let page = cursor.paginate_sorted(
            summaries,
            query_params.limit,
            |record, cursor| {
                PaginationCursor::compare_desc(
                    SystemTime::from(datetime_from_unix_ms(record.created_at_unix_ms)),
                    &record.id,
                    cursor.time(),
                    cursor.value(),
                )
            },
            |record| {
                PaginationCursor::new_descending(
                    SystemTime::from(datetime_from_unix_ms(record.created_at_unix_ms)),
                    record.id.clone(),
                )
            },
        );

        let records = match self
            .snapshot_manager
            .list(SnapshotListFilter::by_ids(
                page.items.into_iter().map(|entry| entry.id),
            ))
            .await
        {
            Ok(records) => records,
            Err(err) => {
                return Ok(V2TemplatesGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        Ok(
            V2TemplatesGetResponse::Status200_SuccessfullyReturnedAllTemplates {
                body: records.into_iter().map(template_from_record).collect(),
                x_next_token: page.next_token,
            },
        )
    }

    async fn templates_template_id_builds_build_id_status_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdBuildsBuildIdStatusGetPathParams,
        query_params: &models::TemplatesTemplateIdBuildsBuildIdStatusGetQueryParams,
    ) -> Result<TemplatesTemplateIdBuildsBuildIdStatusGetResponse, ()> {
        match self
            .build_lookup(&path_params.template_id, &path_params.build_id)
            .await
        {
            Ok(record) => {
                let id = record.id().clone();
                let mut info = models::TemplateBuildInfo::from(record);
                info.log_entries = match self
                    .query_build_logs(
                        &id,
                        query_params.logs_offset.unwrap_or(0) as usize,
                        None,
                        query_params.limit.unwrap_or(100) as usize,
                        false,
                        query_params.level.map(Into::into),
                        None,
                    )
                    .await
                {
                    Ok(entries) => entries,
                    Err(error) => {
                        return Ok(if error.code == 400 {
                            TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status400_BadRequest(
                                error,
                            )
                        } else {
                            TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status500_ServerError(
                                error,
                            )
                        })
                    }
                };
                if let Some(step) = info.reason.as_ref().and_then(|reason| reason.step.clone()) {
                    let reason_logs = match self.query_build_reason_logs(&id, &step).await {
                        Ok(entries) => entries,
                        Err(error) => {
                            return Ok(TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status500_ServerError(error));
                        }
                    };
                    if let Some(reason) = &mut info.reason {
                        reason.log_entries = Some(reason_logs);
                    }
                }
                if self.build_sessions.is_starting(&path_params.build_id) {
                    info.status = models::TemplateBuildStatus::Waiting;
                } else if info.status == models::TemplateBuildStatus::Ready
                    && self.build_sessions.is_finishing(&path_params.build_id)
                {
                    // A sequential CLI build must observe the newly published cache seed.
                    info.status = models::TemplateBuildStatus::Building;
                    info.image_digest = None;
                }
                Ok(TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status200_SuccessfullyReturnedTheTemplate(
                    info,
                ))
            }
            Err(error) => Ok(match error.code {
                400 => {
                    TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status400_BadRequest(error)
                }
                404 => TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status404_NotFound(error),
                _ => {
                    TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status500_ServerError(error)
                }
            }),
        }
    }

    async fn templates_template_id_builds_build_id_logs_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdBuildsBuildIdLogsGetPathParams,
        query_params: &models::TemplatesTemplateIdBuildsBuildIdLogsGetQueryParams,
    ) -> Result<TemplatesTemplateIdBuildsBuildIdLogsGetResponse, ()> {
        use TemplatesTemplateIdBuildsBuildIdLogsGetResponse as Response;
        let result = async {
            let record = self
                .build_lookup(&path_params.template_id, &path_params.build_id)
                .await?;
            let cursor = query_params
                .cursor
                .map(i64::try_from)
                .transpose()
                .map_err(|_| Self::error(400, "cursor exceeds int64 range"))?;
            self.query_build_logs(
                record.id(),
                0,
                cursor,
                query_params.limit.unwrap_or(100) as usize,
                query_params.direction == Some(models::LogsDirection::LogsDirectionBackward),
                query_params.level.map(Into::into),
                query_params.source,
            )
            .await
        }
        .await;
        Ok(match result {
            Ok(logs) => Response::Status200_SuccessfullyReturnedTheTemplateBuildLogs(
                models::TemplateBuildLogsResponse::new(logs),
            ),
            Err(error) if error.code == 400 => Response::Status400_BadRequest(error),
            Err(error) if error.code == 404 => Response::Status404_NotFound(error),
            Err(error) => Response::Status500_ServerError(error),
        })
    }

    async fn templates_template_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdDeletePathParams,
        // The type hint only guides upstream request routing; the API
        // semantics do not depend on it.
        _query_params: &models::TemplatesTemplateIdDeleteQueryParams,
    ) -> Result<TemplatesTemplateIdDeleteResponse, ()> {
        use TemplatesTemplateIdDeleteResponse as Response;
        let server_error = |error: models::Error| {
            if error.code == 503 {
                Response::Status503_ServiceUnavailable(error)
            } else {
                Response::Status500_ServerError(error)
            }
        };
        let parsed_id = SnapshotId::parse(&path_params.template_id).ok();
        if let Some(id) = parsed_id.as_ref() {
            if self.build_sessions.contains(&id.to_string()) {
                return Ok(Response::Status409_Conflict(Self::error(
                    409,
                    "template build is active; cancel the builder or wait for it to finish",
                )));
            }
        }

        let record = match self
            .snapshot_manager
            .get_metadata(&path_params.template_id)
            .await
        {
            Ok(Some(record)) if self.build_sessions.contains(&record.id.to_string()) => {
                return Ok(Response::Status409_Conflict(Self::error(
                    409,
                    "template build is active; cancel the builder or wait for it to finish",
                )));
            }
            Err(err) => return Ok(server_error(Self::reusable_snapshot_manager_error(&err))),
            Ok(Some(record)) => Some(record),
            Ok(None) => None,
        };

        if let Some(record) = record {
            let deleted_id = record.id.clone();
            if let Err(err) = self.snapshot_manager.delete(deleted_id.to_string()).await {
                return Ok(server_error(Self::reusable_snapshot_manager_error(&err)));
            }
            let _ = self
                .meta()
                .delete_build_job(deleted_id.clone(), false)
                .await;
            self.cleanup_build_logs(&deleted_id).await;
            return Ok(Response::Status204_TheTemplateWasDeletedSuccessfully);
        }

        if let Some(id) = parsed_id {
            let meta = self.meta();
            let job = match meta.get_build_job(id.clone()).await {
                Ok(job) => job,
                Err(err) => return Ok(server_error(Self::meta_error(err))),
            };
            if let Some(job) = job {
                let reserve_tombstone = matches!(job.target, BuildTarget::Template { .. });
                if let Err(err) = meta.delete_build_job(id.clone(), reserve_tombstone).await {
                    return Ok(server_error(Self::meta_error(err)));
                }
            } else if let Err(err) = self.snapshot_manager.delete(id.to_string()).await {
                return Ok(server_error(Self::reusable_snapshot_manager_error(&err)));
            }
            self.cleanup_build_logs(&id).await;
        }

        Ok(Response::Status204_TheTemplateWasDeletedSuccessfully)
    }

    async fn templates_template_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdGetPathParams,
        query_params: &models::TemplatesTemplateIdGetQueryParams,
    ) -> Result<TemplatesTemplateIdGetResponse, ()> {
        let record = match self.snapshot_manager.get(&path_params.template_id).await {
            Ok(Some(record)) => record,
            Ok(None) => {
                if let Ok(id) = SnapshotId::parse(&path_params.template_id) {
                    match self.meta().get_build_job(id).await {
                        Ok(Some(job)) if matches!(job.target, BuildTarget::Template { .. }) => {
                            return Ok(
                                TemplatesTemplateIdGetResponse::Status200_SuccessfullyReturnedTheTemplateWithItsBuilds {
                                    body: template_with_builds_from_job(
                                        &job,
                                        query_params.limit,
                                        query_params.next_token.as_deref(),
                                    ),
                                    x_next_token: None,
                                },
                            );
                        }
                        Ok(_) => {}
                        Err(err) => {
                            return Ok(TemplatesTemplateIdGetResponse::Status500_ServerError(
                                Self::meta_error(err),
                            ));
                        }
                    }
                }
                return Ok(TemplatesTemplateIdGetResponse::Status404_NotFound(
                    Self::error(
                        404,
                        format!("template {} not found", path_params.template_id),
                    ),
                ));
            }
            Err(err) => {
                return Ok(TemplatesTemplateIdGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        let mut builds = if query_params.next_token.is_some() {
            Vec::new()
        } else {
            vec![template_build_from_record(&record)]
        };
        if let Some(limit) = query_params.limit {
            builds.truncate(limit as usize);
        }
        let created_at = datetime_from_unix_ms(record.created_at_unix_ms);
        let updated_at = datetime_from_unix_ms(record.updated_at_unix_ms);

        Ok(
            TemplatesTemplateIdGetResponse::Status200_SuccessfullyReturnedTheTemplateWithItsBuilds {
                body: models::TemplateWithBuilds::new(
                    record.id.to_string(),
                    true,
                    build_record_names(&record),
                    created_at,
                    updated_at,
                    Nullable::Null,
                    0,
                    builds,
                ),
                x_next_token: None,
            },
        )
    }

    async fn v3_templates_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::TemplateBuildRequestV3,
    ) -> Result<V3TemplatesPostResponse, ()> {
        if body.image_only == Some(true) {
            return Ok(match self.create_image_build(body).await {
                Ok(response) => {
                    V3TemplatesPostResponse::Status202_TheBuildWasRequestedSuccessfully(response)
                }
                Err(err) => Self::client_or_server_response(
                    err,
                    V3TemplatesPostResponse::Status400_BadRequest,
                    V3TemplatesPostResponse::Status500_ServerError,
                ),
            });
        }
        let Some(name) = body.name.clone() else {
            return Ok(V3TemplatesPostResponse::Status400_BadRequest(Self::error(
                400,
                "template name must be provided",
            )));
        };

        let target = match template_build_target_from_v3_request(body, &name) {
            Ok(target) => target,
            Err(err) => {
                return Ok(Self::client_or_server_response(
                    err,
                    V3TemplatesPostResponse::Status400_BadRequest,
                    V3TemplatesPostResponse::Status500_ServerError,
                ));
            }
        };

        let build_id = SnapshotId::generate();
        let job = match self.meta().create_build_job(build_id.clone(), target).await {
            Ok(job) => job,
            Err(err) => {
                return Ok(Self::client_or_server_response(
                    Self::meta_build_error(err),
                    V3TemplatesPostResponse::Status400_BadRequest,
                    V3TemplatesPostResponse::Status500_ServerError,
                ));
            }
        };

        let response = models::TemplateRequestResponseV3::new(
            job.build_id.to_string(),
            job.build_id.to_string(),
            true,
            vec![name.clone()],
            Vec::new(),
            vec![name],
        );

        Ok(V3TemplatesPostResponse::Status202_TheBuildWasRequestedSuccessfully(response))
    }

    async fn v2_templates_template_id_builds_build_id_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::V2TemplatesTemplateIdBuildsBuildIdPostPathParams,
        body: &models::TemplateBuildStartV2,
    ) -> Result<V2TemplatesTemplateIdBuildsBuildIdPostResponse, ()> {
        if path_params.template_id != path_params.build_id {
            return Ok(v2_start_build_error(Self::error(
                400,
                "templateID and buildID must match in AgentENV compatibility mode",
            )));
        }

        let build_id = match SnapshotId::parse(path_params.build_id.as_str()) {
            Ok(id) => id,
            Err(_) => {
                return Ok(v2_start_build_error(Self::error(
                    400,
                    format!("invalid buildID: {}", path_params.build_id),
                )));
            }
        };
        let meta = self.meta();
        let (alias, resources) = match meta.get_build_job(build_id.clone()).await {
            Ok(Some(BuildJobRecord {
                target: BuildTarget::Template { alias, resources },
                ..
            })) => (alias, resources),
            Ok(_) => {
                return Ok(
                    V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(
                        Self::error(
                            404,
                            format!("template {} not found", path_params.template_id),
                        ),
                    ),
                );
            }
            Err(err) => return Ok(v2_start_build_error(Self::meta_error(err))),
        };
        let base_source = match template_build_start_base_source(body) {
            Ok(source) => source,
            Err(err) => return Ok(v2_start_build_error(err)),
        };
        let registry_auth = match template_registry_auth(body) {
            Ok(auth) => auth,
            Err(err) => return Ok(v2_start_build_error(err)),
        };
        let spec = match template_build_spec_from_start_request(body, alias.as_ref(), resources) {
            Ok(spec) => spec,
            Err(err) => return Ok(v2_start_build_error(err)),
        };

        match meta
            .update_build_job(build_id.clone(), BuildJobTransition::Start)
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Ok(v2_start_build_error(Self::error(
                    400,
                    "template build has already started",
                )));
            }
            Err(err) => return Ok(v2_start_build_error(Self::meta_error(err))),
        }

        let logs = self
            .build_logs
            .start(build_id.clone(), self.snapshot_manager.repository());
        let spec = spec.with_logger(logs.logger.clone());
        let sandbox_id =
            crate::types::SandboxId::parse_str(&build_id.to_string()).expect("build ID is a UUID");
        self.orchestrator.register_template_build(sandbox_id).await;
        let api = self.clone();
        tokio::spawn(async move {
            let span = tracing::info_span!("template_build", build_id = %build_id);
            async move {
                let work = async {
                    info!(build_id = %build_id, "template build started");
                    match base_source {
                        source @ (TemplateBuildStartBaseSource::DefaultImage
                        | TemplateBuildStartBaseSource::Image(_)) => {
                            let requested_image = match source {
                                TemplateBuildStartBaseSource::Image(image) => Some(image),
                                _ => None,
                            };
                            let resolved_rootfs = match resolve_template_rootfs_image(
                                &api,
                                requested_image.as_deref(),
                                registry_auth.as_ref(),
                            )
                            .await
                            {
                                Ok(resolved) => resolved,
                                Err(err) => {
                                    warn!(
                                        build_id = %build_id,
                                        reason = %err.message,
                                        "template build failed while resolving the base image"
                                    );
                                    mark_v2_build_error(
                                        &api,
                                        &build_id,
                                        BuildErrorReason::new(err.message),
                                    )
                                    .await;
                                    return;
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
                            match api
                                .template_builder
                                .build_and_publish_with_id(
                                    api.snapshot_manager.as_ref(),
                                    build_id.clone(),
                                    spec,
                                )
                                .await
                            {
                                Ok(_) => {
                                    info!(build_id = %build_id, "template build completed");
                                }
                                Err(error) => {
                                    let reason = handle_v2_pipeline_error(&build_id, None, &error);
                                    mark_v2_build_error(&api, &build_id, reason).await;
                                }
                            }
                        }
                        TemplateBuildStartBaseSource::Template(alias) => {
                            let base_runnable = match api
                                .snapshot_manager
                                .load_runnable(alias.as_ref())
                                .await
                            {
                                Ok(Some(runnable)) => runnable,
                                Ok(None) => {
                                    warn!(
                                        build_id = %build_id,
                                        base_template = %alias,
                                        reason = "base template alias not found",
                                        "template build failed while resolving the base template"
                                    );
                                    mark_v2_build_error(
                                        &api,
                                        &build_id,
                                        BuildErrorReason::new(format!(
                                            "template alias not found: {alias}"
                                        )),
                                    )
                                    .await;
                                    return;
                                }
                                Err(err) => {
                                    warn!(
                                        build_id = %build_id,
                                        base_template = %alias,
                                        error = %format_args!("{err:#}"),
                                        "template build failed while loading the base template"
                                    );
                                    mark_v2_build_error(
                                        &api,
                                        &build_id,
                                        BuildErrorReason::new(
                                            Self::snapshot_manager_error(&err).message,
                                        ),
                                    )
                                    .await;
                                    return;
                                }
                            };
                            info!(
                                build_id = %build_id,
                                base_template = %alias,
                                base_snapshot_id = %base_runnable.id,
                                "template build base template resolved"
                            );
                            match api
                                .template_builder
                                .build_from_snapshot_and_publish(
                                    api.snapshot_manager.as_ref(),
                                    spec,
                                    build_id.clone(),
                                    &base_runnable,
                                )
                                .await
                            {
                                Ok(_) => {
                                    info!(build_id = %build_id, "template build completed");
                                }
                                Err(error) => {
                                    let reason =
                                        handle_v2_pipeline_error(&build_id, Some(&alias), &error);
                                    mark_v2_build_error(&api, &build_id, reason).await;
                                }
                            }
                        }
                    }
                };
                if std::panic::AssertUnwindSafe(work)
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    mark_v2_build_error(
                        &api,
                        &build_id,
                        BuildErrorReason::new("template build worker panicked"),
                    )
                    .await;
                }
                if let Err(error) = logs.finish().await {
                    warn!(%build_id, %error, "failed to persist final build logs");
                }
                api.orchestrator.unregister_template_build(sandbox_id).await;
            }
            .instrument(span)
            .await;
        });

        Ok(V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status202_TheBuildHasStarted)
    }
}

#[cfg(test)]
mod tests {
    use super::pipeline_build_error;

    #[test]
    fn pipeline_build_error_maps_invalid_input_to_bad_request() {
        let error = pipeline_build_error(&crate::template::TemplatePipelineError::Build(
            crate::template::TemplateBuildError::invalid_input("bad input"),
        ));

        assert_eq!(error.code, 400);
        assert_eq!(error.message, "bad input");
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

        assert_eq!(error.code, 400);
        assert!(error.message.contains("dup"));
    }
}
