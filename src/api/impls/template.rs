use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use chrono::TimeZone;
use headers::Host;
use http::Method;
use std::time::SystemTime;
use tracing::{debug, info, warn};

use agentenv_http_server::apis::templates::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::pagination::PaginationCursor;
use super::template_helpers::{
    template_build_record_from_v3_request, template_build_spec_from_start_request,
    template_build_start_base_source, TemplateBuildStartBaseSource,
};
use super::ApiImpl;
use crate::image::ResolvedBlockImage;
use crate::snapshot::{
    SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotRecord, SnapshotSource,
    TemplateBuildErrorReason, TemplateBuildStatus,
};
use crate::template::{TemplateBuildError, TemplateBuildFailure, TemplatePipelineError};
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

fn pipeline_error_reason(err: &TemplatePipelineError) -> TemplateBuildErrorReason {
    match err {
        TemplatePipelineError::Build(TemplateBuildError::InvalidInput { reason }) => {
            TemplateBuildErrorReason::new(reason.clone())
        }
        TemplatePipelineError::Build(TemplateBuildError::System { reason, .. }) => reason.clone(),
        TemplatePipelineError::Repository(repository) => {
            TemplateBuildErrorReason::new(repository.to_string())
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
) -> TemplateBuildErrorReason {
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

#[tracing::instrument(skip(api))]
async fn resolve_template_rootfs_image(
    api: &ApiImpl,
    requested_image: Option<&str>,
) -> Result<ResolvedBlockImage, models::Error> {
    let image_resolver = api.image_resolver();
    let image_ref = requested_image
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| image_resolver.default_image());
    image_resolver.resolve(image_ref).await.map_err(|err| {
        models::Error::new(
            if err.is_user_error() { 400 } else { 500 },
            format!("{err:#}"),
        )
    })
}

impl From<TemplateBuildStatus> for models::TemplateBuildStatus {
    fn from(status: TemplateBuildStatus) -> Self {
        match status {
            TemplateBuildStatus::Waiting => Self::Waiting,
            TemplateBuildStatus::Building => Self::Building,
            TemplateBuildStatus::Ready => Self::Ready,
            TemplateBuildStatus::Error => Self::Error,
        }
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
        .committed
        .as_ref()
        .map(|snapshot| snapshot.runtime_versions.envd_version.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

pub(super) fn template_build_status(record: &SnapshotRecord) -> TemplateBuildStatus {
    match &record.source {
        SnapshotSource::Template { build } => build.status,
        SnapshotSource::Sandbox { .. } => TemplateBuildStatus::Ready,
    }
}

impl From<SnapshotRecord> for models::Template {
    fn from(record: SnapshotRecord) -> Self {
        let created_at = datetime_from_unix_ms(record.created_at_unix_ms);
        let updated_at = datetime_from_unix_ms(record.updated_at_unix_ms);
        Self::new(
            record.id.to_string(),
            record.id.to_string(),
            record.resources.cpu_count,
            record.resources.memory_mib,
            record.resources.disk_size_mib,
            true,
            build_record_names(&record),
            created_at,
            updated_at,
            Nullable::Null,
            0,
            1,
            build_record_envd_version(&record),
            template_build_status(&record).into(),
        )
    }
}

impl From<&SnapshotRecord> for models::TemplateBuild {
    fn from(record: &SnapshotRecord) -> Self {
        let created_at = datetime_from_unix_ms(record.created_at_unix_ms);
        let updated_at = datetime_from_unix_ms(record.updated_at_unix_ms);
        let build_id = record.id.0;
        let finished_at = match &record.source {
            SnapshotSource::Template { build } => {
                build.finished_at_unix_ms.map(datetime_from_unix_ms)
            }
            SnapshotSource::Sandbox { .. } => None,
        };
        Self {
            build_id,
            status: template_build_status(record).into(),
            created_at,
            updated_at,
            finished_at,
            cpu_count: record.resources.cpu_count,
            memory_mb: record.resources.memory_mib,
            disk_size_mb: Some(record.resources.disk_size_mib),
            envd_version: record
                .committed
                .as_ref()
                .map(|snapshot| snapshot.runtime_versions.envd_version.clone()),
        }
    }
}

impl From<SnapshotRecord> for models::TemplateBuildInfo {
    fn from(record: SnapshotRecord) -> Self {
        let mut info = Self::new(
            Vec::new(),
            Vec::new(),
            record.id.to_string(),
            record.id.to_string(),
            template_build_status(&record).into(),
        );
        if let SnapshotSource::Template { build } = &record.source {
            if let Some(error_reason) = &build.error_reason {
                info.reason = Some(models::BuildStatusReason {
                    message: error_reason.message.clone(),
                    step: error_reason.step.clone(),
                    log_entries: None,
                });
            }
        }
        info
    }
}

fn v2_start_build_error(err: models::Error) -> V2TemplatesTemplateIdBuildsBuildIdPostResponse {
    match err.code {
        400 => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status400_BadRequest(err),
        404 => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(err),
        _ => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status500_ServerError(err),
    }
}

async fn mark_v2_build_error(
    api: &ApiImpl,
    build_id: &SnapshotId,
    reason: TemplateBuildErrorReason,
) {
    if let Err(error) = api
        .snapshot_manager
        .mark_build_error(build_id, reason)
        .await
    {
        warn!(
            build_id = %build_id,
            error = %format_args!("{error:#}"),
            "failed to persist template build error status"
        );
    }
}

#[async_trait]
impl Templates<()> for ApiImpl {
    type Claims = super::Claims;

    async fn templates_builds_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::TemplateBuildSessionRequest,
    ) -> Result<TemplatesBuildsPostResponse, ()> {
        use TemplatesBuildsPostResponse::*;
        Ok(match self.start_image_build(body).await {
            Ok(body) => Status202_TheTemplateBuildHasStarted {
                x_agentenv_build_id: Some(body.build_id.clone()),
                body,
            },
            Err(err) => match err.code {
                400 => Status400_BadRequest(err),
                409 => Status409_Conflict(err),
                _ => Status500_ServerError(err),
            },
        })
    }

    async fn templates_template_id_builds_build_id_builder_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path: &models::TemplatesTemplateIdBuildsBuildIdBuilderPostPathParams,
        body: &models::TemplateBuildImage,
    ) -> Result<TemplatesTemplateIdBuildsBuildIdBuilderPostResponse, ()> {
        use TemplatesTemplateIdBuildsBuildIdBuilderPostResponse::*;
        Ok(
            match self.submit_image_build(&path.template_id, &path.build_id, &body.digest) {
                Ok(()) => Status202_ImagePublicationAccepted,
                Err(err) => match err.code {
                    400 => Status400_BadRequest(err),
                    404 => Status404_NotFound(err),
                    _ => Status409_Conflict(err),
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

        let out = records.into_iter().map(models::Template::from).collect();

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

        let records = match self
            .snapshot_manager
            .list(SnapshotListFilter::templates())
            .await
        {
            Ok(records) => records,
            Err(err) => {
                return Ok(V2TemplatesGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };

        let page = cursor.paginate_sorted(
            records,
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

        Ok(
            V2TemplatesGetResponse::Status200_SuccessfullyReturnedAllTemplates {
                body: page.items.into_iter().map(models::Template::from).collect(),
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
    ) -> Result<TemplatesTemplateIdBuildsBuildIdStatusGetResponse, ()> {
        if path_params.template_id != path_params.build_id {
            return Ok(
                TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status404_NotFound(Self::error(
                    404,
                    format!(
                        "build {} not found for template {}",
                        path_params.build_id, path_params.template_id
                    ),
                )),
            );
        }

        match self.snapshot_manager.get(&path_params.template_id).await {
            Ok(Some(record)) => {
                let mut info = models::TemplateBuildInfo::from(record);
                if info.status == models::TemplateBuildStatus::Ready
                    && self.build_sessions.is_finishing(&path_params.build_id)
                {
                    // A sequential CLI build must observe the newly published cache seed.
                    info.status = models::TemplateBuildStatus::Building;
                }
                Ok(TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status200_SuccessfullyReturnedTheTemplate(
                    info,
                ))
            }
            Ok(None) => Ok(
                TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status404_NotFound(Self::error(
                    404,
                    format!("template {} not found", path_params.template_id),
                )),
            ),
            Err(err) => Ok(
                TemplatesTemplateIdBuildsBuildIdStatusGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ),
            ),
        }
    }

    async fn templates_template_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::TemplatesTemplateIdDeletePathParams,
    ) -> Result<TemplatesTemplateIdDeleteResponse, ()> {
        match self.snapshot_manager.delete(&path_params.template_id).await {
            Ok(_) => {
                Ok(TemplatesTemplateIdDeleteResponse::Status204_TheTemplateWasDeletedSuccessfully)
            }
            Err(err) => Ok(TemplatesTemplateIdDeleteResponse::Status500_ServerError(
                Self::snapshot_manager_error(&err),
            )),
        }
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
            vec![models::TemplateBuild::from(&record)]
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
        let Some(name) = body.name.clone() else {
            return Ok(V3TemplatesPostResponse::Status400_BadRequest(Self::error(
                400,
                "template name must be provided",
            )));
        };

        let snapshot_id = SnapshotId::generate();
        let record = match template_build_record_from_v3_request(body, snapshot_id.clone(), &name) {
            Ok(record) => record,
            Err(err) => {
                return Ok(Self::client_or_server_response(
                    err,
                    V3TemplatesPostResponse::Status400_BadRequest,
                    V3TemplatesPostResponse::Status500_ServerError,
                ));
            }
        };

        let record = match self.snapshot_manager.create(record).await {
            Ok(record) => record,
            Err(err) => {
                let error = Self::bad_request_for_repository_build_error(&err)
                    .unwrap_or_else(|| Self::repository_error(&err));
                return Ok(Self::client_or_server_response(
                    error,
                    V3TemplatesPostResponse::Status400_BadRequest,
                    V3TemplatesPostResponse::Status500_ServerError,
                ));
            }
        };

        let response = models::TemplateRequestResponseV3::new(
            record.id.to_string(),
            record.id.to_string(),
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
        let pending_record = match self.snapshot_manager.get(&path_params.template_id).await {
            Ok(Some(record)) => record,
            Ok(None) => {
                return Ok(
                    V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(
                        Self::error(
                            404,
                            format!("template {} not found", path_params.template_id),
                        ),
                    ),
                );
            }
            Err(err) => {
                return Ok(v2_start_build_error(Self::snapshot_manager_error(&err)));
            }
        };
        if pending_record.id != build_id {
            return Ok(v2_start_build_error(Self::error(
                400,
                "templateID must reference the same build record as buildID",
            )));
        }
        let base_source = match template_build_start_base_source(body) {
            Ok(source) => source,
            Err(err) => return Ok(v2_start_build_error(err)),
        };
        let spec = match template_build_spec_from_start_request(
            body,
            pending_record.alias.as_ref(),
            pending_record.resources,
        ) {
            Ok(spec) => spec,
            Err(err) => return Ok(v2_start_build_error(err)),
        };

        match self.snapshot_manager.try_start_build(&build_id).await {
            Ok(_) => {}
            Err(crate::snapshot::RepositoryError::SnapshotNotFound { .. }) => {
                return Ok(
                    V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(
                        Self::error(
                            404,
                            format!("template {} not found", path_params.template_id),
                        ),
                    ),
                );
            }
            Err(err) => {
                return Ok(v2_start_build_error(Self::repository_error(&err)));
            }
        }

        let api = self.clone();
        tokio::spawn(async move {
            info!(build_id = %build_id, "template build started");
            match base_source {
                source @ (TemplateBuildStartBaseSource::DefaultImage
                | TemplateBuildStartBaseSource::Image(_)) => {
                    let requested_image = match source {
                        TemplateBuildStartBaseSource::Image(image) => Some(image),
                        _ => None,
                    };
                    let resolved_rootfs =
                        match resolve_template_rootfs_image(&api, requested_image.as_deref()).await
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
                                    TemplateBuildErrorReason::new(err.message),
                                )
                                .await;
                                return;
                            }
                        };
                    debug!(
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
                    let base_runnable =
                        match api.snapshot_manager.load_runnable(alias.as_ref()).await {
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
                                    TemplateBuildErrorReason::new(format!(
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
                                    TemplateBuildErrorReason::new(
                                        Self::snapshot_manager_error(&err).message,
                                    ),
                                )
                                .await;
                                return;
                            }
                        };
                    debug!(
                        build_id = %build_id,
                        base_template = %alias,
                        base_snapshot_id = %base_runnable.record().id,
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
                            let reason = handle_v2_pipeline_error(&build_id, Some(&alias), &error);
                            mark_v2_build_error(&api, &build_id, reason).await;
                        }
                    }
                }
            }
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
            crate::snapshot::RepositoryError::AliasConflict {
                alias: "dup".to_string(),
                existing: crate::snapshot::SnapshotId::generate(),
                new_id: crate::snapshot::SnapshotId::generate(),
            },
        ));

        assert_eq!(error.code, 400);
        assert!(error.message.contains("dup"));
    }
}
