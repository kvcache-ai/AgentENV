use std::time::SystemTime;

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use chrono::TimeZone;
use headers::Host;
use http::Method;

use agentenv_domain::{BuildJobRecord, BuildStatus, BuildTarget};
use agentenv_http_server::apis::templates::*;
use agentenv_http_server::models;
use agentenv_http_server::types::Nullable;

use super::error::ApiError;
use super::pagination::PaginationCursor;
use super::template_helpers::{
    template_build_spec_from_start_request, template_build_start_base_source,
    template_build_target_from_v3_request, template_registry_auth,
};
use super::ApiImpl;
use crate::snapshot::{
    SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotRecord, SnapshotSource,
};
use crate::template::{
    image_build::{CreateImageBuildRequest, StartImageBuildRequest},
    logs::BuildLogEntry,
    BuildLogQuery, BuildLogSource, BuildLookup,
};

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
        SnapshotSource::Sandbox { .. } => None,
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

fn v2_start_build_error(err: ApiError) -> V2TemplatesTemplateIdBuildsBuildIdPostResponse {
    match err {
        ApiError::BadRequest(_) => {
            V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status400_BadRequest(err.into_model())
        }
        ApiError::NotFound(_) => {
            V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status404_NotFound(err.into_model())
        }
        other => V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status500_ServerError(
            other.into_server_error_model(),
        ),
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

impl From<BuildLookup> for models::TemplateBuildInfo {
    fn from(lookup: BuildLookup) -> Self {
        match lookup {
            BuildLookup::Job(job) => template_build_info_from_job(job),
            BuildLookup::ReadyTemplate(record) => template_build_info_from_record(*record),
        }
    }
}

fn build_log_source_from_model(source: Option<models::LogsSource>) -> BuildLogSource {
    match source {
        Some(models::LogsSource::LogsSourceTemporary) => BuildLogSource::Temporary,
        Some(models::LogsSource::LogsSourcePersistent) => BuildLogSource::Persistent,
        None => BuildLogSource::Any,
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
        let request = StartImageBuildRequest {
            start_cmd: body.start_cmd.clone(),
            ready_cmd: body.ready_cmd.clone(),
            timeout_secs: body.timeout,
        };
        Ok(
            match self
                .build_service
                .start_image_build(&path.template_id, &path.build_id, request)
                .await
                .map_err(ApiError::from)
            {
                Ok(image) => Status202_BuilderPreparationAccepted {
                    x_agentenv_build_id: Some(path.build_id.clone()),
                    body: models::TemplateBuilder::new(image),
                },
                Err(err) => match err {
                    ApiError::BadRequest(_) => Status400_BadRequest(err.into_model()),
                    ApiError::NotFound(_) => Status404_NotFound(err.into_model()),
                    ApiError::Conflict(_) => Status409_Conflict(err.into_model()),
                    ApiError::TooManyRequests(_) => {
                        Status429_ConcurrentBuildLimitReached(err.into_model())
                    }
                    other => Status500_ServerError(other.into_server_error_model()),
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
                .build_service
                .cancel_image_build(&path.template_id, &path.build_id)
                .await
                .map_err(ApiError::from)
            {
                Ok(()) => Status204_BuilderReleased,
                Err(err) => match err {
                    ApiError::NotFound(_) => Status404_NotFound(err.into_model()),
                    ApiError::Conflict(_) => Status409_Conflict(err.into_model()),
                    other => Status500_ServerError(other.into_server_error_model()),
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
                ApiError::bad_request(err.to_string()).into_model(),
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
                ApiError::not_found(format!("template alias not found: {}", path_params.alias))
                    .into_model(),
            )),
            Err(err) => Ok(TemplatesAliasesAliasGetResponse::Status500_ServerError(
                ApiError::from_snapshot_manager(&err).into_server_error_model(),
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
                    ApiError::from_snapshot_manager(&err).into_server_error_model(),
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
                    return Ok(V2TemplatesGetResponse::Status400_BadRequest(
                        ApiError::bad_request("next token was issued for a different sort order")
                            .into_model(),
                    ));
                }
                Err(err) => {
                    return Ok(V2TemplatesGetResponse::Status400_BadRequest(
                        ApiError::bad_request(format!("invalid next token: {err}")).into_model(),
                    ));
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
                    ApiError::from_snapshot_manager(&err).into_server_error_model(),
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
                    ApiError::from_snapshot_manager(&err).into_server_error_model(),
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
        use TemplatesTemplateIdBuildsBuildIdStatusGetResponse as Response;
        match self
            .build_service
            .build_lookup(&path_params.template_id, &path_params.build_id)
            .await
            .map_err(ApiError::from)
        {
            Ok(record) => {
                let id = record.id().clone();
                let mut info = models::TemplateBuildInfo::from(record);
                info.log_entries = match self
                    .build_service
                    .query_build_logs(
                        &id,
                        BuildLogQuery {
                            offset: query_params.logs_offset.unwrap_or(0) as usize,
                            cursor: None,
                            limit: query_params.limit.unwrap_or(100) as usize,
                            backward: false,
                            level: query_params.level.map(Into::into),
                            source: BuildLogSource::Any,
                        },
                    )
                    .await
                    .map_err(ApiError::from)
                {
                    Ok(entries) => entries.into_iter().map(Into::into).collect(),
                    Err(error) => {
                        return Ok(error.client_or_server_response(
                            Response::Status400_BadRequest,
                            Response::Status500_ServerError,
                        ));
                    }
                };
                if let Some(step) = info.reason.as_ref().and_then(|reason| reason.step.clone()) {
                    let reason_logs = match self
                        .build_service
                        .query_build_reason_logs(&id, &step)
                        .await
                        .map_err(ApiError::from)
                    {
                        Ok(entries) => entries.into_iter().map(Into::into).collect(),
                        Err(error) => {
                            return Ok(Response::Status500_ServerError(
                                error.into_server_error_model(),
                            ));
                        }
                    };
                    if let Some(reason) = &mut info.reason {
                        reason.log_entries = Some(reason_logs);
                    }
                }
                if self.build_service.is_starting(&path_params.build_id) {
                    info.status = models::TemplateBuildStatus::Waiting;
                } else if info.status == models::TemplateBuildStatus::Ready
                    && self.build_service.is_finishing(&path_params.build_id)
                {
                    // A sequential CLI build must observe the newly published cache seed.
                    info.status = models::TemplateBuildStatus::Building;
                    info.image_digest = None;
                }
                Ok(Response::Status200_SuccessfullyReturnedTheTemplate(info))
            }
            Err(error) => Ok(match error {
                ApiError::BadRequest(_) => Response::Status400_BadRequest(error.into_model()),
                ApiError::NotFound(_) => Response::Status404_NotFound(error.into_model()),
                other => Response::Status500_ServerError(other.into_server_error_model()),
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
        let result: Result<Vec<models::BuildLogEntry>, ApiError> = async {
            let record = self
                .build_service
                .build_lookup(&path_params.template_id, &path_params.build_id)
                .await?;
            let cursor = query_params
                .cursor
                .map(i64::try_from)
                .transpose()
                .map_err(|_| ApiError::bad_request("cursor exceeds int64 range"))?;
            let entries = self
                .build_service
                .query_build_logs(
                    record.id(),
                    BuildLogQuery {
                        offset: 0,
                        cursor,
                        limit: query_params.limit.unwrap_or(100) as usize,
                        backward: query_params.direction
                            == Some(models::LogsDirection::LogsDirectionBackward),
                        level: query_params.level.map(Into::into),
                        source: build_log_source_from_model(query_params.source),
                    },
                )
                .await?;
            Ok(entries.into_iter().map(Into::into).collect())
        }
        .await;
        Ok(match result {
            Ok(logs) => Response::Status200_SuccessfullyReturnedTheTemplateBuildLogs(
                models::TemplateBuildLogsResponse::new(logs),
            ),
            Err(error) => match error {
                ApiError::BadRequest(_) => Response::Status400_BadRequest(error.into_model()),
                ApiError::NotFound(_) => Response::Status404_NotFound(error.into_model()),
                other => Response::Status500_ServerError(other.into_server_error_model()),
            },
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
        match self
            .build_service
            .delete_template(&path_params.template_id)
            .await
            .map_err(ApiError::from)
        {
            Ok(()) => Ok(Response::Status204_TheTemplateWasDeletedSuccessfully),
            Err(error) => Ok(match error {
                ApiError::Conflict(_) => Response::Status409_Conflict(error.into_model()),
                ApiError::ServiceUnavailable(_) => {
                    Response::Status503_ServiceUnavailable(error.into_model())
                }
                other => Response::Status500_ServerError(other.into_server_error_model()),
            }),
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
                if let Ok(id) = SnapshotId::parse(&path_params.template_id) {
                    match self
                        .build_service
                        .get_template_build_job(id)
                        .await
                        .map_err(ApiError::from)
                    {
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
                                err.into_server_error_model(),
                            ));
                        }
                    }
                }
                return Ok(TemplatesTemplateIdGetResponse::Status404_NotFound(
                    ApiError::not_found(format!("template {} not found", path_params.template_id))
                        .into_model(),
                ));
            }
            Err(err) => {
                return Ok(TemplatesTemplateIdGetResponse::Status500_ServerError(
                    ApiError::from_snapshot_manager(&err).into_server_error_model(),
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
            let request = CreateImageBuildRequest {
                name: body.name.clone(),
                tags: body.tags.clone().unwrap_or_default(),
                has_resource_overrides: body.cpu_count.is_some() || body.memory_mb.is_some(),
            };
            return Ok(
                match self
                    .build_service
                    .create_image_build(request)
                    .await
                    .map_err(ApiError::from)
                {
                    Ok(created) => {
                        let id = created.build_id.to_string();
                        V3TemplatesPostResponse::Status202_TheBuildWasRequestedSuccessfully(
                            models::TemplateRequestResponseV3::new(
                                id.clone(),
                                id,
                                true,
                                vec![created.image_name],
                                created.tags,
                                Vec::new(),
                            ),
                        )
                    }
                    Err(err) => err.client_or_server_response(
                        V3TemplatesPostResponse::Status400_BadRequest,
                        V3TemplatesPostResponse::Status500_ServerError,
                    ),
                },
            );
        }
        let Some(name) = body.name.clone() else {
            return Ok(V3TemplatesPostResponse::Status400_BadRequest(
                ApiError::bad_request("template name must be provided").into_model(),
            ));
        };

        let target = match template_build_target_from_v3_request(body, &name) {
            Ok(target) => target,
            Err(err) => {
                return Ok(err.client_or_server_response(
                    V3TemplatesPostResponse::Status400_BadRequest,
                    V3TemplatesPostResponse::Status500_ServerError,
                ));
            }
        };

        let job = match self
            .build_service
            .create_template_build(target)
            .await
            .map_err(ApiError::from)
        {
            Ok(job) => job,
            Err(err) => {
                return Ok(err.client_or_server_response(
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
        let (build_id, alias, resources) = match self
            .build_service
            .load_template_build_target(&path_params.template_id, &path_params.build_id)
            .await
            .map_err(ApiError::from)
        {
            Ok(loaded) => loaded,
            Err(err) => return Ok(v2_start_build_error(err)),
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

        if let Err(err) = self
            .build_service
            .start_v2_build(build_id, base_source, registry_auth, spec)
            .await
            .map_err(ApiError::from)
        {
            return Ok(v2_start_build_error(err));
        }

        Ok(V2TemplatesTemplateIdBuildsBuildIdPostResponse::Status202_TheBuildHasStarted)
    }
}
