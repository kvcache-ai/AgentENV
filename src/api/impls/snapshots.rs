use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use agentenv_http_server::apis::snapshots::*;
use agentenv_http_server::models;

use crate::repository::RepositoryError;
use crate::snapshot::{SnapshotId, SnapshotRecord, SnapshotSource, SnapshotSourceKind};

use super::error::ApiError;
use super::pagination::PaginationCursor;
use super::ApiImpl;

pub(super) fn snapshot_info_from_record(record: SnapshotRecord) -> models::SnapshotInfo {
    let snapshot_id = record.id.to_string();
    let image_ref = record.published_rootfs_image_ref().map(str::to_owned);
    let names = if let Some(alias) = record.alias {
        vec![alias.to_string()]
    } else {
        vec![]
    };
    models::SnapshotInfo {
        snapshot_id,
        names,
        cpu_count: record.config.resources.cpu_count,
        memory_mb: record.config.resources.memory_mib,
        disk_size_mb: record.config.resources.disk_size_mib,
        created_at: chrono::DateTime::<chrono::Utc>::from(system_time_from_unix_ms(
            record.created_at_unix_ms,
        )),
        updated_at: chrono::DateTime::<chrono::Utc>::from(system_time_from_unix_ms(
            record.updated_at_unix_ms,
        )),
        image_ref,
        snapshot_type: Some(match record.snapshot_type {
            crate::snapshot::SnapshotPlacement::Local => models::SnapshotType::Local,
            crate::snapshot::SnapshotPlacement::Distributed => models::SnapshotType::Distributed,
        }),
    }
}

fn system_time_from_unix_ms(unix_ms: i64) -> SystemTime {
    if unix_ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(unix_ms as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(unix_ms.unsigned_abs())
    }
}

#[async_trait]
impl Snapshots<()> for ApiImpl {
    type Claims = super::Claims;

    async fn snapshots_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::SnapshotsGetQueryParams,
    ) -> Result<SnapshotsGetResponse, ()> {
        let cursor = match query_params.next_token.as_deref() {
            Some(token) => match PaginationCursor::<SnapshotId>::parse(token) {
                Ok(cursor) if cursor.is_descending() => cursor,
                Ok(_) => {
                    return Ok(SnapshotsGetResponse::Status400_BadRequest(
                        ApiError::bad_request("next token was issued for a different sort order")
                            .into_model(),
                    ));
                }
                Err(err) => {
                    return Ok(SnapshotsGetResponse::Status400_BadRequest(
                        ApiError::bad_request(format!("invalid next token: {err}")).into_model(),
                    ));
                }
            },
            None => PaginationCursor::new_descending(SystemTime::now(), SnapshotId::max()),
        };

        let server_error = |err: &anyhow::Error| match ApiError::from_reusable_snapshot_manager(err)
        {
            error @ ApiError::ServiceUnavailable(_) => {
                SnapshotsGetResponse::Status503_ServiceUnavailable(error.into_model())
            }
            error => SnapshotsGetResponse::Status500_ServerError(error.into_server_error_model()),
        };

        let summaries = match self
            .snapshot_manager
            .list_metadata(crate::snapshot::SnapshotListFilter::sandbox_snapshots(
                query_params.sandbox_id.clone(),
                query_params.name.clone(),
            ))
            .await
        {
            Ok(summaries) => summaries,
            Err(err) => return Ok(server_error(&err)),
        };

        let page = cursor.paginate_sorted(
            summaries,
            query_params.limit,
            |record, cursor| {
                PaginationCursor::compare_desc(
                    system_time_from_unix_ms(record.created_at_unix_ms),
                    &record.id,
                    cursor.time(),
                    cursor.value(),
                )
            },
            |record| {
                PaginationCursor::new_descending(
                    system_time_from_unix_ms(record.created_at_unix_ms),
                    record.id.clone(),
                )
            },
        );

        let records = match self
            .snapshot_manager
            .list(crate::snapshot::SnapshotListFilter::by_ids(
                page.items.into_iter().map(|entry| entry.id),
            ))
            .await
        {
            Ok(records) => records,
            Err(err) => return Ok(server_error(&err)),
        };
        Ok(
            SnapshotsGetResponse::Status200_SuccessfullyReturnedSnapshots {
                body: records.into_iter().map(snapshot_info_from_record).collect(),
                x_next_token: page.next_token,
            },
        )
    }

    async fn snapshots_snapshot_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SnapshotsSnapshotIdDeletePathParams,
        // The type hint only guides upstream request routing; the API
        // semantics do not depend on it.
        _query_params: &models::SnapshotsSnapshotIdDeleteQueryParams,
    ) -> Result<SnapshotsSnapshotIdDeleteResponse, ()> {
        // Mirror the scoping of the get/list endpoints: only sandbox-sourced
        // snapshots are exposed here. Template records must be deleted through
        // the template APIs instead.
        let server_error = |err: &anyhow::Error| match ApiError::from_reusable_snapshot_manager(err)
        {
            error @ ApiError::ServiceUnavailable(_) => {
                SnapshotsSnapshotIdDeleteResponse::Status503_ServiceUnavailable(error.into_model())
            }
            error => SnapshotsSnapshotIdDeleteResponse::Status500_ServerError(
                error.into_server_error_model(),
            ),
        };
        let entry = match self
            .snapshot_manager
            .get_metadata(&path_params.snapshot_id)
            .await
        {
            Ok(Some(entry)) if entry.source_kind == SnapshotSourceKind::Sandbox => entry,
            Ok(_) => {
                return Ok(SnapshotsSnapshotIdDeleteResponse::Status404_NotFound(
                    ApiError::not_found(format!(
                        "snapshot '{}' not found",
                        path_params.snapshot_id
                    ))
                    .into_model(),
                ))
            }
            Err(err) => return Ok(server_error(&err)),
        };

        match self.snapshot_manager.delete(entry.id.to_string()).await {
            Ok(()) => {
                Ok(SnapshotsSnapshotIdDeleteResponse::Status204_TheSnapshotWasDeletedSuccessfully)
            }
            Err(err) => Ok(server_error(&err)),
        }
    }

    async fn snapshots_snapshot_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SnapshotsSnapshotIdGetPathParams,
        // The type hint only guides upstream request routing; the API
        // semantics do not depend on it.
        _query_params: &models::SnapshotsSnapshotIdGetQueryParams,
    ) -> Result<SnapshotsSnapshotIdGetResponse, ()> {
        match self.snapshot_manager.get(&path_params.snapshot_id).await {
            // Scope this endpoint to sandbox-sourced snapshots so it stays
            // consistent with the list API, which only exposes
            // `SnapshotSourceKind::Sandbox` records. Template records are
            // surfaced through the template APIs instead.
            Ok(Some(record)) if matches!(record.config.source, SnapshotSource::Sandbox { .. }) => {
                Ok(
                    SnapshotsSnapshotIdGetResponse::Status200_SuccessfullyReturnedTheSnapshot(
                        snapshot_info_from_record(record),
                    ),
                )
            }
            Ok(_) => Ok(SnapshotsSnapshotIdGetResponse::Status404_NotFound(
                ApiError::not_found(format!("snapshot '{}' not found", path_params.snapshot_id))
                    .into_model(),
            )),
            Err(err) => Ok(match ApiError::from_reusable_snapshot_manager(&err) {
                error @ ApiError::BadRequest(_) => {
                    SnapshotsSnapshotIdGetResponse::Status400_BadRequest(error.into_model())
                }
                error @ ApiError::ServiceUnavailable(_) => {
                    SnapshotsSnapshotIdGetResponse::Status503_ServiceUnavailable(error.into_model())
                }
                error => SnapshotsSnapshotIdGetResponse::Status500_ServerError(
                    error.into_server_error_model(),
                ),
            }),
        }
    }

    async fn snapshots_snapshot_id_promote_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SnapshotsSnapshotIdPromotePostPathParams,
    ) -> Result<SnapshotsSnapshotIdPromotePostResponse, ()> {
        match self
            .snapshot_manager
            .promote(&path_params.snapshot_id)
            .await
        {
            Ok(Some(record)) => Ok(
                SnapshotsSnapshotIdPromotePostResponse::Status200_SnapshotIsAvailableAsDistributed(
                    snapshot_info_from_record(record),
                ),
            ),
            Ok(None) => Ok(SnapshotsSnapshotIdPromotePostResponse::Status404_NotFound(
                ApiError::not_found(format!("snapshot '{}' not found", path_params.snapshot_id))
                    .into_model(),
            )),
            Err(error) => {
                let error = match error {
                    RepositoryError::InvalidRequest { .. } => ApiError::conflict(error.to_string()),
                    _ => ApiError::from_reusable_repository(&error),
                };
                Ok(match error {
                    ApiError::NotFound(_) => {
                        SnapshotsSnapshotIdPromotePostResponse::Status404_NotFound(
                            error.into_model(),
                        )
                    }
                    ApiError::Conflict(_) => {
                        SnapshotsSnapshotIdPromotePostResponse::Status409_Conflict(
                            error.into_model(),
                        )
                    }
                    ApiError::ServiceUnavailable(_) => {
                        SnapshotsSnapshotIdPromotePostResponse::Status503_ServiceUnavailable(
                            error.into_model(),
                        )
                    }
                    other => SnapshotsSnapshotIdPromotePostResponse::Status500_ServerError(
                        other.into_server_error_model(),
                    ),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{
        rootfs_snapshot_image_tag, PersistedDiskImagePublication, SnapshotManifest,
    };

    #[test]
    fn snapshot_info_includes_published_rootfs_image_ref() {
        let mut record = SnapshotRecord::mock_ready(SnapshotManifest::mock());
        let tag = rootfs_snapshot_image_tag(&record.id);
        let expected = format!("registry.example/ns/app:{tag}");
        record.manifest.as_mut().unwrap().disk_publications = vec![PersistedDiskImagePublication {
            image_ref: expected.clone(),
            tag,
            manifest_digest: "sha256:manifest".to_string(),
            repo_blob_url: "https://registry.example/v2/ns/app/blobs".to_string(),
        }];

        let info = snapshot_info_from_record(record);

        assert_eq!(info.image_ref.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn snapshot_info_omits_image_ref_without_publication() {
        let record = SnapshotRecord::mock_ready(SnapshotManifest::mock());

        let info = snapshot_info_from_record(record);

        assert_eq!(info.image_ref, None);
        let serialized = serde_json::to_value(&info).expect("serialize SnapshotInfo");
        assert!(serialized.get("imageRef").is_none());
    }
}
