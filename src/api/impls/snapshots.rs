use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use agentenv_http_server::apis::snapshots::*;
use agentenv_http_server::models;

use crate::snapshot::{SnapshotId, SnapshotRecord, SnapshotSource};

use super::pagination::PaginationCursor;
use super::ApiImpl;

impl From<SnapshotRecord> for models::SnapshotInfo {
    fn from(record: SnapshotRecord) -> Self {
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
            cpu_count: record.resources.cpu_count,
            memory_mb: record.resources.memory_mib,
            disk_size_mb: record.resources.disk_size_mib,
            created_at: chrono::DateTime::<chrono::Utc>::from(system_time_from_unix_ms(
                record.created_at_unix_ms,
            )),
            updated_at: chrono::DateTime::<chrono::Utc>::from(system_time_from_unix_ms(
                record.updated_at_unix_ms,
            )),
            image_ref,
        }
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
                Ok(cursor) => cursor,
                Err(err) => {
                    return Ok(SnapshotsGetResponse::Status400_BadRequest(Self::error(
                        400,
                        format!("invalid next token: {}", err),
                    )));
                }
            },
            None => PaginationCursor::new(SystemTime::now(), SnapshotId::max()),
        };

        let summaries = match self
            .snapshot_manager
            .list(crate::snapshot::SnapshotListFilter::sandbox_snapshots(
                query_params.sandbox_id.clone(),
                query_params.name.clone(),
            ))
            .await
        {
            Ok(summaries) => summaries,
            Err(err) => {
                return Ok(SnapshotsGetResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
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
                PaginationCursor::new(
                    system_time_from_unix_ms(record.created_at_unix_ms),
                    record.id.clone(),
                )
            },
        );

        Ok(
            SnapshotsGetResponse::Status200_SuccessfullyReturnedSnapshots {
                body: page
                    .items
                    .into_iter()
                    .map(models::SnapshotInfo::from)
                    .collect(),
                x_next_token: page.next_token,
            },
        )
    }

    async fn snapshots_snapshot_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SnapshotsSnapshotIdGetPathParams,
    ) -> Result<SnapshotsSnapshotIdGetResponse, ()> {
        match self.snapshot_manager.get(&path_params.snapshot_id).await {
            // Scope this endpoint to sandbox-sourced snapshots so it stays
            // consistent with the list API, which only exposes
            // `SnapshotSourceKind::Sandbox` records. Template records are
            // surfaced through the template APIs instead.
            Ok(Some(record)) if matches!(record.source, SnapshotSource::Sandbox { .. }) => Ok(
                SnapshotsSnapshotIdGetResponse::Status200_SuccessfullyReturnedTheSnapshot(
                    models::SnapshotInfo::from(record),
                ),
            ),
            Ok(_) => Ok(SnapshotsSnapshotIdGetResponse::Status404_NotFound(
                Self::error(
                    404,
                    format!("snapshot '{}' not found", path_params.snapshot_id),
                ),
            )),
            Err(err) => Ok(SnapshotsSnapshotIdGetResponse::Status500_ServerError(
                Self::snapshot_manager_error(&err),
            )),
        }
    }

    async fn snapshots_snapshot_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::SnapshotsSnapshotIdDeletePathParams,
    ) -> Result<SnapshotsSnapshotIdDeleteResponse, ()> {
        let snapshot_id = match self.snapshot_manager.get(&path_params.snapshot_id).await {
            Ok(Some(record)) if matches!(record.source, SnapshotSource::Sandbox { .. }) => {
                record.id.to_string()
            }
            Ok(None) => {
                return Ok(
                    SnapshotsSnapshotIdDeleteResponse::Status204_TheSnapshotWasDeletedOrWasAlreadyAbsent,
                );
            }
            Ok(Some(_)) => {
                return Ok(SnapshotsSnapshotIdDeleteResponse::Status404_NotFound(
                    Self::error(
                        404,
                        format!("snapshot '{}' not found", path_params.snapshot_id),
                    ),
                ));
            }
            Err(err) => {
                return Ok(SnapshotsSnapshotIdDeleteResponse::Status500_ServerError(
                    Self::snapshot_manager_error(&err),
                ));
            }
        };
        match self.snapshot_manager.delete(&snapshot_id).await {
            Ok(()) => Ok(
                SnapshotsSnapshotIdDeleteResponse::Status204_TheSnapshotWasDeletedOrWasAlreadyAbsent,
            ),
            Err(err) => Ok(SnapshotsSnapshotIdDeleteResponse::Status500_ServerError(
                Self::snapshot_manager_error(&err),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{
        rootfs_snapshot_image_tag, CommittedSnapshot, PersistedDiskImagePublication,
    };

    #[test]
    fn snapshot_info_includes_published_rootfs_image_ref() {
        let mut record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let tag = rootfs_snapshot_image_tag(&record.id);
        let expected = format!("registry.example/ns/app:{tag}");
        record.committed.as_mut().unwrap().disk_publications =
            vec![PersistedDiskImagePublication {
                image_ref: expected.clone(),
                tag,
                manifest_digest: "sha256:manifest".to_string(),
                repo_blob_url: "https://registry.example/v2/ns/app/blobs".to_string(),
            }];

        let info = models::SnapshotInfo::from(record);

        assert_eq!(info.image_ref.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn snapshot_info_omits_image_ref_without_publication() {
        let record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());

        let info = models::SnapshotInfo::from(record);

        assert_eq!(info.image_ref, None);
        let serialized = serde_json::to_value(&info).expect("serialize SnapshotInfo");
        assert!(serialized.get("imageRef").is_none());
    }
}
