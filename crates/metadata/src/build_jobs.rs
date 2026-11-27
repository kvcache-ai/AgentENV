//! Asynchronous build job records for template and imageOnly builds.

use agentenv_domain::{
    is_sha256_digest, BuildErrorReason, BuildJobRecord, BuildJobTransition, BuildStatus,
    BuildTarget, ImageName, ImageTag, SnapshotAlias, SnapshotId,
};
use sqlx::AssertSqlSafe;

use crate::database::{begin_tx, commit_tx, dispatch_db, impl_sql_backend, Db, QueryRows};
use crate::layers::now_unix_ms;
use crate::{MetaError, MetaResult, MetaServer};

const BUILD_COLUMNS: &str = "CAST(build_id AS TEXT) AS build_id, status, target_json, result_image_digest, error_message, error_step, started_at, finished_at, created_at, updated_at";

#[derive(sqlx::FromRow)]
pub(crate) struct BuildJobRow {
    build_id: String,
    status: String,
    target_json: String,
    result_image_digest: Option<String>,
    error_message: Option<String>,
    error_step: Option<String>,
    started_at: Option<i64>,
    finished_at: Option<i64>,
    created_at: i64,
    updated_at: i64,
}

impl BuildJobRow {
    fn into_record(self) -> MetaResult<BuildJobRecord> {
        let build_id = SnapshotId::parse(&self.build_id)
            .map_err(|error| MetaError::backend("decode build ID", error))?;
        let status = BuildStatus::parse(&self.status)
            .map_err(|error| MetaError::backend("decode build status", error))?;
        let target: BuildTarget = serde_json::from_str(&self.target_json)
            .map_err(|error| MetaError::backend("decode build target", error))?;
        let error_reason = self.error_message.map(|message| BuildErrorReason {
            message,
            step: self.error_step,
        });
        Ok(BuildJobRecord {
            build_id,
            status,
            target,
            result_image_digest: self.result_image_digest,
            error_reason,
            started_at_unix_ms: self.started_at,
            finished_at_unix_ms: self.finished_at,
            created_at_unix_ms: self.created_at,
            updated_at_unix_ms: self.updated_at,
        })
    }
}

fn validate_target(target: &BuildTarget) -> MetaResult<()> {
    match target {
        BuildTarget::Template { alias, .. } => {
            if let Some(alias) = alias {
                SnapshotAlias::parse(alias.as_ref()).map_err(|error| {
                    MetaError::InvalidRequest {
                        reason: error.to_string(),
                    }
                })?;
            }
        }
        BuildTarget::Image { name, tags } => {
            let parsed_name =
                ImageName::parse(name).map_err(|error| MetaError::InvalidRequest {
                    reason: error.to_string(),
                })?;
            if tags.is_empty() {
                return Err(MetaError::InvalidRequest {
                    reason: "image build requires at least one tag".into(),
                });
            }
            for tag in tags {
                ImageTag::new(&parsed_name, tag).map_err(|error| MetaError::InvalidRequest {
                    reason: error.to_string(),
                })?;
            }
        }
    }
    Ok(())
}

impl MetaServer {
    pub async fn create_build_job(
        &self,
        build_id: &SnapshotId,
        target: &BuildTarget,
    ) -> MetaResult<BuildJobRecord> {
        validate_target(target)?;
        let target_json = serde_json::to_string(target)
            .map_err(|error| MetaError::backend("encode build target", error))?;
        dispatch_db!(&self.database, |pool| {
            Db(pool)
                .create_build_job(build_id, target, &target_json)
                .await
        })
    }

    pub async fn get_build_job(&self, build_id: &SnapshotId) -> MetaResult<Option<BuildJobRecord>> {
        let id = build_id.to_string();
        dispatch_db!(&self.database, |pool| Db(pool).get_build_job(&id).await)
    }

    pub async fn update_build_job(
        &self,
        build_id: &SnapshotId,
        transition: BuildJobTransition,
    ) -> MetaResult<Option<BuildJobRecord>> {
        if let BuildJobTransition::FinishImage { digest } = &transition {
            if !is_sha256_digest(digest) {
                return Err(MetaError::InvalidRequest {
                    reason: format!("invalid result image digest '{digest}'"),
                });
            }
        }
        let id = build_id.to_string();
        dispatch_db!(&self.database, |pool| {
            Db(pool).update_build_job(&id, &transition).await
        })
    }

    pub async fn delete_build_job(
        &self,
        build_id: &SnapshotId,
        reserve_snapshot_tombstone: bool,
    ) -> MetaResult<bool> {
        dispatch_db!(&self.database, |pool| {
            Db(pool)
                .delete_build_job(build_id, reserve_snapshot_tombstone)
                .await
        })
    }
}

impl_sql_backend! {
impl<D> Db<'_, D>
where
    for<'r> BuildJobRow: sqlx::FromRow<'r, D::Row>,
{
    async fn get_build_job(&self, id: &str) -> MetaResult<Option<BuildJobRecord>> {
        let row = sqlx::query_as::<D, BuildJobRow>(AssertSqlSafe(format!(
            "SELECT {BUILD_COLUMNS} FROM build_jobs WHERE build_id = {}",
            D::uuid(1),
        )))
        .bind(id)
        .fetch_optional(self.0)
        .await
        .map_err(|error| MetaError::backend("get build job", error))?;
        row.map(BuildJobRow::into_record).transpose()
    }

    async fn create_build_job(
        &self,
        build_id: &SnapshotId,
        target: &BuildTarget,
        target_json: &str,
    ) -> MetaResult<BuildJobRecord> {
        let id = build_id.to_string();
        let now = now_unix_ms();
        let mut tx = begin_tx(self.0, "begin build job creation").await?;
        if let BuildTarget::Template {
            alias: Some(alias), ..
        } = target
        {
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT CAST(snapshot_id AS TEXT) FROM snapshots
                 WHERE source_kind <> 'builder' AND state = 'active' AND alias = $1",
            )
            .bind(alias.as_ref())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("check template alias", error))?;
            if let Some(existing_id) = existing {
                let existing = SnapshotId::parse(&existing_id)
                    .map_err(|error| MetaError::backend("decode alias owner", error))?;
                return Err(MetaError::AliasConflict {
                    alias: alias.to_string(),
                    existing,
                    new_id: build_id.clone(),
                });
            }
        }
        let inserted = sqlx::query::<D>(AssertSqlSafe(format!(
            "INSERT INTO build_jobs (build_id, status, target_json, created_at, updated_at)
             VALUES ({}, 'waiting', $2, $3, $3)
             ON CONFLICT (build_id) DO NOTHING",
            D::uuid(1),
        )))
        .bind(id.as_str())
        .bind(target_json)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("insert build job", error))?;
        if inserted.rows_affected() == 0 {
            return Err(MetaError::BuildConflict {
                id: build_id.clone(),
            });
        }
        commit_tx(tx, "commit build job creation").await?;
        Ok(BuildJobRecord::waiting(
            build_id.clone(),
            target.clone(),
            now,
        ))
    }

    async fn update_build_job(
        &self,
        id: &str,
        transition: &BuildJobTransition,
    ) -> MetaResult<Option<BuildJobRecord>> {
        let now = now_unix_ms();
        let row = match transition {
            BuildJobTransition::Start => sqlx::query_as::<D, BuildJobRow>(AssertSqlSafe(format!(
                "UPDATE build_jobs
                 SET status = 'building', started_at = $2, updated_at = $2
                 WHERE build_id = {} AND status = 'waiting'
                 RETURNING {BUILD_COLUMNS}",
                D::uuid(1),
            )))
            .bind(id)
            .bind(now)
            .fetch_optional(self.0)
            .await
            .map_err(|error| MetaError::backend("start build job", error))?,
            BuildJobTransition::FinishImage { digest } => {
                sqlx::query_as::<D, BuildJobRow>(AssertSqlSafe(format!(
                    "UPDATE build_jobs
                     SET status = 'ready',
                         result_image_digest = $2,
                         error_message = NULL,
                         error_step = NULL,
                         finished_at = $3,
                         updated_at = $3
                     WHERE build_id = {} AND status IN ('waiting', 'building')
                     RETURNING {BUILD_COLUMNS}",
                    D::uuid(1),
                )))
                .bind(id)
                .bind(digest.as_str())
                .bind(now)
                .fetch_optional(self.0)
                .await
                .map_err(|error| MetaError::backend("finish build job", error))?
            }
            BuildJobTransition::Fail { reason } => {
                sqlx::query_as::<D, BuildJobRow>(AssertSqlSafe(format!(
                    "UPDATE build_jobs
                     SET status = 'error', error_message = $2, error_step = $3, finished_at = $4, updated_at = $4
                     WHERE build_id = {} AND status IN ('waiting', 'building')
                     RETURNING {BUILD_COLUMNS}",
                    D::uuid(1),
                )))
                .bind(id)
                .bind(reason.message.as_str())
                .bind(reason.step.as_deref())
                .bind(now)
                .fetch_optional(self.0)
                .await
                .map_err(|error| MetaError::backend("fail build job", error))?
            }
        };
        row.map(BuildJobRow::into_record).transpose()
    }

    async fn delete_build_job(
        &self,
        build_id: &SnapshotId,
        reserve_snapshot_tombstone: bool,
    ) -> MetaResult<bool> {
        let id = build_id.to_string();
        let mut tx = begin_tx(self.0, "begin build job deletion").await?;
        let status: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT status FROM build_jobs WHERE build_id = {}{}",
            D::uuid(1),
            D::FOR_UPDATE,
        )))
        .bind(id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("lock build job for deletion", error))?;
        if status.as_deref() == Some("building") {
            return Err(MetaError::BuildConflict {
                id: build_id.clone(),
            });
        }
        let deleted = sqlx::query::<D>(AssertSqlSafe(format!(
            "DELETE FROM build_jobs WHERE build_id = {}",
            D::uuid(1),
        )))
        .bind(id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("delete build job", error))?;
        if reserve_snapshot_tombstone {
            sqlx::query::<D>(AssertSqlSafe(format!(
                "INSERT INTO snapshots (snapshot_id, source_kind, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at)
                 VALUES ({}, 'template', NULL, 'reserved', NULL, NULL, NULL, $2, $2)
                 ON CONFLICT (snapshot_id) DO UPDATE
                 SET state = 'retired', alias = NULL, updated_at = EXCLUDED.updated_at
                 WHERE snapshots.state = 'active'",
                D::uuid(1),
            )))
            .bind(id.as_str())
            .bind(now_unix_ms())
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                MetaError::backend("reserve deleted template snapshot tombstone", error)
            })?;
        }
        commit_tx(tx, "commit build job deletion").await?;
        Ok(deleted.rows_affected() > 0)
    }
}
}
