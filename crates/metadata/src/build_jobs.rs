//! Asynchronous build job records for template and imageOnly builds.

use agentenv_domain::{
    is_sha256_digest, BuildErrorReason, BuildJobRecord, BuildKind, BuildStatus, BuildTarget,
    ImageName, ImageTag, SnapshotAlias, SnapshotId,
};
use sqlx::{AssertSqlSafe, ColumnIndex, Decode, Encode, Executor, IntoArguments, Pool, Type};

use crate::database::{begin_tx, commit_tx, dispatch_db, QueryRows, SqlDialect};
use crate::layer_graph::now_unix_ms;
use crate::{MetaError, MetaResult, MetaServer};

const BUILD_COLUMNS: &str = "CAST(build_id AS TEXT) AS build_id, kind, status, target_json, CAST(result_snapshot_id AS TEXT) AS result_snapshot_id, result_image_digest, error_message, error_step, started_at, finished_at, created_at, updated_at";

#[derive(sqlx::FromRow)]
struct BuildJobRow {
    build_id: String,
    kind: String,
    status: String,
    target_json: String,
    result_snapshot_id: Option<String>,
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
        let kind = BuildKind::parse(&self.kind)
            .map_err(|error| MetaError::backend("decode build kind", error))?;
        let status = BuildStatus::parse(&self.status)
            .map_err(|error| MetaError::backend("decode build status", error))?;
        let target: BuildTarget = serde_json::from_str(&self.target_json)
            .map_err(|error| MetaError::backend("decode build target", error))?;
        let result_snapshot_id = self
            .result_snapshot_id
            .as_deref()
            .map(SnapshotId::parse)
            .transpose()
            .map_err(|error| MetaError::backend("decode build result snapshot ID", error))?;
        let error_reason = self.error_message.map(|message| BuildErrorReason {
            message,
            step: self.error_step,
        });
        Ok(BuildJobRecord {
            build_id,
            kind,
            status,
            target,
            result_snapshot_id,
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
            BuildJobStore(pool)
                .create(build_id, target, &target_json)
                .await
        })
    }

    pub async fn get_build_job(&self, build_id: &SnapshotId) -> MetaResult<Option<BuildJobRecord>> {
        let id = build_id.to_string();
        dispatch_db!(&self.database, |pool| BuildJobStore(pool).get(&id).await)
    }

    pub async fn start_build_job(&self, build_id: &SnapshotId) -> MetaResult<bool> {
        let id = build_id.to_string();
        dispatch_db!(&self.database, |pool| BuildJobStore(pool).start(&id).await)
    }

    pub async fn finish_build_job(
        &self,
        build_id: &SnapshotId,
        result_snapshot_id: Option<&SnapshotId>,
        result_image_digest: Option<&str>,
    ) -> MetaResult<Option<BuildJobRecord>> {
        if result_snapshot_id.is_some() && result_image_digest.is_some() {
            return Err(MetaError::InvalidRequest {
                reason: "build job cannot record both a snapshot ID and an image digest".into(),
            });
        }
        if let Some(digest) = result_image_digest {
            if !is_sha256_digest(digest) {
                return Err(MetaError::InvalidRequest {
                    reason: format!("invalid result image digest '{digest}'"),
                });
            }
        }
        dispatch_db!(&self.database, |pool| {
            BuildJobStore(pool)
                .finish(build_id, result_snapshot_id, result_image_digest)
                .await
        })
    }

    pub async fn fail_build_job(
        &self,
        build_id: &SnapshotId,
        reason: &BuildErrorReason,
    ) -> MetaResult<Option<BuildJobRecord>> {
        let id = build_id.to_string();
        dispatch_db!(&self.database, |pool| BuildJobStore(pool)
            .fail(&id, reason)
            .await)
    }

    pub async fn delete_build_job(
        &self,
        build_id: &SnapshotId,
        reserve_snapshot_tombstone: bool,
    ) -> MetaResult<bool> {
        dispatch_db!(&self.database, |pool| {
            BuildJobStore(pool)
                .delete(build_id, reserve_snapshot_tombstone)
                .await
        })
    }
}

struct BuildJobStore<'a, D: SqlDialect>(&'a Pool<D>);

impl<D> BuildJobStore<'_, D>
where
    D: SqlDialect<QueryResult: QueryRows, Arguments: IntoArguments<D>>,
    for<'p> &'p Pool<D>: Executor<'p, Database = D>,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> Option<&'b str>: Encode<'b, D> + Type<D>,
    for<'b> String: Decode<'b, D> + Type<D>,
    for<'b> i64: Encode<'b, D> + Type<D>,
    for<'r> BuildJobRow: sqlx::FromRow<'r, D::Row>,
    usize: ColumnIndex<D::Row>,
{
    async fn get(&self, id: &str) -> MetaResult<Option<BuildJobRecord>> {
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

    async fn start(&self, id: &str) -> MetaResult<bool> {
        let updated = sqlx::query::<D>(AssertSqlSafe(format!(
            "UPDATE build_jobs
             SET status = 'building', started_at = $2, updated_at = $2
             WHERE build_id = {} AND status = 'waiting'",
            D::uuid(1),
        )))
        .bind(id)
        .bind(now_unix_ms())
        .execute(self.0)
        .await
        .map_err(|error| MetaError::backend("start build job", error))?;
        Ok(updated.rows_affected() == 1)
    }

    async fn fail(
        &self,
        id: &str,
        reason: &BuildErrorReason,
    ) -> MetaResult<Option<BuildJobRecord>> {
        let row = sqlx::query_as::<D, BuildJobRow>(AssertSqlSafe(format!(
            "UPDATE build_jobs
             SET status = 'error', error_message = $2, error_step = $3, finished_at = $4, updated_at = $4
             WHERE build_id = {} AND status IN ('waiting', 'building')
             RETURNING {BUILD_COLUMNS}",
            D::uuid(1),
        )))
        .bind(id)
        .bind(reason.message.as_str())
        .bind(reason.step.as_deref())
        .bind(now_unix_ms())
        .fetch_optional(self.0)
        .await
        .map_err(|error| MetaError::backend("fail build job", error))?;
        row.map(BuildJobRow::into_record).transpose()
    }

    async fn create(
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
                 WHERE visibility = 'public' AND state = 'active' AND alias = $1",
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
            "INSERT INTO build_jobs (build_id, kind, status, target_json, created_at, updated_at)
             VALUES ({}, $2, 'waiting', $3, $4, $4)
             ON CONFLICT (build_id) DO NOTHING",
            D::uuid(1),
        )))
        .bind(id.as_str())
        .bind(target.kind().as_str())
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

    async fn finish(
        &self,
        build_id: &SnapshotId,
        result_snapshot_id: Option<&SnapshotId>,
        result_image_digest: Option<&str>,
    ) -> MetaResult<Option<BuildJobRecord>> {
        let id = build_id.to_string();
        let snapshot_id = result_snapshot_id.map(ToString::to_string);
        let row = sqlx::query_as::<D, BuildJobRow>(AssertSqlSafe(format!(
            "UPDATE build_jobs
             SET status = 'ready',
                 result_snapshot_id = COALESCE({}, result_snapshot_id),
                 result_image_digest = COALESCE($3, result_image_digest),
                 error_message = NULL,
                 error_step = NULL,
                 finished_at = $4,
                 updated_at = $4
             WHERE build_id = {} AND status IN ('waiting', 'building')
             RETURNING {BUILD_COLUMNS}",
            D::uuid(2),
            D::uuid(1),
        )))
        .bind(id.as_str())
        .bind(snapshot_id.as_deref())
        .bind(result_image_digest)
        .bind(now_unix_ms())
        .fetch_optional(self.0)
        .await
        .map_err(|error| MetaError::backend("finish build job", error))?;
        row.map(BuildJobRow::into_record).transpose()
    }

    async fn delete(
        &self,
        build_id: &SnapshotId,
        reserve_snapshot_tombstone: bool,
    ) -> MetaResult<bool> {
        let id = build_id.to_string();
        let mut tx = begin_tx(self.0, "begin build job deletion").await?;
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
                "INSERT INTO snapshots (snapshot_id, source_kind, visibility, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at)
                 VALUES ({}, 'template', 'public', NULL, 'reserved', NULL, NULL, NULL, $2, $2)
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
