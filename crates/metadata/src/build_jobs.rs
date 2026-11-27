//! Asynchronous build job records for template and imageOnly builds.

use agentenv_domain::{
    is_sha256_digest, BuildErrorReason, BuildJobRecord, BuildJobTransition, BuildStatus,
    BuildTarget, ImageName, ImageTag, SnapshotId, SnapshotSourceKind,
};

use crate::db::{DbRow, MetaSqlExt, Sql};
use crate::layers::now_unix_ms;
use crate::snapshots::tombstone_sql;
use crate::{MetaError, MetaResult, MetaServer};

const COLS: &str = "build_id, status, target_json, result_image_digest, error_message, error_step, started_at, finished_at, created_at, updated_at";

fn decode_build_job(row: DbRow) -> MetaResult<BuildJobRecord> {
    let step = row.get(5)?;
    Ok(BuildJobRecord {
        build_id: row.snapshot_id(0)?,
        status: BuildStatus::parse(&row.get::<String>(1)?)
            .map_err(|e| MetaError::backend("decode build status", e))?,
        target: serde_json::from_str(&row.get::<String>(2)?)
            .map_err(|e| MetaError::backend("decode build target", e))?,
        result_image_digest: row.get(3)?,
        error_reason: row
            .get::<Option<String>>(4)?
            .map(|message| BuildErrorReason { message, step }),
        started_at_unix_ms: row.get(6)?,
        finished_at_unix_ms: row.get(7)?,
        created_at_unix_ms: row.get(8)?,
        updated_at_unix_ms: row.get(9)?,
    })
}

impl MetaServer {
    pub async fn create_build_job(
        &self,
        build_id: &SnapshotId,
        target: &BuildTarget,
    ) -> MetaResult<BuildJobRecord> {
        if let BuildTarget::Image { name, tags } = target {
            let parsed_name =
                ImageName::parse(name).map_err(|e| MetaError::invalid(e.to_string()))?;
            if tags.is_empty() {
                return Err(MetaError::invalid("image build requires at least one tag"));
            }
            for tag in tags {
                ImageTag::new(&parsed_name, tag).map_err(|e| MetaError::invalid(e.to_string()))?;
            }
        }
        let target_json = serde_json::to_string(target)
            .map_err(|e| MetaError::backend("encode build target", e))?;
        let now = now_unix_ms();
        let mut tx = self.database.begin("begin build job creation").await?;
        if let BuildTarget::Template {
            alias: Some(alias), ..
        } = target
        {
            if let Some(row) = tx
                .fetch_optional(
                    Sql::new(
                        "SELECT snapshot_id FROM snapshots
                         WHERE source_kind <> 'builder' AND state = 'active' AND alias = $1",
                    )
                    .bind(alias.as_ref()),
                )
                .await
                .meta_err("check template alias")?
            {
                return Err(MetaError::AliasConflict {
                    alias: alias.to_string(),
                    existing: row.snapshot_id(0)?,
                    new_id: build_id.clone(),
                });
            }
        }
        let inserted = tx
            .execute(
                Sql::new(
                    "INSERT INTO build_jobs (build_id, status, target_json, created_at, updated_at)
                     VALUES ($1, 'waiting', $2, $3, $3)
                     ON CONFLICT (build_id) DO NOTHING",
                )
                .bind(build_id)
                .bind(&target_json)
                .bind(now),
            )
            .await
            .meta_err("insert build job")?;
        if inserted == 0 {
            return Err(MetaError::BuildConflict {
                id: build_id.clone(),
            });
        }
        tx.commit("commit build job creation").await?;
        Ok(BuildJobRecord::waiting(
            build_id.clone(),
            target.clone(),
            now,
        ))
    }

    pub async fn get_build_job(&self, build_id: &SnapshotId) -> MetaResult<Option<BuildJobRecord>> {
        self.database
            .fetch_optional(
                Sql::new(format!("SELECT {COLS} FROM build_jobs WHERE build_id = $1"))
                    .bind(build_id),
            )
            .await
            .meta_err("get build job")?
            .map(decode_build_job)
            .transpose()
    }

    pub async fn update_build_job(
        &self,
        build_id: &SnapshotId,
        transition: BuildJobTransition,
    ) -> MetaResult<Option<BuildJobRecord>> {
        let now = now_unix_ms();
        let (sql, ctx) = match &transition {
            BuildJobTransition::Start => (
                Sql::new(format!(
                    "UPDATE build_jobs SET status = 'building', started_at = $2, updated_at = $2
                     WHERE build_id = $1 AND status = 'waiting' RETURNING {COLS}"
                ))
                .bind(build_id)
                .bind(now),
                "start build job",
            ),
            BuildJobTransition::FinishImage { digest } => {
                if !is_sha256_digest(digest) {
                    return Err(MetaError::invalid(format!("invalid result image digest '{digest}'")));
                }
                (
                    Sql::new(format!(
                        "UPDATE build_jobs
                         SET status = 'ready', result_image_digest = $2,
                             error_message = NULL, error_step = NULL, finished_at = $3, updated_at = $3
                         WHERE build_id = $1 AND status IN ('waiting', 'building') RETURNING {COLS}"
                    ))
                    .bind(build_id)
                    .bind(digest.as_str())
                    .bind(now),
                    "finish build job",
                )
            }
            BuildJobTransition::Fail { reason } => (
                Sql::new(format!(
                    "UPDATE build_jobs
                     SET status = 'error', error_message = $2, error_step = $3, finished_at = $4, updated_at = $4
                     WHERE build_id = $1 AND status IN ('waiting', 'building') RETURNING {COLS}"
                ))
                .bind(build_id)
                .bind(reason.message.as_str())
                .bind(reason.step.as_deref())
                .bind(now),
                "fail build job",
            ),
        };
        self.database
            .fetch_optional(sql)
            .await
            .meta_err(ctx)?
            .map(decode_build_job)
            .transpose()
    }

    pub async fn delete_build_job(
        &self,
        build_id: &SnapshotId,
        reserve_snapshot_tombstone: bool,
    ) -> MetaResult<bool> {
        let mut tx = self.database.begin("begin build job deletion").await?;
        let status: Option<String> = tx
            .fetch_optional(
                Sql::new("SELECT status FROM build_jobs WHERE build_id = $1")
                    .for_update()
                    .bind(build_id),
            )
            .await
            .meta_err("lock build job for deletion")?
            .map(|row| row.get(0))
            .transpose()?;
        if status.as_deref() == Some("building") {
            return Err(MetaError::BuildConflict {
                id: build_id.clone(),
            });
        }
        let deleted = tx
            .execute(Sql::new("DELETE FROM build_jobs WHERE build_id = $1").bind(build_id))
            .await
            .meta_err("delete build job")?;
        if reserve_snapshot_tombstone {
            tx.execute(tombstone_sql(build_id, SnapshotSourceKind::Template))
                .await
                .meta_err("reserve deleted template snapshot tombstone")?;
        }
        tx.commit("commit build job deletion").await?;
        Ok(deleted > 0)
    }
}
