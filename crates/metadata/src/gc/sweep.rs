use std::collections::BTreeMap;

use agentenv_domain::SnapshotId;
use anyhow::Result;
use sqlx::{
    AssertSqlSafe, ColumnIndex, Connection, Database, Decode, Encode, Executor, IntoArguments,
    Postgres, QueryBuilder, Sqlite, Type,
};

use super::{GcCursor, GcReport, GcStorage};
use crate::database::DatabasePool;
use crate::{MetaError, MetaResult, MetaServer};

pub(super) const ADVISORY_LOCK_KEY: i64 = 0x6165_6e76_6763_0001;
/// Retired snapshots per checkpoint batch (2-3 heads each fills one OSS delete batch).
pub(super) const SNAPSHOT_BATCH_SIZE: usize = 256;
/// Interrupted `deleting` layers per page (aligned with S3/OSS 1,000-key batch ceiling).
pub(super) const LAYER_BATCH_SIZE: usize = 1000;
/// Consecutive zero-progress batches before pausing for caller backoff.
pub(super) const MAX_STALLED_BATCHES: usize = 2;
/// Max pages per `collect_garbage` call to bound work under continuous retirements.
const MAX_SWEEP_BATCHES: usize = 32;

const UNREFERENCED_AVAILABLE_LEAF: &str = "l.state = 'available' \
     AND NOT EXISTS (SELECT 1 FROM snapshot_heads h WHERE h.layer_uuid = l.uuid) \
     AND NOT EXISTS (SELECT 1 FROM layers ch WHERE ch.managed_parent_uuid = l.uuid \
                     AND ch.state IN ('available', 'deleting'))";

/// SQL dialect differences between PostgreSQL and SQLite for GC queries and lock ordering.
trait Dialect: Database {
    const BEGIN: &'static str;
    const UUID_CAST: &'static str;
    const IN_SET: &'static str;
    const LOCK_ROWS: bool;

    fn encode_ids(ids: &[String]) -> Result<String>;
}

impl Dialect for Postgres {
    const BEGIN: &'static str = "BEGIN ISOLATION LEVEL READ COMMITTED";
    const UUID_CAST: &'static str = "::uuid";
    const IN_SET: &'static str = "= ANY(string_to_array($1, ',')::uuid[])";
    const LOCK_ROWS: bool = true;

    fn encode_ids(ids: &[String]) -> Result<String> {
        Ok(ids.join(","))
    }
}

impl Dialect for Sqlite {
    const BEGIN: &'static str = "BEGIN IMMEDIATE";
    const UUID_CAST: &'static str = "";
    const IN_SET: &'static str = "IN (SELECT value FROM json_each($1))";
    const LOCK_ROWS: bool = false;

    fn encode_ids(ids: &[String]) -> Result<String> {
        Ok(serde_json::to_string(ids)?)
    }
}

impl MetaServer {
    /// Recover interrupted `deleting` layers, then reap eligible retired snapshots
    /// and delete newly unreferenced layers wave by wave from leaves to roots.
    ///
    /// PostgreSQL runs every query on one dedicated connection holding a session advisory lock;
    /// if another leader holds the lock, this returns an empty report without mutating state.
    pub async fn collect_garbage(&self, limit: u32) -> MetaResult<GcReport> {
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| MetaError::Unavailable {
                reason: "snapshot repository is not configured for collection".into(),
            })?;
        if limit == 0 {
            return Ok(GcReport::default());
        }
        let mut cursor = self.gc_cursor.lock().await;
        let result = match &self.database {
            DatabasePool::Postgres(pool) => {
                let mut conn = pool
                    .acquire()
                    .await
                    .map_err(|error| MetaError::backend("acquire GC database connection", error))?;
                // Never return a connection that might still hold a session advisory lock.
                conn.close_on_drop();
                let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                    .bind(ADVISORY_LOCK_KEY)
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(|error| MetaError::backend("acquire GC advisory lock", error))?;
                if !acquired {
                    let _ = conn.close().await;
                    return Ok(GcReport::default());
                }
                let result = Sweep::<Postgres>::run(
                    &mut conn,
                    storage.as_ref(),
                    &mut cursor,
                    limit as usize,
                )
                .await;
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(ADVISORY_LOCK_KEY)
                    .execute(&mut *conn)
                    .await;
                let _ = conn.close().await;
                result
            }
            DatabasePool::Sqlite(pool) => {
                let mut conn = pool
                    .acquire()
                    .await
                    .map_err(|error| MetaError::backend("acquire GC database connection", error))?;
                Sweep::<Sqlite>::run(&mut conn, storage.as_ref(), &mut cursor, limit as usize).await
            }
        };
        result.map_err(|error| MetaError::backend("collect snapshot storage", error))
    }
}

struct Sweep<'a, D: Dialect> {
    conn: &'a mut D::Connection,
    storage: &'a dyn GcStorage,
    report: GcReport,
    first_error: Option<anyhow::Error>,
    stalled_batches: usize,
}

impl<'a, D: Dialect> Sweep<'a, D>
where
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> String: Encode<'b, D> + Decode<'b, D> + Type<D>,
    for<'b> bool: Decode<'b, D> + Type<D>,
    for<'b> i64: Encode<'b, D> + Type<D>,
    D::Arguments: IntoArguments<D>,
    usize: ColumnIndex<D::Row>,
{
    async fn run(
        conn: &'a mut D::Connection,
        storage: &'a dyn GcStorage,
        cursor: &mut GcCursor,
        snapshot_limit: usize,
    ) -> Result<GcReport> {
        let mut sweep = Self {
            conn,
            storage,
            report: GcReport::default(),
            first_error: None,
            stalled_batches: 0,
        };
        sweep
            .recover_deleting_layers(&mut cursor.after_layer)
            .await?;
        if sweep.stalled_batches < MAX_STALLED_BATCHES {
            sweep
                .reap_retired_snapshots(&mut cursor.after_snapshot, snapshot_limit)
                .await?;
        }
        if let Some(error) = sweep.first_error {
            return Err(error);
        }
        Ok(sweep.report)
    }

    fn paged_query(
        base: &'static str,
        order_col: &'static str,
        after: Option<&str>,
        limit: usize,
    ) -> QueryBuilder<D> {
        let mut q = QueryBuilder::<D>::new(base);
        if let Some(id) = after {
            q.push(" AND ")
                .push(order_col)
                .push(" > ")
                .push_bind(id.to_owned())
                .push(D::UUID_CAST);
        }
        q.push(" ORDER BY ")
            .push(order_col)
            .push(" ASC LIMIT ")
            .push_bind(limit as i64);
        q
    }

    async fn recover_deleting_layers(&mut self, cursor: &mut Option<String>) -> Result<()> {
        let mut after = cursor.clone();
        let mut reached_end = false;
        for _ in 0..MAX_SWEEP_BATCHES {
            let pending: Vec<(String, String)> = Self::paged_query(
                "SELECT CAST(layers.uuid AS TEXT), digest FROM layers WHERE state = 'deleting'",
                "layers.uuid",
                after.as_deref(),
                LAYER_BATCH_SIZE,
            )
            .build_query_as()
            .fetch_all(&mut *self.conn)
            .await?;
            if pending.is_empty() {
                reached_end = true;
                break;
            }
            after = pending.last().map(|(uuid, _)| uuid.clone());
            let full_page = pending.len() == LAYER_BATCH_SIZE;
            let progressed = self.delete_layer_waves(pending).await?;
            self.stalled_batches = if progressed {
                0
            } else {
                self.stalled_batches + 1
            };
            if self.stalled_batches >= MAX_STALLED_BATCHES {
                break;
            }
            if !full_page {
                reached_end = true;
                break;
            }
        }
        *cursor = if reached_end { None } else { after };
        Ok(())
    }

    async fn reap_retired_snapshots(
        &mut self,
        cursor: &mut Option<String>,
        mut remaining: usize,
    ) -> Result<()> {
        let mut after = cursor.clone();
        let mut reached_end = false;
        for _ in 0..MAX_SWEEP_BATCHES {
            if remaining == 0 {
                break;
            }
            let page_limit = remaining.min(SNAPSHOT_BATCH_SIZE);
            let ids: Vec<String> = Self::paged_query(
                "SELECT CAST(s.snapshot_id AS TEXT) FROM snapshots s \
                 WHERE s.retired = TRUE AND s.snapshot_type = 'distributed' \
                   AND NOT EXISTS (SELECT 1 FROM snapshot_pins p \
                                   WHERE p.snapshot_id = s.snapshot_id AND NOT p.released)",
                "s.snapshot_id",
                after.as_deref(),
                page_limit,
            )
            .build_query_scalar()
            .fetch_all(&mut *self.conn)
            .await?;
            if ids.is_empty() {
                reached_end = true;
                break;
            }
            after = ids.last().cloned();
            let full_page = ids.len() == page_limit;
            remaining = remaining.saturating_sub(ids.len());
            let deleted = self.delete_snapshots(&ids).await;
            let progressed = !deleted.is_empty() && {
                let claimed = self.finalize_snapshots(&deleted).await?;
                claimed.is_empty() || self.delete_layer_waves(claimed).await?
            };
            self.stalled_batches = if progressed {
                0
            } else {
                self.stalled_batches + 1
            };
            if self.stalled_batches >= MAX_STALLED_BATCHES {
                break;
            }
            if !full_page {
                reached_end = true;
                break;
            }
        }
        *cursor = if reached_end { None } else { after };
        Ok(())
    }

    async fn delete_snapshots(&mut self, ids: &[String]) -> Vec<String> {
        let mut deleted = Vec::with_capacity(ids.len());
        for raw in ids {
            let result = match SnapshotId::parse(raw) {
                Ok(id) => self.storage.delete_snapshot(&id).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => deleted.push(raw.clone()),
                Err(error) => {
                    self.first_error.get_or_insert(error);
                }
            }
        }
        deleted
    }

    async fn finalize_snapshots(&mut self, ids: &[String]) -> Result<Vec<(String, String)>> {
        let in_set = D::IN_SET;
        let mut tx = self
            .conn
            .begin_with(AssertSqlSafe(D::BEGIN.to_owned()))
            .await?;

        // Retirement rejects new pins; released pins cannot become active again.
        let finalized: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "UPDATE snapshots SET snapshot_type = 'none'
             WHERE snapshot_id {in_set} AND retired = TRUE
               AND snapshot_type = 'distributed'
               AND NOT EXISTS (SELECT 1 FROM snapshot_pins p
                               WHERE p.snapshot_id = snapshots.snapshot_id AND NOT p.released)
             RETURNING CAST(snapshot_id AS TEXT)"
        )))
        .bind(D::encode_ids(ids)?)
        .fetch_all(&mut *tx)
        .await?;
        if finalized.is_empty() {
            return Ok(Vec::new());
        }
        self.report.reaped_snapshots += finalized.len();

        let freed_heads: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "DELETE FROM snapshot_heads WHERE snapshot_id {in_set} RETURNING CAST(layer_uuid AS TEXT)"
        )))
        .bind(D::encode_ids(&finalized)?)
        .fetch_all(&mut *tx)
        .await?;
        if freed_heads.is_empty() {
            tx.commit().await?;
            return Ok(Vec::new());
        }

        let claim_ids = if D::LOCK_ROWS {
            // Lock-free pre-filter skips hot shared layers before taking FOR UPDATE.
            let candidates: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT CAST(l.uuid AS TEXT) FROM layers l
                 WHERE l.uuid {in_set} AND {UNREFERENCED_AVAILABLE_LEAF}"
            )))
            .bind(D::encode_ids(&freed_heads)?)
            .fetch_all(&mut *tx)
            .await?;
            if candidates.is_empty() {
                tx.commit().await?;
                return Ok(Vec::new());
            }
            let encoded = D::encode_ids(&candidates)?;
            Self::lock_layers(&mut *tx, &encoded).await?;
            encoded
        } else {
            D::encode_ids(&freed_heads)?
        };
        let claimed = Self::claim_locked_leaves(&mut *tx, &claim_ids).await?;
        tx.commit().await?;
        Ok(claimed)
    }

    async fn delete_layer_waves(&mut self, mut claimed: Vec<(String, String)>) -> Result<bool> {
        let in_set = D::IN_SET;
        let mut progressed = false;
        while !claimed.is_empty() {
            let mut by_digest: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for (uuid, digest) in &claimed {
                by_digest
                    .entry(digest.clone())
                    .or_default()
                    .push(uuid.clone());
            }

            let mut deleted = Vec::with_capacity(claimed.len());
            let mut batch_digests = Vec::with_capacity(by_digest.len());
            let mut batch_uuids = Vec::with_capacity(by_digest.len());
            for (digest, uuids) in by_digest {
                batch_digests.push(digest);
                batch_uuids.push(uuids);
            }
            if !batch_digests.is_empty() {
                let outcomes = self.storage.delete_layers(&batch_digests).await;
                for (outcome, uuids) in outcomes.into_iter().zip(batch_uuids) {
                    match outcome {
                        Ok(()) => {
                            self.report.deleted_layers += 1;
                            deleted.extend(uuids);
                        }
                        Err(error) => {
                            self.first_error.get_or_insert(error);
                        }
                    }
                }
            }
            if deleted.is_empty() {
                break;
            }
            progressed = true;

            let ids = D::encode_ids(&deleted)?;
            let mut tx = self
                .conn
                .begin_with(AssertSqlSafe(D::BEGIN.to_owned()))
                .await?;
            // Lock-free pre-filter skips hot shared parents before taking FOR UPDATE.
            let cand_parents: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT CAST(l.uuid AS TEXT) FROM layers l
                 WHERE l.uuid IN (
                     SELECT managed_parent_uuid FROM layers WHERE uuid {in_set} AND state = 'deleting'
                 )
                   AND l.state = 'available'
                   AND NOT EXISTS (SELECT 1 FROM snapshot_heads h WHERE h.layer_uuid = l.uuid)
                   AND NOT EXISTS (
                       SELECT 1 FROM layers ch
                       WHERE ch.managed_parent_uuid = l.uuid
                         AND ch.state IN ('available', 'deleting')
                         AND NOT (ch.uuid {in_set})
                   )"
            )))
            .bind(ids.as_str())
            .fetch_all(&mut *tx)
            .await?;

            if D::LOCK_ROWS {
                // Lock deleted children and unreferenced parent candidates together in
                // ascending UUID order so concurrent publishers cannot deadlock.
                let lock_set: Vec<String> = deleted.iter().chain(&cand_parents).cloned().collect();
                Self::lock_layers(&mut *tx, &D::encode_ids(&lock_set)?).await?;
            }
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE layers SET state = 'deleted' WHERE uuid {in_set} AND state = 'deleting'"
            )))
            .bind(ids.as_str())
            .execute(&mut *tx)
            .await?;

            claimed = if cand_parents.is_empty() {
                Vec::new()
            } else {
                Self::claim_locked_leaves(&mut *tx, &D::encode_ids(&cand_parents)?).await?
            };
            tx.commit().await?;
        }
        Ok(progressed)
    }

    // Match `snapshot_directory::attach_layers`'s `ORDER BY l.uuid FOR SHARE` lock order.
    async fn lock_layers(tx: &mut D::Connection, ids: &str) -> Result<()> {
        let in_set = D::IN_SET;
        sqlx::query(AssertSqlSafe(format!(
            "SELECT uuid FROM layers WHERE uuid {in_set} ORDER BY uuid ASC FOR UPDATE"
        )))
        .bind(ids)
        .execute(tx)
        .await?;
        Ok(())
    }

    // Callers hold the candidate layer locks; PG must read a fresh snapshot after waiting.
    async fn claim_locked_leaves(
        tx: &mut D::Connection,
        ids: &str,
    ) -> Result<Vec<(String, String)>> {
        let in_set = D::IN_SET;
        Ok(sqlx::query_as(AssertSqlSafe(format!(
            "UPDATE layers AS l SET state = 'deleting'
             WHERE l.uuid {in_set} AND {UNREFERENCED_AVAILABLE_LEAF}
             RETURNING CAST(uuid AS TEXT), digest"
        )))
        .bind(ids)
        .fetch_all(tx)
        .await?)
    }
}
