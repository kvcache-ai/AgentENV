use std::collections::{BTreeMap, BTreeSet};

use agentenv_domain::SnapshotId;
use anyhow::Result;
use sqlx::{AssertSqlSafe, Connection, Postgres, QueryBuilder, Sqlite};

use super::{GcCursor, GcReport, GcStorage};
use crate::database::{impl_sql_backend, DatabasePool, SqlDialect};
use crate::{MetaError, MetaResult, MetaServer};

pub(super) const ADVISORY_LOCK_KEY: i64 = 0x6165_6e76_6763_0001;
/// Retired snapshots or released heads per checkpoint batch.
pub(super) const SNAPSHOT_BATCH_SIZE: usize = 256;
/// Interrupted `deleting` layers per page (aligned with S3/OSS 1,000-key batch ceiling).
pub(super) const LAYER_BATCH_SIZE: usize = 1000;
/// Consecutive zero-progress batches before pausing for caller backoff.
pub(super) const MAX_STALLED_BATCHES: usize = 2;
/// Max pages per `collect_garbage` call to bound work under continuous retirements.
const MAX_SWEEP_BATCHES: usize = 32;
/// Chunk size for deleting released `layer_heads` tuples in a single SQL statement.
const RELEASED_HEAD_DELETE_CHUNK: usize = 64;

const UNREFERENCED_AVAILABLE_LEAF: &str = "l.state = 'available' \
     AND NOT EXISTS (SELECT 1 FROM layer_heads h WHERE h.layer_uuid = l.uuid AND NOT h.released) \
     AND NOT EXISTS (SELECT 1 FROM layers ch WHERE ch.managed_parent_uuid = l.uuid \
                     AND ch.state IN ('available', 'deleting'))";

impl MetaServer {
    /// Recover interrupted `deleting` layers, reap eligible retired snapshots and
    /// released resource heads, and delete newly unreferenced layers wave by wave
    /// from leaves to roots.
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
        result.map_err(|error| MetaError::backend("collect storage", error))
    }
}

struct Sweep<'a, D: SqlDialect> {
    conn: &'a mut D::Connection,
    storage: &'a dyn GcStorage,
    report: GcReport,
    first_error: Option<anyhow::Error>,
    stalled_batches: usize,
}

impl_sql_backend! {
impl<'a, D> Sweep<'a, D> {
    async fn run(
        conn: &'a mut D::Connection,
        storage: &'a dyn GcStorage,
        cursor: &mut GcCursor,
        limit: usize,
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
                .reap_retired_snapshots(&mut cursor.after_snapshot, limit)
                .await?;
        }
        if sweep.stalled_batches < MAX_STALLED_BATCHES {
            sweep
                .reap_released_heads(&mut cursor.after_head, limit)
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
            let rows: Vec<(String, String)> = Self::paged_query(
                "SELECT CAST(s.snapshot_id AS TEXT), COALESCE(s.placement, '') FROM snapshots s \
                 WHERE s.state = 'retired' \
                   AND NOT EXISTS (SELECT 1 FROM gc_pins p \
                                   WHERE p.target_kind = 'snapshot' \
                                     AND p.target_id = CAST(s.snapshot_id AS TEXT) \
                                     AND NOT p.released)",
                "s.snapshot_id",
                after.as_deref(),
                page_limit,
            )
            .build_query_as()
            .fetch_all(&mut *self.conn)
            .await?;
            if rows.is_empty() {
                reached_end = true;
                break;
            }
            after = rows.last().map(|(id, _)| id.clone());
            let full_page = rows.len() == page_limit;
            remaining = remaining.saturating_sub(rows.len());
            let deleted = self.delete_snapshots(&rows).await;
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

    async fn delete_snapshots(&mut self, rows: &[(String, String)]) -> Vec<String> {
        let mut deleted = Vec::with_capacity(rows.len());
        for (raw, placement) in rows {
            if placement != "distributed" {
                // Local snapshots store VM state on their owner node, not in the shared
                // snapshot directory; only their managed volume layers live in `layer_heads`.
                deleted.push(raw.clone());
                continue;
            }
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
        let in_uuid = D::IN_UUID_SET;
        let in_text = D::IN_TEXT_SET;
        let mut tx = self
            .conn
            .begin_with(AssertSqlSafe(D::BEGIN.to_owned()))
            .await?;

        // Retirement rejects new pins; released pins cannot become active again.
        let finalized: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "UPDATE snapshots SET state = 'reaped', placement = NULL, owner_node_id = NULL
             WHERE snapshot_id {in_uuid} AND state = 'retired'
               AND NOT EXISTS (SELECT 1 FROM gc_pins p
                               WHERE p.target_kind = 'snapshot'
                                 AND p.target_id = CAST(snapshots.snapshot_id AS TEXT)
                                 AND NOT p.released)
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
            "DELETE FROM layer_heads
             WHERE owner_kind = 'snapshot' AND owner_id {in_text}
             RETURNING CAST(layer_uuid AS TEXT)"
        )))
        .bind(D::encode_ids(&finalized)?)
        .fetch_all(&mut *tx)
        .await?;
        let claimed = Self::claim_freed_heads(&mut *tx, &freed_heads).await?;
        tx.commit().await?;
        Ok(claimed)
    }

    async fn reap_released_heads(
        &mut self,
        cursor: &mut Option<(String, String, String)>,
        mut remaining: usize,
    ) -> Result<()> {
        let mut after = cursor.clone();
        let mut reached_end = false;
        for _ in 0..MAX_SWEEP_BATCHES {
            if remaining == 0 {
                break;
            }
            let page_limit = remaining.min(SNAPSHOT_BATCH_SIZE);
            let mut query = QueryBuilder::<D>::new(
                "SELECT h.owner_kind, h.owner_id, CAST(h.layer_uuid AS TEXT) FROM layer_heads h \
                 WHERE h.released = TRUE \
                   AND NOT EXISTS (SELECT 1 FROM gc_pins p \
                                   WHERE p.target_kind = h.owner_kind \
                                     AND p.target_id = h.owner_id \
                                     AND NOT p.released)",
            );
            if let Some((kind, owner_id, layer_uuid)) = &after {
                query
                    .push(" AND (h.owner_kind, h.owner_id, h.layer_uuid) > (")
                    .push_bind(kind.clone())
                    .push(", ")
                    .push_bind(owner_id.clone())
                    .push(", ")
                    .push_bind(layer_uuid.clone())
                    .push(D::UUID_CAST)
                    .push(")");
            }
            query
                .push(" ORDER BY h.owner_kind ASC, h.owner_id ASC, h.layer_uuid ASC LIMIT ")
                .push_bind(page_limit as i64);
            let rows: Vec<(String, String, String)> =
                query.build_query_as().fetch_all(&mut *self.conn).await?;
            if rows.is_empty() {
                reached_end = true;
                break;
            }
            after = rows.last().cloned();
            let full_page = rows.len() == page_limit;
            remaining = remaining.saturating_sub(rows.len());
            let (reaped, claimed) = self.finalize_released_heads(&rows).await?;
            let progressed =
                reaped > 0 && (claimed.is_empty() || self.delete_layer_waves(claimed).await?);
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

    async fn finalize_released_heads(
        &mut self,
        rows: &[(String, String, String)],
    ) -> Result<(usize, Vec<(String, String)>)> {
        let mut tx = self
            .conn
            .begin_with(AssertSqlSafe(D::BEGIN.to_owned()))
            .await?;
        let mut freed_heads = Vec::new();
        for chunk in rows.chunks(RELEASED_HEAD_DELETE_CHUNK) {
            let mut query = QueryBuilder::<D>::new(
                "DELETE FROM layer_heads
                 WHERE released = TRUE
                   AND NOT EXISTS (SELECT 1 FROM gc_pins p
                                   WHERE p.target_kind = layer_heads.owner_kind
                                     AND p.target_id = layer_heads.owner_id
                                     AND NOT p.released)
                   AND (",
            );
            let mut sep = query.separated(" OR ");
            for (kind, owner_id, layer_uuid) in chunk {
                sep.push("(owner_kind = ")
                    .push_bind_unseparated(kind.as_str())
                    .push_unseparated(" AND owner_id = ")
                    .push_bind_unseparated(owner_id.as_str())
                    .push_unseparated(" AND layer_uuid = ")
                    .push_bind_unseparated(layer_uuid.as_str())
                    .push_unseparated(D::UUID_CAST)
                    .push_unseparated(")");
            }
            query.push(") RETURNING CAST(layer_uuid AS TEXT)");
            let removed: Vec<String> = query.build_query_scalar().fetch_all(&mut *tx).await?;
            freed_heads.extend(removed);
        }
        if freed_heads.is_empty() {
            return Ok((0, Vec::new()));
        }
        let reaped = freed_heads.len();
        self.report.reaped_heads += reaped;
        let claimed = Self::claim_freed_heads(&mut *tx, &freed_heads).await?;
        tx.commit().await?;
        Ok((reaped, claimed))
    }

    async fn claim_freed_heads(
        tx: &mut D::Connection,
        freed_heads: &[String],
    ) -> Result<Vec<(String, String)>> {
        if freed_heads.is_empty() {
            return Ok(Vec::new());
        }
        let in_uuid = D::IN_UUID_SET;
        let claim_ids = if D::LOCK_ROWS {
            // Lock-free pre-filter skips hot shared layers before taking FOR UPDATE.
            let candidates: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT CAST(l.uuid AS TEXT) FROM layers l
                 WHERE l.uuid {in_uuid} AND {UNREFERENCED_AVAILABLE_LEAF}"
            )))
            .bind(D::encode_ids(freed_heads)?)
            .fetch_all(&mut *tx)
            .await?;
            if candidates.is_empty() {
                return Ok(Vec::new());
            }
            let encoded = D::encode_ids(&candidates)?;
            Self::lock_layers(&mut *tx, &encoded).await?;
            encoded
        } else {
            D::encode_ids(freed_heads)?
        };
        Self::claim_locked_leaves(&mut *tx, &claim_ids).await
    }

    async fn delete_layer_waves(&mut self, mut claimed: Vec<(String, String)>) -> Result<bool> {
        let in_uuid = D::IN_UUID_SET;
        let in_text = D::IN_TEXT_SET;
        let mut progressed = false;
        while !claimed.is_empty() {
            let mut by_digest: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for (uuid, digest) in &claimed {
                by_digest
                    .entry(digest.clone())
                    .or_default()
                    .push(uuid.clone());
            }

            let all_digests: Vec<String> = by_digest.keys().cloned().collect();
            let live_shared: BTreeSet<String> = if all_digests.is_empty() {
                BTreeSet::new()
            } else {
                sqlx::query_scalar::<D, String>(AssertSqlSafe(format!(
                    "SELECT DISTINCT digest FROM layers
                     WHERE digest {in_text} AND state = 'available'"
                )))
                .bind(D::encode_ids(&all_digests)?)
                .fetch_all(&mut *self.conn)
                .await?
                .into_iter()
                .collect()
            };

            let mut deleted = Vec::with_capacity(claimed.len());
            let mut batch_digests = Vec::with_capacity(by_digest.len());
            let mut batch_uuids = Vec::with_capacity(by_digest.len());
            for (digest, uuids) in by_digest {
                if live_shared.contains(&digest) {
                    // Another live UUID still points to the same content-addressed blob;
                    // transition these unreferenced UUID rows to `deleted` without unlinking.
                    deleted.extend(uuids);
                } else {
                    batch_digests.push(digest);
                    batch_uuids.push(uuids);
                }
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
                     SELECT managed_parent_uuid FROM layers WHERE uuid {in_uuid} AND state = 'deleting'
                 )
                   AND l.state = 'available'
                   AND NOT EXISTS (SELECT 1 FROM layer_heads h WHERE h.layer_uuid = l.uuid AND NOT h.released)
                   AND NOT EXISTS (
                       SELECT 1 FROM layers ch
                       WHERE ch.managed_parent_uuid = l.uuid
                         AND ch.state IN ('available', 'deleting')
                         AND NOT (ch.uuid {in_uuid})
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
                "UPDATE layers SET state = 'deleted' WHERE uuid {in_uuid} AND state = 'deleting'"
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

    // Match `layer_graph::attach_layers`'s `ORDER BY l.uuid FOR SHARE` lock order.
    async fn lock_layers(tx: &mut D::Connection, ids: &str) -> Result<()> {
        let in_uuid = D::IN_UUID_SET;
        sqlx::query(AssertSqlSafe(format!(
            "SELECT uuid FROM layers WHERE uuid {in_uuid} ORDER BY uuid ASC FOR UPDATE"
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
        let in_uuid = D::IN_UUID_SET;
        Ok(sqlx::query_as(AssertSqlSafe(format!(
            "UPDATE layers AS l SET state = 'deleting'
             WHERE l.uuid {in_uuid} AND {UNREFERENCED_AVAILABLE_LEAF}
             RETURNING CAST(uuid AS TEXT), digest"
        )))
        .bind(ids)
        .fetch_all(tx)
        .await?)
    }
}
}
