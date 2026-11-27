use std::collections::{BTreeMap, BTreeSet};

use agentenv_domain::SnapshotId;
use anyhow::Result;
use uuid::Uuid;

use super::{GcCursor, GcReport, GcStorage};
use crate::db::{DbConn, DbRow, DbTx, Sql};
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

const UNREFERENCED_BY_HEADS: &str =
    "NOT EXISTS (SELECT 1 FROM layer_heads h WHERE h.layer_uuid = l.uuid AND NOT h.released)";

const NO_LIVE_CHILDREN: &str =
    "NOT EXISTS (SELECT 1 FROM layers ch WHERE ch.managed_parent_uuid = l.uuid \
                 AND ch.state IN ('available', 'deleting'))";

const UNPINNED_SNAPSHOT: &str =
    "NOT EXISTS (SELECT 1 FROM snapshot_pins p WHERE p.snapshot_id = snapshots.snapshot_id \
                 AND NOT p.released)";

fn collect_uuids(rows: Vec<DbRow>) -> Result<Vec<Uuid>> {
    rows.into_iter().map(|row| Ok(row.uuid(0)?)).collect()
}

/// Shared bounded keyset-pagination and stall-backoff loop across all GC phases.
macro_rules! sweep_pages {
    (
        $self:ident,
        cursor: $cursor:expr,
        batch_size: $batch_size:expr,
        remaining: $remaining:expr,
        fetch: |$after:ident, $page_limit:ident| $fetch:expr,
        cursor_key: $cursor_key:expr,
        process: |$rows:ident| $process:expr $(,)?
    ) => {{
        let mut remaining: usize = $remaining;
        let mut $after = $cursor.clone();
        let mut reached_end = false;
        for _ in 0..MAX_SWEEP_BATCHES {
            if remaining == 0 {
                break;
            }
            let $page_limit = remaining.min($batch_size);
            let $rows = $fetch;
            if $rows.is_empty() {
                reached_end = true;
                break;
            }
            $after = $rows.last().map($cursor_key);
            let full_page = $rows.len() == $page_limit;
            remaining = remaining.saturating_sub($rows.len());
            let progressed: bool = $process;
            $self.stalled_batches = if progressed {
                0
            } else {
                $self.stalled_batches + 1
            };
            if $self.stalled_batches >= MAX_STALLED_BATCHES {
                break;
            }
            if !full_page {
                reached_end = true;
                break;
            }
        }
        *$cursor = if reached_end { None } else { $after };
    }};
}

impl MetaServer {
    /// Recover interrupted `deleting` layers, reap eligible retired snapshots and
    /// released resource heads, and delete newly unreferenced layers wave by wave
    /// from leaves to roots.
    ///
    /// PostgreSQL runs every query on one dedicated connection holding a session advisory lock;
    /// if another leader holds the lock, this returns an empty report without mutating state.
    pub async fn collect_garbage(&self, limit: u32) -> MetaResult<GcReport> {
        let storage = self.storage.as_ref().ok_or_else(|| {
            MetaError::unavailable("snapshot repository is not configured for collection")
        })?;
        if limit == 0 {
            return Ok(GcReport::default());
        }
        let mut cursor = self.gc_cursor.lock().await;
        let mut conn = self.database.acquire_for_gc().await?;
        if !conn.try_advisory_lock(ADVISORY_LOCK_KEY).await? {
            conn.unlock_and_close(ADVISORY_LOCK_KEY).await;
            return Ok(GcReport::default());
        }
        let result = Sweep::run(&mut conn, storage.as_ref(), &mut cursor, limit as usize).await;
        conn.unlock_and_close(ADVISORY_LOCK_KEY).await;
        result.map_err(|error| MetaError::backend("collect storage", error))
    }
}

struct Sweep<'a> {
    conn: &'a mut DbConn,
    storage: &'a dyn GcStorage,
    report: GcReport,
    first_error: Option<anyhow::Error>,
    stalled_batches: usize,
}

impl<'a> Sweep<'a> {
    async fn run(
        conn: &'a mut DbConn,
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

    fn paged_uuid_query(base: &str, order_col: &str, after: Option<Uuid>, limit: usize) -> Sql {
        let mut q = Sql::new(base);
        if let Some(id) = after {
            q.push(" AND ").push(order_col).push(" > ").push_bind(id);
        }
        q.push(" ORDER BY ")
            .push(order_col)
            .push(" ASC LIMIT ")
            .push_bind(limit as i64);
        q
    }

    async fn recover_deleting_layers(&mut self, cursor: &mut Option<Uuid>) -> Result<()> {
        sweep_pages!(
            self,
            cursor: cursor,
            batch_size: LAYER_BATCH_SIZE,
            remaining: usize::MAX,
            fetch: |after, page_limit| {
                let q = Self::paged_uuid_query(
                    "SELECT layers.uuid, digest FROM layers WHERE state = 'deleting'",
                    "layers.uuid",
                    after,
                    page_limit,
                );
                self.conn
                    .fetch_all(q)
                    .await?
                    .into_iter()
                    .map(|row| Ok((row.uuid(0)?, row.get::<String>(1)?)))
                    .collect::<Result<Vec<_>>>()?
            },
            cursor_key: |&(uuid, _)| uuid,
            process: |pending| self.delete_layer_waves(pending).await?,
        );
        Ok(())
    }

    async fn reap_retired_snapshots(
        &mut self,
        cursor: &mut Option<Uuid>,
        limit: usize,
    ) -> Result<()> {
        sweep_pages!(
            self,
            cursor: cursor,
            batch_size: SNAPSHOT_BATCH_SIZE,
            remaining: limit,
            fetch: |after, page_limit| {
                let q = Self::paged_uuid_query(
                    &format!(
                        "SELECT snapshots.snapshot_id FROM snapshots \
                         WHERE snapshots.state = 'retired' AND {UNPINNED_SNAPSHOT}"
                    ),
                    "snapshots.snapshot_id",
                    after,
                    page_limit,
                );
                self.conn
                    .fetch_all(q)
                    .await?
                    .into_iter()
                    .map(|row| Ok(row.snapshot_id(0)?))
                    .collect::<Result<Vec<_>>>()?
            },
            cursor_key: |id| *id.as_uuid(),
            process: |rows| {
                let deleted = self.delete_snapshots(&rows).await;
                !deleted.is_empty() && {
                    let claimed = self.finalize_snapshots(&deleted).await?;
                    claimed.is_empty() || self.delete_layer_waves(claimed).await?
                }
            },
        );
        Ok(())
    }

    async fn delete_snapshots(&mut self, ids: &[SnapshotId]) -> Vec<SnapshotId> {
        let mut deleted = Vec::with_capacity(ids.len());
        for id in ids {
            match self.storage.delete_snapshot(id).await {
                Ok(()) => deleted.push(id.clone()),
                Err(error) => {
                    self.first_error.get_or_insert(error);
                }
            }
        }
        deleted
    }

    async fn finalize_snapshots(&mut self, ids: &[SnapshotId]) -> Result<Vec<(Uuid, String)>> {
        let mut tx = self.conn.begin().await?;
        // Retirement rejects new pins; released pins cannot become active again.
        let mut update = Sql::new(
            "UPDATE snapshots SET state = 'reaped', placement = NULL, owner_node_id = NULL \
             WHERE snapshot_id IN (",
        );
        update.push_in(ids).push(&format!(
            ") AND state = 'retired' AND {UNPINNED_SNAPSHOT} RETURNING snapshot_id"
        ));
        let finalized: Vec<String> = tx
            .fetch_all(update)
            .await?
            .into_iter()
            .map(|row| Ok(row.snapshot_id(0)?.to_string()))
            .collect::<Result<_>>()?;
        if finalized.is_empty() {
            return Ok(Vec::new());
        }
        self.report.reaped_snapshots += finalized.len();

        let mut delete_heads =
            Sql::new("DELETE FROM layer_heads WHERE owner_kind = 'snapshot' AND owner_id IN (");
        delete_heads
            .push_in(&finalized)
            .push(") RETURNING layer_uuid");
        let freed_heads = collect_uuids(tx.fetch_all(delete_heads).await?)?;
        let claimed = Self::claim_freed_heads(&mut tx, &freed_heads).await?;
        tx.commit("commit finalized snapshots").await?;
        Ok(claimed)
    }

    async fn reap_released_heads(
        &mut self,
        cursor: &mut Option<(String, String, Uuid)>,
        limit: usize,
    ) -> Result<()> {
        sweep_pages!(
            self,
            cursor: cursor,
            batch_size: SNAPSHOT_BATCH_SIZE,
            remaining: limit,
            fetch: |after, page_limit| {
                let mut q = Sql::new(
                    "SELECT h.owner_kind, h.owner_id, h.layer_uuid FROM layer_heads h \
                     WHERE h.released = TRUE",
                );
                if let Some((kind, owner_id, layer_uuid)) = &after {
                    q.push(" AND (h.owner_kind, h.owner_id, h.layer_uuid) > (")
                        .push_bind(kind.as_str())
                        .push(", ")
                        .push_bind(owner_id.as_str())
                        .push(", ")
                        .push_bind(*layer_uuid)
                        .push(")");
                }
                q.push(" ORDER BY h.owner_kind ASC, h.owner_id ASC, h.layer_uuid ASC LIMIT ")
                    .push_bind(page_limit as i64);
                self.conn
                    .fetch_all(q)
                    .await?
                    .into_iter()
                    .map(|row| Ok((row.get::<String>(0)?, row.get::<String>(1)?, row.uuid(2)?)))
                    .collect::<Result<Vec<_>>>()?
            },
            cursor_key: |row| row.clone(),
            process: |rows| {
                let (reaped, claimed) = self.finalize_released_heads(&rows).await?;
                reaped > 0 && (claimed.is_empty() || self.delete_layer_waves(claimed).await?)
            },
        );
        Ok(())
    }

    async fn finalize_released_heads(
        &mut self,
        rows: &[(String, String, Uuid)],
    ) -> Result<(usize, Vec<(Uuid, String)>)> {
        let mut tx = self.conn.begin().await?;
        let mut freed_heads = Vec::new();
        for chunk in rows.chunks(RELEASED_HEAD_DELETE_CHUNK) {
            let mut q = Sql::new("DELETE FROM layer_heads WHERE released = TRUE AND (");
            q.push_sep(" OR ", chunk, |q, (kind, owner_id, layer_uuid)| {
                q.push("(owner_kind = ")
                    .push_bind(kind.as_str())
                    .push(" AND owner_id = ")
                    .push_bind(owner_id.as_str())
                    .push(" AND layer_uuid = ")
                    .push_bind(*layer_uuid)
                    .push(")");
            })
            .push(") RETURNING layer_uuid");
            freed_heads.extend(collect_uuids(tx.fetch_all(q).await?)?);
        }
        if freed_heads.is_empty() {
            return Ok((0, Vec::new()));
        }
        let reaped = freed_heads.len();
        self.report.reaped_heads += reaped;
        let claimed = Self::claim_freed_heads(&mut tx, &freed_heads).await?;
        tx.commit("commit finalized heads").await?;
        Ok((reaped, claimed))
    }

    async fn claim_freed_heads(
        tx: &mut DbTx<'_>,
        freed_heads: &[Uuid],
    ) -> Result<Vec<(Uuid, String)>> {
        if freed_heads.is_empty() {
            return Ok(Vec::new());
        }
        let candidates: Vec<Uuid>;
        let claim_ids: &[Uuid] = if tx.is_postgres() {
            // Lock-free pre-filter skips hot shared layers before taking FOR UPDATE.
            let mut q = Sql::new("SELECT l.uuid FROM layers l WHERE l.uuid IN (");
            q.push_in(freed_heads).push(&format!(
                ") AND l.state = 'available' AND {UNREFERENCED_BY_HEADS} AND {NO_LIVE_CHILDREN}"
            ));
            candidates = collect_uuids(tx.fetch_all(q).await?)?;
            if candidates.is_empty() {
                return Ok(Vec::new());
            }
            Self::lock_layers(tx, &candidates).await?;
            &candidates
        } else {
            freed_heads
        };
        Self::claim_locked_leaves(tx, claim_ids).await
    }

    async fn delete_layer_waves(&mut self, mut claimed: Vec<(Uuid, String)>) -> Result<bool> {
        let mut progressed = false;
        while !claimed.is_empty() {
            let mut by_digest: BTreeMap<String, Vec<Uuid>> = BTreeMap::new();
            for (uuid, digest) in &claimed {
                by_digest.entry(digest.clone()).or_default().push(*uuid);
            }

            let all_digests: Vec<String> = by_digest.keys().cloned().collect();
            let mut q = Sql::new("SELECT DISTINCT digest FROM layers WHERE digest IN (");
            q.push_in(&all_digests).push(") AND state = 'available'");
            let live_shared: BTreeSet<String> = self
                .conn
                .fetch_all(q)
                .await?
                .into_iter()
                .map(|row| Ok(row.get::<String>(0)?))
                .collect::<Result<_>>()?;

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
                let mut outcomes = self.storage.delete_layers(&batch_digests).await.into_iter();
                for (digest, uuids) in batch_digests.iter().zip(batch_uuids) {
                    match outcomes.next() {
                        Some(Ok(())) => {
                            self.report.deleted_layers += 1;
                            deleted.extend(uuids);
                        }
                        Some(Err(error)) => {
                            self.first_error.get_or_insert(error);
                        }
                        None => {
                            self.first_error.get_or_insert_with(|| {
                                anyhow::anyhow!(
                                    "GC storage returned fewer outcomes than requested layers (missing '{digest}')"
                                )
                            });
                        }
                    }
                }
            }
            if deleted.is_empty() {
                break;
            }
            progressed = true;

            let mut tx = self.conn.begin().await?;
            // Lock-free pre-filter skips hot shared parents before taking FOR UPDATE.
            let mut parent_q = Sql::new(
                "SELECT l.uuid FROM layers l \
                 WHERE l.uuid IN ( \
                     SELECT managed_parent_uuid FROM layers WHERE uuid IN (",
            );
            parent_q.push_in(&deleted).push(&format!(
                ") AND state = 'deleting' \
                 ) AND l.state = 'available' AND {UNREFERENCED_BY_HEADS} \
                 AND NOT EXISTS ( \
                     SELECT 1 FROM layers ch \
                     WHERE ch.managed_parent_uuid = l.uuid \
                       AND ch.state IN ('available', 'deleting') \
                       AND ch.uuid NOT IN ("
            ));
            parent_q.push_in(&deleted).push("))");
            let cand_parents = collect_uuids(tx.fetch_all(parent_q).await?)?;

            if tx.is_postgres() {
                // Lock deleted children and unreferenced parent candidates together in
                // ascending UUID order so concurrent publishers cannot deadlock.
                let lock_set: Vec<Uuid> = deleted.iter().chain(&cand_parents).copied().collect();
                Self::lock_layers(&mut tx, &lock_set).await?;
            }
            let mut mark_deleted = Sql::new("UPDATE layers SET state = 'deleted' WHERE uuid IN (");
            mark_deleted
                .push_in(&deleted)
                .push(") AND state = 'deleting'");
            tx.execute(mark_deleted).await?;

            claimed = if cand_parents.is_empty() {
                Vec::new()
            } else {
                Self::claim_locked_leaves(&mut tx, &cand_parents).await?
            };
            tx.commit("commit layer wave").await?;
        }
        Ok(progressed)
    }

    // Match `layers::attach_layers`'s `ORDER BY uuid FOR SHARE` lock order.
    async fn lock_layers(tx: &mut DbTx<'_>, ids: &[Uuid]) -> Result<()> {
        let mut q = Sql::new("SELECT uuid FROM layers WHERE uuid IN (");
        q.push_in(ids).push(") ORDER BY uuid ASC");
        tx.execute(q.for_update()).await?;
        Ok(())
    }

    // Callers hold the candidate layer locks; PG must read a fresh snapshot after waiting.
    async fn claim_locked_leaves(tx: &mut DbTx<'_>, ids: &[Uuid]) -> Result<Vec<(Uuid, String)>> {
        let mut q = Sql::new("UPDATE layers AS l SET state = 'deleting' WHERE l.uuid IN (");
        q.push_in(ids).push(&format!(
            ") AND l.state = 'available' AND {UNREFERENCED_BY_HEADS} AND {NO_LIVE_CHILDREN} \
             RETURNING uuid, digest"
        ));
        tx.fetch_all(q)
            .await?
            .into_iter()
            .map(|row| Ok((row.uuid(0)?, row.get::<String>(1)?)))
            .collect()
    }
}
