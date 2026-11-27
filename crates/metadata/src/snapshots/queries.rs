//! Snapshot directory storage covering sandbox, template, and internal builder snapshots.

use agentenv_domain::{
    ResourceKind, SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotPlacement, SnapshotRecord,
    SnapshotSourceKind,
};

use crate::db::{is_unique_violation, DbRow, MetaSqlExt, Sql};
use crate::layers::{attach_layers, collect_snapshot_relations, now_unix_ms};
use crate::{MetaError, MetaResult, MetaServer, SnapshotEntry, SnapshotState};

const COLS: &str = "snapshot_id, source_kind, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at";

fn decode_snapshot(row: DbRow) -> MetaResult<SnapshotEntry> {
    Ok(SnapshotEntry {
        id: row.snapshot_id(0)?,
        source_kind: SnapshotSourceKind::parse(&row.get::<String>(1)?)
            .map_err(|e| MetaError::unavailable(e.to_string()))?,
        alias: row
            .get::<Option<String>>(2)?
            .as_deref()
            .map(SnapshotAlias::parse)
            .transpose()
            .map_err(|e| MetaError::backend("decode snapshot alias", e))?,
        state: SnapshotState::parse(&row.get::<String>(3)?)
            .map_err(|e| MetaError::unavailable(e.to_string()))?,
        placement: row
            .get::<Option<String>>(4)?
            .as_deref()
            .map(SnapshotPlacement::parse)
            .transpose()
            .map_err(|e| MetaError::unavailable(e.to_string()))?,
        owner_node_id: row.get(5)?,
        source_sandbox_id: row.get(6)?,
        created_at_unix_ms: row.get(7)?,
        updated_at_unix_ms: row.get(8)?,
    })
}

pub(crate) fn tombstone_sql(id: &SnapshotId, source_kind: SnapshotSourceKind) -> Sql {
    // An unknown ID must compete on the same primary key as a late first registration.
    Sql::new(
        "INSERT INTO snapshots (snapshot_id, source_kind, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at)
         VALUES ($1, $2, NULL, 'reserved', NULL, NULL, NULL, $3, $3)
         ON CONFLICT (snapshot_id) DO UPDATE
         SET state = 'retired', alias = NULL, updated_at = EXCLUDED.updated_at
         WHERE snapshots.state = 'active'",
    )
    .bind(id)
    .bind(source_kind.as_str())
    .bind(now_unix_ms())
}

fn scope(internal: bool) -> &'static str {
    if internal {
        "source_kind = 'builder'"
    } else {
        "source_kind <> 'builder'"
    }
}

fn alias_lookup_sql(internal: bool, alias: Option<&SnapshotAlias>) -> Sql {
    Sql::new(format!(
        "SELECT snapshot_id, alias FROM snapshots WHERE {} AND alias = $1",
        scope(internal)
    ))
    .bind(alias.map(AsRef::as_ref))
}

fn alias_conflict(row: DbRow, new_id: SnapshotId) -> MetaResult<MetaError> {
    Ok(MetaError::AliasConflict {
        existing: row.snapshot_id(0)?,
        alias: row.get(1)?,
        new_id,
    })
}

impl MetaServer {
    /// Register a snapshot and its managed layer relationships (including volume snapshots).
    pub async fn register_snapshot(
        &self,
        record: SnapshotRecord,
        restored_layers: &[String],
    ) -> MetaResult<SnapshotEntry> {
        record.validate().map_err(MetaError::invalid)?;
        let metadata = SnapshotEntry::from_record(&record);
        let relations = collect_snapshot_relations(&record)?;
        let snapshot_id = metadata.id.to_string();
        let internal = metadata.source_kind == SnapshotSourceKind::Builder;
        let placement = metadata
            .placement
            .expect("publication metadata was validated")
            .as_str();
        loop {
            let mut tx = self.database.begin("begin snapshot registration").await?;
            let inserted = tx
                .execute(
                    Sql::new(
                        "INSERT INTO snapshots (snapshot_id, source_kind, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at)
                         VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, $8)
                         ON CONFLICT DO NOTHING",
                    )
                    .bind(&metadata.id)
                    .bind(metadata.source_kind.as_str())
                    .bind(metadata.alias.as_ref().map(AsRef::as_ref))
                    .bind(placement)
                    .bind(metadata.owner_node_id.as_deref())
                    .bind(metadata.source_sandbox_id.as_deref())
                    .bind(metadata.created_at_unix_ms)
                    .bind(metadata.updated_at_unix_ms),
                )
                .await
                .meta_err("register snapshot identity")?;
            if inserted == 0 {
                if let Some(row) = tx
                    .fetch_optional(
                        Sql::new("SELECT state FROM snapshots WHERE snapshot_id = $1")
                            .bind(&metadata.id),
                    )
                    .await
                    .meta_err("read conflicting snapshot")?
                {
                    let state: String = row.get(0)?;
                    return Err(if state != SnapshotState::Active.as_str() {
                        MetaError::unavailable(format!("snapshot '{snapshot_id}' is {state}"))
                    } else {
                        MetaError::SnapshotConflict { id: metadata.id }
                    });
                }
                let row = tx
                    .fetch_one(alias_lookup_sql(internal, metadata.alias.as_ref()))
                    .await
                    .meta_err("read conflicting alias")?;
                return Err(alias_conflict(row, metadata.id)?);
            }
            if !attach_layers(
                &mut tx,
                ResourceKind::Snapshot,
                &snapshot_id,
                &relations,
                restored_layers,
            )
            .await?
            {
                tx.rollback("restart publication layer checks").await?;
                continue;
            }
            if metadata.source_kind == SnapshotSourceKind::Template {
                tx.execute(
                    Sql::new(
                        "UPDATE build_jobs
                         SET status = 'ready', error_message = NULL, error_step = NULL,
                             finished_at = COALESCE(finished_at, $2), updated_at = $2
                         WHERE build_id = $1 AND status IN ('waiting', 'building')",
                    )
                    .bind(&metadata.id)
                    .bind(metadata.updated_at_unix_ms.max(now_unix_ms())),
                )
                .await
                .meta_err("finalize template build job")?;
            }
            tx.commit("commit snapshot registration").await?;
            return Ok(metadata);
        }
    }

    /// Promote a Local directory entry after the caller has published immutable content.
    pub async fn promote_snapshot(
        &self,
        record: SnapshotRecord,
        restored_layers: &[String],
    ) -> MetaResult<Option<SnapshotEntry>> {
        record.validate().map_err(MetaError::invalid)?;
        if record.snapshot_type != SnapshotPlacement::Distributed {
            return Err(MetaError::invalid(
                "promotion requires Distributed placement",
            ));
        }
        let metadata = SnapshotEntry::from_record(&record);
        let relations = collect_snapshot_relations(&record)?;
        let snapshot_id = metadata.id.to_string();
        let internal = metadata.source_kind == SnapshotSourceKind::Builder;
        loop {
            let mut tx = self.database.begin("begin snapshot promotion").await?;
            let Some(row) = tx
                .fetch_optional(
                    Sql::new(format!(
                        "SELECT {COLS} FROM snapshots WHERE snapshot_id = $1"
                    ))
                    .for_update()
                    .bind(&metadata.id),
                )
                .await
                .meta_err("lock snapshot for promotion")?
            else {
                return Ok(None);
            };
            let mut entry = decode_snapshot(row)?;
            if entry.state != SnapshotState::Active {
                return Err(MetaError::unavailable(format!(
                    "snapshot '{snapshot_id}' is {}",
                    entry.state
                )));
            }
            if entry.placement == Some(SnapshotPlacement::Distributed) {
                if entry.alias != metadata.alias {
                    return Err(MetaError::SnapshotConflict { id: metadata.id });
                }
                return Ok(Some(entry));
            }

            // Claim the snapshot row and alias before locking or inserting layers so
            // registration and promotion always acquire locks in snapshot/alias -> layers order.
            let updated = tx
                .execute(
                    Sql::new(
                        "UPDATE snapshots
                         SET alias = $2, placement = 'distributed', owner_node_id = NULL, updated_at = $3
                         WHERE snapshot_id = $1",
                    )
                    .bind(&metadata.id)
                    .bind(metadata.alias.as_ref().map(AsRef::as_ref))
                    .bind(metadata.updated_at_unix_ms),
                )
                .await;
            if let Err(error) = updated {
                tx.rollback("abort conflicting promotion").await?;
                if is_unique_violation(&error) {
                    let row = self
                        .database
                        .fetch_one(alias_lookup_sql(internal, metadata.alias.as_ref()))
                        .await
                        .meta_err("read conflicting promotion alias")?;
                    return Err(alias_conflict(row, metadata.id)?);
                }
                return Err(MetaError::backend("promote snapshot", error));
            }
            if !attach_layers(
                &mut tx,
                ResourceKind::Snapshot,
                &snapshot_id,
                &relations,
                restored_layers,
            )
            .await?
            {
                tx.rollback("restart promotion layer checks").await?;
                continue;
            }
            entry.alias = metadata.alias;
            entry.placement = Some(SnapshotPlacement::Distributed);
            entry.owner_node_id = None;
            entry.updated_at_unix_ms = metadata.updated_at_unix_ms;
            tx.commit("commit snapshot promotion").await?;
            return Ok(Some(entry));
        }
    }

    /// Retain the identity and clear its alias even if retirement precedes registration.
    pub async fn retire_snapshot(&self, id: SnapshotId) -> MetaResult<()> {
        self.database
            .execute(tombstone_sql(&id, SnapshotSourceKind::Sandbox))
            .await
            .meta_err("retire snapshot")?;
        Ok(())
    }

    pub async fn get_snapshot(
        &self,
        reference: &str,
        internal: bool,
    ) -> MetaResult<Option<SnapshotEntry>> {
        let mut tombstone = None;
        if let Ok(id) = SnapshotId::parse(reference) {
            if let Some(row) = self
                .database
                .fetch_optional(
                    Sql::new(format!(
                        "SELECT {COLS} FROM snapshots WHERE snapshot_id = $1"
                    ))
                    .bind(&id),
                )
                .await
                .meta_err("get snapshot by ID")?
            {
                let entry = decode_snapshot(row)?;
                if (entry.source_kind == SnapshotSourceKind::Builder) == internal {
                    // Reaped identities still own their IDs; only unknown-ID tombstones allow alias lookup.
                    if entry.state != SnapshotState::Reserved {
                        return Ok(Some(entry));
                    }
                    tombstone = Some(entry);
                }
            }
        }
        let alias =
            SnapshotAlias::parse(reference).map_err(|e| MetaError::invalid(e.to_string()))?;
        if let Some(row) = self
            .database
            .fetch_optional(
                Sql::new(format!(
                    "SELECT {COLS} FROM snapshots WHERE {} AND alias = $1",
                    scope(internal),
                ))
                .bind(alias.as_ref()),
            )
            .await
            .meta_err("get snapshot by alias")?
        {
            return decode_snapshot(row).map(Some);
        }
        Ok(tombstone)
    }

    pub async fn list_snapshots(
        &self,
        filter: SnapshotListFilter,
    ) -> MetaResult<Vec<SnapshotEntry>> {
        if filter.snapshot_ids.as_ref().is_some_and(Vec::is_empty)
            || filter
                .snapshot_id_or_alias
                .as_ref()
                .is_some_and(|r| r.contains('\0'))
            || filter
                .source_sandbox_id
                .as_ref()
                .is_some_and(|s| s.contains('\0'))
        {
            return Ok(Vec::new());
        }
        let mut q = Sql::new(format!(
            "SELECT {COLS} FROM snapshots WHERE state = 'active'"
        ));
        if let Some(ids) = &filter.snapshot_ids {
            q.push(" AND snapshot_id IN (").push_in(ids).push(")");
        }
        if let Some(alias) = &filter.alias {
            q.push(" AND alias = ").push_bind(alias.as_str());
        }
        if let Some(source) = &filter.source_sandbox_id {
            q.push(" AND source_sandbox_id = ")
                .push_bind(source.as_str());
        }
        if let Some(reference) = &filter.snapshot_id_or_alias {
            q.push(" AND (CAST(snapshot_id AS TEXT) = ")
                .push_bind(reference.as_str())
                .push(" OR alias = ")
                .push_bind(reference.as_str())
                .push(")");
        }
        match &filter.sources {
            Some(sources) if sources.is_empty() => {
                q.push(" AND 1 = 0");
            }
            Some(sources) => {
                q.push(" AND source_kind IN (")
                    .push_in(sources.iter().map(|s| s.as_str()))
                    .push(")");
            }
            None => {
                q.push(" AND source_kind <> 'builder'");
            }
        }
        if let Some(placement) = filter.placement {
            q.push(" AND placement = ").push_bind(placement.as_str());
        }
        if let Some(owner) = &filter.owner_node_id {
            q.push(" AND owner_node_id = ").push_bind(owner.as_str());
        }
        q.push(" ORDER BY created_at DESC, snapshot_id ASC");
        if let Some(limit) = filter.limit {
            q.push(" LIMIT ").push_bind(i64::from(limit));
        }
        self.database
            .fetch_all(q)
            .await
            .meta_err("list snapshot records")?
            .into_iter()
            .map(decode_snapshot)
            .collect()
    }
}
