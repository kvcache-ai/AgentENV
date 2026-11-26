//! Snapshot directory storage covering sandbox, template, and internal builder snapshots.

use agentenv_domain::{
    ResourceKind, SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotManifest,
    SnapshotPlacement, SnapshotSourceKind, SnapshotState, SnapshotVisibility, SnapshotVolume,
};
use sqlx::{
    AssertSqlSafe, ColumnIndex, Decode, Encode, Executor, IntoArguments, Pool, QueryBuilder, Type,
};
use uuid::Uuid;

use crate::database::{begin_tx, commit_tx, dispatch_db, QueryRows, SqlDialect};
use crate::layer_graph::{
    attach_layers, collect_snapshot_relations, now_unix_ms, require_non_nil_uuid, upsert_pin_in_tx,
    LayerRelations,
};
use crate::{MetaError, MetaResult, MetaServer, SnapshotMetadata};

const SNAPSHOT_COLUMNS: &str = "CAST(snapshot_id AS TEXT) AS snapshot_id, source_kind, visibility, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at";

#[derive(sqlx::FromRow)]
struct SnapshotRow {
    snapshot_id: String,
    source_kind: String,
    visibility: String,
    alias: Option<String>,
    state: String,
    placement: Option<String>,
    owner_node_id: Option<String>,
    source_sandbox_id: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl SnapshotRow {
    fn into_entry(self) -> MetaResult<SnapshotMetadata> {
        Ok(SnapshotMetadata {
            id: SnapshotId::parse(&self.snapshot_id)
                .map_err(|error| MetaError::backend("decode snapshot ID", error))?,
            source_kind: SnapshotSourceKind::parse(&self.source_kind).map_err(|error| {
                MetaError::Unavailable {
                    reason: error.to_string(),
                }
            })?,
            visibility: SnapshotVisibility::parse(&self.visibility).map_err(|error| {
                MetaError::Unavailable {
                    reason: error.to_string(),
                }
            })?,
            alias: self
                .alias
                .as_deref()
                .map(SnapshotAlias::parse)
                .transpose()
                .map_err(|error| MetaError::backend("decode snapshot alias", error))?,
            state: SnapshotState::parse(&self.state).map_err(|error| MetaError::Unavailable {
                reason: error.to_string(),
            })?,
            placement: self
                .placement
                .as_deref()
                .map(SnapshotPlacement::parse)
                .transpose()
                .map_err(|error| MetaError::Unavailable {
                    reason: error.to_string(),
                })?,
            owner_node_id: self.owner_node_id,
            source_sandbox_id: self.source_sandbox_id,
            created_at_unix_ms: self.created_at,
            updated_at_unix_ms: self.updated_at,
        })
    }

    fn ensure_active(&self) -> MetaResult<()> {
        if self.state != SnapshotState::Active.as_str() {
            return Err(MetaError::Unavailable {
                reason: format!("snapshot '{}' is {}", self.snapshot_id, self.state),
            });
        }
        Ok(())
    }

    fn promoted(self, metadata: &SnapshotMetadata) -> MetaResult<SnapshotMetadata> {
        if self.alias.as_deref() != metadata.alias.as_ref().map(AsRef::as_ref) {
            return Err(MetaError::SnapshotConflict {
                id: metadata.id.clone(),
            });
        }
        self.into_entry()
    }
}

impl MetaServer {
    /// Register a snapshot and its managed layer relationships (including volume snapshots).
    pub async fn register_snapshot(
        &self,
        metadata: SnapshotMetadata,
        volume_snapshots: &[SnapshotVolume],
        manifest: SnapshotManifest,
        restored_layers: &[String],
    ) -> MetaResult<SnapshotMetadata> {
        metadata
            .validate_for_publication()
            .map_err(|reason| MetaError::InvalidRequest { reason })?;
        let placement = metadata
            .placement
            .expect("publication metadata was validated");
        let relations = collect_snapshot_relations(placement, volume_snapshots, &manifest)?;
        dispatch_db!(&self.database, |pool| {
            SnapshotStore(pool)
                .register(&metadata, &relations, restored_layers)
                .await
        })
    }

    /// Promote a Local directory entry after the caller has published immutable content.
    pub async fn promote_snapshot(
        &self,
        metadata: SnapshotMetadata,
        volume_snapshots: &[SnapshotVolume],
        manifest: SnapshotManifest,
        restored_layers: &[String],
    ) -> MetaResult<Option<SnapshotMetadata>> {
        metadata
            .validate_for_publication()
            .map_err(|reason| MetaError::InvalidRequest { reason })?;
        if metadata.placement != Some(SnapshotPlacement::Distributed) {
            return Err(MetaError::InvalidRequest {
                reason: "promotion requires Distributed placement".into(),
            });
        }
        let relations = collect_snapshot_relations(
            SnapshotPlacement::Distributed,
            volume_snapshots,
            &manifest,
        )?;
        dispatch_db!(&self.database, |pool| {
            SnapshotStore(pool)
                .promote(&metadata, &relations, restored_layers)
                .await
        })
    }

    /// Retain the identity and clear its alias even if retirement precedes registration.
    pub async fn retire_snapshot(&self, id: SnapshotId) -> MetaResult<()> {
        dispatch_db!(&self.database, |pool| SnapshotStore(pool).retire(&id).await)
    }

    /// Pin a visible snapshot for node-local usage protection. A previously
    /// released `pin_id` is never reopened.
    pub async fn pin_snapshot(
        &self,
        id: SnapshotId,
        pin_id: Uuid,
        owner_node_id: Option<String>,
    ) -> MetaResult<Option<SnapshotMetadata>> {
        let pin_id = require_non_nil_uuid(pin_id, "pin ID")?;
        dispatch_db!(&self.database, |pool| {
            SnapshotStore(pool)
                .pin(&id, &pin_id, owner_node_id.as_deref())
                .await
        })
    }

    pub async fn get_snapshot_by_id(
        &self,
        id: &SnapshotId,
    ) -> MetaResult<Option<SnapshotMetadata>> {
        dispatch_db!(&self.database, |pool| SnapshotStore(pool)
            .get_by_id(id)
            .await)
    }

    pub async fn get_snapshot(&self, reference: &str) -> MetaResult<Option<SnapshotMetadata>> {
        self.get_snapshot_with_visibility(reference, SnapshotVisibility::Public)
            .await
    }

    pub async fn get_snapshot_with_visibility(
        &self,
        reference: &str,
        visibility: SnapshotVisibility,
    ) -> MetaResult<Option<SnapshotMetadata>> {
        let mut tombstone = None;
        if let Ok(id) = SnapshotId::parse(reference) {
            if let Some(entry) = self.get_snapshot_by_id(&id).await? {
                if entry.visibility == visibility {
                    // Reaped identities still own their IDs; only unknown-ID tombstones allow alias lookup.
                    if entry.state != SnapshotState::Reserved {
                        return Ok(Some(entry));
                    }
                    tombstone = Some(entry);
                }
            }
        }
        let alias = SnapshotAlias::parse(reference).map_err(|error| MetaError::InvalidRequest {
            reason: error.to_string(),
        })?;
        let row = dispatch_db!(&self.database, |pool| {
            sqlx::query_as::<_, SnapshotRow>(AssertSqlSafe(format!(
                "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE visibility = $1 AND alias = $2"
            )))
            .bind(visibility.as_str())
            .bind(alias.as_ref())
            .fetch_optional(pool)
            .await
        })
        .map_err(|error| MetaError::backend("get snapshot by alias", error))?;
        if let Some(row) = row {
            return row.into_entry().map(Some);
        }
        Ok(tombstone)
    }

    pub async fn list_snapshots(
        &self,
        filter: SnapshotListFilter,
    ) -> MetaResult<Vec<SnapshotMetadata>> {
        if filter.snapshot_ids.as_ref().is_some_and(Vec::is_empty)
            || filter
                .snapshot_id_or_alias
                .as_ref()
                .is_some_and(|reference| reference.contains('\0'))
            || filter
                .alias_prefix
                .as_ref()
                .is_some_and(|prefix| prefix.contains('\0'))
            || filter
                .source_sandbox_id
                .as_ref()
                .is_some_and(|source| source.contains('\0'))
        {
            return Ok(Vec::new());
        }
        let ids = filter
            .snapshot_ids
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| MetaError::backend("encode snapshot ID filter", error))?;
        dispatch_db!(&self.database, |pool| SnapshotStore(pool)
            .list(&filter, ids)
            .await)
    }
}

struct SnapshotStore<'a, D: SqlDialect>(&'a Pool<D>);

impl<D> SnapshotStore<'_, D>
where
    D: SqlDialect<QueryResult: QueryRows, Arguments: IntoArguments<D>>,
    for<'p> &'p Pool<D>: Executor<'p, Database = D>,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> Option<&'b str>: Encode<'b, D> + Type<D>,
    for<'b> String: Encode<'b, D> + Decode<'b, D> + Type<D>,
    for<'b> Option<String>: Decode<'b, D> + Type<D>,
    for<'b> i64: Encode<'b, D> + Type<D>,
    for<'b> bool: Decode<'b, D> + Type<D>,
    for<'r> SnapshotRow: sqlx::FromRow<'r, D::Row>,
    usize: ColumnIndex<D::Row>,
{
    async fn get_by_id(&self, id: &SnapshotId) -> MetaResult<Option<SnapshotMetadata>> {
        let row = sqlx::query_as::<D, SnapshotRow>(AssertSqlSafe(format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = {}",
            D::uuid(1),
        )))
        .bind(id.to_string())
        .fetch_optional(self.0)
        .await
        .map_err(|error| MetaError::backend("get snapshot by ID", error))?;
        row.map(SnapshotRow::into_entry).transpose()
    }

    async fn list(
        &self,
        filter: &SnapshotListFilter,
        ids: Option<String>,
    ) -> MetaResult<Vec<SnapshotMetadata>> {
        let mut query = QueryBuilder::<D>::new(format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE state = 'active' AND visibility = "
        ));
        query.push_bind(filter.visibility.as_str());
        if let Some(ids) = ids {
            query
                .push(D::JSON_UUID_SET_PREFIX)
                .push_bind(ids)
                .push(D::JSON_UUID_SET_SUFFIX);
        }
        if let Some(prefix) = &filter.alias_prefix {
            query
                .push(" AND substr(alias, 1, length(")
                .push_bind(prefix.as_str())
                .push(")) = ")
                .push_bind(prefix.as_str());
        }
        if let Some(source_sandbox_id) = &filter.source_sandbox_id {
            query
                .push(" AND source_sandbox_id = ")
                .push_bind(source_sandbox_id.as_str());
        }
        if let Some(reference) = &filter.snapshot_id_or_alias {
            query
                .push(" AND (CAST(snapshot_id AS TEXT) = ")
                .push_bind(reference.as_str())
                .push(" OR alias = ")
                .push_bind(reference.as_str())
                .push(")");
        }
        if let Some(sources) = &filter.sources {
            if sources.is_empty() {
                query.push(" AND 1 = 0");
            } else {
                query.push(" AND source_kind IN (");
                let mut sep = query.separated(", ");
                for source in sources {
                    sep.push_bind(source.as_str());
                }
                query.push(")");
            }
        }
        let rows = query
            .push(" ORDER BY snapshots.created_at DESC, snapshots.snapshot_id ASC")
            .build_query_as::<SnapshotRow>()
            .fetch_all(self.0)
            .await
            .map_err(|error| MetaError::backend("list snapshot records", error))?;
        rows.into_iter().map(SnapshotRow::into_entry).collect()
    }

    async fn read_alias_conflict<'e, E>(
        executor: E,
        visibility: &str,
        alias: Option<&str>,
        new_id: &SnapshotId,
        context: &str,
    ) -> MetaError
    where
        E: Executor<'e, Database = D>,
    {
        match sqlx::query_as::<D, (String, String)>(
            "SELECT CAST(snapshot_id AS TEXT), alias FROM snapshots
             WHERE visibility = $1 AND alias = $2",
        )
        .bind(visibility)
        .bind(alias)
        .fetch_one(executor)
        .await
        {
            Ok((id, alias)) => match SnapshotId::parse(&id) {
                Ok(existing) => MetaError::AliasConflict {
                    alias,
                    existing,
                    new_id: new_id.clone(),
                },
                Err(error) => MetaError::backend("decode alias owner", error),
            },
            Err(error) => MetaError::backend(context, error),
        }
    }

    async fn register(
        &self,
        metadata: &SnapshotMetadata,
        relations: &LayerRelations,
        restored_layers: &[String],
    ) -> MetaResult<SnapshotMetadata> {
        let snapshot_id = metadata.id.to_string();
        let visibility = metadata.visibility.as_str();
        let placement = metadata
            .placement
            .expect("publication metadata was validated")
            .as_str();
        loop {
            let mut tx = begin_tx(self.0, "begin snapshot registration").await?;
            let inserted = sqlx::query::<D>(AssertSqlSafe(format!(
                "INSERT INTO snapshots (snapshot_id, source_kind, visibility, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at)
                 VALUES ({}, $2, $3, $4, 'active', $5, $6, $7, $8, $9)
                 ON CONFLICT DO NOTHING",
                D::uuid(1),
            )))
            .bind(snapshot_id.as_str())
            .bind(metadata.source_kind.as_str())
            .bind(visibility)
            .bind(metadata.alias.as_ref().map(AsRef::as_ref))
            .bind(placement)
            .bind(metadata.owner_node_id.as_deref())
            .bind(metadata.source_sandbox_id.as_deref())
            .bind(metadata.created_at_unix_ms)
            .bind(metadata.updated_at_unix_ms)
            .execute(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("register snapshot identity", error))?;
            if inserted.rows_affected() == 0 {
                let existing: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                    "SELECT state FROM snapshots WHERE snapshot_id = {}",
                    D::uuid(1),
                )))
                .bind(snapshot_id.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("read conflicting snapshot", error))?;
                if let Some(state) = existing {
                    return Err(if state != SnapshotState::Active.as_str() {
                        MetaError::Unavailable {
                            reason: format!("snapshot '{snapshot_id}' is {state}"),
                        }
                    } else {
                        MetaError::SnapshotConflict {
                            id: metadata.id.clone(),
                        }
                    });
                }
                return Err(Self::read_alias_conflict(
                    &mut *tx,
                    visibility,
                    metadata.alias.as_ref().map(AsRef::as_ref),
                    &metadata.id,
                    "read conflicting alias",
                )
                .await);
            }
            if !attach_layers(
                &mut tx,
                ResourceKind::Snapshot,
                &snapshot_id,
                relations,
                restored_layers,
            )
            .await?
            {
                tx.rollback().await.map_err(|error| {
                    MetaError::backend("restart publication layer checks", error)
                })?;
                continue;
            }
            if metadata.source_kind == SnapshotSourceKind::Template {
                let now = metadata.updated_at_unix_ms.max(now_unix_ms());
                sqlx::query::<D>(AssertSqlSafe(format!(
                    "UPDATE build_jobs
                     SET status = 'ready',
                         result_snapshot_id = {},
                         error_message = NULL,
                         error_step = NULL,
                         finished_at = COALESCE(finished_at, $2),
                         updated_at = $2
                     WHERE build_id = {} AND kind = 'template' AND status IN ('waiting', 'building')",
                    D::uuid(1),
                    D::uuid(1),
                )))
                .bind(snapshot_id.as_str())
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("finalize template build job", error))?;
            }
            commit_tx(tx, "commit snapshot registration").await?;
            return Ok(metadata.clone());
        }
    }

    async fn promote(
        &self,
        metadata: &SnapshotMetadata,
        relations: &LayerRelations,
        restored_layers: &[String],
    ) -> MetaResult<Option<SnapshotMetadata>> {
        let snapshot_id = metadata.id.to_string();
        let visibility = metadata.visibility.as_str();
        loop {
            let mut tx = begin_tx(self.0, "begin snapshot promotion").await?;
            let row = sqlx::query_as::<D, SnapshotRow>(AssertSqlSafe(format!(
                "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = {}{}",
                D::uuid(1),
                D::FOR_UPDATE,
            )))
            .bind(snapshot_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("lock snapshot for promotion", error))?;
            let Some(row) = row else { return Ok(None) };
            row.ensure_active()?;
            if row.placement.as_deref() == Some(SnapshotPlacement::Distributed.as_str()) {
                return row.promoted(metadata).map(Some);
            }

            // Claim the snapshot row and alias before locking or inserting layers so
            // registration and promotion always acquire locks in snapshot/alias -> layers order.
            let updated = sqlx::query::<D>(AssertSqlSafe(format!(
                "UPDATE snapshots
                 SET alias = $2, placement = 'distributed', owner_node_id = NULL, updated_at = $3
                 WHERE snapshot_id = {}",
                D::uuid(1),
            )))
            .bind(snapshot_id.as_str())
            .bind(metadata.alias.as_ref().map(AsRef::as_ref))
            .bind(metadata.updated_at_unix_ms)
            .execute(&mut *tx)
            .await;
            if let Err(error) = updated {
                tx.rollback().await.map_err(|rollback| {
                    MetaError::backend("abort conflicting promotion", rollback)
                })?;
                if error
                    .as_database_error()
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
                {
                    return Err(Self::read_alias_conflict(
                        self.0,
                        visibility,
                        metadata.alias.as_ref().map(AsRef::as_ref),
                        &metadata.id,
                        "read conflicting promotion alias",
                    )
                    .await);
                }
                return Err(MetaError::backend("promote snapshot", error));
            }
            if !attach_layers(
                &mut tx,
                ResourceKind::Snapshot,
                &snapshot_id,
                relations,
                restored_layers,
            )
            .await?
            {
                tx.rollback()
                    .await
                    .map_err(|error| MetaError::backend("restart promotion layer checks", error))?;
                continue;
            }
            let mut entry = row.into_entry()?;
            entry.alias = metadata.alias.clone();
            entry.placement = Some(SnapshotPlacement::Distributed);
            entry.owner_node_id = None;
            entry.updated_at_unix_ms = metadata.updated_at_unix_ms;
            commit_tx(tx, "commit snapshot promotion").await?;
            return Ok(Some(entry));
        }
    }

    async fn retire(&self, id: &SnapshotId) -> MetaResult<()> {
        // An unknown ID must compete on the same primary key as a late first registration.
        sqlx::query::<D>(AssertSqlSafe(format!(
            "INSERT INTO snapshots (snapshot_id, source_kind, visibility, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at)
             VALUES ({}, 'sandbox', 'public', NULL, 'reserved', NULL, NULL, NULL, $2, $2)
             ON CONFLICT (snapshot_id) DO UPDATE
             SET state = 'retired', alias = NULL, updated_at = EXCLUDED.updated_at
             WHERE snapshots.state = 'active'",
            D::uuid(1),
        )))
        .bind(id.to_string())
        .bind(now_unix_ms())
        .execute(self.0)
        .await
        .map_err(|error| MetaError::backend("retire snapshot", error))?;
        Ok(())
    }

    async fn pin(
        &self,
        id: &SnapshotId,
        pin_id: &str,
        owner_node_id: Option<&str>,
    ) -> MetaResult<Option<SnapshotMetadata>> {
        let snapshot_id = id.to_string();
        let mut tx = begin_tx(self.0, "begin snapshot pin").await?;
        let row = sqlx::query_as::<D, SnapshotRow>(AssertSqlSafe(format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = {}{}",
            D::uuid(1),
            D::FOR_SHARE,
        )))
        .bind(snapshot_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("lock snapshot for pin", error))?;
        let Some(row) = row else { return Ok(None) };
        if row.state == SnapshotState::Reserved.as_str()
            || row.state == SnapshotState::Reaped.as_str()
        {
            return Err(MetaError::Unavailable {
                reason: format!("snapshot '{}' is {}", row.snapshot_id, row.state),
            });
        }
        let target_active = row.state == SnapshotState::Active.as_str();
        let inactive_reason = format!("snapshot '{}' is retired", row.snapshot_id);
        upsert_pin_in_tx(
            &mut tx,
            ResourceKind::Snapshot,
            &snapshot_id,
            pin_id,
            owner_node_id,
            target_active,
            &inactive_reason,
        )
        .await?;
        let entry = row.into_entry()?;
        commit_tx(tx, "commit snapshot pin").await?;
        Ok(Some(entry))
    }
}
