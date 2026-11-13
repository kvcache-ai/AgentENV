use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use agentenv_domain::{
    CommittedAttachedDrive, LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotId,
    SnapshotListFilter, SnapshotManifest, SnapshotPlacement, SnapshotSourceKind,
};
use sqlx::{AssertSqlSafe, PgPool, Postgres, QueryBuilder, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::database::DatabasePool;
use crate::{DeletableLayer, LayerCheck, LayerState, MetaError, MetaResult, SnapshotMetadata};

const BATCH_SIZE: usize = 128;
const SNAPSHOT_COLUMNS: &str = "CAST(snapshot_id AS TEXT) AS snapshot_id, alias, snapshot_type, source_sandbox_id, owner_node_id, retired, created_at, updated_at";

#[derive(sqlx::FromRow)]
struct SnapshotRow {
    snapshot_id: String,
    alias: Option<String>,
    snapshot_type: String,
    source_sandbox_id: Option<String>,
    owner_node_id: Option<String>,
    retired: bool,
    created_at: i64,
    updated_at: i64,
}

impl SnapshotRow {
    fn into_entry(self) -> MetaResult<SnapshotMetadata> {
        Ok(SnapshotMetadata {
            id: SnapshotId::parse(&self.snapshot_id)
                .map_err(|error| MetaError::backend("decode snapshot ID", error))?,
            alias: self
                .alias
                .as_deref()
                .map(SnapshotAlias::parse)
                .transpose()
                .map_err(|error| MetaError::backend("decode snapshot alias", error))?,
            snapshot_type: match self.snapshot_type.as_str() {
                "local" => Some(SnapshotPlacement::Local),
                "distributed" => Some(SnapshotPlacement::Distributed),
                "none" => None,
                value => {
                    return Err(MetaError::Unavailable {
                        reason: format!("unknown snapshot placement '{value}'"),
                    })
                }
            },
            owner_node_id: self.owner_node_id,
            source_sandbox_id: self.source_sandbox_id,
            retired: self.retired,
            created_at_unix_ms: self.created_at,
            updated_at_unix_ms: self.updated_at,
        })
    }

    fn ensure_active(&self) -> MetaResult<()> {
        if self.retired {
            return Err(MetaError::Unavailable {
                reason: format!("snapshot '{}' is retired or reserved", self.snapshot_id),
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

struct ManagedLayerNode {
    parent: Option<String>,
    digest: String,
    depth: usize,
    collectible: bool,
}

#[derive(Default)]
struct LayerRelations {
    layers: BTreeMap<String, ManagedLayerNode>,
    heads: BTreeSet<String>,
}

fn collect_layer_relations(committed: &SnapshotManifest) -> MetaResult<LayerRelations> {
    fn managed(layers: &[LayerRef]) -> Vec<&ManagedLayerRef> {
        layers
            .iter()
            .filter_map(|layer| match layer {
                LayerRef::Managed(layer) => Some(layer),
                LayerRef::External(_) => None,
            })
            .collect()
    }
    let chains = [
        managed(&committed.rootfs_layers),
        committed.memory_layers.iter().collect(),
    ]
    .into_iter()
    .chain(committed.attached_drives.iter().map(|drive| {
        let CommittedAttachedDrive::Overlaybd { layers, .. } = drive;
        managed(layers)
    }));
    let mut layers: BTreeMap<String, ManagedLayerNode> = BTreeMap::new();
    let mut heads = BTreeSet::new();
    for chain in chains {
        let mut parent = None;
        for (depth, layer) in chain.into_iter().enumerate() {
            let uuid =
                parse_non_nil_uuid(layer.uuid.as_deref().unwrap_or_default(), "managed layer")?;
            if layer.digest.is_empty() {
                return Err(MetaError::InvalidRequest {
                    reason: "managed layer requires a canonical digest".into(),
                });
            }
            if let Some(existing) = layers.get(&uuid) {
                if existing.parent != parent || existing.digest != layer.digest {
                    return Err(MetaError::InvalidRequest {
                        reason: format!(
                            "managed layer '{uuid}' has conflicting publication inputs"
                        ),
                    });
                }
            } else {
                layers.insert(
                    uuid.clone(),
                    ManagedLayerNode {
                        parent: parent.clone(),
                        digest: layer.digest.clone(),
                        depth,
                        collectible: committed.isolated_layers,
                    },
                );
            }
            parent = Some(uuid);
        }
        if let Some(head) = parent {
            heads.insert(head);
        }
    }
    Ok(LayerRelations { layers, heads })
}

pub(crate) async fn register(
    database: &DatabasePool,
    metadata: &SnapshotMetadata,
    manifest: &SnapshotManifest,
    restored_layers: &[String],
) -> MetaResult<SnapshotMetadata> {
    let relations = match metadata.snapshot_type {
        Some(SnapshotPlacement::Local) => LayerRelations::default(),
        Some(SnapshotPlacement::Distributed) => collect_layer_relations(manifest)?,
        None => unreachable!("publication metadata was validated"),
    };
    match database {
        DatabasePool::Postgres(pool) => {
            postgres::register(pool, metadata, &relations, restored_layers).await
        }
        DatabasePool::Sqlite(pool) => {
            sqlite::register(pool, metadata, &relations, restored_layers).await
        }
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub(crate) async fn promote(
    database: &DatabasePool,
    metadata: &SnapshotMetadata,
    manifest: &SnapshotManifest,
    restored_layers: &[String],
) -> MetaResult<Option<SnapshotMetadata>> {
    let relations = collect_layer_relations(manifest)?;
    match database {
        DatabasePool::Postgres(pool) => {
            postgres::promote(pool, metadata, &relations, restored_layers).await
        }
        DatabasePool::Sqlite(pool) => {
            sqlite::promote(pool, metadata, &relations, restored_layers).await
        }
    }
}

pub(crate) async fn retire(database: &DatabasePool, id: &SnapshotId) -> MetaResult<()> {
    match database {
        DatabasePool::Postgres(pool) => postgres::retire(pool, id).await,
        DatabasePool::Sqlite(pool) => sqlite::retire(pool, id).await,
    }
}

fn parse_non_nil_uuid(value: &str, label: &str) -> MetaResult<String> {
    Uuid::parse_str(value)
        .ok()
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.to_string())
        .ok_or_else(|| MetaError::InvalidRequest {
            reason: format!("{label} requires a non-nil UUID"),
        })
}

pub(crate) async fn pin(
    database: &DatabasePool,
    id: &SnapshotId,
    pin_id: &str,
) -> MetaResult<Option<SnapshotMetadata>> {
    let pin_id = parse_non_nil_uuid(pin_id, "pin ID")?;
    match database {
        DatabasePool::Postgres(pool) => postgres::pin(pool, id, &pin_id).await,
        DatabasePool::Sqlite(pool) => sqlite::pin(pool, id, &pin_id).await,
    }
}

pub(crate) async fn release_pin(
    database: &DatabasePool,
    id: &SnapshotId,
    pin_id: &str,
) -> MetaResult<()> {
    let pin_id = parse_non_nil_uuid(pin_id, "pin ID")?;
    match database {
        DatabasePool::Postgres(pool) => postgres::release_pin(pool, id, &pin_id).await,
        DatabasePool::Sqlite(pool) => sqlite::release_pin(pool, id, &pin_id).await,
    }
}

pub(crate) async fn check_layers(
    database: &DatabasePool,
    uuids: &[String],
) -> MetaResult<Vec<LayerCheck>> {
    let normalized: Vec<String> = uuids
        .iter()
        .map(|uuid| parse_non_nil_uuid(uuid, "managed layer"))
        .collect::<MetaResult<_>>()?;
    let mut states = BTreeMap::new();
    for batch in normalized.chunks(BATCH_SIZE) {
        let rows = match database {
            DatabasePool::Postgres(pool) => {
                let mut query = QueryBuilder::<Postgres>::new(
                    "SELECT CAST(uuid AS TEXT), state FROM layers WHERE uuid IN (",
                );
                let mut parameters = query.separated(", ");
                for layer_uuid in batch {
                    parameters
                        .push_bind(layer_uuid.as_str())
                        .push_unseparated("::uuid");
                }
                query
                    .push(")")
                    .build_query_as::<(String, String)>()
                    .fetch_all(pool)
                    .await
            }
            DatabasePool::Sqlite(pool) => {
                let mut query = QueryBuilder::<Sqlite>::new(
                    "SELECT CAST(uuid AS TEXT), state FROM layers WHERE uuid IN (",
                );
                let mut parameters = query.separated(", ");
                for layer_uuid in batch {
                    parameters.push_bind(layer_uuid.as_str());
                }
                query
                    .push(")")
                    .build_query_as::<(String, String)>()
                    .fetch_all(pool)
                    .await
            }
        }
        .map_err(|error| MetaError::backend("check layer states", error))?;
        for (layer_uuid, state) in rows {
            let mapped = match state.as_str() {
                "available" => LayerState::Available,
                "deleting" => LayerState::Deleting,
                "deleted" => LayerState::Deleted,
                other => {
                    return Err(MetaError::Unavailable {
                        reason: format!("unknown layer state '{other}'"),
                    })
                }
            };
            states.insert(layer_uuid, mapped);
        }
    }
    Ok(normalized
        .into_iter()
        .map(|uuid| LayerCheck {
            state: states.get(&uuid).copied().unwrap_or(LayerState::Missing),
            uuid,
        })
        .collect())
}

pub(crate) async fn reap_retired_snapshots(
    database: &DatabasePool,
    limit: u32,
) -> MetaResult<Vec<SnapshotId>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    const SQL: &str = "SELECT CAST(s.snapshot_id AS TEXT)
         FROM snapshots s
         WHERE s.retired = TRUE
           AND s.snapshot_type IN ('local', 'distributed')
           AND NOT EXISTS (
               SELECT 1 FROM snapshot_pins p
               WHERE p.snapshot_id = s.snapshot_id AND NOT p.released
           )
         ORDER BY s.created_at ASC, s.snapshot_id ASC
         LIMIT $1";
    let rows = match database {
        DatabasePool::Postgres(pool) => {
            sqlx::query_scalar::<_, String>(SQL)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await
        }
        DatabasePool::Sqlite(pool) => {
            sqlx::query_scalar::<_, String>(SQL)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await
        }
    }
    .map_err(|error| MetaError::backend("scan retired snapshots for GC", error))?;
    rows.into_iter()
        .map(|raw| {
            SnapshotId::parse(&raw)
                .map_err(|error| MetaError::backend("decode retired snapshot ID", error))
        })
        .collect()
}

pub(crate) async fn complete_reaped_snapshot(
    database: &DatabasePool,
    id: &SnapshotId,
) -> MetaResult<bool> {
    match database {
        DatabasePool::Postgres(pool) => postgres::complete_reaped_snapshot(pool, id).await,
        DatabasePool::Sqlite(pool) => sqlite::complete_reaped_snapshot(pool, id).await,
    }
}

pub(crate) async fn claim_deletable_layers(
    database: &DatabasePool,
    limit: u32,
) -> MetaResult<Vec<DeletableLayer>> {
    match database {
        DatabasePool::Postgres(pool) => postgres::claim_deletable_layers(pool, limit).await,
        DatabasePool::Sqlite(pool) => sqlite::claim_deletable_layers(pool, limit).await,
    }
}

pub(crate) async fn finish_layer_deletions(
    database: &DatabasePool,
    uuids: &[String],
    state: &str,
) -> MetaResult<()> {
    let mut normalized = BTreeSet::new();
    for raw in uuids {
        normalized.insert(parse_non_nil_uuid(raw, "managed layer")?);
    }
    let ordered: Vec<String> = normalized.into_iter().collect();
    match database {
        DatabasePool::Postgres(pool) => {
            postgres::finish_layer_deletions(pool, &ordered, state).await
        }
        DatabasePool::Sqlite(pool) => sqlite::finish_layer_deletions(pool, &ordered, state).await,
    }
}

pub(crate) async fn get(
    database: &DatabasePool,
    reference: &str,
) -> MetaResult<Option<SnapshotMetadata>> {
    let mut tombstone = None;
    if let Ok(id) = SnapshotId::parse(reference) {
        let id = id.to_string();
        let row = match database {
            DatabasePool::Postgres(pool) => {
                sqlx::query_as::<_, SnapshotRow>(AssertSqlSafe(format!(
                    "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = $1::uuid"
                )))
                .bind(&id)
                .fetch_optional(pool)
                .await
            }
            DatabasePool::Sqlite(pool) => {
                sqlx::query_as::<_, SnapshotRow>(AssertSqlSafe(format!(
                    "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = $1"
                )))
                .bind(&id)
                .fetch_optional(pool)
                .await
            }
        }
        .map_err(|error| MetaError::backend("get snapshot by ID", error))?;
        if let Some(row) = row {
            // Reaped identities still own their IDs; only unknown-ID tombstones allow alias lookup.
            if row.snapshot_type != "none" || row.source_sandbox_id.is_some() || row.alias.is_some()
            {
                return row.into_entry().map(Some);
            }
            tombstone = Some(row.into_entry()?);
        }
    }
    let alias = SnapshotAlias::parse(reference).map_err(|error| MetaError::InvalidRequest {
        reason: error.to_string(),
    })?;
    let query = AssertSqlSafe(format!(
        "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE alias = $1"
    ));
    let row = match database {
        DatabasePool::Postgres(pool) => {
            sqlx::query_as::<_, SnapshotRow>(query)
                .bind(alias.as_ref())
                .fetch_optional(pool)
                .await
        }
        DatabasePool::Sqlite(pool) => {
            sqlx::query_as::<_, SnapshotRow>(query)
                .bind(alias.as_ref())
                .fetch_optional(pool)
                .await
        }
    }
    .map_err(|error| MetaError::backend("get snapshot by alias", error))?;
    if let Some(row) = row {
        return row.into_entry().map(Some);
    }
    Ok(tombstone)
}

fn build_list_query<DB>(
    filter: &SnapshotListFilter,
    ids: Option<String>,
    id_filter_sql: (&'static str, &'static str),
) -> QueryBuilder<DB>
where
    DB: sqlx::Database,
    for<'a> String: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> &'a str: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
{
    let mut query = QueryBuilder::<DB>::new(format!(
        "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE NOT retired AND snapshot_type <> 'none'"
    ));
    if let Some(ids) = ids {
        query
            .push(id_filter_sql.0)
            .push_bind(ids)
            .push(id_filter_sql.1);
    }
    if let Some(prefix) = &filter.alias_prefix {
        query
            .push(" AND substr(alias, 1, length(")
            .push_bind(prefix.clone())
            .push(")) = ")
            .push_bind(prefix.clone());
    }
    if let Some(source_sandbox_id) = &filter.source_sandbox_id {
        query
            .push(" AND source_sandbox_id = ")
            .push_bind(source_sandbox_id.clone());
    }
    if let Some(reference) = &filter.snapshot_id_or_alias {
        query
            .push(" AND (CAST(snapshot_id AS TEXT) = ")
            .push_bind(reference.clone())
            .push(" OR alias = ")
            .push_bind(reference.clone())
            .push(")");
    }
    if let Some(sources) = &filter.sources {
        match (
            sources.contains(&SnapshotSourceKind::Template),
            sources.contains(&SnapshotSourceKind::Sandbox),
        ) {
            (true, false) => query.push(" AND source_sandbox_id IS NULL"),
            (false, true) => query.push(" AND source_sandbox_id IS NOT NULL"),
            (false, false) => query.push(" AND 1 = 0"),
            (true, true) => &mut query,
        };
    }
    query.push(" ORDER BY snapshots.created_at DESC, snapshots.snapshot_id ASC");
    query
}

pub(crate) async fn list(
    database: &DatabasePool,
    filter: &SnapshotListFilter,
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
    let rows = match database {
        DatabasePool::Postgres(pool) => build_list_query::<Postgres>(
            filter,
            ids,
            (
                " AND snapshots.snapshot_id IN (SELECT value::uuid FROM json_array_elements_text(",
                "::json))",
            ),
        )
        .build_query_as::<SnapshotRow>()
        .fetch_all(pool)
        .await,
        DatabasePool::Sqlite(pool) => {
            build_list_query::<Sqlite>(
                filter,
                ids,
                (
                    " AND snapshots.snapshot_id IN (SELECT value FROM json_each(",
                    "))",
                ),
            )
            .build_query_as::<SnapshotRow>()
            .fetch_all(pool)
            .await
        }
    }
    .map_err(|error| MetaError::backend("list snapshot records", error))?;
    rows.into_iter().map(SnapshotRow::into_entry).collect()
}

macro_rules! write_backend {
    ($module:ident, $db:ty, $pool:ty, $begin:literal, $pg:literal) => {
        mod $module {
            use super::*;

            const UUID_CAST: &str = if $pg { "::uuid" } else { "" };

            fn uuid(parameter: usize) -> String {
                format!("${parameter}{UUID_CAST}")
            }

            async fn attach_layers(
                transaction: &mut sqlx::Transaction<'_, $db>,
                snapshot_id: &SnapshotId,
                relations: &LayerRelations,
                restored_layers: &[String],
            ) -> MetaResult<bool> {
                let restored: BTreeSet<&str> =
                    restored_layers.iter().map(String::as_str).collect();
                let mut known = BTreeSet::new();
                let mut missing_uuids = Vec::new();
                let mut restored_uuids = Vec::new();
                let mut newly_collectible = Vec::new();
                // Lock existing rows in GC's UUID order, including across batches.
                let layer_ids: Vec<_> = relations.layers.keys().map(String::as_str).collect();
                for batch in layer_ids.chunks(BATCH_SIZE) {
                    let mut query = QueryBuilder::<$db>::new(
                        "SELECT CAST(l.uuid AS TEXT), l.state, l.collectible, l.digest, CAST(l.managed_parent_uuid AS TEXT) FROM layers AS l WHERE l.uuid IN (",
                    );
                    let mut parameters = query.separated(", ");
                    for layer_id in batch {
                        parameters.push_bind(*layer_id).push_unseparated(UUID_CAST);
                    }
                    query.push(") ORDER BY l.uuid");
                    if $pg {
                        query.push(if restored.is_empty() && !relations.layers.values().any(|layer| layer.collectible) {
                            " FOR SHARE"
                        } else {
                            " FOR UPDATE"
                        });
                    }
                    let rows = query
                        .build_query_as::<(String, String, bool, String, Option<String>)>()
                        .fetch_all(&mut **transaction)
                        .await
                        .map_err(|error| MetaError::backend("read publication layers", error))?;
                    for (uuid, state, collectible, digest, parent) in rows {
                        let layer = &relations.layers[&uuid];
                        if layer.digest != digest || layer.parent != parent {
                            return Err(MetaError::InvalidRequest {
                                reason: format!("managed layer '{uuid}' has conflicting publication inputs"),
                            });
                        }
                        if layer.collectible && !collectible {
                            newly_collectible.push(uuid.clone());
                        }
                        match state.as_str() {
                            "available" => {
                                known.insert(uuid);
                            }
                            "deleted" => {
                                if restored.contains(uuid.as_str()) {
                                    known.insert(uuid.clone());
                                    restored_uuids.push(uuid);
                                } else {
                                    missing_uuids.push(uuid);
                                }
                            }
                            _ => {
                                return Err(MetaError::Unavailable {
                                    reason: format!("managed layer '{uuid}' is {state}"),
                                });
                            }
                        }
                    }
                }
                if !missing_uuids.is_empty() {
                    return Err(MetaError::MissingLayers {
                        uuids: missing_uuids,
                    });
                }
                for batch in newly_collectible.chunks(BATCH_SIZE) {
                    let mut query =
                        QueryBuilder::<$db>::new("UPDATE layers SET collectible = TRUE WHERE uuid IN (");
                    let mut parameters = query.separated(", ");
                    for uuid in batch {
                        parameters.push_bind(uuid.as_str()).push_unseparated(UUID_CAST);
                    }
                    query.push(")").build().execute(&mut **transaction).await
                        .map_err(|error| MetaError::backend("track snapshot layer ownership", error))?;
                }
                for batch in restored_uuids.chunks(BATCH_SIZE) {
                    let mut query =
                        QueryBuilder::<$db>::new("UPDATE layers SET state = 'available' WHERE uuid IN (");
                    let mut parameters = query.separated(", ");
                    for layer_uuid in batch {
                        parameters
                            .push_bind(layer_uuid.as_str())
                            .push_unseparated(UUID_CAST);
                    }
                    query.push(")");
                    query
                        .build()
                        .execute(&mut **transaction)
                        .await
                        .map_err(|error| MetaError::backend("restore publication layers", error))?;
                }
                let mut missing: Vec<_> = relations
                    .layers
                    .iter()
                    .filter(|(uuid, _)| !known.contains(*uuid))
                    .collect();
                missing.sort_unstable_by_key(|&(uuid, layer)| (layer.depth, uuid));
                for batch in missing.chunks(BATCH_SIZE) {
                    let mut query = QueryBuilder::<$db>::new(
                        "INSERT INTO layers (uuid, managed_parent_uuid, digest, state, collectible) ",
                    );
                    query.push_values(batch, |mut row, (uuid, layer)| {
                        row.push_bind(uuid.as_str()).push_unseparated(UUID_CAST);
                        row.push_bind(layer.parent.as_deref())
                            .push_unseparated(UUID_CAST);
                        row.push_bind(layer.digest.as_str()).push("'available'")
                            .push_bind(layer.collectible);
                    });
                    query.push(" ON CONFLICT (uuid) DO NOTHING");
                    let inserted = query
                        .build()
                        .execute(&mut **transaction)
                        .await
                        .map_err(|error| {
                            MetaError::backend("register publication layers", error)
                        })?;
                    // Restart to lock concurrent insertions in the same order as existing rows.
                    if inserted.rows_affected() != batch.len() as u64 {
                        return Ok(false);
                    }
                }
                let snapshot_id = snapshot_id.to_string();
                let heads: Vec<_> = relations.heads.iter().collect();
                for batch in heads.chunks(BATCH_SIZE) {
                    let mut query = QueryBuilder::<$db>::new(
                        "INSERT INTO snapshot_heads (snapshot_id, layer_uuid) ",
                    );
                    query.push_values(batch, |mut row, head| {
                        row.push_bind(snapshot_id.as_str())
                            .push_unseparated(UUID_CAST);
                        row.push_bind(head.as_str()).push_unseparated(UUID_CAST);
                    });
                    query.push(" ON CONFLICT DO NOTHING");
                    query
                        .build()
                        .execute(&mut **transaction)
                        .await
                        .map_err(|error| MetaError::backend("retain snapshot heads", error))?;
                }
                Ok(true)
            }

            pub(super) async fn register(
                pool: &$pool,
                metadata: &SnapshotMetadata,
                relations: &LayerRelations,
                restored_layers: &[String],
            ) -> MetaResult<SnapshotMetadata> {
                let snapshot_id = metadata.id.to_string();
                let placement = match metadata.snapshot_type {
                    Some(SnapshotPlacement::Local) => "local",
                    Some(SnapshotPlacement::Distributed) => "distributed",
                    None => unreachable!("publication metadata was validated"),
                };
                let source_sandbox_id = metadata.source_sandbox_id.as_deref();
                loop {
                    let mut transaction = pool.begin_with($begin).await.map_err(|error| {
                        MetaError::backend("begin snapshot registration", error)
                    })?;
                    let inserted = sqlx::query::<$db>(AssertSqlSafe(format!(
                        "INSERT INTO snapshots (snapshot_id, alias, snapshot_type, owner_node_id, source_sandbox_id, retired, created_at, updated_at) VALUES ({}, $2, $3, $4, $5, FALSE, $6, $7) ON CONFLICT DO NOTHING",
                        uuid(1),
                    )))
                    .bind(&snapshot_id)
                    .bind(metadata.alias.as_ref().map(AsRef::as_ref))
                    .bind(placement)
                    .bind(metadata.owner_node_id.as_deref())
                    .bind(source_sandbox_id)
                    .bind(metadata.created_at_unix_ms)
                    .bind(metadata.updated_at_unix_ms)
                    .execute(&mut *transaction).await
                    .map_err(|error| MetaError::backend("register snapshot identity", error))?;
                    if inserted.rows_affected() == 0 {
                        let existing: Option<bool> = sqlx::query_scalar(AssertSqlSafe(format!(
                            "SELECT retired FROM snapshots WHERE snapshot_id = {}",
                            uuid(1),
                        )))
                        .bind(&snapshot_id)
                        .fetch_optional(&mut *transaction).await
                        .map_err(|error| MetaError::backend("read conflicting snapshot", error))?;
                        if let Some(retired) = existing {
                            if retired {
                                return Err(MetaError::Unavailable {
                                    reason: format!("snapshot '{snapshot_id}' is retired or reserved"),
                                });
                            }
                            return Err(MetaError::SnapshotConflict {
                                id: metadata.id.clone(),
                            });
                        }
                        let (id, alias): (String, String) = sqlx::query_as(
                            "SELECT CAST(snapshot_id AS TEXT), alias FROM snapshots WHERE alias = $1",
                        )
                        .bind(metadata.alias.as_ref().map(AsRef::as_ref))
                        .fetch_one(&mut *transaction).await
                        .map_err(|error| MetaError::backend("read conflicting alias", error))?;
                        let existing = SnapshotId::parse(&id)
                            .map_err(|error| MetaError::backend("decode alias owner", error))?;
                        return Err(MetaError::AliasConflict {
                            alias, existing, new_id: metadata.id.clone(),
                        });
                    }
                    if !attach_layers(&mut transaction, &metadata.id, relations, restored_layers).await? {
                        transaction.rollback().await
                            .map_err(|error| MetaError::backend("restart publication layer checks", error))?;
                        continue;
                    }
                    transaction.commit().await
                        .map_err(|error| MetaError::backend("commit snapshot registration", error))?;
                    return Ok(metadata.clone());
                }
            }

            pub(super) async fn promote(
                pool: &$pool,
                metadata: &SnapshotMetadata,
                relations: &LayerRelations,
                restored_layers: &[String],
            ) -> MetaResult<Option<SnapshotMetadata>> {
                let snapshot_id = metadata.id.to_string();
                loop {
                    let mut tx = pool.begin_with($begin).await
                        .map_err(|error| MetaError::backend("begin snapshot promotion", error))?;
                    let row = sqlx::query_as::<$db, SnapshotRow>(AssertSqlSafe(format!(
                        "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = {}{}",
                        uuid(1), if $pg { " FOR UPDATE" } else { "" },
                    )))
                    .bind(&snapshot_id).fetch_optional(&mut *tx).await
                    .map_err(|error| MetaError::backend("lock snapshot for promotion", error))?;
                    let Some(row) = row else { return Ok(None) };
                    row.ensure_active()?;
                    if row.snapshot_type == "distributed" {
                        return row.promoted(metadata).map(Some);
                    }

                    // Claim the snapshot row and alias before locking or inserting layers so
                    // registration and promotion always acquire locks in snapshot/alias -> layers order.
                    let updated = sqlx::query::<$db>(AssertSqlSafe(format!(
                        "UPDATE snapshots SET alias = $2, snapshot_type = 'distributed', owner_node_id = NULL, updated_at = $3
                         WHERE snapshot_id = {}",
                        uuid(1),
                    )))
                    .bind(&snapshot_id)
                    .bind(metadata.alias.as_ref().map(AsRef::as_ref))
                    .bind(metadata.updated_at_unix_ms)
                    .execute(&mut *tx).await;
                    if let Err(error) = updated {
                        tx.rollback().await
                            .map_err(|rollback| MetaError::backend("abort conflicting promotion", rollback))?;
                        if error
                            .as_database_error()
                            .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
                        {
                            let (id, alias): (String, String) = sqlx::query_as(
                                "SELECT CAST(snapshot_id AS TEXT), alias FROM snapshots WHERE alias = $1"
                            )
                            .bind(metadata.alias.as_ref().map(AsRef::as_ref))
                            .fetch_one(pool).await
                            .map_err(|lookup| MetaError::backend("read conflicting promotion alias", lookup))?;
                            let existing = SnapshotId::parse(&id)
                                .map_err(|decode| MetaError::backend("decode alias owner", decode))?;
                            return Err(MetaError::AliasConflict {
                                alias, existing, new_id: metadata.id.clone(),
                            });
                        }
                        return Err(MetaError::backend("promote snapshot", error));
                    }
                    if !attach_layers(&mut tx, &metadata.id, relations, restored_layers).await? {
                        tx.rollback().await
                            .map_err(|error| MetaError::backend("restart promotion layer checks", error))?;
                        continue;
                    }
                    let mut entry = row.into_entry()?;
                    entry.alias = metadata.alias.clone();
                    entry.snapshot_type = Some(SnapshotPlacement::Distributed);
                    entry.owner_node_id = None;
                    entry.updated_at_unix_ms = metadata.updated_at_unix_ms;
                    tx.commit().await
                        .map_err(|error| MetaError::backend("commit snapshot promotion", error))?;
                    return Ok(Some(entry));
                }
            }

            pub(super) async fn retire(pool: &$pool, id: &SnapshotId) -> MetaResult<()> {
                let now = now_unix_ms();
                // An unknown ID must compete on the same primary key as a late first registration.
                sqlx::query::<$db>(AssertSqlSafe(format!(
                    "INSERT INTO snapshots (snapshot_id, snapshot_type, retired, created_at, updated_at)
                     VALUES ({}, 'none', TRUE, $2, $2)
                     ON CONFLICT (snapshot_id) DO UPDATE SET retired = TRUE WHERE NOT snapshots.retired",
                    uuid(1),
                )))
                .bind(id.to_string()).bind(now).execute(pool).await
                .map_err(|error| MetaError::backend("retire snapshot", error))?;
                Ok(())
            }

            pub(super) async fn pin(
                pool: &$pool,
                id: &SnapshotId,
                pin_id: &str,
            ) -> MetaResult<Option<SnapshotMetadata>> {
                let mut tx = pool
                    .begin_with($begin)
                    .await
                    .map_err(|error| MetaError::backend("begin snapshot pin", error))?;
                let row = sqlx::query_as::<$db, SnapshotRow>(AssertSqlSafe(format!(
                    "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = {}{}",
                    uuid(1),
                    if $pg { " FOR SHARE" } else { "" },
                )))
                .bind(id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("lock snapshot for pin", error))?;
                let Some(row) = row else { return Ok(None) };
                if row.snapshot_type == "none" {
                    return Err(MetaError::Unavailable {
                        reason: format!("snapshot '{}' is reserved", row.snapshot_id),
                    });
                }
                if !row.retired {
                    sqlx::query::<$db>(AssertSqlSafe(format!(
                        "INSERT INTO snapshot_pins (pin_id, snapshot_id, released) VALUES ({}, {}, FALSE) ON CONFLICT (pin_id) DO NOTHING",
                        uuid(1),
                        uuid(2),
                    )))
                    .bind(pin_id)
                    .bind(id.to_string())
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| MetaError::backend("insert snapshot pin", error))?;
                }
                let stored_pin = sqlx::query_as::<$db, (String, bool)>(AssertSqlSafe(format!(
                    "SELECT CAST(snapshot_id AS TEXT), released FROM snapshot_pins WHERE pin_id = {}{}",
                    uuid(1),
                    if $pg { " FOR UPDATE" } else { "" },
                )))
                .bind(pin_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("read snapshot pin", error))?;
                let Some((bound_snapshot, released)) = stored_pin else {
                    return Err(MetaError::Unavailable {
                        reason: format!("snapshot '{}' is retired", row.snapshot_id),
                    });
                };
                if bound_snapshot != id.to_string() {
                    return Err(MetaError::InvalidRequest {
                        reason: format!(
                            "pin '{pin_id}' is already bound to snapshot '{bound_snapshot}'"
                        ),
                    });
                }
                if released {
                    return Err(MetaError::Unavailable {
                        reason: format!("pin '{pin_id}' has already been released"),
                    });
                }
                let entry = row.into_entry()?;
                tx.commit()
                    .await
                    .map_err(|error| MetaError::backend("commit snapshot pin", error))?;
                Ok(Some(entry))
            }

            pub(super) async fn release_pin(
                pool: &$pool,
                id: &SnapshotId,
                pin_id: &str,
            ) -> MetaResult<()> {
                let snapshot_id = id.to_string();
                let updated = sqlx::query::<$db>(AssertSqlSafe(format!(
                    "INSERT INTO snapshot_pins (pin_id, snapshot_id, released) VALUES ({}, {}, TRUE)
                     ON CONFLICT (pin_id) DO UPDATE SET released = TRUE
                     WHERE snapshot_pins.snapshot_id = EXCLUDED.snapshot_id",
                    uuid(1),
                    uuid(2),
                )))
                .bind(pin_id)
                .bind(&snapshot_id)
                .execute(pool)
                .await
                .map_err(|error| MetaError::backend("release snapshot pin", error))?;
                if updated.rows_affected() == 0 {
                    let bound_snapshot: String = sqlx::query_scalar(AssertSqlSafe(format!(
                        "SELECT CAST(snapshot_id AS TEXT) FROM snapshot_pins WHERE pin_id = {}",
                        uuid(1),
                    )))
                    .bind(pin_id)
                    .fetch_one(pool)
                    .await
                    .map_err(|error| MetaError::backend("read conflicting snapshot pin", error))?;
                    return Err(MetaError::InvalidRequest {
                        reason: format!(
                            "pin '{pin_id}' is already bound to snapshot '{bound_snapshot}'"
                        ),
                    });
                }
                Ok(())
            }

            pub(super) async fn complete_reaped_snapshot(
                pool: &$pool,
                id: &SnapshotId,
            ) -> MetaResult<bool> {
                let mut tx = pool.begin_with($begin).await.map_err(|error| {
                    MetaError::backend("begin retired snapshot head release", error)
                })?;
                let state = sqlx::query_scalar::<$db, bool>(AssertSqlSafe(format!(
                    "SELECT retired FROM snapshots WHERE snapshot_id = {} AND snapshot_type IN ('local', 'distributed'){}",
                    uuid(1),
                    if $pg { " FOR UPDATE" } else { "" },
                )))
                .bind(id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("lock retired snapshot", error))?;
                let Some(true) = state else {
                    return Ok(false);
                };
                let pinned: bool = sqlx::query_scalar::<$db, bool>(AssertSqlSafe(format!(
                    "SELECT EXISTS (SELECT 1 FROM snapshot_pins WHERE snapshot_id = {} AND NOT released)",
                    uuid(1),
                )))
                .bind(id.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(|error| {
                    MetaError::backend("check active pins for retired snapshot", error)
                })?;
                if pinned {
                    return Ok(false);
                }
                sqlx::query::<$db>(AssertSqlSafe(format!(
                    "DELETE FROM snapshot_heads WHERE snapshot_id = {}",
                    uuid(1),
                )))
                .bind(id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("delete retired snapshot heads", error))?;
                sqlx::query::<$db>(AssertSqlSafe(format!(
                    "UPDATE snapshots SET snapshot_type = 'none' WHERE snapshot_id = {}",
                    uuid(1),
                )))
                .bind(id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(|error| MetaError::backend("complete retired snapshot cleanup", error))?;
                tx.commit().await.map_err(|error| {
                    MetaError::backend("commit retired snapshot head release", error)
                })?;
                Ok(true)
            }

            pub(super) async fn claim_deletable_layers(
                pool: &$pool,
                limit: u32,
            ) -> MetaResult<Vec<DeletableLayer>> {
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let candidates = sqlx::query_scalar::<$db, String>(
                    "SELECT CAST(l.uuid AS TEXT)
                     FROM layers l
                     WHERE l.state = 'available' AND l.collectible
                       AND NOT EXISTS (SELECT 1 FROM snapshot_heads h WHERE h.layer_uuid = l.uuid)
                       AND NOT EXISTS (
                           SELECT 1 FROM layers c
                           WHERE c.managed_parent_uuid = l.uuid
                             AND c.state IN ('available', 'deleting')
                       )
                     ORDER BY l.uuid ASC
                     LIMIT $1",
                )
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await
                .map_err(|error| MetaError::backend("scan candidate leaf layers", error))?;
                if candidates.is_empty() {
                    return Ok(Vec::new());
                }
                let mut tx = pool
                    .begin_with($begin)
                    .await
                    .map_err(|error| MetaError::backend("begin leaf layer claim", error))?;
                let mut claimed = Vec::new();
                for candidate in candidates {
                    let (digest, state) = sqlx::query_as::<$db, (String, String)>(AssertSqlSafe(format!(
                        "SELECT digest, state FROM layers WHERE uuid = {}{}",
                        uuid(1),
                        if $pg { " FOR UPDATE" } else { "" },
                    )))
                    .bind(&candidate)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|error| MetaError::backend("lock candidate leaf layer", error))?;
                    if state != "available" {
                        continue;
                    }
                    let unreferenced: bool = sqlx::query_scalar::<$db, bool>(AssertSqlSafe(format!(
                        "SELECT NOT EXISTS (SELECT 1 FROM snapshot_heads WHERE layer_uuid = {})
                            AND NOT EXISTS (
                                SELECT 1 FROM layers
                                WHERE managed_parent_uuid = {}
                                  AND state IN ('available', 'deleting')
                            )
                            AND NOT EXISTS (
                                SELECT 1 FROM layers other
                                WHERE other.digest = $2 AND other.uuid <> {} AND other.collectible
                                  AND other.state IN ('available', 'deleting')
                            )",
                        uuid(1),
                        uuid(1),
                        uuid(1),
                    )))
                    .bind(&candidate)
                    .bind(&digest)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|error| {
                        MetaError::backend("recheck candidate leaf layer references", error)
                    })?;
                    if !unreferenced {
                        continue;
                    }
                    sqlx::query::<$db>(AssertSqlSafe(format!(
                        "UPDATE layers SET state = 'deleting' WHERE uuid = {}",
                        uuid(1),
                    )))
                    .bind(&candidate)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| MetaError::backend("mark leaf layer deleting", error))?;
                    claimed.push(DeletableLayer {
                        uuid: candidate,
                        digest,
                    });
                }
                tx.commit()
                    .await
                    .map_err(|error| MetaError::backend("commit leaf layer claim", error))?;
                Ok(claimed)
            }

            pub(super) async fn finish_layer_deletions(
                pool: &$pool,
                uuids: &[String],
                state: &str,
            ) -> MetaResult<()> {
                if uuids.is_empty() {
                    return Ok(());
                }
                let mut tx = pool.begin_with($begin).await.map_err(|error| {
                    MetaError::backend("begin layer deletion state update", error)
                })?;
                for batch in uuids.chunks(BATCH_SIZE) {
                    let parameters: Vec<_> = (2..=batch.len() + 1).map(uuid).collect();
                    let mut query = sqlx::query::<$db>(AssertSqlSafe(format!(
                        "UPDATE layers SET state = $1 WHERE uuid IN ({}) AND state = 'deleting'",
                        parameters.join(", "),
                    )))
                    .bind(state);
                    for layer_uuid in batch {
                        query = query.bind(layer_uuid);
                    }
                    query
                        .execute(&mut *tx)
                        .await
                        .map_err(|error| MetaError::backend("update layer deletion state", error))?;
                }
                tx.commit()
                    .await
                    .map_err(|error| MetaError::backend("commit layer deletion state update", error))
            }
        }
    };
}

write_backend!(
    postgres,
    Postgres,
    PgPool,
    "BEGIN ISOLATION LEVEL READ COMMITTED",
    true
);
write_backend!(sqlite, Sqlite, SqlitePool, "BEGIN IMMEDIATE", false);
