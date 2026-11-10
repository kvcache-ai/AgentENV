use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use agentenv_domain::{
    CommittedAttachedDrive, LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotConfig, SnapshotId,
    SnapshotListFilter, SnapshotManifest, SnapshotPlacement, SnapshotRecord, SnapshotSourceKind,
};
use sqlx::{AssertSqlSafe, PgPool, Postgres, QueryBuilder, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::database::DatabasePool;
use crate::{MetaError, MetaResult, SnapshotEntry};

const BATCH_SIZE: usize = 128;
const SNAPSHOT_COLUMNS: &str = "CAST(snapshot_id AS TEXT) AS snapshot_id, alias, snapshot_type, CAST(metadata AS TEXT) AS metadata, owner_node_id, retired, created_at, updated_at";

#[derive(sqlx::FromRow)]
struct SnapshotRow {
    snapshot_id: String,
    alias: Option<String>,
    snapshot_type: String,
    metadata: Option<String>,
    owner_node_id: Option<String>,
    retired: bool,
    created_at: i64,
    updated_at: i64,
}

impl SnapshotRow {
    fn into_entry(self) -> MetaResult<SnapshotEntry> {
        Ok(SnapshotEntry {
            snapshot_id: SnapshotId::parse(&self.snapshot_id)
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
            config: self
                .metadata
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|error| MetaError::backend("decode snapshot metadata", error))?,
            retired: self.retired,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }

    fn ensure_active(&self) -> MetaResult<()> {
        if self.retired || self.snapshot_type == "none" {
            return Err(MetaError::Unavailable {
                reason: format!("snapshot '{}' is retired or reserved", self.snapshot_id),
            });
        }
        Ok(())
    }

    fn promoted(self, record: &SnapshotRecord) -> MetaResult<SnapshotEntry> {
        if self.alias.as_deref() != record.alias.as_ref().map(AsRef::as_ref) {
            return Err(MetaError::SnapshotConflict {
                id: record.id.clone(),
            });
        }
        self.into_entry()
    }
}

struct ManagedLayerNode {
    parent: Option<String>,
    digest: String,
    depth: usize,
}

#[derive(Default)]
struct LayerRelations {
    layers: BTreeMap<String, ManagedLayerNode>,
    heads: BTreeSet<String>,
}

fn collect_layer_relations(
    config: &SnapshotConfig,
    manifest: Option<&SnapshotManifest>,
) -> MetaResult<LayerRelations> {
    let empty = SnapshotManifest::default();
    let manifest = manifest.unwrap_or(&empty);
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
        managed(&manifest.rootfs_layers),
        manifest.memory_layers.iter().collect(),
    ]
    .into_iter()
    .chain(manifest.attached_drives.iter().map(|drive| {
        let CommittedAttachedDrive::Overlaybd { layers, .. } = drive;
        managed(layers)
    }))
    .chain(
        config
            .volume_snapshots
            .iter()
            .map(|volume| managed(&volume.layers)),
    );
    let mut layers: BTreeMap<String, ManagedLayerNode> = BTreeMap::new();
    let mut heads = BTreeSet::new();
    for chain in chains {
        let mut parent = None;
        for (depth, layer) in chain.into_iter().enumerate() {
            let uuid = layer
                .uuid
                .as_deref()
                .and_then(|value| Uuid::parse_str(value).ok())
                .filter(|uuid| !uuid.is_nil())
                .ok_or_else(|| MetaError::InvalidRequest {
                    reason: "managed layer requires a non-nil UUID".into(),
                })?
                .to_string();
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
    record: &SnapshotRecord,
) -> MetaResult<SnapshotEntry> {
    let relations = collect_layer_relations(&record.config, record.manifest.as_ref())?;
    match database {
        DatabasePool::Postgres(pool) => postgres::register(pool, record, &relations).await,
        DatabasePool::Sqlite(pool) => sqlite::register(pool, record, &relations).await,
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
    record: &SnapshotRecord,
) -> MetaResult<Option<SnapshotEntry>> {
    let manifest = record
        .manifest
        .as_ref()
        .expect("promotion metadata was validated");
    let relations = collect_layer_relations(&record.config, Some(manifest))?;
    match database {
        DatabasePool::Postgres(pool) => postgres::promote(pool, record, &relations).await,
        DatabasePool::Sqlite(pool) => sqlite::promote(pool, record, &relations).await,
    }
}

pub(crate) async fn retire(database: &DatabasePool, id: &SnapshotId) -> MetaResult<()> {
    match database {
        DatabasePool::Postgres(pool) => postgres::retire(pool, id).await,
        DatabasePool::Sqlite(pool) => sqlite::retire(pool, id).await,
    }
}

pub(crate) async fn get(
    database: &DatabasePool,
    reference: &str,
) -> MetaResult<Option<SnapshotEntry>> {
    match database {
        DatabasePool::Postgres(pool) => postgres::get(pool, reference).await,
        DatabasePool::Sqlite(pool) => sqlite::get(pool, reference).await,
    }
}

pub(crate) async fn list(
    database: &DatabasePool,
    filter: &SnapshotListFilter,
) -> MetaResult<Vec<SnapshotEntry>> {
    match database {
        DatabasePool::Postgres(pool) => postgres::list(pool, filter).await,
        DatabasePool::Sqlite(pool) => sqlite::list(pool, filter).await,
    }
}

macro_rules! directory_backend {
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
            ) -> MetaResult<bool> {
                let mut known = BTreeSet::new();
                // Lock existing rows in GC's UUID order, including across batches.
                let layer_ids: Vec<_> = relations.layers.keys().map(String::as_str).collect();
                for batch in layer_ids.chunks(BATCH_SIZE) {
                    let mut query = QueryBuilder::<$db>::new(
                        "SELECT CAST(l.uuid AS TEXT), l.state FROM layers AS l WHERE l.uuid IN (",
                    );
                    let mut parameters = query.separated(", ");
                    for layer_id in batch {
                        parameters.push_bind(*layer_id).push_unseparated(UUID_CAST);
                    }
                    query.push(") ORDER BY l.uuid");
                    if $pg {
                        query.push(" FOR SHARE");
                    }
                    let rows = query.build_query_as::<(String, String)>()
                        .fetch_all(&mut **transaction).await
                        .map_err(|error| MetaError::backend("read publication layers", error))?;
                    for (uuid, state) in rows {
                        if state != "available" {
                            return Err(MetaError::Unavailable {
                                reason: format!("managed layer '{uuid}' is {state}"),
                            });
                        }
                        known.insert(uuid);
                    }
                }
                let mut missing: Vec<_> = relations.layers.iter()
                    .filter(|(uuid, _)| !known.contains(*uuid)).collect();
                missing.sort_unstable_by_key(|&(uuid, layer)| (layer.depth, uuid));
                for batch in missing.chunks(BATCH_SIZE) {
                    let mut query = QueryBuilder::<$db>::new(
                        "INSERT INTO layers (uuid, managed_parent_uuid, digest, state) ",
                    );
                    query.push_values(batch, |mut row, (uuid, layer)| {
                        row.push_bind(uuid.as_str()).push_unseparated(UUID_CAST);
                        row.push_bind(layer.parent.as_deref()).push_unseparated(UUID_CAST);
                        row.push_bind(layer.digest.as_str()).push("'available'");
                    });
                    query.push(" ON CONFLICT (uuid) DO NOTHING");
                    let inserted = query.build().execute(&mut **transaction).await
                        .map_err(|error| MetaError::backend("register publication layers", error))?;
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
                        row.push_bind(snapshot_id.as_str()).push_unseparated(UUID_CAST);
                        row.push_bind(head.as_str()).push_unseparated(UUID_CAST);
                    });
                    query.push(" ON CONFLICT (snapshot_id, layer_uuid) DO NOTHING");
                    query.build().execute(&mut **transaction).await
                        .map_err(|error| MetaError::backend("retain snapshot heads", error))?;
                }
                Ok(true)
            }

            pub(super) async fn register(
                pool: &$pool,
                record: &SnapshotRecord,
                relations: &LayerRelations,
            ) -> MetaResult<SnapshotEntry> {
                let snapshot_id = record.id.to_string();
                let placement = match record.snapshot_type {
                    SnapshotPlacement::Local => "local",
                    SnapshotPlacement::Distributed => "distributed",
                };
                let encoded = serde_json::to_string(&record.config)
                    .map_err(|error| MetaError::backend("encode snapshot metadata", error))?;
                loop {
                    let mut transaction = pool.begin_with($begin).await
                        .map_err(|error| MetaError::backend("begin snapshot registration", error))?;
                    let inserted = sqlx::query::<$db>(AssertSqlSafe(format!(
                        "INSERT INTO snapshots (snapshot_id, alias, snapshot_type, owner_node_id, metadata, retired, created_at, updated_at) VALUES ({}, $2, $3, $4, {}, FALSE, $6, $7) ON CONFLICT DO NOTHING",
                        uuid(1), if $pg { "CAST($5 AS JSON)" } else { "$5" },
                    )))
                    .bind(&snapshot_id)
                    .bind(record.alias.as_ref().map(AsRef::as_ref))
                    .bind(placement)
                    .bind(record.owner_node_id.as_deref())
                    .bind(&encoded)
                    .bind(record.created_at_unix_ms)
                    .bind(record.updated_at_unix_ms)
                    .execute(&mut *transaction).await
                    .map_err(|error| MetaError::backend("register snapshot identity", error))?;
                    if inserted.rows_affected() == 0 {
                        let existing: Option<(bool, String)> = sqlx::query_as(AssertSqlSafe(format!(
                            "SELECT retired, snapshot_type FROM snapshots WHERE snapshot_id = {}",
                            uuid(1),
                        )))
                        .bind(&snapshot_id)
                        .fetch_optional(&mut *transaction).await
                        .map_err(|error| MetaError::backend("read conflicting snapshot", error))?;
                        if let Some((retired, snapshot_type)) = existing {
                            if retired || snapshot_type == "none" {
                                return Err(MetaError::Unavailable {
                                    reason: format!("snapshot '{snapshot_id}' is retired or reserved"),
                                });
                            }
                            return Err(MetaError::SnapshotConflict {
                                id: record.id.clone(),
                            });
                        }
                        let (id, alias): (String, String) = sqlx::query_as(
                            "SELECT CAST(snapshot_id AS TEXT), alias FROM snapshots WHERE alias = $1"
                        )
                        .bind(record.alias.as_ref().map(AsRef::as_ref))
                        .fetch_one(&mut *transaction).await
                        .map_err(|error| MetaError::backend("read conflicting alias", error))?;
                        let existing = SnapshotId::parse(&id)
                            .map_err(|error| MetaError::backend("decode alias owner", error))?;
                        return Err(MetaError::AliasConflict {
                            alias, existing, new_id: record.id.clone(),
                        });
                    }
                    if !attach_layers(&mut transaction, &record.id, relations).await? {
                        transaction.rollback().await
                            .map_err(|error| MetaError::backend("restart publication layer checks", error))?;
                        continue;
                    }
                    transaction.commit().await
                        .map_err(|error| MetaError::backend("commit snapshot registration", error))?;
                    return Ok(SnapshotEntry::from_record(record));
                }
            }

            pub(super) async fn promote(
                pool: &$pool,
                record: &SnapshotRecord,
                relations: &LayerRelations,
            ) -> MetaResult<Option<SnapshotEntry>> {
                let snapshot_id = record.id.to_string();
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
                        return row.promoted(record).map(Some);
                    }

                    // Claim the snapshot row and alias before locking or inserting layers so
                    // registration and promotion always acquire locks in snapshot/alias -> layers order.
                    let updated = sqlx::query::<$db>(AssertSqlSafe(format!(
                        "UPDATE snapshots SET alias = $2, snapshot_type = 'distributed', owner_node_id = NULL, updated_at = $3
                         WHERE snapshot_id = {}",
                        uuid(1),
                    )))
                    .bind(&snapshot_id)
                    .bind(record.alias.as_ref().map(AsRef::as_ref))
                    .bind(record.updated_at_unix_ms)
                    .execute(&mut *tx).await;
                    if let Err(error) = updated {
                        tx.rollback().await
                            .map_err(|rollback| MetaError::backend("abort conflicting promotion", rollback))?;
                        if error
                            .as_database_error()
                            .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
                        {
                            if let Some(alias) = record.alias.as_ref() {
                                let owner: Option<String> = sqlx::query_scalar(
                                    "SELECT CAST(snapshot_id AS TEXT) FROM snapshots WHERE alias = $1"
                                )
                                .bind(alias.as_ref())
                                .fetch_optional(pool).await
                                .map_err(|lookup| MetaError::backend("read conflicting promotion alias", lookup))?;
                                if let Some(owner) = owner {
                                    let existing = SnapshotId::parse(&owner)
                                        .map_err(|decode| MetaError::backend("decode alias owner", decode))?;
                                    if existing != record.id {
                                        return Err(MetaError::AliasConflict {
                                            alias: alias.to_string(),
                                            existing,
                                            new_id: record.id.clone(),
                                        });
                                    }
                                }
                            }
                        }
                        return Err(MetaError::backend("promote snapshot", error));
                    }
                    if !attach_layers(&mut tx, &record.id, relations).await? {
                        tx.rollback().await
                            .map_err(|error| MetaError::backend("restart promotion layer checks", error))?;
                        continue;
                    }
                    let mut entry = row.into_entry()?;
                    entry.alias = record.alias.clone();
                    entry.snapshot_type = Some(SnapshotPlacement::Distributed);
                    entry.owner_node_id = None;
                    entry.updated_at = record.updated_at_unix_ms;
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

            pub(super) async fn get(pool: &$pool, reference: &str) -> MetaResult<Option<SnapshotEntry>> {
                let mut tombstone = None;
                if let Ok(id) = SnapshotId::parse(reference) {
                    let row = sqlx::query_as::<$db, SnapshotRow>(AssertSqlSafe(format!(
                        "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = {}", uuid(1),
                    )))
                    .bind(id.to_string())
                    .fetch_optional(pool).await
                    .map_err(|error| MetaError::backend("get snapshot by ID", error))?;
                    if let Some(row) = row {
                        if row.snapshot_type != "none" {
                            return row.into_entry().map(Some);
                        }
                        tombstone = Some(row.into_entry()?);
                    }
                }
                let alias = SnapshotAlias::parse(reference)
                    .map_err(|error| MetaError::InvalidRequest { reason: error.to_string() })?;
                if let Some(row) = sqlx::query_as::<$db, SnapshotRow>(AssertSqlSafe(format!(
                    "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE alias = $1"
                )))
                .bind(alias.as_ref())
                .fetch_optional(pool).await
                .map_err(|error| MetaError::backend("get snapshot by alias", error))?
                {
                    return row.into_entry().map(Some);
                }
                Ok(tombstone)
            }

            pub(super) async fn list(
                pool: &$pool,
                filter: &SnapshotListFilter,
            ) -> MetaResult<Vec<SnapshotEntry>> {
                if filter.sources.as_ref().is_some_and(Vec::is_empty)
                    || filter.template_statuses.as_ref().is_some_and(Vec::is_empty)
                    || filter.snapshot_ids.as_ref().is_some_and(Vec::is_empty)
                    || filter.alias_prefix.as_ref().is_some_and(|prefix| {
                        !prefix.is_empty() && SnapshotAlias::parse(prefix).is_err()
                    })
                    || filter.source_sandbox_id.as_ref().is_some_and(|source| {
                        source.is_empty() || source.contains('\0')
                    })
                {
                    return Ok(Vec::new());
                }
                let mut query = QueryBuilder::<$db>::new(format!(
                    "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE NOT retired AND snapshot_type <> 'none'"
                ));
                if let Some(ids) = &filter.snapshot_ids {
                    let ids = serde_json::to_string(ids)
                        .map_err(|error| MetaError::backend("encode snapshot ID filter", error))?;
                    if $pg {
                        query.push(" AND snapshots.snapshot_id IN (SELECT value::uuid FROM json_array_elements_text(")
                            .push_bind(ids).push("::json))");
                    } else {
                        query.push(" AND snapshots.snapshot_id IN (SELECT value FROM json_each(")
                            .push_bind(ids).push("))");
                    }
                }
                if let Some(prefix) = &filter.alias_prefix {
                    if $pg {
                        query.push(" AND starts_with(alias, ").push_bind(prefix).push(")");
                    } else {
                        query.push(" AND instr(alias, ").push_bind(prefix)
                            .push(") = 1");
                    }
                }
                if let Some(source_sandbox_id) = &filter.source_sandbox_id {
                    query
                        .push(" AND (metadata -> 'source' -> 'Sandbox' ->> 'source_sandbox_id') = ")
                        .push_bind(source_sandbox_id);
                }
                if let Some(reference) = &filter.snapshot_id_or_alias {
                    query.push(" AND (CAST(snapshot_id AS TEXT) = ").push_bind(reference)
                        .push(" OR alias = ").push_bind(reference).push(")");
                }
                if let Some(sources) = &filter.sources {
                    query.push(" AND (");
                    for (index, source) in sources.iter().enumerate() {
                        if index != 0 { query.push(" OR "); }
                        query.push(match source {
                            SnapshotSourceKind::Sandbox => "(metadata -> 'source' -> 'Sandbox') IS NOT NULL",
                            SnapshotSourceKind::Template => "(metadata -> 'source' -> 'Template') IS NOT NULL",
                        });
                    }
                    query.push(")");
                }
                if let Some(statuses) = &filter.template_statuses {
                    query.push(" AND (metadata -> 'source' -> 'Template' -> 'build' ->> 'status') IN (");
                    let mut values = query.separated(", ");
                    for status in statuses { values.push_bind(format!("{status:?}")); }
                    query.push(")");
                }
                query.push(" ORDER BY snapshots.created_at DESC, snapshots.snapshot_id ASC")
                .build_query_as::<SnapshotRow>()
                .fetch_all(pool).await
                .map_err(|error| MetaError::backend("list snapshot records", error))?
                .into_iter().map(SnapshotRow::into_entry).collect()
            }
        }
    };
}

directory_backend!(
    postgres,
    Postgres,
    PgPool,
    "BEGIN ISOLATION LEVEL READ COMMITTED",
    true
);
directory_backend!(sqlite, Sqlite, SqlitePool, "BEGIN IMMEDIATE", false);
