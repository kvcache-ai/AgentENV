use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use agentenv_domain::{
    CommittedAttachedDrive, CommittedSnapshot, LayerRef, ManagedLayerRef, SnapshotAlias,
    SnapshotId, SnapshotPlacement, SnapshotRecord,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{AssertSqlSafe, PgPool, Postgres, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::database::DatabasePool;
use crate::{MetaError, MetaResult, SnapshotEntry};

const BATCH_SIZE: usize = 128;
const SNAPSHOT_COLUMNS: &str = "CAST(snapshot_id AS TEXT) AS snapshot_id, alias, snapshot_type, committed_digest, CAST(metadata AS TEXT) AS metadata, retired, created_at";

#[derive(sqlx::FromRow)]
struct SnapshotRow {
    snapshot_id: String,
    alias: Option<String>,
    snapshot_type: String,
    committed_digest: Option<String>,
    metadata: Option<String>,
    retired: bool,
    created_at: i64,
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
            committed_digest: self.committed_digest,
            metadata: self
                .metadata
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|error| MetaError::backend("decode snapshot metadata", error))?,
            retired: self.retired,
            created_at: self.created_at,
        })
    }

    fn registered(self, requested: &SnapshotEntry, metadata: &Value) -> MetaResult<SnapshotEntry> {
        if self.retired || self.snapshot_type == "none" {
            return Err(MetaError::Unavailable {
                reason: format!("snapshot '{}' is retired or reserved", self.snapshot_id),
            });
        }
        let existing_metadata = self
            .metadata
            .as_deref()
            .map(serde_json::from_str::<Value>)
            .transpose()
            .map_err(|error| MetaError::backend("compare snapshot metadata", error))?
            .unwrap_or(Value::Null);
        let existing = self.into_entry()?;
        if existing.snapshot_type != requested.snapshot_type
            || existing.committed_digest != requested.committed_digest
            || existing.alias != requested.alias
            || existing.created_at != requested.created_at
            || existing_metadata != *metadata
        {
            return Err(MetaError::SnapshotConflict {
                id: requested.snapshot_id.clone(),
            });
        }
        Ok(existing)
    }
}

fn canonical_committed_digest(committed: &CommittedSnapshot) -> MetaResult<String> {
    fn write_canonical(value: &Value, out: &mut Vec<u8>) -> serde_json::Result<()> {
        match value {
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                serde_json::to_writer(out, value)
            }
            Value::Array(items) => {
                out.push(b'[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    write_canonical(item, out)?;
                }
                out.push(b']');
                Ok(())
            }
            Value::Object(map) => {
                let mut entries: Vec<_> = map.iter().collect();
                entries.sort_by_key(|(left, _)| *left);
                out.push(b'{');
                for (index, (key, item)) in entries.into_iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    serde_json::to_writer(&mut *out, key)?;
                    out.push(b':');
                    write_canonical(item, out)?;
                }
                out.push(b'}');
                Ok(())
            }
        }
    }

    let value = serde_json::to_value(committed)
        .map_err(|error| MetaError::backend("encode committed snapshot", error))?;
    let mut canonical = Vec::new();
    write_canonical(&value, &mut canonical)
        .map_err(|error| MetaError::backend("canonicalize committed snapshot", error))?;
    let digest = Sha256::digest(&canonical);
    let mut formatted = String::with_capacity("sha256:".len() + digest.len() * 2);
    formatted.push_str("sha256:");
    for byte in digest {
        let _ = write!(&mut formatted, "{byte:02x}");
    }
    Ok(formatted)
}

struct ManagedLayerNode {
    uuid: String,
    parent: Option<String>,
    digest: String,
    depth: usize,
}

#[derive(Default)]
struct LayerRelations {
    layers: Vec<ManagedLayerNode>,
    heads: BTreeSet<String>,
}

fn collect_layer_relations(committed: &CommittedSnapshot) -> MetaResult<LayerRelations> {
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
    }))
    .chain(
        committed
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
                        uuid: uuid.clone(),
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
    let mut layers: Vec<_> = layers.into_values().collect();
    layers.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then_with(|| left.uuid.cmp(&right.uuid))
    });
    Ok(LayerRelations { layers, heads })
}

pub(crate) async fn register(
    database: &DatabasePool,
    record: &SnapshotRecord,
) -> MetaResult<SnapshotEntry> {
    let committed = record
        .committed
        .as_ref()
        .expect("publication metadata was validated");
    let committed_digest = match record.snapshot_type {
        SnapshotPlacement::Local => None,
        SnapshotPlacement::Distributed => Some(canonical_committed_digest(committed)?),
    };
    let relations = collect_layer_relations(committed)?;
    let requested = SnapshotEntry::from_record(record, committed_digest);
    let metadata = serde_json::to_value(&requested.metadata)
        .map_err(|error| MetaError::backend("encode snapshot metadata", error))?;
    let encoded = metadata.to_string();
    match database {
        DatabasePool::Postgres(pool) => {
            postgres::register(pool, &requested, &metadata, &encoded, &relations).await
        }
        DatabasePool::Sqlite(pool) => {
            sqlite::register(pool, &requested, &metadata, &encoded, &relations).await
        }
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

pub(crate) async fn list(database: &DatabasePool) -> MetaResult<Vec<SnapshotEntry>> {
    match database {
        DatabasePool::Postgres(pool) => postgres::list(pool).await,
        DatabasePool::Sqlite(pool) => sqlite::list(pool).await,
    }
}

macro_rules! directory_backend {
    ($module:ident, $db:ty, $pool:ty, $begin:literal, $pg:literal) => {
        mod $module {
            use super::*;

            fn uuid(parameter: usize) -> String {
                if $pg {
                    format!("CAST(${parameter} AS UUID)")
                } else {
                    format!("${parameter}")
                }
            }

            async fn attach_layers(
                transaction: &mut sqlx::Transaction<'_, $db>,
                snapshot_id: &SnapshotId,
                relations: &LayerRelations,
            ) -> MetaResult<bool> {
                let mut known = BTreeSet::new();
                // Fixed parent/depth identities give every publisher the same lock order.
                for batch in relations.layers.chunks(BATCH_SIZE) {
                    let parameters: Vec<_> = (1..=batch.len()).map(uuid).collect();
                    let order = parameters.iter().enumerate()
                        .map(|(index, parameter)| format!("WHEN {parameter} THEN {index}"))
                        .collect::<Vec<_>>().join(" ");
                    let mut query = sqlx::query_as::<$db, (String, String)>(AssertSqlSafe(format!(
                        "SELECT CAST(uuid AS TEXT), state FROM layers WHERE uuid IN ({}) ORDER BY CASE uuid {order} END{}",
                        parameters.join(", "), if $pg { " FOR SHARE" } else { "" },
                    )));
                    for layer in batch { query = query.bind(&layer.uuid); }
                    for (uuid, state) in query.fetch_all(&mut **transaction).await
                        .map_err(|error| MetaError::backend("read publication layers", error))? {
                        if state != "available" {
                            return Err(MetaError::Unavailable {
                                reason: format!("managed layer '{uuid}' is {state}"),
                            });
                        }
                        known.insert(uuid);
                    }
                }
                let missing: Vec<_> = relations.layers.iter()
                    .filter(|layer| !known.contains(&layer.uuid)).collect();
                for batch in missing.chunks(BATCH_SIZE) {
                    let values = (0..batch.len()).map(|index| format!(
                        "({}, {}, ${}, 'available')",
                        uuid(index * 3 + 1), uuid(index * 3 + 2), index * 3 + 3,
                    )).collect::<Vec<_>>().join(", ");
                    let mut query = sqlx::query_scalar::<$db, String>(AssertSqlSafe(format!(
                        "INSERT INTO layers (uuid, managed_parent_uuid, digest, state) VALUES {values} ON CONFLICT (uuid) DO NOTHING RETURNING CAST(uuid AS TEXT)"
                    )));
                    for layer in batch {
                        query = query.bind(&layer.uuid).bind(layer.parent.as_deref()).bind(&layer.digest);
                    }
                    let inserted = query.fetch_all(&mut **transaction).await
                        .map_err(|error| MetaError::backend("register publication layers", error))?;
                    // A concurrent insertion must be re-read under SHARE in a fresh transaction.
                    if inserted.len() != batch.len() { return Ok(false); }
                }
                let heads: Vec<_> = relations.heads.iter().collect();
                for batch in heads.chunks(BATCH_SIZE) {
                    let values = (0..batch.len()).map(|index| format!(
                        "({}, {})", uuid(1), uuid(index + 2),
                    )).collect::<Vec<_>>().join(", ");
                    let mut query = sqlx::query::<$db>(AssertSqlSafe(format!(
                        "INSERT INTO snapshot_heads (snapshot_id, layer_uuid) VALUES {values} ON CONFLICT DO NOTHING"
                    ))).bind(snapshot_id.to_string());
                    for head in batch { query = query.bind(head.as_str()); }
                    query.execute(&mut **transaction).await
                        .map_err(|error| MetaError::backend("retain snapshot heads", error))?;
                }
                Ok(true)
            }

            pub(super) async fn register(
                pool: &$pool,
                requested: &SnapshotEntry,
                metadata: &Value,
                encoded: &str,
                relations: &LayerRelations,
            ) -> MetaResult<SnapshotEntry> {
                let placement = match requested.snapshot_type.expect("registered snapshot has a placement") {
                    SnapshotPlacement::Local => "local",
                    SnapshotPlacement::Distributed => "distributed",
                };
                loop {
                    let mut transaction = pool.begin_with($begin).await
                        .map_err(|error| MetaError::backend("begin snapshot registration", error))?;
                    let inserted = sqlx::query_as::<$db, SnapshotRow>(AssertSqlSafe(format!(
                        "INSERT INTO snapshots (snapshot_id, alias, snapshot_type, committed_digest, metadata, retired, created_at) VALUES ({}, $2, $3, $4, {}, FALSE, $6) ON CONFLICT DO NOTHING RETURNING {SNAPSHOT_COLUMNS}",
                        uuid(1), if $pg { "CAST($5 AS JSON)" } else { "$5" },
                    )))
                    .bind(requested.snapshot_id.to_string())
                    .bind(requested.alias.as_ref().map(AsRef::as_ref))
                    .bind(placement)
                    .bind(requested.committed_digest.as_deref())
                    .bind(encoded)
                    .bind(requested.created_at)
                    .fetch_optional(&mut *transaction).await
                    .map_err(|error| MetaError::backend("register snapshot identity", error))?;
                    let stored = if let Some(row) = inserted {
                        if !attach_layers(&mut transaction, &requested.snapshot_id, relations).await? {
                            transaction.rollback().await
                                .map_err(|error| MetaError::backend("restart publication layer checks", error))?;
                            continue;
                        }
                        row.into_entry()?
                    } else {
                        let existing = sqlx::query_as::<$db, SnapshotRow>(AssertSqlSafe(format!(
                            "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = {}", uuid(1),
                        )))
                        .bind(requested.snapshot_id.to_string())
                        .fetch_optional(&mut *transaction).await
                        .map_err(|error| MetaError::backend("read conflicting snapshot", error))?;
                        if let Some(row) = existing {
                            row.registered(requested, metadata)?
                        } else {
                            let (id, alias): (String, String) = sqlx::query_as(
                                "SELECT CAST(snapshot_id AS TEXT), alias FROM snapshots WHERE alias = $1"
                            )
                            .bind(requested.alias.as_ref().map(AsRef::as_ref))
                            .fetch_one(&mut *transaction).await
                            .map_err(|error| MetaError::backend("read conflicting alias", error))?;
                            let existing = SnapshotId::parse(&id)
                                .map_err(|error| MetaError::backend("decode alias owner", error))?;
                            return Err(MetaError::AliasConflict {
                                alias, existing, new_id: requested.snapshot_id.clone(),
                            });
                        }
                    };
                    transaction.commit().await
                        .map_err(|error| MetaError::backend("commit snapshot registration", error))?;
                    return Ok(stored);
                }
            }

            pub(super) async fn get(pool: &$pool, reference: &str) -> MetaResult<Option<SnapshotEntry>> {
                if let Ok(id) = SnapshotId::parse(reference) {
                    let row = sqlx::query_as::<$db, SnapshotRow>(AssertSqlSafe(format!(
                        "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE snapshot_id = {}", uuid(1),
                    )))
                    .bind(id.to_string())
                    .fetch_optional(pool).await
                    .map_err(|error| MetaError::backend("get snapshot by ID", error))?;
                    if let Some(row) = row { return row.into_entry().map(Some); }
                }
                let alias = SnapshotAlias::parse(reference)
                    .map_err(|error| MetaError::InvalidRequest { reason: error.to_string() })?;
                sqlx::query_as::<$db, SnapshotRow>(AssertSqlSafe(format!(
                    "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE alias = $1"
                )))
                .bind(alias.as_ref())
                .fetch_optional(pool).await
                .map_err(|error| MetaError::backend("get snapshot by alias", error))?
                .map(SnapshotRow::into_entry).transpose()
            }

            pub(super) async fn list(pool: &$pool) -> MetaResult<Vec<SnapshotEntry>> {
                sqlx::query_as::<$db, SnapshotRow>(AssertSqlSafe(format!(
                    "SELECT {SNAPSHOT_COLUMNS} FROM snapshots WHERE NOT retired AND snapshot_type <> 'none' ORDER BY created_at DESC, snapshot_id ASC"
                )))
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
