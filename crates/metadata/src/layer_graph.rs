//! Shared managed-layer DAG, `layer_heads`, and `gc_pins` operations.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use agentenv_domain::{
    is_sha256_digest, is_valid_volume_component, CommittedAttachedDrive, LayerRef, ManagedLayerRef,
    ResourceKind, SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotVolume,
};
use sqlx::{
    AssertSqlSafe, ColumnIndex, Decode, Encode, Executor, IntoArguments, Pool, QueryBuilder,
    Transaction, Type,
};
use uuid::Uuid;

use crate::database::{begin_tx, commit_tx, dispatch_db, QueryRows, SqlDialect};
use crate::{LayerCheck, LayerState, MetaError, MetaResult, MetaServer};

pub(crate) const BATCH_SIZE: usize = 128;

pub(crate) struct ManagedLayerNode {
    pub(crate) parent: Option<String>,
    pub(crate) digest: String,
    pub(crate) depth: usize,
}

#[derive(Default)]
pub(crate) struct LayerRelations {
    pub(crate) layers: BTreeMap<String, ManagedLayerNode>,
    pub(crate) heads: BTreeSet<String>,
}

pub(crate) fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub(crate) fn parse_non_nil_uuid(value: &str, label: &str) -> MetaResult<String> {
    Uuid::parse_str(value)
        .ok()
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.to_string())
        .ok_or_else(|| MetaError::InvalidRequest {
            reason: format!("{label} requires a non-nil UUID"),
        })
}

pub(crate) fn require_non_nil_uuid(value: Uuid, label: &str) -> MetaResult<String> {
    if value.is_nil() {
        return Err(MetaError::InvalidRequest {
            reason: format!("{label} requires a non-nil UUID"),
        });
    }
    Ok(value.to_string())
}

fn validate_target_id(kind: ResourceKind, target_id: &str) -> MetaResult<String> {
    match kind {
        ResourceKind::Snapshot => SnapshotId::parse(target_id)
            .map(|id| id.to_string())
            .map_err(|error| MetaError::InvalidRequest {
                reason: error.to_string(),
            }),
        ResourceKind::Volume => {
            if !is_valid_volume_component(target_id) {
                return Err(MetaError::InvalidRequest {
                    reason: format!("invalid volume id '{target_id}'"),
                });
            }
            Ok(target_id.to_owned())
        }
        ResourceKind::Image => {
            if !is_sha256_digest(target_id) {
                return Err(MetaError::InvalidRequest {
                    reason: format!("invalid image digest '{target_id}'"),
                });
            }
            Ok(target_id.to_owned())
        }
    }
}

fn managed_refs(layers: &[LayerRef]) -> Vec<&ManagedLayerRef> {
    layers
        .iter()
        .filter_map(|layer| match layer {
            LayerRef::Managed(layer) => Some(layer),
            LayerRef::External(_) => None,
        })
        .collect()
}

pub(crate) fn collect_chains<'a>(
    chains: impl IntoIterator<Item = Vec<&'a ManagedLayerRef>>,
) -> MetaResult<LayerRelations> {
    let mut layers: BTreeMap<String, ManagedLayerNode> = BTreeMap::new();
    let mut heads = BTreeSet::new();
    for chain in chains {
        let mut parent = None;
        for (depth, layer) in chain.into_iter().enumerate() {
            let uuid = layer
                .directory_uuid()
                .map_err(|reason| MetaError::InvalidRequest { reason })?
                .to_string();
            // Legacy sealed bytes with no physical UUID can appear on
            // unrelated branches. Keep one head per digest for such layers.
            let legacy = layer
                .uuid
                .as_deref()
                .is_none_or(|uuid| Uuid::parse_str(uuid).is_ok_and(|uuid| uuid.is_nil()));
            let predecessor = if legacy { None } else { parent.as_ref() };
            if layer.digest.is_empty() {
                return Err(MetaError::InvalidRequest {
                    reason: "managed layer requires a canonical digest".into(),
                });
            }
            if let Some(existing) = layers.get(&uuid) {
                if existing.parent.as_ref() != predecessor || existing.digest != layer.digest {
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
                        parent: predecessor.cloned(),
                        digest: layer.digest.clone(),
                        depth,
                    },
                );
            }
            if legacy {
                heads.insert(uuid);
            } else {
                parent = Some(uuid);
            }
        }
        if let Some(head) = parent {
            heads.insert(head);
        }
    }
    Ok(LayerRelations { layers, heads })
}

pub(crate) fn collect_snapshot_relations(
    placement: SnapshotPlacement,
    volume_snapshots: &[SnapshotVolume],
    manifest: &SnapshotManifest,
) -> MetaResult<LayerRelations> {
    let volume_chains = volume_snapshots
        .iter()
        .map(|volume| managed_refs(&volume.layers));
    match placement {
        SnapshotPlacement::Local => collect_chains(volume_chains),
        SnapshotPlacement::Distributed => {
            let chains = [
                managed_refs(&manifest.rootfs_layers),
                manifest.memory_layers.iter().collect(),
            ]
            .into_iter()
            .chain(manifest.attached_drives.iter().map(|drive| {
                let CommittedAttachedDrive::Overlaybd { layers, .. } = drive;
                managed_refs(layers)
            }))
            .chain(volume_chains);
            collect_chains(chains)
        }
    }
}

pub(crate) fn collect_disk_relations(layers: &[LayerRef]) -> MetaResult<LayerRelations> {
    collect_chains([managed_refs(layers)])
}

fn push_uuid_in_list<'a, D: SqlDialect>(
    query: &mut QueryBuilder<D>,
    uuids: impl IntoIterator<Item = &'a str>,
) where
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> String: Encode<'b, D> + Type<D>,
{
    let mut sep = query.separated(", ");
    for id in uuids {
        sep.push_bind(id.to_owned()).push_unseparated(D::UUID_CAST);
    }
}

impl MetaServer {
    pub async fn check_layers(&self, uuids: &[String]) -> MetaResult<Vec<LayerCheck>> {
        let normalized: Vec<String> = uuids
            .iter()
            .map(|uuid| parse_non_nil_uuid(uuid, "managed layer"))
            .collect::<MetaResult<_>>()?;
        dispatch_db!(&self.database, |pool| check_layers_db(pool, &normalized)
            .await)
    }

    /// Pin any live resource (`snapshot`, `volume`, or `image`) against GC.
    pub async fn pin_resource(
        &self,
        kind: ResourceKind,
        target_id: &str,
        pin_id: Uuid,
        owner_node_id: Option<String>,
    ) -> MetaResult<bool> {
        let target_id = validate_target_id(kind, target_id)?;
        let pin_id = require_non_nil_uuid(pin_id, "pin ID")?;
        dispatch_db!(&self.database, |pool| {
            pin_resource_db(pool, kind, &target_id, &pin_id, owner_node_id.as_deref()).await
        })
    }

    /// Release a pin or record a tombstone so a late pin cannot reopen it.
    pub async fn release_pin(
        &self,
        kind: ResourceKind,
        target_id: &str,
        pin_id: Uuid,
    ) -> MetaResult<()> {
        let target_id = validate_target_id(kind, target_id)?;
        let pin_id = require_non_nil_uuid(pin_id, "pin ID")?;
        dispatch_db!(&self.database, |pool| {
            release_pin_db(pool, kind, &target_id, &pin_id).await
        })
    }
}

async fn check_layers_db<D>(pool: &Pool<D>, normalized: &[String]) -> MetaResult<Vec<LayerCheck>>
where
    D: SqlDialect<Arguments: IntoArguments<D>>,
    for<'p> &'p Pool<D>: Executor<'p, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> String: Encode<'b, D> + Decode<'b, D> + Type<D>,
    usize: ColumnIndex<D::Row>,
{
    let mut states = BTreeMap::new();
    for batch in normalized.chunks(BATCH_SIZE) {
        let mut query =
            QueryBuilder::<D>::new("SELECT CAST(uuid AS TEXT), state FROM layers WHERE uuid IN (");
        push_uuid_in_list(&mut query, batch.iter().map(String::as_str));
        let rows = query
            .push(")")
            .build_query_as::<(String, String)>()
            .fetch_all(pool)
            .await
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
        .iter()
        .map(|uuid| LayerCheck {
            state: states.get(uuid).copied().unwrap_or(LayerState::Missing),
            uuid: uuid.clone(),
        })
        .collect())
}

pub(crate) async fn attach_layers<D>(
    tx: &mut Transaction<'_, D>,
    owner_kind: ResourceKind,
    owner_id: &str,
    relations: &LayerRelations,
    restored_layers: &[String],
) -> MetaResult<bool>
where
    D: SqlDialect<QueryResult: QueryRows, Arguments: IntoArguments<D>>,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> Option<&'b str>: Encode<'b, D> + Type<D>,
    for<'b> String: Encode<'b, D> + Decode<'b, D> + Type<D>,
    for<'b> Option<String>: Decode<'b, D> + Type<D>,
    usize: ColumnIndex<D::Row>,
{
    let restored: BTreeSet<&str> = restored_layers.iter().map(String::as_str).collect();
    let mut known = BTreeSet::new();
    let mut missing_uuids = Vec::new();
    let mut restored_uuids = Vec::new();
    // Lock existing rows in GC's UUID order, including across batches.
    let layer_ids: Vec<&str> = relations.layers.keys().map(String::as_str).collect();
    for batch in layer_ids.chunks(BATCH_SIZE) {
        let mut query = QueryBuilder::<D>::new(
            "SELECT CAST(l.uuid AS TEXT), l.state, l.digest, CAST(l.managed_parent_uuid AS TEXT) \
             FROM layers AS l WHERE l.uuid IN (",
        );
        push_uuid_in_list(&mut query, batch.iter().copied());
        query.push(") ORDER BY l.uuid");
        query.push(if restored.is_empty() {
            D::FOR_SHARE
        } else {
            D::FOR_UPDATE
        });
        let rows = query
            .build_query_as::<(String, String, String, Option<String>)>()
            .fetch_all(&mut **tx)
            .await
            .map_err(|error| MetaError::backend("read publication layers", error))?;
        for (uuid, state, digest, parent) in rows {
            let layer = &relations.layers[&uuid];
            if layer.digest != digest || layer.parent != parent {
                return Err(MetaError::InvalidRequest {
                    reason: format!("managed layer '{uuid}' has conflicting publication inputs"),
                });
            }
            match state.as_str() {
                "available" => {
                    known.insert(uuid);
                }
                "deleted" if restored.contains(uuid.as_str()) => {
                    known.insert(uuid.clone());
                    restored_uuids.push(uuid);
                }
                "deleted" => missing_uuids.push(uuid),
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
    for batch in restored_uuids.chunks(BATCH_SIZE) {
        let mut query =
            QueryBuilder::<D>::new("UPDATE layers SET state = 'available' WHERE uuid IN (");
        push_uuid_in_list(&mut query, batch.iter().map(String::as_str));
        query
            .push(")")
            .build()
            .execute(&mut **tx)
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
        let mut query = QueryBuilder::<D>::new(
            "INSERT INTO layers (uuid, managed_parent_uuid, digest, state) ",
        );
        query.push_values(batch, |mut row, (uuid, layer)| {
            row.push_bind(uuid.as_str()).push_unseparated(D::UUID_CAST);
            row.push_bind(layer.parent.as_deref())
                .push_unseparated(D::UUID_CAST);
            row.push_bind(layer.digest.as_str()).push("'available'");
        });
        let inserted = query
            .push(" ON CONFLICT (uuid) DO NOTHING")
            .build()
            .execute(&mut **tx)
            .await
            .map_err(|error| MetaError::backend("register publication layers", error))?;
        // Restart to lock concurrent insertions in the same order as existing rows.
        if inserted.rows_affected() != batch.len() as u64 {
            return Ok(false);
        }
    }
    let owner_kind_str = owner_kind.as_str();
    let heads: Vec<&str> = relations.heads.iter().map(String::as_str).collect();
    for batch in heads.chunks(BATCH_SIZE) {
        let mut query = QueryBuilder::<D>::new(
            "INSERT INTO layer_heads (owner_kind, owner_id, layer_uuid, released) ",
        );
        query.push_values(batch, |mut row, head| {
            row.push_bind(owner_kind_str);
            row.push_bind(owner_id);
            row.push_bind(*head).push_unseparated(D::UUID_CAST);
            row.push("FALSE");
        });
        query
            .push(" ON CONFLICT (owner_kind, owner_id, layer_uuid) DO UPDATE SET released = FALSE")
            .build()
            .execute(&mut **tx)
            .await
            .map_err(|error| MetaError::backend("retain layer heads", error))?;
    }
    Ok(true)
}

pub(crate) async fn sync_owner_heads<D>(
    tx: &mut Transaction<'_, D>,
    owner_kind: ResourceKind,
    owner_id: &str,
    relations: &LayerRelations,
    restored_layers: &[String],
) -> MetaResult<bool>
where
    D: SqlDialect<QueryResult: QueryRows, Arguments: IntoArguments<D>>,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> Option<&'b str>: Encode<'b, D> + Type<D>,
    for<'b> String: Encode<'b, D> + Decode<'b, D> + Type<D>,
    for<'b> Option<String>: Decode<'b, D> + Type<D>,
    usize: ColumnIndex<D::Row>,
{
    if !attach_layers(tx, owner_kind, owner_id, relations, restored_layers).await? {
        return Ok(false);
    }
    let owner_kind_str = owner_kind.as_str();
    if relations.heads.is_empty() {
        release_owner_heads(tx, owner_kind, owner_id).await?;
    } else {
        let mut query =
            QueryBuilder::<D>::new("UPDATE layer_heads SET released = TRUE WHERE owner_kind = ");
        query
            .push_bind(owner_kind_str)
            .push(" AND owner_id = ")
            .push_bind(owner_id)
            .push(" AND NOT released AND layer_uuid NOT IN (");
        push_uuid_in_list(&mut query, relations.heads.iter().map(String::as_str));
        query
            .push(")")
            .build()
            .execute(&mut **tx)
            .await
            .map_err(|error| MetaError::backend("release superseded heads", error))?;
    }
    Ok(true)
}

pub(crate) async fn release_owner_heads<D>(
    tx: &mut Transaction<'_, D>,
    owner_kind: ResourceKind,
    owner_id: &str,
) -> MetaResult<()>
where
    D: SqlDialect<Arguments: IntoArguments<D>>,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
{
    sqlx::query::<D>(
        "UPDATE layer_heads SET released = TRUE
         WHERE owner_kind = $1 AND owner_id = $2 AND NOT released",
    )
    .bind(owner_kind.as_str())
    .bind(owner_id)
    .execute(&mut **tx)
    .await
    .map_err(|error| MetaError::backend("release resource layer heads", error))?;
    Ok(())
}

/// Verify and record a pin row inside an active transaction that has already
/// locked the target resource row (`FOR SHARE` on PG).
pub(crate) async fn upsert_pin_in_tx<D>(
    tx: &mut Transaction<'_, D>,
    kind: ResourceKind,
    target_id: &str,
    pin_id: &str,
    owner_node_id: Option<&str>,
    target_active: bool,
    inactive_reason: &str,
) -> MetaResult<()>
where
    D: SqlDialect<Arguments: IntoArguments<D>>,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> Option<&'b str>: Encode<'b, D> + Type<D>,
    for<'b> String: Decode<'b, D> + Type<D>,
    for<'b> bool: Decode<'b, D> + Type<D>,
    usize: ColumnIndex<D::Row>,
{
    let kind_str = kind.as_str();
    if target_active {
        sqlx::query::<D>(AssertSqlSafe(format!(
            "INSERT INTO gc_pins (pin_id, target_kind, target_id, owner_node_id, released)
             VALUES ({}, $2, $3, $4, FALSE)
             ON CONFLICT (pin_id) DO NOTHING",
            D::uuid(1),
        )))
        .bind(pin_id)
        .bind(kind_str)
        .bind(target_id)
        .bind(owner_node_id)
        .execute(&mut **tx)
        .await
        .map_err(|error| MetaError::backend("insert resource pin", error))?;
    }
    let stored_pin = sqlx::query_as::<D, (String, String, bool)>(AssertSqlSafe(format!(
        "SELECT target_kind, target_id, released FROM gc_pins WHERE pin_id = {}{}",
        D::uuid(1),
        D::FOR_UPDATE,
    )))
    .bind(pin_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| MetaError::backend("read resource pin", error))?;
    let Some((bound_kind, bound_target, released)) = stored_pin else {
        return Err(MetaError::Unavailable {
            reason: inactive_reason.to_owned(),
        });
    };
    if bound_kind != kind_str || bound_target != target_id {
        return Err(MetaError::InvalidRequest {
            reason: format!("pin '{pin_id}' is already bound to {bound_kind} '{bound_target}'"),
        });
    }
    if released {
        return Err(MetaError::Unavailable {
            reason: format!("pin '{pin_id}' has already been released"),
        });
    }
    Ok(())
}

async fn pin_resource_db<D>(
    pool: &Pool<D>,
    kind: ResourceKind,
    target_id: &str,
    pin_id: &str,
    owner_node_id: Option<&str>,
) -> MetaResult<bool>
where
    D: SqlDialect<Arguments: IntoArguments<D>>,
    for<'c> &'c mut D::Connection: Executor<'c, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> Option<&'b str>: Encode<'b, D> + Type<D>,
    for<'b> String: Decode<'b, D> + Type<D>,
    for<'b> bool: Decode<'b, D> + Type<D>,
    usize: ColumnIndex<D::Row>,
{
    let mut tx = begin_tx(pool, "begin resource pin").await?;
    let (target_active, inactive_reason) = match kind {
        ResourceKind::Snapshot => {
            let state: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT state FROM snapshots WHERE snapshot_id = {}{}",
                D::uuid(1),
                D::FOR_SHARE,
            )))
            .bind(target_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("lock snapshot for pin", error))?;
            let Some(state) = state else {
                return Ok(false);
            };
            if state == "reserved" || state == "reaped" {
                return Err(MetaError::Unavailable {
                    reason: format!("snapshot '{target_id}' is {state}"),
                });
            }
            (
                state == "active",
                format!("snapshot '{target_id}' is retired"),
            )
        }
        ResourceKind::Volume => {
            let status: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT status FROM volumes WHERE volume_id = $1{}",
                D::FOR_SHARE,
            )))
            .bind(target_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("lock volume for pin", error))?;
            if status.is_none() {
                return Ok(false);
            }
            (true, format!("volume '{target_id}' is unavailable"))
        }
        ResourceKind::Image => {
            let retired: Option<bool> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT retired FROM images WHERE digest = $1{}",
                D::FOR_SHARE,
            )))
            .bind(target_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("lock image for pin", error))?;
            let Some(retired) = retired else {
                return Ok(false);
            };
            (!retired, format!("image '{target_id}' is retired"))
        }
    };
    upsert_pin_in_tx(
        &mut tx,
        kind,
        target_id,
        pin_id,
        owner_node_id,
        target_active,
        &inactive_reason,
    )
    .await?;
    commit_tx(tx, "commit resource pin").await?;
    Ok(true)
}

async fn release_pin_db<D>(
    pool: &Pool<D>,
    kind: ResourceKind,
    target_id: &str,
    pin_id: &str,
) -> MetaResult<()>
where
    D: SqlDialect<QueryResult: QueryRows, Arguments: IntoArguments<D>>,
    for<'p> &'p Pool<D>: Executor<'p, Database = D>,
    for<'b> &'b str: Encode<'b, D> + Type<D>,
    for<'b> String: Decode<'b, D> + Type<D>,
    usize: ColumnIndex<D::Row>,
{
    let updated = sqlx::query::<D>(AssertSqlSafe(format!(
        "INSERT INTO gc_pins (pin_id, target_kind, target_id, released)
         VALUES ({}, $2, $3, TRUE)
         ON CONFLICT (pin_id) DO UPDATE SET released = TRUE
         WHERE gc_pins.target_kind = EXCLUDED.target_kind
           AND gc_pins.target_id = EXCLUDED.target_id",
        D::uuid(1),
    )))
    .bind(pin_id)
    .bind(kind.as_str())
    .bind(target_id)
    .execute(pool)
    .await
    .map_err(|error| MetaError::backend("release resource pin", error))?;
    if updated.rows_affected() == 0 {
        let (bound_kind, bound_target): (String, String) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT target_kind, target_id FROM gc_pins WHERE pin_id = {}",
            D::uuid(1),
        )))
        .bind(pin_id)
        .fetch_one(pool)
        .await
        .map_err(|error| MetaError::backend("read conflicting resource pin", error))?;
        return Err(MetaError::InvalidRequest {
            reason: format!("pin '{pin_id}' is already bound to {bound_kind} '{bound_target}'"),
        });
    }
    Ok(())
}
