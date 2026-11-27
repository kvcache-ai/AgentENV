//! Shared managed-layer prefix forest (`layers`) and resource head references (`layer_heads`).

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use agentenv_domain::{
    CommittedAttachedDrive, LayerRef, ManagedLayerRef, ResourceKind, SnapshotPlacement,
    SnapshotRecord,
};
use sqlx::{QueryBuilder, Transaction};
use uuid::Uuid;

use crate::database::{impl_sql_backend, Db, QueryRows};
use crate::{MetaError, MetaResult};

pub(crate) const BATCH_SIZE: usize = 128;

struct ManagedLayerNode {
    parent: Option<String>,
    digest: String,
    depth: usize,
}

#[derive(Default)]
pub(crate) struct LayerRelations {
    layers: BTreeMap<String, ManagedLayerNode>,
    heads: BTreeSet<String>,
}

pub(crate) fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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

fn collect_chains<'a>(
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

pub(crate) fn collect_snapshot_relations(record: &SnapshotRecord) -> MetaResult<LayerRelations> {
    let volume_chains = record
        .config
        .volume_snapshots
        .iter()
        .map(|volume| managed_refs(&volume.layers));
    match record.snapshot_type {
        SnapshotPlacement::Local => collect_chains(volume_chains),
        SnapshotPlacement::Distributed => {
            let manifest = record
                .manifest
                .as_ref()
                .ok_or_else(|| MetaError::InvalidRequest {
                    reason: "Distributed snapshots require a published manifest".into(),
                })?;
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

impl_sql_backend! {
impl<D> Db<'_, D> {
    fn push_uuid_in_list<'a>(
        query: &mut QueryBuilder<D>,
        uuids: impl IntoIterator<Item = &'a str>,
    ) {
        let mut sep = query.separated(", ");
        for id in uuids {
            sep.push_bind(id.to_owned()).push_unseparated(D::UUID_CAST);
        }
    }

    pub(crate) async fn attach_layers(
        tx: &mut Transaction<'_, D>,
        owner_kind: ResourceKind,
        owner_id: &str,
        relations: &LayerRelations,
        restored_layers: &[String],
    ) -> MetaResult<bool> {
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
            Self::push_uuid_in_list(&mut query, batch.iter().copied());
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
                        reason: format!(
                            "managed layer '{uuid}' has conflicting publication inputs"
                        ),
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
            Self::push_uuid_in_list(&mut query, batch.iter().map(String::as_str));
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
                .push(
                    " ON CONFLICT (owner_kind, owner_id, layer_uuid) DO UPDATE SET released = FALSE",
                )
                .build()
                .execute(&mut **tx)
                .await
                .map_err(|error| MetaError::backend("retain layer heads", error))?;
        }
        Ok(true)
    }

    pub(crate) async fn sync_owner_heads(
        tx: &mut Transaction<'_, D>,
        owner_kind: ResourceKind,
        owner_id: &str,
        relations: &LayerRelations,
        restored_layers: &[String],
    ) -> MetaResult<bool> {
        if !Self::attach_layers(tx, owner_kind, owner_id, relations, restored_layers).await? {
            return Ok(false);
        }
        let owner_kind_str = owner_kind.as_str();
        if relations.heads.is_empty() {
            Self::release_owner_heads(tx, owner_kind, owner_id).await?;
        } else {
            let mut query = QueryBuilder::<D>::new(
                "UPDATE layer_heads SET released = TRUE WHERE owner_kind = ",
            );
            query
                .push_bind(owner_kind_str)
                .push(" AND owner_id = ")
                .push_bind(owner_id)
                .push(" AND NOT released AND layer_uuid NOT IN (");
            Self::push_uuid_in_list(&mut query, relations.heads.iter().map(String::as_str));
            query
                .push(")")
                .build()
                .execute(&mut **tx)
                .await
                .map_err(|error| MetaError::backend("release superseded heads", error))?;
        }
        Ok(true)
    }

    pub(crate) async fn release_owner_heads(
        tx: &mut Transaction<'_, D>,
        owner_kind: ResourceKind,
        owner_id: &str,
    ) -> MetaResult<()> {
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
}
}
