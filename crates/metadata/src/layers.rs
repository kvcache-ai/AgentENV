//! Shared managed-layer prefix forest (`layers`) and resource head references (`layer_heads`).

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use agentenv_domain::{
    CommittedAttachedDrive, LayerRef, ManagedLayerRef, ResourceKind, SnapshotPlacement,
    SnapshotRecord,
};
use uuid::Uuid;

use crate::db::{DbTx, MetaSqlExt, Sql};
use crate::{MetaError, MetaResult};

const BATCH_SIZE: usize = 128;

struct LayerNode {
    parent: Option<Uuid>,
    digest: String,
    depth: usize,
}

#[derive(Default)]
pub(crate) struct LayerRelations {
    layers: BTreeMap<Uuid, LayerNode>,
    heads: BTreeSet<Uuid>,
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
    let mut relations = LayerRelations::default();
    for chain in chains {
        let mut parent = None;
        for (depth, layer) in chain.into_iter().enumerate() {
            let uuid = layer.directory_uuid().map_err(MetaError::invalid)?;
            // Legacy sealed bytes with no physical UUID can appear on unrelated branches.
            let legacy = layer
                .uuid
                .as_deref()
                .is_none_or(|raw| Uuid::parse_str(raw).is_ok_and(|u| u.is_nil()));
            let predecessor = (!legacy).then_some(parent).flatten();
            if layer.digest.is_empty() {
                return Err(MetaError::invalid(
                    "managed layer requires a canonical digest",
                ));
            }
            if let Some(existing) = relations.layers.get(&uuid) {
                if existing.parent != predecessor || existing.digest != layer.digest {
                    return Err(MetaError::invalid(format!(
                        "managed layer '{uuid}' has conflicting publication inputs"
                    )));
                }
            } else {
                relations.layers.insert(
                    uuid,
                    LayerNode {
                        parent: predecessor,
                        digest: layer.digest.clone(),
                        depth,
                    },
                );
            }
            if legacy {
                relations.heads.insert(uuid);
            } else {
                parent = Some(uuid);
            }
        }
        if let Some(head) = parent {
            relations.heads.insert(head);
        }
    }
    Ok(relations)
}

pub(crate) fn collect_snapshot_relations(record: &SnapshotRecord) -> MetaResult<LayerRelations> {
    let volumes = record
        .config
        .volume_snapshots
        .iter()
        .map(|v| managed_refs(&v.layers));
    match record.snapshot_type {
        SnapshotPlacement::Local => collect_chains(volumes),
        SnapshotPlacement::Distributed => {
            let manifest = record.manifest.as_ref().ok_or_else(|| {
                MetaError::invalid("Distributed snapshots require a published manifest")
            })?;
            let drives = manifest.attached_drives.iter().map(|d| {
                let CommittedAttachedDrive::Overlaybd { layers, .. } = d;
                managed_refs(layers)
            });
            collect_chains(
                [
                    managed_refs(&manifest.rootfs_layers),
                    manifest.memory_layers.iter().collect(),
                ]
                .into_iter()
                .chain(drives)
                .chain(volumes),
            )
        }
    }
}

pub(crate) fn collect_disk_relations(layers: &[LayerRef]) -> MetaResult<LayerRelations> {
    collect_chains([managed_refs(layers)])
}

pub(crate) async fn attach_layers(
    tx: &mut DbTx<'_>,
    owner_kind: ResourceKind,
    owner_id: &str,
    relations: &LayerRelations,
    restored_layers: &[String],
) -> MetaResult<bool> {
    let restored: BTreeSet<Uuid> = restored_layers
        .iter()
        .filter_map(|raw| Uuid::parse_str(raw).ok())
        .collect();
    let mut known = BTreeSet::new();
    let mut missing_uuids = Vec::new();
    let mut restored_uuids = Vec::new();
    // Lock existing rows in GC's UUID order, including across batches.
    let layer_ids: Vec<Uuid> = relations.layers.keys().copied().collect();
    for batch in layer_ids.chunks(BATCH_SIZE) {
        let mut q =
            Sql::new("SELECT uuid, state, digest, managed_parent_uuid FROM layers WHERE uuid IN (");
        q.push_in(batch).push(") ORDER BY uuid");
        let q = if restored.is_empty() {
            q.for_share()
        } else {
            q.for_update()
        };
        for row in tx.fetch_all(q).await.meta_err("read publication layers")? {
            let uuid = row.uuid(0)?;
            let state: String = row.get(1)?;
            let digest: String = row.get(2)?;
            let parent = row.opt_uuid(3)?;
            let layer = &relations.layers[&uuid];
            if layer.digest != digest || layer.parent != parent {
                return Err(MetaError::invalid(format!(
                    "managed layer '{uuid}' has conflicting publication inputs"
                )));
            }
            match state.as_str() {
                "available" => {
                    known.insert(uuid);
                }
                "deleted" if restored.contains(&uuid) => {
                    known.insert(uuid);
                    restored_uuids.push(uuid);
                }
                "deleted" => missing_uuids.push(uuid.to_string()),
                _ => {
                    return Err(MetaError::unavailable(format!(
                        "managed layer '{uuid}' is {state}"
                    )))
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
        let mut q = Sql::new("UPDATE layers SET state = 'available' WHERE uuid IN (");
        q.push_in(batch).push(")");
        tx.execute(q).await.meta_err("restore publication layers")?;
    }
    let mut missing: Vec<_> = relations
        .layers
        .iter()
        .filter(|(uuid, _)| !known.contains(*uuid))
        .collect();
    missing.sort_unstable_by_key(|&(uuid, layer)| (layer.depth, *uuid));
    for batch in missing.chunks(BATCH_SIZE) {
        let mut q =
            Sql::new("INSERT INTO layers (uuid, managed_parent_uuid, digest, state) VALUES ");
        q.push_sep(", ", batch, |q, (uuid, layer)| {
            q.push("(")
                .push_bind(**uuid)
                .push(", ")
                .push_bind(layer.parent)
                .push(", ")
                .push_bind(layer.digest.as_str())
                .push(", 'available')");
        })
        .push(" ON CONFLICT (uuid) DO NOTHING");
        // Restart to lock concurrent insertions in the same order as existing rows.
        if tx
            .execute(q)
            .await
            .meta_err("register publication layers")?
            != batch.len() as u64
        {
            return Ok(false);
        }
    }
    let heads: Vec<Uuid> = relations.heads.iter().copied().collect();
    for batch in heads.chunks(BATCH_SIZE) {
        let mut q = Sql::new(
            "INSERT INTO layer_heads (owner_kind, owner_id, layer_uuid, released) VALUES ",
        );
        q.push_sep(", ", batch, |q, head| {
            q.push("(")
                .push_bind(owner_kind.as_str())
                .push(", ")
                .push_bind(owner_id)
                .push(", ")
                .push_bind(*head)
                .push(", FALSE)");
        })
        .push(" ON CONFLICT (owner_kind, owner_id, layer_uuid) DO UPDATE SET released = FALSE");
        tx.execute(q).await.meta_err("retain layer heads")?;
    }
    Ok(true)
}

pub(crate) async fn sync_owner_heads(
    tx: &mut DbTx<'_>,
    owner_kind: ResourceKind,
    owner_id: &str,
    relations: &LayerRelations,
    restored_layers: &[String],
) -> MetaResult<bool> {
    if !attach_layers(tx, owner_kind, owner_id, relations, restored_layers).await? {
        return Ok(false);
    }
    if relations.heads.is_empty() {
        release_owner_heads(tx, owner_kind, owner_id).await?;
    } else {
        let mut q = Sql::new("UPDATE layer_heads SET released = TRUE WHERE owner_kind = ");
        q.push_bind(owner_kind.as_str())
            .push(" AND owner_id = ")
            .push_bind(owner_id)
            .push(" AND NOT released AND layer_uuid NOT IN (")
            .push_in(relations.heads.iter().copied())
            .push(")");
        tx.execute(q).await.meta_err("release superseded heads")?;
    }
    Ok(true)
}

pub(crate) async fn release_owner_heads(
    tx: &mut DbTx<'_>,
    owner_kind: ResourceKind,
    owner_id: &str,
) -> MetaResult<()> {
    tx.execute(
        Sql::new(
            "UPDATE layer_heads SET released = TRUE
             WHERE owner_kind = $1 AND owner_id = $2 AND NOT released",
        )
        .bind(owner_kind.as_str())
        .bind(owner_id),
    )
    .await
    .meta_err("release resource layer heads")?;
    Ok(())
}
