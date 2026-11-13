//! Physical collection of retired snapshot records and unreferenced layers.

use agentenv_metadata::{context::current as meta_context, MetaClient};

use super::metadata_catalog::{map_meta_error, map_rpc_error};
use crate::repository::{BlobKey, LayerRole, Repository, RepositoryResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GcReport {
    pub reaped_snapshots: usize,
    pub deleted_layers: usize,
}

pub async fn run_gc_once(
    meta: &MetaClient,
    repository: &dyn Repository,
    limit: u32,
) -> RepositoryResult<GcReport> {
    let retired = meta
        .reap_retired_snapshots(meta_context(), limit)
        .await
        .map_err(|error| map_rpc_error("scan retired snapshots for GC", error))?
        .map_err(map_meta_error)?;
    let mut reaped_snapshots = 0;
    let mut first_delete_error = None;
    for id in retired {
        let record = super::store::read_snapshot_record(repository, &id).await?;
        let publications = record
            .and_then(|record| record.manifest)
            .map(|manifest| manifest.disk_publications)
            .unwrap_or_default();
        if let Err(error) = super::store::delete_snapshot(repository, &id, &publications).await {
            first_delete_error.get_or_insert(error);
            continue;
        }
        let completed = meta
            .complete_reaped_snapshot(meta_context(), id)
            .await
            .map_err(|error| map_rpc_error("complete retired snapshot GC", error))?
            .map_err(map_meta_error)?;
        if completed {
            reaped_snapshots += 1;
        }
    }

    let candidates = meta
        .claim_deletable_layers(meta_context(), limit)
        .await
        .map_err(|error| map_rpc_error("claim deletable leaf layers", error))?
        .map_err(map_meta_error)?;
    let mut confirmed_layers = Vec::new();
    let mut reverted_layers = Vec::new();
    for layer in candidates {
        let key = BlobKey::SnapshotLayer {
            digest: layer.digest.clone(),
            role: LayerRole::Rootfs,
        };
        match repository.delete_snapshot_layer(&layer.digest).await {
            Ok(()) => confirmed_layers.push(layer.uuid),
            Err(error) => {
                if matches!(
                    repository.locate_blob(&key),
                    Ok(crate::repository::BlobLocation::Local(_))
                ) && repository.has_blob(&key).await.unwrap_or(false)
                {
                    reverted_layers.push(layer.uuid);
                }
                first_delete_error.get_or_insert(error);
            }
        }
    }
    let deleted_layers = confirmed_layers.len();
    if !confirmed_layers.is_empty() {
        meta.complete_deleted_layers(meta_context(), confirmed_layers)
            .await
            .map_err(|error| map_rpc_error("complete deleted leaf layers", error))?
            .map_err(map_meta_error)?;
    }
    if !reverted_layers.is_empty() {
        let _ = meta
            .revert_layer_deletions(meta_context(), reverted_layers)
            .await;
    }
    if let Some(error) = first_delete_error {
        return Err(error);
    }
    Ok(GcReport {
        reaped_snapshots,
        deleted_layers,
    })
}
