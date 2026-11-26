pub(crate) mod acr;
pub mod backends;
pub mod errors;
pub mod interfaces;
mod layers;
mod layout;

use agentenv_domain::{
    PersistedDiskImagePublication, SnapshotConfig, SnapshotId, SnapshotManifest, SnapshotRecord,
};

pub(crate) use backends::managed_layers_repo_blob_url_from_config;
pub use backends::open_from_config;
pub use errors::{RepositoryError, RepositoryResult};
pub use interfaces::{BlobBody, BlobLocation, BlobMetadata, LayerRole, PublishPolicy, Repository};
pub(crate) use layers::{
    layer_chain_for_refs, materialize_layer_config, restore_missing_managed_layers,
    same_repo_blob_url, store_disk_layers_with_uploads, RepositoryLayerWriter,
};
pub(crate) use layout::build_log_path;
pub use layout::{BlobKey, LegacyDocumentCollection, LegacyDocumentKey, SnapshotFile};

#[derive(serde::Deserialize, serde::Serialize)]
struct SnapshotContentWire {
    config: SnapshotConfig,
    manifest: Option<SnapshotManifest>,
}

/// Encodes a snapshot's immutable `snapshot.json` content payload.
pub(crate) fn encode_snapshot_content(record: &SnapshotRecord) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec_pretty(&SnapshotContentWire {
        config: record.config.clone(),
        manifest: record.manifest.clone(),
    })
}

/// Reads a snapshot's immutable `snapshot.json` content blob from `repository`.
pub(crate) async fn read_snapshot_content(
    repository: &dyn Repository,
    id: &SnapshotId,
) -> RepositoryResult<Option<(SnapshotConfig, Option<SnapshotManifest>)>> {
    let key = BlobKey::Snapshot(id.clone(), SnapshotFile::Record);
    let bytes = match repository.read_blob(&key).await {
        Ok(bytes) => bytes,
        Err(RepositoryError::ArtifactNotFound { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };
    let parse_error =
        |error| RepositoryError::backend(format!("parse snapshot content '{id}'"), error);
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(parse_error)?;
    if value.get("metadata").is_some() || value.get("id").is_some() {
        let record: SnapshotRecord = serde_json::from_value(value).map_err(parse_error)?;
        if &record.id != id {
            return Err(RepositoryError::Unavailable {
                reason: format!("snapshot record '{id}' has the wrong ID"),
            });
        }
        return Ok(Some((record.config, record.manifest)));
    }
    let content: SnapshotContentWire = serde_json::from_value(value).map_err(parse_error)?;
    Ok(Some((content.config, content.manifest)))
}

/// Removes one snapshot's repository blobs and, best-effort, its source-registry
/// publications. Shared managed layers stay; other resources may reference them.
pub(crate) async fn delete_snapshot_artifacts<R>(
    repository: &R,
    id: &SnapshotId,
    publications: &[PersistedDiskImagePublication],
) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    acr::rollback_disk_publications(id, publications).await;
    repository.delete_snapshot_blobs(id).await
}
