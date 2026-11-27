//! Immutable `snapshot.json` payload encoding/decoding and snapshot artifact
//! cleanup (including source-registry rollback).

use agentenv_domain::{
    PersistedDiskImagePublication, SnapshotConfig, SnapshotId, SnapshotManifest, SnapshotRecord,
};

use super::acr;
use crate::repository::{BlobKey, Repository, RepositoryError, RepositoryResult, SnapshotFile};

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
    let content: SnapshotContentWire = serde_json::from_slice(&bytes).map_err(|error| {
        RepositoryError::backend(format!("parse snapshot content '{id}'"), error)
    })?;
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
