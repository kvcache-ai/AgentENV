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

/// Decodes a pre-SQL snapshot record JSON (legacy flat `source`/`resources`/`committed`,
/// nested `metadata`, or `{"Template": {"build": ...}}`) into [`SnapshotRecord`].
pub(crate) fn decode_legacy_snapshot_record(
    mut value: serde_json::Value,
) -> serde_json::Result<SnapshotRecord> {
    if let Some(record) = value
        .as_object_mut()
        .filter(|record| !record.contains_key("id"))
    {
        if let Some(serde_json::Value::Object(mut metadata)) = record.remove("metadata") {
            for field in [
                "id",
                "snapshot_type",
                "owner_node_id",
                "alias",
                "created_at_unix_ms",
                "updated_at_unix_ms",
            ] {
                if let Some(v) = metadata.remove(field) {
                    record.insert(field.into(), v);
                }
            }
            if !record.contains_key("snapshot_type") {
                if let Some(placement) = metadata.remove("placement").filter(|v| !v.is_null()) {
                    record.insert("snapshot_type".into(), placement);
                }
            }
        }
    }
    if let Some(record) = value
        .as_object_mut()
        .filter(|record| !record.contains_key("config"))
    {
        let mut config = serde_json::Map::new();
        for key in ["source", "resources"] {
            if let Some(v) = record.remove(key) {
                config.insert(key.into(), v);
            }
        }
        if let Some(committed) = record
            .get_mut("committed")
            .and_then(serde_json::Value::as_object_mut)
        {
            for key in [
                "context",
                "startup",
                "runtime_versions",
                "virtualization_mode",
                "image_configs",
                "volume_snapshots",
                "custom_extension_params",
            ] {
                if let Some(v) = committed.remove(key) {
                    config.insert(key.into(), v);
                }
            }
        }
        if record
            .get("committed")
            .is_none_or(serde_json::Value::is_null)
            && config
                .get("source")
                .and_then(|source| source.get("Template"))
                .is_some()
        {
            config.insert(
                "context".into(),
                serde_json::to_value(agentenv_domain::CommandContext::default())?,
            );
            config.insert(
                "runtime_versions".into(),
                serde_json::to_value(agentenv_domain::SnapshotRuntimeVersions::default())?,
            );
        }
        if record
            .get("snapshot_type")
            .and_then(serde_json::Value::as_str)
            == Some("local")
        {
            record.insert("committed".into(), serde_json::Value::Null);
        }
        record.insert("config".into(), serde_json::Value::Object(config));
    }
    if let Some(record) = value.as_object_mut() {
        if let Some(config) = record
            .get_mut("config")
            .and_then(serde_json::Value::as_object_mut)
        {
            if config
                .get("source")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|map| map.len() == 1 && map.contains_key("Template"))
            {
                config.insert(
                    "source".into(),
                    serde_json::Value::String("Template".into()),
                );
            }
        }
        if !record.contains_key("manifest") {
            let committed = record
                .remove("committed")
                .unwrap_or(serde_json::Value::Null);
            record.insert("manifest".into(), committed);
        }
    }
    serde_json::from_value(value)
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
        let record = decode_legacy_snapshot_record(value).map_err(parse_error)?;
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
