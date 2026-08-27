use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};
use overlaybd::config::{load_image_config as load_overlaybd_image_config, LayerConfig};
use overlaybd::dense_export;
use overlaybd::layer_metadata::read_overlaybd_layer_uuid;
use tracing::{debug, info, warn};

use super::client::{OssClient, OssUploadArtifact};
use super::layout::OssSnapshotArtifactLayout;
use crate::cfg::SnapshotImageStoragePolicy;
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::repository::backends::common::acr::{
    AcrDiskImageExporter, DiskImageExportOutcome, DiskImageSubject, SnapshotOciConfigInput,
};
use crate::snapshot::repository::backends::common::{
    materialize_volume_image_config, write_dense_overlaybd_layer_to_file,
};
use crate::snapshot::repository::interfaces::SnapshotRepository;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::{
    CommittedAttachedDrive, CommittedSnapshot, ExternalLayer, ManagedLayer, OverlaybdLayerRef,
    PersistedDiskImagePublication, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
    SnapshotSourceKind, TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
    SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::volume::{VolumeMode, VolumeRecord};

/// Manages the committed‐state layer of the OSS snapshot repository.
///
/// Object layout under the configured prefix:
///
/// ```text
/// catalog/aliases/{name}.json              → "snapshot-id"
/// artifacts/{id}/firecracker-manifest.json → FirecrackerSnapshotManifest (paths omitted)
/// artifacts/{id}/vm_state.bin
/// managed-layers/{digest}
/// ```
pub(crate) struct OssSnapshotRepository {
    client: Arc<OssClient>,
    snapshot_image_storage: SnapshotImageStoragePolicy,
    acr_exporter: AcrDiskImageExporter,
}

const MAX_ALIAS_BIND_ATTEMPTS: usize = 5;
const MAX_VOLUME_CAS_ATTEMPTS: usize = 5;

impl OssSnapshotRepository {
    pub(crate) fn new(
        client: Arc<OssClient>,
        snapshot_image_storage: SnapshotImageStoragePolicy,
    ) -> Self {
        Self {
            client,
            snapshot_image_storage,
            acr_exporter: AcrDiskImageExporter::new(),
        }
    }

    fn layout<'a>(&self, id: &'a SnapshotId) -> OssSnapshotArtifactLayout<'a> {
        OssSnapshotArtifactLayout::new(id)
    }

    async fn snapshot_exists(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        self.client
            .exists(&OssSnapshotArtifactLayout::record_key(id))
            .await
            .map_err(|e| RepositoryError::backend(format!("check snapshot record '{id}'"), e))
    }
}

fn validated_alias_key(alias: &str) -> RepositoryResult<String> {
    SnapshotAlias::parse(alias).map_err(|e| RepositoryError::InvalidRequest {
        reason: format!("invalid alias '{alias}': {e}"),
    })?;
    Ok(OssSnapshotArtifactLayout::alias_key(alias))
}

fn validate_volume_id(volume_id: &str) -> RepositoryResult<()> {
    if !volume_id.is_empty()
        && volume_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        Ok(())
    } else {
        Err(RepositoryError::InvalidRequest {
            reason: format!("invalid volume id '{volume_id}'"),
        })
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn same_repo_blob_url(left: &str, right: &str) -> bool {
    !left.is_empty() && left.trim_end_matches('/') == right.trim_end_matches('/')
}

fn managed_memory_layer_from_remote_lower(
    index: usize,
    layer: LayerConfig,
    repo_blob_url: &str,
    managed_layers_repo_blob_url: &str,
) -> RepositoryResult<ManagedLayer> {
    if !same_repo_blob_url(repo_blob_url, managed_layers_repo_blob_url) {
        return Err(RepositoryError::Unsupported {
            feature: format!("memory layer {index} uses non-OSS managed repoBlobUrl"),
        });
    }
    let digest = if !layer.digest.is_empty() {
        layer.digest
    } else if !layer.target_digest.is_empty() {
        layer.target_digest
    } else {
        return Err(RepositoryError::Unsupported {
            feature: format!("memory layer {index} without digest"),
        });
    };
    Ok(ManagedLayer {
        digest,
        size: layer.size,
        uuid: None,
    })
}

fn overlaybd_layer_uuid(source: &Path) -> Option<String> {
    read_overlaybd_layer_uuid(source)
        .ok()
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.to_string())
}

fn fallback_to_object_storage_would_mix_sources(
    image_config_path: &Path,
    managed_layers_repo_blob_url: &str,
) -> RepositoryResult<bool> {
    let image_config = load_overlaybd_image_config(image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "load overlaybd image config '{}'",
                image_config_path.display()
            ),
            e,
        )
    })?;
    Ok(image_config.lowers.iter().any(|layer| {
        layer.file.is_empty()
            && !same_repo_blob_url(
                layer.effective_repo_blob_url(&image_config.repo_blob_url),
                managed_layers_repo_blob_url,
            )
    }))
}

// ── SnapshotRepository impl ────────────────────────────────────────────

#[async_trait]
impl SnapshotRepository for OssSnapshotRepository {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        if !matches!(record.source, SnapshotSource::Template { .. }) {
            return Err(RepositoryError::InvalidRequest {
                reason: "only template snapshots can be pre-created".to_string(),
            });
        }
        if record.committed.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "pre-created template snapshots must not already be committed".to_string(),
            });
        }
        if self.snapshot_exists(&record.id).await? {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }
        if let Some(alias) = record.alias.as_ref() {
            if let Some(existing) = self.load_alias_target(alias.as_ref()).await? {
                if existing != record.id && self.snapshot_exists(&existing).await? {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing,
                        new_id: record.id.clone(),
                    });
                }
            }
        }
        self.write_record(&record).await?;
        if let Some(alias) = record.alias.as_ref() {
            if let Err(error) = self.bind_alias(alias.as_ref(), &record.id).await {
                let _ = self
                    .client
                    .delete(&OssSnapshotArtifactLayout::record_key(&record.id))
                    .await;
                return Err(error);
            }
        }
        Ok(record)
    }

    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        let id = &metadata.id;
        let layout = self.layout(id);

        // 0. Validate no duplicate drive ids.
        let mut drive_ids_set = HashSet::new();
        for drive in &manifest.attached_drives {
            if !drive_ids_set.insert(drive.drive_id.clone()) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "duplicate attached drive id in publish request: {}",
                        drive.drive_id
                    ),
                });
            }
            if drive.virtual_size == 0 {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "attached drive '{}' virtual_size must be non-zero",
                        drive.drive_id
                    ),
                });
            }
        }

        let mut disk_publications = Vec::new();

        let publish_result = async {
            validate_publish_manifest_image_configs(&manifest)?;

            // 1. Export rootfs disk image with the effective runtime config.
            let rootfs_config = SnapshotOciConfigInput::new(
                &metadata.context,
                metadata.image_configs.rootfs_config(),
            );
            let rootfs_outcome = self
                .export_disk_image(
                    id,
                    DiskImageSubject::Rootfs,
                    &manifest.rootfs.image_config_path,
                    Some(rootfs_config),
                )
                .await?;
            if let Some(publication) = rootfs_outcome.publication.clone() {
                disk_publications.push(publication);
            }
            let rootfs_layers = rootfs_outcome.layers;

            let memory_layers = self
                .derive_and_upload_memory_layers(&manifest.memory.image_config_path)
                .await?;

            // 2. Upload per-snapshot fixed artifacts.
            let vm_state_local_path = manifest.vm_state.path.as_path();
            self.client
                .put_file(
                    &layout.artifact_key(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
                    vm_state_local_path,
                    OssUploadArtifact::VmState,
                )
                .await
                .map_err(|e| {
                    RepositoryError::backend(
                        format!(
                            "upload artifact '{}' from '{}' for snapshot '{}'",
                            SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
                            vm_state_local_path.display(),
                            id
                        ),
                        e,
                    )
                })?;

            let persisted_manifest_bytes = serde_json::to_vec_pretty(&manifest)
                .map_err(|e| RepositoryError::backend("serialize firecracker manifest", e))?;
            self.client
                .put_bytes(
                    &layout.artifact_key(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest),
                    persisted_manifest_bytes,
                    OssUploadArtifact::FirecrackerManifest,
                )
                .await
                .map_err(|e| RepositoryError::backend("write firecracker manifest to oss", e))?;

            // 3. Export attached-drive disk images and derive their committed metadata.
            let attached_drives = self
                .export_attached_drives(id, &manifest, &mut disk_publications)
                .await?;

            // 4. Construct committed CommittedSnapshot.
            let committed = CommittedSnapshot {
                context: metadata.context.clone(),
                startup: metadata.startup.clone(),
                runtime_versions: metadata.runtime_versions.clone(),
                virtualization_mode: metadata.virtualization_mode,
                image_configs: metadata.image_configs.clone(),
                custom_extension_params: metadata.custom_extension_params.clone(),
                rootfs_layers,
                attached_drives,
                volume_snapshots: metadata.volume_snapshots.clone(),
                memory_layers,
                disk_publications: disk_publications.clone(),
            };

            // 5. Bind alias (if present) with conflict detection.
            if let Some(ref alias) = metadata.alias {
                if let Err(e) = self.bind_alias(alias.as_ref(), id).await {
                    // Best-effort rollback. Content-addressed managed layers are intentionally left
                    // in place; they are shared across snapshots and require separate GC.
                    if let Err(error) = self.client.delete_prefix(&layout.artifact_prefix()).await {
                        warn!(snapshot_id = %id, error = %error, "failed to roll back snapshot artifacts after alias bind failure");
                    }
                    return Err(e);
                }
            }

            self.write_committed_record(
                metadata.id.clone(),
                metadata.alias.clone(),
                metadata.resources,
                committed,
                metadata.source.clone(),
            )
            .await
        }
        .await;

        let record = match publish_result {
            Ok(record) => record,
            Err(error) => {
                // Best-effort rollback. Content-addressed managed layers are intentionally left
                // in place; they are shared across snapshots and require separate GC.
                if let Err(error) = self.client.delete_prefix(&layout.artifact_prefix()).await {
                    warn!(snapshot_id = %id, error = %error, "failed to roll back snapshot artifacts after publish failure");
                }
                for publication in disk_publications.iter().rev() {
                    if let Err(rollback_error) =
                        self.acr_exporter.rollback_publication(publication).await
                    {
                        warn!(
                            snapshot_id = %id,
                            image_ref = %publication.image_ref,
                            manifest_digest = %publication.manifest_digest,
                            error = %rollback_error,
                            "failed to roll back ACR snapshot publication; leaving cleanup to registry GC"
                        );
                    }
                }
                return Err(error);
            }
        };

        debug!(snapshot_id = %id, "published snapshot to oss");
        Ok(record)
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        // Try by id first.
        if let Ok(direct_id) = crate::snapshot::SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.read_record(&direct_id).await? {
                return Ok(Some(record));
            }
        }

        // Try by alias.
        let resolved_id = match self.resolve_alias(id_or_alias).await {
            Ok(id) => id,
            Err(error) => return Err(error),
        };
        let Some(resolved_id) = resolved_id else {
            return Ok(None);
        };
        self.read_record(&resolved_id).await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let keys = self
            .client
            .list_keys_recursive("catalog/records/")
            .await
            .map_err(|e| RepositoryError::backend("list snapshot records", e))?;

        let mut records: Vec<SnapshotRecord> = stream::iter(keys)
            .map(|key| async move {
                let bytes = self.client.get_bytes(&key).await.map_err(|e| {
                    RepositoryError::backend(format!("read snapshot record '{key}'"), e)
                })?;
                serde_json::from_slice::<SnapshotRecord>(&bytes).map_err(|e| {
                    RepositoryError::backend(format!("parse snapshot record '{key}'"), e)
                })
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;

        records.retain(|record| Self::matches_record_filter(record, &filter));
        records.sort_by(|a, b| {
            b.created_at_unix_ms
                .cmp(&a.created_at_unix_ms)
                .then_with(|| a.id.to_string().cmp(&b.id.to_string()))
        });

        Ok(records)
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        // Resolve the actual id + metadata.
        let record = match self.get(id_or_alias).await? {
            Some(t) => t,
            None => return Ok(()), // Idempotent.
        };
        let id = &record.id;
        let layout = self.layout(id);

        // 1. Delete alias binding.
        if let Some(ref alias) = record.alias {
            if self.load_alias_target(alias.as_ref()).await?.as_ref() == Some(id) {
                if let Err(error) = self
                    .client
                    .delete(&OssSnapshotArtifactLayout::alias_key(alias.as_ref()))
                    .await
                {
                    warn!(snapshot_id = %id, alias = %alias, error = %error, "failed to delete oss alias during snapshot removal");
                }
            }
        }

        // 2. Delete the catalog record.
        self.client
            .delete(&OssSnapshotArtifactLayout::record_key(id))
            .await
            .map_err(|e| RepositoryError::backend("delete snapshot record from oss", e))?;

        // 3. Delete external disk publications on a best-effort basis.
        if let Some(committed) = record.committed.as_ref() {
            self.delete_disk_publications(id, &committed.disk_publications)
                .await;
        }

        // 4. Delete artifacts.
        if let Err(error) = self.client.delete_prefix(&layout.artifact_prefix()).await {
            warn!(snapshot_id = %id, error = %error, "failed to delete oss snapshot artifacts");
        }

        debug!(snapshot_id = %id, "deleted snapshot from oss");
        Ok(())
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        let data = match self.client.get_bytes(&key).await {
            Ok(d) => d,
            Err(e) if OssClient::is_not_found_error(&e) => return Ok(None),
            Err(e) => {
                return Err(RepositoryError::backend(format!("read alias '{alias}'"), e));
            }
        };

        let id: SnapshotId = serde_json::from_slice(&data)
            .map_err(|e| RepositoryError::backend(format!("parse alias '{alias}'"), e))?;

        // Stale-alias cleanup: if the snapshot record doesn't exist, delete the alias
        // and return None (mirrors PosixFs catalog.rs:236 behavior).
        let snapshot_exists = self.snapshot_exists(&id).await?;
        if !snapshot_exists {
            warn!(alias = %alias, snapshot_id = %id, "cleaning up stale alias pointing to missing snapshot");
            if let Err(error) = self.client.delete(&key).await {
                warn!(alias = %alias, snapshot_id = %id, error = %error, "failed to delete stale oss alias");
            }
            return Ok(None);
        }

        Ok(Some(id))
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let mut record =
            self.read_record(id)
                .await?
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        if build.status != TemplateBuildStatus::Waiting {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("template build '{id}' is not in waiting state"),
            });
        }
        build.status = TemplateBuildStatus::Building;
        build.started_at_unix_ms = Some(now);
        build.error_reason = None;
        record.updated_at_unix_ms = now;
        self.write_record(&record).await?;
        Ok(record)
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let mut record =
            self.read_record(id)
                .await?
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        build.status = TemplateBuildStatus::Error;
        build.finished_at_unix_ms = Some(now);
        build.error_reason = Some(reason);
        record.updated_at_unix_ms = now;
        self.write_record(&record).await
    }

    async fn list_volumes(&self) -> RepositoryResult<Vec<VolumeRecord>> {
        let keys = self
            .client
            .list_keys_recursive(OssSnapshotArtifactLayout::volume_records_prefix())
            .await
            .map_err(|error| RepositoryError::backend("list volume records", error))?;
        let mut records = Vec::with_capacity(keys.len());
        for key in keys {
            let bytes = self.client.get_bytes(&key).await.map_err(|error| {
                RepositoryError::backend(format!("read volume record '{key}'"), error)
            })?;
            records.push(serde_json::from_slice(&bytes).map_err(|error| {
                RepositoryError::backend(format!("parse volume record '{key}'"), error)
            })?);
        }
        Ok(records)
    }

    async fn put_volume(&self, record: VolumeRecord) -> RepositoryResult<()> {
        validate_volume_id(&record.id)?;
        let mut record = record;
        record.backing_image_config = None;
        for _attempt in 0..MAX_VOLUME_CAS_ATTEMPTS {
            let (next, etag) = match self.read_volume_record_versioned(&record.id).await? {
                Some((existing, etag)) => {
                    let mut next = record.clone();
                    if existing.reserved_by_sandbox_id.is_some()
                        && existing.reserved_by_sandbox_id != next.reserved_by_sandbox_id
                    {
                        next.reserved_by_sandbox_id = existing.reserved_by_sandbox_id;
                    }
                    (next, etag)
                }
                None => (record.clone(), None),
            };
            if self
                .write_volume_record_conditionally(&next, etag.as_deref())
                .await?
            {
                return Ok(());
            }
        }
        Err(RepositoryError::Backend {
            message: format!(
                "volume record '{}' changed too often while publishing",
                record.id
            ),
            source: None,
        })
    }

    async fn publish_volume_backing(
        &self,
        _volume_id: &str,
        image_config_path: &std::path::Path,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        self.derive_and_upload_volume_layers(image_config_path)
            .await
    }

    async fn materialize_volume_backing(
        &self,
        _volume_id: &str,
        layers: &[OverlaybdLayerRef],
        destination: &std::path::Path,
    ) -> RepositoryResult<std::path::PathBuf> {
        let managed_url = self.client.managed_layers_repo_blob_url();
        materialize_volume_image_config(layers, destination, |layer| LayerConfig {
            repo_blob_url: managed_url.clone(),
            digest: layer.digest.clone(),
            size: layer.size,
            uuid: layer.uuid.clone().unwrap_or_default(),
            ..LayerConfig::default()
        })
        .await
    }

    async fn delete_volume(&self, volume_id: &str) -> RepositoryResult<()> {
        validate_volume_id(volume_id)?;
        self.client
            .delete(&OssSnapshotArtifactLayout::volume_record_key(volume_id))
            .await
            .map_err(|error| RepositoryError::backend("delete volume record", error))
    }

    async fn reserve_volume(
        &self,
        volume_id: &str,
        owner: &str,
    ) -> RepositoryResult<Option<String>> {
        validate_volume_id(volume_id)?;
        for _attempt in 0..MAX_VOLUME_CAS_ATTEMPTS {
            let Some((mut record, etag)) = self.read_volume_record_versioned(volume_id).await?
            else {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("volume '{volume_id}' does not exist in the repository"),
                });
            };
            if record.mode == VolumeMode::ReadOnly {
                return Ok(None);
            }
            if let Some(existing) = record.reserved_by_sandbox_id.as_deref() {
                if existing != owner {
                    return Ok(Some(existing.to_owned()));
                }
                return Ok(None);
            }
            record.reserved_by_sandbox_id = Some(owner.to_owned());
            if self
                .write_volume_record_conditionally(&record, etag.as_deref())
                .await?
            {
                return Ok(None);
            }
        }
        Err(RepositoryError::Backend {
            message: format!("volume '{volume_id}' changed too often while reserving"),
            source: None,
        })
    }

    async fn replace_volume_owner(
        &self,
        from: &str,
        to: Option<&str>,
    ) -> RepositoryResult<Option<String>> {
        let records = self.list_volumes().await?;
        if let Some(to) = to {
            if from != to
                && records
                    .iter()
                    .any(|record| record.reserved_by_sandbox_id.as_deref() == Some(to))
            {
                return Ok(Some(to.to_owned()));
            }
        }
        for record in records {
            if record.reserved_by_sandbox_id.as_deref() == Some(from) {
                self.replace_volume_record_owner(&record.id, from, to)
                    .await?;
            }
        }
        Ok(None)
    }
}

// ── private helpers ────────────────────────────────────────────────────

impl OssSnapshotRepository {
    async fn replace_volume_record_owner(
        &self,
        volume_id: &str,
        from: &str,
        to: Option<&str>,
    ) -> RepositoryResult<()> {
        for _attempt in 0..MAX_VOLUME_CAS_ATTEMPTS {
            let Some((mut record, etag)) = self.read_volume_record_versioned(volume_id).await?
            else {
                return Ok(());
            };
            if record.reserved_by_sandbox_id.as_deref() != Some(from) {
                return Ok(());
            }
            record.reserved_by_sandbox_id = to.map(str::to_owned);
            if self
                .write_volume_record_conditionally(&record, etag.as_deref())
                .await?
            {
                return Ok(());
            }
        }
        Err(RepositoryError::Backend {
            message: format!("volume '{volume_id}' changed too often while updating its owner"),
            source: None,
        })
    }

    async fn read_volume_record_versioned(
        &self,
        volume_id: &str,
    ) -> RepositoryResult<Option<(VolumeRecord, Option<String>)>> {
        let key = OssSnapshotArtifactLayout::volume_record_key(volume_id);
        match self.client.get_bytes_with_etag(&key).await {
            Ok((bytes, etag)) => serde_json::from_slice(&bytes)
                .map(|record| Some((record, etag)))
                .map_err(|error| RepositoryError::backend("parse volume record", error)),
            Err(error) if OssClient::is_not_found_error(&error) => Ok(None),
            Err(error) => Err(RepositoryError::backend("read volume record", error)),
        }
    }

    async fn write_volume_record_conditionally(
        &self,
        record: &VolumeRecord,
        etag: Option<&str>,
    ) -> RepositoryResult<bool> {
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|error| RepositoryError::backend("serialize volume record", error))?;
        self.client
            .put_bytes_conditionally(
                &OssSnapshotArtifactLayout::volume_record_key(&record.id),
                bytes,
                etag,
            )
            .await
            .map_err(|error| RepositoryError::backend("conditionally write volume record", error))
    }

    async fn read_record(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        let key = OssSnapshotArtifactLayout::record_key(id);
        match self.client.get_bytes(&key).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| RepositoryError::backend(format!("parse snapshot record '{id}'"), e)),
            Err(e) if OssClient::is_not_found_error(&e) => Ok(None),
            Err(e) => Err(RepositoryError::backend(
                format!("read snapshot record '{id}'"),
                e,
            )),
        }
    }

    async fn write_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|e| RepositoryError::backend("serialize snapshot record", e))?;
        self.client
            .put_bytes(
                &OssSnapshotArtifactLayout::record_key(&record.id),
                bytes,
                OssUploadArtifact::CatalogRecord,
            )
            .await
            .map_err(|e| RepositoryError::backend("write snapshot record", e))
    }

    async fn write_committed_record(
        &self,
        id: SnapshotId,
        alias: Option<SnapshotAlias>,
        resources: crate::types::SandboxResources,
        committed: CommittedSnapshot,
        source: SnapshotPublishSource,
    ) -> RepositoryResult<SnapshotRecord> {
        let now = now_unix_ms();
        let record = if let Some(mut record) = self.read_record(&id).await? {
            record.mark_committed(alias, resources, committed, source, now);
            record
        } else {
            let source = match source {
                SnapshotPublishSource::Template => SnapshotSource::Template {
                    build: TemplateBuildInfo {
                        status: TemplateBuildStatus::Ready,
                        started_at_unix_ms: None,
                        finished_at_unix_ms: Some(now),
                        error_reason: None,
                    },
                },
                SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                    SnapshotSource::Sandbox { source_sandbox_id }
                }
            };
            SnapshotRecord {
                id,
                alias,
                source,
                resources,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
                committed: Some(committed),
            }
        };
        self.write_record(&record).await?;
        Ok(record)
    }

    /// Bind an alias to a snapshot id with best-effort conflict detection.
    ///
    /// Alibaba Cloud OSS does not support conditional write headers
    /// (`If-None-Match`, `x-oss-forbid-overwrite`) on any S3-compatible
    /// write path, so we cannot use a true atomic `put_if_not_exists`.
    ///
    /// Instead the algorithm is:
    ///   1. Read the current alias target.
    ///   2. If it already points to `id`, return success (idempotent).
    ///   3. If it points to a live snapshot, return `AliasConflict`.
    ///   4. If it points to a deleted snapshot, remove the stale alias.
    ///   5. Write our binding unconditionally.
    ///   6. Read back and verify we won the race.  If someone else wrote a
    ///      different binding between steps 5 and 6, detect it here and
    ///      either retry or report a conflict.
    ///
    /// The read-back verification (step 6) narrows the race window to the
    /// interval between our write and the subsequent read.  This is weaker
    /// than a true CAS but sufficient for the current deployment model
    /// where concurrent publishes for the *same alias* are rare.
    async fn bind_alias(&self, alias: &str, id: &SnapshotId) -> RepositoryResult<()> {
        let key = validated_alias_key(alias)?;
        let payload = serde_json::to_vec(id)
            .map_err(|e| RepositoryError::backend("serialize alias binding", e))?;

        for _attempt in 0..MAX_ALIAS_BIND_ATTEMPTS {
            // Step 1-4: check current state and clean up stale bindings.
            if let Some(existing_id) = self.load_alias_target(alias).await? {
                if existing_id == *id {
                    return Ok(());
                }

                let still_exists = self.snapshot_exists(&existing_id).await?;
                if still_exists {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing: existing_id,
                        new_id: id.clone(),
                    });
                }

                self.client
                    .delete(&key)
                    .await
                    .map_err(|e| RepositoryError::backend("delete stale alias", e))?;
            }

            // Step 5: write our binding (unconditional — OSS does not
            // support conditional headers on S3-compatible writes).
            self.client
                .put_bytes(&key, payload.clone(), OssUploadArtifact::Alias)
                .await
                .map_err(|e| RepositoryError::backend("write alias binding", e))?;

            // Step 6: read back and verify we won.
            match self.load_alias_target(alias).await? {
                Some(bound_id) if bound_id == *id => return Ok(()),
                Some(existing_id) => {
                    // A concurrent writer overwrote our binding.
                    let still_exists = self.snapshot_exists(&existing_id).await?;
                    if still_exists {
                        return Err(RepositoryError::AliasConflict {
                            alias: alias.to_string(),
                            existing: existing_id,
                            new_id: id.clone(),
                        });
                    }

                    // The concurrent binding points to a deleted snapshot;
                    // clean it up and retry.
                    self.client
                        .delete(&key)
                        .await
                        .map_err(|e| RepositoryError::backend("delete stale alias", e))?;
                }
                None => {
                    warn!(
                        alias,
                        snapshot_id = %id,
                        "alias disappeared after bind attempt; retrying"
                    );
                    continue;
                }
            }
        }

        Err(RepositoryError::Backend {
            message: format!(
                "alias bind for '{alias}' exceeded {MAX_ALIAS_BIND_ATTEMPTS} attempts"
            ),
            source: None,
        })
    }

    async fn export_managed_disk_image(
        &self,
        image_config_path: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<DiskImageExportOutcome> {
        Ok(DiskImageExportOutcome {
            layers: self
                .derive_and_upload_disk_image_layers(image_config_path, artifact)
                .await?,
            publication: None,
        })
    }

    async fn derive_and_upload_disk_image_layers(
        &self,
        image_config_path: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        self.derive_and_upload_disk_image_layers_mode(image_config_path, artifact, false)
            .await
    }

    async fn derive_and_upload_volume_layers(
        &self,
        image_config_path: &Path,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        self.derive_and_upload_disk_image_layers_mode(
            image_config_path,
            OssUploadArtifact::RootfsLayer,
            true,
        )
        .await
    }

    async fn derive_and_upload_disk_image_layers_mode(
        &self,
        image_config_path: &Path,
        artifact: OssUploadArtifact,
        allow_descriptorless: bool,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        let image_config = load_overlaybd_image_config(image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!(
                    "load overlaybd image config '{}'",
                    image_config_path.display()
                ),
                e,
            )
        })?;

        let mut layers = Vec::with_capacity(image_config.lowers.len());

        for (index, layer) in image_config.lowers.into_iter().enumerate() {
            if !layer.file.is_empty() {
                let layer_path = Path::new(&layer.file);
                if !layer.digest.is_empty() && layer.size > 0 {
                    let managed = self
                        .import_managed_layer_with_descriptor(
                            layer_path,
                            &layer.digest,
                            layer.size,
                            artifact,
                        )
                        .await?;
                    layers.push(OverlaybdLayerRef::Managed(managed));
                    continue;
                }
                if crate::image::local_layer::rootfs_layer_is_runtime_generated_delta(layer_path) {
                    let managed = self
                        .import_descriptorless_rootfs_layer(layer_path, artifact)
                        .await?;
                    layers.push(OverlaybdLayerRef::Managed(managed));
                    continue;
                }
                if allow_descriptorless {
                    let managed = self
                        .import_descriptorless_rootfs_layer(layer_path, artifact)
                        .await?;
                    layers.push(OverlaybdLayerRef::Managed(managed));
                    continue;
                }
                return Err(RepositoryError::Unsupported {
                    feature: format!(
                        "local overlaybd lower layer {index} '{}' missing digest/size",
                        layer_path.display()
                    ),
                });
            }
            let repo_blob_url = layer
                .effective_repo_blob_url(&image_config.repo_blob_url)
                .to_string();
            if !repo_blob_url.is_empty() {
                let digest = if !layer.digest.is_empty() {
                    layer.digest
                } else if !layer.target_digest.is_empty() {
                    layer.target_digest
                } else {
                    format!("external:{index}")
                };
                layers.push(OverlaybdLayerRef::External(ExternalLayer {
                    digest,
                    repo_blob_url: repo_blob_url.clone(),
                    size: layer.size,
                }));
                continue;
            }
            return Err(RepositoryError::Unsupported {
                feature: format!("overlaybd lower layer {index} without local file or repoBlobUrl"),
            });
        }

        Ok(layers)
    }

    async fn derive_and_upload_memory_layers(
        &self,
        mem_image_config_path: &Path,
    ) -> RepositoryResult<Vec<ManagedLayer>> {
        let image_config = load_overlaybd_image_config(mem_image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!(
                    "load mem image config '{}'",
                    mem_image_config_path.display()
                ),
                e,
            )
        })?;

        let mut layers = Vec::with_capacity(image_config.lowers.len());
        for (index, layer) in image_config.lowers.into_iter().enumerate() {
            if layer.file.is_empty() {
                let repo_blob_url = layer
                    .effective_repo_blob_url(&image_config.repo_blob_url)
                    .to_string();
                if !repo_blob_url.is_empty() {
                    layers.push(managed_memory_layer_from_remote_lower(
                        index,
                        layer,
                        &repo_blob_url,
                        &self.client.managed_layers_repo_blob_url(),
                    )?);
                    continue;
                }
                return Err(RepositoryError::Unsupported {
                    feature: format!("memory layer {index} without local file path"),
                });
            }
            let layer_path = Path::new(&layer.file);
            if !layer.digest.is_empty() && layer.size > 0 {
                layers.push(
                    self.import_managed_layer_with_descriptor(
                        layer_path,
                        &layer.digest,
                        layer.size,
                        OssUploadArtifact::MemoryLayer,
                    )
                    .await?,
                );
                continue;
            }
            layers.push(
                self.import_managed_layer_by_hash(layer_path, OssUploadArtifact::MemoryLayer)
                    .await?,
            );
        }

        Ok(layers)
    }

    async fn export_disk_image(
        &self,
        snapshot_id: &SnapshotId,
        subject: DiskImageSubject,
        image_config_path: &Path,
        config: Option<SnapshotOciConfigInput<'_>>,
    ) -> RepositoryResult<DiskImageExportOutcome> {
        let artifact = match &subject {
            DiskImageSubject::Rootfs => OssUploadArtifact::RootfsLayer,
            DiskImageSubject::AttachedDrive { .. } => OssUploadArtifact::AttachedDriveLayer,
        };
        if !matches!(
            self.snapshot_image_storage,
            SnapshotImageStoragePolicy::SourceRegistry
        ) {
            return self
                .export_managed_disk_image(image_config_path, artifact)
                .await;
        }
        match self
            .acr_exporter
            .export(snapshot_id, subject.clone(), image_config_path, config)
            .await
        {
            Err(RepositoryError::Unsupported { feature }) => {
                if fallback_to_object_storage_would_mix_sources(
                    image_config_path,
                    &self.client.managed_layers_repo_blob_url(),
                )? {
                    return Err(RepositoryError::Unsupported {
                        feature: format!(
                            "source-registry export is unsupported for this remote-backed disk image: {feature}"
                        ),
                    });
                }
                info!(
                    snapshot_id = %snapshot_id,
                    subject = subject.log_label(),
                    reason = %feature,
                    "falling back to managed disk image layers"
                );
                self.export_managed_disk_image(image_config_path, artifact)
                    .await
            }
            result => result,
        }
    }

    async fn delete_disk_publications(
        &self,
        snapshot_id: &SnapshotId,
        publications: &[PersistedDiskImagePublication],
    ) {
        for publication in publications.iter().rev() {
            if let Err(error) = self.acr_exporter.rollback_publication(publication).await {
                warn!(
                    snapshot_id = %snapshot_id,
                    image_ref = %publication.image_ref,
                    manifest_digest = %publication.manifest_digest,
                    error = %error,
                    "failed to delete ACR publication during snapshot removal; continuing OSS cleanup"
                );
            }
        }
    }

    /// Export attached-drive disk images and derive committed metadata.
    async fn export_attached_drives(
        &self,
        snapshot_id: &SnapshotId,
        manifest: &crate::sandbox::FirecrackerSnapshotManifest,
        publications: &mut Vec<PersistedDiskImagePublication>,
    ) -> RepositoryResult<Vec<CommittedAttachedDrive>> {
        let mut drives = Vec::new();

        for drive in &manifest.attached_drives {
            let outcome = self
                .export_disk_image(
                    snapshot_id,
                    DiskImageSubject::AttachedDrive {
                        drive_id: drive.drive_id.clone(),
                    },
                    &drive.image_config_path,
                    None,
                )
                .await?;
            if let Some(publication) = outcome.publication.clone() {
                publications.push(publication);
            }
            drives.push(CommittedAttachedDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                layers: outcome.layers,
                read_only: drive.read_only,
                virtual_size: drive.virtual_size,
                mount_path: crate::sandbox::normalize_mount_path_for_drive(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                )
                .unwrap_or_else(|_| {
                    crate::sandbox::ExtraDrive::default_mount_path(&drive.drive_id)
                }),
                sub_path: drive.sub_path.clone(),
            });
        }

        Ok(drives)
    }

    async fn load_alias_target(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        let data = match self.client.get_bytes(&key).await {
            Ok(data) => data,
            Err(e) if OssClient::is_not_found_error(&e) => return Ok(None),
            Err(e) => {
                return Err(RepositoryError::backend(
                    format!("read alias target '{alias}'"),
                    e,
                ));
            }
        };

        let target = serde_json::from_slice::<SnapshotId>(&data)
            .map_err(|e| RepositoryError::backend(format!("parse alias target '{alias}'"), e))?;
        Ok(Some(target))
    }

    fn matches_record_filter(record: &SnapshotRecord, filter: &SnapshotListFilter) -> bool {
        if let Some(alias_prefix) = filter.alias_prefix.as_deref() {
            match record.alias.as_ref() {
                Some(alias) if alias.to_string().starts_with(alias_prefix) => {}
                _ => return false,
            }
        }

        if let Some(ids) = filter.snapshot_ids.as_ref() {
            if !ids.iter().any(|id| id == &record.id) {
                return false;
            }
        }

        if let Some(id_or_alias) = filter.snapshot_id_or_alias.as_deref() {
            if record.id.to_string() != id_or_alias
                && record
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias.as_ref() != id_or_alias)
            {
                return false;
            }
        }

        if let Some(source_sandbox_id) = filter.source_sandbox_id.as_deref() {
            match &record.source {
                SnapshotSource::Sandbox {
                    source_sandbox_id: record_source_sandbox_id,
                } if record_source_sandbox_id == source_sandbox_id => {}
                _ => return false,
            }
        }

        if let Some(sources) = filter.sources.as_ref() {
            let source = match &record.source {
                SnapshotSource::Template { .. } => SnapshotSourceKind::Template,
                SnapshotSource::Sandbox { .. } => SnapshotSourceKind::Sandbox,
            };
            if !sources.contains(&source) {
                return false;
            }
        }

        if let Some(statuses) = filter.template_statuses.as_ref() {
            let SnapshotSource::Template { build } = &record.source else {
                return false;
            };
            if !statuses.contains(&build.status) {
                return false;
            };
        }

        true
    }
}

impl OssSnapshotRepository {
    async fn import_descriptorless_rootfs_layer(
        &self,
        source: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = std::fs::canonicalize(source).map_err(|e| {
            RepositoryError::backend(
                format!("canonicalize managed layer '{}'", source.display()),
                e,
            )
        })?;
        if dense_export::should_dense_export_layer(&canonical) {
            return self
                .import_sparse_overlaybd_layer_dense(&canonical, artifact)
                .await;
        }
        self.import_managed_layer_by_hash(&canonical, artifact)
            .await
    }

    async fn import_sparse_overlaybd_layer_dense(
        &self,
        canonical: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let dense_temp = tempfile::NamedTempFile::new().map_err(|e| {
            RepositoryError::backend(
                format!(
                    "create temp dense overlaybd layer for '{}'",
                    canonical.display()
                ),
                e,
            )
        })?;
        let dense_path = dense_temp.path().to_path_buf();
        let descriptor = write_dense_overlaybd_layer_to_file(canonical, &dense_path)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!(
                        "dense-export sparse overlaybd layer '{}'",
                        canonical.display()
                    ),
                    e,
                )
            })?;
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(&descriptor.digest);
        upload_managed_layer_if_missing(&self.client, &oss_key, &dense_path, artifact).await?;

        Ok(ManagedLayer {
            digest: descriptor.digest,
            size: descriptor.size,
            uuid: None,
        })
    }

    async fn import_managed_layer_by_hash(
        &self,
        source: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = std::fs::canonicalize(source).map_err(|e| {
            RepositoryError::backend(
                format!("canonicalize managed layer '{}'", source.display()),
                e,
            )
        })?;
        let descriptor = crate::digest::FileDigest::describe(&canonical)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("describe managed layer '{}'", canonical.display()),
                    e,
                )
            })?;
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(&descriptor.sha256);
        upload_managed_layer_if_missing(&self.client, &oss_key, &canonical, artifact).await?;

        Ok(ManagedLayer {
            digest: descriptor.sha256,
            size: descriptor.size,
            uuid: overlaybd_layer_uuid(&canonical),
        })
    }

    async fn import_managed_layer_with_descriptor(
        &self,
        source: &Path,
        digest: &str,
        size: u64,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = std::fs::canonicalize(source).map_err(|e| {
            RepositoryError::backend(
                format!("canonicalize managed layer '{}'", source.display()),
                e,
            )
        })?;
        let source_size = std::fs::metadata(&canonical)
            .map_err(|e| {
                RepositoryError::backend(
                    format!("read managed layer metadata '{}'", canonical.display()),
                    e,
                )
            })?
            .len();
        if source_size != size {
            return Err(RepositoryError::Backend {
                message: format!(
                    "managed layer descriptor size mismatch for '{}': descriptor says {}, file has {}",
                    canonical.display(),
                    size,
                    source_size
                ),
                source: None,
            });
        }

        // Descriptor-backed imports intentionally trust internally generated
        // content digests and only validate the cheap size invariant here.
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(digest);
        upload_managed_layer_if_missing(&self.client, &oss_key, &canonical, artifact).await?;

        Ok(ManagedLayer {
            digest: digest.to_string(),
            size,
            uuid: overlaybd_layer_uuid(&canonical),
        })
    }
}

/// Upload a content-addressed managed layer if it is not already present.
///
/// Managed layers are keyed by `sha256:{digest}`, so concurrent writers
/// uploading the same digest always produce identical content.  This makes
/// the `exists() → put()` TOCTOU benign: the worst case is a redundant
/// upload of identical bytes, never data corruption.
///
/// We intentionally use an unconditional `put_file` instead of the
/// conditional `put_file_if_not_exists` here because Alibaba Cloud OSS
/// does not support conditional headers (`x-oss-forbid-overwrite`) on
/// multipart/streaming uploads — only on single-PUT operations.  OpenDAL's
/// `writer_with().if_not_exists(true)` triggers the multipart path for
/// large files, which causes a `NotImplemented` error on OSS.
async fn upload_managed_layer_if_missing(
    client: &OssClient,
    key: &str,
    canonical: &Path,
    artifact: OssUploadArtifact,
) -> RepositoryResult<()> {
    let already_exists = client
        .exists(key)
        .await
        .map_err(|e| RepositoryError::backend("check managed layer existence", e))?;

    if !already_exists {
        client
            .put_file(key, canonical, artifact)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("upload managed layer '{}'", canonical.display()),
                    e,
                )
            })?;
    }

    Ok(())
}

fn validate_publish_manifest_image_configs(
    manifest: &FirecrackerSnapshotManifest,
) -> RepositoryResult<()> {
    load_overlaybd_image_config(&manifest.rootfs.image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "validate rootfs image config '{}'",
                manifest.rootfs.image_config_path.display()
            ),
            e,
        )
    })?;
    load_overlaybd_image_config(&manifest.memory.image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "validate memory image config '{}'",
                manifest.memory.image_config_path.display()
            ),
            e,
        )
    })?;
    for drive in &manifest.attached_drives {
        load_overlaybd_image_config(&drive.image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!(
                    "validate drive image config '{}' for drive '{}'",
                    drive.image_config_path.display(),
                    drive.drive_id
                ),
                e,
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store_operator::CredentialSource;
    use overlaybd::config::ImageConfig;
    use serde_json::json;

    fn write_test_image(path: &Path, value: serde_json::Value) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&value).expect("serialize image config"),
        )
        .expect("write image config");
    }

    fn test_repository() -> OssSnapshotRepository {
        let client = OssClient::new(
            "bucket".to_string(),
            "https://oss.example.com".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("oss client");
        OssSnapshotRepository::new(Arc::new(client), SnapshotImageStoragePolicy::ObjectStorage)
    }

    #[test]
    fn publish_manifest_preflight_rejects_invalid_memory_image_config() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rootfs_image_config = temp.path().join("rootfs-image.json");
        let memory_image_config = temp.path().join("mem-image.json");

        write_test_image(
            &rootfs_image_config,
            json!({
                "lowers": [
                    { "file": "rootfs.commit" }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );
        write_test_image(
            &memory_image_config,
            json!({
                "lowers": [
                    { "digest": "sha256:parent", "size": 4096 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );

        let mut manifest = FirecrackerSnapshotManifest::for_test(1024, &[]);
        manifest.rootfs.image_config_path = rootfs_image_config;
        manifest.memory.image_config_path = memory_image_config;

        let err = validate_publish_manifest_image_configs(&manifest)
            .expect_err("missing memory repoBlobUrl should fail preflight");
        assert!(err.to_string().contains("validate memory image config"));
    }

    #[test]
    fn detects_source_registry_fallback_source_mixing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_registry_image = temp.path().join("source-image.json");
        write_test_image(
            &source_registry_image,
            json!({
                "repoBlobUrl": "https://registry.example/v2/ns/image/blobs",
                "lowers": [
                    { "digest": "sha256:base", "size": 4096 },
                    { "file": "snapshot.commit", "digest": "sha256:delta", "size": 5 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );
        assert!(fallback_to_object_storage_would_mix_sources(
            &source_registry_image,
            "s3://bucket/prefix/managed-layers"
        )
        .unwrap());

        let oss_image = temp.path().join("oss-image.json");
        write_test_image(
            &oss_image,
            json!({
                "repoBlobUrl": "s3://bucket/prefix/managed-layers",
                "lowers": [
                    { "digest": "sha256:base", "size": 4096 },
                    { "file": "snapshot.commit", "digest": "sha256:delta", "size": 5 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );

        assert!(!fallback_to_object_storage_would_mix_sources(
            &oss_image,
            "s3://bucket/prefix/managed-layers"
        )
        .unwrap());

        let layer_level_image = temp.path().join("layer-level-image.json");
        write_test_image(
            &layer_level_image,
            json!({
                "repoBlobUrl": "",
                "lowers": [
                    {
                        "digest": "sha256:base",
                        "size": 4096,
                        "repoBlobUrl": "https://registry.example/v2/ns/image/blobs"
                    },
                    { "file": "snapshot.commit", "digest": "sha256:delta", "size": 5 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );
        assert!(fallback_to_object_storage_would_mix_sources(
            &layer_level_image,
            "s3://bucket/prefix/managed-layers"
        )
        .unwrap());
    }

    #[tokio::test]
    async fn derives_external_layers_from_layer_repo_blob_urls() {
        let temp = tempfile::tempdir().expect("tempdir");
        let image = temp.path().join("image.json");
        write_test_image(
            &image,
            json!({
                "repoBlobUrl": "",
                "lowers": [
                    {
                        "digest": "sha256:base",
                        "size": 4096,
                        "repoBlobUrl": "https://registry.example/v2/ns/image/blobs"
                    },
                    {
                        "digest": "sha256:delta",
                        "size": 8192,
                        "repoBlobUrl": "s3://bucket/prefix/managed-layers"
                    }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );

        let layers = test_repository()
            .derive_and_upload_disk_image_layers(&image, OssUploadArtifact::RootfsLayer)
            .await
            .expect("derive layers");

        assert_eq!(
            layers,
            vec![
                OverlaybdLayerRef::External(ExternalLayer {
                    digest: "sha256:base".to_string(),
                    repo_blob_url: "https://registry.example/v2/ns/image/blobs".to_string(),
                    size: 4096,
                }),
                OverlaybdLayerRef::External(ExternalLayer {
                    digest: "sha256:delta".to_string(),
                    repo_blob_url: "s3://bucket/prefix/managed-layers".to_string(),
                    size: 8192,
                }),
            ]
        );
    }

    #[tokio::test]
    async fn materializes_volume_backing_with_backend_layer_urls() {
        let temp = tempfile::tempdir().expect("tempdir");
        let repository = test_repository();
        let destination = temp.path().join("volume/image.json");
        let layers = vec![
            OverlaybdLayerRef::Managed(ManagedLayer {
                digest: "sha256:managed".to_string(),
                size: 12,
                uuid: None,
            }),
            OverlaybdLayerRef::External(ExternalLayer {
                digest: "sha256:external".to_string(),
                repo_blob_url: "https://registry.example/v2/data/blobs".to_string(),
                size: 24,
            }),
        ];

        repository
            .materialize_volume_backing("vol_test", &layers, &destination)
            .await
            .expect("materialize volume config");
        let config: ImageConfig =
            serde_json::from_slice(&tokio::fs::read(&destination).await.unwrap()).unwrap();
        assert_eq!(config.lowers.len(), 2);
        assert_eq!(
            config.lowers[0].repo_blob_url,
            "s3://bucket/prefix/managed-layers"
        );
        assert_eq!(
            config.lowers[1].repo_blob_url,
            "https://registry.example/v2/data/blobs"
        );
        assert!(config.lowers.iter().all(|layer| layer.file.is_empty()));
    }
}
