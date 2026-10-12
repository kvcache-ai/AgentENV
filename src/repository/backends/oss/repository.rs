use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use overlaybd::config::{load_image_config as load_overlaybd_image_config, LayerConfig};
use overlaybd::dense_export;
use overlaybd::layer_metadata::read_overlaybd_layer_uuid;
use tracing::{debug, info, warn};

use super::client::{OssClient, OssUploadArtifact};
use super::documents;
use super::layout::OssSnapshotArtifactLayout;
use crate::cfg::{SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
use crate::repository::backends::common::acr::{
    AcrDiskImageExporter, DiskImageExportOutcome, DiskImageSubject, SnapshotOciConfigInput,
};
use crate::repository::backends::common::recontainerize::{
    prepare_layer_upload, PreparedLayerUpload,
};
use crate::repository::backends::common::{
    materialize_volume_image_config, write_dense_overlaybd_layer_to_file,
};
use crate::repository::interfaces::{Repository, StoredSnapshot};
use crate::repository::{
    DocumentCollection, DocumentKey, DocumentUpdate, RepositoryError, RepositoryResult,
};
use crate::sandbox::{OverlaybdCompactOutput, SandboxSnapshotManifest};
use crate::snapshot::{
    CommittedAttachedDrive, ExternalLayer, ManagedLayer, OverlaybdLayerRef,
    PersistedDiskImagePublication, SnapshotId, SnapshotPublishMetadata,
    MEMORY_STARTUP_PACK_ARTIFACT, SNAPSHOT_ARTIFACT_LAYOUT,
};

/// Manages the committed‐state layer of the OSS snapshot repository.
///
/// Object layout under the configured prefix:
///
/// ```text
/// catalog/aliases/{name}.json              → "snapshot-id"
/// volumes/records/{volume-id}.json         → VolumeRecord
/// volumes/aliases/{volume-name}.json       → "volume-id"
/// artifacts/{id}/firecracker-manifest.json → SandboxSnapshotManifest (paths omitted)
/// artifacts/{id}/vm_state.bin
/// managed-layers/{digest}
/// ```
pub(crate) struct OssRepository {
    client: Arc<OssClient>,
    snapshot_image_storage: SnapshotImageStoragePolicy,
    acr_exporter: AcrDiskImageExporter,
    publish_compression: OverlaybdCompactOutput,
}

impl OssRepository {
    pub(crate) fn new(
        client: Arc<OssClient>,
        snapshot_image_storage: SnapshotImageStoragePolicy,
        publish_compression: &SnapshotPublishCompressionConfig,
    ) -> Self {
        let publish_compression =
            OverlaybdCompactOutput::from_publish_compression_config(publish_compression);
        Self {
            client,
            snapshot_image_storage,
            acr_exporter: AcrDiskImageExporter::new(publish_compression),
            publish_compression,
        }
    }

    fn layout<'a>(&self, id: &'a SnapshotId) -> OssSnapshotArtifactLayout<'a> {
        OssSnapshotArtifactLayout::new(id)
    }
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

// ── Repository impl ────────────────────────────────────────────

#[async_trait]
impl Repository for OssRepository {
    async fn store_snapshot(
        &self,
        metadata: &SnapshotPublishMetadata,
        manifest: &SandboxSnapshotManifest,
    ) -> RepositoryResult<StoredSnapshot> {
        let id = &metadata.id;
        let layout = self.layout(id);
        let mut disk_publications = Vec::new();
        let mut local_layers = Vec::new();

        let stored = async {
            validate_publish_manifest_image_configs(manifest)?;

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
            local_layers.extend(rootfs_outcome.local_layers);
            let rootfs_layers = rootfs_outcome.layers;

            let memory_layers = self
                .derive_and_upload_memory_layers(
                    &manifest.memory.image_config_path,
                    &mut local_layers,
                )
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

            let persisted_manifest_bytes = serde_json::to_vec_pretty(manifest)
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
                .export_attached_drives(id, manifest, &mut disk_publications, &mut local_layers)
                .await?;
            RepositoryResult::Ok((rootfs_layers, memory_layers, attached_drives))
        }
        .await;

        match stored {
            Ok((rootfs_layers, memory_layers, attached_drives)) => {
                debug!(snapshot_id = %id, "stored snapshot in oss");
                Ok(StoredSnapshot {
                    rootfs_layers,
                    memory_layers,
                    attached_drives,
                    disk_publications,
                    local_layers,
                })
            }
            Err(error) => {
                if let Err(cleanup) = self.delete_snapshot_files(id, &disk_publications).await {
                    warn!(snapshot_id = %id, error = %cleanup, "failed to roll back snapshot files after store failure");
                }
                Err(error)
            }
        }
    }

    async fn delete_snapshot_files(
        &self,
        id: &SnapshotId,
        publications: &[PersistedDiskImagePublication],
    ) -> RepositoryResult<()> {
        // External disk publications are removed on a best-effort basis.
        self.delete_disk_publications(id, publications).await;
        // Content-addressed managed layers are intentionally left in place;
        // they are shared across snapshots and require separate GC.
        if let Err(error) = self
            .client
            .delete_prefix(&self.layout(id).artifact_prefix())
            .await
        {
            warn!(snapshot_id = %id, error = %error, "failed to delete oss snapshot artifacts");
        }
        Ok(())
    }

    async fn put_startup_manifest(&self, id: &SnapshotId, bytes: Vec<u8>) -> RepositoryResult<()> {
        self.client
            .put_bytes(
                &self.layout(id).artifact_key(MEMORY_STARTUP_PACK_ARTIFACT),
                bytes,
                OssUploadArtifact::StartupPack,
            )
            .await
            .map_err(|error| RepositoryError::backend("upload startup manifest", error))
    }

    async fn delete_startup_manifest(&self, id: &SnapshotId) -> RepositoryResult<()> {
        self.client
            .delete(&self.layout(id).artifact_key(MEMORY_STARTUP_PACK_ARTIFACT))
            .await
            .map_err(|error| RepositoryError::backend("delete startup manifest", error))
    }

    async fn get_document(&self, key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        documents::get(&self.client, &key).await
    }

    async fn put_document(&self, key: DocumentKey, bytes: Vec<u8>) -> RepositoryResult<()> {
        documents::put(&self.client, &key, bytes).await
    }

    async fn update_document(
        &self,
        key: DocumentKey,
        update: DocumentUpdate,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        documents::update(&self.client, &key, update).await
    }

    async fn delete_document(&self, key: DocumentKey) -> RepositoryResult<()> {
        documents::delete(&self.client, &key).await
    }

    async fn list_documents(
        &self,
        collection: DocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        documents::list(&self.client, collection, after, limit).await
    }

    async fn read_documents(
        &self,
        collection: DocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        documents::read_all(&self.client, collection).await
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
}

// ── private helpers ────────────────────────────────────────────────────

impl OssRepository {
    async fn export_managed_disk_image(
        &self,
        image_config_path: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<DiskImageExportOutcome> {
        let mut local_layers = Vec::new();
        let layers = self
            .derive_and_upload_disk_image_layers_mode(
                image_config_path,
                artifact,
                false,
                &mut local_layers,
            )
            .await?;
        Ok(DiskImageExportOutcome {
            layers,
            publication: None,
            local_layers,
        })
    }

    #[cfg(test)]
    async fn derive_and_upload_disk_image_layers(
        &self,
        image_config_path: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        self.derive_and_upload_disk_image_layers_mode(
            image_config_path,
            artifact,
            false,
            &mut Vec::new(),
        )
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
            &mut Vec::new(),
        )
        .await
    }

    async fn derive_and_upload_disk_image_layers_mode(
        &self,
        image_config_path: &Path,
        artifact: OssUploadArtifact,
        allow_descriptorless: bool,
        local_layers: &mut Vec<PreparedLayerUpload>,
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
                            local_layers,
                        )
                        .await?;
                    layers.push(OverlaybdLayerRef::Managed(managed));
                    continue;
                }
                if crate::image::local_layer::rootfs_layer_is_runtime_generated_delta(layer_path) {
                    let managed = self
                        .import_descriptorless_rootfs_layer(layer_path, artifact, local_layers)
                        .await?;
                    layers.push(OverlaybdLayerRef::Managed(managed));
                    continue;
                }
                if allow_descriptorless {
                    let managed = self
                        .import_descriptorless_rootfs_layer(layer_path, artifact, local_layers)
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
        local_layers: &mut Vec<PreparedLayerUpload>,
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
                        local_layers,
                    )
                    .await?,
                );
                continue;
            }
            layers.push(
                self.import_managed_layer_by_hash(
                    layer_path,
                    OssUploadArtifact::MemoryLayer,
                    local_layers,
                )
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
        manifest: &crate::sandbox::SandboxSnapshotManifest,
        publications: &mut Vec<PersistedDiskImagePublication>,
        local_layers: &mut Vec<PreparedLayerUpload>,
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
            local_layers.extend(outcome.local_layers);
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
}

impl OssRepository {
    async fn import_descriptorless_rootfs_layer(
        &self,
        source: &Path,
        artifact: OssUploadArtifact,
        local_layers: &mut Vec<PreparedLayerUpload>,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = std::fs::canonicalize(source).map_err(|e| {
            RepositoryError::backend(
                format!("canonicalize managed layer '{}'", source.display()),
                e,
            )
        })?;
        if dense_export::should_dense_export_layer(&canonical) {
            return self
                .import_sparse_overlaybd_layer_dense(&canonical, artifact, local_layers)
                .await;
        }
        self.import_managed_layer_by_hash(&canonical, artifact, local_layers)
            .await
    }

    async fn import_sparse_overlaybd_layer_dense(
        &self,
        canonical: &Path,
        artifact: OssUploadArtifact,
        local_layers: &mut Vec<PreparedLayerUpload>,
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
        let mut upload = prepare_layer_upload(
            &dense_path,
            self.publish_compression,
            Some((&descriptor.digest, descriptor.size)),
        )
        .await?;
        upload.retain_source(dense_temp);
        // Dense-exported layers never carry a layer uuid, recontainerized or not.
        self.upload_prepared_layer(upload, None, artifact, local_layers)
            .await
    }

    async fn import_managed_layer_by_hash(
        &self,
        source: &Path,
        artifact: OssUploadArtifact,
        local_layers: &mut Vec<PreparedLayerUpload>,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = std::fs::canonicalize(source).map_err(|e| {
            RepositoryError::backend(
                format!("canonicalize managed layer '{}'", source.display()),
                e,
            )
        })?;
        let upload = prepare_layer_upload(&canonical, self.publish_compression, None).await?;
        let uuid = overlaybd_layer_uuid(upload.path());
        self.upload_prepared_layer(upload, uuid, artifact, local_layers)
            .await
    }

    async fn import_managed_layer_with_descriptor(
        &self,
        source: &Path,
        digest: &str,
        size: u64,
        artifact: OssUploadArtifact,
        local_layers: &mut Vec<PreparedLayerUpload>,
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
        // Publish compression recontainerizes raw layers as zfile, which
        // changes the physical bytes, so `prepare_layer_upload` re-hashes the
        // compressed output instead of trusting this descriptor.
        let upload =
            prepare_layer_upload(&canonical, self.publish_compression, Some((digest, size)))
                .await?;
        let uuid = overlaybd_layer_uuid(upload.path());
        self.upload_prepared_layer(upload, uuid, artifact, local_layers)
            .await
    }

    /// Upload a prepared layer under its content-addressed key and build the
    /// committed managed-layer reference. The object key, digest, and size all
    /// describe exactly the prepared (possibly recontainerized) bytes.
    async fn upload_prepared_layer(
        &self,
        upload: PreparedLayerUpload,
        uuid: Option<String>,
        artifact: OssUploadArtifact,
        local_layers: &mut Vec<PreparedLayerUpload>,
    ) -> RepositoryResult<ManagedLayer> {
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(upload.digest());
        upload_managed_layer_if_missing(&self.client, &oss_key, upload.path(), artifact).await?;
        let layer = ManagedLayer {
            digest: upload.digest().to_string(),
            size: upload.size(),
            uuid,
        };
        local_layers.push(upload);
        Ok(layer)
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
    manifest: &SandboxSnapshotManifest,
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

// ── Detached startup-manifest continuation ─────────────────────────────────

/// Detached startup-manifest continuation: joins the recording, builds and
/// uploads the manifest, then attaches its descriptor to the already
/// committed record. Best-effort throughout — any failure only means the
/// snapshot resumes on-demand.
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

    fn test_repository() -> OssRepository {
        let client = OssClient::new(
            "bucket".to_string(),
            "https://oss.example.com".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("oss client");
        OssRepository::new(
            Arc::new(client),
            SnapshotImageStoragePolicy::ObjectStorage,
            &SnapshotPublishCompressionConfig {
                enabled: false,
                ..Default::default()
            },
        )
    }

    #[test]
    fn publish_compression_config_resolves_into_compact_output() {
        let client = || {
            Arc::new(
                OssClient::new(
                    "bucket".to_string(),
                    "https://oss.example.com".to_string(),
                    "region".to_string(),
                    "prefix".to_string(),
                    CredentialSource::Anonymous,
                    None,
                )
                .expect("oss client"),
            )
        };

        let disabled = OssRepository::new(
            client(),
            SnapshotImageStoragePolicy::ObjectStorage,
            &SnapshotPublishCompressionConfig {
                enabled: false,
                ..Default::default()
            },
        );
        assert_eq!(disabled.publish_compression, OverlaybdCompactOutput::Raw);

        let enabled = OssRepository::new(
            client(),
            SnapshotImageStoragePolicy::ObjectStorage,
            &SnapshotPublishCompressionConfig {
                enabled: true,
                algorithm: crate::cfg::OverlaybdCompressionAlgorithm::Zstd,
                workers: 4,
            },
        );
        assert_eq!(
            enabled.publish_compression,
            OverlaybdCompactOutput::ZFile {
                algorithm: crate::cfg::OverlaybdCompressionAlgorithm::Zstd,
                workers: 4,
            }
        );
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

        let mut manifest = SandboxSnapshotManifest::for_test(1024, &[]);
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
