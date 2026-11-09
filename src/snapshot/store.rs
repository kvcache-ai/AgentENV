//! Storing a snapshot's files and layers, and a volume's layers, through the
//! repository's storage primitives. The rules here are the same on every
//! backend; only the repository's [`PublishPolicy`] differs.

use crate::layer::{LayerChain, LayerRef, LayerSource};
use std::path::Path;

use tracing::{info, warn};

use super::image_export::acr::{AcrDiskImageExporter, DiskImageSubject, SnapshotOciConfigInput};
use super::recontainerize::PreparedLayerUpload;
use super::{
    CommittedAttachedDrive, ManagedLayer, OverlaybdLayerRef, PersistedDiskImagePublication,
    SnapshotId, SnapshotPublishMetadata,
};
use crate::cfg::SnapshotImageStoragePolicy;
use crate::overlaybd::OverlaybdCompactOutput;
use crate::repository::{
    BlobBody, BlobKey, BlobLocation, LayerRole, Repository, RepositoryError, RepositoryResult,
    SnapshotFile,
};
use crate::snapshot::SandboxSnapshotManifest;

/// What was stored for one snapshot before its record is committed.
#[derive(Debug)]
pub(crate) struct StoredSnapshot {
    pub(crate) rootfs_layers: Vec<OverlaybdLayerRef>,
    pub(crate) memory_layers: Vec<ManagedLayer>,
    pub(crate) attached_drives: Vec<CommittedAttachedDrive>,
    pub(crate) disk_publications: Vec<PersistedDiskImagePublication>,
    /// The exact files stored as layers, kept for post-commit P2P publication.
    pub(crate) local_layers: Vec<PreparedLayerUpload>,
}

/// Stores one captured snapshot's files and layers. On failure it removes
/// what it stored, except shared content-addressed layers.
pub(crate) async fn store_snapshot<R>(
    repository: &R,
    metadata: &SnapshotPublishMetadata,
    manifest: &SandboxSnapshotManifest,
) -> RepositoryResult<StoredSnapshot>
where
    R: Repository + ?Sized,
{
    let mut store = LayerStore::new(repository);
    match store.store_snapshot(metadata, manifest).await {
        Ok((rootfs_layers, memory_layers, attached_drives)) => Ok(StoredSnapshot {
            rootfs_layers,
            memory_layers,
            attached_drives,
            disk_publications: store.disk_publications,
            local_layers: store.local_layers,
        }),
        Err(error) => {
            rollback_disk_publications(&metadata.id, &store.disk_publications).await;
            Err(error)
        }
    }
}

/// Removes one snapshot's files and, best-effort, its source-registry
/// publications. Shared layers stay; other snapshots may reference them.
pub(crate) async fn delete_snapshot<R>(
    repository: &R,
    id: &SnapshotId,
    publications: &[PersistedDiskImagePublication],
) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    rollback_disk_publications(id, publications).await;
    repository.delete_snapshot_blobs(id).await
}

async fn rollback_disk_publications(
    id: &SnapshotId,
    publications: &[PersistedDiskImagePublication],
) {
    if !publications.is_empty() {
        let exporter = AcrDiskImageExporter::new(OverlaybdCompactOutput::Raw);
        for publication in publications.iter().rev() {
            if let Err(error) = exporter.rollback_publication(publication).await {
                warn!(
                    snapshot_id = %id,
                    image_ref = %publication.image_ref,
                    manifest_digest = %publication.manifest_digest,
                    error = %error,
                    "failed to delete source-registry publication; continuing snapshot removal"
                );
            }
        }
    }
}

/// Stores a volume's node-local backing layers and returns their references.
pub(crate) async fn store_volume_layers<R>(
    repository: &R,
    image_config_path: &Path,
) -> RepositoryResult<Vec<OverlaybdLayerRef>>
where
    R: Repository + ?Sized,
{
    LayerStore::new(repository)
        .store_disk_layers(image_config_path, LayerRole::Rootfs, true)
        .await
}

struct LayerStore<'a, R: ?Sized> {
    repository: &'a R,
    compression: OverlaybdCompactOutput,
    image_storage: SnapshotImageStoragePolicy,
    managed_layers_repo_blob_url: Option<String>,
    disk_publications: Vec<PersistedDiskImagePublication>,
    local_layers: Vec<PreparedLayerUpload>,
}

impl<'a, R: Repository + ?Sized> LayerStore<'a, R> {
    fn new(repository: &'a R) -> Self {
        let policy = repository.publish_policy();
        Self {
            repository,
            compression: OverlaybdCompactOutput::from(&policy.compression),
            image_storage: policy.image_storage,
            managed_layers_repo_blob_url: repository.managed_layers_repo_blob_url(),
            disk_publications: Vec::new(),
            local_layers: Vec::new(),
        }
    }

    async fn store_snapshot(
        &mut self,
        metadata: &SnapshotPublishMetadata,
        manifest: &SandboxSnapshotManifest,
    ) -> RepositoryResult<(
        Vec<OverlaybdLayerRef>,
        Vec<ManagedLayer>,
        Vec<CommittedAttachedDrive>,
    )> {
        let id = &metadata.id;
        let exporter = matches!(
            self.image_storage,
            SnapshotImageStoragePolicy::SourceRegistry
        )
        .then(|| AcrDiskImageExporter::new(self.compression));

        // Rootfs is exported with the effective runtime config.
        let rootfs_config =
            SnapshotOciConfigInput::new(&metadata.context, metadata.image_configs.rootfs_config());
        let rootfs_layers = self
            .store_disk_image(
                exporter.as_ref(),
                id,
                DiskImageSubject::Rootfs,
                &manifest.rootfs.layers,
                Some(rootfs_config),
            )
            .await?;
        let memory_layers = self.store_memory_chain(&manifest.memory.layers).await?;

        self.repository
            .put_blob(
                &BlobKey::Snapshot(id.clone(), SnapshotFile::VmState),
                BlobBody::File(&manifest.vm_state.path),
            )
            .await?;
        let manifest_bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|error| RepositoryError::backend("serialize firecracker manifest", error))?;
        self.repository
            .put_blob(
                &BlobKey::Snapshot(id.clone(), SnapshotFile::FirecrackerManifest),
                BlobBody::Bytes(manifest_bytes),
            )
            .await?;

        let mut attached_drives = Vec::with_capacity(manifest.attached_drives.len());
        for drive in &manifest.attached_drives {
            // Only logical layers are committed; the build-time image config
            // is regenerated by the runtime resolver.
            let layers = self
                .store_disk_image(
                    exporter.as_ref(),
                    id,
                    DiskImageSubject::AttachedDrive {
                        drive_id: drive.drive_id.clone(),
                    },
                    &drive.layers,
                    None,
                )
                .await?;
            attached_drives.push(CommittedAttachedDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                layers,
                read_only: drive.read_only,
                virtual_size: drive.virtual_size,
                mount_path: crate::types::normalize_mount_path_for_drive(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                )
                .unwrap_or_else(|_| crate::types::ExtraDrive::default_mount_path(&drive.drive_id)),
                sub_path: drive.sub_path.clone(),
            });
        }
        Ok((rootfs_layers, memory_layers, attached_drives))
    }

    /// Publishes a disk image to its source registry when the policy asks
    /// for it, otherwise (or when the registry cannot take it) stores its
    /// local layers here.
    async fn store_disk_image(
        &mut self,
        exporter: Option<&AcrDiskImageExporter>,
        id: &SnapshotId,
        subject: DiskImageSubject,
        layers: &LayerChain,
        config: Option<SnapshotOciConfigInput<'_>>,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        let role = match subject {
            DiskImageSubject::Rootfs => LayerRole::Rootfs,
            DiskImageSubject::AttachedDrive { .. } => LayerRole::AttachedDrive,
        };
        let Some(exporter) = exporter else {
            return self.store_disk_chain(layers, role).await;
        };
        match exporter.export(id, subject.clone(), layers, config).await {
            Ok(outcome) => {
                self.disk_publications.extend(outcome.publication);
                self.local_layers.extend(outcome.local_layers);
                Ok(outcome.layers)
            }
            Err(RepositoryError::Unsupported { feature }) => {
                let managed_url = self.managed_layers_repo_blob_url.as_deref().unwrap_or("");
                if fallback_would_mix_sources(layers, managed_url) {
                    return Err(RepositoryError::Unsupported {
                        feature: format!(
                            "source-registry export is unsupported for this remote-backed disk image: {feature}"
                        ),
                    });
                }
                info!(
                    snapshot_id = %id,
                    subject = subject.log_label(),
                    reason = %feature,
                    "falling back to managed disk image layers"
                );
                self.store_disk_chain(layers, role).await
            }
            Err(error) => Err(error),
        }
    }

    /// Volumes and legacy config inputs enter the same validated layer-chain adapter.
    async fn store_disk_layers(
        &mut self,
        image_config_path: &Path,
        role: LayerRole,
        _allow_descriptorless: bool,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        let layers = crate::layer::load_layer_chain_from_config(
            image_config_path,
            self.managed_layers_repo_blob_url.as_deref(),
        )
        .await
        .map_err(|error| RepositoryError::backend("import layer configuration", error))?;
        self.store_disk_chain(&layers, role).await
    }

    async fn store_disk_chain(
        &mut self,
        chain: &LayerChain,
        role: LayerRole,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        let mut layers = Vec::with_capacity(chain.layers.len());
        for layer in &chain.layers {
            match layer {
                LayerSource::Referenced {
                    reference: LayerRef::External(external),
                    ..
                } => {
                    layers.push(LayerRef::External(external.clone()));
                }
                layer => layers.push(LayerRef::Managed(
                    self.store_source(layer.clone(), role, chain.managed_repo_blob_url.as_deref())
                        .await?,
                )),
            }
        }
        Ok(layers)
    }

    async fn store_memory_chain(
        &mut self,
        chain: &LayerChain,
    ) -> RepositoryResult<Vec<ManagedLayer>> {
        let mut layers = Vec::with_capacity(chain.layers.len());
        for layer in &chain.layers {
            layers.push(
                self.store_source(
                    layer.clone(),
                    LayerRole::Memory,
                    chain.managed_repo_blob_url.as_deref(),
                )
                .await?,
            );
        }
        Ok(layers)
    }

    async fn store_source(
        &mut self,
        source: LayerSource,
        role: LayerRole,
        managed_repo: Option<&str>,
    ) -> RepositoryResult<ManagedLayer> {
        let file = match source {
            LayerSource::Referenced {
                reference: LayerRef::Managed(metadata),
                local_file,
            } => {
                let key = BlobKey::Layer {
                    digest: metadata.digest.clone(),
                    role,
                };
                match self.repository.locate_blob(&key)? {
                    BlobLocation::Remote(_) => {
                        let current = self
                            .repository
                            .managed_layers_repo_blob_url()
                            .unwrap_or_default();
                        if !managed_repo.is_some_and(|source| same_repo_blob_url(source, &current))
                        {
                            return Err(RepositoryError::Unsupported {
                                feature: "managed layer belongs to a different repository".into(),
                            });
                        }
                    }
                    BlobLocation::Local(destination) => {
                        if managed_repo.is_some() {
                            return Err(RepositoryError::Unsupported {
                                feature: "POSIX publication of a remote managed layer".into(),
                            });
                        }
                        let source = local_file.as_deref().unwrap_or(&destination);
                        if !source.is_file() {
                            return Err(RepositoryError::ArtifactNotFound {
                                artifact: format!("managed layer '{}'", metadata.digest),
                            });
                        }
                        match tokio::fs::metadata(&destination).await {
                            Ok(stored) if stored.len() != metadata.size => {
                                return Err(RepositoryError::IntegrityMismatch {
                                    artifact: format!("managed layer '{}'", metadata.digest),
                                    expected: metadata.size.to_string(),
                                    actual: stored.len().to_string(),
                                });
                            }
                            Ok(_) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => {
                                return Err(RepositoryError::backend(
                                    "read managed layer metadata",
                                    error,
                                ))
                            }
                        }
                        self.repository
                            .put_blob(&key, BlobBody::File(source))
                            .await?;
                    }
                }
                return Ok(metadata);
            }
            LayerSource::Referenced {
                reference: LayerRef::External(_),
                ..
            } => {
                return Err(RepositoryError::Unsupported {
                    feature: "external memory layers".into(),
                });
            }
            LayerSource::File(file) => file,
        };
        let staging = self.repository.staging_file()?;
        let (upload, uuid) =
            super::recontainerize::prepare_layer_file_upload(file, self.compression, staging)
                .await?;
        self.put_prepared(upload, uuid, role).await
    }

    /// The layer's digest and size describe exactly the prepared (possibly
    /// recontainerized) bytes.
    async fn put_prepared(
        &mut self,
        upload: PreparedLayerUpload,
        uuid: Option<String>,
        role: LayerRole,
    ) -> RepositoryResult<ManagedLayer> {
        let layer = ManagedLayer {
            digest: upload.digest().to_string(),
            size: upload.size(),
            uuid,
        };
        let key = BlobKey::Layer {
            digest: layer.digest.clone(),
            role,
        };
        self.repository
            .put_blob(&key, BlobBody::File(upload.path()))
            .await?;
        self.local_layers.push(upload);
        Ok(layer)
    }
}

fn same_repo_blob_url(left: &str, right: &str) -> bool {
    !left.is_empty() && left.trim_end_matches('/') == right.trim_end_matches('/')
}

/// Storing a source-registry image's local lowers here would leave its other
/// lowers in a different registry.
fn fallback_would_mix_sources(layers: &LayerChain, managed_layers_repo_blob_url: &str) -> bool {
    layers.layers.iter().any(|layer| {
        let source = match layer {
            LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer { repo_blob_url, .. }),
                ..
            } => Some(repo_blob_url.as_str()),
            LayerSource::Referenced {
                reference: LayerRef::Managed(_),
                ..
            } => layers.managed_repo_blob_url.as_deref(),
            LayerSource::File(_) => None,
        };
        source.is_some_and(|source| !same_repo_blob_url(source, managed_layers_repo_blob_url))
    })
}

#[cfg(test)]
mod tests {
    use crate::snapshot::ExternalLayer;
    use serde_json::json;

    use super::*;
    use crate::snapshot::mock::MockRepository;

    fn write_test_image(path: &Path, value: serde_json::Value) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&value).expect("serialize image config"),
        )
        .expect("write image config");
    }

    #[test]
    fn default_policy_stores_raw_layers_as_managed_layers() {
        let store = LayerStore::new(&MockRepository);
        assert_eq!(store.compression, OverlaybdCompactOutput::Raw);
        assert_eq!(
            store.image_storage,
            SnapshotImageStoragePolicy::ObjectStorage
        );
        assert_eq!(store.managed_layers_repo_blob_url, None);
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

        // Remote lowers are referenced, never stored.
        let layers = LayerStore::new(&MockRepository)
            .store_disk_layers(&image, LayerRole::Rootfs, false)
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

    #[test]
    fn detects_source_registry_fallback_source_mixing() {
        let external = LayerChain::from(vec![LayerSource::Referenced {
            reference: LayerRef::External(ExternalLayer {
                digest: "sha256:base".into(),
                size: 4096,
                repo_blob_url: "https://registry.example/v2/ns/image/blobs".into(),
            }),
            local_file: Some(std::path::PathBuf::from("cached-base.commit")),
        }]);
        assert!(fallback_would_mix_sources(
            &external,
            "s3://bucket/prefix/managed-layers"
        ));

        let mut managed = LayerChain {
            layers: vec![LayerSource::Referenced {
                reference: LayerRef::Managed(ManagedLayer {
                    digest: "sha256:base".into(),
                    size: 4096,
                    uuid: None,
                }),
                local_file: None,
            }],
            managed_repo_blob_url: Some("s3://bucket/prefix/managed-layers".into()),
        };
        assert!(!fallback_would_mix_sources(
            &managed,
            "s3://bucket/prefix/managed-layers"
        ));
        managed.managed_repo_blob_url = Some("s3://other/managed-layers".into());
        assert!(fallback_would_mix_sources(
            &managed,
            "s3://bucket/prefix/managed-layers"
        ));
        managed.managed_repo_blob_url = None;
        assert!(!fallback_would_mix_sources(
            &managed,
            "s3://bucket/prefix/managed-layers"
        ));
    }
}

#[cfg(test)]
mod posix_tests;
