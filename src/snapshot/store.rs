//! Storing a snapshot's files and layers through the repository's storage
//! primitives and optional source-registry export. The rules here are the same
//! on every backend; only the repository's [`PublishPolicy`] differs.

use agentenv_domain::SnapshotContent;
use overlaybd::config::OverlaybdCompactOutput;
use tracing::{debug, info, warn};

use super::catalog::SnapshotCatalog;
use super::file_catalog::RepositoryCatalog;
use super::image_export::acr::{AcrDiskImageExporter, DiskImageSubject, SnapshotOciConfigInput};
use super::startup_pack::build_startup_manifest;
use super::{
    CommittedAttachedDrive, ManagedLayerRef, MemoryStartupPackInfo, PersistedDiskImagePublication,
    SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotPublishMetadata, SnapshotRecord,
    SnapshotRuntimeManifest, SnapshotSource, StartupRecording,
};
use crate::cfg::SnapshotImageStoragePolicy;
use crate::layer::{ExternalLayer, LayerChain, LayerRef, LayerSource, PreparedLayerUpload};
use crate::repository::{
    same_repo_blob_url, BlobBody, BlobKey, LayerRole, Repository, RepositoryError,
    RepositoryLayerWriter, RepositoryResult, SnapshotFile,
};

/// A committed snapshot and the local upload files kept alive for post-commit
/// publication. Temporary paths are never stored in the durable record.
#[derive(Debug)]
pub struct SnapshotPublication {
    pub record: SnapshotRecord,
    pub(crate) local_layers: Vec<PreparedLayerUpload>,
}

pub(crate) async fn publish<R>(
    repository: &R,
    metadata: SnapshotPublishMetadata,
    manifest: SnapshotRuntimeManifest,
) -> RepositoryResult<SnapshotPublication>
where
    R: Repository + ?Sized,
{
    let catalog = RepositoryCatalog::new(repository);
    publish_snapshot(&catalog, repository, metadata, manifest, None).await
}

/// Publish artifacts before committing their record to the supplied directory.
pub async fn publish_snapshot<R: Repository + ?Sized>(
    catalog: &dyn SnapshotCatalog,
    repository: &R,
    metadata: SnapshotPublishMetadata,
    manifest: SnapshotRuntimeManifest,
    recording: Option<StartupRecording>,
) -> RepositoryResult<SnapshotPublication> {
    metadata
        .validate()
        .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
    if metadata.snapshot_type != SnapshotPlacement::Distributed {
        return Err(RepositoryError::Unsupported {
            feature: "Distributed publication required".to_string(),
        });
    }
    manifest
        .validate_attached_drives()
        .map_err(|error| RepositoryError::InvalidRequest {
            reason: error.to_string(),
        })?;
    if manifest.rootfs.layers.layers.is_empty() {
        return Err(RepositoryError::InvalidRequest {
            reason: "snapshot rootfs has no layers".into(),
        });
    }
    if let Some(record) = catalog
        .prepare_distributed(&metadata.id, metadata.alias.as_ref())
        .await?
    {
        return Ok(SnapshotPublication {
            record,
            local_layers: Vec::new(),
        });
    }

    let stored = match store_snapshot(repository, &metadata, &manifest).await {
        Ok(stored) => stored,
        Err(error) => {
            if matches!(&metadata.config.source, SnapshotSource::Template { .. })
                && can_rollback_distributed(catalog, &metadata.id, &error).await
            {
                if let Err(cleanup) = repository.delete_snapshot_blobs(&metadata.id).await {
                    warn!(snapshot_id = %metadata.id, %cleanup, "failed to roll back snapshot files");
                }
            }
            return Err(error);
        }
    };
    let StoredSnapshot {
        rootfs_layers,
        memory_layers,
        attached_drives,
        disk_publications,
        local_layers,
    } = stored;
    let memory_startup = match recording {
        Some(recording) => store_startup_manifest(repository, &metadata.id, recording).await,
        None => None,
    };
    let committed = SnapshotManifest {
        rootfs_layers,
        attached_drives,
        memory_layers,
        disk_publications: disk_publications.clone(),
        memory_startup,
    };
    let upload_sources: Vec<_> = local_layers
        .iter()
        .map(|layer| (layer.digest(), layer.path()))
        .collect();
    match catalog
        .commit_distributed(&metadata, committed, &upload_sources)
        .await
    {
        Ok(record) => {
            debug!(snapshot_id = %record.id, "published snapshot");
            Ok(SnapshotPublication {
                record,
                local_layers,
            })
        }
        Err(error) => {
            if matches!(&metadata.config.source, SnapshotSource::Template { .. })
                && can_rollback_distributed(catalog, &metadata.id, &error).await
            {
                if let Err(cleanup) =
                    delete_snapshot(repository, &metadata.id, &disk_publications).await
                {
                    warn!(snapshot_id = %metadata.id, error = %cleanup, "failed to roll back snapshot files");
                }
            } else {
                warn!(snapshot_id = %metadata.id, "retaining snapshot artifacts for reconciliation or retry");
            }
            Err(error)
        }
    }
}

async fn store_startup_manifest<R: Repository + ?Sized>(
    repository: &R,
    id: &SnapshotId,
    recording: StartupRecording,
) -> Option<MemoryStartupPackInfo> {
    let StartupRecording { trace, keep_alive } = recording;
    let _keep_alive = keep_alive;
    let path = match trace.await {
        Ok(Some(path)) => path,
        Ok(None) => return None,
        Err(error) => {
            warn!(%error, snapshot_id = %id, "startup recording failed");
            return None;
        }
    };
    let stored = async {
        let (info, bytes) = build_startup_manifest(&path).await?;
        repository
            .put_blob(
                &BlobKey::Snapshot(id.clone(), SnapshotFile::StartupManifest),
                BlobBody::Bytes(bytes),
            )
            .await?;
        anyhow::Ok(info)
    }
    .await;
    match stored {
        Ok(info) => Some(info),
        Err(error) => {
            warn!(%error, snapshot_id = %id, "startup manifest failed (best-effort)");
            None
        }
    }
}

async fn can_rollback_distributed(
    catalog: &dyn SnapshotCatalog,
    id: &SnapshotId,
    error: &RepositoryError,
) -> bool {
    if matches!(error, RepositoryError::Backend { .. }) {
        return false;
    }
    // A Local directory entry still needs its artifact record after a failed promotion.
    matches!(catalog.get(&id.to_string()).await, Ok(record) if !record.as_ref().is_some_and(|record| {
        record.manifest.is_some()
            || (record.snapshot_type == SnapshotPlacement::Local
                && catalog.committed_content_is_immutable())
    }))
}

/// What was stored for one snapshot before its record is committed.
#[derive(Debug)]
pub(crate) struct StoredSnapshot {
    pub(crate) rootfs_layers: Vec<LayerRef>,
    pub(crate) memory_layers: Vec<ManagedLayerRef>,
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
    manifest: &SnapshotRuntimeManifest,
) -> RepositoryResult<StoredSnapshot>
where
    R: Repository + ?Sized,
{
    let mut store = SnapshotArtifactPublisher::new(repository);
    match store.store_snapshot(metadata, manifest).await {
        Ok((rootfs_layers, memory_layers, attached_drives)) => Ok(StoredSnapshot {
            rootfs_layers,
            memory_layers,
            attached_drives,
            disk_publications: store.disk_publications,
            local_layers: store.layers.into_local_layers(),
        }),
        Err(error) => {
            rollback_disk_publications(&metadata.id, &store.disk_publications).await;
            Err(error)
        }
    }
}

pub(super) async fn read_snapshot_content(
    repository: &dyn Repository,
    id: &SnapshotId,
) -> RepositoryResult<Option<SnapshotContent>> {
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
        return Ok(Some(SnapshotContent::from(&record)));
    }
    serde_json::from_value(value).map(Some).map_err(parse_error)
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

struct SnapshotArtifactPublisher<'a, R: ?Sized> {
    repository: &'a R,
    layers: RepositoryLayerWriter<'a, R>,
    image_storage: SnapshotImageStoragePolicy,
    disk_publications: Vec<PersistedDiskImagePublication>,
}

impl<'a, R: Repository + ?Sized> SnapshotArtifactPublisher<'a, R> {
    fn new(repository: &'a R) -> Self {
        let policy = repository.publish_policy();
        Self {
            repository,
            layers: RepositoryLayerWriter::new(repository),
            image_storage: policy.image_storage,
            disk_publications: Vec::new(),
        }
    }

    async fn store_snapshot(
        &mut self,
        metadata: &SnapshotPublishMetadata,
        manifest: &SnapshotRuntimeManifest,
    ) -> RepositoryResult<(
        Vec<LayerRef>,
        Vec<ManagedLayerRef>,
        Vec<CommittedAttachedDrive>,
    )> {
        let id = &metadata.id;
        let exporter = matches!(
            self.image_storage,
            SnapshotImageStoragePolicy::SourceRegistry
        )
        .then(|| AcrDiskImageExporter::new(self.layers.compression()));

        // Rootfs is exported with the effective runtime config.
        let rootfs_config = SnapshotOciConfigInput::new(
            &metadata.config.context,
            metadata.config.image_configs.rootfs_config(),
        );
        let rootfs_layers = self
            .store_disk_image(
                exporter.as_ref(),
                id,
                DiskImageSubject::Rootfs,
                &manifest.rootfs.layers,
                Some(rootfs_config),
            )
            .await?;
        let memory_layers = self
            .layers
            .store_memory_chain(&manifest.memory.layers)
            .await?;

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
    ) -> RepositoryResult<Vec<LayerRef>> {
        let role = match subject {
            DiskImageSubject::Rootfs => LayerRole::Rootfs,
            DiskImageSubject::AttachedDrive { .. } => LayerRole::AttachedDrive,
        };
        let Some(exporter) = exporter else {
            return self.layers.store_disk_chain(layers, role).await;
        };
        match exporter.export(id, subject.clone(), layers, config).await {
            Ok(outcome) => {
                self.disk_publications.extend(outcome.publication);
                self.layers.record_local_layers(outcome.local_layers);
                Ok(outcome.layers)
            }
            Err(RepositoryError::Unsupported { feature }) => {
                let managed_url = self.layers.managed_layers_repo_blob_url().unwrap_or("");
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
                self.layers.store_disk_chain(layers, role).await
            }
            Err(error) => Err(error),
        }
    }
}

/// Storing a source-registry image's local lowers here would leave its other
/// lowers in a different registry.
fn fallback_would_mix_sources(layers: &LayerChain, managed_layers_repo_blob_url: &str) -> bool {
    layers.layers.iter().any(|layer| {
        let source = match layer {
            LayerSource::Referenced {
                reference: LayerRef::External(ExternalLayer { repo_blob_url, .. }),
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
    use super::*;
    use crate::snapshot::mock::MockRepository;

    #[test]
    fn default_policy_stores_raw_layers_as_managed_layers() {
        let store = SnapshotArtifactPublisher::new(&MockRepository);
        assert_eq!(store.layers.compression(), OverlaybdCompactOutput::Raw);
        assert_eq!(
            store.image_storage,
            SnapshotImageStoragePolicy::ObjectStorage
        );
        assert_eq!(store.layers.managed_layers_repo_blob_url(), None);
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
                reference: LayerRef::Managed(ManagedLayerRef {
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
