//! Storing a snapshot's files and layers through the repository's storage
//! primitives and optional source-registry export. The rules here are the same
//! on every backend; only the repository's [`PublishPolicy`] differs.

use agentenv_metadata::MetaClient;
use tracing::{debug, info, warn};

use super::acr::{
    rollback_disk_publications, AcrDiskImageExporter, DiskImageSubject, SnapshotOciConfigInput,
};
use super::catalog;
use super::content::delete_snapshot_artifacts;
use super::startup_pack::build_startup_manifest;
use super::{
    CommittedAttachedDrive, MemoryStartupPackInfo, PersistedDiskImagePublication, SnapshotAlias,
    SnapshotConfig, SnapshotId, SnapshotManifest, SnapshotRecord, SnapshotRuntimeManifest,
    SnapshotSource, StartupRecording,
};
use crate::cfg::SnapshotImageStoragePolicy;
use crate::layer::{ExternalLayer, LayerChain, LayerRef, LayerSource, PreparedLayerUpload};
use crate::repository::{
    same_repo_blob_url, BlobBody, BlobKey, LayerRole, Repository, RepositoryError,
    RepositoryLayerWriter, RepositoryResult, SnapshotFile,
};

/// Publish artifacts before committing their record to the metadata directory.
pub(crate) async fn publish_snapshot(
    meta: &MetaClient,
    repository: &dyn Repository,
    id: SnapshotId,
    alias: Option<SnapshotAlias>,
    config: SnapshotConfig,
    manifest: SnapshotRuntimeManifest,
    recording: Option<StartupRecording>,
) -> RepositoryResult<(SnapshotRecord, Vec<PreparedLayerUpload>)> {
    config
        .validate()
        .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
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
    let internal = matches!(config.source, SnapshotSource::Builder);
    catalog::prepare_distributed(meta, &id, alias.as_ref(), internal).await?;

    let (mut committed, local_layers) =
        match store_snapshot(repository, &id, &config, &manifest).await {
            Ok(stored) => stored,
            Err(error) => {
                if can_rollback_distributed(meta, &id, internal, &error).await {
                    if let Err(cleanup) = repository.delete_snapshot_blobs(&id).await {
                        warn!(snapshot_id = %id, %cleanup, "failed to roll back snapshot files");
                    }
                }
                return Err(error);
            }
        };
    committed.memory_startup = match recording {
        Some(recording) => store_startup_manifest(repository, &id, recording).await,
        None => None,
    };
    let disk_publications = committed.disk_publications.clone();
    match catalog::commit_distributed(
        meta,
        repository,
        id.clone(),
        alias,
        config,
        committed,
        &local_layers,
    )
    .await
    {
        Ok(record) => {
            debug!(snapshot_id = %record.id, "published snapshot");
            Ok((record, local_layers))
        }
        Err(error) => {
            if can_rollback_distributed(meta, &id, internal, &error).await {
                if let Err(cleanup) =
                    delete_snapshot_artifacts(repository, &id, &disk_publications).await
                {
                    warn!(snapshot_id = %id, error = %cleanup, "failed to roll back snapshot files");
                }
            } else {
                warn!(snapshot_id = %id, "retaining snapshot artifacts for reconciliation or retry");
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
    let Some(_guard) = super::startup_pack::StartupManifestTaskGuard::try_register() else {
        recording.detach();
        return None;
    };
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
    if super::startup_pack::startup_manifest_abort_requested() {
        return None;
    }
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
    meta: &MetaClient,
    id: &SnapshotId,
    internal: bool,
    error: &RepositoryError,
) -> bool {
    if matches!(error, RepositoryError::Backend { .. }) {
        return false;
    }
    // A Local directory entry still needs its artifact record after a failed promotion.
    matches!(
        meta.get_snapshot(id.to_string(), internal).await,
        Ok(entry) if !entry.as_ref().is_some_and(agentenv_metadata::SnapshotEntry::is_visible)
    )
}

/// Stores one captured snapshot's files and layers. On failure it removes
/// what it stored, except shared content-addressed layers.
pub(crate) async fn store_snapshot<R>(
    repository: &R,
    id: &SnapshotId,
    config: &SnapshotConfig,
    manifest: &SnapshotRuntimeManifest,
) -> RepositoryResult<(SnapshotManifest, Vec<PreparedLayerUpload>)>
where
    R: Repository + ?Sized,
{
    let mut layers = RepositoryLayerWriter::new(repository);
    let exporter = matches!(
        repository.publish_policy().image_storage,
        SnapshotImageStoragePolicy::SourceRegistry
    )
    .then(|| AcrDiskImageExporter::new(layers.compression()));
    let mut disk_publications = Vec::new();
    let result = async {
        // Rootfs is exported with the effective runtime config.
        let rootfs_config =
            SnapshotOciConfigInput::new(&config.context, config.image_configs.rootfs_config());
        let rootfs_layers = store_disk_image(
            &mut layers,
            &mut disk_publications,
            exporter.as_ref(),
            id,
            DiskImageSubject::Rootfs,
            &manifest.rootfs.layers,
            Some(rootfs_config),
        )
        .await?;
        let memory_layers = layers.store_memory_chain(&manifest.memory.layers).await?;

        repository
            .put_blob(
                &BlobKey::Snapshot(id.clone(), SnapshotFile::VmState),
                BlobBody::File(&manifest.vm_state.path),
            )
            .await?;
        let manifest_bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|error| RepositoryError::backend("serialize firecracker manifest", error))?;
        repository
            .put_blob(
                &BlobKey::Snapshot(id.clone(), SnapshotFile::FirecrackerManifest),
                BlobBody::Bytes(manifest_bytes),
            )
            .await?;

        let mut attached_drives = Vec::with_capacity(manifest.attached_drives.len());
        for drive in &manifest.attached_drives {
            // Only logical layers are committed; the build-time image config
            // is regenerated by the runtime resolver.
            let drive_layers = store_disk_image(
                &mut layers,
                &mut disk_publications,
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
                layers: drive_layers,
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
    .await;
    match result {
        Ok((rootfs_layers, memory_layers, attached_drives)) => Ok((
            SnapshotManifest {
                rootfs_layers,
                attached_drives,
                memory_layers,
                disk_publications,
                memory_startup: None,
            },
            layers.into_local_layers(),
        )),
        Err(error) => {
            rollback_disk_publications(id, &disk_publications).await;
            Err(error)
        }
    }
}

/// Publishes a disk image to its source registry when the policy asks
/// for it, otherwise (or when the registry cannot take it) stores its
/// local layers here.
async fn store_disk_image<R: Repository + ?Sized>(
    layers: &mut RepositoryLayerWriter<'_, R>,
    disk_publications: &mut Vec<PersistedDiskImagePublication>,
    exporter: Option<&AcrDiskImageExporter>,
    id: &SnapshotId,
    subject: DiskImageSubject,
    chain: &LayerChain,
    config: Option<SnapshotOciConfigInput<'_>>,
) -> RepositoryResult<Vec<LayerRef>> {
    let role = match subject {
        DiskImageSubject::Rootfs => LayerRole::Rootfs,
        DiskImageSubject::AttachedDrive { .. } => LayerRole::AttachedDrive,
    };
    let Some(exporter) = exporter else {
        return layers.store_disk_chain(chain, role).await;
    };
    match exporter.export(id, subject.clone(), chain, config).await {
        Ok(outcome) => {
            disk_publications.extend(outcome.publication);
            layers.record_local_layers(outcome.local_layers);
            Ok(outcome.layers)
        }
        Err(RepositoryError::Unsupported { feature }) => {
            let managed_url = layers.managed_layers_repo_blob_url().unwrap_or("");
            if fallback_would_mix_sources(chain, managed_url) {
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
            layers.store_disk_chain(chain, role).await
        }
        Err(error) => Err(error),
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
    use crate::snapshot::ManagedLayerRef;

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
