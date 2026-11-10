//! Publishing a captured snapshot: the repository stores its files and layers,
//! then the catalog commits its record.

use std::sync::Arc;

use tracing::{debug, info, warn};

use super::catalog::{self, RepositoryCatalog, SnapshotCatalog};
use super::recontainerize::PreparedLayerUpload;
use super::startup_pack::{
    build_startup_manifest, startup_manifest_abort_notify, startup_manifest_abort_requested,
    StartupManifestTaskGuard,
};
use super::store::{self, StoredSnapshot};
use super::{
    SnapshotId, SnapshotManifest, SnapshotPublishMetadata, SnapshotRecord, StartupRecording,
};
use crate::repository::{
    BlobBody, BlobKey, DocumentKey, Repository, RepositoryError, RepositoryResult, SnapshotFile,
};
use crate::snapshot::SnapshotRuntimeManifest;

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
    if metadata.snapshot_type != super::SnapshotPlacement::Distributed {
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
    catalog
        .prepare_distributed(&metadata.id, metadata.alias.as_ref())
        .await?;

    let stored = match store::store_snapshot(repository, &metadata, &manifest).await {
        Ok(stored) => stored,
        Err(error) => {
            if can_rollback_distributed(catalog, &metadata.id, &error).await {
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
    match catalog.commit_distributed(&metadata, committed).await {
        Ok(record) => {
            debug!(snapshot_id = %record.id, "published snapshot");
            Ok(SnapshotPublication {
                record,
                local_layers,
            })
        }
        Err(error) => {
            if can_rollback_distributed(catalog, &metadata.id, &error).await {
                if let Err(cleanup) =
                    store::delete_snapshot(repository, &metadata.id, &disk_publications).await
                {
                    warn!(snapshot_id = %metadata.id, error = %cleanup, "failed to roll back snapshot files");
                }
            } else {
                warn!(snapshot_id = %metadata.id, "snapshot commit may be durable; retaining its artifacts");
            }
            Err(error)
        }
    }
}

async fn store_startup_manifest<R: Repository + ?Sized>(
    repository: &R,
    id: &SnapshotId,
    recording: StartupRecording,
) -> Option<super::MemoryStartupPackInfo> {
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
    !matches!(error, RepositoryError::Backend { .. })
        && matches!(catalog.get(&id.to_string()).await, Ok(record) if !record.as_ref().is_some_and(|record| record.snapshot_type == super::SnapshotPlacement::Distributed && record.manifest.is_some()))
}

/// Republishing a committed snapshot would overwrite and then, on failure,
/// delete the files its record points at.
pub(super) async fn ensure_not_committed<R>(repository: &R, id: &SnapshotId) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    let Some(bytes) = repository
        .get_document(DocumentKey::SnapshotRecord(id.clone()))
        .await?
    else {
        return Ok(());
    };
    let record: SnapshotRecord = serde_json::from_slice(&bytes).map_err(|error| {
        RepositoryError::backend(format!("parse snapshot record '{id}'"), error)
    })?;
    if record.deleting
        || (record.manifest.is_some() && record.snapshot_type != super::SnapshotPlacement::Local)
    {
        return Err(RepositoryError::InvalidRequest {
            reason: format!("snapshot '{id}' is already committed"),
        });
    }
    Ok(())
}

/// Builds the startup manifest once `recording` finishes and attaches it to
/// the committed snapshot. Publication never waits on it.
pub(crate) fn spawn_startup_manifest(
    repository: Arc<dyn Repository>,
    id: SnapshotId,
    recording: StartupRecording,
) {
    if !crate::cfg::ConfigManager::global_config()
        .snapshot
        .memory_startup_pack
        .enabled
    {
        return;
    }
    match StartupManifestTaskGuard::try_register() {
        Some(guard) => {
            tokio::spawn(finish_startup_manifest(
                repository,
                id,
                recording,
                guard,
                startup_manifest_abort_requested,
            ));
        }
        // Cleanup retains the capture lease without delaying publication.
        None => recording.detach(),
    }
}

pub(crate) async fn finish_startup_manifest(
    repository: Arc<dyn Repository>,
    id: SnapshotId,
    recording: StartupRecording,
    _guard: StartupManifestTaskGuard,
    abort_requested: impl Fn() -> bool,
) {
    let StartupRecording {
        mut trace,
        keep_alive,
    } = recording;
    // Hold the captured artifacts alive until the manifest is stored.
    let _keep_alive = keep_alive;
    if abort_requested() {
        // The recorder may still be using captured VM/device artifacts.
        let _ = trace.await;
        return;
    }
    let trace_path = match tokio::select! {
        joined = &mut trace => joined,
        _ = startup_manifest_abort_notify() => {
            let _ = trace.await;
            return;
        },
    } {
        Ok(Some(path)) => path,
        Ok(None) => {
            debug!(snapshot_id = %id, "startup manifest recording produced no trace");
            return;
        }
        Err(error) => {
            warn!(%error, snapshot_id = %id, "startup manifest recording join failed");
            return;
        }
    };
    if abort_requested() {
        return;
    }
    let stored = async {
        let (info, manifest) = build_startup_manifest(&trace_path).await.map_err(|error| {
            RepositoryError::Backend {
                message: "build startup manifest".to_owned(),
                source: Some(error),
            }
        })?;
        info!(snapshot_id = %id, manifest_bytes = manifest.len(), "startup manifest built");
        let key = BlobKey::Snapshot(id.clone(), SnapshotFile::StartupManifest);
        repository.put_blob(&key, BlobBody::Bytes(manifest)).await?;
        RepositoryResult::Ok(info)
    }
    .await;
    let info = match stored {
        Ok(info) => info,
        Err(error) => {
            warn!(%error, snapshot_id = %id, "startup manifest failed (best-effort)");
            return;
        }
    };
    // Attach even when aborting, or the stored manifest is never removed.
    match catalog::attach_startup_manifest(repository.as_ref(), &id, info).await {
        Ok(true) => {}
        Ok(false) => {
            // The delete may have removed the snapshot's files before the
            // manifest was stored.
            debug!(snapshot_id = %id, "snapshot deleted while attaching its startup manifest");
            if let Err(error) = repository.delete_snapshot_blobs(&id).await {
                warn!(%error, snapshot_id = %id, "failed to remove the files of a deleted snapshot");
            }
        }
        Err(error) => {
            warn!(%error, snapshot_id = %id, "attach startup manifest failed (best-effort)");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::snapshot::mock::MockRepository;

    struct LeaseProbe(Arc<AtomicBool>);

    impl Drop for LeaseProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn aborted_continuation_keeps_capture_lease_until_trace_finishes() {
        let lease_dropped = Arc::new(AtomicBool::new(false));
        let trace_finished = Arc::new(AtomicBool::new(false));
        let (release_trace, trace_release) = tokio::sync::oneshot::channel::<()>();
        let trace_finished_in_task = Arc::clone(&trace_finished);
        let recording = StartupRecording {
            trace: tokio::spawn(async move {
                trace_release.await.expect("release trace");
                trace_finished_in_task.store(true, Ordering::SeqCst);
                None
            }),
            keep_alive: Box::new(LeaseProbe(Arc::clone(&lease_dropped))),
        };
        let guard = StartupManifestTaskGuard::try_register().expect("register continuation");
        let mut continuation = tokio::spawn(finish_startup_manifest(
            Arc::new(MockRepository),
            SnapshotId::generate(),
            recording,
            guard,
            || true,
        ));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut continuation)
                .await
                .is_err(),
            "aborted continuation must wait for the pending recorder"
        );
        assert!(!trace_finished.load(Ordering::SeqCst));
        assert!(!lease_dropped.load(Ordering::SeqCst));

        release_trace.send(()).expect("trace still pending");
        continuation.await.expect("continuation");
        assert!(trace_finished.load(Ordering::SeqCst));
        assert!(lease_dropped.load(Ordering::SeqCst));
    }
}
