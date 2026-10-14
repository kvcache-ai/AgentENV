//! Publishing a captured snapshot: the repository stores its files and layers,
//! then the catalog commits its record.

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{debug, info, warn};

use super::catalog;
use super::recontainerize::PreparedLayerUpload;
use super::startup_pack::{
    build_startup_manifest, startup_manifest_abort_notify, startup_manifest_abort_requested,
    StartupManifestTaskGuard,
};
use super::store::{self, StoredSnapshot};
use super::{
    CommittedSnapshot, SnapshotId, SnapshotPublishMetadata, SnapshotRecord, StartupRecording,
};
use crate::repository::{
    BlobBody, BlobKey, DocumentKey, Repository, RepositoryError, RepositoryResult, SnapshotFile,
};
use crate::snapshot::SandboxSnapshotManifest;

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
    manifest: SandboxSnapshotManifest,
) -> RepositoryResult<SnapshotPublication>
where
    R: Repository + ?Sized,
{
    validate_attached_drives(&manifest)?;
    // Fail before uploading anything; the commit checks both again.
    ensure_not_committed(repository, &metadata.id).await?;
    if let Some(alias) = metadata.alias.as_ref() {
        catalog::ensure_alias_available(repository, alias, &metadata.id).await?;
    }

    let stored = store::store_snapshot(repository, &metadata, &manifest).await?;
    let StoredSnapshot {
        rootfs_layers,
        memory_layers,
        attached_drives,
        disk_publications,
        local_layers,
    } = stored;
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
        // Attached later by the startup-manifest continuation.
        memory_startup: None,
    };
    match catalog::commit(repository, &metadata, committed).await {
        Ok(record) => {
            debug!(snapshot_id = %record.id, "published snapshot");
            Ok(SnapshotPublication {
                record,
                local_layers,
            })
        }
        Err(error) => {
            if let Err(cleanup) =
                store::delete_snapshot(repository, &metadata.id, &disk_publications).await
            {
                warn!(snapshot_id = %metadata.id, error = %cleanup, "failed to roll back snapshot files");
            }
            Err(error)
        }
    }
}

fn validate_attached_drives(manifest: &SandboxSnapshotManifest) -> RepositoryResult<()> {
    let mut drive_ids = HashSet::new();
    for drive in &manifest.attached_drives {
        if !drive_ids.insert(drive.drive_id.clone()) {
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
    Ok(())
}

/// Republishing a committed snapshot would overwrite and then, on failure,
/// delete the files its record points at.
async fn ensure_not_committed<R>(repository: &R, id: &SnapshotId) -> RepositoryResult<()>
where
    R: Repository + ?Sized,
{
    let Some(bytes) = repository
        .get_document(DocumentKey::SnapshotRecord(id.clone()))
        .await?
    else {
        return Ok(());
    };
    let committed = serde_json::from_slice::<serde_json::Value>(&bytes)
        .map_err(|error| RepositoryError::backend(format!("parse snapshot record '{id}'"), error))?
        .get("committed")
        .is_some_and(|committed| !committed.is_null());
    if committed {
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
