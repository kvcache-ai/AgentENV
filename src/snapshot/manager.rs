use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use futures::{stream, StreamExt};
use tracing::warn;

use super::backends::build_snapshot_backend;
use super::catalog::SnapshotCatalog;
use super::local_store::LocalSnapshotStore;
use super::p2p::SnapshotP2pArtifact;
use super::publish::spawn_startup_manifest;
use super::types::{now_unix_ms, RuntimeArtifactLease, SNAPSHOT_ARTIFACT_LAYOUT};
use super::{SnapshotListFilter, SnapshotPublication, SnapshotRuntimeResolver};
use crate::p2p::P2pTransport;
use crate::repository::{Repository, RepositoryError};
use crate::snapshot::{
    CommittedSnapshot, ManagedLayer, OverlaybdLayerRef, RunnableSnapshot, SandboxSnapshotManifest,
    SnapshotId, SnapshotPlacement, SnapshotPublishMetadata, SnapshotRecord,
};

/// Concurrency limit for publishing snapshot artifacts to P2P after commit.
const SNAPSHOT_P2P_PUBLISH_CONCURRENCY: usize = 8;

fn managed_layer_uuids(layers: &[OverlaybdLayerRef]) -> HashSet<String> {
    layers
        .iter()
        .filter_map(|layer| match layer {
            OverlaybdLayerRef::Managed(managed) => managed.uuid.clone(),
            OverlaybdLayerRef::External(_) => None,
        })
        .collect()
}

fn managed_layer_uuids_from_managed(layers: &[ManagedLayer]) -> HashSet<String> {
    layers
        .iter()
        .filter_map(|layer| layer.uuid.clone())
        .collect()
}

/// Every layer digest the committed record references for one snapshot
/// subject (rootfs or one attached drive), managed and external alike.
fn committed_layer_digests(layers: &[OverlaybdLayerRef]) -> HashSet<String> {
    layers
        .iter()
        .map(|layer| match layer {
            OverlaybdLayerRef::Managed(managed) => managed.digest.clone(),
            OverlaybdLayerRef::External(external) => external.digest.clone(),
        })
        .collect()
}

fn committed_memory_layer_digests(layers: &[ManagedLayer]) -> HashSet<String> {
    layers.iter().map(|layer| layer.digest.clone()).collect()
}

#[derive(Clone)]
/// Coordinates committed snapshot lifecycle operations over repository-backed state.
///
/// Durable reachability of committed snapshots is owned entirely by the
/// [`Repository`] (PosixFS `managed-layers/`, OSS object storage, or the
/// source registry). The node-local overlaybd layer cache (`image-cache/commits/`)
/// is reclaimable - committed snapshots never pin it - so this manager records no
/// local image ref pins.
pub struct SnapshotManager {
    repository: Arc<dyn Repository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
    local_store: Option<LocalSnapshotStore>,
    node_id: String,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
}

impl SnapshotManager {
    /// Builds a manager using the configured repository backend.
    pub fn new(
        node_id: String,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> anyhow::Result<Self> {
        let (repository, runtime_resolver) = build_snapshot_backend(p2p_transport.clone())?;
        let local_store_root = crate::cfg::ConfigManager::global_config()
            .snapshot
            .local_cache_path
            .clone();
        let local_store = Some(LocalSnapshotStore::new(local_store_root));
        Ok(Self {
            repository,
            runtime_resolver,
            local_store,
            node_id,
            p2p_transport,
        })
    }

    /// Builds a manager from the given components.
    pub fn from_parts(
        repository: Arc<dyn Repository>,
        runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Self {
        Self {
            repository,
            runtime_resolver,
            local_store: None,
            node_id: String::new(),
            p2p_transport,
        }
    }

    /// Returns the configured durable repository so sibling resource catalogs
    /// can share the same PosixFS/OSS source of truth.
    pub fn repository(&self) -> Arc<dyn Repository> {
        Arc::clone(&self.repository)
    }

    pub async fn create(
        &self,
        record: SnapshotRecord,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        self.repository.create(record).await
    }

    #[tracing::instrument(skip(self, metadata, manifest, recording), fields(snapshot_id = %metadata.id))]
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
        recording: Option<crate::snapshot::StartupRecording>,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        if metadata.snapshot_type == SnapshotPlacement::Local {
            return Err(RepositoryError::Unsupported {
                feature: "Local publication requires an owned captured snapshot".to_string(),
            });
        }
        self.publish_with_recording(metadata, manifest, recording)
            .await
    }

    /// Publishes into the repository, then starts the startup-manifest
    /// continuation and best-effort P2P publication. A failed publish drops
    /// `recording`, whose recorder keeps its own capture lease.
    async fn publish_with_recording(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
        recording: Option<crate::snapshot::StartupRecording>,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        let publication = self
            .repository
            .publish_with_local_layers(metadata, manifest.clone())
            .await?;
        if let Some(recording) = recording {
            spawn_startup_manifest(
                Arc::clone(&self.repository),
                publication.record.id.clone(),
                recording,
            );
        }
        self.publish_p2p_artifacts(&publication, &manifest).await;
        Ok(publication.record)
    }

    /// Local publication uses a fresh ID; a retried capture allocates a new ID.
    pub(crate) async fn publish_local(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        let Some(local_store) = self.local_store.as_ref() else {
            return Err(RepositoryError::Unsupported {
                feature: "node-local snapshot store is not configured".to_string(),
            });
        };

        metadata
            .validate()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        local_store
            .commit_snapshot(metadata.id.clone(), manifest)
            .await?;
        let committed = CommittedSnapshot {
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            runtime_versions: metadata.runtime_versions.clone(),
            image_configs: metadata.image_configs.clone(),
            virtualization_mode: metadata.virtualization_mode,
            volume_snapshots: metadata.volume_snapshots.clone(),
            memory_startup: None,
            rootfs_layers: Vec::new(),
            attached_drives: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            custom_extension_params: metadata.custom_extension_params.clone(),
        };
        // The configured primary repository is the only public metadata
        // authority, even when the immutable bytes remain node-local.
        let mut record = SnapshotRecord::new_committed(&metadata, committed, now_unix_ms());
        record.owner_node_id = Some(self.node_id.clone());
        match self.repository.commit_record(record).await {
            Ok(record) => Ok(record),
            Err(error) => {
                // A failed response may follow a successful catalog write.
                // Reclaim only when the record is confirmed absent.
                if matches!(
                    self.repository.get(&metadata.id.to_string()).await,
                    Ok(None)
                ) {
                    local_store.delete_snapshot(&metadata.id);
                }
                Err(error)
            }
        }
    }

    fn require_local_owner(
        &self,
        record: &SnapshotRecord,
    ) -> crate::repository::RepositoryResult<()> {
        match record.owner_node_id.as_deref() {
            Some(owner) if owner == self.node_id => Ok(()),
            Some(owner) => Err(RepositoryError::Unavailable {
                reason: format!(
                    "Local snapshot '{}' belongs to node '{owner}', not '{}'",
                    record.id, self.node_id
                ),
            }),
            None => Err(RepositoryError::Unavailable {
                reason: format!("Local snapshot '{}' has no owner metadata", record.id),
            }),
        }
    }

    fn acquire_local_manifest(
        &self,
        canonical: &SnapshotRecord,
    ) -> crate::repository::RepositoryResult<(SandboxSnapshotManifest, Arc<RuntimeArtifactLease>)>
    {
        let snapshot_id = canonical.id.clone();
        self.require_local_owner(canonical)?;
        let local_store =
            self.local_store
                .as_ref()
                .ok_or_else(|| RepositoryError::Unavailable {
                    reason: "node-local snapshot store is not configured".to_string(),
                })?;
        // Canonical metadata supplies identity and owner routing; the local
        // store resolves the manifest and immutable deltas.
        local_store
            .acquire_manifest(&snapshot_id)
            .map_err(|error| RepositoryError::Unavailable {
                reason: format!(
                    "resolve node-local artifacts for snapshot '{snapshot_id}': {error}"
                ),
            })
    }

    /// Best effort attempt to publish snapshot artifacts to P2P.
    #[tracing::instrument(skip(self, publication, manifest), fields(snapshot_id = %publication.record.id))]
    async fn publish_p2p_artifacts(
        &self,
        publication: &SnapshotPublication,
        manifest: &SandboxSnapshotManifest,
    ) {
        let Some(transport) = self.p2p_transport.as_ref() else {
            return;
        };
        let record = &publication.record;
        let snapshot_id = &record.id;
        let Some(committed) = record.committed.as_ref() else {
            return;
        };

        // Prepare the manifest and VM state.
        let manifest_bytes = serde_json::to_vec(manifest).expect("manifest should serialize");
        let mut artifacts = vec![
            SnapshotP2pArtifact::fixed(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
                manifest.vm_state.path.clone(),
            ),
            SnapshotP2pArtifact::bytes(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
                manifest_bytes,
            ),
        ];

        // Uploaded descriptors describe the committed bytes, including compressed
        // and dense-exported layers. Keep their files alive until P2P copies finish.
        artifacts.extend(publication.local_layers.iter().map(|layer| {
            SnapshotP2pArtifact::content_addressed_overlaybd_layer(
                layer.path(),
                layer.digest(),
                layer.size(),
            )
        }));

        // Collect any overlaybd layers referenced by this snapshot's runtime images.
        let rootfs_uuids = managed_layer_uuids(&committed.rootfs_layers);
        let rootfs_digests = committed_layer_digests(&committed.rootfs_layers);
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.rootfs.image_config_path,
            &rootfs_digests,
            &rootfs_uuids,
        ));
        let memory_uuids = managed_layer_uuids_from_managed(&committed.memory_layers);
        let memory_digests = committed_memory_layer_digests(&committed.memory_layers);
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.memory.image_config_path,
            &memory_digests,
            &memory_uuids,
        ));
        for drive in &manifest.attached_drives {
            let (drive_digests, drive_uuids) = committed
                .attached_drives
                .iter()
                .find_map(|committed_drive| match committed_drive {
                    crate::snapshot::CommittedAttachedDrive::Overlaybd {
                        drive_id, layers, ..
                    } if drive_id == &drive.drive_id => {
                        Some((committed_layer_digests(layers), managed_layer_uuids(layers)))
                    }
                    _ => None,
                })
                .unwrap_or_default();
            artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
                &drive.image_config_path,
                &drive_digests,
                &drive_uuids,
            ));
        }

        // An unchanged local layer may appear in both the uploads and image config.
        let mut keys = HashSet::new();
        artifacts.retain(|artifact| keys.insert(artifact.key.clone()));

        // Publish all artifacts concurrently, but don't fail if any individual artifact fails to publish.
        stream::iter(artifacts)
            .for_each_concurrent(SNAPSHOT_P2P_PUBLISH_CONCURRENCY, |artifact| async move {
                if let Err(error) = artifact.publish(transport).await {
                    warn!(
                        key = %artifact.key,
                        source = %artifact.source,
                        error = %error,
                        "failed to publish snapshot artifact to P2P"
                    );
                }
            })
            .await;
    }

    /// Loads a snapshot record by id or alias.
    pub async fn get(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.repository
            .get(id_or_alias.as_ref())
            .await
            .with_context(|| {
                format!(
                    "load committed snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })
    }

    /// Lists snapshot records that match the given filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> anyhow::Result<Vec<SnapshotRecord>> {
        self.repository
            .list(filter)
            .await
            .context("list committed snapshots through repository")
    }

    /// Deletes a snapshot by id or alias.
    ///
    /// Returns `Ok(())` on success. The operation is idempotent:
    /// if the snapshot does not exist, it is still considered success.
    pub async fn delete(&self, id_or_alias: impl AsRef<str>) -> anyhow::Result<()> {
        let lookup = id_or_alias.as_ref();
        let local_id = SnapshotId::parse(lookup).ok().filter(|id| {
            self.local_store
                .as_ref()
                .is_some_and(|store| store.contains_snapshot(id))
        });
        if let Some(id) = local_id {
            let manager = self.clone();
            return tokio::spawn(async move {
                let reference = id.to_string();
                if let Err(error) = manager.repository.delete(&reference).await {
                    if !matches!(manager.repository.get(&reference).await, Ok(None)) {
                        return Err(anyhow::Error::new(error));
                    }
                }
                if let Some(local_store) = &manager.local_store {
                    local_store.delete_snapshot(&id);
                }
                Ok(())
            })
            .await
            .context("finish Local snapshot deletion")?;
        }
        // Also permits removal of an unavailable Local record after its owner exits.
        self.repository
            .delete(lookup)
            .await
            .with_context(|| format!("delete snapshot '{lookup}' through repository"))
    }

    /// Resolves an alias to its committed snapshot id.
    pub async fn resolve_committed_alias(&self, alias: &str) -> anyhow::Result<Option<SnapshotId>> {
        self.repository.resolve_alias(alias).await.with_context(|| {
            format!("resolve committed snapshot alias '{alias}' through repository")
        })
    }

    /// Resolves a committed snapshot into node-local runnable artifact paths.
    pub async fn resolve_runnable(
        &self,
        snapshot: SnapshotRecord,
    ) -> anyhow::Result<RunnableSnapshot> {
        let result = match snapshot.snapshot_type {
            SnapshotPlacement::Distributed => self
                .runtime_resolver
                .resolve(Arc::new(snapshot))
                .await
                .map_err(anyhow::Error::new),
            SnapshotPlacement::Local => self
                .acquire_local_manifest(&snapshot)
                .map(|(manifest, lease)| RunnableSnapshot::new(snapshot, manifest, lease))
                .map_err(anyhow::Error::new),
        };
        result.context("resolve committed snapshot into runnable runtime paths")
    }

    /// Loads a committed snapshot and immediately resolves it into runnable state.
    #[tracing::instrument(
        skip(self, id_or_alias),
        fields(snapshot_ref = %id_or_alias.as_ref())
    )]
    pub async fn load_runnable(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<RunnableSnapshot>> {
        let Some(snapshot) = self.get(id_or_alias.as_ref()).await? else {
            return Ok(None);
        };
        self.resolve_runnable(snapshot).await.map(Some)
    }

    /// Atomically transitions one template build from waiting to building.
    pub async fn try_start_build(
        &self,
        id: &SnapshotId,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        self.repository.try_start_build(id).await
    }

    /// Marks one template build as failed.
    pub async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> crate::repository::RepositoryResult<()> {
        self.repository.mark_build_error(id, reason).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlaybd::layer_key_from_digest;
    use crate::p2p::mock::MockTransport;
    use crate::sandbox::CapturedSandboxSnapshot;
    use crate::snapshot::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::p2p::fixed_artifact_key;
    use crate::snapshot::{
        SnapshotAlias, SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource,
    };
    use std::path::Path;
    use tempfile::TempDir;

    fn test_manager(root: &Path) -> SnapshotManager {
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join("repository"),
            cache_root: Some(root.join("runtime-cache")),
            runtime_cache_root: Some(root.join("runtime-cache").join("runtime")),
        })
        .expect("posix backend");
        let (repository, runtime_resolver) = backend.into_parts();
        SnapshotManager::from_parts(repository, runtime_resolver, None)
    }

    fn local_manager(root: &Path) -> SnapshotManager {
        let mut manager = test_manager(root);
        manager.local_store = Some(LocalSnapshotStore::new(root.join("local")));
        manager.node_id = "node-a".to_string();
        manager
    }

    fn local_capture(root: &Path) -> (SnapshotPublishMetadata, CapturedSandboxSnapshot) {
        let metadata = SnapshotPublishMetadata {
            snapshot_type: SnapshotPlacement::Local,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sandbox-source".to_string(),
                source_sandbox_metadata: None,
            },
            ..SnapshotPublishMetadata::mock()
        };
        let capture_dir = tempfile::Builder::new()
            .prefix(root.file_name().unwrap().to_str().unwrap())
            .rand_bytes(0)
            .tempdir_in(root.parent().unwrap())
            .unwrap();
        let manifest = write_mock_built_artifacts(capture_dir.path()).unwrap().2;
        let capture = CapturedSandboxSnapshot::new(manifest, capture_dir);
        (metadata, capture)
    }

    struct DeferredCommitRepository {
        inner: Arc<dyn Repository>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl Repository for DeferredCommitRepository {
        async fn get_document(
            &self,
            key: crate::repository::DocumentKey,
        ) -> crate::repository::RepositoryResult<Option<Vec<u8>>> {
            self.inner.get_document(key).await
        }
        async fn update_document(
            &self,
            key: crate::repository::DocumentKey,
            update: crate::repository::DocumentUpdate,
        ) -> crate::repository::RepositoryResult<Option<Vec<u8>>> {
            self.entered.notify_one();
            self.release.notified().await;
            self.inner.update_document(key, update).await
        }
    }

    #[tokio::test]
    async fn cancelled_local_publication_keeps_its_capture_until_catalog_commit_finishes(
    ) -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let mut manager = local_manager(root.path());
        let primary = Arc::clone(&manager.repository);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        manager.repository = Arc::new(DeferredCommitRepository {
            inner: Arc::clone(&primary),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let capture_root = root.path().join("capture");
        let (metadata, capture) = local_capture(&capture_root);
        let id = metadata.id.clone();
        let publishing = manager.clone();
        let caller = tokio::spawn(async move {
            crate::sandbox::publish_captured(&publishing, metadata, capture).await
        });
        entered.notified().await;
        assert!(primary.get(&id.to_string()).await.unwrap().is_none());
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(capture_root.exists());

        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if primary.get(&id.to_string()).await.unwrap().is_some() && !capture_root.exists() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let runnable = manager.load_runnable(id.to_string()).await?.unwrap();
        assert!(runnable.manifest().vm_state.path.is_file());
        Ok(())
    }

    async fn seed_built_snapshot(manager: &SnapshotManager, snapshot_id: SnapshotId, alias: &str) {
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let metadata = SnapshotPublishMetadata {
            snapshot_type: crate::snapshot::SnapshotPlacement::Distributed,
            id: snapshot_id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        };
        manager
            .publish(metadata, manifest, None)
            .await
            .expect("seed publish should work");
    }

    struct LeaseProbe(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for LeaseProbe {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// A recording whose recorder holds a capture lease until `release` fires.
    fn pending_recording() -> (
        crate::snapshot::StartupRecording,
        tokio::sync::oneshot::Sender<()>,
        Arc<std::sync::atomic::AtomicBool>,
    ) {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let recording = crate::snapshot::StartupRecording::spawn_with_capture_lease(
            async move {
                let _ = wait.await;
                None
            },
            Arc::new(LeaseProbe(Arc::clone(&dropped))),
        );
        (recording, release, dropped)
    }

    async fn wait_for_lease_release(dropped: &std::sync::atomic::AtomicBool) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !dropped.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recorder release");
    }

    #[tokio::test]
    async fn failed_publish_keeps_capture_lease_until_pending_recorder_finishes() {
        let tempdir = TempDir::new().unwrap();
        let manager = test_manager(tempdir.path());
        let (_, _, mut manifest) = write_mock_built_artifacts(tempdir.path()).unwrap();
        manifest.vm_state.path = tempdir.path().join("missing-vm-state");
        let (recording, release, dropped) = pending_recording();
        assert!(manager
            .publish(SnapshotPublishMetadata::mock(), manifest, Some(recording))
            .await
            .is_err());
        assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
        release.send(()).unwrap();
        wait_for_lease_release(&dropped).await;
    }

    #[tokio::test]
    async fn cancelled_publish_keeps_capture_lease_until_pending_recorder_finishes() {
        let tempdir = TempDir::new().unwrap();
        let manager = test_manager(tempdir.path());
        let (_, _, manifest) = write_mock_built_artifacts(tempdir.path()).unwrap();
        let (recording, release, dropped) = pending_recording();
        let publish = manager.publish(SnapshotPublishMetadata::mock(), manifest, Some(recording));
        // Cancel before the first poll; the recorder is already detached.
        drop(publish);
        assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
        release.send(()).unwrap();
        wait_for_lease_release(&dropped).await;
    }

    #[tokio::test]
    async fn uncertain_catalog_commit_keeps_local_files() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        std::fs::write(root.path().join("repository"), b"not a directory")?;
        let manager = local_manager(root.path());
        let (metadata, capture) = local_capture(&root.path().join("capture"));
        let id = metadata.id.clone();

        crate::sandbox::publish_captured(&manager, metadata, capture)
            .await
            .unwrap_err();
        assert!(manager.get(id.to_string()).await.is_err());
        let (manifest, _lease) = manager
            .local_store
            .as_ref()
            .unwrap()
            .acquire_manifest(&id)?;
        assert!(manifest.vm_state.path.is_file());
        Ok(())
    }

    #[tokio::test]
    async fn local_snapshot_requires_its_publishing_process() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let manager = local_manager(root.path());
        let (metadata, capture) = local_capture(&root.path().join("capture"));
        let record = crate::sandbox::publish_captured(&manager, metadata, capture).await?;
        let runnable = manager.load_runnable(record.id.to_string()).await?.unwrap();
        let foreign = SnapshotManager {
            node_id: "node-b".to_string(),
            ..manager.clone()
        };
        assert!(foreign.resolve_runnable(record.clone()).await.is_err());
        drop(foreign);
        drop(manager);

        let restarted = local_manager(root.path());
        let error = restarted
            .resolve_runnable(record.clone())
            .await
            .unwrap_err();
        assert!(error.chain().any(|cause| matches!(
            cause.downcast_ref::<RepositoryError>(),
            Some(RepositoryError::Unavailable { .. })
        )));
        restarted.delete(record.id.to_string()).await?;
        assert!(restarted.get(record.id.to_string()).await?.is_none());
        assert!(runnable.manifest().vm_state.path.is_file());
        Ok(())
    }

    #[tokio::test]
    async fn repository_management_methods_delegate_to_committed_store() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, snapshot_id.clone(), "managed").await;

        let resolved = manager
            .resolve_committed_alias("managed")
            .await
            .expect("resolve alias should work");
        assert_eq!(resolved, Some(snapshot_id.clone()));

        let loaded = manager
            .get("managed")
            .await
            .expect("load should work")
            .expect("snapshot should exist");
        assert_eq!(loaded.id, snapshot_id);

        let listed = manager
            .list(SnapshotListFilter::matches_all())
            .await
            .expect("list should work");
        assert_eq!(listed.len(), 1);

        manager.delete("managed").await.expect("delete should work");
        assert!(manager
            .get("managed")
            .await
            .expect("load after delete should work")
            .is_none());
    }

    #[tokio::test]
    async fn load_runnable_uses_committed_snapshot_and_runtime_resolution() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, snapshot_id.clone(), "runnable").await;

        let runnable = manager
            .load_runnable("runnable")
            .await
            .expect("load runnable should work")
            .expect("runnable snapshot should exist");

        assert_eq!(runnable.record().id, snapshot_id);
        assert!(runnable.manifest().rootfs.image_config_path.exists());
        assert!(runnable.manifest().vm_state.path.exists());
    }

    #[tokio::test]
    async fn publish_advertises_snapshot_artifacts_to_p2p_after_commit() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: tempdir.path().join("repository"),
            cache_root: Some(tempdir.path().join("runtime-cache")),
            runtime_cache_root: Some(tempdir.path().join("runtime-cache").join("runtime")),
        })
        .expect("posix backend");
        let (repository, runtime_resolver) = backend.into_parts();
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(repository, runtime_resolver, Some(p2p.clone()));

        let workspace = TempDir::new().expect("tempdir should exist");
        let (rootfs_lower, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let snapshot_id = SnapshotId::generate();
        let metadata = SnapshotPublishMetadata {
            snapshot_type: crate::snapshot::SnapshotPlacement::Distributed,
            id: snapshot_id.clone(),
            ..SnapshotPublishMetadata::mock()
        };

        manager
            .publish(metadata, manifest, None)
            .await
            .expect("publish should commit");

        let vm_state_key = fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        let manifest_key =
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        let rootfs_layer_digest = crate::digest::FileDigest::describe(&rootfs_lower)
            .await
            .expect("describe rootfs lower");
        let rootfs_layer_key = layer_key_from_digest(&rootfs_layer_digest.sha256);

        assert!(p2p
            .lookup(&vm_state_key)
            .await
            .expect("lookup vm state")
            .is_some());
        assert!(p2p
            .lookup(&manifest_key)
            .await
            .expect("lookup manifest")
            .is_some());
        assert!(p2p
            .lookup(&rootfs_layer_key)
            .await
            .expect("lookup rootfs layer")
            .is_some());
    }

    #[tokio::test]
    async fn posix_resolve_never_repairs_missing_files_from_peers() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: tempdir.path().join("repository"),
            cache_root: Some(tempdir.path().join("runtime-cache")),
            runtime_cache_root: Some(tempdir.path().join("runtime-cache").join("runtime")),
        })
        .expect("posix backend");
        let repository = backend.repository();
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(
            Arc::clone(&repository),
            backend.runtime_resolver(),
            Some(p2p.clone()),
        );
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let snapshot_id = SnapshotId::generate();
        let metadata = SnapshotPublishMetadata {
            snapshot_type: crate::snapshot::SnapshotPlacement::Distributed,
            id: snapshot_id.clone(),
            ..SnapshotPublishMetadata::mock()
        };
        let record = manager
            .publish(metadata, manifest, None)
            .await
            .expect("publish should commit");
        let crate::repository::BlobLocation::Local(vm_state) = repository
            .locate_blob(&crate::repository::BlobKey::Snapshot(
                snapshot_id.clone(),
                crate::repository::SnapshotFile::VmState,
            ))
            .expect("posix locates snapshot files")
        else {
            panic!("posix reads snapshot files in place");
        };
        std::fs::remove_file(&vm_state).expect("remove vm state");

        // Peers still have the file, even given to the resolver directly.
        let resolver = crate::snapshot::resolver::RepositoryRuntimeResolver::new(
            repository,
            crate::snapshot::artifact_cache::LocalArtifactCache::new(
                tempdir.path().join("other-cache"),
                None,
            )
            .expect("local cache"),
            tempdir.path().join("other-runtime"),
            crate::image::cache::local_image_services_from_global_config().overlaybd_layers,
            Some(p2p),
        );
        let error = resolver
            .resolve(Arc::new(record))
            .await
            .expect_err("missing vm state must not be fetched from peers");
        assert!(matches!(
            error,
            crate::repository::RepositoryError::ArtifactNotFound { .. }
        ));
    }

    #[tokio::test]
    async fn failed_repository_commit_does_not_publish_to_p2p() {
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(
            Arc::new(crate::snapshot::mock::MockRepository),
            Arc::new(crate::snapshot::mock::MockSnapshotRuntimeResolver),
            Some(p2p.clone()),
        );
        let workspace = TempDir::new().expect("tempdir");
        let (_, _, manifest) = write_mock_built_artifacts(workspace.path()).expect("artifacts");
        manager
            .publish(SnapshotPublishMetadata::mock(), manifest, None)
            .await
            .expect_err("repository rejects publication");
        assert_eq!(
            p2p.publish_count.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn failed_p2p_publication_does_not_undo_repository_commit() {
        let workspace = TempDir::new().expect("tempdir");
        let mut manager = test_manager(workspace.path());
        let p2p = Arc::new(MockTransport::default());
        p2p.fail_publish
            .store(true, std::sync::atomic::Ordering::Relaxed);
        manager.p2p_transport = Some(p2p.clone());
        let (_, _, manifest) =
            write_mock_built_artifacts(&workspace.path().join("artifacts")).expect("artifacts");
        let stored = manager
            .publish(SnapshotPublishMetadata::mock(), manifest, None)
            .await
            .expect("P2P failure must not fail the commit");
        assert!(p2p.publish_count.load(std::sync::atomic::Ordering::Relaxed) > 0);
        let loaded = manager.get(stored.id.to_string()).await.unwrap().unwrap();
        assert_eq!(loaded.id, stored.id);
        assert!(loaded.committed.is_some());
    }
}
