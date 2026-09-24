use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use futures::{stream, StreamExt};
use tracing::warn;

use super::p2p::SnapshotP2pArtifact;
use super::types::SNAPSHOT_ARTIFACT_LAYOUT;
use crate::p2p::P2pTransport;
use crate::sandbox::{
    CapturedSandboxSnapshot, FirecrackerCaptureArtifacts, SandboxSnapshotManifest,
};
use crate::snapshot::repository::backends::build_snapshot_backend;
use crate::snapshot::repository::interfaces::{
    SnapshotPublication, SnapshotRepository, SnapshotRuntimeResolver,
};
use crate::snapshot::repository::SnapshotListFilter;
use crate::snapshot::{
    ManagedLayer, OverlaybdLayerRef, RunnableSnapshot, SnapshotId, SnapshotPublishMetadata,
    SnapshotRecord,
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
/// [`SnapshotRepository`] (PosixFS `managed-layers/`, OSS object storage, or the
/// source registry). The node-local overlaybd layer cache (`image-cache/commits/`)
/// is reclaimable - committed snapshots never pin it - so this manager records no
/// local image ref pins.
pub struct SnapshotManager {
    repository: Arc<dyn SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
}

impl SnapshotManager {
    /// Builds a manager using the configured repository backend.
    pub fn new(p2p_transport: Option<Arc<dyn P2pTransport>>) -> anyhow::Result<Self> {
        let (repository, runtime_resolver) = build_snapshot_backend(p2p_transport.clone())?;
        Ok(Self::from_parts(
            repository,
            runtime_resolver,
            p2p_transport,
        ))
    }

    /// Builds a manager from the given components.
    pub fn from_parts(
        repository: Arc<dyn SnapshotRepository>,
        runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Self {
        Self {
            repository,
            runtime_resolver,
            p2p_transport,
        }
    }

    /// Returns the configured durable repository so sibling resource catalogs
    /// can share the same PosixFS/OSS source of truth.
    pub fn repository(&self) -> Arc<dyn SnapshotRepository> {
        Arc::clone(&self.repository)
    }

    pub async fn create(
        &self,
        record: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.create(record).await
    }

    #[tracing::instrument(skip(self, metadata, manifest, recording), fields(snapshot_id = %metadata.id))]
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
        recording: Option<crate::snapshot::StartupRecording>,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let publication = self
            .repository
            .publish_with_local_layers(metadata, manifest.clone(), recording)
            .await?;
        self.publish_p2p_artifacts(&publication, &manifest).await;
        Ok(publication.record)
    }

    #[tracing::instrument(skip(self, metadata), fields(snapshot_id = %metadata.id))]
    pub async fn publish_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let manifest = captured_snapshot.manifest().clone();

        // Spawn the startup-manifest recording up front (a no-op task when
        // the feature is disabled): the throwaway recording VM overlaps the
        // backend's layer uploads, and a detached continuation uploads the
        // manifest once the trace lands — publish never waits on it. The
        // capture root guard travels with the task so the artifacts outlive
        // the whole continuation.
        let recording = captured_snapshot
            .downcast_artifacts_ref::<FirecrackerCaptureArtifacts>()
            .map(|artifacts| crate::snapshot::StartupRecording {
                trace: tokio::spawn(crate::sandbox::record_startup_pack(
                    artifacts.snapshot_config().clone(),
                    artifacts.snapshot_dir().to_path_buf(),
                )),
                keep_alive: Box::new(artifacts.snapshot_root_guard()),
            });

        let publication = self
            .repository
            .publish_with_local_layers(metadata, manifest.clone(), recording)
            .await?;
        self.publish_p2p_artifacts(&publication, &manifest).await;
        Ok(publication.record)
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
        self.repository
            .delete(id_or_alias.as_ref())
            .await
            .with_context(|| {
                format!(
                    "delete snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })
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
        self.runtime_resolver
            .resolve(Arc::new(snapshot))
            .await
            .context("resolve committed snapshot into runnable runtime paths")
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
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.try_start_build(id).await
    }

    /// Marks one template build as failed.
    pub async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> crate::snapshot::RepositoryResult<()> {
        self.repository.mark_build_error(id, reason).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlaybd::layer_key_from_digest;
    use crate::p2p::mock::MockTransport;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::p2p::fixed_artifact_key;
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::{SnapshotAlias, SnapshotId, SnapshotPublishMetadata};
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

    async fn seed_built_snapshot(manager: &SnapshotManager, snapshot_id: SnapshotId, alias: &str) {
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        };
        manager
            .publish(metadata, manifest, None)
            .await
            .expect("seed publish should work");
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
            .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
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
    async fn failed_repository_commit_does_not_publish_to_p2p() {
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(
            Arc::new(crate::snapshot::mock::MockSnapshotRepository),
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
