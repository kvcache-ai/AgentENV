use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use futures::{stream, StreamExt};
use tracing::warn;

use super::p2p::SnapshotP2pArtifact;
use super::types::SNAPSHOT_ARTIFACT_LAYOUT;
use crate::p2p::P2pTransport;
use crate::sandbox::{
    CapturedSandboxSnapshot, FirecrackerCapturedSnapshot, FirecrackerSnapshotManifest,
};
use crate::snapshot::repository::backends::build_snapshot_backend;
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
use crate::snapshot::repository::{RepositoryError, SnapshotListFilter};
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

    pub async fn create(
        &self,
        record: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.create(record).await
    }

    /// Returns the shared build-context archive store, when the configured
    /// repository backend provides one.
    pub fn template_build_files(
        &self,
    ) -> Option<Arc<dyn crate::snapshot::repository::TemplateBuildFileStore>> {
        self.repository.template_build_files()
    }

    #[tracing::instrument(skip(self, metadata, manifest), fields(snapshot_id = %metadata.id))]
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let record = self
            .repository
            .publish(metadata.clone(), manifest.clone())
            .await?;
        self.publish_p2p_artifacts(&record, &manifest).await;
        Ok(record)
    }

    #[tracing::instrument(skip(self, metadata), fields(snapshot_id = %metadata.id))]
    pub async fn publish_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let manifest = captured_snapshot
            .downcast_ref::<FirecrackerCapturedSnapshot>()
            .map(|snapshot| snapshot.manifest().clone())
            .ok_or_else(|| RepositoryError::Unsupported {
                feature: "publishing captured snapshots for this sandbox backend".to_string(),
            })?;

        let record = self
            .repository
            .publish(metadata.clone(), manifest.clone())
            .await?;
        self.publish_p2p_artifacts(&record, &manifest).await;
        Ok(record)
    }

    /// Best effort attempt to publish snapshot artifacts to P2P.
    #[tracing::instrument(skip(self, record, manifest), fields(snapshot_id = %record.id))]
    async fn publish_p2p_artifacts(
        &self,
        record: &SnapshotRecord,
        manifest: &FirecrackerSnapshotManifest,
    ) {
        let Some(transport) = self.p2p_transport.as_ref() else {
            return;
        };
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

        // Collect any overlaybd layers referenced by this snapshot's runtime images.
        let rootfs_uuids = managed_layer_uuids(&committed.rootfs_layers);
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.rootfs.image_config_path,
            &rootfs_uuids,
        ));
        let memory_uuids = managed_layer_uuids_from_managed(&committed.memory_layers);
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.memory.image_config_path,
            &memory_uuids,
        ));
        for drive in &manifest.attached_drives {
            let drive_uuids = committed
                .attached_drives
                .iter()
                .find_map(|committed_drive| match committed_drive {
                    crate::snapshot::CommittedAttachedDrive::Overlaybd {
                        drive_id, layers, ..
                    } if drive_id == &drive.drive_id => Some(managed_layer_uuids(layers)),
                    _ => None,
                })
                .unwrap_or_default();
            artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
                &drive.image_config_path,
                &drive_uuids,
            ));
        }

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
            .publish(metadata, manifest)
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
            .publish(metadata, manifest)
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
}
