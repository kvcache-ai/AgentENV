use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use dashmap::DashMap;
use futures::{stream, StreamExt};
use tracing::warn;

use super::backends::build_snapshot_backend;
use super::catalog::SnapshotCatalog;
use super::p2p::SnapshotP2pArtifact;
use super::publish::spawn_startup_manifest;
use super::types::{now_unix_ms, SNAPSHOT_ARTIFACT_LAYOUT};
use super::{SnapshotListFilter, SnapshotPublication, SnapshotRuntimeResolver};
use crate::p2p::P2pTransport;
use crate::repository::{Repository, RepositoryError};
use crate::snapshot::{
    CommittedSnapshot, ManagedLayer, OverlaybdLayerRef, RunnableSnapshot, SandboxSnapshotManifest,
    Snapshot, SnapshotId, SnapshotPlacement, SnapshotPublishMetadata, SnapshotPublishSource,
    SnapshotRecord, SnapshotSource,
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
    local_snapshots: Arc<DashMap<SnapshotId, Arc<Snapshot>>>,
    node_id: String,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
    mutation_locks: Arc<DashMap<SnapshotId, Arc<tokio::sync::Mutex<()>>>>,
}

struct SnapshotMutationGuard<'a> {
    locks: &'a DashMap<SnapshotId, Arc<tokio::sync::Mutex<()>>>,
    id: SnapshotId,
    lock: Option<Arc<tokio::sync::Mutex<()>>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for SnapshotMutationGuard<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        drop(self.lock.take());
        self.locks
            .remove_if(&self.id, |_, lock| Arc::strong_count(lock) == 1);
    }
}

impl SnapshotManager {
    /// Builds a manager using the configured repository backend.
    pub fn new(
        node_id: String,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> anyhow::Result<Self> {
        let (repository, runtime_resolver) = build_snapshot_backend(p2p_transport.clone())?;
        Ok(Self {
            repository,
            runtime_resolver,
            local_snapshots: Arc::new(DashMap::new()),
            node_id,
            p2p_transport,
            mutation_locks: Arc::new(DashMap::new()),
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
            local_snapshots: Arc::new(DashMap::new()),
            node_id: String::new(),
            p2p_transport,
            mutation_locks: Arc::new(DashMap::new()),
        }
    }

    /// Returns the configured durable repository so sibling resource catalogs
    /// can share the same PosixFS/OSS source of truth.
    pub fn repository(&self) -> Arc<dyn Repository> {
        Arc::clone(&self.repository)
    }

    async fn lock_snapshot_mutation(&self, id: SnapshotId) -> SnapshotMutationGuard<'_> {
        let lock = Arc::clone(
            self.mutation_locks
                .entry(id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .value(),
        );
        let mut lease = SnapshotMutationGuard {
            locks: &self.mutation_locks,
            id,
            lock: Some(Arc::clone(&lock)),
            guard: None,
        };
        // Construct the lease before waiting so cancellation also releases its key.
        lease.guard = Some(lock.lock_owned().await);
        lease
    }

    async fn resolve_snapshot_id(
        &self,
        reference: &str,
    ) -> crate::repository::RepositoryResult<Option<SnapshotId>> {
        if let Ok(id) = SnapshotId::parse(reference) {
            return Ok(Some(id));
        }
        Ok(self
            .repository
            .get(reference)
            .await?
            .map(|record| record.id))
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

    /// Publication outlives caller cancellation so all readers retain their files.
    /// Local publication expects a fresh ID; API retries capture a new snapshot.
    #[tracing::instrument(skip(self, snapshot, metadata), fields(snapshot_id = %snapshot.id()))]
    pub async fn publish_snapshot(
        &self,
        snapshot: Arc<Snapshot>,
        metadata: SnapshotPublishMetadata,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        if snapshot.id() != &metadata.id {
            return Err(RepositoryError::InvalidRequest {
                reason: "snapshot ID differs from publication metadata".to_string(),
            });
        }
        let manager = self.clone();
        tokio::spawn(async move { manager.publish_inner(snapshot, metadata).await })
            .await
            .map_err(|error| RepositoryError::backend("finish snapshot publication", error))?
    }

    async fn publish_inner(
        &self,
        snapshot: Arc<Snapshot>,
        metadata: SnapshotPublishMetadata,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        if metadata.snapshot_type == SnapshotPlacement::Local {
            return self.publish_local(snapshot, metadata).await;
        }
        let recording = snapshot.take_startup_recording();
        self.publish_with_recording(metadata, snapshot.manifest().clone(), recording)
            .await
    }

    async fn publish_local(
        &self,
        snapshot: Arc<Snapshot>,
        metadata: SnapshotPublishMetadata,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        metadata
            .validate()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        if self.node_id.is_empty() {
            return Err(RepositoryError::Unsupported {
                feature: "Local publication without a node identity".to_string(),
            });
        }
        let id = snapshot.id().clone();
        let _mutation_guard = self.lock_snapshot_mutation(id.clone()).await;
        match self.local_snapshots.entry(id.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&snapshot));
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!("snapshot {id} is already registered"),
                })
            }
        }
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
        let mut record =
            SnapshotRecord::new_committed(id.clone(), &metadata, committed, now_unix_ms());
        record.owner_node_id = Some(self.node_id.clone());
        match self.repository.commit_record(record).await {
            Ok(record) => Ok(record),
            Err(error) => {
                // A failed response may follow a successful catalog write.
                if matches!(self.repository.get(&id.to_string()).await, Ok(None)) {
                    self.local_snapshots.remove(&id);
                }
                Err(error)
            }
        }
    }

    /// Promotes one reusable Local snapshot to Distributed without recapture.
    ///
    /// The lookup is always resolved by the canonical repository. A missing or
    /// foreign-owner Local snapshot is never substituted from node-local state.
    pub async fn promote(
        &self,
        id_or_alias: &str,
    ) -> crate::repository::RepositoryResult<Option<SnapshotRecord>> {
        let Some(snapshot_id) = self.resolve_snapshot_id(id_or_alias).await? else {
            return Ok(None);
        };
        let manager = self.clone();
        tokio::spawn(async move { manager.promote_inner(snapshot_id).await })
            .await
            .map_err(|error| RepositoryError::backend("finish snapshot promotion", error))?
    }

    async fn promote_inner(
        &self,
        snapshot_id: SnapshotId,
    ) -> crate::repository::RepositoryResult<Option<SnapshotRecord>> {
        let canonical_ref = snapshot_id.to_string();
        let _mutation_guard = self.lock_snapshot_mutation(snapshot_id).await;
        let Some(record) = self.repository.get(&canonical_ref).await? else {
            return Ok(None);
        };
        let SnapshotSource::Sandbox {
            source_sandbox_id,
            source_sandbox_metadata,
        } = &record.source
        else {
            return Ok(None);
        };
        if record.snapshot_type == SnapshotPlacement::Distributed {
            self.local_snapshots.remove(&record.id);
            return Ok(Some(record));
        }
        let snapshot = self.acquire_local_snapshot(&record)?;
        let committed =
            record
                .committed
                .as_ref()
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{}' has no committed artifacts", record.id),
                })?;
        // Publish from the leased immutable manifest. The repository prepares
        // dense output without rewriting files used by existing Local readers.
        let metadata = SnapshotPublishMetadata {
            id: snapshot.id().clone(),
            snapshot_type: SnapshotPlacement::Distributed,
            alias: record.alias.clone(),
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: source_sandbox_id.clone(),
                source_sandbox_metadata: source_sandbox_metadata.clone(),
            },
            context: committed.context.clone(),
            startup: committed.startup.clone(),
            resources: record.resources,
            runtime_versions: committed.runtime_versions.clone(),
            virtualization_mode: committed.virtualization_mode,
            volume_snapshots: committed.volume_snapshots.clone(),
            image_configs: committed.image_configs.clone(),
            custom_extension_params: committed.custom_extension_params.clone(),
        };
        let promoted =
            self.publish_inner(snapshot, metadata)
                .await
                .map_err(|error| match error {
                    error @ (RepositoryError::ArtifactNotFound { .. }
                    | RepositoryError::ManagedLayerNotFound { .. }) => {
                        RepositoryError::Unavailable {
                            reason: format!(
                                "publish Distributed closure for snapshot '{}': {error}",
                                record.id
                            ),
                        }
                    }
                    error => error,
                })?;

        self.local_snapshots.remove(&record.id);
        Ok(Some(promoted))
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

    fn acquire_local_snapshot(
        &self,
        canonical: &SnapshotRecord,
    ) -> crate::repository::RepositoryResult<Arc<Snapshot>> {
        self.require_local_owner(canonical)?;
        self.local_snapshots
            .get(&canonical.id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| RepositoryError::Unavailable {
                reason: format!(
                    "Local snapshot '{}' is unavailable on this owner",
                    canonical.id
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
        let reference = id_or_alias.as_ref().to_owned();
        let id = self.resolve_snapshot_id(&reference).await?;
        let manager = self.clone();
        tokio::spawn(async move {
            let _mutation_guard = match id.as_ref() {
                Some(id) => Some(manager.lock_snapshot_mutation(id.clone()).await),
                None => None,
            };
            let reference = id.as_ref().map(ToString::to_string).unwrap_or(reference);
            manager.repository.delete(&reference).await?;
            if let Some(id) = id {
                manager.local_snapshots.remove(&id);
            }
            Ok(())
        })
        .await
        .context("finish snapshot deletion")?
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
                .acquire_local_snapshot(&snapshot)
                .map(|files| RunnableSnapshot::new(snapshot, files))
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
        manager.node_id = "node-a".to_string();
        manager
    }

    fn local_capture(root: &Path) -> (SnapshotPublishMetadata, Arc<Snapshot>) {
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
        crate::snapshot::local::finish_snapshot(&manifest, capture_dir.path()).unwrap();
        let capture = Arc::new(Snapshot::new(metadata.id.clone(), manifest, capture_dir));
        (metadata, capture)
    }

    struct DeferredPublicationRepository {
        inner: Arc<dyn Repository>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl Repository for DeferredPublicationRepository {
        fn publish_policy(&self) -> crate::repository::PublishPolicy {
            self.inner.publish_policy()
        }
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
            let current = self.inner.get_document(key.clone()).await?;
            if let crate::repository::DocumentChange::Put(bytes) = update(current.as_deref())? {
                let record: SnapshotRecord = serde_json::from_slice(&bytes).unwrap();
                if !record.deleting {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
            }
            self.inner.update_document(key, update).await
        }
        async fn delete_document(
            &self,
            key: crate::repository::DocumentKey,
        ) -> crate::repository::RepositoryResult<()> {
            self.inner.delete_document(key).await
        }
        async fn put_blob(
            &self,
            key: &crate::repository::BlobKey,
            body: crate::repository::BlobBody<'_>,
        ) -> crate::repository::RepositoryResult<()> {
            self.inner.put_blob(key, body).await
        }
        fn locate_blob(
            &self,
            key: &crate::repository::BlobKey,
        ) -> crate::repository::RepositoryResult<crate::repository::BlobLocation> {
            self.inner.locate_blob(key)
        }
        async fn has_blob(
            &self,
            key: &crate::repository::BlobKey,
        ) -> crate::repository::RepositoryResult<bool> {
            self.inner.has_blob(key).await
        }
        async fn fetch_blob(
            &self,
            key: &crate::repository::BlobKey,
            to: &Path,
        ) -> crate::repository::RepositoryResult<u64> {
            self.inner.fetch_blob(key, to).await
        }
        async fn delete_snapshot_blobs(
            &self,
            id: &SnapshotId,
        ) -> crate::repository::RepositoryResult<()> {
            self.inner.delete_snapshot_blobs(id).await
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
        manager.repository = Arc::new(DeferredPublicationRepository {
            inner: Arc::clone(&primary),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let capture_root = root.path().join("capture");
        let (metadata, capture) = local_capture(&capture_root);
        let id = metadata.id.clone();
        let publishing = manager.clone();
        let caller =
            tokio::spawn(async move { publishing.publish_snapshot(capture, metadata).await });
        entered.notified().await;
        assert!(primary.get(&id.to_string()).await.unwrap().is_none());
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(capture_root.exists());

        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if primary.get(&id.to_string()).await.unwrap().is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let runnable = manager.load_runnable(id.to_string()).await?.unwrap();
        assert!(runnable.manifest().vm_state.path.starts_with(&capture_root));
        manager.delete(id.to_string()).await?;
        assert!(capture_root.exists());
        drop(runnable);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while capture_root.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
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
    async fn resolved_posix_snapshot_keeps_vm_state_after_repository_deletion() -> anyhow::Result<()>
    {
        let root = TempDir::new()?;
        let manager = test_manager(root.path());
        let id = SnapshotId::generate();
        seed_built_snapshot(&manager, id.clone(), "retained").await;
        let reader = manager.load_runnable("retained").await?.unwrap();
        let vm_state = reader.manifest().vm_state.path.clone();
        let expected = std::fs::read(&vm_state)?;

        manager.delete("retained").await?;

        assert!(!root
            .path()
            .join("repository/snapshots")
            .join(id.to_string())
            .exists());
        assert_eq!(std::fs::read(&vm_state)?, expected);
        assert!(reader.manifest().memory.image_config_path.is_file());
        assert!(reader.manifest().rootfs.image_config_path.is_file());
        Ok(())
    }

    #[tokio::test]
    async fn promotion_retries_failed_publish_and_keeps_existing_readers() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let manager = local_manager(root.path());
        let (mut metadata, capture) = local_capture(&root.path().join("capture"));
        let source_metadata =
            std::collections::HashMap::from([("team".to_string(), "infra".to_string())]);
        metadata.source = SnapshotPublishSource::Sandbox {
            source_sandbox_id: "sandbox-source".to_string(),
            source_sandbox_metadata: Some(source_metadata.clone()),
        };
        let local = manager.publish_snapshot(capture, metadata).await?;
        let reader = manager.resolve_runnable(local.clone()).await?;
        let config = &reader.manifest().memory.image_config_path;
        let original = std::fs::read(config)?;
        let reference = local.id.to_string();
        let blocked = root.path().join("repository/managed-layers");
        std::fs::write(&blocked, b"blocked")?;
        assert!(manager.promote(&reference).await.is_err());
        assert_eq!(
            manager.get(&reference).await?.unwrap().snapshot_type,
            SnapshotPlacement::Local
        );
        std::fs::remove_file(blocked)?;

        let (first, repeated) =
            tokio::join!(manager.promote(&reference), manager.promote(&reference));
        let promoted = first?.unwrap();
        let repeated = repeated?.unwrap();
        let stored = manager.repository.get(&reference).await?.unwrap();
        for record in [&promoted, &repeated, &stored] {
            let SnapshotSource::Sandbox {
                source_sandbox_id,
                source_sandbox_metadata,
            } = &record.source
            else {
                panic!("promotion must preserve the sandbox source");
            };
            assert_eq!(source_sandbox_id, "sandbox-source");
            assert_eq!(source_sandbox_metadata.as_ref(), Some(&source_metadata));
        }
        assert_eq!(promoted.id, local.id);
        assert_eq!(promoted.snapshot_type, SnapshotPlacement::Distributed);
        assert!(promoted.owner_node_id.is_none());
        assert_eq!(repeated.updated_at_unix_ms, promoted.updated_at_unix_ms);
        assert_eq!(std::fs::read(config)?, original);
        assert!(!manager.local_snapshots.contains_key(&local.id));
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_promotion_keeps_delete_waiting_for_publication() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let mut manager = local_manager(root.path());
        let (metadata, capture) = local_capture(&root.path().join("capture"));
        let record = manager.publish_snapshot(capture, metadata).await?;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        manager.repository = Arc::new(DeferredPublicationRepository {
            inner: Arc::clone(&manager.repository),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let publishing = manager.clone();
        let reference = record.id.to_string();
        let caller = tokio::spawn(async move { publishing.promote(&reference).await });
        entered.notified().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let mut waiting = Box::pin(manager.lock_snapshot_mutation(record.id.clone()));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(waiting);
        release.notify_one();
        manager.delete(record.id.to_string()).await?;
        assert!(manager.get(record.id.to_string()).await?.is_none());
        assert!(!manager.local_snapshots.contains_key(&record.id));
        Ok(())
    }

    #[tokio::test]
    async fn uncertain_catalog_commit_keeps_local_files() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        std::fs::write(root.path().join("repository"), b"not a directory")?;
        let manager = local_manager(root.path());
        let (metadata, capture) = local_capture(&root.path().join("capture"));
        let id = metadata.id.clone();

        manager
            .publish_snapshot(capture, metadata)
            .await
            .unwrap_err();
        assert!(manager.get(id.to_string()).await.is_err());
        let snapshot = manager.local_snapshots.get(&id).unwrap();
        assert!(snapshot.manifest().vm_state.path.is_file());
        Ok(())
    }

    #[tokio::test]
    async fn local_snapshot_requires_its_publishing_process() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let manager = local_manager(root.path());
        let (metadata, capture) = local_capture(&root.path().join("capture"));
        let record = manager.publish_snapshot(capture, metadata).await?;
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
