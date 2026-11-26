use std::collections::HashSet;
use std::sync::Arc;

use agentenv_metadata::SnapshotEntry;
use anyhow::Context;
use dashmap::DashMap;
use futures::{stream, StreamExt};
use tracing::warn;

use super::backends::build_snapshot_storage;
use super::catalog::MetadataCatalog;
use super::p2p::SnapshotP2pArtifact;
use super::{
    now_unix_ms, SnapshotListFilter, SnapshotPublication, SnapshotRuntimeResolver,
    SNAPSHOT_ARTIFACT_LAYOUT,
};
use crate::metadata::SnapshotPinner;
use crate::p2p::P2pTransport;
use crate::repository::{Repository, RepositoryError};
use crate::snapshot::{
    LayerRef, SnapshotAlias, SnapshotConfig, SnapshotHandle, SnapshotId, SnapshotPlacement,
    SnapshotRecord, SnapshotRuntimeManifest, SnapshotSource,
};

/// Concurrency limit for publishing snapshot artifacts to P2P after commit.
const SNAPSHOT_P2P_PUBLISH_CONCURRENCY: usize = 8;

fn managed_layer_uuids(layers: &[LayerRef]) -> HashSet<String> {
    layers
        .iter()
        .filter_map(|layer| match layer {
            LayerRef::Managed(managed) => managed.uuid.clone(),
            LayerRef::External(_) => None,
        })
        .collect()
}

/// Every layer digest the committed record references for one snapshot
/// subject (rootfs or one attached drive), managed and external alike.
fn committed_layer_digests(layers: &[LayerRef]) -> HashSet<String> {
    layers
        .iter()
        .map(|layer| match layer {
            LayerRef::Managed(managed) => managed.digest.clone(),
            LayerRef::External(external) => external.digest.clone(),
        })
        .collect()
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
    catalog: Arc<MetadataCatalog>,
    repository: Arc<dyn Repository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
    local_snapshots: Arc<DashMap<SnapshotId, Arc<SnapshotHandle>>>,
    pub(super) node_id: String,
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
    /// Builds a Node manager with the configured artifact backend, SQL directory,
    /// and local `pins.db` ledger under `snapshot.local_root`.
    pub async fn with_metadata(
        node_id: String,
        meta: agentenv_metadata::MetaClient,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> anyhow::Result<Self> {
        let (repository, runtime_resolver) = build_snapshot_storage(p2p_transport.clone())?;
        let config = crate::cfg::ConfigManager::global_config();
        let pins = Arc::new(
            crate::metadata::NodePins::open(&config.snapshot.local_root, node_id.clone()).await?,
        );
        let catalog =
            Arc::new(super::MetadataCatalog::new(meta, Arc::clone(&repository)).with_pins(pins));
        let mut manager = Self::from_parts(catalog, repository, runtime_resolver, p2p_transport);
        manager.node_id = node_id;
        Ok(manager)
    }

    /// Builds a manager from the given components.
    pub fn from_parts(
        catalog: Arc<MetadataCatalog>,
        repository: Arc<dyn Repository>,
        runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Self {
        Self {
            catalog,
            repository,
            runtime_resolver,
            local_snapshots: Arc::new(DashMap::new()),
            node_id: String::new(),
            p2p_transport,
            mutation_locks: Arc::new(DashMap::new()),
        }
    }

    /// Assigns a node identity to a manually constructed manager (e.g. in tests).
    pub fn with_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.node_id = node_id.into();
        self
    }

    /// Returns the underlying SQL metadata client.
    pub fn meta(&self) -> &agentenv_metadata::MetaClient {
        self.catalog.meta_client()
    }

    /// Returns the shared snapshot GC pinner so sibling services (such as the
    /// Orchestrator) share the same node-local pin ledger.
    pub fn pinner(&self) -> Arc<dyn SnapshotPinner> {
        Arc::clone(&self.catalog) as Arc<dyn SnapshotPinner>
    }

    /// Returns the configured durable repository so sibling resource services
    /// can share the same PosixFS/OSS source of truth.
    pub fn repository(&self) -> Arc<dyn Repository> {
        Arc::clone(&self.repository)
    }

    pub fn release_orphan_pins(&self) {
        self.catalog.release_orphan_pins();
    }

    /// Retires Local snapshots previously owned by this node after process restart.
    pub async fn retire_local_snapshots(&self) -> anyhow::Result<()> {
        if self.node_id.is_empty() {
            return Ok(());
        }
        let entries = self
            .catalog
            .list_metadata(SnapshotListFilter::default())
            .await
            .context("list metadata after runtime cleanup")?;
        for entry in entries {
            if entry.placement == Some(SnapshotPlacement::Local)
                && entry.owner_node_id.as_deref() == Some(self.node_id.as_str())
            {
                self.delete(entry.id.to_string())
                    .await
                    .context("retire previous node-local snapshot")?;
            }
        }
        Ok(())
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
        Ok(self
            .catalog
            .get_metadata(reference)
            .await?
            .map(|entry| entry.id)
            .or_else(|| SnapshotId::parse(reference).ok()))
    }

    /// Publishes into the repository and metadata directory, then performs
    /// best-effort P2P publication. A failed publish drops `recording`, whose
    /// recorder keeps its own capture lease.
    #[tracing::instrument(skip(self, alias, config, manifest, recording), fields(snapshot_id = %id))]
    pub async fn publish(
        &self,
        id: SnapshotId,
        alias: Option<SnapshotAlias>,
        config: SnapshotConfig,
        manifest: SnapshotRuntimeManifest,
        recording: Option<crate::snapshot::StartupRecording>,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        let publication = super::publish_snapshot(
            self.catalog.as_ref(),
            self.repository.as_ref(),
            id,
            alias,
            config,
            manifest.clone(),
            recording,
        )
        .await?;
        self.publish_p2p_artifacts(&publication, &manifest).await;
        Ok(publication.record)
    }

    /// Publication outlives caller cancellation so all readers retain their files.
    /// Local publication expects a fresh ID; API retries capture a new snapshot.
    #[tracing::instrument(skip(self, snapshot, alias, config), fields(snapshot_id = %snapshot.id()))]
    pub async fn publish_snapshot(
        &self,
        snapshot: Arc<SnapshotHandle>,
        placement: SnapshotPlacement,
        alias: Option<SnapshotAlias>,
        config: SnapshotConfig,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        let manager = self.clone();
        tokio::spawn(async move {
            manager
                .publish_inner(snapshot, placement, alias, config)
                .await
        })
        .await
        .map_err(|error| RepositoryError::backend("finish snapshot publication", error))?
    }

    async fn publish_inner(
        &self,
        snapshot: Arc<SnapshotHandle>,
        placement: SnapshotPlacement,
        alias: Option<SnapshotAlias>,
        config: SnapshotConfig,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        if placement == SnapshotPlacement::Local {
            return self.publish_local(snapshot, alias, config).await;
        }
        let recording = snapshot.take_startup_recording();
        self.publish(
            snapshot.id().clone(),
            alias,
            config,
            snapshot.runtime_manifest().clone(),
            recording,
        )
        .await
    }

    async fn publish_local(
        &self,
        snapshot: Arc<SnapshotHandle>,
        alias: Option<SnapshotAlias>,
        config: SnapshotConfig,
    ) -> crate::repository::RepositoryResult<SnapshotRecord> {
        if self.node_id.is_empty() {
            return Err(RepositoryError::Unsupported {
                feature: "Local publication without a node identity".to_string(),
            });
        }
        let id = snapshot.id().clone();
        let mut record = SnapshotRecord::new(
            id.clone(),
            SnapshotPlacement::Local,
            alias,
            config,
            None,
            now_unix_ms(),
        );
        record.owner_node_id = Some(self.node_id.clone());
        record
            .validate()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
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
        match self.catalog.commit_local(record).await {
            Ok(record) => Ok(record),
            Err(error) => {
                // A failed response may follow a successful catalog write.
                if matches!(self.catalog.get(&id.to_string()).await, Ok(None)) {
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
        let _mutation_guard = self.lock_snapshot_mutation(snapshot_id.clone()).await;
        let Some(record) = self
            .catalog
            .get(&snapshot_id.to_string())
            .await?
            .filter(|record| record.id == snapshot_id)
        else {
            return Ok(None);
        };
        if !matches!(record.config.source, SnapshotSource::Sandbox { .. }) {
            return Ok(None);
        }
        if record.snapshot_type == SnapshotPlacement::Distributed {
            self.local_snapshots.remove(&record.id);
            return Ok(Some(record));
        }
        let snapshot = self.acquire_local_snapshot(&record)?;
        // Publish from the leased immutable manifest. The repository prepares
        // dense output without rewriting files used by existing Local readers.
        let promoted = self
            .publish_inner(
                snapshot,
                SnapshotPlacement::Distributed,
                record.alias.clone(),
                record.config.clone(),
            )
            .await
            .map_err(|error| match error {
                error @ (RepositoryError::ArtifactNotFound { .. }
                | RepositoryError::ManagedLayerNotFound { .. }) => RepositoryError::Unavailable {
                    reason: format!(
                        "publish Distributed closure for snapshot '{}': {error}",
                        record.id
                    ),
                },
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
    ) -> crate::repository::RepositoryResult<Arc<SnapshotHandle>> {
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
        manifest: &SnapshotRuntimeManifest,
    ) {
        let Some(transport) = self.p2p_transport.as_ref() else {
            return;
        };
        let record = &publication.record;
        let snapshot_id = &record.id;
        let Some(committed) = record.manifest.as_ref() else {
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
            &manifest.rootfs.layers,
            &rootfs_digests,
            &rootfs_uuids,
        ));
        let memory_uuids: HashSet<String> = committed
            .memory_layers
            .iter()
            .filter_map(|layer| layer.uuid.clone())
            .collect();
        let memory_digests: HashSet<String> = committed
            .memory_layers
            .iter()
            .map(|layer| layer.digest.clone())
            .collect();
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.memory.layers,
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
                &drive.layers,
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

    /// Loads a public snapshot record by id or alias.
    pub async fn get(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.catalog
            .get(id_or_alias.as_ref())
            .await
            .with_context(|| {
                format!(
                    "load committed snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })
    }

    /// Loads an internal (e.g. BuildKit builder) snapshot record by id or alias.
    pub async fn get_internal(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.catalog
            .get_in_namespace(id_or_alias.as_ref(), true)
            .await
            .with_context(|| {
                format!(
                    "load internal snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })
    }

    /// Lists snapshot records that match the given filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> anyhow::Result<Vec<SnapshotRecord>> {
        self.catalog
            .list(filter)
            .await
            .context("list committed snapshots through repository")
    }

    pub(crate) async fn list_metadata(
        &self,
        filter: SnapshotListFilter,
    ) -> anyhow::Result<Vec<SnapshotEntry>> {
        self.catalog
            .list_metadata(filter)
            .await
            .context("list snapshot directory")
    }

    pub(crate) async fn get_metadata(
        &self,
        reference: &str,
    ) -> anyhow::Result<Option<SnapshotEntry>> {
        self.catalog
            .get_metadata(reference)
            .await
            .context("look up snapshot placement")
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
            manager.catalog.delete(&reference).await?;
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
        SnapshotAlias::parse(alias).map_err(|error| RepositoryError::InvalidRequest {
            reason: error.to_string(),
        })?;
        self.catalog
            .get_metadata(alias)
            .await
            .map(|entry| entry.map(|entry| entry.id))
            .with_context(|| {
                format!("resolve committed snapshot alias '{alias}' through repository")
            })
    }

    /// Resolves a committed snapshot into node-local runnable artifact paths.
    pub async fn resolve_runnable(
        &self,
        snapshot: SnapshotRecord,
    ) -> anyhow::Result<SnapshotRecord<Arc<SnapshotHandle>>> {
        let result = match snapshot.snapshot_type {
            SnapshotPlacement::Distributed => {
                self.catalog
                    .pin(&snapshot.id)
                    .await
                    .context("pin committed snapshot before resolving runtime paths")?;
                let id = snapshot.id.clone();
                let record = Arc::new(snapshot);
                match self.runtime_resolver.resolve(Arc::clone(&record)).await {
                    Ok(handle) => {
                        let handle = handle.with_unpin(self.pinner(), id);
                        let record =
                            Arc::try_unwrap(record).unwrap_or_else(|shared| (*shared).clone());
                        Ok(record.with_manifest(handle))
                    }
                    Err(error) => {
                        self.catalog.unpin(&id);
                        Err(anyhow::Error::new(error))
                    }
                }
            }
            SnapshotPlacement::Local => self
                .acquire_local_snapshot(&snapshot)
                .map(|files| snapshot.with_manifest(files))
                .map_err(anyhow::Error::new),
        };
        result.context("resolve committed snapshot into runnable runtime paths")
    }

    /// Loads a committed public snapshot and immediately resolves it into runnable state.
    #[tracing::instrument(
        skip(self, id_or_alias),
        fields(snapshot_ref = %id_or_alias.as_ref())
    )]
    pub async fn load_runnable(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord<Arc<SnapshotHandle>>>> {
        let Some(snapshot) = self.get(id_or_alias.as_ref()).await? else {
            return Ok(None);
        };
        self.resolve_runnable(snapshot).await.map(Some)
    }

    /// Loads a committed internal snapshot and resolves it into runnable state.
    #[tracing::instrument(
        skip(self, id_or_alias),
        fields(snapshot_ref = %id_or_alias.as_ref())
    )]
    pub async fn load_runnable_internal(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord<Arc<SnapshotHandle>>>> {
        let Some(snapshot) = self.get_internal(id_or_alias.as_ref()).await? else {
            return Ok(None);
        };
        self.resolve_runnable(snapshot).await.map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlaybd::layer_key_from_digest;
    use crate::p2p::mock::MockTransport;
    use crate::repository::{BlobKey, SnapshotFile};
    use crate::snapshot::mock::{
        test_metadata_catalog, test_posix_storage, test_snapshot_manager,
        write_mock_built_artifacts,
    };
    use crate::snapshot::p2p::fixed_artifact_key;
    use crate::snapshot::{
        MetadataCatalog, SnapshotAlias, SnapshotConfig, SnapshotId, SnapshotSource,
    };
    use agentenv_metadata::{transport, MetaServer};
    use std::path::Path;
    use tempfile::TempDir;

    async fn test_manager(root: &Path) -> (MetaServer, SnapshotManager) {
        test_snapshot_manager(root).await
    }

    async fn local_manager(root: &Path) -> (MetaServer, SnapshotManager) {
        let (server, mut manager) = test_manager(root).await;
        manager.node_id = "node-a".to_string();
        (server, manager)
    }

    fn local_capture(root: &Path) -> (SnapshotConfig, Arc<SnapshotHandle>) {
        let config = SnapshotConfig {
            source: SnapshotSource::Sandbox {
                source_sandbox_id: "sandbox-source".to_string(),
                source_sandbox_metadata: None,
            },
            ..SnapshotConfig::mock()
        };
        let capture_dir = tempfile::Builder::new()
            .prefix(root.file_name().unwrap().to_str().unwrap())
            .rand_bytes(0)
            .tempdir_in(root.parent().unwrap())
            .unwrap();
        let manifest = write_mock_built_artifacts(capture_dir.path()).unwrap().2;
        let capture = Arc::new(SnapshotHandle::new(
            SnapshotId::generate(),
            manifest,
            capture_dir,
        ));
        (config, capture)
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
        async fn put_blob(
            &self,
            key: &crate::repository::BlobKey,
            body: crate::repository::BlobBody<'_>,
        ) -> crate::repository::RepositoryResult<()> {
            if matches!(key, BlobKey::Snapshot(_, SnapshotFile::Record)) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.put_blob(key, body).await
        }
        async fn read_blob(
            &self,
            key: &crate::repository::BlobKey,
        ) -> crate::repository::RepositoryResult<Vec<u8>> {
            self.inner.read_blob(key).await
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
        let (server, mut manager) = local_manager(root.path()).await;
        let primary = Arc::clone(&manager.repository);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        manager.repository = Arc::new(DeferredPublicationRepository {
            inner: Arc::clone(&primary),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        manager.catalog = Arc::new(MetadataCatalog::new(
            transport::embedded(server.clone()),
            Arc::clone(&manager.repository),
        ));
        let capture_root = root.path().join("capture");
        let (config, capture) = local_capture(&capture_root);
        let id = capture.id().clone();
        let publishing = manager.clone();
        let caller = tokio::spawn(async move {
            publishing
                .publish_snapshot(capture, SnapshotPlacement::Local, None, config)
                .await
        });
        entered.notified().await;
        assert!(server.get_snapshot(&id.to_string(), false).await?.is_none());
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(capture_root.exists());

        release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if server
                    .get_snapshot(&id.to_string(), false)
                    .await
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let runnable = manager.load_runnable(id.to_string()).await?.unwrap();
        assert!(runnable
            .manifest
            .runtime_manifest()
            .vm_state
            .path
            .starts_with(&capture_root));
        manager.delete(id.to_string()).await?;
        assert!(capture_root.exists());
        drop(runnable);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while capture_root.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        server.close().await;
        Ok(())
    }

    async fn seed_built_snapshot(manager: &SnapshotManager, snapshot_id: SnapshotId, alias: &str) {
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        manager
            .publish(
                snapshot_id,
                Some(SnapshotAlias::parse(alias).expect("alias should parse")),
                SnapshotConfig::mock(),
                manifest,
                None,
            )
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
        let (server, manager) = test_manager(tempdir.path()).await;
        let (_, _, mut manifest) = write_mock_built_artifacts(tempdir.path()).unwrap();
        manifest.vm_state.path = tempdir.path().join("missing-vm-state");
        let (recording, release, dropped) = pending_recording();
        assert!(manager
            .publish(
                SnapshotId::generate(),
                None,
                SnapshotConfig::mock(),
                manifest,
                Some(recording),
            )
            .await
            .is_err());
        assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
        release.send(()).unwrap();
        wait_for_lease_release(&dropped).await;
        server.close().await;
    }

    #[tokio::test]
    async fn cancelled_publish_keeps_capture_lease_until_pending_recorder_finishes() {
        let tempdir = TempDir::new().unwrap();
        let (server, manager) = test_manager(tempdir.path()).await;
        let (_, _, manifest) = write_mock_built_artifacts(tempdir.path()).unwrap();
        let (recording, release, dropped) = pending_recording();
        let publish = manager.publish(
            SnapshotId::generate(),
            None,
            SnapshotConfig::mock(),
            manifest,
            Some(recording),
        );
        // Cancel before the first poll; the recorder is already detached.
        drop(publish);
        assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
        release.send(()).unwrap();
        wait_for_lease_release(&dropped).await;
        server.close().await;
    }

    #[tokio::test]
    async fn resolved_posix_snapshot_keeps_vm_state_after_repository_deletion() -> anyhow::Result<()>
    {
        let root = TempDir::new()?;
        let (server, manager) = test_manager(root.path()).await;
        let id = SnapshotId::generate();
        seed_built_snapshot(&manager, id.clone(), "retained").await;
        let reader = manager.load_runnable("retained").await?.unwrap();
        let vm_state = reader.manifest.runtime_manifest().vm_state.path.clone();
        let expected = std::fs::read(&vm_state)?;

        manager.delete("retained").await?;
        let report = server.collect_garbage(64).await?;
        assert_eq!(report.reaped_snapshots, 1);

        assert!(!root
            .path()
            .join("repository/snapshots")
            .join(id.to_string())
            .exists());
        assert_eq!(std::fs::read(&vm_state)?, expected);
        assert!(reader
            .manifest
            .runtime_manifest()
            .memory
            .image_config_path
            .is_file());
        assert!(reader
            .manifest
            .runtime_manifest()
            .rootfs
            .image_config_path
            .is_file());
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn promotion_retries_failed_publish_and_keeps_existing_readers() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let (server, manager) = local_manager(root.path()).await;
        let (mut config, capture) = local_capture(&root.path().join("capture"));
        let source_metadata =
            std::collections::HashMap::from([("team".to_string(), "infra".to_string())]);
        config.source = SnapshotSource::Sandbox {
            source_sandbox_id: "sandbox-source".to_string(),
            source_sandbox_metadata: Some(source_metadata.clone()),
        };
        let local = manager
            .publish_snapshot(capture, SnapshotPlacement::Local, None, config)
            .await?;
        let reader = manager.resolve_runnable(local.clone()).await?;
        let config = &reader.manifest.runtime_manifest().memory.image_config_path;
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
        let stored = manager.catalog.get(&reference).await?.unwrap();
        for record in [&promoted, &repeated, &stored] {
            let SnapshotSource::Sandbox {
                source_sandbox_id,
                source_sandbox_metadata,
            } = &record.config.source
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
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_promotion_keeps_delete_waiting_for_publication() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let (server, mut manager) = local_manager(root.path()).await;
        let (config, capture) = local_capture(&root.path().join("capture"));
        let record = manager
            .publish_snapshot(capture, SnapshotPlacement::Local, None, config)
            .await?;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        manager.repository = Arc::new(DeferredPublicationRepository {
            inner: Arc::clone(&manager.repository),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        manager.catalog = Arc::new(MetadataCatalog::new(
            transport::embedded(server.clone()),
            Arc::clone(&manager.repository),
        ));
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
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn uncertain_catalog_commit_keeps_local_files() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let (server, manager) = local_manager(root.path()).await;
        server.close().await;
        let (config, capture) = local_capture(&root.path().join("capture"));
        let id = capture.id().clone();

        manager
            .publish_snapshot(capture, SnapshotPlacement::Local, None, config)
            .await
            .unwrap_err();
        assert!(manager.get(id.to_string()).await.is_err());
        let snapshot = manager.local_snapshots.get(&id).unwrap();
        assert!(snapshot.runtime_manifest().vm_state.path.is_file());
        Ok(())
    }

    #[tokio::test]
    async fn local_snapshot_requires_its_publishing_process() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let (server, manager) = local_manager(root.path()).await;
        let (config, capture) = local_capture(&root.path().join("capture"));
        let record = manager
            .publish_snapshot(capture, SnapshotPlacement::Local, None, config)
            .await?;
        let runnable = manager.load_runnable(record.id.to_string()).await?.unwrap();
        let foreign = SnapshotManager {
            node_id: "node-b".to_string(),
            ..manager.clone()
        };
        assert!(foreign.resolve_runnable(record.clone()).await.is_err());
        drop(foreign);
        drop(manager);

        let (repository, resolver) = test_posix_storage(root.path(), root.path().to_path_buf())?;
        let catalog = Arc::new(MetadataCatalog::new(
            transport::embedded(server.clone()),
            Arc::clone(&repository),
        ));
        let mut restarted = SnapshotManager::from_parts(catalog, repository, resolver, None);
        restarted.node_id = "node-a".to_string();
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
        assert!(runnable.manifest.runtime_manifest().vm_state.path.is_file());
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn repository_management_methods_delegate_to_committed_store() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (server, manager) = test_manager(tempdir.path()).await;
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
            .list(SnapshotListFilter::default())
            .await
            .expect("list should work");
        assert_eq!(listed.len(), 1);

        manager.delete("managed").await.expect("delete should work");
        assert!(manager
            .get("managed")
            .await
            .expect("load after delete should work")
            .is_none());
        server.close().await;
    }

    #[tokio::test]
    async fn load_runnable_uses_committed_snapshot_and_runtime_resolution() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (server, manager) = test_manager(tempdir.path()).await;
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, snapshot_id.clone(), "runnable").await;

        let runnable = manager
            .load_runnable("runnable")
            .await
            .expect("load runnable should work")
            .expect("runnable snapshot should exist");

        assert_eq!(runnable.id, snapshot_id);
        assert!(runnable
            .manifest
            .runtime_manifest()
            .rootfs
            .image_config_path
            .exists());
        assert!(runnable.manifest.runtime_manifest().vm_state.path.exists());
        server.close().await;
    }

    #[tokio::test]
    async fn publish_advertises_snapshot_artifacts_to_p2p_after_commit() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (repository, runtime_resolver) =
            test_posix_storage(tempdir.path(), tempdir.path().to_path_buf())
                .expect("posix storage");
        let (server, catalog) =
            test_metadata_catalog(tempdir.path(), Arc::clone(&repository)).await;
        let p2p = Arc::new(MockTransport::default());
        let manager =
            SnapshotManager::from_parts(catalog, repository, runtime_resolver, Some(p2p.clone()));

        let workspace = TempDir::new().expect("tempdir should exist");
        let (rootfs_lower, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let snapshot_id = SnapshotId::generate();

        manager
            .publish(
                snapshot_id.clone(),
                None,
                SnapshotConfig::mock(),
                manifest,
                None,
            )
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
        server.close().await;
    }

    #[tokio::test]
    async fn posix_resolve_never_repairs_missing_files_from_peers() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (repository, runtime_resolver) =
            test_posix_storage(tempdir.path(), tempdir.path().to_path_buf())
                .expect("posix storage");
        let (server, catalog) =
            test_metadata_catalog(tempdir.path(), Arc::clone(&repository)).await;
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(
            catalog,
            Arc::clone(&repository),
            runtime_resolver,
            Some(p2p.clone()),
        );
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let snapshot_id = SnapshotId::generate();
        let record = manager
            .publish(
                snapshot_id.clone(),
                None,
                SnapshotConfig::mock(),
                manifest,
                None,
            )
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
        server.close().await;
    }

    #[tokio::test]
    async fn failed_repository_commit_does_not_publish_to_p2p() {
        let workspace = TempDir::new().expect("tempdir");
        let (server, meta) = crate::metadata::test_meta(workspace.path()).await;
        let p2p = Arc::new(MockTransport::default());
        let repo: Arc<dyn Repository> = Arc::new(crate::snapshot::mock::MockRepository);
        let manager = SnapshotManager::from_parts(
            Arc::new(MetadataCatalog::new(meta, Arc::clone(&repo))),
            repo,
            Arc::new(crate::snapshot::mock::MockSnapshotRuntimeResolver),
            Some(p2p.clone()),
        );
        let (_, _, manifest) = write_mock_built_artifacts(workspace.path()).expect("artifacts");
        manager
            .publish(
                SnapshotId::generate(),
                None,
                SnapshotConfig::mock(),
                manifest,
                None,
            )
            .await
            .expect_err("repository rejects publication");
        assert_eq!(
            p2p.publish_count.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        server.close().await;
    }

    #[tokio::test]
    async fn failed_p2p_publication_does_not_undo_repository_commit() {
        let workspace = TempDir::new().expect("tempdir");
        let (server, mut manager) = test_manager(workspace.path()).await;
        let p2p = Arc::new(MockTransport::default());
        p2p.fail_publish
            .store(true, std::sync::atomic::Ordering::Relaxed);
        manager.p2p_transport = Some(p2p.clone());
        let (_, _, manifest) =
            write_mock_built_artifacts(&workspace.path().join("artifacts")).expect("artifacts");
        let stored = manager
            .publish(
                SnapshotId::generate(),
                None,
                SnapshotConfig::mock(),
                manifest,
                None,
            )
            .await
            .expect("P2P failure must not fail the commit");
        assert!(p2p.publish_count.load(std::sync::atomic::Ordering::Relaxed) > 0);
        let loaded = manager.get(stored.id.to_string()).await.unwrap().unwrap();
        assert_eq!(loaded.id, stored.id);
        assert!(loaded.manifest.is_some());
        server.close().await;
    }
}
