use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use super::errors::{RepositoryError, RepositoryResult};
use super::{DocumentCollection, DocumentKey};
use crate::sandbox::SandboxSnapshotManifest;
use crate::snapshot::{
    CommittedAttachedDrive, ManagedLayer, OverlaybdLayerRef, PersistedDiskImagePublication,
    RunnableSnapshot, SnapshotId, SnapshotPublishMetadata, SnapshotRecord, SnapshotSourceKind,
    TemplateBuildStatus,
};

/// A committed snapshot and the local upload files kept alive for post-commit
/// publication. Temporary paths are never stored in the durable record.
#[derive(Debug)]
pub struct SnapshotPublication {
    pub record: SnapshotRecord,
    pub(crate) local_layers: Vec<crate::snapshot::recontainerize::PreparedLayerUpload>,
}

/// What a backend stored for one snapshot before its record is committed.
#[derive(Debug, Default)]
pub struct StoredSnapshot {
    pub rootfs_layers: Vec<OverlaybdLayerRef>,
    pub memory_layers: Vec<ManagedLayer>,
    pub attached_drives: Vec<CommittedAttachedDrive>,
    pub disk_publications: Vec<PersistedDiskImagePublication>,
    /// Transformed local layer files, kept for post-commit P2P publication.
    pub(crate) local_layers: Vec<crate::snapshot::recontainerize::PreparedLayerUpload>,
}

/// Snapshot record list filter.
///
/// When multiple fields are present they combine with AND semantics.
/// When all fields are `None`, the filter matches all snapshot records,
/// including pending template builds and committed snapshots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotListFilter {
    /// Match aliases that start with this prefix.
    pub alias_prefix: Option<String>,
    /// Restrict results to this exact set of snapshot ids.
    pub snapshot_ids: Option<Vec<SnapshotId>>,
    /// Restrict results to a single snapshot id or exact alias.
    pub snapshot_id_or_alias: Option<String>,
    /// Restrict results to snapshots captured from this source sandbox id.
    ///
    /// This only matches records whose source is [`SnapshotSourceKind::Sandbox`].
    pub source_sandbox_id: Option<String>,
    /// Restrict results to records with these source kinds.
    pub sources: Option<Vec<SnapshotSourceKind>>,
    /// Restrict results to template records whose build status is in this set.
    ///
    /// Sandbox records never match this field.
    pub template_statuses: Option<Vec<TemplateBuildStatus>>,
}

/// What an update does to a document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DocumentChange {
    Keep,
    Put(Vec<u8>),
    Delete,
}

/// Decides how to change a document from its current bytes, if any. Backends
/// may call it again when a concurrent writer wins, so it must be a pure
/// function of its input.
pub type DocumentUpdate =
    Box<dyn Fn(Option<&[u8]>) -> RepositoryResult<DocumentChange> + Send + Sync>;

fn unsupported<T>(feature: &str) -> RepositoryResult<T> {
    Err(RepositoryError::Unsupported {
        feature: feature.to_owned(),
    })
}

impl SnapshotListFilter {
    pub fn matches_all() -> Self {
        Self::default()
    }

    pub fn by_ids<I>(snapshot_ids: I) -> Self
    where
        I: IntoIterator<Item = SnapshotId>,
    {
        Self {
            snapshot_ids: Some(snapshot_ids.into_iter().collect()),
            ..Self::default()
        }
    }

    pub fn templates() -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Template]),
            ..Self::default()
        }
    }

    pub fn sandbox_snapshots(
        source_sandbox_id: Option<String>,
        snapshot_id_or_alias: Option<String>,
    ) -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Sandbox]),
            source_sandbox_id,
            snapshot_id_or_alias: snapshot_id_or_alias.map(|value| {
                let unqualified = value
                    .rsplit_once('/')
                    .map_or(value.as_str(), |(_, name)| name);
                unqualified
                    .split_once(':')
                    .map_or(unqualified, |(name, _)| name)
                    .to_string()
            }),
            ..Self::default()
        }
    }
}

#[async_trait]
/// Durable storage shared by every node: small shared documents, snapshot
/// files and layers, and volume backing layers.
///
/// A repository stores; it does not decide. The catalogs built on its
/// documents ([`crate::snapshot::SnapshotCatalog`],
/// [`crate::volume::VolumeCatalog`], [`super::BuildCacheStore`]) own the
/// records, aliases, and their rules, the same on every backend.
///
/// The repository boundary excludes node-local derived state: runtime-ready
/// `image.json` files are materialized by [`SnapshotRuntimeResolver`], and
/// node-local cache directories never leak into durable records. Shared
/// layer imports must use atomic protocols so concurrent writers never expose
/// half-written layers.
pub trait Repository: Send + Sync {
    /// Stores one captured snapshot's per-snapshot files and layers, before
    /// its record is committed. On failure the backend removes what it
    /// stored, except shared content-addressed layers.
    async fn store_snapshot(
        &self,
        _metadata: &SnapshotPublishMetadata,
        _manifest: &SandboxSnapshotManifest,
    ) -> RepositoryResult<StoredSnapshot> {
        unsupported("snapshot storage")
    }

    /// Removes one snapshot's per-snapshot files and external disk
    /// publications. Shared layers stay. Missing files are considered success.
    async fn delete_snapshot_files(
        &self,
        _id: &SnapshotId,
        _publications: &[PersistedDiskImagePublication],
    ) -> RepositoryResult<()> {
        unsupported("snapshot storage")
    }

    /// Stores one snapshot's startup memory manifest.
    async fn put_startup_manifest(
        &self,
        _id: &SnapshotId,
        _bytes: Vec<u8>,
    ) -> RepositoryResult<()> {
        unsupported("snapshot storage")
    }

    /// Removes one snapshot's startup memory manifest, if any.
    async fn delete_startup_manifest(&self, _id: &SnapshotId) -> RepositoryResult<()> {
        unsupported("snapshot storage")
    }

    /// Reads one shared document; `None` when absent.
    async fn get_document(&self, _key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        unsupported("repository documents")
    }

    /// Atomically replaces one shared document.
    async fn put_document(&self, _key: DocumentKey, _bytes: Vec<u8>) -> RepositoryResult<()> {
        unsupported("repository documents")
    }

    /// Atomically applies `update` against concurrent writers on every node,
    /// returning the document it was applied to.
    async fn update_document(
        &self,
        _key: DocumentKey,
        _update: DocumentUpdate,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        unsupported("repository documents")
    }

    /// Removes one shared document. Missing documents are considered success.
    async fn delete_document(&self, _key: DocumentKey) -> RepositoryResult<()> {
        unsupported("repository documents")
    }

    /// Lists up to `limit` document IDs after `after`, in the backend's stable order.
    async fn list_documents(
        &self,
        _collection: DocumentCollection,
        _after: Option<&str>,
        _limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        unsupported("repository documents")
    }

    /// Reads every live document in `collection`, in the backend's stable ID order.
    async fn read_documents(
        &self,
        _collection: DocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        unsupported("repository documents")
    }

    /// Publishes the current node-local OverlayBD backing and returns logical
    /// layer references suitable for the shared volume catalog.
    async fn publish_volume_backing(
        &self,
        _volume_id: &str,
        _image_config_path: &Path,
    ) -> RepositoryResult<Vec<crate::snapshot::OverlaybdLayerRef>> {
        unsupported("volume backing publication")
    }

    /// Materializes a node-local runtime image config from shared logical layers.
    async fn materialize_volume_backing(
        &self,
        _volume_id: &str,
        _layers: &[crate::snapshot::OverlaybdLayerRef],
        _destination: &Path,
    ) -> RepositoryResult<PathBuf> {
        unsupported("volume backing materialization")
    }
}

#[async_trait]
/// Resolves committed snapshot records into node-local runnable paths.
///
/// This trait sits at the boundary between repository truth and launch-time derived state.
/// Implementations consume a committed [`SnapshotRecord`] and may materialize node-local helper files
/// such as runnable overlaybd `image.json` configs for the current node.
///
/// Contract:
///
/// - consumes a [`SnapshotRecord`] whose committed payload is present
/// - returns paths that are directly usable by sandbox / firecracker launch code on the current node
/// - may materialize node-local derived files such as runnable `image.json`
/// - must not mutate committed repository truth when generating node-local cache files
///
/// Backend guidance:
///
/// - runtime cache directories should be treated as node-local derived state
/// - shared repository storage and node-local runtime cache should be separated whenever possible
/// - concurrent resolves on one node should prefer atomic cache writes so readers never observe
///   partially written derived configs
pub trait SnapshotRuntimeResolver: Send + Sync {
    /// Resolves one committed snapshot record into a runtime-ready view for the current node.
    async fn resolve(&self, snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot>;
}
