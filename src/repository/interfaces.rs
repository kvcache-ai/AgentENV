use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tempfile::NamedTempFile;

use super::errors::{RepositoryError, RepositoryResult};
use super::{BlobKey, DocumentCollection, DocumentKey};
use crate::cfg::{SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
use crate::snapshot::{
    RunnableSnapshot, SnapshotId, SnapshotRecord, SnapshotSourceKind, TemplateBuildStatus,
};

/// A committed snapshot and the local upload files kept alive for post-commit
/// publication. Temporary paths are never stored in the durable record.
#[derive(Debug)]
pub struct SnapshotPublication {
    pub record: SnapshotRecord,
    pub(crate) local_layers: Vec<crate::snapshot::recontainerize::PreparedLayerUpload>,
}

/// How snapshots and volumes are published into one repository.
#[derive(Clone, Debug)]
pub struct PublishPolicy {
    /// Recontainerizes raw local layers as zfile before they are stored.
    pub compression: SnapshotPublishCompressionConfig,
    /// Whether disk images go to their source registry or to managed layers.
    pub image_storage: SnapshotImageStoragePolicy,
}

impl Default for PublishPolicy {
    /// Raw layers, stored as managed layers.
    fn default() -> Self {
        Self {
            compression: SnapshotPublishCompressionConfig {
                enabled: false,
                ..SnapshotPublishCompressionConfig::default()
            },
            image_storage: SnapshotImageStoragePolicy::default(),
        }
    }
}

/// What a stored layer is used for. It labels metrics and logs only; layers
/// are stored by digest alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayerRole {
    Rootfs,
    AttachedDrive,
    Memory,
}

/// The contents [`Repository::put_blob`] stores.
#[derive(Debug)]
pub enum BlobBody<'a> {
    File(&'a Path),
    Bytes(Vec<u8>),
}

/// Where a stored blob is read from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobLocation {
    /// A path on this node.
    Local(PathBuf),
    /// The blob's URL.
    Remote(String),
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
/// `image.json` files are materialized by one [`SnapshotRuntimeResolver`]
/// over every backend, and node-local cache directories never leak into
/// durable records. Shared
/// layer imports must use atomic protocols so concurrent writers never expose
/// half-written layers.
pub trait Repository: Send + Sync {
    /// How snapshots and volumes are published into this repository.
    fn publish_policy(&self) -> PublishPolicy {
        PublishPolicy::default()
    }

    /// The overlaybd `repoBlobUrl` that reads layers stored here, if remote
    /// readers address them by URL.
    fn managed_layers_repo_blob_url(&self) -> Option<String> {
        None
    }

    /// Stores one blob. A layer that is already stored is kept, since its
    /// digest names its contents; a snapshot file is replaced.
    async fn put_blob(&self, _key: &BlobKey, _body: BlobBody<'_>) -> RepositoryResult<()> {
        unsupported("blob storage")
    }

    /// Where readers on this node find a blob: in place on a shared filesystem,
    /// or by URL.
    fn locate_blob(&self, _key: &BlobKey) -> RepositoryResult<BlobLocation> {
        unsupported("blob storage")
    }

    /// Whether a blob is stored.
    async fn has_blob(&self, _key: &BlobKey) -> RepositoryResult<bool> {
        unsupported("blob storage")
    }

    /// Downloads one blob to `destination`, returning its size.
    async fn fetch_blob(&self, _key: &BlobKey, _destination: &Path) -> RepositoryResult<u64> {
        unsupported("blob storage")
    }

    /// Reads one small blob; `ArtifactNotFound` when it is absent.
    async fn read_blob(&self, _key: &BlobKey) -> RepositoryResult<Vec<u8>> {
        unsupported("blob storage")
    }

    /// Removes every file of one snapshot. Shared layers stay. Missing files
    /// are considered success.
    async fn delete_snapshot_blobs(&self, _id: &SnapshotId) -> RepositoryResult<()> {
        unsupported("blob storage")
    }

    /// A new temporary file to prepare a blob in. It is placed where putting
    /// it costs least: a POSIX repository links it in without copying.
    fn staging_file(&self) -> RepositoryResult<NamedTempFile> {
        NamedTempFile::new().map_err(|error| RepositoryError::backend("create staging file", error))
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
