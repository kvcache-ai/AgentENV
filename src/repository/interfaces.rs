use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tempfile::NamedTempFile;

use super::errors::{RepositoryError, RepositoryResult};
use super::{BlobKey, DocumentCollection, DocumentKey};
use crate::cfg::{SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
use crate::types::SnapshotId;

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

#[async_trait]
/// Durable storage shared by every node: small shared documents, snapshot
/// files and layers, and volume backing layers.
///
/// A repository stores; it does not decide. The catalogs built on its
/// documents (the snapshot and volume catalogs, and
/// [`super::BuildCacheStore`]) own the records, aliases, and their rules, the
/// same on every backend. It depends on no module that builds on it.
///
/// The repository boundary excludes node-local derived state: runtime-ready
/// `image.json` files are materialized by the snapshot runtime resolver over
/// every backend, and node-local cache directories never leak into durable
/// records. Shared
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
