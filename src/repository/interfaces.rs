use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tempfile::NamedTempFile;

use super::errors::{RepositoryError, RepositoryResult};
use super::{BlobKey, LegacyDocumentCollection, LegacyDocumentKey};
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

/// Stored metadata for one repository blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobMetadata {
    pub size: u64,
}

fn unsupported<T>(feature: &str) -> RepositoryResult<T> {
    Err(RepositoryError::Unsupported {
        feature: feature.to_owned(),
    })
}

#[async_trait]
/// Durable storage shared by every node: snapshot artifacts, content-addressed
/// managed layers, build logs, and read-only legacy pre-SQL documents scanned
/// by `aenv-snapshot import`.
///
/// All mutable metadata, naming, reservations, and layer GC state live in the
/// SQL metadata service. The repository boundary excludes node-local derived
/// state: runtime-ready `image.json` files are materialized over every backend
/// and node-local cache directories never leak into durable records.
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

    /// Delete a batch of managed layers, returning one result per input digest in order.
    async fn delete_managed_layers(&self, digests: &[String]) -> Vec<RepositoryResult<()>> {
        (0..digests.len())
            .map(|_| unsupported("managed layer collection"))
            .collect()
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

    /// Stored metadata for one blob; `None` when absent.
    async fn stat_blob(&self, _key: &BlobKey) -> RepositoryResult<Option<BlobMetadata>> {
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

    // ── Runtime build logs ───────────────────────────────────────

    /// Reads one build's persisted log payload; `None` when absent.
    async fn get_build_log(&self, _id: &SnapshotId) -> RepositoryResult<Option<Vec<u8>>> {
        unsupported("build logs")
    }

    /// Atomically replaces one build's persisted log payload.
    async fn put_build_log(&self, _id: &SnapshotId, _bytes: Vec<u8>) -> RepositoryResult<()> {
        unsupported("build logs")
    }

    /// Removes one build's persisted log payload. Missing files succeed.
    async fn delete_build_log(&self, _id: &SnapshotId) -> RepositoryResult<()> {
        unsupported("build logs")
    }

    // ── Legacy pre-SQL migration scans ───────────────────────────

    /// Reads one legacy pre-SQL catalog document; `None` when absent.
    async fn get_legacy_document(
        &self,
        _key: LegacyDocumentKey,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        unsupported("legacy document migration")
    }

    /// Lists up to `limit` legacy document IDs after `after`, in stable order.
    async fn list_legacy_documents(
        &self,
        _collection: LegacyDocumentCollection,
        _after: Option<&str>,
        _limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        unsupported("legacy document migration")
    }

    /// Reads every live document in a legacy collection, in stable ID order.
    async fn read_legacy_documents(
        &self,
        _collection: LegacyDocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        unsupported("legacy document migration")
    }

    /// Writes a legacy pre-SQL document for migration test fixtures.
    #[doc(hidden)]
    async fn put_legacy_document(
        &self,
        _key: LegacyDocumentKey,
        _bytes: Vec<u8>,
    ) -> RepositoryResult<()> {
        unsupported("legacy document migration")
    }
}
