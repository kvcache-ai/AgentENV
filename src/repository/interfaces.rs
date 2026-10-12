use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use super::errors::{RepositoryError, RepositoryResult};
use super::{DocumentCollection, DocumentKey};
use crate::sandbox::SandboxSnapshotManifest;
use crate::snapshot::{
    RunnableSnapshot, SnapshotId, SnapshotPublishMetadata, SnapshotRecord, SnapshotSourceKind,
    TemplateBuildErrorReason, TemplateBuildStatus,
};

/// A committed snapshot and the local upload files kept alive for post-commit
/// publication. Temporary paths are never stored in the durable record.
#[derive(Debug)]
pub struct SnapshotPublication {
    pub record: SnapshotRecord,
    pub(crate) local_layers: Vec<super::backends::common::recontainerize::PreparedLayerUpload>,
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

/// Computes a document's new bytes from its current bytes, if any, or `None`
/// to leave it unchanged. Backends may call it again when a concurrent writer
/// wins, so it must be a pure function of its input.
pub type DocumentUpdate =
    Box<dyn Fn(Option<&[u8]>) -> RepositoryResult<Option<Vec<u8>>> + Send + Sync>;

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
/// Durable repository shared by every node: snapshots, volumes, and small
/// shared documents.
///
/// This trait owns the repository truth for [`SnapshotRecord`] values and committed snapshot
/// artifacts. A record is the catalog identity and lifecycle state for a snapshot:
///
/// - template records may exist before build artifacts are committed
/// - sandbox records are created by publishing an already captured runtime snapshot
/// - committed records carry a [`crate::snapshot::CommittedSnapshot`] payload with artifact
///   references
///
/// Implementations are responsible for durable state that may be shared across processes or nodes:
///
/// - snapshot records and template build state
/// - alias-to-snapshot bindings
/// - committed artifacts such as Firecracker snapshots and managed layers
/// - publish / delete visibility rules
///
/// Callers may assume returned records describe durable repository state rather than process-local
/// working directories or node-local runtime cache files.
///
/// The repository boundary intentionally excludes node-local derived state. In particular:
///
/// - local build-artifact allocation is owned by the manager rather than the repository
/// - runtime-ready `image.json` files should be materialized by [`SnapshotRuntimeResolver`]
/// - node-local cache directories should not leak back into snapshot records
/// - repository implementations should not require sandbox launch code to understand backend-specific
///   local build layouts
///
/// Backend guidance for shared POSIX or distributed filesystems:
///
/// - alias claim / release should be concurrency-safe
/// - publish should only make aliases visible after the snapshot record and committed artifact
///   payload are durable enough for subsequent readers to resolve
/// - delete should avoid exposing partially removed records or committed artifacts
/// - shared artifact imports should use atomic protocols so concurrent writers do not expose
///   half-written managed layers
pub trait Repository: Send + Sync {
    /// Creates a durable template snapshot record before build artifacts exist.
    ///
    /// Backends should reject records that already contain a committed artifact payload and should
    /// only accept records whose source kind is template.
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord>;

    /// Publishes manager-owned local artifacts into committed repository state.
    ///
    /// Implementations are responsible for:
    ///
    /// - reading build artifacts from the provided local artifact description
    /// - validating publish metadata
    /// - importing any repository-owned managed layers
    /// - committing a durable snapshot record with a committed artifact payload
    /// - binding aliases only after the record is durable
    /// - cleaning up or rolling back partially committed state on failure
    ///
    /// On success, the returned [`SnapshotRecord`] must contain committed artifact state.
    /// On failure, callers may assume the backend attempted best-effort cleanup of partially published
    /// state, but durable shared artifacts may still be retained when doing so is safe and intentional.
    ///
    /// `recording`, when present, is the in-flight startup-manifest recording
    /// for this snapshot; the backend awaits it after the layer uploads and
    /// before building the manifest, so the recording VM overlaps the upload
    /// phase instead of serializing ahead of it.
    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
        recording: Option<crate::snapshot::StartupRecording>,
    ) -> RepositoryResult<SnapshotRecord>;

    /// Like `publish`, retaining any transformed local layer files until the
    /// caller finishes best-effort P2P publication after the durable commit.
    async fn publish_with_local_layers(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: SandboxSnapshotManifest,
        recording: Option<crate::snapshot::StartupRecording>,
    ) -> RepositoryResult<SnapshotPublication> {
        Ok(SnapshotPublication {
            record: self.publish(metadata, manifest, recording).await?,
            local_layers: Vec::new(),
        })
    }

    /// Loads one snapshot record by repository id or alias.
    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>>;

    /// Lists snapshot records matching the provided filter.
    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>>;

    /// Deletes one snapshot record by repository id or alias.
    ///
    /// Returns `Ok(())` on success. The operation is idempotent:
    /// if the snapshot does not exist, it is still considered success.
    ///
    /// For committed records, implementations should also remove per-snapshot committed artifacts and
    /// any alias binding that still points at the deleted id.
    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()>;

    /// Resolves a human-readable alias to the current snapshot id.
    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>>;

    /// Atomically transitions one template build from waiting to building.
    ///
    /// Backends should reject non-template records and template records that are no longer waiting.
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord>;

    /// Marks one template build as failed.
    ///
    /// Backends should preserve the existing record identity, alias, resources, and source while
    /// recording the failure state and reason.
    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()>;

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
