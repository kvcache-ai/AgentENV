use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use super::errors::{RepositoryError, RepositoryResult};
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::types::{
    RunnableSnapshot, SnapshotId, SnapshotPublishMetadata, SnapshotRecord, SnapshotSourceKind,
    TemplateBuildErrorReason, TemplateBuildStatus,
};

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VolumeRecordPage {
    pub records: Vec<crate::volume::VolumeRecord>,
    /// The last returned volume ID. Supplying it to the next request resumes
    /// after that record in the repository's stable shard-and-ID order.
    pub next_volume_id: Option<String>,
}

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
/// Durable snapshot repository.
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
pub trait SnapshotRepository: Send + Sync {
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
    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord>;

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

    /// Resolves a volume by ID first, then by its unique name.
    async fn get_volume(
        &self,
        _reference: &str,
    ) -> RepositoryResult<Option<crate::volume::VolumeRecord>> {
        unsupported("volume catalog")
    }

    /// Lists one bounded page ordered by the repository's stable volume cursor.
    async fn list_volumes_page(
        &self,
        _after_volume_id: Option<&str>,
        _limit: usize,
    ) -> RepositoryResult<VolumeRecordPage> {
        unsupported("volume catalog")
    }

    /// Creates one durable volume record and atomically claims its unique name.
    async fn create_volume(&self, _record: crate::volume::VolumeRecord) -> RepositoryResult<()> {
        unsupported("volume catalog")
    }

    /// Updates one existing durable volume record.
    async fn put_volume(&self, _record: crate::volume::VolumeRecord) -> RepositoryResult<()> {
        unsupported("volume catalog")
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

    /// Removes one durable volume record. Missing records are considered success.
    async fn delete_volume(&self, _volume_id: &str) -> RepositoryResult<()> {
        unsupported("volume catalog")
    }

    /// Acquires an exclusive volume reservation atomically where the backend
    /// supports it. `Ok(Some(owner))` reports an existing conflicting owner.
    async fn reserve_volume(
        &self,
        _volume_id: &str,
        _owner: &str,
    ) -> RepositoryResult<Option<String>> {
        unsupported("volume reservations")
    }

    /// Adds a shared lease for a read-only volume.
    async fn reserve_read_only_volume(
        &self,
        _volume_id: &str,
        _owner: &str,
    ) -> RepositoryResult<()> {
        unsupported("read-only volume reservations")
    }

    /// Returns only volumes currently reserved or leased by one owner.
    async fn list_volumes_by_owner(
        &self,
        _owner: &str,
    ) -> RepositoryResult<Vec<crate::volume::VolumeRecord>> {
        unsupported("volume reservations")
    }

    /// Releases reservations held by `from`, or rebinds them when `to` is set.
    async fn replace_volume_owner(&self, _from: &str, _to: Option<&str>) -> RepositoryResult<()> {
        unsupported("volume reservations")
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
