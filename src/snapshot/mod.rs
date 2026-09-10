mod artifact_cache;
pub mod image_export;
mod manager;
#[doc(hidden)]
pub mod mock;
mod p2p;
mod prefetch;
pub mod repository;
pub(crate) mod runtime_support;
mod types;

pub use manager::SnapshotManager;
pub use prefetch::{
    parse_prefetch_manifest, MemoryPrefetchFile, MAX_PREFETCH_BYTES, MAX_PREFETCH_RANGES,
    MEMORY_PREFETCH_ARTIFACT, MEMORY_PREFETCH_VERSION,
};
pub use repository::{RepositoryError, RepositoryResult, SnapshotListFilter};
pub(crate) use types::rootfs_snapshot_image_tag;
#[cfg(test)]
pub(crate) use types::RuntimeArtifactLease;
pub use types::{
    CommandContext, CommittedAttachedDrive, CommittedSnapshot, ExternalLayer, ManagedLayer,
    OverlaybdLayerRef, PersistedDiskImagePublication, ResolvedAttachedDrive, RunnableSnapshot,
    SnapshotAlias, SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord,
    SnapshotRuntimeVersions, SnapshotSource, SnapshotSourceKind, SnapshotVolume, StartupCommand,
    TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus, SNAPSHOT_ARTIFACT_LAYOUT,
};
