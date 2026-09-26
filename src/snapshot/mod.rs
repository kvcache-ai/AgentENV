mod artifact_cache;
pub mod image_export;
mod manager;
#[doc(hidden)]
pub mod mock;
mod p2p;
pub mod repository;
pub(crate) mod runtime_support;
pub(crate) mod startup_pack;
mod types;

pub use manager::SnapshotManager;
pub use repository::{RepositoryError, RepositoryResult, SnapshotListFilter};
pub use startup_pack::{
    drain_startup_manifest_tasks, startup_manifest_shutdown_requested, MemoryStartupPackInfo,
    ResolvedStartupPack, ResolvedStartupPackSource, StartupRecording, MEMORY_STARTUP_PACK_ARTIFACT,
    MEMORY_STARTUP_TRACE_ARTIFACT,
};
pub(crate) use types::rootfs_snapshot_image_tag;
pub use types::{
    CommandContext, CommittedAttachedDrive, CommittedSnapshot, ExternalLayer, ManagedLayer,
    OverlaybdLayerRef, PersistedDiskImagePublication, ResolvedAttachedDrive, RunnableSnapshot,
    SnapshotAlias, SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord,
    SnapshotRuntimeVersions, SnapshotSource, SnapshotSourceKind, SnapshotVolume, StartupCommand,
    TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus, SNAPSHOT_ARTIFACT_LAYOUT,
};
