pub(crate) mod artifact_cache;
pub mod backends;
mod catalog;
mod gc;
pub mod image_export;
pub(crate) mod local;
mod manager;
mod metadata_catalog;
pub use metadata_catalog::{MetadataCatalog, MetadataHandle};
pub(crate) mod pins;
pub use gc::run_gc_once;
pub(crate) use pins::SnapshotLease;
pub use pins::SnapshotPinLease;
#[doc(hidden)]
pub mod mock;
pub(crate) mod p2p;
pub(crate) mod publish;
pub(crate) mod recontainerize;
pub(crate) mod resolver;
pub(crate) mod runtime_support;
pub(crate) mod startup_pack;
pub(crate) mod store;
mod types;

pub use catalog::{RepositoryCatalog, RepositorySnapshotExt, SnapshotCatalog, SnapshotListFilter};
pub use manager::SnapshotManager;
pub use publish::publish_snapshot;
pub use publish::SnapshotPublication;
pub use resolver::SnapshotRuntimeResolver;
pub use startup_pack::{
    drain_startup_manifest_tasks, MemoryStartupPackInfo, ResolvedStartupPack,
    ResolvedStartupPackSource, StartupRecording, MEMORY_STARTUP_PACK_ARTIFACT,
    MEMORY_STARTUP_TRACE_ARTIFACT,
};
pub(crate) use types::rootfs_snapshot_image_tag;
pub use types::{
    CommandContext, CommittedAttachedDrive, ExternalLayer, ManagedLayer, OverlaybdLayerRef,
    PersistedDiskImagePublication, ResolvedAttachedDrive, RunnableSnapshot, SnapshotAlias,
    SnapshotConfig, SnapshotHandle, SnapshotId, SnapshotManifest, SnapshotPlacement,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotRuntimeVersions,
    SnapshotSource, SnapshotSourceKind, SnapshotVolume, StartupCommand, TemplateBuildErrorReason,
    TemplateBuildInfo, TemplateBuildStatus, SNAPSHOT_ARTIFACT_LAYOUT,
};
pub use types::{
    SnapshotAttachedDriveArtifacts, SnapshotMemoryArtifacts, SnapshotRootfsArtifacts,
    SnapshotRuntimeManifest, FIRECRACKER_BACKEND,
};
