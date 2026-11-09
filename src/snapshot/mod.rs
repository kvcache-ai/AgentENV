pub(crate) mod artifact_cache;
pub mod backends;
mod catalog;
pub mod image_export;
pub(crate) mod local;
mod manager;
mod metadata_catalog;
pub use metadata_catalog::MetadataCatalog;
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
    CommandContext, CommittedAttachedDrive, CommittedSnapshot, ExternalLayer, ManagedLayer,
    OverlaybdLayerRef, PersistedDiskImagePublication, ResolvedAttachedDrive, RunnableSnapshot,
    Snapshot, SnapshotAlias, SnapshotId, SnapshotPlacement, SnapshotPublishMetadata,
    SnapshotPublishSource, SnapshotRecord, SnapshotRuntimeVersions, SnapshotSource,
    SnapshotSourceKind, SnapshotVolume, StartupCommand, TemplateBuildErrorReason,
    TemplateBuildInfo, TemplateBuildStatus, SNAPSHOT_ARTIFACT_LAYOUT,
};
pub use types::{
    SandboxSnapshotManifest, SnapshotAttachedDriveArtifacts, SnapshotMemoryArtifacts,
    SnapshotRootfsArtifacts, FIRECRACKER_BACKEND,
};
