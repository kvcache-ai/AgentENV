pub(crate) mod artifact_cache;
pub mod backends;
mod catalog;
mod local;
mod manager;
mod manifest;
#[doc(hidden)]
pub mod mock;
mod p2p;
mod resolver;
mod runtime;
pub(crate) mod startup_pack;
mod store;
pub mod tool;

pub(crate) use crate::types::now_unix_ms;
pub(crate) use agentenv_domain::rootfs_snapshot_image_tag;
pub use agentenv_domain::{
    BuildErrorReason, BuildJobRecord, BuildKind, BuildStatus, BuildTarget, CommandContext,
    CommittedAttachedDrive, ExternalLayer, LayerRef, ManagedLayerRef,
    PersistedDiskImagePublication, SnapshotAlias, SnapshotConfig, SnapshotId, SnapshotManifest,
    SnapshotPlacement, SnapshotRecord, SnapshotRuntimeVersions, SnapshotSource, SnapshotSourceKind,
    SnapshotVisibility, SnapshotVolume, StartupCommand,
};
pub use agentenv_metadata::{SnapshotEntry, SnapshotState};
pub use catalog::{MetadataCatalog, SnapshotListFilter};
pub(crate) use local::{create_snapshot_dir, finish_snapshot};
pub use manager::SnapshotManager;
pub use manifest::{
    SnapshotAttachedDriveArtifacts, SnapshotMemoryArtifacts, SnapshotRootfsArtifacts,
    SnapshotRuntimeManifest, FIRECRACKER_BACKEND, SNAPSHOT_ARTIFACT_LAYOUT,
};
pub use resolver::SnapshotRuntimeResolver;
pub use runtime::SnapshotHandle;
pub use startup_pack::{
    drain_startup_manifest_tasks, MemoryStartupPackInfo, ResolvedStartupPack,
    ResolvedStartupPackSource, StartupRecording, MEMORY_STARTUP_PACK_ARTIFACT,
    MEMORY_STARTUP_TRACE_ARTIFACT,
};
pub use store::{publish_snapshot, SnapshotPublication};
