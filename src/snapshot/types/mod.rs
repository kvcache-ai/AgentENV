mod artifacts;
mod drive;
mod manifest;
mod snapshot;

pub(crate) use crate::types::now_unix_ms;
pub use crate::types::{ExternalLayer, ManagedLayer, OverlaybdLayerRef, SnapshotAlias, SnapshotId};
pub use agentenv_domain::SnapshotRuntimeVersions;
pub use artifacts::SNAPSHOT_ARTIFACT_LAYOUT;
pub use drive::{CommittedAttachedDrive, ResolvedAttachedDrive};
pub use manifest::{
    SandboxSnapshotManifest, SnapshotAttachedDriveArtifacts, SnapshotMemoryArtifacts,
    SnapshotRootfsArtifacts, FIRECRACKER_BACKEND,
};
pub(crate) use snapshot::rootfs_snapshot_image_tag;
pub use snapshot::{
    CommandContext, CommittedSnapshot, PersistedDiskImagePublication, RunnableSnapshot, Snapshot,
    SnapshotPlacement, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord,
    SnapshotSource, SnapshotSourceKind, SnapshotVolume, StartupCommand, TemplateBuildErrorReason,
    TemplateBuildInfo, TemplateBuildStatus,
};
