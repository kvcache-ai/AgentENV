mod artifacts;
mod drive;
mod manifest;
mod snapshot;
mod version;

pub use crate::types::{SnapshotAlias, SnapshotId};
pub use artifacts::SNAPSHOT_ARTIFACT_LAYOUT;
pub use drive::{CommittedAttachedDrive, ResolvedAttachedDrive};
pub use manifest::{SandboxSnapshotManifest, FIRECRACKER_BACKEND};
pub(crate) use snapshot::{rootfs_snapshot_image_tag, RuntimeArtifactLease};
pub use snapshot::{
    CommandContext, CommittedSnapshot, ExternalLayer, ManagedLayer, OverlaybdLayerRef,
    PersistedDiskImagePublication, RunnableSnapshot, SnapshotPublishMetadata,
    SnapshotPublishSource, SnapshotRecord, SnapshotSource, SnapshotSourceKind, SnapshotVolume,
    StartupCommand, TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
pub use version::SnapshotRuntimeVersions;
