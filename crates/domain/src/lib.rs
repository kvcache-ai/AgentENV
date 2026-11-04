//! Shared snapshot records and their image, layer and sandbox values.

pub mod image;
pub mod layer;
pub mod sandbox;
pub mod snapshot;

pub use image::{ImageConfigEntry, ImageConfigs};
pub use layer::{ExternalLayer, LayerRef, ManagedLayerRef};
pub use sandbox::{CustomExtensionParams, SandboxResources, VirtualizationMode, VolumeMode};
pub use snapshot::{
    rootfs_snapshot_image_tag, CommandContext, CommittedAttachedDrive, CommittedSnapshot,
    MemoryStartupPackInfo, PersistedDiskImagePublication, SnapshotAlias, SnapshotId,
    SnapshotListFilter, SnapshotPlacement, SnapshotPublishMetadata, SnapshotPublishSource,
    SnapshotRecord, SnapshotRuntimeVersions, SnapshotSource, SnapshotSourceKind, SnapshotVolume,
    StartupCommand, TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
