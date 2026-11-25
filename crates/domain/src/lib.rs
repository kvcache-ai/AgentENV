//! Shared snapshot records and their image, layer and sandbox values.

pub mod image;
pub mod layer;
pub mod sandbox;
pub mod snapshot;

pub use image::{ImageConfigEntry, ImageConfigs};
pub use layer::{ExternalLayer, LayerRef, ManagedLayerRef};
pub use sandbox::{CustomExtensionParams, SandboxResources, VirtualizationMode, VolumeMode};
pub use snapshot::{
    layer_path, posix_layer_path, posix_snapshot_dir, rootfs_snapshot_image_tag, snapshot_dir,
    CommandContext, CommittedAttachedDrive, MemoryStartupPackInfo, PersistedDiskImagePublication,
    SnapshotAlias, SnapshotConfig, SnapshotContent, SnapshotId, SnapshotListFilter,
    SnapshotManifest, SnapshotMetadata, SnapshotPlacement, SnapshotPublishMetadata,
    SnapshotPublishSource, SnapshotRecord, SnapshotRuntimeVersions, SnapshotSource,
    SnapshotSourceKind, SnapshotVolume, StartupCommand, TemplateBuildErrorReason,
    TemplateBuildInfo, TemplateBuildStatus, LAYERS_DIR,
};
