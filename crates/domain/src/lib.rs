//! Shared domain records and values across snapshots, volumes, images, builds, and layers.

pub mod build;
pub mod image;
pub mod layer;
pub mod resource;
pub mod sandbox;
pub mod snapshot;
pub mod volume;

pub use build::{BuildErrorReason, BuildJobRecord, BuildKind, BuildStatus, BuildTarget};
pub use image::{
    is_sha256_digest, sha256_digest, ImageConfigEntry, ImageConfigs, ImageName, ImageTag,
    ImageTagRecord, PublishedImage, DEFAULT_IMAGE_TAG,
};
pub use layer::{ExternalLayer, LayerRef, ManagedLayerRef};
pub use resource::ResourceKind;
pub use sandbox::{CustomExtensionParams, SandboxResources, VirtualizationMode, VolumeMode};
pub use snapshot::{
    layer_path, posix_layer_path, posix_snapshot_dir, rootfs_snapshot_image_tag, snapshot_dir,
    CommandContext, CommittedAttachedDrive, MemoryStartupPackInfo, PersistedDiskImagePublication,
    SnapshotAlias, SnapshotConfig, SnapshotContent, SnapshotId, SnapshotListFilter,
    SnapshotManifest, SnapshotMetadata, SnapshotPlacement, SnapshotPublishMetadata, SnapshotRecord,
    SnapshotRuntimeVersions, SnapshotSource, SnapshotSourceKind, SnapshotState, SnapshotVisibility,
    SnapshotVolume, StartupCommand, LAYERS_DIR,
};
pub use volume::{
    is_valid_volume_component, BuildCacheState, VolumePurpose, VolumeRecord, VolumeRecordPage,
    VolumeStatus,
};
