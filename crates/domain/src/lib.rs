//! Shared domain records and values across snapshots, volumes, images, builds, and layers.

pub mod build;
pub mod image;
pub mod layer;
pub mod resource;
pub mod sandbox;
pub mod snapshot;
pub mod volume;

macro_rules! impl_str_enum {
    ($ty:ty, $label:literal { $($variant:ident => $str:literal),+ $(,)? }) => {
        impl $ty {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $str,)+
                }
            }

            pub fn parse(value: &str) -> anyhow::Result<Self> {
                match value {
                    $($str => Ok(Self::$variant),)+
                    other => anyhow::bail!("unknown {} '{other}'", $label),
                }
            }
        }
    };
}
pub(crate) use impl_str_enum;

pub use build::{BuildErrorReason, BuildJobRecord, BuildJobTransition, BuildStatus, BuildTarget};
pub use image::{
    is_sha256_digest, is_valid_oci_tag, sha256_digest, ImageConfigEntry, ImageConfigs, ImageName,
    ImageTag, ImageTagRecord, PublishedImage, DEFAULT_IMAGE_TAG,
};
pub use layer::{ExternalLayer, LayerRef, ManagedLayerRef};
pub use resource::ResourceKind;
pub use sandbox::{CustomExtensionParams, SandboxResources, VirtualizationMode};
pub use snapshot::{
    layer_path, posix_layer_path, posix_snapshot_dir, rootfs_snapshot_image_tag, snapshot_dir,
    CommandContext, CommittedAttachedDrive, MemoryStartupPackInfo, PersistedDiskImagePublication,
    SnapshotAlias, SnapshotConfig, SnapshotId, SnapshotListFilter, SnapshotManifest,
    SnapshotPlacement, SnapshotRecord, SnapshotRuntimeVersions, SnapshotSource, SnapshotSourceKind,
    SnapshotVolume, StartupCommand, LAYERS_DIR,
};
pub use volume::{
    is_valid_volume_component, VolumeLeaseOp, VolumeMode, VolumeRecord, VolumeStatus,
};
