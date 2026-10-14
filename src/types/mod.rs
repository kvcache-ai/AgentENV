mod extra_drive;
mod id;
mod image_configs;
mod resources;
mod runtime_artifacts;
mod snapshot_id;
mod volume_mode;

pub use extra_drive::{
    normalize_mount_path, normalize_mount_path_for_drive, validate_drive_id, validate_mount_path,
    validate_sub_path, ExtraDrive,
};
pub(crate) use extra_drive::{ROOTFS_DRIVE_ID, USER_ROOTFS_DRIVE_ID, VOLUME_DRIVE_SLOT_PREFIX};
pub use id::SandboxId;
pub use image_configs::ImageConfigs;
pub use resources::{bytes_to_mib_ceil, SandboxResources};
pub use runtime_artifacts::RuntimeArtifactSet;
pub use snapshot_id::{SnapshotAlias, SnapshotId};
pub use volume_mode::VolumeMode;

/// Custom extension params: an opaque JSON object interpreted only by the
/// custom extension. `None` and an empty map are equivalent (empty params).
pub(crate) type CustomExtensionParams = serde_json::Map<String, serde_json::Value>;
