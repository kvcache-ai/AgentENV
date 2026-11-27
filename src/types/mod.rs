mod extra_drive;
mod id;
mod resources;
mod runtime_artifacts;

pub use agentenv_domain::{
    ImageConfigs, ImageName, ImageTag, SandboxResources, SnapshotAlias, SnapshotId, VolumeMode,
    DEFAULT_IMAGE_TAG,
};
pub use extra_drive::{
    normalize_mount_path, normalize_mount_path_for_drive, validate_drive_id, validate_mount_path,
    validate_sub_path, ExtraDrive, ExtraDriveKind,
};
pub(crate) use extra_drive::{ROOTFS_DRIVE_ID, USER_ROOTFS_DRIVE_ID, VOLUME_DRIVE_SLOT_PREFIX};
pub use id::SandboxId;
pub use resources::bytes_to_mib_ceil;
pub use runtime_artifacts::RuntimeArtifactSet;

/// Custom extension params: an opaque JSON object interpreted only by the
/// custom extension. `None` and an empty map are equivalent (empty params).
pub(crate) use agentenv_domain::CustomExtensionParams;

pub(crate) fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
