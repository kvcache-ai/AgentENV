use crate::repository::SnapshotFile;

#[derive(Clone, Copy, Debug)]
/// Shared relative artifact names used by local and committed snapshot layouts.
pub struct SnapshotArtifactLayoutSpec {
    pub firecracker_manifest: &'static str,
    pub vm_state: &'static str,
    pub memory_dump: &'static str,
    pub memory_image_config: &'static str,
    pub rootfs_dir: &'static str,
    pub drives_dir: &'static str,
    pub drive_layers_dir: &'static str,
    pub rootfs_image_config: &'static str,
    pub overlaybd_image_config_file: &'static str,
}

pub const SNAPSHOT_ARTIFACT_LAYOUT: SnapshotArtifactLayoutSpec = SnapshotArtifactLayoutSpec {
    firecracker_manifest: SnapshotFile::FirecrackerManifest.name(),
    vm_state: SnapshotFile::VmState.name(),
    memory_dump: "mem.bin",
    memory_image_config: "mem_image.json",
    rootfs_dir: "rootfs",
    drives_dir: "drives",
    drive_layers_dir: "drives/layers",
    rootfs_image_config: "rootfs/image.json",
    overlaybd_image_config_file: "image.json",
};
