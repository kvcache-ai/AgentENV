use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use super::ExtraDrive;

pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Name `backend` takes when a record predates the field.
pub const FIRECRACKER_BACKEND: &str = "firecracker";

/// Names a restore knows how to reach a VMM by.
const KNOWN_BACKENDS: &[&str] = &[FIRECRACKER_BACKEND];

fn default_backend() -> String {
    FIRECRACKER_BACKEND.to_string()
}

/// Manifest describing the on-disk layout of a captured sandbox.
///
/// The shape is the same whichever VMM took the capture: one VM-state file,
/// an overlaybd image for the memory and one for the root filesystem, and a
/// descriptor per attached drive. `backend` names the VMM, so a factory can
/// refuse a snapshot it cannot restore.
///
/// This is intentionally decoupled from in-memory snapshot representations.
/// Snapshot-layer retrieve artifacts based on the manifest during snapshot
/// publication, and reconstruct the manifest with hydrated paths during snapshot resolution.
///
/// All paths in the manifest should be absolute.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxSnapshotManifest {
    /// Schema/version marker for persisted manifest format.
    pub version: u32,
    /// The VMM which took the capture. Records written before the field
    /// existed carry Firecracker captures, so that is what a missing value
    /// means.
    #[serde(default = "default_backend")]
    pub backend: String,
    pub vm_state: SnapshotVmStateArtifacts,
    pub memory: SnapshotMemoryArtifacts,
    pub rootfs: SnapshotRootfsArtifacts,
    pub attached_drives: Vec<SnapshotAttachedDriveArtifacts>,
    /// Number of reserved virtio-block slots available for launch-time volumes.
    #[serde(default)]
    pub volume_drive_slots: usize,
    /// Number of attached drives represented by IDs in the Firecracker snapshot.
    /// Launch-time volumes use the reserved slots that follow these drives.
    #[serde(default)]
    pub physical_extra_drive_count: usize,
    /// Runtime-only startup pack reference resolved from the committed
    /// record when consumption is enabled. It is absent for older snapshots,
    /// disabled consumption, and best-effort manifest resolution failures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_startup_pack: Option<crate::snapshot::ResolvedStartupPack>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotVmStateArtifacts {
    #[serde(skip)]
    pub path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotMemoryArtifacts {
    #[serde(skip)]
    pub image_config_path: PathBuf,
    pub virtual_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotRootfsArtifacts {
    #[serde(skip)]
    pub image_config_path: PathBuf,
    pub virtual_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotAttachedDriveArtifacts {
    pub drive_id: String,
    pub read_only: bool,
    #[serde(default)]
    pub mount_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<PathBuf>,
    pub virtual_size: u64,
    #[serde(skip)]
    pub image_config_path: PathBuf,
}

impl SandboxSnapshotManifest {
    /// Describe a capture `backend` wrote. Pass the backend's own name, one
    /// of the `*_BACKEND` constants, so a restore reaches the VMM which took
    /// it. A name no restore knows is refused here, before it reaches a
    /// published record.
    pub fn new(
        backend: &str,
        vm_state_path: impl Into<PathBuf>,
        mem_image_config_path: impl Into<PathBuf>,
        mem_virtual_size: u64,
        rootfs_image_config_path: impl Into<PathBuf>,
        rootfs_virtual_size: u64,
        attached_drives: &[ExtraDrive],
    ) -> Result<Self> {
        if !KNOWN_BACKENDS.contains(&backend) {
            bail!("backend '{backend}' names no VMM a restore can reach");
        }
        Self {
            version: MANIFEST_FORMAT_VERSION,
            backend: backend.to_string(),
            vm_state: SnapshotVmStateArtifacts {
                path: vm_state_path.into(),
            },
            memory: SnapshotMemoryArtifacts {
                image_config_path: mem_image_config_path.into(),
                virtual_size: mem_virtual_size,
            },
            rootfs: SnapshotRootfsArtifacts {
                image_config_path: rootfs_image_config_path.into(),
                virtual_size: rootfs_virtual_size,
            },
            attached_drives: Vec::new(),
            volume_drive_slots: 0,
            physical_extra_drive_count: attached_drives.len(),
            memory_startup_pack: None,
        }
        .with_extra_drives(attached_drives)
    }

    pub fn extra_drives(&self) -> Vec<ExtraDrive> {
        self.attached_drives
            .iter()
            .map(|drive| ExtraDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                image_config_path: drive.image_config_path.clone(),
                read_only: drive.read_only,
                virtual_size: Some(drive.virtual_size),
                mount_path: crate::sandbox::normalize_mount_path_for_drive(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                )
                .unwrap_or_else(|_| ExtraDrive::default_mount_path(&drive.drive_id)),
                sub_path: drive.sub_path.clone(),
                snapshot_output_dir: None,
                volume: false,
            })
            .collect()
    }

    pub fn with_extra_drives(&self, extra_drives: &[ExtraDrive]) -> Result<Self> {
        let mut new = self.clone();
        new.attached_drives = extra_drives
            .iter()
            .map(|drive| {
                let virtual_size = drive.virtual_size().ok_or_else(|| {
                    anyhow::anyhow!(
                        "snapshot attached drive '{}' virtual size must be known",
                        drive.drive_id()
                    )
                })?;
                if virtual_size == 0 {
                    bail!(
                        "snapshot attached drive '{}' virtual size must be non-zero",
                        drive.drive_id()
                    );
                }
                Ok(SnapshotAttachedDriveArtifacts {
                    drive_id: drive.drive_id().to_string(),
                    read_only: drive.read_only(),
                    mount_path: drive.mount_path().to_path_buf(),
                    sub_path: drive.sub_path().map(Path::to_path_buf),
                    virtual_size,
                    image_config_path: drive.image_config_path().to_path_buf(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(new)
    }
}

#[cfg(test)]
#[doc(hidden)]
impl SandboxSnapshotManifest {
    pub(crate) fn for_test(
        rootfs_virtual_size: u64,
        attached_drives: &[ExtraDrive],
    ) -> SandboxSnapshotManifest {
        let mut manifest = SandboxSnapshotManifest::new(
            FIRECRACKER_BACKEND,
            "vm_state.bin",
            "mem_image.json",
            0,
            "rootfs/image.json",
            rootfs_virtual_size,
            attached_drives,
        )
        .expect("test snapshot attached drive virtual size must be known");

        for drive in &mut manifest.attached_drives {
            drive.image_config_path = PathBuf::from("drives")
                .join(&drive.drive_id)
                .join("image.json");
        }
        manifest.volume_drive_slots = 4;

        manifest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attached_drive_virtual_size_is_required() {
        let err = serde_json::from_value::<SnapshotAttachedDriveArtifacts>(serde_json::json!({
            "driveId": "data",
            "readOnly": true,
            "mountPath": "/mnt/data"
        }))
        .expect_err("attached drive artifact should require virtualSize");

        assert!(err.to_string().contains("virtualSize"));
    }

    #[test]
    fn attached_drive_virtual_size_is_serialized_and_mapped_to_runtime_input() {
        let known = SnapshotAttachedDriveArtifacts {
            drive_id: "data".to_string(),
            read_only: true,
            mount_path: PathBuf::from("/mnt/data"),
            sub_path: None,
            virtual_size: 4096,
            image_config_path: PathBuf::from("drives/data/image.json"),
        };

        let known_json = serde_json::to_value(&known).unwrap();
        assert_eq!(known_json["virtualSize"], serde_json::json!(4096));

        let manifest = SandboxSnapshotManifest {
            version: MANIFEST_FORMAT_VERSION,
            backend: default_backend(),
            vm_state: SnapshotVmStateArtifacts {
                path: PathBuf::from("vm_state.bin"),
            },
            memory: SnapshotMemoryArtifacts {
                image_config_path: PathBuf::from("mem_image.json"),
                virtual_size: 4096,
            },
            rootfs: SnapshotRootfsArtifacts {
                image_config_path: PathBuf::from("rootfs/image.json"),
                virtual_size: 4096,
            },
            attached_drives: vec![known],
            volume_drive_slots: 0,
            physical_extra_drive_count: 1,
            memory_startup_pack: None,
        };

        let drives = manifest.extra_drives();
        assert_eq!(drives[0].virtual_size(), Some(4096));
    }

    #[test]
    fn manifest_without_memory_startup_pack_parses_as_none() {
        let json = serde_json::json!({
            "version": MANIFEST_FORMAT_VERSION,
            "vmState": {},
            "memory": { "virtualSize": 4096 },
            "rootfs": { "virtualSize": 4096 },
            "attachedDrives": [],
        });
        let manifest: SandboxSnapshotManifest =
            serde_json::from_value(json).expect("old manifest must parse");
        assert!(manifest.memory_startup_pack.is_none());
    }

    #[test]
    fn new_rejects_attached_drive_without_virtual_size() {
        let drive = ExtraDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: PathBuf::from("/tmp/data/image.json"),
            read_only: true,
            mount_path: ExtraDrive::default_mount_path("data"),
            virtual_size: None,
            sub_path: None,
            snapshot_output_dir: None,
            volume: false,
        };

        let err = SandboxSnapshotManifest::new(
            FIRECRACKER_BACKEND,
            "vm_state.bin",
            "mem_image.json",
            4096,
            "rootfs/image.json",
            4096,
            &[drive],
        )
        .expect_err("snapshot attached drive virtual size should be required");

        assert!(err.to_string().contains("virtual size must be known"));
    }

    #[test]
    fn with_extra_drives_rejects_zero_virtual_size() {
        let manifest = SandboxSnapshotManifest::new(
            FIRECRACKER_BACKEND,
            "vm_state.bin",
            "mem_image.json",
            4096,
            "rootfs/image.json",
            4096,
            &[],
        )
        .expect("empty attached drives should be valid");
        let drive = ExtraDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: PathBuf::from("/tmp/data/image.json"),
            read_only: true,
            mount_path: ExtraDrive::default_mount_path("data"),
            virtual_size: Some(0),
            sub_path: None,
            snapshot_output_dir: None,
            volume: false,
        };

        let err = manifest
            .with_extra_drives(&[drive])
            .expect_err("snapshot attached drive virtual size should be non-zero");

        assert!(err.to_string().contains("virtual size must be non-zero"));
    }
}
