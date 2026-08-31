use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::sandbox::ExtraDrive;

pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Manifest describing the on-disk layout of a Firecracker snapshot.
///
/// This is intentionally decoupled from in-memory snapshot representations.
/// Snapshot-layer retrieve artifacts based on the manifest during snapshot
/// publication, and reconstruct the manifest with hydrated paths during snapshot resolution.
///
/// All paths in the manifest should be absolute.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerSnapshotManifest {
    /// Schema/version marker for persisted manifest format.
    pub version: u32,
    pub vm_state: FirecrackerVmStateArtifacts,
    pub memory: FirecrackerMemoryArtifacts,
    pub rootfs: FirecrackerRootfsArtifacts,
    pub attached_drives: Vec<FirecrackerAttachedDriveArtifacts>,
    /// Number of reserved virtio-block slots available for launch-time volumes.
    #[serde(default)]
    pub volume_drive_slots: usize,
    /// Number of attached drives represented by IDs in the Firecracker snapshot.
    /// Launch-time volumes use the reserved slots that follow these drives.
    #[serde(default)]
    pub physical_extra_drive_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerVmStateArtifacts {
    #[serde(skip)]
    pub path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerMemoryArtifacts {
    #[serde(skip)]
    pub image_config_path: PathBuf,
    pub virtual_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerRootfsArtifacts {
    #[serde(skip)]
    pub image_config_path: PathBuf,
    pub virtual_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FirecrackerAttachedDriveArtifacts {
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

impl FirecrackerSnapshotManifest {
    pub fn new(
        vm_state_path: impl Into<PathBuf>,
        mem_image_config_path: impl Into<PathBuf>,
        mem_virtual_size: u64,
        rootfs_image_config_path: impl Into<PathBuf>,
        rootfs_virtual_size: u64,
        attached_drives: &[ExtraDrive],
    ) -> Result<Self> {
        Self {
            version: MANIFEST_FORMAT_VERSION,
            vm_state: FirecrackerVmStateArtifacts {
                path: vm_state_path.into(),
            },
            memory: FirecrackerMemoryArtifacts {
                image_config_path: mem_image_config_path.into(),
                virtual_size: mem_virtual_size,
            },
            rootfs: FirecrackerRootfsArtifacts {
                image_config_path: rootfs_image_config_path.into(),
                virtual_size: rootfs_virtual_size,
            },
            attached_drives: Vec::new(),
            volume_drive_slots: 0,
            physical_extra_drive_count: attached_drives.len(),
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
                Ok(FirecrackerAttachedDriveArtifacts {
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
impl FirecrackerSnapshotManifest {
    pub(crate) fn for_test(
        rootfs_virtual_size: u64,
        attached_drives: &[ExtraDrive],
    ) -> FirecrackerSnapshotManifest {
        let mut manifest = FirecrackerSnapshotManifest::new(
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
        let err = serde_json::from_value::<FirecrackerAttachedDriveArtifacts>(serde_json::json!({
            "driveId": "data",
            "readOnly": true,
            "mountPath": "/mnt/data"
        }))
        .expect_err("attached drive artifact should require virtualSize");

        assert!(err.to_string().contains("virtualSize"));
    }

    #[test]
    fn attached_drive_virtual_size_is_serialized_and_mapped_to_runtime_input() {
        let known = FirecrackerAttachedDriveArtifacts {
            drive_id: "data".to_string(),
            read_only: true,
            mount_path: PathBuf::from("/mnt/data"),
            sub_path: None,
            virtual_size: 4096,
            image_config_path: PathBuf::from("drives/data/image.json"),
        };

        let known_json = serde_json::to_value(&known).unwrap();
        assert_eq!(known_json["virtualSize"], serde_json::json!(4096));

        let manifest = FirecrackerSnapshotManifest {
            version: MANIFEST_FORMAT_VERSION,
            vm_state: FirecrackerVmStateArtifacts {
                path: PathBuf::from("vm_state.bin"),
            },
            memory: FirecrackerMemoryArtifacts {
                image_config_path: PathBuf::from("mem_image.json"),
                virtual_size: 4096,
            },
            rootfs: FirecrackerRootfsArtifacts {
                image_config_path: PathBuf::from("rootfs/image.json"),
                virtual_size: 4096,
            },
            attached_drives: vec![known],
            volume_drive_slots: 0,
            physical_extra_drive_count: 1,
        };

        let drives = manifest.extra_drives();
        assert_eq!(drives[0].virtual_size(), Some(4096));
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

        let err = FirecrackerSnapshotManifest::new(
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
        let manifest = FirecrackerSnapshotManifest::new(
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
