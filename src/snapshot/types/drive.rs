use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::snapshot::OverlaybdLayerRef;
use crate::sandbox::ExtraDrive;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Attached-drive state resolved into local runtime inputs for the current node.
pub enum ResolvedAttachedDrive {
    /// Node-local OverlayBD drive runtime input.
    ///
    /// These absolute paths are valid only on the current node after repository
    /// resolution. They must not be persisted directly in committed snapshot
    /// metadata.
    Overlaybd {
        drive_id: String,
        image_config_path: PathBuf,
        read_only: bool,
        virtual_size: u64,
        #[serde(default)]
        mount_path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sub_path: Option<PathBuf>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommittedAttachedDrive {
    Overlaybd {
        drive_id: String,
        layers: Vec<OverlaybdLayerRef>,
        read_only: bool,
        virtual_size: u64,
        #[serde(default)]
        mount_path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sub_path: Option<PathBuf>,
    },
}

impl ResolvedAttachedDrive {
    pub(crate) fn to_extra_drive(&self) -> ExtraDrive {
        match self {
            Self::Overlaybd {
                drive_id,
                image_config_path,
                read_only,
                virtual_size,
                mount_path,
                sub_path,
            } => ExtraDrive::Overlaybd {
                drive_id: drive_id.clone(),
                image_config_path: image_config_path.clone(),
                read_only: *read_only,
                mount_path: mount_path.clone(),
                virtual_size: Some(*virtual_size),
                sub_path: sub_path.clone(),
                snapshot_output_dir: None,
            },
        }
    }

    /// Returns the image config path.
    pub fn image_config_path(&self) -> &Path {
        match self {
            Self::Overlaybd {
                image_config_path, ..
            } => image_config_path,
        }
    }

    /// Returns the drive id.
    pub fn drive_id(&self) -> &str {
        match self {
            Self::Overlaybd { drive_id, .. } => drive_id,
        }
    }

    /// Returns whether the drive should be attached read-only.
    pub fn read_only(&self) -> bool {
        match self {
            Self::Overlaybd { read_only, .. } => *read_only,
        }
    }

    pub fn virtual_size(&self) -> u64 {
        match self {
            Self::Overlaybd { virtual_size, .. } => *virtual_size,
        }
    }

    pub fn mount_path(&self) -> PathBuf {
        match self {
            Self::Overlaybd { mount_path, .. } => mount_path.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{CommittedAttachedDrive, ResolvedAttachedDrive};
    use crate::sandbox::ExtraDrive;

    fn sample_resolved() -> ResolvedAttachedDrive {
        ResolvedAttachedDrive::Overlaybd {
            drive_id: "data".to_string(),
            image_config_path: PathBuf::from("/tmp/image.json"),
            read_only: false,
            virtual_size: 4096,
            mount_path: ExtraDrive::default_mount_path("data"),
            sub_path: None,
        }
    }

    #[test]
    fn to_extra_drive_preserves_fields() {
        let extra_drive = sample_resolved().to_extra_drive();

        assert_eq!(extra_drive.drive_id(), "data");
        assert!(!extra_drive.read_only());
        assert_eq!(extra_drive.mount_path(), Path::new("/mnt/data"));
        assert_eq!(extra_drive.virtual_size(), Some(4096));
        assert_eq!(extra_drive.sub_path(), None);
    }

    #[test]
    fn to_extra_drive_propagates_sub_path() {
        let mut drive = sample_resolved();
        let ResolvedAttachedDrive::Overlaybd { sub_path, .. } = &mut drive;
        *sub_path = Some(PathBuf::from("workspace/data"));

        let extra_drive = drive.to_extra_drive();
        assert_eq!(extra_drive.sub_path(), Some(Path::new("workspace/data")));
    }

    #[test]
    fn attached_drive_virtual_size_is_required() {
        let resolved = serde_json::from_value::<ResolvedAttachedDrive>(serde_json::json!({
            "Overlaybd": {
                "drive_id": "data",
                "image_config_path": "/tmp/image.json",
                "read_only": true
            }
        }))
        .expect_err("resolved attached drive should require virtual_size");
        assert!(resolved.to_string().contains("virtual_size"));

        let committed = serde_json::from_value::<CommittedAttachedDrive>(serde_json::json!({
            "Overlaybd": {
                "drive_id": "data",
                "layers": [],
                "read_only": true
            }
        }))
        .expect_err("committed attached drive should require virtual_size");
        assert!(committed.to_string().contains("virtual_size"));
    }

    #[test]
    fn attached_drive_virtual_size_is_serialized() {
        let resolved_json = serde_json::to_value(sample_resolved()).unwrap();
        assert_eq!(resolved_json["Overlaybd"]["virtual_size"], 4096);

        let committed = CommittedAttachedDrive::Overlaybd {
            drive_id: "data".to_string(),
            layers: Vec::new(),
            read_only: true,
            virtual_size: 4096,
            mount_path: ExtraDrive::default_mount_path("data"),
            sub_path: None,
        };
        let committed_json = serde_json::to_value(&committed).unwrap();
        assert_eq!(committed_json["Overlaybd"]["virtual_size"], 4096);
    }
}
