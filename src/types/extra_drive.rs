use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_EXTRA_DRIVE_MOUNT_ROOT: &str = "/mnt";
pub(crate) const ROOTFS_DRIVE_ID: &str = "rootfs";
pub(crate) const USER_ROOTFS_DRIVE_ID: &str = "user_rootfs";
pub(crate) const VOLUME_DRIVE_SLOT_PREFIX: &str = "agentenv_volume_slot_";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExtraDrive {
    Overlaybd {
        drive_id: String,
        image_config_path: PathBuf,
        read_only: bool,
        #[serde(default)]
        mount_path: PathBuf,
        /// Phase-specific OverlayBD virtual size in bytes.
        ///
        /// During a fresh launch this carries the optional target size requested
        /// by the API (`attachedDrives[].diskSizeMB`). During snapshot/resume it
        /// carries the known actual block-device size recorded in snapshot
        /// metadata. When this is `None`, the ublk daemon resolves the source
        /// image size while materializing the runtime device.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        virtual_size: Option<u64>,
        /// Optional sub-path inside the drive root to bind-mount onto
        /// `mount_path`. Behaves like Kubernetes `subPath` / a Docker volume
        /// sub-path. Stored as a relative path.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sub_path: Option<PathBuf>,
        /// Optional persistent destination for a volume snapshot.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        snapshot_output_dir: Option<PathBuf>,
        #[serde(default, rename = "volume", with = "extra_drive_kind_serde")]
        kind: ExtraDriveKind,
    },
}

/// How a drive is attached to Firecracker and whether captures commit it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExtraDriveKind {
    /// Attached under its own drive ID and committed to snapshot manifests.
    #[default]
    Physical,
    /// Added to a snapshot launch through a reserved slot. Captures commit it
    /// to the manifest, and it keeps its slot in later launches.
    LaunchSlot,
    /// Persistent volume: omitted from committed attached-drive manifests and
    /// rebound through reserved Firecracker slots.
    Volume,
}

/// Physical drives and volumes keep the legacy boolean `volume` field, so
/// persisted configs stay readable in both directions.
mod extra_drive_kind_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::ExtraDriveKind;

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Legacy(bool),
        Named(Named),
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Named {
        LaunchSlot,
    }

    pub(super) fn serialize<S: Serializer>(
        kind: &ExtraDriveKind,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match kind {
            ExtraDriveKind::Physical => Repr::Legacy(false),
            ExtraDriveKind::LaunchSlot => Repr::Named(Named::LaunchSlot),
            ExtraDriveKind::Volume => Repr::Legacy(true),
        }
        .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ExtraDriveKind, D::Error> {
        Ok(match Repr::deserialize(deserializer)? {
            Repr::Legacy(false) => ExtraDriveKind::Physical,
            Repr::Named(Named::LaunchSlot) => ExtraDriveKind::LaunchSlot,
            Repr::Legacy(true) => ExtraDriveKind::Volume,
        })
    }
}

impl ExtraDrive {
    pub fn try_new_overlaybd(
        drive_id: impl Into<String>,
        image_config_path: impl Into<PathBuf>,
        read_only: bool,
    ) -> Result<Self> {
        let drive_id = drive_id.into();
        let mount_path = Self::default_mount_path(&drive_id);
        Self::try_new_overlaybd_with_mount_path(
            drive_id,
            image_config_path,
            read_only,
            mount_path,
            None::<PathBuf>,
        )
    }

    pub fn try_new_overlaybd_with_mount_path(
        drive_id: impl Into<String>,
        image_config_path: impl Into<PathBuf>,
        read_only: bool,
        mount_path: impl Into<PathBuf>,
        sub_path: Option<impl Into<PathBuf>>,
    ) -> Result<Self> {
        let drive_id = drive_id.into();
        validate_drive_id(&drive_id)?;
        let mount_path = normalize_mount_path_for_drive(&drive_id, mount_path.into())?;
        let sub_path = sub_path.map(validate_sub_path).transpose()?;
        Ok(Self::Overlaybd {
            drive_id,
            image_config_path: image_config_path.into(),
            read_only,
            mount_path,
            virtual_size: None,
            sub_path,
            snapshot_output_dir: None,
            kind: ExtraDriveKind::Physical,
        })
    }

    pub fn default_mount_path(drive_id: &str) -> PathBuf {
        PathBuf::from(DEFAULT_EXTRA_DRIVE_MOUNT_ROOT).join(drive_id)
    }

    pub fn drive_id(&self) -> &str {
        match self {
            Self::Overlaybd { drive_id, .. } => drive_id,
        }
    }

    pub fn read_only(&self) -> bool {
        match self {
            Self::Overlaybd { read_only, .. } => *read_only,
        }
    }

    pub(crate) fn image_config_path(&self) -> &Path {
        match self {
            Self::Overlaybd {
                image_config_path, ..
            } => image_config_path,
        }
    }

    pub fn mount_path(&self) -> &Path {
        match self {
            Self::Overlaybd { mount_path, .. } => mount_path,
        }
    }

    pub fn sub_path(&self) -> Option<&Path> {
        match self {
            Self::Overlaybd { sub_path, .. } => sub_path.as_deref(),
        }
    }

    pub(crate) fn virtual_size(&self) -> Option<u64> {
        match self {
            Self::Overlaybd { virtual_size, .. } => *virtual_size,
        }
    }

    pub(crate) fn runtime_dir(&self, sandbox_work_dir: &Path) -> PathBuf {
        sandbox_work_dir.join(format!("extra-drive-runtime-{}", self.drive_id()))
    }

    pub(crate) fn attachment_symlink_name(&self) -> String {
        format!("extra-drive-{}", self.drive_id())
    }

    pub(crate) fn snapshot_output_dir(&self) -> Option<&Path> {
        match self {
            Self::Overlaybd {
                snapshot_output_dir,
                ..
            } => snapshot_output_dir.as_deref(),
        }
    }

    pub fn kind(&self) -> ExtraDriveKind {
        match self {
            Self::Overlaybd { kind, .. } => *kind,
        }
    }

    pub(crate) fn is_volume(&self) -> bool {
        self.kind() == ExtraDriveKind::Volume
    }

    /// Whether the drive is bound to a reserved Firecracker slot.
    pub(crate) fn uses_reserved_slot(&self) -> bool {
        self.kind() != ExtraDriveKind::Physical
    }

    pub fn with_kind(&self, kind: ExtraDriveKind) -> Self {
        let mut drive = self.clone();
        match &mut drive {
            Self::Overlaybd { kind: current, .. } => *current = kind,
        }
        drive
    }

    pub(crate) fn with_volume_snapshot_output_dir(&self, output_dir: Option<PathBuf>) -> Self {
        match self {
            Self::Overlaybd {
                drive_id,
                image_config_path,
                read_only,
                mount_path,
                virtual_size,
                sub_path,
                ..
            } => Self::Overlaybd {
                drive_id: drive_id.clone(),
                image_config_path: image_config_path.clone(),
                read_only: *read_only,
                mount_path: mount_path.clone(),
                virtual_size: *virtual_size,
                sub_path: sub_path.clone(),
                snapshot_output_dir: output_dir,
                kind: ExtraDriveKind::Volume,
            },
        }
    }

    pub(crate) fn with_image_config_path(mut self, image_config_path: PathBuf) -> Self {
        let Self::Overlaybd {
            image_config_path: path,
            ..
        } = &mut self;
        *path = image_config_path;
        self
    }

    pub(crate) fn try_with_virtual_size(mut self, virtual_size: u64) -> Result<Self> {
        anyhow::ensure!(
            virtual_size > 0,
            "extra drive virtual size must be non-zero"
        );
        let Self::Overlaybd {
            virtual_size: size, ..
        } = &mut self;
        *size = Some(virtual_size);
        Ok(self)
    }
}

pub fn validate_drive_id(drive_id: &str) -> Result<()> {
    if drive_id.trim().is_empty() {
        anyhow::bail!("attached drive driveID must not be empty");
    }
    if !drive_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        anyhow::bail!(
            "attached drive driveID must contain only ASCII letters, numbers, and underscores: {drive_id}"
        );
    }
    if matches!(drive_id, ROOTFS_DRIVE_ID | USER_ROOTFS_DRIVE_ID)
        || drive_id.starts_with(VOLUME_DRIVE_SLOT_PREFIX)
    {
        anyhow::bail!("attached drive driveID is reserved: {drive_id}");
    }
    Ok(())
}

pub fn validate_mount_path(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        anyhow::bail!(
            "attached drive mountPath must be absolute: {}",
            path.display()
        );
    }
    if path == Path::new("/") {
        anyhow::bail!("attached drive mountPath must not be /");
    }
    let raw = path
        .to_str()
        .context("attached drive mountPath must be valid UTF-8")?;
    if raw.chars().any(char::is_whitespace) || raw.contains(',') || raw.contains(':') {
        anyhow::bail!(
            "attached drive mountPath must not contain whitespace, commas, or colons: {}",
            path.display()
        );
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        anyhow::bail!(
            "attached drive mountPath must not contain '..': {}",
            path.display()
        );
    }
    const RESERVED: &[&str] = &[
        "/proc",
        "/sys",
        "/dev",
        "/run",
        "/agentenv",
        "/opt/agentenv",
    ];
    // /tmp is intentionally not reserved: it belongs to the guest filesystem,
    // and replacing it does not hide AgentENV control-plane files.
    for reserved in RESERVED {
        let reserved_path = Path::new(reserved);
        // Reject both descendants of reserved paths and ancestors that would
        // shadow reserved guest/control-plane paths, such as /p for /proc.
        if path == reserved_path
            || path.starts_with(reserved_path)
            || reserved_path.starts_with(path)
        {
            anyhow::bail!(
                "attached drive mountPath conflicts with reserved path {}: {}",
                reserved,
                path.display()
            );
        }
    }
    Ok(())
}

pub fn validate_sub_path(sub_path: impl Into<PathBuf>) -> Result<PathBuf> {
    let sub_path = sub_path.into();
    if sub_path.as_os_str().is_empty() {
        anyhow::bail!("attached drive subPath must not be empty");
    }
    if sub_path.is_absolute() {
        anyhow::bail!(
            "attached drive subPath must be a relative path: {}",
            sub_path.display()
        );
    }
    let raw = sub_path.to_string_lossy();
    if raw.chars().any(char::is_whitespace) || raw.contains(',') || raw.contains(':') {
        anyhow::bail!(
            "attached drive subPath must not contain whitespace, commas, or colons: {}",
            sub_path.display()
        );
    }
    if sub_path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        anyhow::bail!(
            "attached drive subPath must not contain '..': {}",
            sub_path.display()
        );
    }
    Ok(sub_path)
}

pub fn normalize_mount_path_for_drive(drive_id: &str, mount_path: PathBuf) -> Result<PathBuf> {
    let mount_path = if mount_path.as_os_str().is_empty() {
        ExtraDrive::default_mount_path(drive_id)
    } else {
        mount_path
    };
    normalize_mount_path(mount_path)
}

pub fn normalize_mount_path(mount_path: PathBuf) -> Result<PathBuf> {
    validate_mount_path(&mount_path)?;
    let mut normalized = PathBuf::from("/");
    for component in mount_path.components() {
        if let std::path::Component::Normal(value) = component {
            normalized.push(value);
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn overlaybd_drive_defaults_mount_path_from_drive_id() {
        let drive = ExtraDrive::try_new_overlaybd("data", "/tmp/image.json", true)
            .expect("drive should parse");

        assert_eq!(drive.mount_path(), Path::new("/mnt/data"));
    }

    #[test]
    fn overlaybd_drive_rejects_internal_drive_id() {
        let err = ExtraDrive::try_new_overlaybd(USER_ROOTFS_DRIVE_ID, "/tmp/image.json", true)
            .expect_err("internal drive id should fail");

        assert!(err.to_string().contains("reserved"));

        let err = ExtraDrive::try_new_overlaybd(
            format!("{VOLUME_DRIVE_SLOT_PREFIX}0"),
            "/tmp/image.json",
            true,
        )
        .expect_err("reserved volume slot id should fail");
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn read_only_volume_is_explicitly_marked() {
        let drive = ExtraDrive::try_new_overlaybd("data", "/tmp/image.json", true)
            .expect("drive should parse")
            .with_volume_snapshot_output_dir(None);

        assert!(drive.is_volume());
        assert!(drive.snapshot_output_dir().is_none());
    }

    #[test]
    fn drive_kind_keeps_legacy_volume_field() {
        let drive = ExtraDrive::try_new_overlaybd("data", "/tmp/image.json", false)
            .expect("drive should parse");
        for (kind, encoded) in [
            (ExtraDriveKind::Physical, serde_json::json!(false)),
            (ExtraDriveKind::LaunchSlot, serde_json::json!("launch_slot")),
            (ExtraDriveKind::Volume, serde_json::json!(true)),
        ] {
            let drive = drive.with_kind(kind);
            let value = serde_json::to_value(&drive).expect("drive should serialize");
            assert_eq!(value["Overlaybd"]["volume"], encoded);
            let decoded: ExtraDrive =
                serde_json::from_value(value).expect("drive should deserialize");
            assert_eq!(decoded, drive);
        }

        let mut legacy = serde_json::to_value(&drive).expect("drive should serialize");
        legacy["Overlaybd"]
            .as_object_mut()
            .expect("drive fields")
            .remove("volume");
        let decoded: ExtraDrive =
            serde_json::from_value(legacy).expect("drive without kind should deserialize");
        assert_eq!(decoded.kind(), ExtraDriveKind::Physical);
    }

    #[test]
    fn overlaybd_drive_rejects_firecracker_incompatible_drive_id() {
        for drive_id in ["resume-data", "resume/data", "resume data", "résumé"] {
            let err = ExtraDrive::try_new_overlaybd(drive_id, "/tmp/image.json", true)
                .expect_err("Firecracker-incompatible drive id should fail");

            assert!(err.to_string().contains("ASCII letters"));
        }
    }

    #[test]
    fn overlaybd_drive_rejects_invalid_mount_path() {
        let err = ExtraDrive::try_new_overlaybd_with_mount_path(
            "data",
            "/tmp/image.json",
            true,
            "/proc/data",
            None::<PathBuf>,
        )
        .expect_err("reserved mount path should fail");

        assert!(err.to_string().contains("reserved path"));
    }

    #[test]
    fn overlaybd_drive_rejects_reserved_path_ancestor() {
        let err = ExtraDrive::try_new_overlaybd_with_mount_path(
            "data",
            "/tmp/image.json",
            true,
            "/opt",
            None::<PathBuf>,
        )
        .expect_err("mounting over /opt should fail");

        assert!(err.to_string().contains("reserved path /opt/agentenv"));
    }

    #[test]
    fn mount_paths_are_normalized_for_overlap_checks() {
        assert_eq!(
            normalize_mount_path(PathBuf::from("/workspace//data")).expect("path should normalize"),
            PathBuf::from("/workspace/data")
        );
        assert!(normalize_mount_path(PathBuf::from("/workspace/data/../logs")).is_err());
    }

    #[test]
    fn mount_path_rejects_non_utf8() {
        use std::os::unix::ffi::OsStringExt;
        let mount = PathBuf::from(std::ffi::OsString::from_vec(b"/data/\xff".to_vec()));
        assert!(validate_mount_path(&mount).is_err());
    }

    #[test]
    fn overlaybd_drive_rejects_whitespace_mount_path() {
        for mount_path in [
            "/workspace/data set",
            "/workspace/data\tset",
            "/workspace/data\nset",
        ] {
            let err = ExtraDrive::try_new_overlaybd_with_mount_path(
                "data",
                "/tmp/image.json",
                true,
                mount_path,
                None::<PathBuf>,
            )
            .expect_err("whitespace mount path should fail");

            assert!(err.to_string().contains("whitespace"));
        }
    }

    #[test]
    fn overlaybd_drive_rejects_zero_runtime_virtual_size() {
        let drive = ExtraDrive::try_new_overlaybd("data", "/tmp/image.json", true)
            .expect("drive should parse");
        let err = drive
            .try_with_virtual_size(0)
            .expect_err("runtime virtual size should be non-zero");

        assert!(err.to_string().contains("virtual size must be non-zero"));
    }
}
