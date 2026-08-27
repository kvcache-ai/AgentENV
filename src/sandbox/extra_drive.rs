use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use overlaybd::config::UpperMode;
use serde::{Deserialize, Serialize};
use tracing::warn;
use uvm_ublk_daemon::CreateOverlaybdRuntimeDeviceRequest;

use crate::sandbox::ublk::{OverlaybdRuntimeHandle, UblkDeviceManager};

pub const DEFAULT_EXTRA_DRIVE_MOUNT_ROOT: &str = "/mnt";
pub(crate) const ROOTFS_DRIVE_ID: &str = "rootfs";
pub(crate) const USER_ROOTFS_DRIVE_ID: &str = "user_rootfs";

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
    },
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

    pub(crate) fn with_snapshot_output_dir(&self, output_dir: Option<PathBuf>) -> Self {
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
            },
        }
    }

    pub(crate) fn with_image_config_path(&self, image_config_path: PathBuf) -> Self {
        match self {
            Self::Overlaybd {
                drive_id,
                read_only,
                mount_path,
                virtual_size,
                sub_path,
                snapshot_output_dir,
                ..
            } => Self::Overlaybd {
                drive_id: drive_id.clone(),
                image_config_path,
                read_only: *read_only,
                mount_path: mount_path.clone(),
                virtual_size: *virtual_size,
                sub_path: sub_path.clone(),
                snapshot_output_dir: snapshot_output_dir.clone(),
            },
        }
    }

    pub(crate) fn try_with_virtual_size(&self, virtual_size: u64) -> Result<Self> {
        anyhow::ensure!(
            virtual_size > 0,
            "extra drive virtual size must be non-zero"
        );
        match self {
            Self::Overlaybd {
                drive_id,
                image_config_path,
                read_only,
                mount_path,
                snapshot_output_dir,
                sub_path,
                ..
            } => Ok(Self::Overlaybd {
                drive_id: drive_id.clone(),
                image_config_path: image_config_path.clone(),
                read_only: *read_only,
                mount_path: mount_path.clone(),
                virtual_size: Some(virtual_size),
                sub_path: sub_path.clone(),
                snapshot_output_dir: snapshot_output_dir.clone(),
            }),
        }
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
    if matches!(drive_id, ROOTFS_DRIVE_ID | USER_ROOTFS_DRIVE_ID) {
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
    let raw = path.to_string_lossy();
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
    validate_mount_path(&mount_path)?;
    Ok(mount_path)
}

#[derive(Clone, Debug)]
pub(crate) struct DriveMount {
    pub(crate) drive_id: String,
    pub(crate) attachment_path: PathBuf,
    pub(crate) read_only: bool,
}

pub(crate) struct PreparedDrives {
    mounts: Vec<DriveMount>,
    cleanup_paths: Vec<PathBuf>,
    runtimes: Vec<OverlaybdRuntimeHandle>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExtraDrivePrepareMode {
    Fresh { allow_shrink: bool },
    Resume,
}

impl ExtraDrivePrepareMode {
    fn device_sizes(self, drive: &ExtraDrive) -> (Option<u64>, Option<u64>) {
        match self {
            Self::Fresh { .. } => {
                // For fresh launches, virtual_size mirrors rootfs_virtual_size:
                // it is the optional API-requested target size. The source/base
                // size is intentionally left unknown so the daemon can read it.
                (drive.virtual_size(), None)
            }
            Self::Resume => {
                // For resume/snapshot-backed launches, virtual_size is the
                // known actual block-device size recorded in snapshot metadata.
                (drive.virtual_size(), drive.virtual_size())
            }
        }
    }

    fn allow_shrink(self) -> bool {
        match self {
            Self::Fresh { allow_shrink } => allow_shrink,
            Self::Resume => false,
        }
    }
}

impl PreparedDrives {
    pub(crate) fn into_parts(self) -> (Vec<DriveMount>, Vec<OverlaybdRuntimeHandle>) {
        (self.mounts, self.runtimes)
    }

    async fn cleanup(self) {
        for runtime in self.runtimes {
            if let Err(err) = UblkDeviceManager::global()
                .release_device(&runtime.device)
                .await
            {
                warn!(
                    error = %err,
                    "failed to delete prepared extra drive device during rollback"
                );
            }
        }

        for path in self.cleanup_paths {
            if let Err(err) = fs::remove_file(&path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    warn!(
                        path = %path.display(),
                        error = %err,
                        "failed to remove prepared extra drive attachment during rollback"
                    );
                }
            }
        }
    }
}

pub(crate) async fn prepare_extra_drives(
    extra_drives: &[ExtraDrive],
    global_config_path: &Path,
    sandbox_work_dir: &Path,
    runtime_upper_mode: UpperMode,
    mode: ExtraDrivePrepareMode,
) -> Result<PreparedDrives> {
    let mut mounts = Vec::with_capacity(extra_drives.len());
    let mut cleanup_paths = Vec::with_capacity(extra_drives.len());
    let mut runtimes = Vec::with_capacity(extra_drives.len());

    for drive in extra_drives {
        let result = async {
            let runtime_dir = drive.runtime_dir(sandbox_work_dir);
            let (requested_virtual_size, known_source_virtual_size) = mode.device_sizes(drive);
            let allow_shrink = mode.allow_shrink();
            let runtime_device = UblkDeviceManager::global()
                .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                    source_image_config: drive.image_config_path(),
                    global_config: global_config_path,
                    runtime_dir: &runtime_dir,
                    read_only: drive.read_only(),
                    runtime_upper_mode,
                    requested_virtual_size,
                    known_source_virtual_size,
                    allow_shrink,
                })
                .await
                .context("create overlaybd extra drive runtime device")?;
            let symlink_name = drive.attachment_symlink_name();
            let symlink_path = sandbox_work_dir.join(&symlink_name);
            let device_path = runtime_device.device.device_path().to_path_buf();
            let symlink_result = std::os::unix::fs::symlink(&device_path, &symlink_path)
                .with_context(|| {
                    format!(
                        "symlink extra drive {} -> {}",
                        symlink_path.display(),
                        device_path.display()
                    )
                });
            if let Err(err) = symlink_result {
                if let Err(release_err) = UblkDeviceManager::global()
                    .release_device(&runtime_device.device)
                    .await
                {
                    warn!(
                        error = %release_err,
                        "failed to release extra drive ublk device after symlink failure"
                    );
                }
                return Err(err);
            }
            Ok::<_, anyhow::Error>((
                runtime_device.device,
                runtime_device.image_config_path,
                runtime_device.actual_virtual_size,
                symlink_name,
                symlink_path,
            ))
        }
        .await;

        match result {
            Ok((
                device,
                runtime_image_config_path,
                actual_virtual_size,
                symlink_name,
                symlink_path,
            )) => {
                mounts.push(DriveMount {
                    drive_id: drive.drive_id().to_string(),
                    attachment_path: PathBuf::from(symlink_name),
                    read_only: drive.read_only(),
                });
                cleanup_paths.push(symlink_path);
                runtimes.push(OverlaybdRuntimeHandle {
                    device,
                    image_config_path: runtime_image_config_path,
                    actual_virtual_size,
                });
            }
            Err(err) => {
                let prepared = PreparedDrives {
                    mounts,
                    cleanup_paths,
                    runtimes,
                };
                prepared.cleanup().await;
                return Err(err);
            }
        }
    }

    Ok(PreparedDrives {
        mounts,
        cleanup_paths,
        runtimes,
    })
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

    #[test]
    fn extra_drive_prepare_mode_maps_virtual_size_by_launch_phase() {
        let drive_without_size = ExtraDrive::try_new_overlaybd("data", "/tmp/image.json", true)
            .expect("drive should parse");
        assert_eq!(
            ExtraDrivePrepareMode::Fresh {
                allow_shrink: false
            }
            .device_sizes(&drive_without_size),
            (None, None)
        );
        assert_eq!(
            ExtraDrivePrepareMode::Resume.device_sizes(&drive_without_size),
            (None, None)
        );

        let sized = drive_without_size
            .try_with_virtual_size(2 * 1024 * 1024 * 1024)
            .expect("virtual size should parse");

        assert_eq!(
            ExtraDrivePrepareMode::Fresh { allow_shrink: true }.device_sizes(&sized),
            (Some(2 * 1024 * 1024 * 1024), None)
        );
        assert_eq!(
            ExtraDrivePrepareMode::Resume.device_sizes(&sized),
            (Some(2 * 1024 * 1024 * 1024), Some(2 * 1024 * 1024 * 1024))
        );
    }
}
