use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use overlaybd::config::UpperMode;
use tracing::warn;
use uvm_ublk_daemon::CreateOverlaybdRuntimeDeviceRequest;

use crate::sandbox::ublk::OverlaybdConfig;
use crate::sandbox::ublk::{OverlaybdRuntimeHandle, UblkDeviceManager};
use crate::types::ExtraDrive;

/// Maximum extra disks in the Linux guest tools-drive layout (vdc through vdz).
pub(crate) const MAX_EXTRA_DRIVES: usize = (b'z' - b'c' + 1) as usize;

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

/// Prepares each drive with the mode `mode` returns for its index.
pub(crate) async fn prepare_extra_drives(
    extra_drives: &[ExtraDrive],
    global_config_path: &Path,
    sandbox_work_dir: &Path,
    runtime_upper_mode: UpperMode,
    mode: impl Fn(usize) -> ExtraDrivePrepareMode,
) -> Result<PreparedDrives> {
    let mut mounts = Vec::with_capacity(extra_drives.len());
    let mut cleanup_paths = Vec::with_capacity(extra_drives.len());
    let mut runtimes = Vec::with_capacity(extra_drives.len());

    for (index, drive) in extra_drives.iter().enumerate() {
        let mode = mode(index);
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

/// Overlaybd image config paths a sandbox opens (rootfs + extra drives).
/// For fresh launches these are source configs; for paused states they are
/// snapshot artifact configs.
/// (Memory snapshot layers are remote/repository-backed, never local-only, so
/// they are not included.)
pub(crate) fn rootfs_and_extra_drive_image_config_paths(
    rootfs: Option<&OverlaybdConfig>,
    extra_drives: &[ExtraDrive],
) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(rootfs) = rootfs {
        paths.push(rootfs.image_config_path.clone());
    }
    paths.extend(
        extra_drives
            .iter()
            .map(|drive| drive.image_config_path().to_path_buf()),
    );
    paths
}

/// Read-only drives in writable reserved slots, such as read-only volumes,
/// would have their superblock written by init's default mount. They are
/// mounted `ro,noload` through envd after boot instead, as on snapshot launches.
pub(crate) fn mounts_after_boot(drive: &ExtraDrive) -> bool {
    drive.uses_reserved_slot() && drive.read_only()
}

/// Build the `agentenv_drives=vdc:/mnt/data,vdd:/mnt/logs:sub/path` boot arg
/// for extra drives that init mounts. Returns `None` if there are none.
/// Drive letter mapping: vda = tools drive, vdb = user image, vdc = first extra drive.
/// Each entry is `vd<letter>:<mountPath>[:<subPath>]`, with optional
/// Kubernetes-style `subPath` semantics. API validation rejects `:` in both
/// `mountPath` and `subPath`, so the `:` separators are unambiguous.
pub(crate) fn build_drives_boot_arg(extra_drives: &[ExtraDrive]) -> Option<String> {
    assert!(
        extra_drives.len() <= MAX_EXTRA_DRIVES,
        "too many extra drives for guest naming: {} > {}",
        extra_drives.len(),
        MAX_EXTRA_DRIVES
    );
    let entries: Vec<String> = extra_drives
        .iter()
        .enumerate()
        .filter(|(_, drive)| !mounts_after_boot(drive))
        .map(|(i, drive)| {
            let dev_letter = (b'c' + i as u8) as char;
            let mut entry = format!("vd{}:{}", dev_letter, drive.mount_path().display());
            if let Some(sub_path) = drive.sub_path() {
                entry.push(':');
                entry.push_str(&sub_path.display().to_string());
            }
            entry
        })
        .collect();
    (!entries.is_empty()).then(|| format!("agentenv_drives={}", entries.join(",")))
}

/// Append a space-separated argument to an optional kernel command line.
pub(crate) fn append_boot_arg(boot_args: &mut Option<String>, arg: &str) {
    *boot_args = Some(match boot_args.take() {
        Some(existing) => format!("{existing} {arg}"),
        None => arg.to_owned(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

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

#[cfg(test)]
mod boot_tests {
    use super::*;
    #[test]
    fn build_drives_boot_arg_uses_custom_mount_path() -> Result<()> {
        let drives = vec![
            ExtraDrive::try_new_overlaybd_with_mount_path(
                "data",
                "/tmp/data-image.json",
                true,
                "/workspace/data",
                None::<std::path::PathBuf>,
            )?,
            ExtraDrive::try_new_overlaybd("logs", "/tmp/logs-image.json", true)?,
        ];

        assert_eq!(
            build_drives_boot_arg(&drives),
            Some("agentenv_drives=vdc:/workspace/data,vdd:/mnt/logs".to_string())
        );
        Ok(())
    }

    #[test]
    fn build_drives_boot_arg_includes_sub_path() -> Result<()> {
        let drives = vec![ExtraDrive::try_new_overlaybd_with_mount_path(
            "data",
            "/tmp/data-image.json",
            true,
            "/mnt/data",
            Some("sub/dir"),
        )?];

        assert_eq!(
            build_drives_boot_arg(&drives),
            Some("agentenv_drives=vdc:/mnt/data:sub/dir".to_string())
        );
        Ok(())
    }

    #[test]
    fn build_drives_boot_arg_omits_drives_mounted_after_boot() -> Result<()> {
        let drives = vec![
            ExtraDrive::try_new_overlaybd("image", "/tmp/image.json", true)?
                .with_volume_snapshot_output_dir(None),
        ];

        assert_eq!(build_drives_boot_arg(&drives), None);
        Ok(())
    }
}
