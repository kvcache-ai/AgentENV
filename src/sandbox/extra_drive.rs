use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use overlaybd::config::UpperMode;
use tracing::warn;
use uvm_ublk_daemon::CreateOverlaybdRuntimeDeviceRequest;

use crate::sandbox::ublk::{OverlaybdRuntimeHandle, UblkDeviceManager};
use crate::types::ExtraDrive;

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
