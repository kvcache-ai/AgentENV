use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use overlaybd::config::UpperMode;
use uvm_ublk_daemon::CreateOverlaybdRuntimeDeviceRequest;

use crate::sandbox::ublk::{OverlaybdRuntimeHandle, UblkDeviceManager};
use crate::types::ExtraDrive;

#[derive(Clone, Debug)]
pub(crate) struct DriveMount {
    pub(crate) drive_id: String,
    pub(crate) attachment_path: PathBuf,
    pub(crate) read_only: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExtraDrivePrepareMode {
    Fresh {
        allow_shrink: bool,
    },
    Snapshot,
    /// Reopen a paused sandbox's existing upper instead of building a runtime copy.
    Paused,
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
            Self::Snapshot | Self::Paused => {
                // Snapshot and paused launches use the actual block-device
                // size recorded in the saved state.
                (drive.virtual_size(), drive.virtual_size())
            }
        }
    }

    fn allow_shrink(self) -> bool {
        match self {
            Self::Fresh { allow_shrink } => allow_shrink,
            Self::Snapshot | Self::Paused => false,
        }
    }
}

/// Append devices as soon as they are acquired so sandbox stop owns rollback,
/// including failures that occur before the attachment is ready.
pub(crate) async fn prepare_extra_drives(
    extra_drives: &[ExtraDrive],
    global_config_path: &Path,
    sandbox_work_dir: &Path,
    storage_root: &Path,
    runtime_upper_mode: UpperMode,
    mode: impl Fn(usize) -> ExtraDrivePrepareMode,
    runtimes: &mut Vec<OverlaybdRuntimeHandle>,
) -> Result<Vec<DriveMount>> {
    let mut mounts = Vec::with_capacity(extra_drives.len());
    for (index, drive) in extra_drives.iter().enumerate() {
        let mode = mode(index);
        let runtime_dir = drive.runtime_dir(storage_root);
        let (requested_virtual_size, known_source_virtual_size) = mode.device_sizes(drive);
        let allow_shrink = mode.allow_shrink();
        if matches!(mode, ExtraDrivePrepareMode::Snapshot) {
            match tokio::fs::remove_dir_all(&runtime_dir).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let manager = UblkDeviceManager::global();
        let runtime_device = match mode {
            ExtraDrivePrepareMode::Paused => {
                manager
                    .open_overlaybd_runtime(
                        drive.image_config_path(),
                        global_config_path,
                        drive.virtual_size().context("paused drive size")?,
                    )
                    .await?
            }
            _ => manager
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
                .context("create overlaybd extra drive runtime device")?,
        };
        let symlink_name = drive.attachment_symlink_name();
        let symlink_path = sandbox_work_dir.join(&symlink_name);
        let device_path = runtime_device.device.device_path().to_path_buf();
        runtimes.push(runtime_device);
        std::os::unix::fs::symlink(&device_path, &symlink_path).with_context(|| {
            format!(
                "symlink extra drive {} -> {}",
                symlink_path.display(),
                device_path.display()
            )
        })?;
        mounts.push(DriveMount {
            drive_id: drive.drive_id().to_string(),
            attachment_path: PathBuf::from(symlink_name),
            read_only: drive.read_only(),
        });
    }
    Ok(mounts)
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
            ExtraDrivePrepareMode::Snapshot.device_sizes(&drive_without_size),
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
            ExtraDrivePrepareMode::Snapshot.device_sizes(&sized),
            (Some(2 * 1024 * 1024 * 1024), Some(2 * 1024 * 1024 * 1024))
        );
    }
}
