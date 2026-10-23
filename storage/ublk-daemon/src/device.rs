//! Kernel block-device frontends for daemon-managed OverlayBD targets.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use overlaybd::image_file::ImageFile;
use serde::Deserialize;
use storage_util::io_ring::IoRingHandle;
use uvm_nbd::kernel::NbdDevice;
use uvm_ublk::{
    delete_dev, wait_for_block_dev, wait_for_ublk_dev, OverlaybdTarget, UVMUblkCtrlBuilder,
    UVMUblkDev, UVMUblkDevBuilder, UVMUblkTarget,
};

/// Kernel interface selected for exposing OverlayBD images.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum DeviceBackendKind {
    /// `/dev/ublkbN` devices (Linux 6.0+ with `ublk_drv`).
    Ublk,
    /// `/dev/nbdN` devices for hosts without ublk.
    Nbd,
}

impl DeviceBackendKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ublk => "ublk",
            Self::Nbd => "nbd",
        }
    }
}

/// Runtime handle of the selected kernel interface.
#[derive(Clone)]
pub enum DeviceBackend {
    /// ublk devices driven through the shared ublk control io_uring.
    Ublk {
        ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    },
    /// NBD devices configured through generic netlink.
    Nbd,
}

impl DeviceBackend {
    /// Feature flags reported through `GetFeatures` (ublk `UBLK_F_*`).
    /// NBD reports none, so the warm pool reuses only same-size devices.
    pub(crate) async fn detect_features(&self) -> Result<u64> {
        match self {
            Self::Ublk { ctrl_ring } => detect_ublk_features(ctrl_ring).await,
            Self::Nbd => Ok(0),
        }
    }

    /// Create a started, accessible device serving `image`.
    pub(crate) async fn create(
        &self,
        image_config: &Path,
        image: &Arc<ImageFile>,
    ) -> Result<BlockDevice> {
        let discard_supported = !image.is_read_only().await;
        let target = OverlaybdTarget::from_opened_image(
            image_config.to_path_buf(),
            Arc::clone(image),
            discard_supported,
        )
        .context("create overlaybd target")?;

        match self {
            Self::Ublk { ctrl_ring } => {
                let dev = create_ublk_device(ctrl_ring, target).await?;
                Ok(BlockDevice::Ublk {
                    dev: Box::new(dev),
                    ctrl_ring: ctrl_ring.clone(),
                })
            }
            Self::Nbd => create_nbd_device(target).await.map(BlockDevice::Nbd),
        }
    }
}

/// A started kernel block device serving an [`OverlaybdTarget`].
pub(crate) enum BlockDevice {
    Ublk {
        dev: Box<UVMUblkDev<OverlaybdTarget>>,
        ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    },
    Nbd(NbdDevice<OverlaybdTarget>),
}

impl BlockDevice {
    pub(crate) fn dev_id(&self) -> u32 {
        match self {
            Self::Ublk { dev, .. } => dev.dev_id(),
            Self::Nbd(dev) => dev.index(),
        }
    }

    pub(crate) fn device_path(&self) -> PathBuf {
        match self {
            Self::Ublk { dev, .. } => dev.device_path(),
            Self::Nbd(dev) => dev.device_path(),
        }
    }

    pub(crate) fn target(&self) -> &Arc<OverlaybdTarget> {
        match self {
            Self::Ublk { dev, .. } => dev.target(),
            Self::Nbd(dev) => dev.export(),
        }
    }

    /// Stop the device and remove it from the kernel.
    pub(crate) async fn delete(self) -> Result<()> {
        match self {
            Self::Ublk { mut dev, ctrl_ring } => {
                let dev_id = dev.dev_id();
                quiesce_ublk_device(&mut *dev).await;
                // The device holds an open fd to the ublk char dev. Drop it
                // before DEL_DEV, or the command blocks.
                drop(dev);
                delete_dev(ctrl_ring, dev_id).await
            }
            Self::Nbd(dev) => {
                let index = dev.index();
                dev.disconnect()
                    .await
                    .with_context(|| format!("disconnect nbd device {index}"))
            }
        }
    }

    /// Resize the kernel device. Only valid when [`DeviceBackend::detect_features`]
    /// reported `UBLK_F_UPDATE_SIZE`; the returned future does not borrow `self`.
    pub(crate) fn update_size(
        &self,
        new_sectors: u64,
    ) -> Result<impl Future<Output = Result<()>> + Send + 'static> {
        match self {
            Self::Ublk { dev, .. } => Ok(dev.ctrl.update_size(new_sectors)),
            Self::Nbd(_) => anyhow::bail!("nbd devices cannot be resized"),
        }
    }
}

async fn create_ublk_device(
    ctrl_ring: &IoRingHandle<io_uring::squeue::Entry128>,
    target: OverlaybdTarget,
) -> Result<UVMUblkDev<OverlaybdTarget>> {
    let ctrl = UVMUblkCtrlBuilder::new()
        .name("overlaybd-blk")
        .build(ctrl_ring.clone())
        .context("build ublk ctrl")?;

    let mut dev = UVMUblkDevBuilder::new(ctrl)
        .set_target(target)
        .build()
        .await
        .context("build ublk dev")?;

    let dev_id = dev.dev_id();
    if let Err(err) = dev.start().await.context("start ublk dev") {
        cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
        return Err(err);
    }
    if let Err(err) = wait_for_ublk_dev(dev_id).context("wait for ublk device") {
        cleanup_failed_ublk_start(ctrl_ring.clone(), dev).await;
        return Err(err);
    }

    tracing::debug!(dev_id, path = %dev.device_path().display(), "created new ublk device");
    Ok(dev)
}

async fn create_nbd_device(target: OverlaybdTarget) -> Result<NbdDevice<OverlaybdTarget>> {
    let dev = NbdDevice::connect(Arc::new(target))
        .await
        .context("connect nbd device")?;
    let device_path = dev.device_path();
    let ready = tokio::task::spawn_blocking(move || wait_for_block_dev(&device_path))
        .await
        .context("join nbd device wait")
        .and_then(|ready| ready.context("wait for nbd device"));
    if let Err(err) = ready {
        let index = dev.index();
        if let Err(cleanup_err) = dev.disconnect().await {
            tracing::warn!(index, error = %cleanup_err, "failed to disconnect nbd device after startup failure");
        }
        return Err(err);
    }
    tracing::debug!(index = dev.index(), path = %dev.device_path().display(), "created new nbd device");
    Ok(dev)
}

async fn quiesce_ublk_device<T: UVMUblkTarget>(dev: &mut UVMUblkDev<T>) {
    let dev_id = dev.dev_id();

    if let Err(err) = dev.ctrl.stop_dev().await {
        tracing::warn!(dev_id, ?err, "failed to stop ublk device before delete");
        return;
    }

    if let Err(err) = tokio::time::timeout(Duration::from_secs(5), dev.wait_for_bg_tasks()).await {
        tracing::warn!(
            dev_id,
            ?err,
            "timed out waiting for ublk queue workers to exit after stop_dev"
        );
    }
}

async fn cleanup_failed_ublk_start<T: UVMUblkTarget>(
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    mut dev: UVMUblkDev<T>,
) {
    let dev_id = dev.dev_id();

    match dev.ctrl.stop_dev().await {
        Ok(()) => {}
        Err(err)
            if matches!(
                err.root_cause()
                    .downcast_ref::<std::io::Error>()
                    .and_then(|err| err.raw_os_error()),
                Some(libc::ENODEV)
            ) =>
        {
            tracing::info!(dev_id, "ublk device disappeared before startup cleanup");
            drop(dev);
            return;
        }
        Err(err) => {
            tracing::warn!(
                dev_id,
                ?err,
                "failed to stop ublk device after startup failure"
            );
        }
    }

    if let Err(err) = tokio::time::timeout(Duration::from_secs(5), dev.wait_for_bg_tasks()).await {
        tracing::warn!(
            dev_id,
            ?err,
            "timed out waiting for ublk queue workers after startup failure"
        );
    }

    drop(dev);

    let mut ctrl = match UVMUblkCtrlBuilder::new().dev_id(dev_id).build(ctrl_ring) {
        Ok(ctrl) => ctrl,
        Err(err) => {
            tracing::warn!(
                dev_id,
                ?err,
                "failed to build ublk ctrl for startup cleanup; kernel device may remain active"
            );
            return;
        }
    };

    match ctrl.del_dev().await {
        Ok(()) => {
            tracing::info!(dev_id, "deleted ublk device after startup failure");
        }
        Err(err)
            if matches!(
                err.root_cause()
                    .downcast_ref::<std::io::Error>()
                    .and_then(|err| err.raw_os_error()),
                Some(libc::ENODEV)
            ) =>
        {
            tracing::info!(dev_id, "ublk device already deleted after startup failure");
        }
        Err(err) => {
            tracing::warn!(
                dev_id,
                ?err,
                "failed to delete ublk device after startup failure; kernel device may remain active"
            );
        }
    }
}

/// Detect ublk features by sending GET_FEATURES through the ublk control ring.
async fn detect_ublk_features(ctrl_ring: &IoRingHandle<io_uring::squeue::Entry128>) -> Result<u64> {
    // GET_FEATURES does not create a device, so this fixed dev_id is only a
    // control-command field. Create paths omit dev_id to request kernel assignment.
    let ctrl = UVMUblkCtrlBuilder::new()
        .dev_id(0)
        .build(ctrl_ring.clone())
        .context("build ctrl for feature detection")?;

    match ctrl.get_features().await {
        Ok(features) => Ok(features),
        Err(err)
            if matches!(
                err.root_cause()
                    .downcast_ref::<std::io::Error>()
                    .and_then(|err| err.raw_os_error()),
                Some(libc::ENOTTY | libc::EINVAL | libc::EOPNOTSUPP)
            ) =>
        {
            tracing::warn!(
                error = %err,
                "ublk feature query unsupported; continuing without dynamic resize support"
            );
            Ok(0)
        }
        Err(err) => Err(err).context("get ublk features"),
    }
}
