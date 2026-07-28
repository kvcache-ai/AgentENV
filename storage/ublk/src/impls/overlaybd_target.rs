use anyhow::{bail, Context, Result};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use clap::Args;
use overlaybd::image_file::ImageFile;
use overlaybd::image_service::ImageService;
use overlaybd::virtual_file::{IoCtx, VirtualFile};
use std::fmt;
use std::io::ErrorKind;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use storage_util::io_ring::AsyncIoRing;

use crate::io_weight_scheduler::IoWeightHandle;
use crate::{IOBuffer, UVMUblkTarget, UblkDescOperation};

const DEFAULT_PHYSICAL_BS_SHIFT: u8 = 12;
const DEFAULT_IO_OPT_SHIFT: u8 = DEFAULT_PHYSICAL_BS_SHIFT;

/// Internal state of an OverlaybdTarget that can be hot-swapped.
#[derive(Clone)]
struct TargetState {
    image_config_path: PathBuf,
    image: Arc<ImageFile>,
    dev_sectors: u64,
    logical_bs_shift: u8,
    physical_bs_shift: u8,
    discard_supported: bool,
}

impl TargetState {
    fn dev_bytes(&self) -> u64 {
        self.dev_sectors << self.logical_bs_shift
    }
}

#[derive(Debug, Clone, Args)]
pub struct OverlaybdTargetConfig {
    /// Path to overlaybd global config json.
    #[arg(long)]
    pub global_config: PathBuf,
    /// Path to overlaybd image config json.
    #[arg(long)]
    pub image_config: PathBuf,
}

pub struct OverlaybdTarget {
    state: ArcSwap<TargetState>,
    /// Optional weight-based I/O throttle handle. Set via `set_io_weight` after creation.
    io_weight: ArcSwap<Option<Arc<IoWeightHandle>>>,
}

impl fmt::Debug for OverlaybdTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load();
        f.debug_struct("OverlaybdTarget")
            .field("image_config_path", &state.image_config_path)
            .field("dev_sectors", &state.dev_sectors)
            .field("logical_bs_shift", &state.logical_bs_shift)
            .field("physical_bs_shift", &state.physical_bs_shift)
            .field("discard_supported", &state.discard_supported)
            .finish_non_exhaustive()
    }
}

impl OverlaybdTarget {
    pub async fn from_config(config: &OverlaybdTargetConfig) -> Result<Self> {
        Self::open(&config.global_config, &config.image_config).await
    }

    pub async fn open(
        global_config_path: impl AsRef<Path>,
        image_config_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let global_config_path = global_config_path.as_ref().to_path_buf();
        let image_config_path = image_config_path.as_ref().to_path_buf();
        let image_service = ImageService::from_config_path(&global_config_path)
            .await
            .with_context(|| {
                format!(
                    "open overlaybd image service failed: {}",
                    global_config_path.display()
                )
            })?;
        let image = Arc::new(
            image_service
                .create_image_file(&image_config_path)
                .await
                .with_context(|| {
                    format!(
                        "open overlaybd image file failed: {}",
                        image_config_path.display()
                    )
                })?,
        );
        let discard_supported = !image.is_read_only().await;
        Self::from_opened_image(image_config_path, image, discard_supported)
    }

    pub fn from_opened_image(
        image_config_path: PathBuf,
        image: Arc<ImageFile>,
        discard_supported: bool,
    ) -> Result<Self> {
        let logical_bs_shift = validate_block_size_shift(image.block_size)?;
        let dev_sectors = image.num_lbas();
        if dev_sectors == 0 {
            bail!(
                "overlaybd image has zero capacity: {}",
                image_config_path.display()
            );
        }

        let state = TargetState {
            image_config_path,
            image,
            dev_sectors,
            logical_bs_shift,
            physical_bs_shift: DEFAULT_PHYSICAL_BS_SHIFT.max(logical_bs_shift),
            discard_supported,
        };

        Ok(Self {
            state: ArcSwap::new(Arc::new(state)),
            io_weight: ArcSwap::new(Arc::new(None)),
        })
    }

    /// Attach a weight-based I/O scheduling handle to this target.
    pub fn set_io_weight(&self, handle: Arc<IoWeightHandle>) {
        self.io_weight.store(Arc::new(Some(handle)));
    }

    /// Swap the target's backing image and capacity atomically.
    ///
    /// This is used by the daemon warm pool to reuse a ublk device across
    /// different overlaybd images without tearing down the device.
    ///
    /// Precondition: the ublk device must be idle with no in-flight I/O.
    /// Requests that already loaded the previous state will continue to use it.
    pub fn swap_state(
        &self,
        image_config_path: PathBuf,
        image: Arc<ImageFile>,
        discard_supported: bool,
    ) -> Result<()> {
        let logical_bs_shift = validate_block_size_shift(image.block_size)?;
        let dev_sectors = image.num_lbas();
        if dev_sectors == 0 {
            bail!(
                "overlaybd image has zero capacity: {}",
                image_config_path.display()
            );
        }

        let new_state = TargetState {
            image_config_path,
            image,
            dev_sectors,
            logical_bs_shift,
            physical_bs_shift: DEFAULT_PHYSICAL_BS_SHIFT.max(logical_bs_shift),
            discard_supported,
        };

        self.state.store(Arc::new(new_state));
        Ok(())
    }

    fn request_meta(
        &self,
        state: &TargetState,
        io_desc: ublk_sys::ublksrv_io_desc,
    ) -> std::result::Result<(UblkDescOperation, u64, usize), i32> {
        let op = UblkDescOperation::try_from(io_desc.op_flags & 0xff).map_err(|_| -libc::EINVAL)?;
        let offset = io_desc.start_sector << state.logical_bs_shift;
        let len_u64 = (io_desc.nr_sectors as u64) << state.logical_bs_shift;
        let end = offset.checked_add(len_u64).ok_or(-libc::EINVAL)?;
        if end > state.dev_bytes() {
            return Err(-libc::EINVAL);
        }
        let len = usize::try_from(len_u64).map_err(|_| -libc::EINVAL)?;
        Ok((op, offset, len))
    }

    async fn handle_read(
        &self,
        state: &TargetState,
        ctx: IoCtx<'_>,
        offset: u64,
        len: usize,
        buf: &mut IOBuffer,
    ) -> Result<usize> {
        if len == 0 {
            return Ok(0);
        }
        match buf {
            IOBuffer::User(user_buf) => {
                let dst = user_buf.subslice_mut(0, len);
                let n = state.image.read_at_into_with_ctx(ctx, offset, dst).await?;
                if n != len {
                    bail!("overlaybd target short read at offset {offset}: expect {len}, got {n}");
                }
                Ok(n)
            }
            IOBuffer::AutoReg(_) => bail!("overlaybd target does not support zero-copy IO buffer"),
        }
    }

    async fn handle_write(
        &self,
        state: &TargetState,
        ctx: IoCtx<'_>,
        offset: u64,
        len: usize,
        buf: &mut IOBuffer,
    ) -> Result<usize> {
        if len == 0 {
            return Ok(0);
        }
        let data = match buf {
            IOBuffer::User(user_buf) => user_buf.subslice(0, len),
            IOBuffer::AutoReg(_) => {
                bail!("overlaybd target does not support zero-copy IO buffer");
            }
        };
        let written = state.image.write_at_with_ctx(ctx, offset, data).await?;
        if written != len {
            bail!("overlaybd target short write at offset {offset}: expect {len}, wrote {written}");
        }
        Ok(written)
    }

    async fn handle_discard(&self, state: &TargetState, offset: u64, len: usize) -> Result<i32> {
        if len == 0 {
            return Ok(0);
        }
        if !state.discard_supported {
            return Err(std::io::Error::new(
                ErrorKind::PermissionDenied,
                "overlaybd discard on read-only image",
            )
            .into());
        }
        state.image.discard(offset, len as u64).await?;
        Ok(0)
    }
}

fn validate_block_size_shift(block_size: u32) -> Result<u8> {
    if block_size == 0 || !block_size.is_power_of_two() {
        bail!("overlaybd block size must be power-of-two, got {block_size}");
    }
    Ok(block_size.trailing_zeros() as u8)
}

fn build_ublk_params(
    dev_sectors: u64,
    logical_bs_shift: u8,
    physical_bs_shift: u8,
    max_io_buf_bytes: u32,
    discard_supported: bool,
) -> ublk_sys::ublk_params {
    let logical_block_size = 1u32 << logical_bs_shift;
    ublk_sys::ublk_params {
        types: ublk_sys::UBLK_PARAM_TYPE_BASIC
            | if discard_supported {
                ublk_sys::UBLK_PARAM_TYPE_DISCARD
            } else {
                0
            },
        len: size_of::<ublk_sys::ublk_params>() as u32,
        basic: ublk_sys::ublk_param_basic {
            attrs: 0,
            logical_bs_shift,
            physical_bs_shift,
            io_min_shift: logical_bs_shift,
            io_opt_shift: DEFAULT_IO_OPT_SHIFT.max(physical_bs_shift),
            max_sectors: max_io_buf_bytes >> logical_bs_shift,
            dev_sectors,
            ..Default::default()
        },
        discard: if discard_supported {
            ublk_sys::ublk_param_discard {
                discard_alignment: logical_block_size,
                discard_granularity: logical_block_size,
                max_discard_sectors: max_io_buf_bytes >> logical_bs_shift,
                max_write_zeroes_sectors: 0,
                max_discard_segments: 1,
                ..Default::default()
            }
        } else {
            Default::default()
        },
        ..Default::default()
    }
}

fn anyhow_to_ublk_errno(err: &anyhow::Error) -> i32 {
    if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
        if let Some(errno) = io_err.raw_os_error() {
            return -errno;
        }
        let errno = match io_err.kind() {
            ErrorKind::PermissionDenied => libc::EROFS,
            ErrorKind::Unsupported => libc::EOPNOTSUPP,
            ErrorKind::InvalidInput | ErrorKind::InvalidData => libc::EINVAL,
            ErrorKind::WouldBlock => libc::EAGAIN,
            ErrorKind::TimedOut => libc::ETIMEDOUT,
            ErrorKind::Interrupted => libc::EINTR,
            ErrorKind::UnexpectedEof | ErrorKind::WriteZero => libc::EIO,
            _ => libc::EIO,
        };
        return -errno;
    }
    -libc::EIO
}

#[async_trait(?Send)]
impl UVMUblkTarget for OverlaybdTarget {
    const DEV_NAME: &str = "overlaybd";

    async fn init(&self, _qid: u16, _io_ring: &AsyncIoRing) -> Result<()> {
        Ok(())
    }

    fn per_slot_extra_buf_len(&self) -> Option<usize> {
        None
    }

    fn ublk_dev_params(&self, dev_info: &ublk_sys::ublksrv_ctrl_dev_info) -> ublk_sys::ublk_params {
        let state = self.state.load();
        build_ublk_params(
            state.dev_sectors,
            state.logical_bs_shift,
            state.physical_bs_shift,
            dev_info.max_io_buf_bytes,
            state.discard_supported,
        )
    }

    async fn handle_io_request(
        self: &Arc<Self>,
        qid: u16,
        tag: u16,
        io_desc: ublk_sys::ublksrv_io_desc,
        buf: &mut IOBuffer,
        _extra: Option<&mut IOBuffer>,
        io_ring: &AsyncIoRing,
    ) -> i32 {
        let state = self.state.load_full();
        let (op, offset, len) = match self.request_meta(&state, io_desc) {
            Ok(meta) => meta,
            Err(err) => {
                tracing::error!(qid, tag, ?io_desc, "invalid overlaybd ublk request");
                return err;
            }
        };

        tracing::debug!(qid, tag, ?op, offset, len, "handle overlaybd ublk request");

        // Weight-based I/O throttle: acquire tokens proportional to request size.
        if let Some(ref wh) = **self.io_weight.load() {
            wh.acquire(len as u64).await;
        }

        let ctx = IoCtx::new(io_ring);
        let result: Result<i32> = match op {
            UblkDescOperation::Read => self
                .handle_read(&state, ctx, offset, len, buf)
                .await
                .map(|n| n as i32),
            UblkDescOperation::Write => self
                .handle_write(&state, ctx, offset, len, buf)
                .await
                .map(|n| n as i32),
            UblkDescOperation::Flush => state.image.sync().await.map(|_| 0),
            UblkDescOperation::Discard => self.handle_discard(&state, offset, len).await,
            UblkDescOperation::WriteZeroes => {
                Err(anyhow::anyhow!("overlaybd target does not support {op:?}"))
            }
            _ => Err(anyhow::anyhow!("overlaybd target does not support {op:?}")),
        };

        match result {
            Ok(ret) => ret,
            Err(err) => {
                let errno = anyhow_to_ublk_errno(&err);
                tracing::error!(
                    qid,
                    tag,
                    ?op,
                    offset,
                    len,
                    ?err,
                    errno,
                    "overlaybd request failed"
                );
                errno
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        anyhow_to_ublk_errno, build_ublk_params, validate_block_size_shift, OverlaybdTarget,
    };
    use crate::{IOBuffer, UVMUblkTarget, UserBuffer};
    use overlaybd::config::UpperMode;
    use overlaybd::helper::prepare_runtime_upper;
    use overlaybd::virtual_file::VirtualFile;
    use overlaybd::ImageService;
    use std::io::{Error, ErrorKind};
    use std::sync::Arc;
    use storage_util::io_ring::AsyncIoRingBuilder;
    use tempfile::TempDir;
    use ublk_sys::ublksrv_io_desc;

    #[test]
    fn test_overlaybd_block_size_shift_validation() {
        assert_eq!(validate_block_size_shift(512).unwrap(), 9);
        assert_eq!(validate_block_size_shift(4096).unwrap(), 12);
        assert!(validate_block_size_shift(0).is_err());
        assert!(validate_block_size_shift(768).is_err());
    }

    #[test]
    fn test_overlaybd_build_params_geometry() {
        let params = build_ublk_params(1024, 9, 12, 512 * 1024, false);
        assert_eq!(params.types, ublk_sys::UBLK_PARAM_TYPE_BASIC);
        assert_eq!(params.basic.logical_bs_shift, 9);
        assert_eq!(params.basic.physical_bs_shift, 12);
        assert_eq!(params.basic.io_min_shift, 9);
        assert_eq!(params.basic.io_opt_shift, 12);
        assert_eq!(params.basic.max_sectors, 1024);
        assert_eq!(params.basic.dev_sectors, 1024);
        assert_eq!(params.basic.attrs, 0);
    }

    #[test]
    fn test_overlaybd_build_params_advertise_discard_when_supported() {
        let params = build_ublk_params(1024, 9, 12, 512 * 1024, true);
        assert_eq!(
            params.types,
            ublk_sys::UBLK_PARAM_TYPE_BASIC | ublk_sys::UBLK_PARAM_TYPE_DISCARD
        );
        assert_eq!(params.discard.discard_alignment, 512);
        assert_eq!(params.discard.discard_granularity, 512);
        assert_eq!(params.discard.max_discard_sectors, 1024);
        assert_eq!(params.discard.max_write_zeroes_sectors, 0);
        assert_eq!(params.discard.max_discard_segments, 1);
    }

    #[test]
    fn test_overlaybd_io_error_mapping() {
        let to_errno = |err: Error| anyhow_to_ublk_errno(&anyhow::Error::from(err));
        assert_eq!(
            to_errno(Error::new(ErrorKind::PermissionDenied, "ro")),
            -libc::EROFS
        );
        assert_eq!(
            to_errno(Error::new(ErrorKind::Unsupported, "unsupported")),
            -libc::EOPNOTSUPP
        );
        assert_eq!(
            to_errno(Error::new(ErrorKind::InvalidInput, "bad input")),
            -libc::EINVAL
        );
        assert_eq!(
            to_errno(Error::from_raw_os_error(libc::ENOENT)),
            -libc::ENOENT
        );
        assert_eq!(
            anyhow_to_ublk_errno(&anyhow::anyhow!("unknown error")),
            -libc::EIO
        );
    }

    fn write_global_config(tmp: &TempDir) -> anyhow::Result<std::path::PathBuf> {
        let path = tmp.path().join("overlaybd-global.json");
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "registryFsVersion": "v2",
                "nrIoRings": 1,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": tmp.path().join("cache"),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                    "blockSize": 65536
                },
                "download": {
                    "enable": false
                }
            }))?,
        )?;
        Ok(path)
    }

    fn write_sparse_image_config(
        tmp: &TempDir,
        upper_data: &std::path::Path,
    ) -> anyhow::Result<std::path::PathBuf> {
        let path = tmp.path().join("overlaybd-image.json");
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "lowers": [],
                "upper": {
                    "mode": "sparse",
                    "data": upper_data
                },
                "resultFile": tmp.path().join("result.txt")
            }))?,
        )?;
        Ok(path)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_overlaybd_target_discard_passthrough_sparse_image() {
        let tmp = TempDir::new().expect("tempdir");
        let global_config = write_global_config(&tmp).expect("write global config");
        let upper_data = tmp.path().join("upper.data");
        prepare_runtime_upper(&upper_data, None, 8192, UpperMode::Sparse)
            .expect("prepare sparse upper");
        let image_config =
            write_sparse_image_config(&tmp, &upper_data).expect("write image config");

        let service = ImageService::from_config_path(&global_config)
            .await
            .expect("open image service");
        let image = Arc::new(
            service
                .create_image_file(&image_config)
                .await
                .expect("open image"),
        );
        image
            .write_at(0, &[0x7Au8; 4096])
            .await
            .expect("prime sparse image");

        let target = Arc::new(
            OverlaybdTarget::from_opened_image(image_config.clone(), image, true).unwrap(),
        );
        let ring = AsyncIoRingBuilder::new()
            .nr_sparse_buffer(2)
            .nr_sparse_file(2)
            .sqe_entries(8)
            .cqe_entries(16)
            .build()
            .expect("build async io ring");
        let mut buf = IOBuffer::User(
            UserBuffer::new(ring.clone(), 4096, 512)
                .await
                .expect("allocate io buffer"),
        );
        let io_desc = ublksrv_io_desc {
            op_flags: ublk_sys::UBLK_IO_OP_DISCARD,
            nr_sectors: 8,
            start_sector: 0,
            addr: buf.uring_buf_idx() as u64,
        };

        let ret = target
            .handle_io_request(0, 0, io_desc, &mut buf, None, &ring)
            .await;
        assert_eq!(ret, 0);

        drop(target);
        let reopened = OverlaybdTarget::open(&global_config, &image_config)
            .await
            .expect("reopen target");
        let state = reopened.state.load_full();
        let got = state
            .image
            .read_at(0, 4096)
            .await
            .expect("read after discard");
        assert!(got.iter().all(|&b| b == 0));
    }
}
