/// The original ublkqueue from libublk is not flexible for our use case,
/// we try to build our own.
use anyhow::{anyhow, bail, Context, Result};
use io_uring::{opcode::UringCmd16, types::Fixed};
use nix::sys::mman::{mmap, munmap, MapFlags, ProtFlags};
use std::os::fd::AsFd;
use std::{cell::OnceCell, ops::Deref};
use ublk_sys::ublk_auto_buf_reg_to_sqe_addr;

use crate::IOBuffer;
use storage_util::io_ring::{AsyncIoRing, AsyncIoRingBuilder, RingFuture};

thread_local! {
    pub(crate) static UBLK_QUEUE_URING: OnceCell<AsyncIoRing> = const { OnceCell:: new() };
}

// NOTE: Copy from [libublk](https://github.com/ublk-org/libublk-rs)
/// A struct with the same memory layout as `io_uring_sys::io_uring_sqe`.
/// The definition must match the one in `io-uring-sys` crate.
/// This is a simplified version for demonstration.
#[repr(C)]
pub struct RawSqe {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: u64,
    pub addr: u64,
    pub len: u32,
    pub rw_flags: u32,
    user_data: u64,
    pub buf_index: u16,
    personality: u16,
    splice_fd_in: i32,
    __pad2: u32,
}

#[macro_export]
macro_rules! override_sqe {
    ($entry:expr, $field:ident, $value:expr) => {
        let entry = $entry;
        let value = $value;
        unsafe {
            let sqe = &mut *std::ptr::from_mut(entry).cast::<$crate::queue::RawSqe>();
            sqe.$field = value;
        }
    };
    ($entry:expr, $field:ident, |=, $value:expr) => {
        let entry = $entry;
        let value = $value;
        unsafe {
            let sqe = &mut *std::ptr::from_mut(entry).cast::<$crate::queue::RawSqe>();
            sqe.$field |= value;
        }
    };
}

/// Represent a hardware queue of the ublk device.
/// Each queue should running inside a single thread, as it contains Rc and RefCell.
pub struct UVMUblkQueue {
    pub dev_info: ublk_sys::ublksrv_ctrl_dev_info,
    pub queue_id: u16,
    pub depth: u16,
    pub ring: AsyncIoRing,
    io_desc: UblkQueueIoDesc,
    cdev_idx: u32,
}

/// The io descriptor array for a queue
pub struct UblkQueueIoDesc {
    ptr: std::ptr::NonNull<ublk_sys::ublksrv_io_desc>,
    len: usize,
    mark: std::marker::PhantomData<ublk_sys::ublksrv_io_desc>,
}

unsafe impl Send for UblkQueueIoDesc {}
unsafe impl Sync for UblkQueueIoDesc {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UblkDescOperation {
    Read,
    Write,
    Flush,
    Discard,
    WriteSame,
    WriteZeroes,
    ZoneOpen,
    ZoneClose,
    ZoneFinish,
    ZoneAppend,
    ZoneResetAll,
    ZoneReset,
}

impl TryFrom<u32> for UblkDescOperation {
    type Error = ();

    fn try_from(value: u32) -> std::result::Result<Self, Self::Error> {
        match value {
            ublk_sys::UBLK_IO_OP_READ => Ok(Self::Read),
            ublk_sys::UBLK_IO_OP_WRITE => Ok(Self::Write),
            ublk_sys::UBLK_IO_OP_FLUSH => Ok(Self::Flush),
            ublk_sys::UBLK_IO_OP_DISCARD => Ok(Self::Discard),
            ublk_sys::UBLK_IO_OP_WRITE_SAME => Ok(Self::WriteSame),
            ublk_sys::UBLK_IO_OP_WRITE_ZEROES => Ok(Self::WriteZeroes),
            ublk_sys::UBLK_IO_OP_ZONE_OPEN => Ok(Self::ZoneOpen),
            ublk_sys::UBLK_IO_OP_ZONE_CLOSE => Ok(Self::ZoneClose),
            ublk_sys::UBLK_IO_OP_ZONE_FINISH => Ok(Self::ZoneFinish),
            ublk_sys::UBLK_IO_OP_ZONE_APPEND => Ok(Self::ZoneAppend),
            ublk_sys::UBLK_IO_OP_ZONE_RESET_ALL => Ok(Self::ZoneResetAll),
            ublk_sys::UBLK_IO_OP_ZONE_RESET => Ok(Self::ZoneReset),
            _ => Err(()),
        }
    }
}

impl UblkQueueIoDesc {
    pub fn new<F: AsFd>(queue_id: u16, queue_depth: usize, cdev: F) -> Result<Self> {
        // Take the code from libublk:
        // Each queue will have at most UBLK_MAX_QUEUE_DEPTH io_dsec
        // use it to get the mmap offset of ublkc dev.
        let pgsize = nix::unistd::sysconf(nix::unistd::SysconfVar::PAGE_SIZE)
            .context("get system page size")?
            .ok_or_else(|| anyhow!("get page size unsupported"))? as usize;
        let max_cmd_buf_sz = (ublk_sys::UBLK_MAX_QUEUE_DEPTH as usize)
            * std::mem::size_of::<ublk_sys::ublksrv_io_desc>();
        let max_cmd_buf_sz = max_cmd_buf_sz.next_multiple_of(pgsize) as libc::off_t;

        let desc_buf_sz = queue_depth * std::mem::size_of::<ublk_sys::ublksrv_io_desc>();

        let off = ublk_sys::UBLKSRV_CMD_BUF_OFFSET as libc::off_t
            + (queue_id as libc::off_t) * max_cmd_buf_sz;

        let io_desc_buf = unsafe {
            mmap(
                None,
                std::num::NonZero::new(desc_buf_sz).ok_or_else(|| anyhow!("zero desc buf"))?,
                ProtFlags::PROT_READ,
                MapFlags::MAP_SHARED | MapFlags::MAP_POPULATE,
                &cdev,
                off,
            )
            .context("mmap ublksrv_io_dsec buffer")?
        };
        Ok(Self {
            ptr: io_desc_buf.cast::<ublk_sys::ublksrv_io_desc>(),
            len: queue_depth,
            mark: Default::default(),
        })
    }
}

impl Deref for UblkQueueIoDesc {
    type Target = [ublk_sys::ublksrv_io_desc];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for UblkQueueIoDesc {
    fn drop(&mut self) {
        let len = self.len * std::mem::size_of::<ublk_sys::ublksrv_io_desc>();
        unsafe {
            let _ = munmap(self.ptr.cast::<libc::c_void>(), len);
        }
    }
}

impl UVMUblkQueue {
    /// Call this function in a LocalSet tokio runtime
    pub fn new<F: AsFd>(
        queue_id: u16,
        dev_info: ublk_sys::ublksrv_ctrl_dev_info,
        cdev: F,
    ) -> Result<Self> {
        let depth = dev_info.queue_depth as usize;
        let ring = AsyncIoRingBuilder::new()
            .nr_sparse_buffer(depth * 4)
            .nr_sparse_file(16)
            .sqe_entries(depth * 2)
            .cqe_entries(depth * 2)
            .build()
            .with_context(|| format!("create io uring for queue {queue_id}"))?;
        tokio::task::spawn_local({
            let ring = ring.clone();
            async move {
                if let Err(err) = ring.handle_completion().await {
                    panic!("UVMUblkQueue uring handle_completion has exited unexpectly: {err:?}",);
                }
            }
        });
        if UBLK_QUEUE_URING
            .with(|uring| uring.set(ring.clone()))
            .is_err()
        {
            bail!("uring for ublk queue {queue_id} has already been initialized");
        }
        let io_desc = UblkQueueIoDesc::new(queue_id, depth, cdev.as_fd())
            .context("mmap for ublksrv_io_dsec area")?;
        let cdev_idx = ring
            .register_fd(cdev.as_fd())
            .context("register ublk char device into io uring")?;
        Ok(Self {
            dev_info,
            queue_id,
            depth: depth as _,
            ring,
            io_desc,
            cdev_idx,
        })
    }

    pub fn is_zero_copy(&self) -> bool {
        (self.dev_info.flags & (ublk_sys::UBLK_F_AUTO_BUF_REG as u64)) != 0
    }

    pub fn iodepth(&self) -> usize {
        self.dev_info.queue_depth as usize
    }

    /// Send the UBLK IO command to ublkdrv
    /// - `cmd_op`: e.g., UBLK_U_IO_COMMIT_AND_FETCH_REQ or UBLK_U_IO_FETCH_REQ.
    /// - `io_cmd`: Contains the info of the result of previous completion, and the buf for next data.
    /// - `sqe_addr`: When using auto_buf_reg, we need to set a special addr in sqe.
    fn send_io_cmd(
        &self,
        cmd_op: u32,
        io_cmd: ublk_sys::ublksrv_io_cmd,
        sqe_addr: Option<u64>,
    ) -> std::io::Result<RingFuture> {
        let sqe = {
            let mut sqe = UringCmd16::new(Fixed(self.cdev_idx), cmd_op)
                .cmd(unsafe { std::mem::transmute::<ublk_sys::ublksrv_io_cmd, [u8; 16]>(io_cmd) })
                .build();
            if let Some(sqe_addr) = sqe_addr {
                override_sqe!(&mut sqe, addr, sqe_addr);
            }
            sqe
        };
        self.ring.send(sqe)
    }

    /// Setup the IO environment for depth at `tag`, fetch for the first request
    /// (i.e., send UBLK_U_IO_FETCH_REQ).
    /// If the ublk device has enabled zero copy, register the `buf` for ublkdrv
    /// (i.e., send UBLK_U_IO_REGISTER_IO_BUF).
    /// - `tag`: a slot within the the queue (corresponding to the depth).
    /// - `buf`: the IO buffer used for next fetched IO rquest.
    /// - `extra_bufs`: the extra IO buffer for this slot.
    pub fn prep_slot(&self, tag: u16, buf: &IOBuffer) -> std::io::Result<RingFuture> {
        // register the buf for io uring
        let io_cmd = ublk_sys::ublksrv_io_cmd {
            q_id: self.queue_id,
            tag,
            result: 0,
            addr: match buf {
                // NOTE:  Linux requires addr to be unset when enable AUTO_BUF_REG, ZERO_COPY or
                // USER_COPY
                IOBuffer::AutoReg(_) => 0,
                IOBuffer::User(buf) => buf.subslice(0, buf.len()).as_ptr() as _,
            },
        };
        let sqe_addr = match buf {
            IOBuffer::AutoReg(auto_buf) => Some(ublk_auto_buf_reg_to_sqe_addr(
                auto_buf.as_ublk_auto_buf_reg(),
            )),
            IOBuffer::User(_) => None,
        };
        tracing::debug!(?io_cmd, "prep_slot send FETCH_REQ cmd");
        self.send_io_cmd(ublk_sys::UBLK_U_IO_FETCH_REQ, io_cmd, sqe_addr)
    }

    /// Complete the previous request at `tag` and start to wait for next one.
    /// This should be used after handle the IO request at slot `tag`.
    /// - `tag`: a slot within the the queue (corresponding to the depth).
    /// - `buf`: the IO buffer, which contains the completed data and will be used for next fetched IO request.
    /// - `result`: the result of the previous completed IO request.
    pub fn submit_and_fetch_for_slot(
        &self,
        tag: u16,
        buf: &IOBuffer,
        result: i32,
    ) -> std::io::Result<RingFuture> {
        let io_cmd = ublk_sys::ublksrv_io_cmd {
            q_id: self.queue_id,
            tag,
            result,
            addr: match buf {
                // NOTE:  Linux requires addr to be unset when enable AUTO_BUF_REG, ZERO_COPY or
                // USER_COPY
                IOBuffer::AutoReg(_) => 0,
                IOBuffer::User(buf) => buf.subslice(0, buf.len()).as_ptr() as _,
            },
        };
        let sqe_addr = match buf {
            IOBuffer::AutoReg(auto_buf) => Some(ublk_auto_buf_reg_to_sqe_addr(
                auto_buf.as_ublk_auto_buf_reg(),
            )),
            IOBuffer::User(_) => None,
        };
        self.send_io_cmd(ublk_sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ, io_cmd, sqe_addr)
    }

    pub fn io_desc(&self, tag: u16) -> Option<ublk_sys::ublksrv_io_desc> {
        self.io_desc.get(tag as usize).copied()
    }
}

impl Drop for UVMUblkQueue {
    fn drop(&mut self) {
        if let Err(err) = self.ring.unregister_fd_sync(self.cdev_idx) {
            tracing::error!(
                ?err,
                idx = self.cdev_idx,
                "unregister ublk char in UVMUblkQueue failed"
            );
        }
    }
}
