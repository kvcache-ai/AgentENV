use anyhow::{bail, Context, Result};
use io_uring::{squeue::EntryMarker as SQEMarker, IoUring};
use slab::Slab;
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::ops::Deref;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::rc::Rc;
use tokio::io::unix::AsyncFd;

use super::IoUringSubmitter;
use super::IoUringSubmitterBox;
use crate::ReloadableIDAllocator;

#[derive(Debug)]
pub struct RingFutureData {
    waker: Option<std::task::Waker>,
    result: Option<i32>,
    /// When `true` the owning [`RingFuture`] has been dropped before the
    /// kernel delivered the CQE.  [`reap_cqe`](AsyncIoRing::reap_cqe) will
    /// discard the late result and reclaim the slab slot.
    cancelled: bool,
}

pub struct RingFuture {
    /// The key returned by `Slab`, used to find RingFutureData
    pub slab_key: usize,
    store: Rc<RefCell<Slab<RingFutureData>>>,
}

impl RingFuture {
    pub fn as_user_data(&self) -> u64 {
        self.slab_key as u64
    }

    pub fn slab_key_from_user_data(user_data: u64) -> usize {
        user_data as usize
    }
}

impl std::future::Future for RingFuture {
    type Output = i32;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut store = self.store.borrow_mut();
        match store.get_mut(self.slab_key) {
            None => {
                // this is weird, should not happened
                panic!("poll for RingFuture without related RingFutureData");
            }
            Some(data) => match data.result {
                Some(result) => std::task::Poll::Ready(result),
                None => {
                    data.waker = Some(cx.waker().clone());
                    std::task::Poll::Pending
                }
            },
        }
    }
}

impl Drop for RingFuture {
    fn drop(&mut self) {
        let mut store = self.store.borrow_mut();
        if let Some(data) = store.get_mut(self.slab_key) {
            if data.result.is_some() {
                // CQE already arrived, safe to free the slot immediately.
                store.remove(self.slab_key);
            } else {
                // CQE has not arrived yet.  Mark the slot as cancelled so
                // that `reap_cqe` will discard the late result and reclaim
                // the slot, instead of delivering to a recycled future.
                data.cancelled = true;
                data.waker = None;
            }
        }
    }
}

pub trait AsyncIoRingSQE: SQEMarker + Send + Sync + Sized + 'static {
    fn user_data(self, user_data: u64) -> Self;
}

impl AsyncIoRingSQE for io_uring::squeue::Entry {
    fn user_data(self, user_data: u64) -> Self {
        self.user_data(user_data)
    }
}

impl AsyncIoRingSQE for io_uring::squeue::Entry128 {
    fn user_data(self, user_data: u64) -> Self {
        self.user_data(user_data)
    }
}

/// generic type `S`: [io_uring::squeue::Entry] or [io_uring::squeue::Entry128].
pub struct AsyncIoRingBuilder<'a, S> {
    nr_sparse_buffer: Option<usize>,
    nr_sparse_file: Option<usize>,
    fds: Option<&'a [RawFd]>,
    sqe_entries: usize,
    cqe_entries: usize,
    _marker: PhantomData<S>,
}

impl<'a, S: AsyncIoRingSQE> Default for AsyncIoRingBuilder<'a, S> {
    fn default() -> Self {
        Self {
            nr_sparse_buffer: None,
            nr_sparse_file: None,
            fds: None,
            sqe_entries: 128,
            cqe_entries: 128,
            _marker: Default::default(),
        }
    }
}

impl<'a, S: AsyncIoRingSQE> AsyncIoRingBuilder<'a, S> {
    pub fn new() -> Self {
        Default::default()
    }

    /// Register a sparse buffer (i.e., IORING_REGISTER_BUFFERS2)
    /// during [build()](Self::build).
    /// - `nr`: The number of the empty sparse buffer.
    pub fn nr_sparse_buffer(mut self, nr: usize) -> Self {
        self.nr_sparse_buffer = Some(nr);
        self
    }

    /// Register these number of sparse file during [build()](Self::build).
    /// - `nr`: The number of the empty sparse files.
    ///
    /// In linux, the upper bound of is (1 << 20)
    pub fn nr_sparse_file(mut self, nr: usize) -> Self {
        self.nr_sparse_file = Some(nr);
        self
    }

    /// The `fds` will be register to io uring (i.e., IORING_REGISTER_FILES)
    /// during [build()](Self::build).
    ///
    /// This is conflict with [nr_sparse_file](Self::nr_sparse_file). If you
    /// set both, during build, it will failed.
    pub fn set_fds(mut self, fds: &'a [RawFd]) -> Self {
        self.fds = Some(fds);
        self
    }

    /// The upper bound in linux is 32768.
    pub fn sqe_entries(mut self, num: usize) -> Self {
        self.sqe_entries = num;
        self
    }

    /// The upper bound in linux is 65536.
    pub fn cqe_entries(mut self, num: usize) -> Self {
        self.cqe_entries = num;
        self
    }

    /// Create the io uring.
    pub fn build(self) -> Result<AsyncIoRing<S>> {
        if self.sqe_entries > 4096 {
            bail!("too much sqe entries")
        }
        if self.cqe_entries > 8192 {
            bail!("too much cqe entries")
        }
        if let Some(fds) = self.fds.as_ref() {
            if fds.len() > 64 {
                bail!("try to register too much fds");
            }
        }
        if let Some(nr) = self.nr_sparse_buffer {
            // NOTE: in kernel, support at most 1 << 14 buffers
            if nr > 4096 {
                bail!("too large nr_sparse_buffer");
            }
        }
        if let Some(nr) = self.nr_sparse_file {
            // NOTE: in kernel, support at most 1 << 20 files
            if nr > 8192 {
                bail!("too large nr_sparse_file");
            }
            if self.fds.is_some() {
                bail!("cannot set both fds and nr_sparse_file");
            }
        }
        let ring = IoUring::builder()
            .setup_coop_taskrun()
            .setup_single_issuer()
            .setup_cqsize(self.cqe_entries as u32)
            .build(self.sqe_entries as u32)
            .context("create io uring")?;
        if let Some(fds) = self.fds {
            ring.submitter()
                .register_files(fds)
                .context("register_files")?;
        }
        if let Some(nr) = self.nr_sparse_buffer {
            ring.submitter()
                .register_buffers_sparse(nr as _)
                .context("register_buffers_sparse")?;
        }
        if let Some(nr) = self.nr_sparse_file {
            ring.submitter()
                .register_files_sparse(nr as _)
                .context("register_files_sparse")?;
        }
        Ok(AsyncIoRing {
            ring: Rc::new(RefCell::new(ring)),
            future_data_store: Rc::new(RefCell::new(Slab::new())),
            inner: Rc::new(AsyncIoRingInner {
                inflight: Cell::new(0),
                sparse_buffer_id_allocator: ReloadableIDAllocator::new(0),
                nr_sparse_buffer: self.nr_sparse_buffer,
                sparse_file_id_allocator: ReloadableIDAllocator::new(0),
                nr_sparse_file: self.nr_sparse_file,
            }),
        })
    }
}

/// A wrapper for io uring, to be used in tokio async runtime.
/// Currently, only one thread can hold this io uring.
///
/// It implements Deref<Target=RefCell<IoUring>>, so you can use it like
/// a refcell-based IoUring.
#[derive(Clone)]
pub struct AsyncIoRing<S: AsyncIoRingSQE = io_uring::squeue::Entry> {
    pub ring: Rc<RefCell<IoUring<S>>>,
    pub future_data_store: Rc<RefCell<Slab<RingFutureData>>>,
    inner: Rc<AsyncIoRingInner>,
}

struct AsyncIoRingInner {
    /// track there are how many inflight requests
    inflight: Cell<usize>,

    sparse_buffer_id_allocator: ReloadableIDAllocator,
    nr_sparse_buffer: Option<usize>,

    sparse_file_id_allocator: ReloadableIDAllocator,
    nr_sparse_file: Option<usize>,
}

impl<S: AsyncIoRingSQE> AsyncIoRing<S> {
    pub(crate) fn reap_cqe(&self) {
        let mut ring = self.ring.borrow_mut();
        while let Some(cqe) = ring.completion().next() {
            self.inner.inflight.set(self.inner.inflight.get() - 1);
            let user_data = cqe.user_data();
            let slab_key = RingFuture::slab_key_from_user_data(user_data);
            let mut store = self.future_data_store.borrow_mut();
            match store.get_mut(slab_key) {
                Some(data) if data.cancelled => {
                    // The owning RingFuture was dropped before this CQE
                    // arrived.  Discard the result and reclaim the slot so
                    // the slab key can be safely reused.
                    store.remove(slab_key);
                }
                Some(data) => {
                    data.result = Some(cqe.result());
                    if let Some(waker) = data.waker.take() {
                        waker.wake();
                    }
                }
                None => {}
            }
        }
    }

    /// This typically should be spawned as a background task.
    pub async fn handle_completion(&self) -> Result<()> {
        let ring_fd = {
            // SAFETY: the ring is valid within this closure.
            let ring_fd = unsafe { BorrowedFd::borrow_raw(self.ring.borrow().as_raw_fd()) };
            let ring_fd = nix::unistd::dup(ring_fd).context("dup ring fd")?;
            AsyncFd::new(ring_fd).context("create tokio asyncfd for data ring")?
        };
        // there might be entries in cqe before we are epoll on uring, so try reap first
        self.reap_cqe();
        loop {
            let mut res = ring_fd
                .readable()
                .await
                .context("wait for ring to be readable")?;
            // reap the completion queue
            self.reap_cqe();
            res.clear_ready();
        }
    }

    fn prepare_fut(&self) -> RingFuture {
        let slab_key = self.future_data_store.borrow_mut().insert(RingFutureData {
            waker: None,
            result: None,
            cancelled: false,
        });
        RingFuture {
            slab_key,
            store: self.future_data_store.clone(),
        }
    }

    fn send_entry_(&self, entry: &S) -> std::io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        loop {
            // each submission() call will try to sync the sqe head
            let res = unsafe { ring.submission().push(entry) };
            match res {
                Ok(_) => {
                    self.inner.inflight.set(self.inner.inflight.get() + 1);
                    return Ok(());
                }
                Err(_) => {
                    if let Err(err) = ring.submit_and_wait(0) {
                        // if EINTR, just retry
                        if err.raw_os_error() == Some(libc::EINTR) {
                            continue;
                        }
                        return Err(err);
                    }
                }
            }
        }
    }

    /// The number of requests that has been submitted, but not completed.
    pub fn inflight_req(&self) -> usize {
        self.inner.inflight.get()
    }

    /// Register a buffer into the ring at runtime, returned the corresponding index.
    pub fn register_buffer(&self, buf: &mut [u8]) -> Result<u32> {
        let Some(nr) = self.inner.nr_sparse_buffer else {
            bail!("io uring does not support sparse buffer");
        };

        let idx = self.inner.sparse_buffer_id_allocator.allocate();
        if idx >= nr as u32 {
            bail!("try to register too many buffers for io uring");
        }
        let registered = unsafe {
            self.ring.borrow().submitter().register_buffers_update(
                idx,
                &[libc::iovec {
                    iov_base: buf.as_mut_ptr() as _,
                    iov_len: buf.len(),
                }],
                None,
            )
        };
        if let Err(err) = registered {
            self.inner.sparse_buffer_id_allocator.recycle(idx);
            return Err(err).context("register buffers");
        }
        Ok(idx)
    }

    /// Similar to [register_buffers](Self::register_buffers), but instead of actually
    /// register the buffer, just occupy an index of the sparse buffer table. So futher
    /// buffer register will not use the returned index.
    /// Typically used by [UBLK_F_AUTO_BUF_REG](ublk_sys::UBLK_F_AUTO_BUF_REG).
    pub fn occupy_sparse_buffer_index(&self) -> Result<u32> {
        let Some(nr) = self.inner.nr_sparse_buffer else {
            bail!("io uring does not support sparse buffer");
        };
        let idx = self.inner.sparse_buffer_id_allocator.allocate();
        if idx >= nr as u32 {
            bail!("io uring try to register too many buffers");
        }
        Ok(idx)
    }

    pub fn release_sparse_buffer_index(&self, idx: u32) -> Result<()> {
        if self.inner.sparse_buffer_id_allocator.is_free(idx) {
            bail!("try to release an unused buffer id {idx}");
        }
        self.inner.sparse_buffer_id_allocator.recycle(idx);
        Ok(())
    }

    /// Unregister a single buffer registered by [register_buffers][Self::register_buffers].
    ///
    /// The arguments is the index returned by [register_buffers][Self::register_buffers].
    pub fn unregister_buffer(&self, idx: u32) -> Result<()> {
        if self.inner.sparse_buffer_id_allocator.is_free(idx) {
            bail!("try to unregister an unused buffer with id {idx}");
        }
        let bufs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }];
        unsafe {
            self.ring
                .borrow()
                .submitter()
                .register_buffers_update(idx, &bufs, None)
                .context("unregister buffer")?;
        }
        self.inner.sparse_buffer_id_allocator.recycle(idx);
        Ok(())
    }

    /// Register a fd into the ring at runtime, returned the correspdoning index.
    ///
    /// You should set [nr_sparse_file](AsyncIoRingBuilder::nr_sparse_file) when build io uring.
    pub fn register_fd<R>(&self, fd: R) -> Result<u32>
    where
        R: AsRawFd,
    {
        let Some(nr) = self.inner.nr_sparse_file else {
            bail!("io uring does not support sparse file");
        };
        let idx = self.inner.sparse_file_id_allocator.allocate();
        if idx >= nr as u32 {
            bail!("try to register too many files for io uring");
        }
        self.ring
            .borrow()
            .submitter()
            .register_files_update(idx, &[fd.as_raw_fd()])
            .context("register fds")?;
        Ok(idx)
    }

    /// Caller should not pass any user data in the entry. It will be overwritten
    /// by this function.
    pub fn send(&self, entry: S) -> std::io::Result<RingFuture> {
        let fut = self.prepare_fut();
        self.send_entry_(&entry.user_data(fut.as_user_data()))?;
        Ok(fut)
    }
    /// Unregister a single fd registered by [register_buffers][Self::register_fds].
    ///
    /// The arguments is the index returned by [register_buffers][Self::register_fds].
    pub fn unregister_fd_sync(&self, idx: u32) -> Result<()> {
        if self.inner.sparse_file_id_allocator.is_free(idx) {
            bail!("try to unregister an unused file with id {idx}");
        }
        self.ring
            .borrow()
            .submitter()
            .register_files_update(idx, &[-1])
            .context("unregister_fd_sync")?;
        self.inner.sparse_file_id_allocator.recycle(idx);
        Ok(())
    }
}

impl IoUringSubmitterBox for AsyncIoRing<io_uring::squeue::Entry> {
    fn submit_box<'a>(
        &'a self,
        entry: io_uring::squeue::Entry,
    ) -> std::pin::Pin<Box<dyn std::prelude::rust_2024::Future<Output = std::io::Result<i32>> + 'a>>
    {
        Box::pin(async {
            let fut = self.send(entry)?;
            Ok(fut.await)
        })
    }
}

impl IoUringSubmitter for AsyncIoRing<io_uring::squeue::Entry> {
    async fn submit(&self, entry: io_uring::squeue::Entry) -> std::io::Result<i32> {
        let fut = self.send(entry)?;
        Ok(fut.await)
    }
}

impl AsyncIoRing<io_uring::squeue::Entry> {
    /// Unregister a single fd registered by [register_buffers][Self::register_fds].
    ///
    /// The arguments is the index returned by [register_buffers][Self::register_fds].
    pub async fn unregister_fd(&self, idx: u32) -> Result<()> {
        if self.inner.sparse_file_id_allocator.is_free(idx) {
            bail!("try to unregister an unused file with id {idx}");
        }
        let fds = [-1];
        let entry = io_uring::opcode::FilesUpdate::new(fds.as_ptr(), 1)
            .offset(idx as i32)
            .build();
        let fut = self.send(entry).context("send FilesUpdate to uring")?;
        let res = fut.await;
        if res < 0 {
            bail!(
                "unregister_fd {idx} failed: {:?}",
                std::io::Error::from_raw_os_error(-res)
            );
        }
        self.inner.sparse_file_id_allocator.recycle(idx);
        Ok(())
    }
}

impl<S: AsyncIoRingSQE> Deref for AsyncIoRing<S> {
    type Target = RefCell<IoUring<S>>;

    fn deref(&self) -> &Self::Target {
        self.ring.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::{AsyncIoRing, AsyncIoRingBuilder};

    #[test]
    fn failed_buffer_registration_releases_its_index() {
        let ring: AsyncIoRing = AsyncIoRingBuilder::new()
            .nr_sparse_buffer(1)
            .sqe_entries(8)
            .cqe_entries(16)
            .build()
            .expect("build async io ring");

        // io_uring rejects buffers over 1 GiB with EFAULT before pinning them.
        // The mapping is valid memory that is never touched.
        let len = (1 << 30) + 4096;
        let map = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        assert_ne!(map, libc::MAP_FAILED);
        let too_large = unsafe { std::slice::from_raw_parts_mut(map.cast::<u8>(), len) };
        let err = ring
            .register_buffer(too_large)
            .expect_err("a buffer over 1 GiB must fail");
        unsafe { libc::munmap(map, len) };
        let errno = err
            .chain()
            .find_map(|cause| cause.downcast_ref::<std::io::Error>())
            .and_then(std::io::Error::raw_os_error);
        assert_eq!(errno, Some(libc::EFAULT));

        // The only slot is free again.
        let mut good = vec![0u8; 4096];
        assert_eq!(ring.register_buffer(&mut good).expect("register buffer"), 0);
        ring.unregister_buffer(0).expect("unregister buffer");
    }
}
