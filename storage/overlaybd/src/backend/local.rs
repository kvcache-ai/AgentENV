use anyhow::{anyhow, ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use nix::errno::Errno;
use std::cmp::min;
use std::ffi::CString;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::io::virtual_file::VirtualFile;
#[cfg(feature = "io-uring")]
use crate::io::virtual_file::{IoCtx, LocalBoxFuture};
#[cfg(feature = "io-uring")]
use storage_util::io_ring::{self, IoUringSubmitter};
use storage_util::AlignedBuffer;

const DIRECT_IO_ALIGNMENT: usize = 512;
#[cfg(any(test, feature = "io-uring"))]
const BUFFERED_PWRITE_FAST_PATH_MAX: usize = 4096;

/// Builder for constructing a [`LocalFile`] with a fluent API.
///
/// # Example
/// ```ignore
/// let file = LocalFile::builder()
///     .read(true)
///     .write(true)
///     .create(true)
///     .open(path)
///     .await?;
/// ```
pub struct LocalFileBuilder {
    read: bool,
    write: bool,
    create: bool,
    truncate: bool,
    mode: u32,
    direct_io: bool,
}

impl Default for LocalFileBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalFileBuilder {
    pub fn new() -> Self {
        Self {
            read: true,
            write: true,
            create: true,
            truncate: false,
            mode: 0o644,
            direct_io: false,
        }
    }

    /// Set whether reading is enabled (default: `true`).
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Set whether writing is enabled (default: `true`).
    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Set whether the file should be created if it doesn't exist (default: `true`).
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Set whether the file should be truncated on open (default: `false`).
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// Set the file permission mode (default: `0o644`).
    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }

    /// Enable O_DIRECT for aligned I/O (default: `false`).
    pub fn direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }

    /// Open the file at the given path with the configured options.
    pub fn open(self, path: impl AsRef<Path>) -> Result<LocalFile> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options
            .read(self.read)
            .write(self.write)
            .create(self.create)
            .truncate(self.truncate)
            .mode(self.mode);
        if self.direct_io {
            options.custom_flags(libc::O_DIRECT);
        }

        let file = options.open(&path)?;
        Ok(LocalFile {
            inner: Arc::new(LocalFileInner {
                path,
                file,
                direct_io: self.direct_io,
                #[cfg(test)]
                write_calls: std::sync::atomic::AtomicU64::new(0),
            }),
        })
    }
}

/// Shared state of a [`LocalFile`], held behind an `Arc`.
///
/// Split out from `LocalFile` so that the `'static` closures handed to
/// [`tokio::task::spawn_blocking`] can own a cheap `Arc` clone. A closure that
/// borrowed `&self` instead would not outlive the async method's frame, which
/// `spawn_blocking`'s `'static` bound rejects.
struct LocalFileInner {
    path: PathBuf,
    file: File,
    direct_io: bool,
    /// Test-only probe counting synchronous positional-write submissions (the
    /// buffered small-write fast path plus explicit sync-pwrite calls).
    #[cfg(test)]
    write_calls: std::sync::atomic::AtomicU64,
}

pub struct LocalFile {
    inner: Arc<LocalFileInner>,
}

impl fmt::Debug for LocalFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalFile")
            .field("path", &self.inner.path)
            .finish_non_exhaustive()
    }
}

impl LocalFile {
    /// Create a [`LocalFileBuilder`].
    ///
    /// The builder defaults to read+write+create, mode 0o644, no truncate, no
    /// direct I/O – the same defaults the old `LocalFileOpenOptions::default()`
    /// had.
    pub fn builder() -> LocalFileBuilder {
        LocalFileBuilder::new()
    }

    /// Convenience: create-and-truncate (equivalent to the old `LocalFile::new`).
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::builder().truncate(true).open(path)
    }

    /// Convenience: open read-only (equivalent to the old `LocalFile::open_ro`).
    pub fn open_ro(path: impl AsRef<Path>) -> Result<Self> {
        Self::builder().write(false).create(false).open(path)
    }

    /// Convenience: open read-write with optional create (equivalent to the old
    /// `LocalFile::open_rw`).
    pub fn open_rw(path: impl AsRef<Path>, create: bool) -> Result<Self> {
        Self::builder().create(create).open(path)
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Test-only probe: number of synchronous positional-write submissions issued
    /// through the buffered small-write fast path and
    /// [`Self::write_at_sync_pwrite`].
    #[cfg(test)]
    pub fn write_calls(&self) -> u64 {
        self.inner.write_calls()
    }

    /// Write the whole buffer with one synchronous positional write, bypassing
    /// io_uring.
    ///
    /// This intentionally blocks the current thread for the duration of the
    /// syscall. The caller guarantees all of the following:
    /// - exclusive access to the file (no concurrent operations through this
    ///   `LocalFile`);
    /// - the file lives on local storage backed by page cache (buffered, not
    ///   O_DIRECT, not a network filesystem), so the syscall returns quickly;
    /// - blocking the calling thread (typically a Tokio `LocalSet` thread)
    ///   for one syscall is acceptable.
    ///
    /// For large buffered local writes this is cheaper than the io_uring
    /// submit/completion round trip; slow storage can still stall the
    /// thread, which is why the entry point is explicit rather than part of
    /// the async `write_at` path.
    pub fn write_at_sync_pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        self.inner.write_at_sync_pwrite(offset, buf)
    }

    /// Run one blocking file operation on the Tokio blocking pool.
    ///
    /// The closure receives an owned `Arc<LocalFileInner>` clone rather than a
    /// borrow of `self`, which is what lets it satisfy `spawn_blocking`'s
    /// `'static` bound.
    ///
    /// On the ublk queue threads (`current_thread` runtime driving a
    /// `LocalSet`) awaiting the join handle parks the runtime, which runs the
    /// `on_thread_park` io_uring submit and lets the other queue tasks make
    /// progress — the whole point of not running these syscalls inline.
    async fn offload<F, T>(&self, op: &'static str, f: F) -> Result<T>
    where
        F: FnOnce(&LocalFileInner) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || f(&inner))
            .await
            .map_err(|e| anyhow!("LocalFile {op} join blocking task failed: {e:?}"))
            .flatten()
    }
}

impl LocalFileInner {
    /// Test-only probe, see [`LocalFile::write_calls`].
    #[cfg(test)]
    fn write_calls(&self) -> u64 {
        self.write_calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn to_off_t(offset: u64) -> Result<libc::off_t> {
        libc::off_t::try_from(offset).context(format!("offset {offset} does not fit in off_t"))
    }

    fn posix_fadvise_dontneed(fd: i32, offset: u64, len: u64) -> Result<()> {
        let offset = Self::to_off_t(offset)?;
        let len = Self::to_off_t(len)?;
        let ret = unsafe { libc::posix_fadvise(fd, offset, len, libc::POSIX_FADV_DONTNEED) };
        if ret == 0 {
            return Ok(());
        }
        Err(Errno::from_raw(ret).into())
    }

    fn punch_hole_keep_size(fd: i32, offset: u64, len: u64) -> Result<()> {
        let offset = Self::to_off_t(offset)?;
        let len = Self::to_off_t(len)?;
        let ret = unsafe {
            libc::fallocate(
                fd,
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                offset,
                len,
            )
        };
        if ret == 0 {
            return Ok(());
        }
        Err(Errno::last().into())
    }

    /// Read as most `buf.len()` bytes into `buf`.
    ///
    /// Return the number of read bytes.
    fn read_into_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut read = 0;
        while read < buf.len() {
            let read_offset = offset
                .checked_add(read as u64)
                .context("read_up_to_at offset overflow")?;
            match file.read_at(&mut buf[read..], read_offset) {
                Ok(0) => break,
                Ok(n) => read += n,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(read)
    }

    /// Synchronous positional write of the whole buffer, see
    /// [`LocalFile::write_at_sync_pwrite`].
    fn write_at_sync_pwrite(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        #[cfg(test)]
        self.write_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.file.write_all_at(buf, offset)?;
        Ok(buf.len())
    }

    fn align_down(value: u64, alignment: u64) -> u64 {
        debug_assert!(alignment.is_power_of_two());
        value & !(alignment - 1)
    }

    fn align_up(value: u64, alignment: u64) -> u64 {
        debug_assert!(alignment.is_power_of_two());
        value.saturating_add(alignment - 1) & !(alignment - 1)
    }

    /// Read at most `len` bytes (as much as possible) started at `offset`.
    ///
    /// Note the returned Bytes might be shorter than `len`.
    fn read_buffered_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let mut buf = vec![0u8; len];
        let n = Self::read_into_at(&self.file, offset, &mut buf)?;
        buf.truncate(n);
        Ok(Bytes::from(buf))
    }

    fn read_direct_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let size = self.file.metadata()?.len();
        if offset >= size {
            return Ok(Bytes::new());
        }

        let expected = min((size - offset) as usize, len);
        let alignment = DIRECT_IO_ALIGNMENT as u64;
        let aligned_offset = Self::align_down(offset, alignment);
        let head =
            usize::try_from(offset - aligned_offset).context("read offset delta overflow")?;
        let aligned_end = Self::align_up(offset + expected as u64, alignment);
        let aligned_len = usize::try_from(aligned_end.saturating_sub(aligned_offset))
            .context("aligned read length overflow")?;
        let mut buffer = AlignedBuffer::new(aligned_len, DIRECT_IO_ALIGNMENT)?;
        let got = Self::read_into_at(&self.file, aligned_offset, buffer.as_mut())?;
        if got <= head {
            return Ok(Bytes::new());
        }
        let tail = (head + expected).min(got);
        let buffer = buffer.into_sub_range(head..tail)?;
        Ok(Bytes::from_owner(buffer))
    }

    fn read_at_sync(&self, offset: u64, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        if self.direct_io {
            return self.read_direct_at(offset, len);
        }
        self.read_buffered_at(offset, len)
    }

    fn read_at_into_sync(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }
        if self.direct_io {
            let data = self.read_direct_at(offset, dst.len())?;
            let n = data.len();
            dst[..n].copy_from_slice(&data);
            return Ok(n);
        }
        Self::read_into_at(&self.file, offset, dst)
    }

    fn write_at_sync(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.direct_io {
            ensure!(
                offset.is_multiple_of(DIRECT_IO_ALIGNMENT as u64),
                "write_at_sync: offset {offset} is not aligned to {DIRECT_IO_ALIGNMENT}"
            );
            ensure!(
                buf.len().is_multiple_of(DIRECT_IO_ALIGNMENT),
                "write_at_sync: buf len {} is not aligned to {DIRECT_IO_ALIGNMENT}",
                buf.len()
            );
        }
        #[cfg(test)]
        self.write_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.direct_io && !(buf.as_ptr() as usize).is_multiple_of(DIRECT_IO_ALIGNMENT) {
            let mut aligned = AlignedBuffer::new(buf.len(), DIRECT_IO_ALIGNMENT)?;
            aligned.as_mut().copy_from_slice(buf);
            self.file.write_all_at(aligned.as_ref(), offset)?;
            return Ok(buf.len());
        }
        self.file.write_all_at(buf, offset)?;
        Ok(buf.len())
    }
}

/*
 * Async read via IO URing
 */
#[cfg(feature = "io-uring")]
impl LocalFileInner {
    async fn read_buffered_at_via<S>(&self, submitter: &S, offset: u64, len: usize) -> Result<Bytes>
    where
        S: IoUringSubmitter + ?Sized,
    {
        let fd = self.file.as_raw_fd();
        let mut buf = vec![0u8; len];
        let n = io_ring::read_exact_at(submitter, fd, &mut buf, offset).await?;
        buf.truncate(n);
        Ok(Bytes::from(buf))
    }

    async fn read_direct_at_via<S>(&self, submitter: &S, offset: u64, len: usize) -> Result<Bytes>
    where
        S: IoUringSubmitter + ?Sized,
    {
        let size = self.file.metadata()?.len();
        if offset >= size {
            return Ok(Bytes::new());
        }

        let expected = min((size - offset) as usize, len);
        let alignment = DIRECT_IO_ALIGNMENT as u64;
        let aligned_offset = Self::align_down(offset, alignment);
        let head =
            usize::try_from(offset - aligned_offset).context("read offset delta overflow")?;
        let aligned_end = Self::align_up(offset + expected as u64, alignment);
        let aligned_len = usize::try_from(aligned_end.saturating_sub(aligned_offset))
            .context("aligned read length overflow")?;
        let fd = self.file.as_raw_fd();

        let mut buffer = AlignedBuffer::new(aligned_len, DIRECT_IO_ALIGNMENT)?;
        // For O_DIRECT, we must use aligned memory.  io_uring still requires
        // aligned buffers for O_DIRECT files, so we submit the aligned read
        // and then slice out the requested region.
        let got = io_ring::read_exact_at(submitter, fd, buffer.as_mut(), aligned_offset).await?;

        if got <= head {
            return Ok(Bytes::new());
        }
        let tail = (head + expected).min(got);
        let buffer = buffer.into_sub_range(head..tail)?;
        Ok(Bytes::from_owner(buffer))
    }

    /// Write `buf` to an O_DIRECT file at `offset`.
    ///
    /// Both `offset` and `buf.len()` must be multiples of [`DIRECT_IO_ALIGNMENT`]; an
    /// error is returned otherwise.
    ///
    /// If the caller's buffer pointer is already 512-byte aligned the write is submitted
    /// directly with no extra allocation or copy.  If the pointer is unaligned, the data is
    /// first copied into a temporary [`AlignedBuffer`] before the io_uring submission.
    async fn write_direct_at_via<S>(&self, submitter: &S, offset: u64, buf: &[u8]) -> Result<usize>
    where
        S: IoUringSubmitter + ?Sized,
    {
        ensure!(
            offset.is_multiple_of(DIRECT_IO_ALIGNMENT as u64),
            "write_direct_at: offset {offset} is not aligned to {DIRECT_IO_ALIGNMENT}"
        );
        ensure!(
            buf.len().is_multiple_of(DIRECT_IO_ALIGNMENT),
            "write_direct_at: buf len {} is not aligned to {DIRECT_IO_ALIGNMENT}",
            buf.len()
        );

        let fd = self.file.as_raw_fd();

        if (buf.as_ptr() as usize).is_multiple_of(DIRECT_IO_ALIGNMENT) {
            // Buffer pointer is already aligned — submit directly, no copy.
            Ok(io_ring::write_exact_at(submitter, fd, buf, offset).await?)
        } else {
            // Buffer pointer is unaligned — copy into an aligned bounce buffer first.
            let mut aligned = AlignedBuffer::new(buf.len(), DIRECT_IO_ALIGNMENT)?;
            aligned.as_mut().copy_from_slice(buf);
            Ok(io_ring::write_exact_at(submitter, fd, aligned.as_ref(), offset).await?)
        }
    }

    /// Shared implementation of `read_at` parameterised on the submitter.
    async fn read_at_via<S>(&self, submitter: &S, offset: u64, len: usize) -> Result<Bytes>
    where
        S: IoUringSubmitter + ?Sized,
    {
        if len == 0 {
            return Ok(Bytes::new());
        }
        if self.direct_io {
            return self.read_direct_at_via(submitter, offset, len).await;
        }
        self.read_buffered_at_via(submitter, offset, len).await
    }

    /// Shared implementation of `read_at_into` parameterised on the submitter.
    async fn read_at_into_via<S>(&self, submitter: &S, offset: u64, dst: &mut [u8]) -> Result<usize>
    where
        S: IoUringSubmitter + ?Sized,
    {
        if dst.is_empty() {
            return Ok(0);
        }
        let fd = self.file.as_raw_fd();

        if self.direct_io {
            let dst_ptr = dst.as_mut_ptr() as usize;
            let dst_len = dst.len();

            if dst_ptr.is_multiple_of(DIRECT_IO_ALIGNMENT)
                && dst_len.is_multiple_of(DIRECT_IO_ALIGNMENT)
                && offset.is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
            {
                // if dst is already aligned, just read into dst
                Ok(io_ring::read_exact_at(submitter, fd, dst, offset).await?)
            } else {
                // if dst not aligned, we need allocate an aligned buffer
                // and copy into dst instead.
                let data = self
                    .read_direct_at_via(submitter, offset, dst.len())
                    .await?;
                let n = data.len();
                dst[..n].copy_from_slice(&data);
                Ok(n)
            }
        } else {
            Ok(io_ring::read_exact_at(submitter, fd, dst, offset).await?)
        }
    }

    /// Shared implementation of `write_at` parameterised on the submitter.
    async fn write_at_via<S>(&self, submitter: &S, offset: u64, buf: &[u8]) -> Result<usize>
    where
        S: IoUringSubmitter + ?Sized,
    {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.direct_io {
            return self.write_direct_at_via(submitter, offset, buf).await;
        }

        self.write_buffered_at_via(submitter, offset, buf).await
    }

    /// Shared implementation of `write_bytes_at` parameterised on the submitter.
    async fn write_bytes_at_via<S>(&self, submitter: &S, offset: u64, data: &Bytes) -> Result<usize>
    where
        S: IoUringSubmitter + ?Sized,
    {
        if data.is_empty() {
            return Ok(0);
        }
        if self.direct_io {
            return self.write_direct_at_via(submitter, offset, data).await;
        }

        self.write_buffered_at_via(submitter, offset, data).await
    }

    async fn write_buffered_at_via<S>(
        &self,
        submitter: &S,
        offset: u64,
        buf: &[u8],
    ) -> Result<usize>
    where
        S: IoUringSubmitter + ?Sized,
    {
        if buf.len() <= BUFFERED_PWRITE_FAST_PATH_MAX {
            // Tradeoff: this is a synchronous syscall inside async code. It avoids
            // io_uring submit/completion overhead for 4 KiB data writes and tiny
            // index appends, while direct I/O and larger writes stay on io_uring.
            #[cfg(test)]
            self.write_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.file.write_all_at(buf, offset)?;
            return Ok(buf.len());
        }

        let fd = self.file.as_raw_fd();
        Ok(io_ring::write_exact_at(submitter, fd, buf, offset).await?)
    }
}

#[async_trait]
impl VirtualFile for LocalFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        self.offload("read_at", move |inner| inner.read_at_sync(offset, len))
            .await
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    /// TODO: this blocks the calling thread for the duration of the `pread`.
    /// It cannot be offloaded with [`tokio::task::spawn_blocking`] because the
    /// closure would have to capture `dst: &mut [u8]`, which does not satisfy
    /// `spawn_blocking`'s `'static` bound; and because a `spawn_blocking` task
    /// cannot be cancelled,
    /// a raw-pointer workaround would let the blocking write outlive a dropped
    /// future and scribble over freed memory. Making this properly async needs
    /// either an ownership-passing variant of the trait method (hand the buffer
    /// in and get it back, like `tokio-uring` does) or `block_in_place` gated
    /// on the runtime flavor.
    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.inner.read_at_into_sync(offset, dst)
    }

    /// TODO: blocks the calling thread — see [`Self::read_at_into`] for why the
    /// borrowed `buf` keeps this off `spawn_blocking`. Callers that already own
    /// their payload should prefer [`Self::write_bytes_at`], which does offload.
    async fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        self.inner.write_at_sync(offset, buf)
    }

    async fn write_bytes_at(&self, offset: u64, data: Bytes) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        self.offload("write_bytes_at", move |inner| {
            inner.write_at_sync(offset, &data)
        })
        .await
    }

    #[cfg(feature = "io-uring")]
    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(self.inner.read_at_via(ctx.ring(), offset, len))
    }

    #[cfg(feature = "io-uring")]
    fn read_at_into_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(self.inner.read_at_into_via(ctx.ring(), offset, dst))
    }

    #[cfg(feature = "io-uring")]
    fn write_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(self.inner.write_at_via(ctx.ring(), offset, data))
    }

    #[cfg(feature = "io-uring")]
    fn write_bytes_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: Bytes,
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            self.inner
                .write_bytes_at_via(ctx.ring(), offset, &data)
                .await
        })
    }

    /// Deliberately inline: this is a bare `fstat`, served from the cached
    /// inode with no I/O, and callers hit it often enough (LSMT stack open,
    /// cache-store size refresh) that a `spawn_blocking` round trip would cost
    /// more than the syscall it offloads.
    async fn size(&self) -> Result<u64> {
        Ok(self.inner.file.metadata()?.len())
    }

    /// Offloaded: `ftruncate` is cheap when growing a sparse file, but a shrink
    /// has to invalidate page cache and free extents while holding the inode
    /// lock exclusively, so it can queue behind in-flight writeback or a
    /// journal commit. It is never on the per-I/O path here (only LSMT
    /// create/resize), so the offload is free.
    async fn truncate(&self, size: u64) -> Result<()> {
        self.offload("truncate", move |inner| {
            inner.file.set_len(size)?;
            Ok(())
        })
        .await
    }

    /// Offloaded: `fsync` has unbounded latency — it waits for writeback of
    /// every dirty page plus a device flush.
    async fn sync(&self) -> Result<()> {
        self.offload("sync", |inner| {
            inner.file.sync_all()?;
            Ok(())
        })
        .await
    }

    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        let fd = self.inner.file.as_raw_fd();
        let off = LocalFileInner::to_off_t(offset)?;
        let result = unsafe { libc::lseek(fd, off, libc::SEEK_DATA) };
        if result < 0 {
            let err = Errno::last();
            if err == Errno::ENXIO {
                return Ok(None);
            }
            return Err(err.into());
        }
        let val = u64::try_from(result)
            .context(format!("seek_data returned negative offset {result}"))?;
        Ok(Some(val))
    }

    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        let fd = self.inner.file.as_raw_fd();
        let off = LocalFileInner::to_off_t(offset)?;
        let result = unsafe { libc::lseek(fd, off, libc::SEEK_HOLE) };
        if result < 0 {
            let err = Errno::last();
            if err == Errno::ENXIO {
                return Ok(None);
            }
            return Err(err.into());
        }
        let val = u64::try_from(result)
            .context(format!("seek_hole returned negative offset {result}"))?;
        Ok(Some(val))
    }

    /// Offloaded: `FALLOC_FL_PUNCH_HOLE` is real filesystem work (freeing
    /// extents plus a journal commit), not a hint, and it sits on a
    /// guest-triggerable path — a guest `fstrim` reaches here in a loop from
    /// `LSMTFile::discard_range`. Inline it would stall every in-flight I/O on
    /// the ublk queue thread.
    ///
    /// The yield point this adds only widens an existing window:
    /// `discard_range` takes the index write lock *after* this call returns, so
    /// a concurrent read could already see the punched-but-not-yet-remapped
    /// range. Such a read resolves through the old mapping into the punched
    /// region and gets zeros, which is what a completed discard means anyway.
    async fn discard(&self, offset: u64, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        self.offload("discard", move |inner| {
            LocalFileInner::punch_hole_keep_size(inner.file.as_raw_fd(), offset, len)
        })
        .await
    }

    /// Offloaded: `POSIX_FADV_DONTNEED` is a hint, but servicing it still walks
    /// the page cache over the range and can queue behind writeback of any
    /// dirty page it finds. No caller is on a hot path — the only in-tree ones
    /// are the ZFile checksum/decompress retry branches — so the offload costs
    /// nothing. Awaiting it keeps the ordering those retries rely on: the
    /// eviction completes before the range is re-read.
    async fn evict_range(&self, offset: u64, len: u64) -> Result<()> {
        self.offload("evict_range", move |inner| {
            LocalFileInner::posix_fadvise_dontneed(inner.file.as_raw_fd(), offset, len)
        })
        .await
    }

    /// Offloaded for the same reason as [`Self::evict_range`], and more so:
    /// `len == 0` means "to end of file", so this walks the page cache of an
    /// entire layer file.
    async fn evict_all(&self) -> Result<()> {
        self.offload("evict_all", |inner| {
            LocalFileInner::posix_fadvise_dontneed(inner.file.as_raw_fd(), 0, 0)
        })
        .await
    }

    async fn fgetxattr(&self, name: &str) -> Result<Vec<u8>> {
        let cname = CString::new(name).context("xattr name contains interior NUL byte")?;
        let fd = self.inner.file.as_raw_fd();

        // SAFETY: fd and C string are valid; null buffer with size 0 is allowed to query length.
        let need = unsafe { libc::fgetxattr(fd, cname.as_ptr(), std::ptr::null_mut(), 0) };
        if need < 0 {
            return Err(Errno::last().into());
        }
        let need = usize::try_from(need).context("xattr length overflow")?;
        let mut buf = vec![0u8; need];
        // SAFETY: destination buffer is valid for `need` bytes; fd/name unchanged.
        let got = unsafe {
            libc::fgetxattr(
                fd,
                cname.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_void,
                need,
            )
        };
        if got < 0 {
            return Err(Errno::last().into());
        }
        let got = usize::try_from(got).context("xattr length overflow")?;
        buf.truncate(got);
        Ok(buf)
    }

    async fn flistxattr(&self) -> Result<Vec<String>> {
        let fd = self.inner.file.as_raw_fd();

        // SAFETY: fd is valid; null buffer with size 0 is allowed to query length.
        let need = unsafe { libc::flistxattr(fd, std::ptr::null_mut(), 0) };
        if need < 0 {
            return Err(Errno::last().into());
        }
        let need = usize::try_from(need).context("xattr list length overflow")?;
        if need == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; need];
        // SAFETY: destination buffer is valid for `need` bytes.
        let got = unsafe { libc::flistxattr(fd, buf.as_mut_ptr() as *mut libc::c_char, need) };
        if got < 0 {
            return Err(Errno::last().into());
        }
        let got = usize::try_from(got).context("xattr list length overflow")?;
        buf.truncate(got);

        let mut out = Vec::new();
        for raw in buf.split(|b| *b == 0).filter(|s| !s.is_empty()) {
            out.push(String::from_utf8_lossy(raw).into_owned());
        }
        Ok(out)
    }

    async fn fsetxattr(&self, name: &str, value: &[u8], flags: i32) -> Result<()> {
        let cname = CString::new(name).context("xattr name contains interior NUL byte")?;
        let fd = self.inner.file.as_raw_fd();
        // SAFETY: fd/name/value pointers are valid for the supplied length.
        let ret = unsafe {
            libc::fsetxattr(
                fd,
                cname.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                flags,
            )
        };
        if ret != 0 {
            return Err(Errno::last().into());
        }
        Ok(())
    }

    async fn fremovexattr(&self, name: &str) -> Result<()> {
        let cname = CString::new(name).context("xattr name contains interior NUL byte")?;
        let fd = self.inner.file.as_raw_fd();
        // SAFETY: fd/name are valid.
        let ret = unsafe { libc::fremovexattr(fd, cname.as_ptr()) };
        if ret != 0 {
            return Err(Errno::last().into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_util::AlignedBuffer;
    use tempfile::{NamedTempFile, TempDir};

    /// Open a fresh O_DIRECT file at `path` (truncated).
    fn open_direct_io_file(path: impl AsRef<std::path::Path>) -> LocalFile {
        LocalFile::builder()
            .direct_io(true)
            .truncate(true)
            .open(path)
            .expect("open direct-io file")
    }

    #[tokio::test]
    async fn test_local_file_write_at_preserves_offset_semantics() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("offset.bin");
        let file = LocalFile::new(&path).expect("create local file");

        file.write_at(4096, &[0x22; 512])
            .await
            .expect("write high offset");
        file.write_at(0, &[0x11; 512])
            .await
            .expect("write low offset");
        file.sync().await.expect("sync local file");

        let head = file.read_at(0, 512).await.expect("read head");
        assert_eq!(head.as_ref(), &[0x11; 512]);

        let tail = file.read_at(4096, 512).await.expect("read tail");
        assert_eq!(tail.as_ref(), &[0x22; 512]);

        let meta = std::fs::metadata(&path).expect("stat written file");
        assert!(
            meta.len() >= 4096 + 512,
            "file size should include the higher-offset write"
        );
    }

    #[tokio::test]
    async fn test_local_file_small_buffered_write_at() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("small-write-at.bin");
        let file = LocalFile::new(&path).expect("create local file");

        let payload = vec![0x5A; BUFFERED_PWRITE_FAST_PATH_MAX];
        let written = file
            .write_at(128, &payload)
            .await
            .expect("small buffered write_at");
        assert_eq!(written, payload.len());

        let got = file.read_at(128, payload.len()).await.expect("read back");
        assert_eq!(got.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn test_local_file_small_buffered_write_bytes_at() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("small-write-bytes-at.bin");
        let file = LocalFile::new(&path).expect("create local file");

        let payload = Bytes::from(vec![0x6B; 257]);
        let written = file
            .write_bytes_at(64, payload.clone())
            .await
            .expect("small buffered write_bytes_at");
        assert_eq!(written, payload.len());

        let got = file.read_at(64, payload.len()).await.expect("read back");
        assert_eq!(got.as_ref(), payload.as_ref());
    }

    #[tokio::test]
    async fn test_local_file_large_buffered_write_at() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("large-write-at.bin");
        let file = LocalFile::new(&path).expect("create local file");

        let payload = vec![0x7C; BUFFERED_PWRITE_FAST_PATH_MAX + 1];
        let written = file
            .write_at(0, &payload)
            .await
            .expect("large buffered write_at");
        assert_eq!(written, payload.len());

        let got = file.read_at(0, payload.len()).await.expect("read back");
        assert_eq!(got.as_ref(), payload.as_slice());
    }

    /// A 512-byte-aligned buffer pointer takes the fast path (no copy inside
    /// `write_direct_at`).  We verify the data round-trips correctly.
    #[tokio::test]
    async fn test_write_direct_at_aligned_ptr_fast_path() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("aligned.bin");
        let file = open_direct_io_file(&path);

        // AlignedBuffer guarantees a 512-byte-aligned pointer — fast path.
        let mut wbuf = AlignedBuffer::new(512, DIRECT_IO_ALIGNMENT).expect("alloc");
        wbuf.as_mut().fill(0xAB);
        file.write_at(0, wbuf.as_ref())
            .await
            .expect("aligned write");

        let got = file.read_at(0, 512).await.expect("read back");
        assert_eq!(got.as_ref(), &[0xAB_u8; 512]);
    }

    /// An unaligned buffer pointer triggers the bounce-buffer copy path inside
    /// `write_direct_at`.  We synthesise a guaranteed-unaligned pointer by
    /// allocating a 513-byte `AlignedBuffer` (512-aligned base) and using
    /// `into_sub_range(1..513)` — the resulting slice starts at base+1, which
    /// is never a multiple of 512.
    #[tokio::test]
    async fn test_write_direct_at_unaligned_ptr_bounce() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("bounce.bin");
        let file = open_direct_io_file(&path);

        // Allocate 513 bytes with 512-byte alignment.  The base pointer is
        // aligned, but sub_range(1..513) shifts it by one byte — guaranteed unaligned.
        let mut wbuf = AlignedBuffer::new(513, DIRECT_IO_ALIGNMENT).expect("alloc 513-byte buffer");
        wbuf.as_mut().fill(0xCD);
        let unaligned = wbuf.into_sub_range(1..513).expect("sub-range");
        assert_ne!(
            unaligned.as_ref().as_ptr() as usize % DIRECT_IO_ALIGNMENT,
            0,
            "pointer must be unaligned for this test to be meaningful"
        );

        file.write_at(0, unaligned.as_ref())
            .await
            .expect("unaligned-ptr write via bounce buffer");

        let got = file.read_at(0, 512).await.expect("read back");
        assert_eq!(got.as_ref(), &[0xCD_u8; 512]);
    }

    /// `write_at` on a direct-IO file must reject an offset that is not a
    /// multiple of 512.
    #[tokio::test]
    async fn test_write_direct_at_unaligned_offset_rejected() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("bad_offset.bin");
        let file = open_direct_io_file(&path);

        let err = file
            .write_at(1, &[0u8; 512])
            .await
            .expect_err("unaligned offset must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("not aligned"),
            "error should mention alignment, got: {msg}"
        );
    }

    /// `write_at` on a direct-IO file must reject a buffer whose length is not
    /// a multiple of 512.
    #[tokio::test]
    async fn test_write_direct_at_unaligned_len_rejected() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("bad_len.bin");
        let file = open_direct_io_file(&path);

        let err = file
            .write_at(0, &[0u8; 100])
            .await
            .expect_err("unaligned length must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("not aligned"),
            "error should mention alignment, got: {msg}"
        );
    }

    /// `write_bytes_at` on a direct-IO file dispatches through the same
    /// `write_direct_at` path and must round-trip correctly.
    #[tokio::test]
    async fn test_write_bytes_at_direct_io() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("bytes_at.bin");
        let file = open_direct_io_file(&path);

        let data = bytes::Bytes::from(vec![0xBB_u8; 512]);
        file.write_bytes_at(0, data).await.expect("write_bytes_at");

        let got = file.read_at(0, 512).await.expect("read back");
        assert_eq!(got.as_ref(), &[0xBB_u8; 512]);
    }

    #[tokio::test]
    async fn test_local_file_read_empty() {
        let tmp = NamedTempFile::new().expect("create temp");
        let path = tmp.path().to_path_buf();
        drop(tmp);

        let file = LocalFile::new(&path).unwrap();
        let data = file.read_at(0, 100).await.unwrap();
        assert_eq!(data.len(), 0);
    }

    #[tokio::test]
    async fn test_local_file_sync() {
        let tmp = NamedTempFile::new().expect("create temp");
        let path = tmp.path().to_path_buf();
        drop(tmp);

        let file = LocalFile::new(&path).unwrap();
        file.write_at(0, &[0xAA; 512]).await.unwrap();
        file.sync().await.unwrap();

        let data = file.read_at(0, 512).await.unwrap();
        assert_eq!(data.as_ref(), &[0xAA; 512]);
    }
}
