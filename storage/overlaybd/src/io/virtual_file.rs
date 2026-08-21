// src/core/fs.rs

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
#[cfg(feature = "io-uring")]
use std::future::Future;
#[cfg(feature = "io-uring")]
use std::pin::Pin;
use std::sync::Arc;

#[cfg(feature = "io-uring")]
use storage_util::io_ring::AsyncIoRing;
use storage_util::{CompactBuffer, CompactWriter};

/// Boxed non-`Send` future used by the `_with_ctx` variants on
/// [`VirtualFile`]. The `IoCtx` borrows a `!Send` [`AsyncIoRing`] (it holds
/// `Rc<...>` internally), so any future that captures the ring must also be
/// non-`Send`. The existing methods on the trait (transformed by
/// `#[async_trait]`) keep returning `Send` futures because `async_trait`
/// only rewrites `async fn` signatures, leaving these hand-rolled methods
/// untouched.
#[cfg(feature = "io-uring")]
pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Per-IO context that lets callers pass a specific io_uring instance down
/// through the `VirtualFile` stack. The `_with_ctx` variants of read/write
/// methods accept this so that, e.g., ublk queue threads can submit IO via
/// their own [`AsyncIoRing`] instead of falling back to synchronous positional
/// I/O in [`VirtualFile::read_at`] and [`VirtualFile::write_at`].
#[cfg(feature = "io-uring")]
#[derive(Clone, Copy)]
pub struct IoCtx<'a> {
    ring: &'a AsyncIoRing,
}

#[cfg(feature = "io-uring")]
impl<'a> IoCtx<'a> {
    pub fn new(ring: &'a AsyncIoRing) -> Self {
        Self { ring }
    }

    pub fn ring(&self) -> &'a AsyncIoRing {
        self.ring
    }
}

#[async_trait]
pub trait VirtualFile: Send + Sync {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>;

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }
        let data = self.read_at(offset, dst.len()).await?;
        let n = data.len().min(dst.len());
        dst[..n].copy_from_slice(&data[..n]);
        Ok(n)
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize>;

    async fn write_bytes_at(&self, offset: u64, data: Bytes) -> Result<usize> {
        self.write_at(offset, &data).await
    }

    /// Optional downcast handle for concrete-type fast paths. The default
    /// returns `None`; implementations that want to expose their concrete
    /// type (e.g. `LocalFile` for the zfile sync-pwrite fast path) override
    /// this to return `Some(self)`.
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    /// Context-aware read. Default forwards to [`Self::read_at`], ignoring
    /// `ctx`. Implementations that can submit IO directly via the provided
    /// io_uring (e.g. `LocalFile`) override this and any layer that wants
    /// to keep the ctx alive while dispatching to inner files overrides it
    /// to propagate `ctx` downward.
    ///
    /// Returned future is **not** `Send` because `AsyncIoRing` is `!Send`
    /// (holds `Rc<...>`). The whole ublk dispatch path runs on a
    /// `LocalSet` single-thread runtime so this matches the execution model.
    #[cfg(feature = "io-uring")]
    fn read_at_with_ctx<'a>(
        &'a self,
        _ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(self.read_at(offset, len))
    }

    /// Context-aware variant of [`Self::read_at_into`]. The default
    /// implementation mirrors `read_at_into`'s default but goes through
    /// [`Self::read_at_with_ctx`] so subclasses only need to override the
    /// bytes-returning variant when a single propagation point suffices.
    #[cfg(feature = "io-uring")]
    fn read_at_into_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            if dst.is_empty() {
                return Ok(0);
            }
            let data = self.read_at_with_ctx(ctx, offset, dst.len()).await?;
            let n = data.len().min(dst.len());
            dst[..n].copy_from_slice(&data[..n]);
            Ok(n)
        })
    }

    /// Context-aware write. Default forwards to [`Self::write_at`].
    #[cfg(feature = "io-uring")]
    fn write_at_with_ctx<'a>(
        &'a self,
        _ctx: IoCtx<'a>,
        offset: u64,
        data: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(self.write_at(offset, data))
    }

    /// Context-aware variant of [`Self::write_bytes_at`]. The default
    /// implementation forwards through [`Self::write_at_with_ctx`] so a
    /// single override is enough for most implementations.
    #[cfg(feature = "io-uring")]
    fn write_bytes_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: Bytes,
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move { self.write_at_with_ctx(ctx, offset, &data).await })
    }

    async fn size(&self) -> Result<u64>;

    async fn truncate(&self, _size: u64) -> Result<()> {
        bail!("truncate is not supported")
    }

    async fn sync(&self) -> Result<()> {
        bail!("sync is not supported")
    }

    async fn seek_data(&self, _offset: u64) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn seek_hole(&self, _offset: u64) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Discard a file range. Implementations may encode this as hole punching
    /// while preserving logical file size.
    async fn discard(&self, _offset: u64, _len: u64) -> Result<()> {
        bail!("discard is not supported")
    }

    /// Hint that underlying cache/page-cache for a range can be evicted.
    /// Implementations may return Unsupported when not available.
    async fn evict_range(&self, _offset: u64, _len: u64) -> Result<()> {
        Ok(())
    }

    /// Hint that underlying cache/page-cache for the whole file can be evicted.
    /// Implementations may return Unsupported when not available.
    async fn evict_all(&self) -> Result<()> {
        Ok(())
    }

    /// File xattr operations, aligned with overlaybd IFileXAttr semantics.
    async fn fgetxattr(&self, _name: &str) -> Result<Vec<u8>> {
        bail!("fgetxattr is not supported")
    }

    async fn flistxattr(&self) -> Result<Vec<String>> {
        bail!("flistxattr is not supported")
    }

    async fn fsetxattr(&self, _name: &str, _value: &[u8], _flags: i32) -> Result<()> {
        bail!("fsetxattr is not supported")
    }

    async fn fremovexattr(&self, _name: &str) -> Result<()> {
        bail!("fremovexattr is not supported")
    }
}

/// [`CompactWriter`] backed by a [`VirtualFile`].
///
/// Allocates plain `Vec<u8>` buffers and delegates writes to
/// [`VirtualFile::write_at`]. This preserves the existing behaviour for
/// local files, io_uring-backed files, and any other `VirtualFile`
/// implementation.
pub struct VirtualFileWriter {
    file: Arc<dyn VirtualFile>,
    buf_size: usize,
}

impl VirtualFileWriter {
    /// Create a writer that uses 32 KB buffers (the historical default).
    pub fn new(file: Arc<dyn VirtualFile>) -> Self {
        Self {
            file,
            // default buffer size is 32 KB
            buf_size: 32 * 1024,
        }
    }

    /// Create a writer with a custom buffer size.
    pub fn with_buffer_size(file: Arc<dyn VirtualFile>, buf_size: usize) -> Self {
        Self { file, buf_size }
    }
}

#[async_trait]
impl CompactWriter for VirtualFileWriter {
    async fn alloc_buffer(&self) -> Result<Box<dyn CompactBuffer>> {
        Ok(Box::new(vec![0u8; self.buf_size]))
    }

    fn buffer_size(&self) -> usize {
        self.buf_size
    }

    async fn write(&self, buf: Box<dyn CompactBuffer>, offset: u64, len: usize) -> Result<()> {
        let data = buf.as_ref().as_ref();
        if len > data.len() {
            bail!("VirtualFileWriter write len exceed buffer range");
        }
        let written = self.file.write_at(offset, &data[..len]).await?;
        if written != len {
            bail!(
                "VirtualFileWriter: short write at offset {offset}: expected {len}, got {written}"
            );
        }
        Ok(())
    }

    /// Direct path: writes the caller's slice without an intermediate copy.
    async fn write_all_at(&self, data: &[u8], offset: u64) -> Result<()> {
        let written = self.file.write_at(offset, data).await?;
        if written != data.len() {
            bail!(
                "VirtualFileWriter: short write at offset {offset}: expected {}, got {written}",
                data.len()
            );
        }
        Ok(())
    }
}
