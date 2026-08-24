//! Strategy types that let generic code drive IO through a [`VirtualFile`]
//! while choosing between the plain methods and their `_with_ctx` counterparts
//! at the call site.
//!
//! Two implementations of each trait exist:
//! - [`DirectRead`] / [`DirectWrite`] dispatch to [`VirtualFile::read_at_into`]
//!   / [`VirtualFile::write_at`]. The resulting futures are `Send`, so
//!   background callers that submit work to a Tokio multi-thread runtime keep
//!   working unchanged.
//! - [`CtxRead`] / [`CtxWrite`] dispatch to the `_with_ctx` variants. The
//!   captured [`IoCtx`] borrows a `!Send` [`AsyncIoRing`], so the future is
//!   also `!Send`; this matches the ublk queue handler, which runs on a
//!   `LocalSet` and submits SQEs to its own thread-local ring.
//!
//! The trait+ZST approach (instead of an `AsyncFn` closure) is required: async
//! closures that accept `&dyn VirtualFile` cannot produce HRTB-friendly
//! `AsyncFn` impls today.
//!
//! [`AsyncIoRing`]: storage_util::io_ring::AsyncIoRing

#[cfg(feature = "io-uring")]
use crate::io::virtual_file::IoCtx;
use crate::io::virtual_file::VirtualFile;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use std::future::Future;

/// Reads bytes out of a [`VirtualFile`]. Implementations decide whether to
/// dispatch through the non-ctx [`VirtualFile`] methods or the ctx-aware
/// `_with_ctx` variants.
pub trait FileReader {
    /// Read into the caller-provided buffer. Mirrors
    /// [`VirtualFile::read_at_into`].
    fn read<'a>(
        &'a self,
        file: &'a dyn VirtualFile,
        offset: u64,
        dst: &'a mut [u8],
    ) -> impl Future<Output = Result<usize>> + 'a;

    /// Read and return an owned [`Bytes`]. Mirrors [`VirtualFile::read_at`].
    fn read_bytes<'a>(
        &'a self,
        file: &'a dyn VirtualFile,
        offset: u64,
        len: usize,
    ) -> impl Future<Output = Result<Bytes>> + 'a;
}

/// Writes bytes into a [`VirtualFile`]. Mirrors [`FileReader`] for the write
/// path.
pub trait FileWriter {
    fn write<'a>(
        &'a self,
        file: &'a dyn VirtualFile,
        offset: u64,
        data: &'a [u8],
    ) -> impl Future<Output = Result<usize>> + 'a;
}

/// Direct read strategy — dispatches to [`VirtualFile::read_at_into`].
/// Produces a `Send` future so background callers can `tokio::spawn` it.
pub struct DirectRead;

impl FileReader for DirectRead {
    fn read<'a>(
        &'a self,
        file: &'a dyn VirtualFile,
        offset: u64,
        dst: &'a mut [u8],
    ) -> impl Future<Output = Result<usize>> + 'a {
        file.read_at_into(offset, dst)
    }

    fn read_bytes<'a>(
        &'a self,
        file: &'a dyn VirtualFile,
        offset: u64,
        len: usize,
    ) -> impl Future<Output = Result<Bytes>> + 'a {
        file.read_at(offset, len)
    }
}

/// Context-aware read strategy — dispatches to
/// [`VirtualFile::read_at_into_with_ctx`]. The captured [`IoCtx`] makes the
/// future `!Send`; use this on the ublk queue thread.
#[cfg(feature = "io-uring")]
pub struct CtxRead<'c> {
    pub ctx: IoCtx<'c>,
}

#[cfg(feature = "io-uring")]
impl<'c> FileReader for CtxRead<'c> {
    fn read<'a>(
        &'a self,
        file: &'a dyn VirtualFile,
        offset: u64,
        dst: &'a mut [u8],
    ) -> impl Future<Output = Result<usize>> + 'a {
        file.read_at_into_with_ctx(self.ctx, offset, dst)
    }

    fn read_bytes<'a>(
        &'a self,
        file: &'a dyn VirtualFile,
        offset: u64,
        len: usize,
    ) -> impl Future<Output = Result<Bytes>> + 'a {
        file.read_at_with_ctx(self.ctx, offset, len)
    }
}

/// Direct write strategy — dispatches to [`VirtualFile::write_at`].
pub struct DirectWrite;

impl FileWriter for DirectWrite {
    fn write<'a>(
        &'a self,
        file: &'a dyn VirtualFile,
        offset: u64,
        data: &'a [u8],
    ) -> impl Future<Output = Result<usize>> + 'a {
        file.write_at(offset, data)
    }
}

/// Context-aware write strategy — dispatches to
/// [`VirtualFile::write_at_with_ctx`].
#[cfg(feature = "io-uring")]
pub struct CtxWrite<'c> {
    pub ctx: IoCtx<'c>,
}

#[cfg(feature = "io-uring")]
impl<'c> FileWriter for CtxWrite<'c> {
    fn write<'a>(
        &'a self,
        file: &'a dyn VirtualFile,
        offset: u64,
        data: &'a [u8],
    ) -> impl Future<Output = Result<usize>> + 'a {
        file.write_at_with_ctx(self.ctx, offset, data)
    }
}

/// Read exactly `dst.len()` bytes from `file` starting at `offset` using the
/// given `reader`. Loops over short reads and fails on premature EOF.
pub async fn read_exact<R: FileReader + ?Sized>(
    reader: &R,
    file: &dyn VirtualFile,
    mut offset: u64,
    mut dst: &mut [u8],
) -> Result<()> {
    while !dst.is_empty() {
        let n = reader.read(file, offset, dst).await?;
        if n == 0 {
            bail!("unexpected EOF: still need {} byte(s)", dst.len());
        }
        let (_, rest) = dst.split_at_mut(n);
        dst = rest;
        offset = offset.checked_add(n as u64).context("offset overflow")?;
    }
    Ok(())
}
