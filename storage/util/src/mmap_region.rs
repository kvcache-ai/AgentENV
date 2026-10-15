//! Memory-mapped region abstraction for mapping files into process memory.
//!
//! Provides [`MMapRegion`] for creating `MAP_SHARED` mappings over file
//! descriptors, and [`MMapRegionSlice`] for zero-copy sub-slices that keep
//! the underlying mapping alive via reference counting.

use anyhow::{anyhow, ensure, Context, Result};
use nix::sys::mman::{MapFlags, ProtFlags};
use std::os::fd::AsFd;
use std::sync::Arc;

/// A reference-counted memory-mapped region backed by a file descriptor.
///
/// Created via [`MMapRegion::from_fd`]. The mapping uses `MAP_SHARED` so
/// writes are visible to other mappings of the same file and will eventually
/// be flushed to disk by the kernel.
///
/// Cloning is cheap (Arc bump). The mapping is unmapped when the last
/// reference is dropped.
#[derive(Clone)]
pub struct MMapRegion {
    inner: Arc<MMapRegionInner>,
}

struct MMapRegionInner {
    addr: std::ptr::NonNull<u8>,
    length: usize,
    on_unmap: Option<Box<dyn FnOnce() + Send + Sync + 'static>>,
}

// Safety: the mmap'd region is process-wide memory accessible from any thread.
// Synchronisation of concurrent writes is the caller's responsibility (e.g. the
// acquire/finish refill pattern guarantees at most one writer per block).
// The callback is Send + Sync and runs only during exclusive destruction.
unsafe impl Send for MMapRegionInner {}
unsafe impl Sync for MMapRegionInner {}

/// A zero-copy sub-slice of an [`MMapRegion`].
///
/// Keeps the parent region alive via an internal clone. Implements
/// `AsRef<[u8]>` for read access.
pub struct MMapRegionSlice {
    region: MMapRegion,
    offset: u64,
    length: usize,
}

// Safety: MMapRegionSlice only provides shared (&[u8]) access through AsRef.
// The underlying memory is process-global and the region is kept alive by Arc.
unsafe impl Send for MMapRegionSlice {}
unsafe impl Sync for MMapRegionSlice {}

impl MMapRegion {
    /// Create a new memory-mapped region over `fd` starting at `offset` with
    /// length `len` bytes.
    ///
    /// The mapping is `PROT_READ | PROT_WRITE` and `MAP_SHARED`, so:
    /// - Writes go through to the underlying file (page cache).
    /// - `fsync` on the fd flushes dirty pages to disk.
    ///
    /// # Errors
    /// Returns an error if `len` is zero or the `mmap` syscall fails.
    pub fn from_fd<F: AsFd>(fd: F, offset: u64, len: usize) -> Result<Self> {
        let length =
            std::num::NonZero::new(len).ok_or_else(|| anyhow!("MMapRegion: length must be > 0"))?;
        let addr = unsafe {
            nix::sys::mman::mmap(
                None,
                length,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_FILE | MapFlags::MAP_SHARED,
                fd,
                offset as _,
            )
            .context("mmap failed")?
        };
        Ok(MMapRegion {
            inner: Arc::new(MMapRegionInner {
                addr: addr.cast::<u8>(),
                length: len,
                on_unmap: None,
            }),
        })
    }

    /// Register a callback to run once, after the last reference is dropped
    /// and the mapping is successfully unmapped. The callback runs on the
    /// thread that drops the last reference; it must not block or panic.
    ///
    /// # Errors
    /// Returns an error if the mapping is shared by another region or slice,
    /// or if a callback is already registered. An existing callback is kept.
    pub fn on_unmap(&mut self, callback: impl FnOnce() + Send + Sync + 'static) -> Result<()> {
        let inner = Arc::get_mut(&mut self.inner)
            .context("cannot register on_unmap while the mapping is shared")?;
        ensure!(inner.on_unmap.is_none(), "on_unmap is already registered");
        inner.on_unmap = Some(Box::new(callback));
        Ok(())
    }

    /// Return the total length of the mapped region in bytes.
    pub fn len(&self) -> usize {
        self.inner.length
    }

    /// Return whether the mapped region is empty (length == 0).
    ///
    /// In practice this is always `false` because [`from_fd`](Self::from_fd)
    /// rejects zero-length mappings.
    pub fn is_empty(&self) -> bool {
        self.inner.length == 0
    }

    /// Return a zero-copy read-only sub-slice of this region.
    ///
    /// Returns `None` if `[offset, offset + len)` exceeds the region bounds.
    pub fn subslice(&self, offset: u64, len: usize) -> Option<MMapRegionSlice> {
        if (offset as usize).checked_add(len)? > self.inner.length {
            return None;
        }
        Some(MMapRegionSlice {
            region: self.clone(),
            offset,
            length: len,
        })
    }

    /// Return a mutable slice into the mapped memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - No other reference (mutable or shared) to the same byte range exists
    ///   concurrently. In the cache system this is guaranteed by the
    ///   acquire/finish refill protocol (at most one loader per block).
    /// - The returned slice must not outlive `&self`.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get_mut(&self, offset: u64, len: usize) -> Option<&mut [u8]> {
        if (offset as usize).checked_add(len)? > self.inner.length {
            return None;
        }
        Some(unsafe {
            let start = self.inner.addr.add(offset as usize);
            std::slice::from_raw_parts_mut(start.as_ptr(), len)
        })
    }

    /// Return the raw pointer to the start of the mapped region.
    ///
    /// Provided for advanced use (e.g. `madvise` calls). The pointer is valid
    /// as long as this `MMapRegion` (or any clone) is alive.
    pub fn as_ptr(&self) -> *mut u8 {
        self.inner.addr.as_ptr()
    }
}

impl AsRef<[u8]> for MMapRegionSlice {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            let start = self.region.inner.addr.add(self.offset as usize);
            std::slice::from_raw_parts(start.as_ptr(), self.length)
        }
    }
}

impl Drop for MMapRegionInner {
    fn drop(&mut self) {
        if let Err(err) = unsafe { nix::sys::mman::munmap(self.addr.cast(), self.length) } {
            tracing::error!(?err, "munmap failed when dropping MMapRegionInner");
            return;
        }
        if let Some(callback) = self.on_unmap.take() {
            callback();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_mmap_basic_read_write() {
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        let data = b"hello, mmap world!";
        tmpfile.write_all(data).unwrap();
        tmpfile.as_file().sync_all().unwrap();

        let region = MMapRegion::from_fd(tmpfile.as_file(), 0, data.len()).unwrap();
        assert_eq!(region.len(), data.len());

        // Read via subslice
        let slice = region.subslice(0, data.len()).unwrap();
        assert_eq!(slice.as_ref(), data);

        // Partial subslice
        let partial = region.subslice(7, 4).unwrap();
        assert_eq!(partial.as_ref(), b"mmap");

        // Out-of-bounds subslice returns None
        assert!(region.subslice(0, data.len() + 1).is_none());

        // Write via get_mut
        let buf = unsafe { region.get_mut(0, 5).unwrap() };
        buf.copy_from_slice(b"HELLO");

        let slice = region.subslice(0, 5).unwrap();
        assert_eq!(slice.as_ref(), b"HELLO");
    }

    #[test]
    fn test_on_unmap_runs_once_after_last_reference() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(4096).unwrap();
        let mut region = MMapRegion::from_fd(&file, 0, 4096).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        let owned = String::from("consumed by FnOnce");
        region
            .on_unmap(move || {
                drop(owned);
                callback_calls.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        let clone = region.clone();
        let slice = region.subslice(0, 1).unwrap();
        drop(region);
        drop(clone);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // A slice can keep the mapping alive and release it on another thread.
        std::thread::spawn(move || drop(slice)).join().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_on_unmap_requires_exclusive_ownership() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(4096).unwrap();
        let mut region = MMapRegion::from_fd(&file, 0, 4096).unwrap();
        let clone = region.clone();
        assert!(region.on_unmap(|| panic!("rejected callback ran")).is_err());
        drop(clone);
        let slice = region.subslice(0, 1).unwrap();
        assert!(region.on_unmap(|| panic!("rejected callback ran")).is_err());
        drop(slice);

        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        region
            .on_unmap(move || {
                callback_calls.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        drop(region);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_on_unmap_rejects_replacement() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(4096).unwrap();
        let mut region = MMapRegion::from_fd(&file, 0, 4096).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        region
            .on_unmap(move || {
                callback_calls.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        assert!(region
            .on_unmap(|| panic!("replacement callback ran"))
            .is_err());
        drop(region);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_on_unmap_does_not_run_when_munmap_fails() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(4096).unwrap();
        let mut region = MMapRegion::from_fd(&file, 0, 4096).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        region
            .on_unmap(move || {
                callback_calls.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        let addr = region.inner.addr;
        let len = region.len();
        // Force EINVAL in Drop without unmapping the region prematurely.
        Arc::get_mut(&mut region.inner).unwrap().length = 0;
        drop(region);
        // SAFETY: the failed zero-length munmap left this original mapping live.
        unsafe { nix::sys::mman::munmap(addr.cast(), len) }.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_mmap_zero_length_fails() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let result = MMapRegion::from_fd(tmpfile.as_file(), 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_mmap_clone_keeps_mapping_alive() {
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        tmpfile.write_all(b"test data").unwrap();
        tmpfile.as_file().sync_all().unwrap();

        let region = MMapRegion::from_fd(tmpfile.as_file(), 0, 9).unwrap();
        let slice = region.subslice(0, 4).unwrap();

        // Drop the original region; the slice should still be valid
        // because MMapRegionSlice holds a clone of MMapRegion.
        drop(region);
        assert_eq!(slice.as_ref(), b"test");
    }
}
