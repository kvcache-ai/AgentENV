use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use nix::fcntl::FallocateFlags;
use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use zerocopy::U64;

use super::super::meta::{
    fsync, now_unix_nanos, BlockLoadState, CacheMetaDisk, CacheMetaDiskHeader, EntryPaths,
};
use super::cache_pool::FileCacheBackendOptions;
use storage_util::MMapRegion;

/// The core per-cache-entry object. One per remote file (keyed by cache_id).
///
/// All mutable state is internally synchronised so the struct can be shared
/// via `Arc<CacheFile>` without any external lock.
pub(crate) struct CacheEntry {
    // Immutable identity (set at creation, never changed)
    pub(crate) cache_id: String,
    pub(crate) key: String,
    pub(crate) paths: EntryPaths,
    pub(crate) block_size: u64,

    // Hot: block presence bitmap (checked on every read)
    /// Each bit represents whether a [block_size][Self::block_size] block has been
    /// refilled in the cache.
    pub(crate) index: RwLock<RoaringBitmap>,
    // Hot: per-block inflight dedup (only accessed on miss)
    pub(crate) block_states: Mutex<HashMap<u64, BlockLoadState>>,

    /// Memory-mapped region over the data file for zero-copy I/O
    pub(crate) mem_region: MMapRegion,
    /// Sparse data file (e.g., the backing file of [mem_region][Self::mem_region])
    pub(crate) data_file: std::fs::File,
    // Size (atomic for lock-free reads)
    pub(crate) source_size: AtomicU64,
    // Stats (all AtomicU64 — no mutex needed)
    pub(crate) last_access_nanos: AtomicU64,
    pub(crate) hits: AtomicU64,
    pub(crate) misses: AtomicU64,
    pub(crate) refills: AtomicU64,
    // Dirty flag: set whenever bitmap or metadata changes
    pub(crate) dirty: AtomicBool,
    // Open-file reference count
    pub(crate) open_count: AtomicUsize,
    /// Coordinates block refills with logical eviction of this entry.
    pub(crate) refill_eviction_barrier: tokio::sync::RwLock<()>,
}

impl std::fmt::Debug for CacheEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheEntry")
            .field("cache_id", &self.cache_id)
            .field("key", &self.key)
            .field("source_size", &self.source_size.load(Ordering::Relaxed))
            .field("open_count", &self.open_count.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Result of polling block load state
// ---------------------------------------------------------------------------

pub(crate) enum AcquireRefillResult {
    /// Block is already cached.
    InCache,
    /// We became the loader; proceed to fetch from source.
    ShouldLoad,
}

// ---------------------------------------------------------------------------
// Disk range reservation
// ---------------------------------------------------------------------------

/// Outcome of preallocating a byte range of the sparse data file.
pub(crate) enum RangeAllocation {
    /// Disk blocks are reserved; a punch can roll the range back.
    Reserved,
    /// The filesystem does not support fallocate; nothing was reserved and
    /// writes through the mmap keep the pre-reservation behavior.
    Unsupported,
}

/// RAII guard for a reserved-but-not-yet-published disk range, returned by
/// [`CacheEntry::reserve_range`]. Dropping an armed guard punches the range
/// back to a hole so a failed or cancelled refill leaves no allocated disk
/// blocks that capacity accounting never saw. Call
/// [`commit`](Self::commit) once the covered blocks are published.
pub(crate) struct ReservedRange<'a> {
    entry: &'a CacheEntry,
    offset: u64,
    len: u64,
    armed: bool,
}

impl ReservedRange<'_> {
    /// Keep the reservation. Call this after the covered data is fully
    /// written and before publishing the blocks in the bitmap, so an armed
    /// guard never coexists with published blocks.
    pub(crate) fn commit(mut self) {
        self.armed = false;
    }
}

impl Drop for ReservedRange<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Best-effort: the range was never published, so leaving it allocated
        // only wastes disk until the entry is evicted (whole-file punch).
        if let Err(err) = nix::fcntl::fallocate(
            &self.entry.data_file,
            FallocateFlags::FALLOC_FL_PUNCH_HOLE | FallocateFlags::FALLOC_FL_KEEP_SIZE,
            self.offset as i64,
            self.len as i64,
        ) {
            tracing::warn!(
                cache_id = %self.entry.cache_id,
                offset = self.offset,
                len = self.len,
                %err,
                "failed to punch back reserved cache range"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// AcquireRefillFut — custom Future for block load deduplication
// ---------------------------------------------------------------------------

pub(crate) struct AcquireRefillFut<'a> {
    pub(crate) cache_entry: &'a CacheEntry,
    pub(crate) block_id: u64,
}

impl<'a> Future for AcquireRefillFut<'a> {
    type Output = AcquireRefillResult;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let index = self.cache_entry.index.read();
        if index.contains(self.block_id as u32) {
            return Poll::Ready(AcquireRefillResult::InCache);
        }

        let mut block_states = self.cache_entry.block_states.lock();

        use std::collections::hash_map::Entry;
        match block_states.entry(self.block_id) {
            Entry::Occupied(mut entry) => {
                let waker = ctx.waker();
                let BlockLoadState { wakers } = entry.get_mut();
                if wakers.iter().all(|w| !w.will_wake(waker)) {
                    wakers.push(waker.clone());
                }
                Poll::Pending
            }
            Entry::Vacant(entry) => {
                entry.insert(BlockLoadState { wakers: vec![] });
                Poll::Ready(AcquireRefillResult::ShouldLoad)
            }
        }
    }
}

impl CacheEntry {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new cache entry. Opens/creates the sparse data file and
    /// mmaps it into process memory.
    ///
    /// Returns an error if `source_size` is 0, since mmap requires a
    /// non-zero length.
    pub(crate) fn create(
        cache_id: String,
        key: String,
        source_size: u64,
        options: &FileCacheBackendOptions,
        paths: EntryPaths,
    ) -> Result<Arc<Self>> {
        if source_size == 0 {
            bail!("cannot create cache entry with source_size == 0");
        }

        std::fs::create_dir_all(&paths.dir)?;

        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&paths.data_path)?;

        // Make it a sparse file of the right size.
        data_file.set_len(source_size)?;

        let mem_region = MMapRegion::from_fd(&data_file, 0, source_size as usize)
            .map_err(|e| anyhow!("mmap cache data file: {e}"))?;

        Ok(Arc::new(Self {
            cache_id,
            key,
            paths,
            block_size: options.block_size,
            index: RwLock::new(RoaringBitmap::new()),
            block_states: Mutex::new(HashMap::new()),
            data_file,
            mem_region,
            source_size: AtomicU64::new(source_size),
            last_access_nanos: AtomicU64::new(now_unix_nanos()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            refills: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            open_count: AtomicUsize::new(0),
            refill_eviction_barrier: tokio::sync::RwLock::new(()),
        }))
    }

    /// Reconstruct from persisted state on disk.
    ///
    /// On recovery, the persisted bitmap may be a subset of the actual data on disk
    /// (if a crash happened between a data write and a metadata checkpoint). This is
    /// safe: missing blocks will simply be re-fetched from the source.
    pub(crate) async fn load_from_disk(
        cache_id: String,
        paths: EntryPaths,
        options: &FileCacheBackendOptions,
    ) -> Result<Arc<Self>> {
        // Verify data file exists
        if !paths.meta_path.try_exists()? {
            bail!("cache meta not exists");
        }
        // Load meta.bin
        let meta = match CacheMetaDisk::load_from_path(&paths.meta_path).await {
            Ok(m) => m,
            Err(err) => {
                // Corrupt metadata → discard this entry.
                return Err(err);
            }
        };

        // Verify data file exists
        if !paths.data_path.try_exists()? {
            bail!("cache data file not exists");
        }

        // Open the existing data file.
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&paths.data_path)?;

        // Ensure file size matches source_size.
        let source_size = meta.header.source_size.get();
        let file_len = data_file.metadata()?.len();
        if file_len != source_size {
            bail!("cache data file size {file_len} not match those in meta {source_size}");
        }

        if source_size == 0 {
            bail!("cannot load cache entry with source_size == 0");
        }

        // Reject persisted metadata whose block_size does not match the
        // current configuration.  The bitmap indices are only meaningful
        // under the original block_size; using a different value would
        // map block IDs to wrong file offsets.
        let meta_block_size = meta.header.block_size.get();
        if meta_block_size != options.block_size {
            bail!(
                "cache block_size mismatch: persisted {meta_block_size}, config {}",
                options.block_size
            );
        }

        let mem_region = MMapRegion::from_fd(&data_file, 0, source_size as usize)
            .map_err(|e| anyhow!("mmap cache data file on load: {e}"))?;

        Ok(Arc::new(Self {
            cache_id,
            key: meta.key,
            paths,
            block_size: options.block_size,
            index: RwLock::new(meta.bitmap),
            block_states: Mutex::new(HashMap::new()),
            data_file,
            mem_region,
            source_size: AtomicU64::new(source_size),
            last_access_nanos: AtomicU64::new(meta.header.last_access_unix_nanos.get()),
            hits: AtomicU64::new(meta.header.hits.get()),
            misses: AtomicU64::new(meta.header.misses.get()),
            refills: AtomicU64::new(meta.header.refills.get()),
            dirty: AtomicBool::new(false),
            open_count: AtomicUsize::new(0),
            refill_eviction_barrier: tokio::sync::RwLock::new(()),
        }))
    }

    // -----------------------------------------------------------------------
    // Block I/O — zero-copy via mmap
    // -----------------------------------------------------------------------

    /// Read a cached block. Returns `None` if the block is not in the bitmap.
    ///
    /// The returned `Bytes` is backed by the mmap region (zero-copy). The
    /// underlying mapping is kept alive by reference counting inside the
    /// `MMapRegionSlice` owner.
    pub(crate) fn read_block(&self, block_id: u64) -> Result<Option<Bytes>> {
        if !self.index.read().contains(block_id as u32) {
            return Ok(None);
        }

        let block_size = self.block_size;
        let source_size = self.source_size.load(Ordering::Relaxed);
        let offset = block_id.saturating_mul(block_size);
        if offset >= source_size {
            return Ok(None);
        }

        let want = source_size.saturating_sub(offset).min(block_size) as usize;

        let slice = self.mem_region.subslice(offset, want).ok_or_else(|| {
            anyhow!(
                "mmap subslice out of range: offset={offset}, want={want}, region_len={}",
                self.mem_region.len()
            )
        })?;

        Ok(Some(Bytes::from_owner(slice)))
    }

    /// Reserve disk blocks for `[offset, offset + len)` of the sparse data
    /// file before writing through the mmap region.
    ///
    /// Writing into an unallocated hole of a shared file mapping makes the
    /// kernel allocate blocks at page-fault time; if the filesystem is out of
    /// space that fault fails and the process gets SIGBUS instead of an error
    /// return. Allocating up front turns disk exhaustion into a plain ENOSPC
    /// that the refill path can surface, after which reads fall back to the
    /// source. Filesystems without fallocate support keep the old behavior
    /// and report [`RangeAllocation::Unsupported`].
    ///
    /// Call this directly only when no fallible or cancellable step sits
    /// between the reservation and publication of the covered blocks;
    /// otherwise use [`reserve_range`](Self::reserve_range) so a failure
    /// rolls the reservation back.
    pub(crate) fn ensure_range_allocated(&self, offset: u64, len: u64) -> Result<RangeAllocation> {
        if len == 0 {
            return Ok(RangeAllocation::Unsupported);
        }
        // KEEP_SIZE: allocation must never move the file length; the entry is
        // rejected on reload if the data file size drifts from source_size.
        match nix::fcntl::fallocate(
            &self.data_file,
            FallocateFlags::FALLOC_FL_KEEP_SIZE,
            offset as i64,
            len as i64,
        ) {
            Ok(()) => Ok(RangeAllocation::Reserved),
            Err(nix::errno::Errno::EOPNOTSUPP | nix::errno::Errno::ENOSYS) => {
                Ok(RangeAllocation::Unsupported)
            }
            Err(err) => Err(anyhow!(
                "preallocate cache range failed: offset={offset}, len={len}: {err}"
            )),
        }
    }

    /// Reserve `[offset, offset + len)` and return a guard that punches the
    /// range back to a hole on drop unless [`ReservedRange::commit`] is
    /// called first.
    ///
    /// Use this when a fallible or cancellable step (such as an async source
    /// read into the mmap) runs between reservation and publication: the
    /// guard guarantees a failed refill leaves neither allocated-but-
    /// unaccounted disk blocks nor partially written pages behind.
    ///
    /// # Punch invariant
    ///
    /// While the guard is armed, no block overlapping the range may be
    /// published in the bitmap and the caller must be the sole elected
    /// loader for every covered block — dropping the guard zeroes the range.
    pub(crate) fn reserve_range(&self, offset: u64, len: u64) -> Result<ReservedRange<'_>> {
        let armed = matches!(
            self.ensure_range_allocated(offset, len)?,
            RangeAllocation::Reserved
        );
        Ok(ReservedRange {
            entry: self,
            offset,
            len,
            armed,
        })
    }

    /// Write a block via the mmap region and mark it in the bitmap.
    ///
    /// The block's disk range is preallocated first so a full disk surfaces
    /// as ENOSPC instead of SIGBUS. No rollback is needed: nothing between
    /// the reservation and the bitmap publication below can fail or be
    /// cancelled, and callers may overwrite already cached blocks, which a
    /// punch-based rollback would corrupt.
    ///
    /// # Safety invariant
    /// The acquire/finish refill protocol guarantees at most one writer per
    /// block, so the `get_mut` call does not race with concurrent readers or
    /// writers for the same byte range.
    pub(crate) fn write_block(&self, block_id: u64, data: &[u8]) -> Result<()> {
        let offset = block_id.saturating_mul(self.block_size);
        self.ensure_range_allocated(offset, data.len() as u64)?;

        let buf = unsafe {
            self.mem_region.get_mut(offset, data.len()).ok_or_else(|| {
                anyhow!(
                    "mmap get_mut out of range: offset={offset}, len={}, region_len={}",
                    data.len(),
                    self.mem_region.len()
                )
            })?
        };
        buf.copy_from_slice(data);

        self.mark_block_cached(block_id);
        Ok(())
    }

    /// Evict all cached blocks within this CacheFile.
    ///
    /// Uses `fallocate(PUNCH_HOLE | KEEP_SIZE)` to release disk blocks and
    /// page cache. This avoids munmap/remap while still freeing resources.
    pub(crate) async fn evict_all_blocks(&self) -> Result<u64> {
        // Hold the write side across bitmap clearing, metadata removal, and
        // hole punching so no refill can publish into the entry afterward.
        let _barrier = self.refill_eviction_barrier.write().await;
        let bytes_before = self.total_cached_bytes();

        // Clear the in-memory bitmap before any destructive filesystem
        // operation: from here on every read misses and falls back to the
        // source, so a failure partway can never leave a bitmap that claims
        // cached data whose blocks are already gone.
        self.index.write().clear();
        self.dirty.store(true, Ordering::Relaxed);

        // Filesystem operations can block under disk pressure — exactly when
        // eviction runs — so keep them off the async executor.
        let meta_path = self.paths.meta_path.clone();
        let data_file = self.data_file.try_clone()?;
        let source_size = self.source_size.load(Ordering::Relaxed);
        tokio::task::spawn_blocking(move || -> Result<()> {
            // Remove the on-disk index before punching: a crash after the
            // punch with meta.bin still present would otherwise reload a
            // bitmap claiming blocks the punch already released.
            match std::fs::remove_file(&meta_path) {
                Ok(_) => {}
                Err(err) if err.raw_os_error() == Some(libc::ENOENT) => {}
                Err(err) => return Err(err.into()),
            }
            if source_size > 0 {
                nix::fcntl::fallocate(
                    &data_file,
                    FallocateFlags::FALLOC_FL_PUNCH_HOLE | FallocateFlags::FALLOC_FL_KEEP_SIZE,
                    0,
                    source_size as i64,
                )?;
            }
            Ok(())
        })
        .await??;

        Ok(bytes_before)
    }

    // -----------------------------------------------------------------------
    // Direct mmap access for zero-copy refill
    // -----------------------------------------------------------------------

    /// Return a mutable slice into the mmap region for the given block.
    ///
    /// Used by the refill path to let the source write directly into the
    /// page cache, avoiding an intermediate buffer allocation.
    ///
    /// # Safety
    ///
    /// The caller must ensure exclusive access to this block. In the cache
    /// system this is guaranteed by the acquire_refill/finish_refill protocol
    /// (at most one loader per block).
    ///
    /// The caller must also reserve the block's disk range first (via
    /// [`reserve_range`](Self::reserve_range) or
    /// [`ensure_range_allocated`](Self::ensure_range_allocated)); writing the
    /// returned slice over an unallocated hole raises SIGBUS when the disk
    /// is full.
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn mmap_mut_for_block(
        &self,
        block_id: u64,
        source_size: u64,
    ) -> Result<Option<&mut [u8]>> {
        let offset = block_id.saturating_mul(self.block_size);
        if offset >= source_size {
            return Ok(None);
        }
        let want = (source_size - offset).min(self.block_size) as usize;
        if want == 0 {
            return Ok(None);
        }
        let buf = self.mem_region.get_mut(offset, want).ok_or_else(|| {
            anyhow!(
                "mmap_mut_for_block out of range: offset={offset}, want={want}, region_len={}",
                self.mem_region.len()
            )
        })?;
        Ok(Some(buf))
    }

    /// Mark a block as cached in the bitmap without performing any I/O.
    ///
    /// Used after data has been written directly into the mmap region via
    /// [`mmap_mut_for_block`](Self::mmap_mut_for_block).
    pub(crate) fn mark_block_cached(&self, block_id: u64) {
        self.index.write().insert(block_id as u32);
        self.dirty.store(true, Ordering::Relaxed);
    }

    // -----------------------------------------------------------------------
    // Resize
    // -----------------------------------------------------------------------

    /// Update source_size and truncate blocks beyond the new size.
    pub(crate) fn set_source_size(&self, new_size: u64) -> Result<()> {
        let old_size = self.source_size.load(Ordering::Relaxed);
        if old_size != new_size {
            bail!("CacheEntry does not support change set_source_size");
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Inflight deduplication: waker-based acquire/finish pattern
    // -----------------------------------------------------------------------

    /// Create a future that acquires the right to load `block_id`.
    pub(crate) fn acquire_refill(&self, block_id: u64) -> AcquireRefillFut<'_> {
        AcquireRefillFut {
            cache_entry: self,
            block_id,
        }
    }

    /// Called after a refill attempt completes (success or failure).
    /// Always removes the block from `block_states` and wakes waiters.
    /// On success the bitmap is already set by `do_refill_block`;
    /// on failure/cancellation the block returns to Vacant for retry.
    pub(crate) fn finish_refill(&self, block_id: u64) {
        let wakers = match self.block_states.lock().remove(&block_id) {
            Some(state) => state.wakers,
            None => vec![],
        };
        for w in wakers {
            w.wake();
        }
    }

    // -----------------------------------------------------------------------
    // Metadata & checkpoint
    // -----------------------------------------------------------------------

    /// Total cached bytes according to the bitmap.
    pub(crate) fn total_cached_bytes(&self) -> u64 {
        let index = self.index.read();
        let block_count = index.len();
        let source_size = self.source_size.load(Ordering::Relaxed);
        if block_count == 0 {
            return 0;
        }
        let full_blocks_bytes = block_count
            .saturating_sub(1)
            .saturating_mul(self.block_size);
        let last_block_id = index.max().unwrap_or(0) as u64;
        let last_block_offset = last_block_id.saturating_mul(self.block_size);
        let last_block_bytes = source_size
            .saturating_sub(last_block_offset)
            .min(self.block_size);
        full_blocks_bytes.saturating_add(last_block_bytes)
    }

    /// Write a crash-safe checkpoint for the cache entry (metadata):
    ///   1. Sync the data file (ensures all written blocks are on disk)
    ///   2. Write meta.bin atomically (tmp → fsync → rename)
    ///
    /// After a crash, the persisted bitmap may be a subset of blocks that
    /// are actually on disk. This is safe — the missing blocks will just
    /// be re-fetched from the remote source.
    pub(crate) async fn checkpoint(&self) -> Result<()> {
        // Keep checkpoint publication ordered with logical eviction. Without
        // this permit, an older bitmap snapshot could recreate meta.bin after
        // eviction punched the data file and cleared the in-memory bitmap.
        let _barrier = self.refill_eviction_barrier.read().await;
        let bitmap_snapshot = self.index.read().clone();

        let meta = CacheMetaDisk {
            header: CacheMetaDiskHeader {
                source_size: U64::new(self.source_size.load(Ordering::Relaxed)),
                block_size: U64::new(self.block_size),
                last_access_unix_nanos: U64::new(self.last_access_nanos.load(Ordering::Relaxed)),
                hits: U64::new(self.hits.load(Ordering::Relaxed)),
                misses: U64::new(self.misses.load(Ordering::Relaxed)),
                refills: U64::new(self.refills.load(Ordering::Relaxed)),
                ..Default::default()
            },
            key: self.key.clone(),
            bitmap: bitmap_snapshot,
        };

        // Step 1: fsync data file
        fsync(&self.data_file).await?;
        // Step 2: write meta.bin atomically (tmp → fsync → rename)
        meta.write_to_path(&self.paths.meta_path).await?;
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // LRU / stats helpers
    // -----------------------------------------------------------------------

    pub(crate) fn touch(&self) {
        self.last_access_nanos
            .store(now_unix_nanos(), Ordering::Relaxed);
    }

    pub(crate) fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.touch();
    }

    pub(crate) fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.touch();
    }

    pub(crate) fn record_refill(&self) {
        self.refills.fetch_add(1, Ordering::Relaxed);
        self.touch();
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub(crate) fn last_access(&self) -> u64 {
        self.last_access_nanos.load(Ordering::Relaxed)
    }

    pub(crate) fn stats_snapshot(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.refills.load(Ordering::Relaxed),
        )
    }
}
