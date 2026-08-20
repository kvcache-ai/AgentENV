use anyhow::{anyhow, bail, ensure, Result};
use async_trait::async_trait;
use bytes::Bytes;
use nix::errno::Errno;
use parking_lot::RwLock;
use std::fmt;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use super::super::{
    align_down, align_up, div_round_up, DEFAULT_MAX_REFILLING, MAX_PREFETCH_SIZE, O_CACHE_ONLY,
    PAGE_SIZE, RW_V2_CACHE_ONLY,
};
use super::cache_entry::{AcquireRefillResult, CacheEntry};
use super::cache_pool::FileCacheBackend;
use crate::io::vfile_io::{CtxRead, DirectRead, FileReader};
use crate::io::virtual_file::{IoCtx, LocalBoxFuture, VirtualFile};

#[derive(Clone, Copy)]
struct ReadCtx {
    source_size: u64,
    open_flags: u32,
    rw_flags: u32,
}

// ---------------------------------------------------------------------------
// RefillGuard — RAII cancellation safety for the acquire/finish protocol
// ---------------------------------------------------------------------------

pub(crate) struct RefillGuard<'a> {
    entry: &'a CacheEntry,
    block_id: u64,
    active_refills: &'a AtomicU32,
}

impl Drop for RefillGuard<'_> {
    fn drop(&mut self) {
        self.active_refills.fetch_sub(1, Ordering::Relaxed);
        self.entry.finish_refill(self.block_id);
    }
}

impl FileCacheBackend {
    async fn do_preadv2_generic<R: FileReader>(
        &self,
        reader: &R,
        cache_id: &str,
        source: Option<&Arc<dyn VirtualFile>>,
        ctx: ReadCtx,
        offset: u64,
        len: usize,
    ) -> Result<Bytes> {
        if len == 0 || offset >= ctx.source_size {
            return Ok(Bytes::new());
        }
        let max_len = ctx.source_size.saturating_sub(offset);
        let want = max_len.min(len as u64).min(usize::MAX as u64) as usize;
        if want == 0 {
            return Ok(Bytes::new());
        }

        let block_size = self.options.block_size;
        let read_end = offset
            .checked_add(want as u64)
            .ok_or_else(|| anyhow!("read range overflow"))?;
        let start_block = offset / block_size;
        let end_block = (read_end - 1) / block_size;
        let cache_only =
            (ctx.rw_flags & RW_V2_CACHE_ONLY) != 0 || (ctx.open_flags & O_CACHE_ONLY) != 0;
        let active_refills = self.active_refills.load(Ordering::Relaxed);
        let allow_refill = active_refills < DEFAULT_MAX_REFILLING;

        // Pre-fill: attempt to refill the range if there are holes.
        let mut did_prefill = false;
        if self.options.capacity_bytes > 0 && !cache_only && allow_refill {
            if let Some(source) = source {
                if let Some(entry) = self.get_cache_entry(cache_id) {
                    if self
                        .query_refill_range_for(&entry, offset, want as u64, ctx.source_size)
                        .is_some()
                    {
                        entry.record_miss();
                    }
                }
                let _ = self
                    .refill_range_generic(
                        reader,
                        cache_id,
                        source,
                        ctx.source_size,
                        offset,
                        want as u64,
                    )
                    .await;
                did_prefill = true;
            }
        }

        // Fast path: single-block read returns a zero-copy Bytes slice
        // directly from the mmap region, avoiding the Vec<u8> allocation.
        if start_block == end_block {
            let block = self
                .try_preadv2_generic(reader, cache_id, source, ctx, !did_prefill, start_block)
                .await?;
            let block_base = start_block * block_size;
            let from = usize::try_from(offset.saturating_sub(block_base)).unwrap_or(0);
            let to = usize::try_from(read_end.saturating_sub(block_base))
                .unwrap_or(block.len())
                .min(block.len());
            ensure!(
                to >= from && to <= block.len(),
                "invalid block slice in single-block read"
            );
            return Ok(block.slice(from..to));
        }

        let mut out = vec![0u8; want];
        let mut written = 0usize;
        for block_id in start_block..=end_block {
            let block = self
                .try_preadv2_generic(reader, cache_id, source, ctx, !did_prefill, block_id)
                .await?;
            let block_base = block_id * block_size;
            let from = if block_id == start_block {
                usize::try_from(offset.saturating_sub(block_base)).unwrap_or(0)
            } else {
                0
            };
            let to = if block_id == end_block {
                usize::try_from(read_end.saturating_sub(block_base)).unwrap_or(block.len())
            } else {
                block.len()
            }
            .min(block.len());
            ensure!(
                to >= from && to <= block.len(),
                "invalid block slice when assembling cached read"
            );
            let seg = &block[from..to];
            let end = written.saturating_add(seg.len()).min(out.len());
            let dst_len = end.saturating_sub(written);
            out[written..end].copy_from_slice(&seg[..dst_len]);
            written = end;
        }

        if written != want {
            if cache_only {
                bail!("short cached read while cache-only flag is set");
            }
            let Some(source) = source else {
                bail!("short cached read without source: got {written}, want {want}");
            };
            let direct = reader.read_bytes(source.as_ref(), offset, want).await?;
            if direct.len() != want {
                bail!(
                    "short read from source at offset {offset}: got {}, want {want}",
                    direct.len()
                );
            }
            return Ok(direct);
        }

        out.truncate(written);
        Ok(Bytes::from(out))
    }

    /// Like [`do_preadv2`](Self::do_preadv2) but writes directly into the
    /// caller-provided `dst` buffer, avoiding the intermediate `Vec<u8>`
    /// allocation in the multi-block path and the `Bytes` wrapper in the
    /// single-block path.
    ///
    // TODO: `do_preadv2` and `do_preadv2_into` share ~120 lines of near-identical
    // logic (refill decision, block iteration, bounds checks, fallback). Consider
    // extracting the shared skeleton into a helper to reduce divergence risk.
    async fn do_preadv2_into_generic<R: FileReader>(
        &self,
        reader: &R,
        cache_id: &str,
        source: Option<&Arc<dyn VirtualFile>>,
        ctx: ReadCtx,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<usize> {
        if dst.is_empty() || offset >= ctx.source_size {
            return Ok(0);
        }
        let max_len = ctx.source_size.saturating_sub(offset);
        let want = max_len.min(dst.len() as u64).min(usize::MAX as u64) as usize;
        if want == 0 {
            return Ok(0);
        }

        let block_size = self.options.block_size;
        let read_end = offset
            .checked_add(want as u64)
            .ok_or_else(|| anyhow!("read range overflow"))?;
        let start_block = offset / block_size;
        let end_block = (read_end - 1) / block_size;
        let cache_only =
            (ctx.rw_flags & RW_V2_CACHE_ONLY) != 0 || (ctx.open_flags & O_CACHE_ONLY) != 0;
        let active_refills = self.active_refills.load(Ordering::Relaxed);
        let allow_refill = active_refills < DEFAULT_MAX_REFILLING;

        // Pre-fill: attempt to refill the range if there are holes.
        let mut did_prefill = false;
        if self.options.capacity_bytes > 0 && !cache_only && allow_refill {
            if let Some(source) = source {
                if let Some(entry) = self.get_cache_entry(cache_id) {
                    if self
                        .query_refill_range_for(&entry, offset, want as u64, ctx.source_size)
                        .is_some()
                    {
                        entry.record_miss();
                    }
                }
                let _ = self
                    .refill_range_generic(
                        reader,
                        cache_id,
                        source,
                        ctx.source_size,
                        offset,
                        want as u64,
                    )
                    .await;
                did_prefill = true;
            }
        }

        let out = &mut dst[..want];
        let mut written = 0usize;
        for block_id in start_block..=end_block {
            let block = self
                .try_preadv2_generic(reader, cache_id, source, ctx, !did_prefill, block_id)
                .await?;
            let block_base = block_id * block_size;
            let from = if block_id == start_block {
                usize::try_from(offset.saturating_sub(block_base)).unwrap_or(0)
            } else {
                0
            };
            let to = if block_id == end_block {
                usize::try_from(read_end.saturating_sub(block_base)).unwrap_or(block.len())
            } else {
                block.len()
            }
            .min(block.len());
            ensure!(
                to >= from && to <= block.len(),
                "invalid block slice when assembling cached read"
            );
            let seg = &block[from..to];
            let end = written.saturating_add(seg.len()).min(out.len());
            let dst_len = end.saturating_sub(written);
            out[written..end].copy_from_slice(&seg[..dst_len]);
            written = end;
        }

        if written != want {
            if cache_only {
                bail!("short cached read while cache-only flag is set");
            }
            let Some(source) = source else {
                bail!("short cached read without source: got {written}, want {want}");
            };
            let n = reader
                .read(source.as_ref(), offset, &mut out[..want])
                .await?;
            if n != want {
                bail!("short read from source at offset {offset}: got {n}, want {want}");
            }
            return Ok(n);
        }

        Ok(written)
    }

    async fn try_preadv2_generic<R: FileReader>(
        &self,
        reader: &R,
        cache_id: &str,
        source: Option<&Arc<dyn VirtualFile>>,
        ctx: ReadCtx,
        enable_refill: bool,
        block_id: u64,
    ) -> Result<Bytes> {
        // Try reading from cache (fast path via bitmap read lock).
        if let Some(entry) = self.get_cache_entry(cache_id) {
            if let Some(bytes) = entry.read_block(block_id)? {
                entry.record_hit();
                return Ok(bytes);
            }
        }

        // Cache miss.
        if let Some(entry) = self.get_cache_entry(cache_id) {
            entry.record_miss();
        }

        let cache_only =
            (ctx.rw_flags & RW_V2_CACHE_ONLY) != 0 || (ctx.open_flags & O_CACHE_ONLY) != 0;

        if self.options.capacity_bytes > 0 {
            let Some(source) = source else {
                bail!("cache miss without source file");
            };
            if !cache_only && enable_refill {
                let _ = self
                    .refill_range_generic(
                        reader,
                        cache_id,
                        source,
                        ctx.source_size,
                        block_id.saturating_mul(self.options.block_size),
                        self.options.block_size,
                    )
                    .await;
            }
            // Retry after refill.
            if let Some(entry) = self.get_cache_entry(cache_id) {
                if let Some(bytes) = entry.read_block(block_id)? {
                    entry.record_hit();
                    return Ok(bytes);
                }
            }
        }

        if cache_only {
            bail!("cache miss while cache-only flag is set");
        } else if let Some(source) = source {
            let block_size_usize = usize::try_from(self.options.block_size)
                .map_err(|_| anyhow!("block_size too large"))?;
            self.pread_from_source_generic(
                reader,
                source,
                ctx.source_size,
                block_id,
                block_size_usize,
            )
            .await
        } else {
            bail!("cache miss without source file");
        }
    }

    async fn pread_from_source_generic<R: FileReader>(
        &self,
        reader: &R,
        source: &Arc<dyn VirtualFile>,
        source_size: u64,
        block_id: u64,
        block_size_usize: usize,
    ) -> Result<Bytes> {
        let block_size = self.options.block_size;
        let offset = block_id
            .checked_mul(block_size)
            .ok_or_else(|| anyhow!("block offset overflow"))?;
        if offset >= source_size {
            return Ok(Bytes::new());
        }
        let want = source_size
            .saturating_sub(offset)
            .min(block_size)
            .min(block_size_usize as u64) as usize;
        let bytes = reader.read_bytes(source.as_ref(), offset, want).await?;
        if bytes.len() != want {
            bail!(
                "short read from source at offset {offset}: got {}, want {want}",
                bytes.len()
            );
        }
        Ok(bytes)
    }

    // -------------------------------------------------------------------
    // Refill range query — uses bitmap instead of BTreeMap
    // -------------------------------------------------------------------

    pub(crate) fn query_refill_range(
        &self,
        cache_id: &str,
        offset: u64,
        size: u64,
        source_size: u64,
    ) -> Option<(u64, u64)> {
        let entry = self.get_cache_entry(cache_id)?;
        self.query_refill_range_for(&entry, offset, size, source_size)
    }

    fn query_refill_range_for(
        &self,
        entry: &CacheEntry,
        offset: u64,
        size: u64,
        source_size: u64,
    ) -> Option<(u64, u64)> {
        // Identify the contiguous hole covering all gaps in the requested
        // range (trimming fully-cached prefix/suffix), aligned to block
        // boundaries.
        if size == 0 || offset >= source_size {
            return None;
        }
        let req_end = offset.checked_add(size)?.min(source_size);
        if req_end <= offset {
            return None;
        }

        let block_size = self.options.block_size;
        let align_left = align_down(offset, block_size);
        let align_right = align_up(req_end, block_size).min(source_size);
        if align_right <= align_left {
            return None;
        }

        let (start_block, end_block) = (
            align_left / block_size,
            div_round_up(align_right, block_size).saturating_sub(1),
        );

        // Build extents from bitmap range query.
        let mut extents = Vec::new();
        {
            let index = entry.index.read();
            for block_id in index.range(start_block as u32..=end_block as u32) {
                let block_id = block_id as u64;
                let extent_start = block_id.saturating_mul(block_size);
                let extent_end = extent_start.saturating_add(block_size).min(source_size);
                if extent_end <= align_left || extent_start >= align_right {
                    continue;
                }
                extents.push((extent_start.max(align_left), extent_end.min(align_right)));
            }
        }

        let mut hole_start = align_left;
        let mut hole_end = align_right;

        // Trim cached suffix: walk extents in reverse, shrinking hole_end
        // past contiguous cached blocks at the right edge.
        for (extent_start, extent_end) in extents.iter().rev() {
            if *extent_start < hole_end {
                if *extent_end >= hole_end {
                    hole_end = *extent_start;
                } else {
                    break;
                }
            }
        }

        // Trim cached prefix: walk extents forward, advancing hole_start
        // past contiguous cached blocks at the left edge.
        for (extent_start, extent_end) in &extents {
            if *extent_end > hole_start {
                if *extent_start <= hole_start {
                    hole_start = *extent_end;
                } else {
                    break;
                }
            }
        }

        if hole_start >= hole_end {
            return None;
        }

        Some((hole_start, hole_end - hole_start))
    }

    // -------------------------------------------------------------------
    // Refill — fetch from source, write blocks to sparse file
    // -------------------------------------------------------------------

    async fn refill_range_generic<R: FileReader>(
        &self,
        reader: &R,
        cache_id: &str,
        source: &Arc<dyn VirtualFile>,
        source_size: u64,
        miss_offset: u64,
        miss_size: u64,
    ) -> Result<()> {
        let Some((refill_off, refill_size)) =
            self.query_refill_range(cache_id, miss_offset, miss_size, source_size)
        else {
            return Ok(());
        };
        self.refill_exact_range_generic(
            reader,
            cache_id,
            source,
            source_size,
            refill_off,
            refill_size,
        )
        .await
    }

    async fn refill_exact_range_generic<R: FileReader>(
        &self,
        reader: &R,
        cache_id: &str,
        source: &Arc<dyn VirtualFile>,
        source_size: u64,
        refill_off: u64,
        refill_size: u64,
    ) -> Result<()> {
        if refill_size == 0 {
            return Ok(());
        }

        let entry = self
            .get_cache_entry(cache_id)
            .ok_or_else(|| anyhow!("cache entry evicted: cache_id={cache_id}"))?;

        let block_size = self.options.block_size;
        let group_start = refill_off / block_size;
        let group_end = div_round_up(refill_off.saturating_add(refill_size), block_size);

        // Use the waker-based acquire pattern per block. The entry-owned read
        // permit spans source I/O, mmap write, bitmap publication, and refill
        // guard cleanup, so logical eviction cannot interleave with a refill.
        for block_id in group_start..group_end {
            let _barrier = entry.refill_eviction_barrier.read().await;
            let result = entry.acquire_refill(block_id).await;
            match result {
                AcquireRefillResult::InCache => continue,
                AcquireRefillResult::ShouldLoad => {
                    self.active_refills.fetch_add(1, Ordering::Relaxed);
                    let _guard = RefillGuard {
                        entry: entry.as_ref(),
                        block_id,
                        active_refills: &self.active_refills,
                    };
                    self.do_refill_block_generic(
                        reader,
                        entry.as_ref(),
                        cache_id,
                        source,
                        source_size,
                        block_id,
                    )
                    .await?;
                    // NOTE: _guard will drop before _barrier, decrementing
                    // active_refills and calling finish_refill while protected.
                }
            }
        }

        // Trigger eviction if over capacity.
        self.eviction_inner().await;
        Ok(())
    }

    /// Fetch a single block from the source directly into the mmap region.
    ///
    /// Uses `read_at_into` to write source data straight into the page cache
    /// via the mmap'd buffer, then marks the block as cached in the bitmap.
    /// This eliminates the intermediate `Bytes` allocation and the `pwrite`
    /// syscall that the old path used.
    async fn do_refill_block_generic<R: FileReader>(
        &self,
        reader: &R,
        entry: &CacheEntry,
        cache_id: &str,
        source: &Arc<dyn VirtualFile>,
        source_size: u64,
        block_id: u64,
    ) -> Result<()> {
        let block_size = self.options.block_size;
        let offset = block_id.saturating_mul(block_size).min(source_size);
        let want = source_size
            .saturating_sub(offset)
            .min(block_size)
            .min(usize::MAX as u64) as usize;
        if want == 0 {
            return Ok(());
        }

        // Check capacity.
        if self.options.capacity_bytes == 0 {
            return Err(Errno::ENOSPC.into());
        }
        if self.state.is_full.load(Ordering::Relaxed) {
            return Err(Errno::ENOSPC.into());
        }

        // The caller holds the entry's refill barrier and passes the stable
        // entry reference. Never look up by cache_id here: that would allow a
        // stale task to populate a replacement entry after an ABA recycle.
        // Get a mutable slice into the mmap region for this block.
        // Safety: the acquire_refill protocol guarantees we are the sole
        // loader for this block_id.
        let buf = unsafe {
            entry
                .mmap_mut_for_block(block_id, source_size)?
                .ok_or_else(|| anyhow!("block offset beyond source size"))?
        };
        let buf_len = buf.len();

        // Read from source directly into the mmap region (zero-copy into page cache).
        let n = reader.read(source.as_ref(), offset, buf).await?;
        if n != buf_len {
            bail!(
                "short refill read for cache_id={cache_id}, offset={offset}, got {n}, want {buf_len}"
            );
        }

        // Just update the bitmap — data is already in place via mmap.
        entry.mark_block_cached(block_id);
        entry.record_refill();

        // Update global capacity and pressure state atomically as a pair.
        self.add_current_bytes(want as u64);

        Ok(())
    }

    // -------------------------------------------------------------------
    // try_refill_range — used by fadvise/prefetch
    // -------------------------------------------------------------------

    async fn try_refill_range(
        &self,
        cache_id: &str,
        source: Option<&Arc<dyn VirtualFile>>,
        source_size: u64,
        offset: u64,
        count: usize,
    ) -> Result<usize> {
        if count == 0 || offset >= source_size {
            return Ok(0);
        }
        let capped = source_size.saturating_sub(offset).min(count as u64) as usize;
        if capped == 0 {
            return Ok(0);
        }
        if self.options.capacity_bytes == 0 {
            return Ok(capped);
        }

        let Some(source) = source else {
            bail!("try_refill_range requires source file on cache miss");
        };

        let end = offset.saturating_add(capped as u64);
        let mut cursor = offset;
        while cursor < end {
            let Some((refill_off, refill_len)) =
                self.query_refill_range(cache_id, cursor, end.saturating_sub(cursor), source_size)
            else {
                break;
            };
            self.refill_exact_range_generic(
                &DirectRead,
                cache_id,
                source,
                source_size,
                refill_off,
                refill_len,
            )
            .await?;
            let next = refill_off.saturating_add(refill_len).min(end);
            if next <= cursor {
                break;
            }
            cursor = next;
        }
        Ok(capped)
    }

    // -------------------------------------------------------------------
    // cache_refill_with_data — write path
    // -------------------------------------------------------------------

    // NOTE: used only in test and write_at interface of CachedFile
    async fn cache_refill_with_data_generic<R: FileReader>(
        &self,
        reader: &R,
        cache_id: &str,
        source: Option<&Arc<dyn VirtualFile>>,
        source_size: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        let write_end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| anyhow!("refill range overflow"))?;
        let block_size = self.options.block_size;
        let block_size_usize =
            usize::try_from(block_size).map_err(|_| anyhow!("block_size too large"))?;
        let start_block = offset / block_size;
        let end_block = (write_end - 1) / block_size;
        let new_size = source_size.max(write_end);

        let entry = self
            .get_cache_entry(cache_id)
            .ok_or_else(|| anyhow!("cache entry evicted: cache_id={cache_id}"))?;
        let _barrier = entry.refill_eviction_barrier.read().await;

        // Ensure data file is large enough.
        if new_size > entry.source_size.load(Ordering::Relaxed) {
            entry.set_source_size(new_size)?;
        }

        let mut bytes_added = 0u64;
        for block_id in start_block..=end_block {
            let block_start = block_id.saturating_mul(block_size);
            let in_write_start = offset.max(block_start);
            let in_write_end = write_end.min(block_start.saturating_add(block_size));
            if in_write_start >= in_write_end {
                continue;
            }

            let mut block = vec![0u8; block_size_usize];
            // Try loading existing cached block.
            if let Some(existing) = entry.read_block(block_id)? {
                let n = existing.len().min(block.len());
                block[..n].copy_from_slice(&existing[..n]);
            } else if let Some(source) = source {
                if block_start < source_size {
                    let want = source_size
                        .saturating_sub(block_start)
                        .min(block_size)
                        .min(block_size_usize as u64) as usize;
                    if want > 0 {
                        let existed = reader
                            .read_bytes(source.as_ref(), block_start, want)
                            .await?;
                        let n = existed.len().min(block.len());
                        block[..n].copy_from_slice(&existed[..n]);
                    }
                }
            }

            let src_begin = usize::try_from(in_write_start.saturating_sub(offset)).unwrap_or(0);
            let src_end = usize::try_from(in_write_end.saturating_sub(offset)).unwrap_or(src_begin);
            let dst_begin =
                usize::try_from(in_write_start.saturating_sub(block_start)).unwrap_or(0);
            let dst_end = dst_begin.saturating_add(src_end.saturating_sub(src_begin));
            ensure!(
                src_begin <= src_end && src_end <= data.len() && dst_end <= block.len(),
                "invalid refill segment boundaries"
            );
            block[dst_begin..dst_end].copy_from_slice(&data[src_begin..src_end]);

            let valid = new_size.saturating_sub(block_start).min(block_size) as usize;
            let valid = valid.min(block.len());
            if valid == 0 {
                continue;
            }

            // Check capacity.
            if self.options.capacity_bytes == 0 {
                return Err(Errno::ENOSPC.into());
            }
            if self.state.is_full.load(Ordering::Relaxed) {
                return Err(Errno::ENOSPC.into());
            }

            let was_cached = entry.index.read().contains(block_id as u32);
            entry.write_block(block_id, &block[..valid])?;
            if !was_cached {
                bytes_added += valid as u64;
            }
        }

        entry.record_refill();

        // Update global capacity and pressure state atomically as a pair.
        if bytes_added > 0 {
            self.add_current_bytes(bytes_added);
        }

        // Trigger eviction check asynchronously.
        self.eviction_inner().await;
        Ok(data.len())
    }

    // -------------------------------------------------------------------
    // set_cached_size
    // -------------------------------------------------------------------

    pub(crate) async fn set_cached_size(&self, cache_id: &str, size: u64) -> Result<()> {
        let entry = match self.get_cache_entry(cache_id) {
            Some(e) => e,
            None => return Ok(()),
        };

        let old_bytes = entry.total_cached_bytes();
        entry.set_source_size(size)?;
        let new_bytes = entry.total_cached_bytes();

        if old_bytes != new_bytes {
            if new_bytes < old_bytes {
                self.subtract_current_bytes(old_bytes - new_bytes);
            } else {
                self.add_current_bytes(new_bytes - old_bytes);
            }
        }
        Ok(())
    }

    // -------------------------------------------------------------------
    // Eviction delegator — whole-file only
    // -------------------------------------------------------------------

    async fn evict_all_blocks(&self, cache_id: &str) -> Result<()> {
        if let Some(entry) = self.get_cache_entry(cache_id) {
            let released = entry.evict_all_blocks().await?;
            if released > 0 {
                self.subtract_current_bytes(released);
            }
        }
        Ok(())
    }

    // -------------------------------------------------------------------
    // Public-ish entry points called from CachedFile.
    //
    // The non-`_with_ctx` variants use the source's ordinary `VirtualFile`
    // methods (default `FileReader`) and yield `Send` futures. Local files use
    // synchronous positional I/O on this fallback path; remote sources remain
    // asynchronous.
    //
    // The `_with_ctx` variants accept an [`IoCtx`] and dispatch source IO
    // through it. The returned future is `!Send` because [`IoCtx`] borrows a
    // `!Send` [`AsyncIoRing`]; callers must run them on a `LocalSet`-backed
    // runtime (typically the ublk queue handler).
    //
    // [`AsyncIoRing`]: storage_util::io_ring::AsyncIoRing
    // -------------------------------------------------------------------

    async fn do_preadv2(
        &self,
        cache_id: &str,
        source: Option<&Arc<dyn VirtualFile>>,
        ctx: ReadCtx,
        offset: u64,
        len: usize,
    ) -> Result<Bytes> {
        self.do_preadv2_generic(&DirectRead, cache_id, source, ctx, offset, len)
            .await
    }

    async fn do_preadv2_with_ctx<'a>(
        &'a self,
        io_ctx: IoCtx<'a>,
        cache_id: &'a str,
        source: Option<&'a Arc<dyn VirtualFile>>,
        ctx: ReadCtx,
        offset: u64,
        len: usize,
    ) -> Result<Bytes> {
        self.do_preadv2_generic(&CtxRead { ctx: io_ctx }, cache_id, source, ctx, offset, len)
            .await
    }

    async fn do_preadv2_into(
        &self,
        cache_id: &str,
        source: Option<&Arc<dyn VirtualFile>>,
        ctx: ReadCtx,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<usize> {
        self.do_preadv2_into_generic(&DirectRead, cache_id, source, ctx, offset, dst)
            .await
    }

    async fn do_preadv2_into_with_ctx<'a>(
        &'a self,
        io_ctx: IoCtx<'a>,
        cache_id: &'a str,
        source: Option<&'a Arc<dyn VirtualFile>>,
        ctx: ReadCtx,
        offset: u64,
        dst: &'a mut [u8],
    ) -> Result<usize> {
        self.do_preadv2_into_generic(&CtxRead { ctx: io_ctx }, cache_id, source, ctx, offset, dst)
            .await
    }

    async fn cache_refill_with_data(
        &self,
        cache_id: &str,
        source: Option<&Arc<dyn VirtualFile>>,
        source_size: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<usize> {
        self.cache_refill_with_data_generic(
            &DirectRead,
            cache_id,
            source,
            source_size,
            offset,
            data,
        )
        .await
    }

    async fn cache_refill_with_data_with_ctx<'a>(
        &'a self,
        io_ctx: IoCtx<'a>,
        cache_id: &'a str,
        source: Option<&'a Arc<dyn VirtualFile>>,
        source_size: u64,
        offset: u64,
        data: &'a [u8],
    ) -> Result<usize> {
        self.cache_refill_with_data_generic(
            &CtxRead { ctx: io_ctx },
            cache_id,
            source,
            source_size,
            offset,
            data,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// CachedFile — per-open handle (unchanged public API)
// ---------------------------------------------------------------------------

pub struct CachedFile {
    pub(crate) backend: FileCacheBackend,
    pub(crate) source: Arc<RwLock<Option<Arc<dyn VirtualFile>>>>,
    pub(crate) cache_id: String,
    pub(crate) source_size: AtomicU64,
    pub(crate) open_flags: u32,
}

impl fmt::Debug for CachedFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedFile")
            .field("cache_id", &self.cache_id)
            .field("source_size", &self.source_size.load(Ordering::Relaxed))
            .field("open_flags", &format_args!("0x{:08x}", self.open_flags))
            .finish_non_exhaustive()
    }
}

impl Drop for CachedFile {
    fn drop(&mut self) {
        self.backend.remove_open_file(&self.cache_id);
    }
}

impl CachedFile {
    fn current_source(&self) -> Option<Arc<dyn VirtualFile>> {
        self.source.read().clone()
    }

    pub fn get_source(&self) -> Option<Arc<dyn VirtualFile>> {
        self.current_source()
    }

    pub fn set_source(&self, source: Option<Arc<dyn VirtualFile>>) {
        *self.source.write() = source;
    }

    pub(crate) fn cache_id(&self) -> &str {
        &self.cache_id
    }

    pub(crate) fn background_source_snapshot(&self) -> Result<(Arc<dyn VirtualFile>, u64)> {
        let source = self
            .current_source()
            .ok_or_else(|| anyhow!("background cache download requires source file"))?;
        Ok((source, self.source_size.load(Ordering::Acquire)))
    }

    pub(crate) fn background_block_size(&self) -> u64 {
        self.backend.options.block_size
    }

    fn background_block_count(&self, source_size: u64) -> Result<u64> {
        let block_count = div_round_up(source_size, self.background_block_size());
        ensure!(
            block_count <= u64::from(u32::MAX) + 1,
            "background cache block count {block_count} exceeds bitmap range"
        );
        Ok(block_count)
    }

    pub(crate) fn missing_background_blocks(&self, source_size: u64) -> Result<Vec<u64>> {
        let block_count = self.background_block_count(source_size)?;
        let index = self
            .backend
            .get_cache_entry(&self.cache_id)
            .ok_or_else(|| anyhow!("cache entry evicted: cache_id={}", self.cache_id))?
            .index
            .read()
            .clone();
        (0..block_count)
            .filter_map(|block_id| {
                let bitmap_id = match u32::try_from(block_id) {
                    Ok(bitmap_id) => bitmap_id,
                    Err(error) => return Some(Err(error.into())),
                };
                if !index.contains(bitmap_id) {
                    Some(Ok(block_id))
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn background_is_complete(&self, source_size: u64) -> Result<bool> {
        Ok(self.missing_background_blocks(source_size)?.is_empty())
    }

    /// Enumerate download chunks (`blocks_per_chunk` cache blocks each) that
    /// still have at least one missing block, as `(start_block, len_blocks)`
    /// pairs; the final chunk may be shorter. The download chunk is the
    /// background fetch granularity (`download.blockSize`) expressed in cache
    /// blocks — the cache itself keeps storing and exposing blocks in its own
    /// smaller block size.
    pub(crate) fn missing_background_chunks(
        &self,
        source_size: u64,
        blocks_per_chunk: u32,
    ) -> Result<Vec<(u64, u32)>> {
        let blocks_per_chunk = u64::from(blocks_per_chunk.max(1));
        let block_count = self.background_block_count(source_size)?;
        let entry = self
            .backend
            .get_cache_entry(&self.cache_id)
            .ok_or_else(|| anyhow!("cache entry evicted: cache_id={}", self.cache_id))?;
        let index = entry.index.read();
        let mut chunks = Vec::new();
        let mut start = 0u64;
        while start < block_count {
            let len = (block_count - start).min(blocks_per_chunk);
            let has_missing = (start..start + len).any(|block_id| !index.contains(block_id as u32));
            if has_missing {
                chunks.push((start, u32::try_from(len)?));
            }
            start += len;
        }
        Ok(chunks)
    }

    /// Refill a range of cache blocks from the source with as few requests as
    /// possible: missing blocks are acquired through the shared loader
    /// election, then each contiguous run of acquired blocks is fetched with
    /// one source read and published per block. Blocks already cached (or
    /// completed by a concurrent loader while we waited) are never rewritten.
    /// Returns the number of bytes read from the source.
    ///
    /// The caller owns pacing (gate, block slots, timeouts); this function
    /// owns loader election, the eviction barrier, and bitmap publication.
    pub(crate) async fn background_refill_range(
        &self,
        source: &Arc<dyn VirtualFile>,
        source_size: u64,
        start_block: u64,
        len_blocks: u32,
    ) -> Result<u64> {
        let end_block = start_block
            .checked_add(u64::from(len_blocks))
            .ok_or_else(|| anyhow!("background cache block range overflow"))?;
        ensure!(
            end_block <= self.background_block_count(source_size)?,
            "background cache block range out of bounds"
        );

        // Back off while the cache is full: the task-level retry loop turns
        // this into a bounded ENOSPC wait instead of churning eviction.
        if self.backend.options.capacity_bytes == 0
            || self.backend.state.is_full.load(Ordering::Relaxed)
        {
            return Err(Errno::ENOSPC.into());
        }

        // Loader election for the whole range before any source I/O. The
        // barrier blocks logical eviction until every acquired block is
        // published or released.
        let entry = self
            .backend
            .get_cache_entry(&self.cache_id)
            .ok_or_else(|| anyhow!("cache entry evicted: cache_id={}", self.cache_id))?;
        let _barrier = entry.refill_eviction_barrier.read().await;
        let mut runs: Vec<Vec<(u64, RefillGuard<'_>)>> = Vec::new();
        for block_id in start_block..end_block {
            match entry.acquire_refill(block_id).await {
                AcquireRefillResult::InCache => {}
                AcquireRefillResult::ShouldLoad => {
                    self.backend.active_refills.fetch_add(1, Ordering::Relaxed);
                    let guard = RefillGuard {
                        entry: entry.as_ref(),
                        block_id,
                        active_refills: &self.backend.active_refills,
                    };
                    match runs.last_mut() {
                        Some(last) if last.last().expect("non-empty run").0 + 1 == block_id => {
                            last.push((block_id, guard))
                        }
                        _ => runs.push(vec![(block_id, guard)]),
                    }
                }
            }
        }

        // Fetch each contiguous run with a single source read. A failed run
        // releases its blocks for a later retry (guards drop) while runs
        // already written stay published.
        let mut read_bytes = 0u64;
        for run in runs {
            read_bytes += self
                .refill_block_run(&entry, source, source_size, run)
                .await?;
        }
        Ok(read_bytes)
    }

    /// Read one contiguous run of loader-owned blocks in a single source
    /// request into task scratch, then copy per block into the mmap and
    /// publish. Guards are consumed so each block's election is released
    /// right after publication. A failed or timed-out read never touches the
    /// mmap, so no unaccounted physical pages remain; capacity accounting is
    /// batched into one update per run.
    async fn refill_block_run(
        &self,
        entry: &Arc<CacheEntry>,
        source: &Arc<dyn VirtualFile>,
        source_size: u64,
        run: Vec<(u64, RefillGuard<'_>)>,
    ) -> Result<u64> {
        let block_size = self.background_block_size();
        let first = run.first().expect("non-empty run").0;
        let last = run.last().expect("non-empty run").0;
        let offset = first
            .checked_mul(block_size)
            .ok_or_else(|| anyhow!("background cache block offset overflow"))?;
        let read_len = source_size
            .saturating_sub(offset)
            .min((last - first + 1) * block_size);
        if read_len == 0 {
            return Ok(0);
        }
        let mut scratch =
            vec![0u8; usize::try_from(read_len).map_err(|_| anyhow!("chunk too large"))?];
        let n = source.read_at_into(offset, &mut scratch).await?;
        ensure!(
            n as u64 == read_len,
            "short background chunk read at offset {offset}: got {n}, want {read_len}"
        );
        let mut committed = 0u64;
        for (block_id, guard) in run {
            let want = source_size
                .saturating_sub(block_id * block_size)
                .min(block_size) as usize;
            if want == 0 {
                continue;
            }
            let block_off = usize::try_from((block_id - first) * block_size)?;
            // Safety: this task is the elected loader for every block in the
            // run, so writing the mmap directly is race-free.
            let dst = unsafe {
                entry
                    .mmap_mut_for_block(block_id, source_size)?
                    .ok_or_else(|| anyhow!("background cache block offset beyond source size"))?
            };
            dst[..want].copy_from_slice(&scratch[block_off..block_off + want]);
            entry.mark_block_cached(block_id);
            entry.record_refill();
            committed += want as u64;
            drop(guard);
        }
        if committed > 0 {
            self.backend.add_current_bytes(committed);
        }
        Ok(read_len)
    }

    pub async fn query(&self, offset: u64, count: usize) -> Result<u64> {
        let size = self.tryget_size(offset, count).await?;
        if count == 0 || offset >= size {
            return Ok(0);
        }
        Ok(self
            .backend
            .query_refill_range(&self.cache_id, offset, count as u64, size)
            .map(|(_, missing)| missing)
            .unwrap_or(0))
    }

    fn update_size_after_write(&self, end: u64) {
        let mut old = self.source_size.load(Ordering::Relaxed);
        while end > old {
            match self.source_size.compare_exchange_weak(
                old,
                end,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(cur) => old = cur,
            }
        }
    }

    async fn tryget_size(&self, offset: u64, count: usize) -> Result<u64> {
        let mut size = self.source_size.load(Ordering::Relaxed);
        let req_end = offset.saturating_add(count as u64);
        let source = self.current_source();
        if (offset >= size || req_end > size) && (self.open_flags & O_CACHE_ONLY) == 0 {
            if let Some(source) = source {
                let latest = source.size().await?;
                if latest != size {
                    self.backend.set_cached_size(&self.cache_id, latest).await?;
                    self.source_size.store(latest, Ordering::Relaxed);
                    size = latest;
                }
            }
        }
        Ok(size)
    }

    // NOTE: only used during test
    pub async fn refill_from_slice(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let size = self.source_size.load(Ordering::Relaxed);
        let source = self.current_source();
        let written = self
            .backend
            .cache_refill_with_data(&self.cache_id, source.as_ref(), size, offset, data)
            .await?;
        if written > 0 {
            self.update_size_after_write(offset.saturating_add(written as u64));
        }
        Ok(written)
    }

    pub async fn try_refill_range(&self, offset: u64, count: usize) -> Result<usize> {
        let size = self.source_size.load(Ordering::Relaxed);
        let source = self.current_source();
        self.backend
            .try_refill_range(&self.cache_id, source.as_ref(), size, offset, count)
            .await
    }

    pub async fn refill_range(&self, offset: u64, count: usize) -> Result<usize> {
        self.try_refill_range(offset, count).await
    }

    pub async fn fadvise(&self, offset: u64, len: u64, advice: i32) -> Result<()> {
        if advice != libc::POSIX_FADV_WILLNEED {
            bail!("advice {advice} is not supported");
        }

        let page_size = 4096;
        let aligned_off = offset / page_size * page_size;
        let mut end = offset.saturating_add(len);
        if !end.is_multiple_of(page_size) {
            end = end
                .checked_add(page_size - 1)
                .ok_or_else(|| anyhow!("fadvise range overflow"))?
                / page_size
                * page_size;
        }
        if end <= aligned_off {
            return Ok(());
        }

        let mut cur = aligned_off;
        while cur < end {
            let step = (end - cur).min(MAX_PREFETCH_SIZE);
            let ret = self.try_refill_range(cur, step as usize).await?;
            if ret == 0 {
                break;
            }
            cur = cur.saturating_add(ret as u64);
            if (ret as u64) < step {
                break;
            }
        }
        Ok(())
    }

    pub async fn read_at_with_flags(
        &self,
        offset: u64,
        len: usize,
        rw_flags: u32,
    ) -> Result<Bytes> {
        let size = self.tryget_size(offset, len).await?;
        let source = self.current_source();
        self.backend
            .do_preadv2(
                &self.cache_id,
                source.as_ref(),
                ReadCtx {
                    source_size: size,
                    open_flags: self.open_flags,
                    rw_flags,
                },
                offset,
                len,
            )
            .await
    }

    pub async fn write_at_with_flags(
        &self,
        offset: u64,
        data: &[u8],
        _rw_flags: u32,
    ) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        let source_size = self.tryget_size(offset, data.len()).await?;
        let write_end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| anyhow!("write range overflow"))?;

        ensure!(
            offset.is_multiple_of(PAGE_SIZE),
            "offset {offset} is not aligned to {PAGE_SIZE}"
        );

        let write_len = {
            if offset >= source_size {
                return Ok(0);
            }
            if !(data.len() as u64).is_multiple_of(PAGE_SIZE) && write_end < source_size {
                bail!("size {} is not aligned to {}", data.len(), PAGE_SIZE);
            }
            source_size.saturating_sub(offset).min(data.len() as u64) as usize
        };

        if write_len == 0 {
            return Ok(0);
        }
        let source = self.current_source();
        let written = self
            .backend
            .cache_refill_with_data(
                &self.cache_id,
                source.as_ref(),
                source_size,
                offset,
                &data[..write_len],
            )
            .await?;
        if written > 0 {
            self.update_size_after_write(offset.saturating_add(written as u64));
        }
        Ok(written)
    }
}

#[async_trait]
impl VirtualFile for CachedFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        self.read_at_with_flags(offset, len, 0).await
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }
        let size = self.tryget_size(offset, dst.len()).await?;
        let source = self.current_source();
        self.backend
            .do_preadv2_into(
                &self.cache_id,
                source.as_ref(),
                ReadCtx {
                    source_size: size,
                    open_flags: self.open_flags,
                    rw_flags: 0,
                },
                offset,
                dst,
            )
            .await
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.write_at_with_flags(offset, data, 0).await
    }

    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let size = self.tryget_size(offset, len).await?;
            let source = self.current_source();
            self.backend
                .do_preadv2_with_ctx(
                    ctx,
                    &self.cache_id,
                    source.as_ref(),
                    ReadCtx {
                        source_size: size,
                        open_flags: self.open_flags,
                        rw_flags: 0,
                    },
                    offset,
                    len,
                )
                .await
        })
    }

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
            let size = self.tryget_size(offset, dst.len()).await?;
            let source = self.current_source();
            self.backend
                .do_preadv2_into_with_ctx(
                    ctx,
                    &self.cache_id,
                    source.as_ref(),
                    ReadCtx {
                        source_size: size,
                        open_flags: self.open_flags,
                        rw_flags: 0,
                    },
                    offset,
                    dst,
                )
                .await
        })
    }

    fn write_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            if data.is_empty() {
                return Ok(0);
            }
            let source_size = self.tryget_size(offset, data.len()).await?;
            let write_end = offset
                .checked_add(data.len() as u64)
                .ok_or_else(|| anyhow!("write range overflow"))?;

            ensure!(
                offset.is_multiple_of(PAGE_SIZE),
                "offset {offset} is not aligned to {PAGE_SIZE}"
            );

            let write_len = {
                if offset >= source_size {
                    return Ok(0);
                }
                if !(data.len() as u64).is_multiple_of(PAGE_SIZE) && write_end < source_size {
                    bail!("size {} is not aligned to {}", data.len(), PAGE_SIZE);
                }
                source_size.saturating_sub(offset).min(data.len() as u64) as usize
            };

            if write_len == 0 {
                return Ok(0);
            }
            let source = self.current_source();
            let written = self
                .backend
                .cache_refill_with_data_with_ctx(
                    ctx,
                    &self.cache_id,
                    source.as_ref(),
                    source_size,
                    offset,
                    &data[..write_len],
                )
                .await?;
            if written > 0 {
                self.update_size_after_write(offset.saturating_add(written as u64));
            }
            Ok(written)
        })
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.source_size.load(Ordering::Relaxed))
    }

    async fn sync(&self) -> Result<()> {
        if let Some(source) = self.current_source() {
            source.sync().await?;
        }
        // Checkpoint metadata on explicit sync.
        if let Some(entry) = self.backend.get_cache_entry(&self.cache_id) {
            let _ = entry.checkpoint().await;
        }
        Ok(())
    }

    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        if let Some(source) = self.current_source() {
            source.seek_data(offset).await
        } else {
            bail!("seek_data is not supported without source");
        }
    }

    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        if let Some(source) = self.current_source() {
            source.seek_hole(offset).await
        } else {
            bail!("seek_hole is not supported without source");
        }
    }

    async fn evict_range(&self, _offset: u64, _len: u64) -> Result<()> {
        // We only support whole-file eviction; partial evict_range
        // delegates to evict_all.
        self.backend.evict_all_blocks(&self.cache_id).await
    }

    async fn evict_all(&self) -> Result<()> {
        self.backend.evict_all_blocks(&self.cache_id).await
    }

    async fn fgetxattr(&self, name: &str) -> Result<Vec<u8>> {
        if let Some(source) = self.current_source() {
            source.fgetxattr(name).await
        } else {
            bail!("fgetxattr is not supported without source");
        }
    }

    async fn flistxattr(&self) -> Result<Vec<String>> {
        if let Some(source) = self.current_source() {
            source.flistxattr().await
        } else {
            bail!("flistxattr is not supported without source");
        }
    }

    async fn fsetxattr(&self, name: &str, value: &[u8], flags: i32) -> Result<()> {
        if let Some(source) = self.current_source() {
            source.fsetxattr(name, value, flags).await
        } else {
            bail!("fsetxattr is not supported without source");
        }
    }

    async fn fremovexattr(&self, name: &str) -> Result<()> {
        if let Some(source) = self.current_source() {
            source.fremovexattr(name).await
        } else {
            bail!("fremovexattr is not supported without source");
        }
    }
}
