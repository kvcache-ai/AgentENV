use super::helper::*;
use super::readonly::LSMTReadOnlyFile;
use super::types::*;
use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::cmp::min;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};
use uuid::Uuid;
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, IntoBytes};

use crate::io::vfile_io::{read_exact, DirectRead, DirectWrite, FileReader, FileWriter};
#[cfg(feature = "io-uring")]
use crate::io::vfile_io::{CtxRead, CtxWrite};
use crate::io::virtual_file::VirtualFile;
#[cfg(feature = "io-uring")]
use crate::io::virtual_file::{IoCtx, LocalBoxFuture};
use crate::lsmt::format::{DiskSegmentMapping, HeaderTrailer};
use crate::lsmt::index::{
    ComboIndex, LogIndex, MutableIndex, ReadOnlyIndex, Segment, SegmentMapping,
};

pub struct LSMTFile {
    pub(super) index: Arc<RwLock<ComboIndex>>,
    /// the layers order is as following:
    /// [top layer, top - 1 layer, ... , bottom layer, rw (i.e., upper) layer].
    /// Top means the newest (the direct parent of upper) and the bottom means the oldest (base layer)
    pub(super) layers: Vec<Arc<dyn VirtualFile>>,
    pub(super) rw_data_file: Arc<dyn VirtualFile>,
    pub(super) rw_index_file: Option<Arc<dyn VirtualFile>>,
    pub(super) virtual_size: AtomicU64,
    rw_tag: usize,
    sealed: AtomicBool,
    pub(super) max_io_size: AtomicUsize,
    /// The order of uuids is similar to [layers][Self::layers]
    pub(super) uuids: Vec<Uuid>,
    file_type: LSMTFileType,
    pub(super) group_commit_size: AtomicUsize,
    group_commit_buf: Arc<Mutex<Vec<DiskSegmentMapping>>>,
    /// In-memory append cursor for the RW data file. This avoids expensive
    /// `size()` metadata calls in log/hybrid write hot paths.
    ///
    /// Log/Hybrid: initialized from the actual file size and advanced after
    /// successful appends. Sparse: initialized to `virtual_size + HEADER_SIZE`
    /// so resize/seal paths can still reason about the physical EOF, but sparse
    /// writes use logical offsets instead of this cursor.
    ///
    /// Invariant: after completed writes, this matches the RW data file EOF.
    rw_data_append_offset: AtomicU64,
    /// In-memory append cursor for the RW index file. Index appends are
    /// serialized by the same index write lock that protects mapping updates.
    ///
    /// Invariant: after completed index writes, this matches the RW index EOF.
    rw_index_append_offset: AtomicU64,
    append_digest: Arc<Mutex<Option<AppendDigestTracker>>>,
}

impl LSMTFile {
    pub fn index(&self) -> Arc<RwLock<ComboIndex>> {
        self.index.clone()
    }

    fn rw_layout_from_header(header: &HeaderTrailer) -> Result<RwLayout> {
        let is_sparse_rw = header.is_sparse_rw();
        let is_hybrid_rw = header.is_hybrid_rw();
        match (is_sparse_rw, is_hybrid_rw) {
            (false, false) => Ok(RwLayout::LogStructured),
            (true, false) => Ok(RwLayout::Sparse),
            (false, true) => Ok(RwLayout::HybridLogStructured),
            (true, true) => bail!("invalid RW header: sparse and hybrid flags are both set"),
        }
    }

    // NOTE: the lower_layers order is as following:
    // [top layer, top - 1 layer, ... , bottom layer].
    // Top means the newest (the direct parent of upper) and the bottom means the oldest (base layer).
    pub async fn open(
        rw_data_file: Arc<dyn VirtualFile>,
        rw_index_file: Option<Arc<dyn VirtualFile>>,
        lower_index: Option<Arc<ReadOnlyIndex>>,
        mut lower_layers: Vec<Arc<dyn VirtualFile>>,
    ) -> Result<Self> {
        let data_file_size = rw_data_file.size().await?;

        let header = verify_ht(&rw_data_file, false, data_file_size).await?;
        let rw_layout = Self::rw_layout_from_header(&header)?.ensure_supported()?;

        if data_file_size >= HEADER_SIZE + 4096
            && verify_ht(&rw_data_file, true, data_file_size).await.is_ok()
        {
            bail!("Cannot open a Sealed LSMT file in RW mode.");
        }

        let rw_tag = lower_layers.len();
        lower_layers.push(rw_data_file.clone());
        let mut uuids = vec![Uuid::nil(); rw_tag];
        let rw_uuid = parse_uuid_field(&header.uuid).unwrap_or_else(Uuid::nil);
        uuids.push(rw_uuid);

        let mut mutable_index = MutableIndex::new();
        let mut rw_index_append_offset = HEADER_SIZE;

        if rw_layout == RwLayout::Sparse {
            // Redundant with the `ensure_supported` above, and kept anyway: the
            // hazard is not "this layout is Sparse", it is "we are about to let
            // an extent map tell us which blocks this layer owns". Guarding the
            // point where that trust is actually placed keeps the check attached
            // to the reason it exists, so a future refactor of the layout
            // plumbing cannot quietly drop it. See
            // `create_mappings_from_sparse` for why its other caller
            // (raw-image packaging) is deliberately not gated.
            ensure!(
                crate::sys::sparse_extents_are_reliable(),
                "cannot recover a sparse upper's index on this platform: it does \
                 not guarantee that unwritten regions are reported as holes, so \
                 the extent map cannot tell us which blocks the upper owns, and \
                 over-reported blocks would mask the lower layers with zeros"
            );
            let mappings = create_mappings_from_sparse(&rw_data_file, HEADER_SIZE).await?;
            for m in mappings {
                mutable_index.insert(m);
            }
        } else if let Some(ref idx_file) = rw_index_file {
            let idx_file_size = idx_file.size().await?;
            rw_index_append_offset = idx_file_size;
            if idx_file_size > HEADER_SIZE {
                let mapping_area_size = idx_file_size - HEADER_SIZE;
                let stride = size_of::<DiskSegmentMapping>() as u64;

                if !mapping_area_size.is_multiple_of(stride) {
                    bail!(
                        "Index file corrupted: size {} is not aligned",
                        mapping_area_size
                    );
                }

                let count = mapping_area_size / stride;
                let mappings =
                    load_index_and_reset_tags(idx_file, HEADER_SIZE, count as usize).await?;

                for m in mappings {
                    mutable_index.insert(m);
                }
            }
        } else {
            bail!("missing index file for non-sparse LSMT open");
        }

        let combo_index = ComboIndex::new(mutable_index, lower_index, rw_tag as u8);
        let file_type = rw_layout.file_type();
        let append_digest = Self::initial_append_digest(&rw_data_file, file_type, data_file_size)
            .await
            .with_context(|| {
                format!("initialize append digest tracker for rw data size {data_file_size}")
            })?;

        Ok(Self {
            index: Arc::new(RwLock::new(combo_index)),
            layers: lower_layers,
            rw_data_file,
            rw_index_file,
            virtual_size: AtomicU64::new(header.virtual_size.get()),
            rw_tag,
            sealed: AtomicBool::new(false),
            max_io_size: AtomicUsize::new(MAX_IO_SIZE),
            uuids,
            file_type,
            group_commit_size: AtomicUsize::new(0),
            group_commit_buf: Arc::new(Mutex::new(Vec::new())),
            rw_data_append_offset: AtomicU64::new(data_file_size),
            rw_index_append_offset: AtomicU64::new(rw_index_append_offset),
            append_digest: Arc::new(Mutex::new(append_digest)),
        })
    }

    pub async fn create(
        rw_data_file: Arc<dyn VirtualFile>,
        rw_index_file: Option<Arc<dyn VirtualFile>>,
        virtual_size: u64,
        sparse_rw: bool,
    ) -> Result<Self> {
        let uuid = Uuid::new_v4();
        // Legacy helper retained for existing tests/callers. Hybrid creation
        // must use create_file_rw/LayerInfo so the full RwLayout is explicit.
        Self::create_with_metadata(
            rw_data_file,
            rw_index_file,
            virtual_size,
            if sparse_rw {
                RwLayout::Sparse
            } else {
                RwLayout::LogStructured
            },
            uuid,
            None,
            None,
        )
        .await
    }

    pub(super) async fn create_with_metadata(
        rw_data_file: Arc<dyn VirtualFile>,
        rw_index_file: Option<Arc<dyn VirtualFile>>,
        virtual_size: u64,
        rw_layout: RwLayout,
        uuid: Uuid,
        parent_uuid: Option<Uuid>,
        user_tag: Option<&[u8]>,
    ) -> Result<Self> {
        let rw_layout = rw_layout.ensure_supported()?;
        let header = rw_header(virtual_size, rw_layout, uuid, parent_uuid, user_tag);
        write_header_block(&rw_data_file, &header).await?;

        match rw_layout {
            RwLayout::Sparse => {
                rw_data_file.truncate(virtual_size + HEADER_SIZE).await?;
            }
            RwLayout::LogStructured | RwLayout::HybridLogStructured => {
                if let Some(ref idx_file) = rw_index_file {
                    let mut idx_header = header;
                    idx_header.set_index_file();
                    idx_header.set_unsealed_index_offset();
                    write_header_block(idx_file, &idx_header).await?;
                } else {
                    bail!("missing index file for non-sparse LSMT create");
                }
            }
        }

        let mutable_index = MutableIndex::new();
        let rw_tag = 0;
        let combo_index = ComboIndex::new(mutable_index, None, 0);

        let file_type = rw_layout.file_type();
        let append_digest = Self::initial_append_digest(&rw_data_file, file_type, HEADER_SIZE)
            .await
            .context("initialize append digest tracker for created rw data")?;

        Ok(Self {
            index: Arc::new(RwLock::new(combo_index)),
            layers: vec![rw_data_file.clone()],
            rw_data_file,
            rw_index_file,
            virtual_size: AtomicU64::new(virtual_size),
            rw_tag,
            sealed: AtomicBool::new(false),
            max_io_size: AtomicUsize::new(MAX_IO_SIZE),
            uuids: vec![uuid],
            file_type,
            group_commit_size: AtomicUsize::new(0),
            group_commit_buf: Arc::new(Mutex::new(Vec::new())),
            rw_data_append_offset: AtomicU64::new(match rw_layout {
                RwLayout::Sparse => virtual_size + HEADER_SIZE,
                RwLayout::LogStructured | RwLayout::HybridLogStructured => HEADER_SIZE,
            }),
            rw_index_append_offset: AtomicU64::new(HEADER_SIZE),
            append_digest: Arc::new(Mutex::new(append_digest)),
        })
    }

    async fn initial_append_digest(
        rw_data_file: &Arc<dyn VirtualFile>,
        file_type: LSMTFileType,
        data_file_size: u64,
    ) -> Result<Option<AppendDigestTracker>> {
        if file_type != LSMTFileType::ReadWrite {
            return Ok(None);
        }
        if data_file_size != HEADER_SIZE {
            return Ok(None);
        }
        AppendDigestTracker::from_header(rw_data_file)
            .await
            .map(Some)
    }

    async fn rebuild_append_digest_from_file(
        rw_data_file: &Arc<dyn VirtualFile>,
        data_file_size: u64,
    ) -> Result<AppendDigestTracker> {
        ensure!(
            data_file_size >= HEADER_SIZE,
            "rw data file is smaller than header while rebuilding append digest"
        );
        let mut tracker = AppendDigestTracker::from_header(rw_data_file).await?;
        let mut offset = HEADER_SIZE;
        let mut buffer = vec![0u8; 128 * 1024];
        while offset < data_file_size {
            let want = min(buffer.len() as u64, data_file_size - offset) as usize;
            let read = rw_data_file
                .read_at_into(offset, &mut buffer[..want])
                .await?;
            ensure!(
                read == want,
                "short read while rebuilding append digest tracker: expected {want}, got {read}"
            );
            tracker.absorb(offset, &buffer[..read])?;
            offset += read as u64;
        }
        Ok(tracker)
    }

    pub fn file_type(&self) -> LSMTFileType {
        self.file_type
    }

    pub fn set_max_io_size(&self, size: usize) -> Result<()> {
        ensure!(
            size != 0 && size.is_multiple_of(ALIGNMENT_4K),
            "size {size} must be non-zero and aligned to 4K"
        );
        self.max_io_size.store(size, Ordering::Release);
        Ok(())
    }

    pub fn get_max_io_size(&self) -> usize {
        self.max_io_size.load(Ordering::Acquire)
    }

    pub fn set_index_group_commit(&self, buffer_size: usize) -> Result<()> {
        self.group_commit_size.store(buffer_size, Ordering::Release);
        Ok(())
    }

    pub fn get_uuid(&self, layer_idx: usize) -> Result<Uuid> {
        self.uuids
            .get(layer_idx)
            .copied()
            .context(format!("layer_idx {layer_idx} out of range"))
    }

    pub fn get_lower_files(&self) -> Vec<Arc<dyn VirtualFile>> {
        self.layers.clone()
    }

    fn data_append_offset(&self) -> Result<u64> {
        let offset = self.rw_data_append_offset.load(Ordering::Acquire);
        ensure!(
            offset.is_multiple_of(ALIGNMENT),
            "Underlying RW data file is not aligned"
        );
        Ok(offset)
    }

    fn advance_data_append_offset(&self, start: u64, len: usize) -> Result<()> {
        let next = start
            .checked_add(len as u64)
            .context("rw data append offset overflow")?;
        // Data appends are structurally serialized by the LSMT index write
        // lock. The CAS is a defensive invariant check: failure means a future
        // code change introduced an unsynchronized append path. Return an
        // error instead of panicking so production callers fail the write
        // without aborting the process.
        self.rw_data_append_offset
            .compare_exchange(start, next, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|actual| {
                anyhow!(
                    "rw data append cursor race: expected {start}, got {actual}; append serialization is broken"
                )
            })?;
        Ok(())
    }

    fn index_append_offset(&self, _idx: &RwLockWriteGuard<'_, ComboIndex>) -> Result<u64> {
        let offset = self.rw_index_append_offset.load(Ordering::Acquire);
        ensure!(
            offset.is_multiple_of(size_of::<DiskSegmentMapping>() as u64),
            "Underlying RW index file is not mapping-aligned"
        );
        Ok(offset)
    }

    fn advance_index_append_offset(
        &self,
        _idx: &RwLockWriteGuard<'_, ComboIndex>,
        start: u64,
        len: usize,
    ) -> Result<()> {
        let next = start
            .checked_add(len as u64)
            .context("rw index append offset overflow")?;
        // Index appends are serialized by the LSMT index write lock. The CAS
        // is a defensive invariant check mirroring the data cursor.
        self.rw_index_append_offset
            .compare_exchange(start, next, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|actual| {
                anyhow!(
                    "rw index append cursor race: expected {start}, got {actual}; append serialization is broken"
                )
            })?;
        Ok(())
    }

    async fn append_index_bytes<W: FileWriter>(
        &self,
        idx: &RwLockWriteGuard<'_, ComboIndex>,
        writer: &W,
        bytes: &[u8],
    ) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if let Some(ref idx_file) = self.rw_index_file {
            let offset = self.index_append_offset(idx)?;
            writer.write(idx_file.as_ref(), offset, bytes).await?;
            self.advance_index_append_offset(idx, offset, bytes.len())?;
        }
        Ok(())
    }

    pub async fn update_vsize(&self, vsize: u64) -> Result<()> {
        // Only the pure append-only log-structured upper maintains an append
        // digest. Sparse and Hybrid can rewrite existing data blocks in place.
        let append_digest = if matches!(self.file_type, LSMTFileType::ReadWrite) {
            Some(self.append_digest.lock().await)
        } else {
            None
        };
        let rw_data_size_before = self.rw_data_append_offset.load(Ordering::Acquire);
        let rebuild_append_digest = append_digest
            .as_ref()
            .and_then(|tracker| tracker.as_ref())
            .is_some_and(|state| {
                state.offset == rw_data_size_before && rw_data_size_before >= HEADER_SIZE
            });
        let mut append_digest = append_digest;
        if let Some(tracker) = append_digest.as_mut() {
            **tracker = None;
        }
        drop(append_digest);

        self.virtual_size.store(vsize, Ordering::Release);
        update_header_vsize(&self.rw_data_file, vsize).await?;
        if rebuild_append_digest {
            let rebuilt =
                Self::rebuild_append_digest_from_file(&self.rw_data_file, rw_data_size_before)
                    .await?;
            let current_size = self.rw_data_append_offset.load(Ordering::Acquire);
            let mut tracker = self.append_digest.lock().await;
            if current_size == rebuilt.offset {
                *tracker = Some(rebuilt);
            }
        }
        if self.file_type == LSMTFileType::SparseReadWrite {
            self.rw_data_file.truncate(vsize + HEADER_SIZE).await?;
            self.rw_data_append_offset
                .store(vsize + HEADER_SIZE, Ordering::Release);
        }
        if let Some(ref idx_file) = self.rw_index_file {
            update_header_vsize(idx_file, vsize).await?;
        }
        Ok(())
    }

    pub async fn data_stat(&self) -> Result<DataStat> {
        let total = self.rw_data_file.size().await?.saturating_sub(HEADER_SIZE);
        let virtual_size = self.virtual_size.load(Ordering::Acquire);
        let query = Segment::new(0, virtual_size.div_ceil(ALIGNMENT) as u32);
        let mut mappings = Vec::new();
        {
            let idx = self.index.read().await;
            idx.lookup(query, &mut mappings);
        }
        let valid_blocks: u64 = mappings
            .iter()
            .map(|m| m.length() as u64 * (!m.zeroed as u64))
            .sum();
        Ok(DataStat {
            total_data_size: total,
            valid_data_size: valid_blocks * ALIGNMENT,
        })
    }

    pub async fn seek_data(&self, begin: u64, end: u64, segs: &mut Vec<Segment>) -> Result<usize> {
        if end <= begin {
            return Ok(0);
        }
        let begin_blk = begin / ALIGNMENT;
        let end_blk = end.div_ceil(ALIGNMENT);
        if end_blk <= begin_blk {
            return Ok(0);
        }
        let query = Segment::new(begin_blk, (end_blk - begin_blk) as u32);
        let mut mappings = Vec::new();
        {
            let idx = self.index.read().await;
            idx.lookup(query, &mut mappings);
        }
        for m in mappings {
            if m.zeroed {
                continue;
            }
            segs.push(Segment::new(m.offset(), m.length()));
        }
        Ok(segs.len())
    }

    pub async fn flatten(&self, dest_file: Arc<dyn VirtualFile>) -> Result<()> {
        self.flatten_with_args(CommitArgs::new(dest_file)).await
    }

    pub async fn flatten_with_args(&self, args: CommitArgs) -> Result<()> {
        let virtual_size = self.virtual_size.load(Ordering::Acquire);
        let query = Segment::new(0, virtual_size.div_ceil(ALIGNMENT) as u32);
        let mut mappings = Vec::new();
        {
            let idx = self.index.read().await;
            idx.lookup(query, &mut mappings);
        }
        compact_to(&self.layers, &mappings, virtual_size, args).await
    }

    pub async fn commit_with_args(&self, args: CommitArgs) -> Result<()> {
        ensure!(
            self.layers.len() <= 1,
            "not supported: commit stacked files"
        );
        self.flush_group_commit().await?;

        let virtual_size = self.virtual_size.load(Ordering::Acquire);
        let query = Segment::new(0, (virtual_size.div_ceil(ALIGNMENT)) as u32);
        let mut mappings = Vec::new();
        {
            let idx = self.index.read().await;
            idx.lookup(query, &mut mappings);
        }
        compact_to(&self.layers, &mappings, virtual_size, args).await
    }

    async fn flush_group_commit(&self) -> Result<()> {
        let idx = self.index.write().await;
        let mut buf = self.group_commit_buf.lock().await;
        if buf.is_empty() {
            return Ok(());
        }
        let mut bytes = Vec::with_capacity(buf.len() * size_of::<DiskSegmentMapping>());
        for m in buf.iter() {
            bytes.extend_from_slice(m.as_bytes());
        }
        self.append_index_bytes(&idx, &DirectWrite, &bytes).await?;
        buf.clear();
        Ok(())
    }

    /// Body shared with [`Self::append_index_mapping`]; the actual index-file
    /// write is routed through `reader` so the ublk write path can keep the
    /// io_uring submission on its own thread via [`CtxWrite`].
    async fn append_index_mapping_generic<W: FileWriter>(
        &self,
        idx: &RwLockWriteGuard<'_, ComboIndex>,
        writer: &W,
        m: DiskSegmentMapping,
    ) -> Result<()> {
        let buffer_size = self.group_commit_size.load(Ordering::Acquire);
        if buffer_size == 0 {
            self.append_index_bytes(idx, writer, m.as_bytes()).await?;
            return Ok(());
        }

        let capacity = (buffer_size / size_of::<DiskSegmentMapping>()).max(1);
        let mut buf = self.group_commit_buf.lock().await;
        buf.push(m);
        if buf.len() >= capacity {
            let mut bytes = Vec::with_capacity(buf.len() * size_of::<DiskSegmentMapping>());
            for x in buf.iter() {
                bytes.extend_from_slice(x.as_bytes());
            }
            self.append_index_bytes(idx, writer, &bytes).await?;
            buf.clear();
        }
        Ok(())
    }

    fn plan_hybrid_write(
        upper: &MutableIndex,
        logical_offset: u64,
        data_len: usize,
    ) -> Result<Vec<WriteFragment>> {
        let start_blk = logical_offset / ALIGNMENT;
        let block_count = (data_len / ALIGNMENT_USIZE) as u32;
        let end_blk = start_blk + u64::from(block_count);
        let mut upper_results = Vec::new();
        upper.lookup(Segment::new(start_blk, block_count), &mut upper_results);

        let mut fragments = Vec::new();
        let mut current_blk = start_blk;

        for m in upper_results {
            if m.offset() > current_blk {
                let gap_blocks = m.offset() - current_blk;
                fragments.push(WriteFragment::Append {
                    logical_offset: current_blk * ALIGNMENT,
                    data_offset: ((current_blk - start_blk) * ALIGNMENT) as usize,
                    len: (gap_blocks * ALIGNMENT) as usize,
                });
                current_blk = m.offset();
            }

            if current_blk >= end_blk {
                break;
            }

            let fragment_end = m.end().min(end_blk);
            if fragment_end <= current_blk {
                continue;
            }

            let fragment_blocks = fragment_end - current_blk;
            let data_offset = ((current_blk - start_blk) * ALIGNMENT) as usize;
            let len = (fragment_blocks * ALIGNMENT) as usize;
            if m.zeroed {
                fragments.push(WriteFragment::Append {
                    logical_offset: current_blk * ALIGNMENT,
                    data_offset,
                    len,
                });
            } else {
                let phys_offset = m
                    .moffset
                    .checked_add(current_blk - m.offset())
                    .context("hybrid write physical offset overflow")?
                    * ALIGNMENT;
                fragments.push(WriteFragment::InPlace {
                    data_offset,
                    len,
                    phys_offset,
                });
            }
            current_blk = fragment_end;
        }

        if current_blk < end_blk {
            let gap_blocks = end_blk - current_blk;
            fragments.push(WriteFragment::Append {
                logical_offset: current_blk * ALIGNMENT,
                data_offset: ((current_blk - start_blk) * ALIGNMENT) as usize,
                len: (gap_blocks * ALIGNMENT) as usize,
            });
        }

        Ok(fragments)
    }

    pub async fn discard_range(&self, offset: u64, len: u64) -> Result<()> {
        ensure!(!self.sealed.load(Ordering::Acquire), "File is sealed.");
        if len == 0 {
            return Ok(());
        }
        ensure!(
            offset.is_multiple_of(ALIGNMENT) && len.is_multiple_of(ALIGNMENT),
            "discard must be aligned to {} bytes (offset: {}, len: {})",
            ALIGNMENT,
            offset,
            len
        );

        let max_discard_bytes = Segment::MAX_LENGTH as u64 * ALIGNMENT;
        let mut remaining = len;
        let mut current_offset = offset;

        while remaining > 0 {
            let step_bytes = remaining.min(max_discard_bytes);
            let step_blocks = (step_bytes / ALIGNMENT) as u32;

            let m_mem = if self.file_type == LSMTFileType::SparseReadWrite {
                let phys_offset = HEADER_SIZE + current_offset;
                self.rw_data_file.discard(phys_offset, step_bytes).await?;
                SegmentMapping::new(
                    current_offset / ALIGNMENT,
                    step_blocks,
                    phys_offset / ALIGNMENT,
                    true,
                    self.rw_tag as u8,
                )
            } else {
                // Match upstream append-only OverlayBD: discard appends only a
                // zeroed index entry at current EOF. Zeroed mappings never read
                // from `moffset`, so no data block is materialized here.
                let phys_offset = self.data_append_offset()?;
                SegmentMapping::new(
                    current_offset / ALIGNMENT,
                    step_blocks,
                    phys_offset / ALIGNMENT,
                    true,
                    self.rw_tag as u8,
                )
            };

            let mut idx = self.index.write().await;
            idx.insert(m_mem);

            if self.file_type != LSMTFileType::SparseReadWrite {
                let mut m_disk = m_mem;
                m_disk.tag = 0;
                let disk_m = DiskSegmentMapping::from_memory(&m_disk);
                self.append_index_mapping_generic(&idx, &DirectWrite, disk_m)
                    .await?;
            }

            current_offset += step_bytes;
            remaining -= step_bytes;
        }

        Ok(())
    }

    async fn write_internal_generic<W: FileWriter>(
        &self,
        writer: &W,
        offset: u64,
        data: &[u8],
    ) -> Result<usize> {
        ensure!(!self.sealed.load(Ordering::Acquire), "File is sealed.");

        let total_len = data.len();
        if total_len == 0 {
            return Ok(0);
        }
        check_alignment(offset, total_len)?;

        let mut remaining_data = data;
        let mut current_offset = offset;
        let mut total_written = 0;
        let max_io_size = self.max_io_size.load(Ordering::Acquire);

        while !remaining_data.is_empty() {
            let chunk_len = std::cmp::min(remaining_data.len(), max_io_size);
            let chunk = &remaining_data[..chunk_len];

            let mut idx = self.index.write().await;

            if self.file_type == LSMTFileType::HybridReadWrite {
                // Hybrid keeps the index write lock through physical I/O by design.
                // In-place fragments are planned from the current upper mappings,
                // readers use the same lock to avoid torn reads, and append fragments
                // need serialized EOF reservation before their mappings are published.
                let fragments = Self::plan_hybrid_write(&idx.upper, current_offset, chunk_len)?;
                let mut inserted_mappings = Vec::new();
                let mut next_append_offset = None;
                for fragment in fragments {
                    match fragment {
                        WriteFragment::InPlace {
                            data_offset,
                            len,
                            phys_offset,
                        } => {
                            writer
                                .write(
                                    self.rw_data_file.as_ref(),
                                    phys_offset,
                                    &chunk[data_offset..data_offset + len],
                                )
                                .await?;
                        }
                        WriteFragment::Append {
                            logical_offset,
                            data_offset,
                            len,
                        } => {
                            let phys_offset = match next_append_offset {
                                Some(offset) => offset,
                                None => self.data_append_offset()?,
                            };
                            writer
                                .write(
                                    self.rw_data_file.as_ref(),
                                    phys_offset,
                                    &chunk[data_offset..data_offset + len],
                                )
                                .await?;
                            self.advance_data_append_offset(phys_offset, len)?;
                            let m_mem = SegmentMapping::new(
                                logical_offset / ALIGNMENT,
                                (len / ALIGNMENT_USIZE) as u32,
                                phys_offset / ALIGNMENT,
                                false,
                                self.rw_tag as u8,
                            );
                            let mut m_disk = m_mem;
                            m_disk.tag = 0;
                            self.append_index_mapping_generic(
                                &idx,
                                writer,
                                DiskSegmentMapping::from_memory(&m_disk),
                            )
                            .await?;
                            inserted_mappings.push(m_mem);
                            next_append_offset = Some(
                                phys_offset
                                    .checked_add(len as u64)
                                    .context("hybrid append offset overflow")?,
                            );
                        }
                    }
                }
                for m_mem in inserted_mappings {
                    idx.insert(m_mem);
                }
                drop(idx);
                remaining_data = &remaining_data[chunk_len..];
                current_offset += chunk_len as u64;
                total_written += chunk_len;
                continue;
            }

            let m_mem = if self.file_type == LSMTFileType::SparseReadWrite {
                let phys_offset = HEADER_SIZE + current_offset;
                writer
                    .write(self.rw_data_file.as_ref(), phys_offset, chunk)
                    .await?;
                SegmentMapping::new(
                    current_offset / ALIGNMENT,
                    (chunk_len / ALIGNMENT_USIZE) as u32,
                    phys_offset / ALIGNMENT,
                    false,
                    self.rw_tag as u8,
                )
            } else {
                let phys_offset = self.data_append_offset()?;
                writer
                    .write(self.rw_data_file.as_ref(), phys_offset, chunk)
                    .await?;
                self.advance_data_append_offset(phys_offset, chunk_len)?;
                let mut tracker = self.append_digest.lock().await;
                if let Some(state) = tracker.as_mut() {
                    if state.absorb(phys_offset, chunk).is_err() {
                        *tracker = None;
                    }
                }
                SegmentMapping::new(
                    current_offset / ALIGNMENT,
                    (chunk_len / ALIGNMENT_USIZE) as u32,
                    phys_offset / ALIGNMENT,
                    false,
                    self.rw_tag as u8,
                )
            };

            let mut m_disk = m_mem;
            m_disk.tag = 0;

            idx.insert(m_mem);
            if self.file_type != LSMTFileType::SparseReadWrite {
                let disk_m = DiskSegmentMapping::from_memory(&m_disk);
                self.append_index_mapping_generic(&idx, writer, disk_m)
                    .await?;
            }

            remaining_data = &remaining_data[chunk_len..];
            current_offset += chunk_len as u64;
            total_written += chunk_len;
        }

        Ok(total_written)
    }

    async fn write_internal(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.write_internal_generic(&DirectWrite, offset, data)
            .await
    }

    #[cfg(feature = "io-uring")]
    async fn write_internal_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: &'a [u8],
    ) -> Result<usize> {
        self.write_internal_generic(&CtxWrite { ctx }, offset, data)
            .await
    }

    pub async fn commit(&self, dest_file: Arc<dyn VirtualFile>) -> Result<()> {
        let args = CommitArgs::new(dest_file);
        self.commit_with_args(args).await
    }

    fn absorb_seal_bytes(tracker: &mut Option<AppendDigestTracker>, offset: u64, data: &[u8]) {
        if let Some(state) = tracker.as_mut() {
            if state.absorb(offset, data).is_err() {
                *tracker = None;
            }
        }
    }

    pub async fn close_seal(&self) -> Result<Option<LayerDescriptor>> {
        ensure!(
            !self.sealed.swap(true, Ordering::SeqCst),
            "File has already been sealed."
        );
        self.flush_group_commit().await?;

        let virtual_size = self.virtual_size.load(Ordering::Acquire);
        let query = Segment::new(0, (virtual_size.div_ceil(ALIGNMENT)) as u32);
        let mut mappings = Vec::new();
        {
            let idx = self.index.read().await;
            idx.lookup(query, &mut mappings);
        }

        let mut compact_index: Vec<SegmentMapping> = Vec::new();
        for m in mappings {
            if m.tag as usize == self.rw_tag {
                let mut cm = m;
                cm.tag = 0;
                compact_index.push(cm);
            }
        }

        let data_file_size = self.rw_data_file.size().await?;
        // Append the sealed index at current data EOF. For sparse RW this EOF is
        // HEADER_SIZE + virtual_size; punching the last virtual block ends
        // exactly at this boundary, so index bytes cannot overlap virtual data.
        let index_offset = data_file_size;

        let mut index_bytes =
            Vec::with_capacity(compact_index.len() * size_of::<DiskSegmentMapping>());
        for m in &compact_index {
            let dm = DiskSegmentMapping::from_memory(m);
            index_bytes.extend_from_slice(dm.as_bytes());
        }

        let remainder = index_bytes.len() % ALIGNMENT_USIZE;
        if remainder != 0 {
            let pad = ALIGNMENT_USIZE - remainder;
            index_bytes.extend(vec![0xff; pad]);
        }

        self.rw_data_file
            .write_at(index_offset, &index_bytes)
            .await?;
        // close_seal flips `sealed` before reaching this point, so this lock
        // only protects digest state while the final seal bytes are appended.
        let mut tracker = self.append_digest.lock().await;
        Self::absorb_seal_bytes(&mut tracker, index_offset, &index_bytes);

        let mut trailer_offset = index_offset + index_bytes.len() as u64;
        if !trailer_offset.is_multiple_of(4096) {
            let pad_len = 4096 - (trailer_offset % 4096);
            let pad = vec![0u8; pad_len as usize];
            self.rw_data_file.write_at(trailer_offset, &pad).await?;
            Self::absorb_seal_bytes(&mut tracker, trailer_offset, &pad);
            trailer_offset += pad_len;
        }

        let header_bytes = self.rw_data_file.read_at(0, HEADER_SIZE as usize).await?;
        ensure!(
            header_bytes.len() >= size_of::<HeaderTrailer>(),
            "failed to read rw data header when sealing"
        );
        let mut trailer =
            HeaderTrailer::read_from_bytes(&header_bytes[..size_of::<HeaderTrailer>()])
                .map_err(|_| anyhow!("invalid rw data header"))?;
        trailer.virtual_size = U64::new(virtual_size);
        trailer.index_offset = U64::new(index_offset);
        trailer.index_size = U64::new(compact_index.len() as u64);

        trailer.set_sealed();
        trailer.set_data_file();
        trailer.set_trailer();

        let mut trailer_buf = vec![0u8; 4096];
        let trailer_bytes_enc = trailer.as_bytes();
        trailer_buf[..trailer_bytes_enc.len()].copy_from_slice(trailer_bytes_enc);

        self.rw_data_file
            .write_at(trailer_offset, &trailer_buf)
            .await?;
        Self::absorb_seal_bytes(&mut tracker, trailer_offset, &trailer_buf);

        Ok(tracker.as_ref().map(AppendDigestTracker::descriptor))
    }

    pub async fn close_seal_and_reopen(
        &self,
    ) -> Result<(LSMTReadOnlyFile, Option<LayerDescriptor>)> {
        let descriptor = self.close_seal().await?;
        let reopened = LSMTReadOnlyFile::open(self.rw_data_file.clone()).await?;
        reopened.set_max_io_size(self.get_max_io_size())?;
        Ok((reopened, descriptor))
    }

    pub fn lower_layer_files_bottom_to_top(&self) -> Vec<Arc<dyn VirtualFile>> {
        self.layers[..self.rw_tag].iter().rev().cloned().collect()
    }

    pub fn get_index_group_commit_size(&self) -> usize {
        self.group_commit_size.load(Ordering::Acquire)
    }

    /// Export the current RW layer's dirty blocks into a separate sealed LSMT file.
    ///
    /// Only the upper (RW) layer's mappings are read from the ComboIndex and
    /// written in dense format (header + data + index + trailer) via `args.writer`.
    /// The output is a valid single-layer sealed LSMT file, openable by
    /// `LSMTReadOnlyFile::open`.
    ///
    /// The source LSMTFile is NOT modified — it remains writable after this call.
    pub async fn export_upper_as_sealed(&self, mut args: CommitArgs) -> Result<()> {
        self.flush_group_commit().await?;

        let virtual_size = self.virtual_size.load(Ordering::Acquire);
        let rw_mappings = {
            let idx = self.index.read().await;
            idx.upper.dump()
        };
        // The snapshot's parent is the topmost (newest) lower layer.
        // After open_files_ro + stack_files, self.uuids is ordered as:
        //   [newest_lower, ..., oldest_lower, upper_rw]
        // So uuids[0] is the topmost lower layer — the RW layer's
        // immediate parent in the UUID chain.
        // For a single-layer file (len < 2), there is no parent.
        args.parent_uuid = if self.uuids.len() >= 2 {
            Some(self.uuids[0])
        } else {
            None
        };

        compact_to(&self.layers, &rw_mappings, virtual_size, args).await
    }

    /// Core read logic that reads directly into the caller-provided buffer,
    /// avoiding intermediate allocations. Each layer segment is read via
    /// `read_at_into` so that data flows directly into `dst`.
    async fn read_internal_into_generic<R: FileReader>(
        &self,
        reader: &R,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<usize> {
        check_alignment(offset, dst.len())?;
        let virtual_size = self.virtual_size.load(Ordering::Acquire);

        if offset >= virtual_size {
            return Ok(0);
        }
        let read_len = min(dst.len() as u64, virtual_size - offset) as usize;
        if read_len == 0 {
            return Ok(0);
        }

        let buf = &mut dst[..read_len];
        // Slice the read into MAX_IO_SIZE chunks (aligned with the C++ implementation).
        let mut remaining = read_len;
        let mut curr_offset = offset;
        let mut buf_pos = 0;
        let max_io_size = self.max_io_size.load(Ordering::Acquire);

        while remaining > 0 {
            let step = min(remaining, max_io_size);

            let begin_blk = curr_offset / ALIGNMENT;
            let count_blk = (step as u64).div_ceil(ALIGNMENT);
            let query = Segment::new(begin_blk, count_blk as u32);

            let mut mappings: Vec<SegmentMapping> = Vec::new();
            if self.file_type == LSMTFileType::HybridReadWrite {
                let idx = self.index.read().await;
                idx.lookup(query, &mut mappings);
                let reads_current_upper = mappings
                    .iter()
                    .any(|m| !m.zeroed && m.tag as usize == self.rw_tag);
                if !reads_current_upper {
                    drop(idx);
                }
                // Keep the read lock across physical I/O when reading current
                // upper data. Hybrid in-place writes hold the write lock while
                // mutating that data, so this prevents torn reads.
                self.read_mappings_into_generic(
                    reader, mappings, begin_blk, count_blk, step, buf_pos, buf,
                )
                .await?;
            } else {
                {
                    let idx = self.index.read().await;
                    idx.lookup(query, &mut mappings);
                }
                self.read_mappings_into_generic(
                    reader, mappings, begin_blk, count_blk, step, buf_pos, buf,
                )
                .await?;
            }

            remaining -= step;
            curr_offset += step as u64;
            buf_pos += step;
        }

        Ok(read_len)
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_mappings_into_generic<R: FileReader>(
        &self,
        reader: &R,
        mappings: Vec<SegmentMapping>,
        begin_blk: u64,
        count_blk: u64,
        step: usize,
        buf_pos: usize,
        buf: &mut [u8],
    ) -> Result<()> {
        let mut current_blk = begin_blk;
        let end_blk = begin_blk + count_blk;

        for m in mappings {
            if m.offset() > current_blk {
                let hole_blks = m.offset() - current_blk;
                let hole_bytes = (hole_blks * ALIGNMENT) as usize;
                let local_buf_pos = buf_pos + ((current_blk - begin_blk) * ALIGNMENT) as usize;

                let fill_len = min(hole_bytes, step.saturating_sub(local_buf_pos - buf_pos));
                if fill_len > 0 {
                    buf[local_buf_pos..local_buf_pos + fill_len].fill(0);
                }
                current_blk = m.offset();
            }

            if current_blk >= end_blk {
                break;
            }

            let local_buf_pos = buf_pos + ((current_blk - begin_blk) * ALIGNMENT) as usize;
            if local_buf_pos - buf_pos >= step {
                break;
            }

            let chunk_bytes = (m.length() as u64 * ALIGNMENT) as usize;
            let actual_read_len = min(chunk_bytes, step - (local_buf_pos - buf_pos));

            if m.zeroed {
                buf[local_buf_pos..local_buf_pos + actual_read_len].fill(0);
            } else {
                let phys_offset = m.moffset * ALIGNMENT;
                let layer_idx = m.tag as usize;

                if let Some(layer) = self.layers.get(layer_idx) {
                    read_exact(
                        reader,
                        layer.as_ref(),
                        phys_offset,
                        &mut buf[local_buf_pos..local_buf_pos + actual_read_len],
                    )
                    .await?;
                } else {
                    bail!("Invalid layer tag {}", layer_idx);
                }
            }
            current_blk = m.end();
        }

        if current_blk < end_blk {
            let local_buf_pos = buf_pos + ((current_blk - begin_blk) * ALIGNMENT) as usize;
            if local_buf_pos - buf_pos < step {
                buf[local_buf_pos..buf_pos + step].fill(0);
            }
        }

        Ok(())
    }

    async fn read_internal_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.read_internal_into_generic(&DirectRead, offset, dst)
            .await
    }

    #[cfg(feature = "io-uring")]
    async fn read_internal_into_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> Result<usize> {
        self.read_internal_into_generic(&CtxRead { ctx }, offset, dst)
            .await
    }
}

#[async_trait]
impl VirtualFile for LSMTFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let virtual_size = self.virtual_size.load(Ordering::Acquire);
        if len == 0 || offset >= virtual_size {
            return Ok(Bytes::new());
        }
        let read_len = min(len as u64, virtual_size - offset) as usize;
        // NOTE: zeroed() means hole regions will be zeroed twice (once here, once
        // in read_internal_into). This is acceptable because read_at is not the
        // hot path in ublk — that uses read_at_into which avoids this allocation
        // entirely.
        let mut buffer = BytesMut::zeroed(read_len);
        let n = self.read_internal_into(offset, &mut buffer).await?;
        buffer.truncate(n);
        Ok(buffer.freeze())
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }
        self.read_internal_into(offset, dst).await
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.write_internal(offset, data).await
    }

    async fn discard(&self, offset: u64, len: u64) -> Result<()> {
        self.discard_range(offset, len).await
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.virtual_size.load(Ordering::Acquire))
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        ensure!(!self.sealed.load(Ordering::Acquire), "File is sealed.");
        self.flush_group_commit().await?;
        self.update_vsize(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.flush_group_commit().await?;
        self.rw_data_file.sync().await?;
        if let Some(ref idx_file) = self.rw_index_file {
            idx_file.sync().await?;
        }
        Ok(())
    }

    #[cfg(feature = "io-uring")]
    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let virtual_size = self.virtual_size.load(Ordering::Acquire);
            if len == 0 || offset >= virtual_size {
                return Ok(Bytes::new());
            }
            let read_len = min(len as u64, virtual_size - offset) as usize;
            let mut buffer = BytesMut::zeroed(read_len);
            let n = self
                .read_internal_into_with_ctx(ctx, offset, &mut buffer)
                .await?;
            buffer.truncate(n);
            Ok(buffer.freeze())
        })
    }

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
            self.read_internal_into_with_ctx(ctx, offset, dst).await
        })
    }

    #[cfg(feature = "io-uring")]
    fn write_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(self.write_internal_with_ctx(ctx, offset, data))
    }
}
