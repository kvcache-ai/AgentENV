use super::helper::*;
use super::readonly::LSMTReadOnlyFile;
use super::readwrite::LSMTFile;
use super::stack::{
    create_file_rw, merge_files_ro, open_file_ro, open_file_rw, open_files_ro,
    open_files_ro_with_premerged_cache, stack_files,
};
use super::types::*;
use crate::backend::local::LocalFile;
use crate::io::virtual_file::{IoCtx, LocalBoxFuture, VirtualFile, VirtualFileWriter};
use crate::lsmt::format::{DiskSegmentMapping, HeaderTrailer};
use crate::lsmt::index::{ReadOnlyIndex, Segment, SegmentMapping};
use crate::test_utils::test_io_ring;
use anyhow::{bail, ensure, Result};
use async_trait::async_trait;
use bytes::Bytes;
use rand::{rngs::StdRng, RngExt, SeedableRng};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Weak};
use tempfile::TempDir;
use tokio::sync::{Mutex as TokioMutex, Notify};
use uuid::Uuid;
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, IntoBytes};

fn assert_err_contains(err: &anyhow::Error, needle: &str) {
    let message = err.to_string();
    assert!(
        message.contains(needle),
        "expected error containing `{needle}`, got `{message}`"
    );
}

struct CountingSizeFile {
    inner: Arc<dyn VirtualFile>,
    size_calls: std::sync::atomic::AtomicUsize,
}

impl CountingSizeFile {
    fn new(inner: Arc<dyn VirtualFile>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            size_calls: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn reset_size_calls(&self) {
        self.size_calls
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    fn size_calls(&self) -> usize {
        self.size_calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait]
impl VirtualFile for CountingSizeFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        self.inner.read_at(offset, len).await
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.inner.read_at_into(offset, dst).await
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.inner.write_at(offset, data).await
    }

    async fn write_bytes_at(&self, offset: u64, data: Bytes) -> Result<usize> {
        self.inner.write_bytes_at(offset, data).await
    }

    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        self.inner.read_at_with_ctx(ctx, offset, len)
    }

    fn read_at_into_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        self.inner.read_at_into_with_ctx(ctx, offset, dst)
    }

    fn write_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        self.inner.write_at_with_ctx(ctx, offset, data)
    }

    fn write_bytes_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: Bytes,
    ) -> LocalBoxFuture<'a, Result<usize>> {
        self.inner.write_bytes_at_with_ctx(ctx, offset, data)
    }

    async fn size(&self) -> Result<u64> {
        self.size_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.size().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }

    async fn discard(&self, offset: u64, len: u64) -> Result<()> {
        self.inner.discard(offset, len).await
    }

    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        self.inner.seek_data(offset).await
    }

    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        self.inner.seek_hole(offset).await
    }
}

async fn create_lsmt_env(dir: &TempDir, vsize: u64) -> (Arc<LocalFile>, Arc<LocalFile>, LSMTFile) {
    let ring = test_io_ring();
    let data_path = dir.path().join("data.lsmt");
    let index_path = dir.path().join("index.lsmt");

    let data_file = Arc::new(
        LocalFile::new(&data_path, ring.clone())
            .await
            .expect("create data file"),
    );
    let index_file = Arc::new(
        LocalFile::new(&index_path, ring)
            .await
            .expect("create index file"),
    );

    let lsmt = LSMTFile::create(data_file.clone(), Some(index_file.clone()), vsize, false)
        .await
        .expect("create LSMTFile failed");

    (data_file, index_file, lsmt)
}

async fn open_lsmt_env(
    data_file: Arc<LocalFile>,
    index_file: Arc<LocalFile>,
    lower_layers: Vec<Arc<dyn VirtualFile>>,
) -> Result<LSMTFile> {
    LSMTFile::open(data_file, Some(index_file), None, lower_layers).await
}

async fn assert_unsealed_upper_headers_and_reopen(
    data_file: Arc<LocalFile>,
    index_file: Arc<LocalFile>,
) {
    let data: Arc<dyn VirtualFile> = data_file.clone();
    let index: Arc<dyn VirtualFile> = index_file.clone();
    let data_header = verify_ht(&data, false, data.size().await.unwrap())
        .await
        .unwrap();
    let index_header = verify_ht(&index, false, index.size().await.unwrap())
        .await
        .unwrap();

    assert_eq!(data_header.index_offset.get(), 0);
    assert_eq!(index_header.index_offset.get(), HeaderTrailer::SPACE);
    open_file_rw(data_file, Some(index_file))
        .await
        .expect("reopen unsealed upper");
}

#[tokio::test]
async fn prepared_log_and_hybrid_uppers_use_compatible_index_offset() {
    for mode in [
        crate::config::UpperMode::LogStructured,
        crate::config::UpperMode::HybridLogStructured,
    ] {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("upper.data");
        let index_path = dir.path().join("upper.index");
        crate::image::helper::prepare_runtime_upper(&data_path, Some(&index_path), 8192, mode)
            .unwrap();

        let ring = test_io_ring();
        let data_file = Arc::new(
            LocalFile::open_rw(data_path, false, ring.clone())
                .await
                .unwrap(),
        );
        let index_file = Arc::new(LocalFile::open_rw(index_path, false, ring).await.unwrap());
        assert_unsealed_upper_headers_and_reopen(data_file, index_file).await;
    }
}

#[test]
fn validate_rw_header_pair_paths_accepts_all_layouts() {
    for (mode, layout) in [
        (
            crate::config::UpperMode::LogStructured,
            RwLayout::LogStructured,
        ),
        (
            crate::config::UpperMode::HybridLogStructured,
            RwLayout::HybridLogStructured,
        ),
        (crate::config::UpperMode::Sparse, RwLayout::Sparse),
    ] {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("upper.data");
        let index = dir.path().join("upper.index");
        let index_path = (layout != RwLayout::Sparse).then_some(index.as_path());
        crate::image::helper::prepare_runtime_upper(&data, index_path, 8192, mode).unwrap();
        validate_rw_header_pair_paths(&data, index_path, 8192, layout).unwrap();
    }
}

#[test]
fn validate_rw_header_pair_paths_rejects_header_and_layout_mismatches() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("upper.data");
    let index = dir.path().join("upper.index");
    crate::image::helper::prepare_runtime_upper(
        &data,
        Some(&index),
        8192,
        crate::config::UpperMode::LogStructured,
    )
    .unwrap();

    assert_err_contains(
        &validate_rw_header_pair_paths(&data, Some(&index), 4096, RwLayout::LogStructured)
            .unwrap_err(),
        "virtual size mismatch",
    );
    assert_err_contains(
        &validate_rw_header_pair_paths(&data, Some(&index), 8192, RwLayout::HybridLogStructured)
            .unwrap_err(),
        "layout mismatch",
    );

    let mut index_header = rw_header(8192, RwLayout::LogStructured, Uuid::new_v4(), None, None);
    index_header.set_index_file();
    index_header.set_unsealed_index_offset();
    fs::write(&index, index_header.as_bytes()).unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(&index)
        .unwrap()
        .set_len(HEADER_SIZE)
        .unwrap();
    assert_err_contains(
        &validate_rw_header_pair_paths(&data, Some(&index), 8192, RwLayout::LogStructured)
            .unwrap_err(),
        "UUID mismatch",
    );

    let data_uuid = {
        let bytes = fs::read(&data).unwrap();
        HeaderTrailer::read_from_bytes(&bytes[..size_of::<HeaderTrailer>()])
            .unwrap()
            .uuid
    };
    let mut index_header = rw_header(8192, RwLayout::LogStructured, Uuid::nil(), None, None);
    index_header.uuid = data_uuid;
    index_header.set_index_file();
    index_header.index_offset = U64::new(0);
    fs::write(&index, index_header.as_bytes()).unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(&index)
        .unwrap()
        .set_len(HEADER_SIZE)
        .unwrap();
    assert_err_contains(
        &validate_rw_header_pair_paths(&data, Some(&index), 8192, RwLayout::LogStructured)
            .unwrap_err(),
        "invalid unsealed index offset",
    );

    index_header.set_unsealed_index_offset();
    fs::write(&index, index_header.as_bytes()).unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(&index)
        .unwrap()
        .set_len(HEADER_SIZE)
        .unwrap();
    let mut index_file = OpenOptions::new().append(true).open(&index).unwrap();
    index_file.write_all(&[0]).unwrap();
    assert_err_contains(
        &validate_rw_header_pair_paths(&data, Some(&index), 8192, RwLayout::LogStructured)
            .unwrap_err(),
        "mapping area is misaligned",
    );

    let mut data_file = OpenOptions::new().write(true).open(&data).unwrap();
    data_file.seek(SeekFrom::End(0)).unwrap();
    data_file.write_all(&[0]).unwrap();
    assert_err_contains(
        &validate_rw_header_pair_paths(&data, Some(&index), 8192, RwLayout::LogStructured)
            .unwrap_err(),
        "data file size is not",
    );
}

#[tokio::test]
async fn created_log_and_hybrid_uppers_use_compatible_index_offset() {
    for layout in [RwLayout::LogStructured, RwLayout::HybridLogStructured] {
        let dir = TempDir::new().unwrap();
        let ring = test_io_ring();
        let data_file = Arc::new(
            LocalFile::new(dir.path().join("upper.data"), ring.clone())
                .await
                .unwrap(),
        );
        let index_file = Arc::new(
            LocalFile::new(dir.path().join("upper.index"), ring)
                .await
                .unwrap(),
        );
        let mut args = LayerInfo::new(data_file.clone(), Some(index_file.clone()), 8192);
        args.rw_layout = layout;
        create_file_rw(args).await.unwrap();

        assert_unsealed_upper_headers_and_reopen(data_file, index_file).await;
    }
}

async fn create_sparse_lsmt_env(dir: &TempDir, vsize: u64) -> (Arc<LocalFile>, LSMTFile) {
    let ring = test_io_ring();
    let data_path = dir.path().join("sparse-data.lsmt");
    let data_file = Arc::new(
        LocalFile::new(&data_path, ring)
            .await
            .expect("create sparse data file"),
    );

    let lsmt = LSMTFile::create(data_file.clone(), None, vsize, true)
        .await
        .expect("create sparse LSMTFile failed");

    (data_file, lsmt)
}

async fn create_hybrid_lsmt_env(
    dir: &TempDir,
    vsize: u64,
) -> (Arc<LocalFile>, Arc<LocalFile>, LSMTFile) {
    let ring = test_io_ring();
    let data_path = dir.path().join("hybrid-data.lsmt");
    let index_path = dir.path().join("hybrid-index.lsmt");

    let data_file = Arc::new(
        LocalFile::new(&data_path, ring.clone())
            .await
            .expect("create hybrid data file"),
    );
    let index_file = Arc::new(
        LocalFile::new(&index_path, ring)
            .await
            .expect("create hybrid index file"),
    );

    let mut args = LayerInfo::new(data_file.clone(), Some(index_file.clone()), vsize);
    args.rw_layout = RwLayout::HybridLogStructured;
    let lsmt = create_file_rw(args)
        .await
        .expect("create hybrid LSMTFile failed");

    (data_file, index_file, lsmt)
}

async fn create_blocking_hybrid_lsmt_env(
    dir: &TempDir,
    vsize: u64,
    writes_before_block: usize,
) -> (Arc<BlockingWriteFile>, Arc<LocalFile>, LSMTFile) {
    let ring = test_io_ring();
    let data_path = dir.path().join("hybrid-blocking-data.lsmt");
    let index_path = dir.path().join("hybrid-blocking-index.lsmt");

    let local_data: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::new(&data_path, ring.clone()).await.unwrap());
    let data_file = BlockingWriteFile::new(local_data, writes_before_block);
    let index_file = Arc::new(LocalFile::new(&index_path, ring).await.unwrap());
    let mut args = LayerInfo::new(
        data_file.clone() as Arc<dyn VirtualFile>,
        Some(index_file.clone()),
        vsize,
    );
    args.rw_layout = RwLayout::HybridLogStructured;
    let lsmt = create_file_rw(args).await.unwrap();

    (data_file, index_file, lsmt)
}

async fn create_sealed_layer(
    dir: &TempDir,
    name: &str,
    vsize: u64,
    writes: &[(u64, u8)],
) -> Arc<LocalFile> {
    let ring = test_io_ring();
    let data = Arc::new(
        LocalFile::new(dir.path().join(format!("{name}.data")), ring.clone())
            .await
            .expect("create layer data"),
    );
    let index = Arc::new(
        LocalFile::new(dir.path().join(format!("{name}.index")), ring)
            .await
            .expect("create layer index"),
    );
    let layer = LSMTFile::create(data.clone(), Some(index), vsize, false)
        .await
        .expect("create layer");
    for (offset, byte) in writes {
        layer
            .write_at(*offset, &[*byte; 4096])
            .await
            .expect("write layer data");
    }
    layer.close_seal().await.expect("seal layer");
    drop(layer);
    data
}

async fn update_trailer(file: &Arc<LocalFile>, update: impl FnOnce(&mut HeaderTrailer)) {
    let file_size = file.size().await.unwrap();
    let trailer_offset = file_size - HEADER_SIZE;
    let bytes = file
        .read_at(trailer_offset, size_of::<HeaderTrailer>())
        .await
        .unwrap();
    let mut trailer = HeaderTrailer::read_from_bytes(&bytes).unwrap();
    update(&mut trailer);
    file.write_at(trailer_offset, trailer.as_bytes())
        .await
        .unwrap();
}

#[tokio::test]
async fn rejects_sealed_index_offset_out_of_bounds() {
    let temp_dir = TempDir::new().unwrap();
    let data = create_sealed_layer(&temp_dir, "bad-index-offset", 8192, &[(0, 0x11)]).await;
    let file_size = data.size().await.unwrap();

    update_trailer(&data, |trailer| {
        trailer.index_offset = U64::new(file_size);
    })
    .await;

    let err = open_file_ro(data as Arc<dyn VirtualFile>)
        .await
        .err()
        .expect("malformed index offset should be rejected");
    assert_err_contains(&err, "index offset");
}

#[tokio::test]
async fn rejects_sealed_index_offset_before_header() {
    let temp_dir = TempDir::new().unwrap();
    let data = create_sealed_layer(&temp_dir, "index-before-header", 8192, &[(0, 0x12)]).await;

    update_trailer(&data, |trailer| {
        trailer.index_offset = U64::new(HEADER_SIZE - 1);
    })
    .await;

    let err = open_file_ro(data as Arc<dyn VirtualFile>)
        .await
        .err()
        .expect("index offset inside the header should be rejected");
    assert_err_contains(&err, "index offset");
}

#[tokio::test]
async fn rejects_sealed_index_size_out_of_bounds() {
    let temp_dir = TempDir::new().unwrap();
    let data = create_sealed_layer(&temp_dir, "bad-index-size", 8192, &[(0, 0x22)]).await;
    let file_size = data.size().await.unwrap();
    let trailer = verify_ht(&(data.clone() as Arc<dyn VirtualFile>), true, file_size)
        .await
        .unwrap();
    let stride = size_of::<DiskSegmentMapping>() as u64;
    let remaining = file_size - HEADER_SIZE - trailer.index_offset.get();
    let bad_index_size = remaining / stride + 1;

    update_trailer(&data, |trailer| {
        trailer.index_size = U64::new(bad_index_size);
    })
    .await;

    let err = open_file_ro(data as Arc<dyn VirtualFile>)
        .await
        .err()
        .expect("malformed index size should be rejected");
    assert_err_contains(&err, "index byte length");
}

#[tokio::test]
async fn rejects_index_count_overflow_before_allocation() {
    let temp_dir = TempDir::new().unwrap();
    let data = create_sealed_layer(&temp_dir, "overflow-index", 8192, &[]).await;
    let count = u64::MAX / size_of::<DiskSegmentMapping>() as u64 + 1;
    let err = load_index_and_reset_tags(&(data as Arc<dyn VirtualFile>), HEADER_SIZE, count)
        .await
        .expect_err("overflowing index count should be rejected");
    let message = err.to_string();
    assert!(
        message.contains("overflow") || message.contains("does not fit usize"),
        "expected an overflow or conversion error, got {message}"
    );
}

#[tokio::test]
async fn rejects_index_exceeding_decoded_memory_budget() {
    let temp_dir = TempDir::new().unwrap();
    let data = create_sealed_layer(&temp_dir, "oversized-index", 8192, &[]).await;
    let bytes_per_mapping =
        size_of::<DiskSegmentMapping>() as u64 + size_of::<SegmentMapping>() as u64;
    let count = MAX_INDEX_MEMORY_BYTES as u64 / bytes_per_mapping + 1;
    let err = load_index_and_reset_tags(&(data as Arc<dyn VirtualFile>), HEADER_SIZE, count)
        .await
        .expect_err("decoded index exceeding the memory budget should be rejected");
    assert_err_contains(&err, "exceeds per-layer limit");
}

async fn open_sparse_lsmt_env(
    data_file: Arc<LocalFile>,
    lower_layers: Vec<Arc<dyn VirtualFile>>,
) -> Result<LSMTFile> {
    LSMTFile::open(data_file, None, None, lower_layers).await
}

async fn open_hybrid_lsmt_env(
    data_file: Arc<LocalFile>,
    index_file: Arc<LocalFile>,
    lower_index: Option<Arc<ReadOnlyIndex>>,
    lower_layers: Vec<Arc<dyn VirtualFile>>,
) -> Result<LSMTFile> {
    LSMTFile::open(data_file, Some(index_file), lower_index, lower_layers).await
}

async fn load_base_index(data_file: Arc<LocalFile>) -> ReadOnlyIndex {
    let size = data_file.size().await.unwrap();
    let trailer = verify_ht(&(data_file.clone() as Arc<dyn VirtualFile>), true, size)
        .await
        .unwrap();

    let mappings = load_index_and_reset_tags(
        &(data_file as Arc<dyn VirtualFile>),
        trailer.index_offset.get(),
        trailer.index_size.get(),
    )
    .await
    .expect("Failed to load mappings");

    ReadOnlyIndex::new(mappings)
}

#[derive(Debug)]
struct ShortReadFile {
    data: Vec<u8>,
    trim: usize,
}

impl ShortReadFile {
    fn new(data: Vec<u8>, trim: usize) -> Self {
        Self { data, trim }
    }
}

#[async_trait]
impl VirtualFile for ShortReadFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let offset = offset as usize;
        if offset >= self.data.len() {
            return Ok(Bytes::new());
        }
        let end = self.data.len().min(offset + len);
        let mut out = self.data[offset..end].to_vec();
        if out.len() > self.trim {
            out.truncate(out.len() - self.trim);
        }
        Ok(Bytes::from(out))
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("write unsupported");
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.data.len() as u64)
    }
}

struct FailingIndexReadFile {
    inner: Arc<dyn VirtualFile>,
    index_offset: u64,
    index_len: u64,
}

impl FailingIndexReadFile {
    fn new(inner: Arc<dyn VirtualFile>, index_offset: u64, index_size: u64) -> Self {
        Self {
            inner,
            index_offset,
            index_len: index_size * size_of::<DiskSegmentMapping>() as u64,
        }
    }

    fn overlaps_index(&self, offset: u64, len: usize) -> bool {
        let end = offset.saturating_add(len as u64);
        let index_end = self.index_offset.saturating_add(self.index_len);
        offset < index_end && end > self.index_offset
    }
}

#[async_trait]
impl VirtualFile for FailingIndexReadFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        ensure!(
            !self.overlaps_index(offset, len),
            "unexpected layer index read at offset {offset} len {len}"
        );
        self.inner.read_at(offset, len).await
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        ensure!(
            !self.overlaps_index(offset, dst.len()),
            "unexpected layer index read_at_into at offset {} len {}",
            offset,
            dst.len()
        );
        self.inner.read_at_into(offset, dst).await
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.inner.write_at(offset, data).await
    }

    async fn size(&self) -> Result<u64> {
        self.inner.size().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }
}

struct BlockingWriteFile {
    inner: Arc<dyn VirtualFile>,
    write_count: AtomicUsize,
    writes_before_block: TokioMutex<usize>,
    write_started: Notify,
    release_write: Notify,
}

impl BlockingWriteFile {
    fn new(inner: Arc<dyn VirtualFile>, writes_before_block: usize) -> Arc<Self> {
        Arc::new(Self {
            inner,
            write_count: AtomicUsize::new(0),
            writes_before_block: TokioMutex::new(writes_before_block),
            write_started: Notify::new(),
            release_write: Notify::new(),
        })
    }

    async fn wait_for_blocked_write(&self) {
        self.write_started.notified().await;
    }

    fn release_blocked_write(&self) {
        self.release_write.notify_waiters();
    }
}

#[async_trait]
impl VirtualFile for BlockingWriteFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        self.inner.read_at(offset, len).await
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.inner.read_at_into(offset, dst).await
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.write_count.fetch_add(1, AtomicOrdering::AcqRel);
        let should_block = {
            let mut remaining = self.writes_before_block.lock().await;
            if *remaining == 0 {
                false
            } else {
                *remaining -= 1;
                *remaining == 0
            }
        };
        if should_block {
            self.write_started.notify_waiters();
            self.release_write.notified().await;
        }
        self.inner.write_at(offset, data).await
    }

    async fn size(&self) -> Result<u64> {
        self.inner.size().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }
}

// ---------------------------------------------------------------------------
// SparseMemoryFile — mock VirtualFile with configurable data/hole layout
// Used by create_mappings_from_sparse unit tests.
// ---------------------------------------------------------------------------

/// A mock [`VirtualFile`] that simulates a sparse file.
///
/// `data_ranges` is a sorted, non-overlapping list of `(start, end)` byte
/// ranges where data is present. Everything outside those ranges is a hole.
/// All offsets and lengths must be multiples of [`ALIGNMENT`] (512 bytes).
struct SparseMemoryFile {
    size: u64,
    /// Sorted, non-overlapping `[start, end)` byte ranges that contain data.
    data_ranges: Vec<(u64, u64)>,
}

impl SparseMemoryFile {
    fn new(size: u64, data_ranges: Vec<(u64, u64)>) -> Arc<Self> {
        Arc::new(Self { size, data_ranges })
    }
}

#[async_trait]
impl VirtualFile for SparseMemoryFile {
    async fn read_at(&self, _offset: u64, _len: usize) -> Result<Bytes> {
        bail!("read_at not needed");
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("write_at not needed");
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.size)
    }

    /// Returns the start of the first data range that begins at or after
    /// `offset`. Returns `ENXIO` when no such range exists.
    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        for &(start, end) in &self.data_ranges {
            if end > offset {
                // The range overlaps with [offset, ..); data starts at
                // max(start, offset) but the OS lseek(SEEK_DATA) contract
                // returns the start of the region that contains `offset`,
                // which may be before `offset` if we are already inside a
                // data range.
                return Ok(Some(start.max(offset)));
            }
        }
        Ok(None)
    }

    /// Returns the start of the first hole at or after `offset`.
    /// If the file ends with data (no trailing hole) returns `self.size`.
    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        for &(start, end) in &self.data_ranges {
            if start <= offset && offset < end {
                // `offset` is inside this data range; the next hole begins
                // at the end of this range.
                return Ok(Some(end));
            }
            if start > offset {
                // `offset` is already in a hole before this data range.
                return Ok(Some(offset));
            }
        }
        // `offset` is past all data ranges — it is already in a hole
        // (or at EOF). Return `offset` itself (or self.size, whichever is
        // smaller, matching typical lseek(SEEK_HOLE) semantics).
        Ok(Some(offset.min(self.size)))
    }
}

// ---------------------------------------------------------------------------
// create_mappings_from_sparse unit tests
// ---------------------------------------------------------------------------

/// File smaller than start_offset → empty result.
#[tokio::test]
async fn test_create_mappings_from_sparse_empty_file() {
    // File size (512) < start_offset (HEADER_SIZE = 4096)
    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(512, vec![]);
    let result = create_mappings_from_sparse(&file, HEADER_SIZE)
        .await
        .unwrap();
    assert!(result.is_empty());
}

/// File with no data regions at all → empty result.
#[tokio::test]
async fn test_create_mappings_from_sparse_no_data_regions() {
    // 8 KiB file, entirely holes, start_offset = 0
    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(8 * 1024, vec![]);
    let result = create_mappings_from_sparse(&file, 0).await.unwrap();
    assert!(result.is_empty());
}

/// A single contiguous data region starting exactly at start_offset.
#[tokio::test]
async fn test_create_mappings_from_sparse_single_region_at_start() {
    // Data: [4096, 8192) with start_offset = 4096 (HEADER_SIZE)
    // logical = (4096 - 4096) / 512 = 0
    // physical = 4096 / 512 = 8
    // blocks = (8192 - 4096) / 512 = 8
    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(8192, vec![(HEADER_SIZE, 8192)]);
    let result = create_mappings_from_sparse(&file, HEADER_SIZE)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].offset(), 0); // logical block 0
    assert_eq!(result[0].moffset, 8); // physical block 8 (4096 / 512)
    assert_eq!(result[0].length(), 8); // 8 blocks
    assert!(!result[0].zeroed);
}

/// Multiple disjoint data regions, both after start_offset.
#[tokio::test]
async fn test_create_mappings_from_sparse_multiple_disjoint_regions() {
    // Data: [4096, 8192) and [16384, 20480), start_offset = 4096
    // Region 1: logical=0, physical=8, length=8
    // Region 2: logical=(16384-4096)/512=24, physical=16384/512=32, length=8
    let file: Arc<dyn VirtualFile> =
        SparseMemoryFile::new(32768, vec![(4096, 8192), (16384, 20480)]);
    let result = create_mappings_from_sparse(&file, HEADER_SIZE)
        .await
        .unwrap();
    assert_eq!(result.len(), 2);

    assert_eq!(result[0].offset(), 0);
    assert_eq!(result[0].moffset, 8);
    assert_eq!(result[0].length(), 8);

    assert_eq!(result[1].offset(), 24); // (16384 - 4096) / 512
    assert_eq!(result[1].moffset, 32); // 16384 / 512
    assert_eq!(result[1].length(), 8); // (20480 - 16384) / 512
}

/// Data entirely before start_offset → empty result.
#[tokio::test]
async fn test_create_mappings_from_sparse_data_only_before_start_offset() {
    // Data: [0, 4096), start_offset = 4096
    // The data range ends at start_offset, so seek_data(4096) returns ENXIO.
    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(8192, vec![(0, 4096)]);
    let result = create_mappings_from_sparse(&file, HEADER_SIZE)
        .await
        .unwrap();
    assert!(result.is_empty());
}

/// Data range spans across start_offset (starts before, ends after).
/// The portion before start_offset must be skipped; the portion after
/// becomes logical block 0.
#[tokio::test]
async fn test_create_mappings_from_sparse_data_spanning_start_offset() {
    // Data: [2048, 8192), start_offset = 4096
    // seek_data(4096) returns max(2048, 4096) = 4096 (inside the range)
    // seek_hole(4096) returns end of range = 8192
    // begin=4096 is NOT < start_offset, so we fall through to mapping:
    //   logical = (4096 - 4096) / 512 = 0
    //   physical = 4096 / 512 = 8
    //   blocks   = (8192 - 4096) / 512 = 8
    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(8192, vec![(2048, 8192)]);
    let result = create_mappings_from_sparse(&file, HEADER_SIZE)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].offset(), 0);
    assert_eq!(result[0].moffset, 8);
    assert_eq!(result[0].length(), 8);
}

/// start_offset = 0: physical and logical offsets are identical.
#[tokio::test]
async fn test_create_mappings_from_sparse_start_offset_zero() {
    // Data: [0, 4096), start_offset = 0
    // logical = (0 - 0) / 512 = 0
    // physical = 0 / 512 = 0
    // blocks = (4096 - 0) / 512 = 8
    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(8192, vec![(0, 4096)]);
    let result = create_mappings_from_sparse(&file, 0).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].offset(), 0);
    assert_eq!(result[0].moffset, 0);
    assert_eq!(result[0].length(), 8);
}

/// Unaligned start_offset must be rejected with InvalidInput.
#[tokio::test]
async fn test_create_mappings_from_sparse_unaligned_start_offset_rejected() {
    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(8192, vec![(0, 8192)]);
    let err = create_mappings_from_sparse(&file, 100).await.unwrap_err();
    assert_err_contains(&err, "must be aligned");
}

/// A data region whose size exceeds Segment::MAX_LENGTH blocks must be
/// split into multiple SegmentMappings.
#[tokio::test]
async fn test_create_mappings_from_sparse_large_region_split() {
    // MAX_LENGTH = (1 << 14) - 1 = 16383 blocks = 16383 * 512 = ~8 MB
    // Use a region of MAX_LENGTH + 10 blocks to force a split.
    let max_blocks = Segment::MAX_LENGTH as u64;
    let extra_blocks = 10u64;
    let total_blocks = max_blocks + extra_blocks;
    let region_size = total_blocks * ALIGNMENT;
    let file_size = HEADER_SIZE + region_size;
    let data_range = (HEADER_SIZE, file_size);

    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(file_size, vec![data_range]);
    let result = create_mappings_from_sparse(&file, HEADER_SIZE)
        .await
        .unwrap();

    // Expect exactly 2 mappings: one of MAX_LENGTH blocks, one of extra_blocks.
    assert_eq!(result.len(), 2, "large region should split into 2 mappings");

    assert_eq!(result[0].offset(), 0);
    assert_eq!(result[0].moffset, HEADER_SIZE / ALIGNMENT);
    assert_eq!(result[0].length(), Segment::MAX_LENGTH);

    assert_eq!(result[1].offset(), max_blocks);
    assert_eq!(result[1].moffset, HEADER_SIZE / ALIGNMENT + max_blocks);
    assert_eq!(result[1].length(), extra_blocks as u32);
}

/// Two adjacent data ranges (no hole between them) are reported as separate
/// mappings since each seek_data/seek_hole call returns them individually.
#[tokio::test]
async fn test_create_mappings_from_sparse_adjacent_regions() {
    // Data: [0, 4096) and [4096, 8192) — adjacent, no gap.
    // The mock's seek_hole(0) returns 4096 (end of first range), so the
    // loop iterates twice and produces two mappings.
    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(8192, vec![(0, 4096), (4096, 8192)]);
    let result = create_mappings_from_sparse(&file, 0).await.unwrap();
    assert_eq!(result.len(), 2);

    assert_eq!(result[0].offset(), 0);
    assert_eq!(result[0].moffset, 0);
    assert_eq!(result[0].length(), 8);

    assert_eq!(result[1].offset(), 8);
    assert_eq!(result[1].moffset, 8);
    assert_eq!(result[1].length(), 8);
}

/// Non-zero start_offset shifts logical offsets correctly relative to
/// physical ones.
#[tokio::test]
async fn test_create_mappings_from_sparse_logical_physical_offset_relationship() {
    // Data: [8192, 12288), start_offset = 4096
    // logical = (8192 - 4096) / 512 = 8
    // physical = 8192 / 512 = 16
    // blocks = (12288 - 8192) / 512 = 8
    let file: Arc<dyn VirtualFile> = SparseMemoryFile::new(16384, vec![(8192, 12288)]);
    let result = create_mappings_from_sparse(&file, HEADER_SIZE)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].offset(), 8); // logical block 8
    assert_eq!(result[0].moffset, 16); // physical block 16
    assert_eq!(result[0].length(), 8);
}

#[tokio::test]
async fn test_create_open() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 64 * 1024 * 1024;

    let (f_data, f_index, _) = create_lsmt_env(&temp_dir, vsize).await;

    let lsmt2 = open_lsmt_env(f_data, f_index, vec![]).await.unwrap();
    assert_eq!(lsmt2.size().await.unwrap(), vsize);
}

#[tokio::test]
async fn test_sparse_rw() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 64 * 1024 * 1024;
    let (f_data, lsmt) = create_sparse_lsmt_env(&temp_dir, vsize).await;

    let ops = vec![
        (5, 5),
        (10, 10),
        (20, 10),
        (100, 10),
        (130944 / 512, 128 / 512),
    ];

    let mut rng = StdRng::seed_from_u64(42);

    for (blk_offset, blk_len) in &ops {
        let offset = *blk_offset * 512;
        let len = *blk_len * 512;
        let mut buf = vec![0u8; len as usize];
        rng.fill(&mut buf[..]);
        buf.fill(0xAA);

        lsmt.write_at(offset, &buf).await.expect("write failed");
    }

    drop(lsmt);
    let lsmt2 = open_sparse_lsmt_env(f_data, vec![]).await.unwrap();

    for (blk_offset, blk_len) in &ops {
        let offset = *blk_offset * 512;
        let len = *blk_len * 512;

        let read_bytes = lsmt2
            .read_at(offset, len as usize)
            .await
            .expect("read failed");
        assert_eq!(read_bytes.len(), len as usize);
        for b in read_bytes {
            assert_eq!(b, 0xAA, "Data mismatch at offset {}", offset);
        }
    }

    let hole_offset = 4 * 512;
    let hole_data = lsmt2
        .read_at(hole_offset, 512)
        .await
        .expect("read hole failed");
    for b in hole_data {
        assert_eq!(b, 0, "Hole should be zero");
    }
}

#[tokio::test]
async fn test_hybrid_reopen_detects_file_type() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, f_index, lsmt) = create_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;
    assert_eq!(lsmt.file_type(), LSMTFileType::HybridReadWrite);
    drop(lsmt);

    let reopened = open_hybrid_lsmt_env(f_data, f_index, None, vec![])
        .await
        .unwrap();
    assert_eq!(reopened.file_type(), LSMTFileType::HybridReadWrite);
}

#[tokio::test]
async fn test_open_rejects_sparse_and_hybrid_header() {
    let temp_dir = TempDir::new().unwrap();
    let ring = test_io_ring();
    let data = Arc::new(
        LocalFile::new(temp_dir.path().join("invalid-rw.data"), ring.clone())
            .await
            .unwrap(),
    );
    let index = Arc::new(
        LocalFile::new(temp_dir.path().join("invalid-rw.index"), ring)
            .await
            .unwrap(),
    );

    let mut header = rw_header(
        1024 * 1024,
        RwLayout::HybridLogStructured,
        Uuid::new_v4(),
        None,
        None,
    );
    header.set_sparse_rw();
    write_header_block(&(data.clone() as Arc<dyn VirtualFile>), &header)
        .await
        .unwrap();

    let err = match open_file_rw(data, Some(index)).await {
        Ok(_) => panic!("conflicting sparse/hybrid flags should fail"),
        Err(err) => err,
    };
    assert_err_contains(&err, "sparse and hybrid flags are both set");
}

#[tokio::test]
async fn test_hybrid_overwrite_existing_upper_block_in_place() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, f_index, lsmt) = create_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;

    let first = vec![0x11; 4096];
    let second = vec![0x22; 4096];
    lsmt.write_at(0, &first).await.unwrap();
    lsmt.sync().await.unwrap();
    let data_size_after_first = f_data.size().await.unwrap();
    let index_size_after_first = f_index.size().await.unwrap();

    lsmt.write_at(0, &second).await.unwrap();
    lsmt.sync().await.unwrap();

    assert_eq!(
        f_data.size().await.unwrap(),
        data_size_after_first,
        "hybrid overwrite should not append data"
    );
    assert_eq!(
        f_index.size().await.unwrap(),
        index_size_after_first,
        "hybrid overwrite should not append index mappings"
    );
    let got = lsmt.read_at(0, 4096).await.unwrap();
    assert_eq!(got.as_ref(), second.as_slice());
}

#[tokio::test]
async fn test_hybrid_mixed_write_in_place_and_append() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, f_index, lsmt) = create_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;

    let initial = vec![0x10; 4096];
    lsmt.write_at(0, &initial).await.unwrap();
    lsmt.sync().await.unwrap();
    let data_size_after_first = f_data.size().await.unwrap();
    let index_size_after_first = f_index.size().await.unwrap();

    let mixed = vec![0x33; 8192];
    lsmt.write_at(0, &mixed).await.unwrap();
    lsmt.sync().await.unwrap();

    assert_eq!(
        f_data.size().await.unwrap(),
        data_size_after_first + 4096,
        "only the new upper gap should be appended"
    );
    assert_eq!(
        f_index.size().await.unwrap(),
        index_size_after_first + size_of::<DiskSegmentMapping>() as u64,
        "only the appended gap should add an index mapping"
    );
    let got = lsmt.read_at(0, 8192).await.unwrap();
    assert_eq!(got.as_ref(), mixed.as_slice());
}

#[tokio::test]
async fn test_hybrid_complex_write_fragments_and_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 8 * 4096;
    let base_data = create_sealed_layer(
        &temp_dir,
        "hybrid-complex-base",
        vsize,
        &[(0, 0xA0), (4096, 0xA1), (4 * 4096, 0xA4)],
    )
    .await;
    let base_ro_index = load_base_index(base_data.clone()).await;
    let (f_data, f_index, _upper) = create_hybrid_lsmt_env(&temp_dir, vsize).await;
    let stacked = LSMTFile::open(
        f_data.clone(),
        Some(f_index.clone()),
        Some(Arc::new(base_ro_index)),
        vec![base_data as Arc<dyn VirtualFile>],
    )
    .await
    .unwrap();

    stacked.write_at(4096, &[0xB1; 4096]).await.unwrap();
    stacked.write_at(2 * 4096, &[0xB2; 4096]).await.unwrap();
    <LSMTFile as VirtualFile>::discard(&stacked, 2 * 4096, 4096)
        .await
        .unwrap();
    stacked.sync().await.unwrap();
    let data_size_before = f_data.size().await.unwrap();
    let index_size_before = f_index.size().await.unwrap();

    let update = vec![0xCC; 5 * 4096];
    stacked.write_at(0, &update).await.unwrap();
    stacked.sync().await.unwrap();

    assert_eq!(
        f_data.size().await.unwrap(),
        data_size_before + 4 * 4096,
        "lower-only, zeroed, and gap fragments append; existing upper fragment is in-place"
    );
    assert_eq!(
        f_index.size().await.unwrap(),
        index_size_before + 3 * size_of::<DiskSegmentMapping>() as u64,
        "only appended fragments add index mappings"
    );
    let got = stacked.read_at(0, 5 * 4096).await.unwrap();
    assert_eq!(got.as_ref(), update.as_slice());
    drop(stacked);

    let reopened = open_hybrid_lsmt_env(f_data, f_index, None, vec![])
        .await
        .unwrap();
    let reopened_got = reopened.read_at(0, 5 * 4096).await.unwrap();
    assert_eq!(reopened_got.as_ref(), update.as_slice());
}

#[tokio::test]
async fn test_hybrid_reopen_preserves_in_place_update() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, f_index, lsmt) = create_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;

    let first = vec![0x44; 4096];
    let second = vec![0x55; 4096];
    lsmt.write_at(4096, &first).await.unwrap();
    lsmt.write_at(4096, &second).await.unwrap();
    lsmt.sync().await.unwrap();
    drop(lsmt);

    let reopened = open_hybrid_lsmt_env(f_data, f_index, None, vec![])
        .await
        .unwrap();
    let got = reopened.read_at(4096, 4096).await.unwrap();
    assert_eq!(got.as_ref(), second.as_slice());
}

#[tokio::test]
async fn test_hybrid_read_waits_for_in_place_write() {
    let temp_dir = TempDir::new().unwrap();
    let (data_file, _index_file, lsmt) =
        create_blocking_hybrid_lsmt_env(&temp_dir, 1024 * 1024, 3).await;
    let lsmt = Arc::new(lsmt);

    let first = vec![0x11; 4096];
    lsmt.write_at(0, &first).await.unwrap();

    let writer = lsmt.clone();
    let write_task = tokio::spawn(async move {
        writer.write_at(0, &[0x22; 4096]).await.unwrap();
    });
    data_file.wait_for_blocked_write().await;

    let reader = lsmt.clone();
    let mut read_task = Box::pin(tokio::spawn(async move {
        reader.read_at(0, 4096).await.unwrap()
    }));
    tokio::select! {
        _ = &mut read_task => panic!("hybrid read completed during in-place write"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
    }

    data_file.release_blocked_write();
    write_task.await.unwrap();

    let got = read_task.await.unwrap();
    assert_eq!(got.as_ref(), &[0x22; 4096]);
}

#[tokio::test]
async fn test_hybrid_concurrent_in_place_writes_are_serialized() {
    let temp_dir = TempDir::new().unwrap();
    let (data_file, f_index, lsmt) =
        create_blocking_hybrid_lsmt_env(&temp_dir, 1024 * 1024, 3).await;
    let lsmt = Arc::new(lsmt);

    lsmt.write_at(0, &[0x10; 4096]).await.unwrap();
    let data_size_after_first = data_file.size().await.unwrap();
    let index_size_after_first = f_index.size().await.unwrap();

    let first_writer = lsmt.clone();
    let first_task = tokio::spawn(async move {
        first_writer.write_at(0, &[0x20; 4096]).await.unwrap();
    });
    data_file.wait_for_blocked_write().await;

    let second_writer = lsmt.clone();
    let mut second_task = Box::pin(tokio::spawn(async move {
        second_writer.write_at(0, &[0x30; 4096]).await.unwrap();
    }));
    tokio::select! {
        _ = &mut second_task => panic!("second in-place write completed while first write held the lock"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
    }

    data_file.release_blocked_write();
    first_task.await.unwrap();
    second_task.await.unwrap();

    assert_eq!(data_file.size().await.unwrap(), data_size_after_first);
    assert_eq!(f_index.size().await.unwrap(), index_size_after_first);
    let got = lsmt.read_at(0, 4096).await.unwrap();
    assert_eq!(got.as_ref(), &[0x30; 4096]);
}

#[tokio::test]
async fn test_hybrid_concurrent_append_writes_are_serialized_and_persist() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, f_index, lsmt) = create_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;
    let lsmt = Arc::new(lsmt);
    let data_size_before = f_data.size().await.unwrap();
    let index_size_before = f_index.size().await.unwrap();

    let mut tasks = Vec::new();
    for i in 0..8u64 {
        let writer = lsmt.clone();
        tasks.push(tokio::spawn(async move {
            let byte = 0x40 + i as u8;
            writer
                .write_at(i * 4096, &[byte; 4096])
                .await
                .expect("hybrid append write");
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    lsmt.sync().await.unwrap();

    assert_eq!(f_data.size().await.unwrap(), data_size_before + 8 * 4096);
    assert_eq!(
        f_index.size().await.unwrap(),
        index_size_before + 8 * size_of::<DiskSegmentMapping>() as u64
    );
    for i in 0..8u64 {
        let got = lsmt.read_at(i * 4096, 4096).await.unwrap();
        assert_eq!(got.as_ref(), &[0x40 + i as u8; 4096]);
    }
}

#[tokio::test]
async fn test_hybrid_discard_waits_for_in_place_write() {
    let temp_dir = TempDir::new().unwrap();
    let (data_file, _index_file, lsmt) =
        create_blocking_hybrid_lsmt_env(&temp_dir, 1024 * 1024, 3).await;
    let lsmt = Arc::new(lsmt);

    lsmt.write_at(0, &[0x11; 4096]).await.unwrap();
    let writer = lsmt.clone();
    let write_task = tokio::spawn(async move {
        writer.write_at(0, &[0x22; 4096]).await.unwrap();
    });
    data_file.wait_for_blocked_write().await;

    let discarder = lsmt.clone();
    let mut discard_task = Box::pin(tokio::spawn(async move {
        <LSMTFile as VirtualFile>::discard(&discarder, 0, 4096)
            .await
            .unwrap();
    }));
    tokio::select! {
        _ = &mut discard_task => panic!("discard completed while in-place write held the lock"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
    }

    data_file.release_blocked_write();
    write_task.await.unwrap();
    discard_task.await.unwrap();

    let got = lsmt.read_at(0, 4096).await.unwrap();
    assert!(got.iter().all(|&b| b == 0));
}

#[tokio::test]
async fn test_hybrid_lower_cow_first_write_appends() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 1024 * 1024;
    let base_data = create_sealed_layer(&temp_dir, "hybrid-base", vsize, &[(0, 0x66)]).await;
    let base_ro_index = load_base_index(base_data.clone()).await;
    let (f_data, f_index, upper) = create_hybrid_lsmt_env(&temp_dir, vsize).await;
    let stacked = LSMTFile::open(
        f_data.clone(),
        Some(f_index.clone()),
        Some(Arc::new(base_ro_index)),
        vec![base_data.clone() as Arc<dyn VirtualFile>],
    )
    .await
    .unwrap();

    let before = f_data.size().await.unwrap();
    let update = vec![0x77; 4096];
    stacked.write_at(0, &update).await.unwrap();
    stacked.sync().await.unwrap();
    assert_eq!(f_data.size().await.unwrap(), before + 4096);
    let got = stacked.read_at(0, 4096).await.unwrap();
    assert_eq!(got.as_ref(), update.as_slice());
    let base_got = base_data.read_at(HEADER_SIZE, 4096).await.unwrap();
    assert_eq!(base_got.as_ref(), &[0x66; 4096]);
    drop(upper);
}

#[tokio::test]
async fn test_hybrid_lower_cow_write_survives_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 1024 * 1024;
    let base_data = create_sealed_layer(&temp_dir, "hybrid-reopen-base", vsize, &[(0, 0x66)]).await;
    let base_ro_index = load_base_index(base_data.clone()).await;
    let (f_data, f_index, _upper) = create_hybrid_lsmt_env(&temp_dir, vsize).await;
    let stacked = LSMTFile::open(
        f_data.clone(),
        Some(f_index.clone()),
        Some(Arc::new(base_ro_index)),
        vec![base_data.clone() as Arc<dyn VirtualFile>],
    )
    .await
    .unwrap();

    let update = vec![0x77; 4096];
    stacked.write_at(0, &update).await.unwrap();
    stacked.sync().await.unwrap();
    drop(stacked);

    let reopened = open_hybrid_lsmt_env(f_data, f_index, None, vec![])
        .await
        .unwrap();
    let got = reopened.read_at(0, 4096).await.unwrap();
    assert_eq!(got.as_ref(), update.as_slice());
    let base_got = base_data.read_at(HEADER_SIZE, 4096).await.unwrap();
    assert_eq!(base_got.as_ref(), &[0x66; 4096]);
}

#[tokio::test]
async fn test_hybrid_discard_then_rewrite_appends() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, _f_index, lsmt) = create_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;

    let first = vec![0x88; 4096];
    let second = vec![0x99; 4096];
    lsmt.write_at(0, &first).await.unwrap();
    lsmt.sync().await.unwrap();
    let size_after_first = f_data.size().await.unwrap();

    <LSMTFile as VirtualFile>::discard(&lsmt, 0, 4096)
        .await
        .unwrap();
    let zero = lsmt.read_at(0, 4096).await.unwrap();
    assert!(zero.iter().all(|&b| b == 0));

    lsmt.write_at(0, &second).await.unwrap();
    lsmt.sync().await.unwrap();
    assert_eq!(
        f_data.size().await.unwrap(),
        size_after_first + 4096,
        "rewrite after zeroed mapping should append a fresh block"
    );
    let got = lsmt.read_at(0, 4096).await.unwrap();
    assert_eq!(got.as_ref(), second.as_slice());
}

#[tokio::test]
async fn test_hybrid_discard_and_rewrite_survive_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, f_index, lsmt) = create_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;

    let first = vec![0x31; 4096];
    let second = vec![0x32; 4096];
    lsmt.write_at(0, &first).await.unwrap();
    <LSMTFile as VirtualFile>::discard(&lsmt, 0, 4096)
        .await
        .unwrap();
    lsmt.sync().await.unwrap();
    drop(lsmt);

    let reopened_zero = open_hybrid_lsmt_env(f_data.clone(), f_index.clone(), None, vec![])
        .await
        .unwrap();
    let zero = reopened_zero.read_at(0, 4096).await.unwrap();
    assert!(zero.iter().all(|&b| b == 0));
    reopened_zero.write_at(0, &second).await.unwrap();
    reopened_zero.sync().await.unwrap();
    drop(reopened_zero);

    let reopened_data = open_hybrid_lsmt_env(f_data, f_index, None, vec![])
        .await
        .unwrap();
    let got = reopened_data.read_at(0, 4096).await.unwrap();
    assert_eq!(got.as_ref(), second.as_slice());
}

#[tokio::test]
async fn test_hybrid_close_seal_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, _f_index, lsmt) = create_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;

    let first = vec![0xAB; 4096];
    let second = vec![0xCD; 4096];
    lsmt.write_at(0, &first).await.unwrap();
    lsmt.write_at(0, &second).await.unwrap();
    let descriptor = lsmt.close_seal().await.unwrap();
    assert!(
        descriptor.is_none(),
        "hybrid in-place upper should not maintain append digest descriptor"
    );
    drop(lsmt);

    let ro = LSMTReadOnlyFile::open(f_data).await.unwrap();
    let got = ro.read_at(0, 4096).await.unwrap();
    assert_eq!(got.as_ref(), second.as_slice());
}

#[tokio::test]
async fn test_stack_files() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 10 * 1024 * 1024;

    let (base_data, _base_index, base_lsmt) = create_lsmt_env(&temp_dir, vsize).await;
    let base_content = vec![0xBB; 4096];
    base_lsmt.write_at(4096, &base_content).await.unwrap();
    base_lsmt.close_seal().await.unwrap();
    drop(base_lsmt);

    let base_ro_index = load_base_index(base_data.clone()).await;

    let (upper_data, upper_index, _) = create_lsmt_env(&temp_dir, vsize).await;

    let ring = test_io_ring();
    let raw_base_path = temp_dir.path().join("base.raw");
    let raw_base_file = Arc::new(LocalFile::new(&raw_base_path, ring).await.unwrap());
    raw_base_file.write_at(4096, &base_content).await.unwrap();

    let lsmt_stacked = LSMTFile::open(
        upper_data,
        Some(upper_index),
        Some(Arc::new(base_ro_index)),
        vec![raw_base_file.clone()],
    )
    .await
    .expect("stack open failed");

    let read_base = lsmt_stacked.read_at(4096, 4096).await.unwrap();
    assert_eq!(read_base[0], 0xBB, "Should read from base layer");

    let upper_content = vec![0xCC; 4096];
    lsmt_stacked.write_at(4096, &upper_content).await.unwrap();
    let read_overlay = lsmt_stacked.read_at(4096, 4096).await.unwrap();
    assert_eq!(read_overlay[0], 0xCC, "Should read from upper layer");

    lsmt_stacked.write_at(8192, &upper_content).await.unwrap();
    let read_new = lsmt_stacked.read_at(8192, 4096).await.unwrap();
    assert_eq!(read_new[0], 0xCC);
}

#[tokio::test]
async fn test_stack_files_with_sparse_upper_and_sparse_lower() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 10 * 1024 * 1024;

    let (base_data, base_lsmt) = create_sparse_lsmt_env(&temp_dir, vsize).await;
    let base_content = vec![0xBB; 4096];
    base_lsmt.write_at(4096, &base_content).await.unwrap();
    base_lsmt.close_seal().await.unwrap();
    drop(base_lsmt);

    let base_ro = LSMTReadOnlyFile::open(base_data).await.unwrap();

    let upper_dir = TempDir::new().unwrap();
    let (upper_data, upper_lsmt) = create_sparse_lsmt_env(&upper_dir, vsize).await;
    drop(upper_lsmt);

    let lsmt_stacked = LSMTFile::open(
        upper_data,
        None,
        base_ro.index_view.clone(),
        base_ro.layers.clone(),
    )
    .await
    .expect("stack sparse open failed");

    let read_base = lsmt_stacked.read_at(4096, 4096).await.unwrap();
    assert_eq!(read_base[0], 0xBB, "Should read from sparse base layer");

    let upper_content = vec![0xCC; 4096];
    lsmt_stacked.write_at(4096, &upper_content).await.unwrap();
    let read_overlay = lsmt_stacked.read_at(4096, 4096).await.unwrap();
    assert_eq!(read_overlay[0], 0xCC, "Should read from sparse upper layer");
    assert_eq!(lsmt_stacked.file_type(), LSMTFileType::SparseReadWrite);
}

#[tokio::test]
async fn test_stack_files_with_sparse_upper_and_multiple_sparse_lowers() {
    let base_dir = TempDir::new().unwrap();
    let mid_dir = TempDir::new().unwrap();
    let vsize = 10 * 1024 * 1024;

    let (base_data, base_lsmt) = create_sparse_lsmt_env(&base_dir, vsize).await;
    base_lsmt.write_at(0, &vec![0xAA; 4096]).await.unwrap();
    base_lsmt.write_at(12288, &vec![0xAB; 4096]).await.unwrap();
    base_lsmt.close_seal().await.unwrap();
    drop(base_lsmt);

    let (mid_data, mid_lsmt) = create_sparse_lsmt_env(&mid_dir, vsize).await;
    mid_lsmt.write_at(0, &vec![0xBB; 4096]).await.unwrap();
    mid_lsmt.write_at(4096, &vec![0xBC; 4096]).await.unwrap();
    mid_lsmt.close_seal().await.unwrap();
    drop(mid_lsmt);

    let lower_files: Vec<Arc<dyn VirtualFile>> = vec![
        base_data.clone() as Arc<dyn VirtualFile>,
        mid_data.clone() as Arc<dyn VirtualFile>,
    ];
    let lower = open_files_ro(&lower_files).await.unwrap();

    let upper_dir = TempDir::new().unwrap();
    let (_upper_data, upper_lsmt) = create_sparse_lsmt_env(&upper_dir, vsize).await;

    let stacked = stack_files(&upper_lsmt, &lower, false).await.unwrap();
    assert_eq!(stacked.file_type(), LSMTFileType::SparseReadWrite);

    let got0 = stacked.read_at(0, 4096).await.unwrap();
    assert_eq!(got0.as_ref(), vec![0xBB; 4096].as_slice());
    let got4k = stacked.read_at(4096, 4096).await.unwrap();
    assert_eq!(got4k.as_ref(), vec![0xBC; 4096].as_slice());
    let got12k = stacked.read_at(12288, 4096).await.unwrap();
    assert_eq!(got12k.as_ref(), vec![0xAB; 4096].as_slice());
}

#[tokio::test]
async fn test_sparse_restack_twice_preserves_lower_chain() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 64 * 1024u64;

    let (base_data, base_lsmt) = create_sparse_lsmt_env(&temp_dir, vsize).await;
    let base_payload = vec![0xA1; 4096];
    base_lsmt.write_at(0, &base_payload).await.unwrap();
    base_lsmt.close_seal().await.unwrap();
    drop(base_lsmt);

    let lower0_files: Vec<Arc<dyn VirtualFile>> = vec![base_data.clone() as Arc<dyn VirtualFile>];
    let lower0 = open_files_ro(&lower0_files).await.unwrap();

    let upper1_dir = TempDir::new().unwrap();
    let (upper1_data, upper1_lsmt) = create_sparse_lsmt_env(&upper1_dir, vsize).await;
    let stacked1 = stack_files(&upper1_lsmt, &lower0, false).await.unwrap();
    let payload1 = vec![0xB2; 4096];
    stacked1.write_at(4096, &payload1).await.unwrap();
    let (sealed1, _) = stacked1.close_seal_and_reopen().await.unwrap();
    let layer1 = sealed1
        .get_lower_files()
        .into_iter()
        .next()
        .expect("sealed sparse upper should expose file");
    drop(upper1_data);

    let lower1_files: Vec<Arc<dyn VirtualFile>> =
        vec![base_data.clone() as Arc<dyn VirtualFile>, layer1.clone()];
    let lower1 = open_files_ro(&lower1_files).await.unwrap();

    let upper2_dir = TempDir::new().unwrap();
    let (_upper2_data, upper2_lsmt) = create_sparse_lsmt_env(&upper2_dir, vsize).await;
    let stacked2 = stack_files(&upper2_lsmt, &lower1, false).await.unwrap();
    let payload2 = vec![0xC3; 4096];
    stacked2.write_at(8192, &payload2).await.unwrap();
    let (sealed2, _) = stacked2.close_seal_and_reopen().await.unwrap();
    let layer2 = sealed2
        .get_lower_files()
        .into_iter()
        .next()
        .expect("second sealed sparse upper should expose file");

    let lower2_files: Vec<Arc<dyn VirtualFile>> =
        vec![base_data as Arc<dyn VirtualFile>, layer1, layer2];
    let lower2 = open_files_ro(&lower2_files).await.unwrap();

    let upper3_dir = TempDir::new().unwrap();
    let (_upper3_data, upper3_lsmt) = create_sparse_lsmt_env(&upper3_dir, vsize).await;
    let stacked3 = stack_files(&upper3_lsmt, &lower2, false).await.unwrap();
    assert_eq!(stacked3.file_type(), LSMTFileType::SparseReadWrite);

    let got0 = stacked3.read_at(0, 4096).await.unwrap();
    assert_eq!(got0.as_ref(), base_payload.as_slice());
    let got4k = stacked3.read_at(4096, 4096).await.unwrap();
    assert_eq!(got4k.as_ref(), payload1.as_slice());
    let got8k = stacked3.read_at(8192, 4096).await.unwrap();
    assert_eq!(got8k.as_ref(), payload2.as_slice());

    let payload3 = vec![0xD4; 4096];
    stacked3.write_at(12288, &payload3).await.unwrap();
    let got12k = stacked3.read_at(12288, 4096).await.unwrap();
    assert_eq!(got12k.as_ref(), payload3.as_slice());
}

#[tokio::test]
async fn test_close_seal() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, f_index, lsmt) = create_lsmt_env(&temp_dir, 1024 * 1024).await;

    lsmt.write_at(0, &[1u8; 512]).await.unwrap();

    lsmt.close_seal().await.unwrap();

    let data_size = f_data.size().await.unwrap();
    assert!(data_size > 4096);

    let result = open_lsmt_env(f_data, f_index, vec![]).await;
    assert!(
        result.is_err(),
        "Should forbid opening sealed file in RW mode"
    );
}

#[tokio::test]
async fn test_close_seal_and_reopen_returns_readable_lower() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, _f_index, lsmt) = create_lsmt_env(&temp_dir, 1024 * 1024).await;

    let payload = vec![0x4d; 4096];
    lsmt.write_at(0, &payload).await.unwrap();

    let (reopened, _) = lsmt.close_seal_and_reopen().await.unwrap();

    let got = reopened.read_at(0, payload.len()).await.unwrap();
    assert_eq!(got.as_ref(), payload.as_slice());

    let sealed = LSMTReadOnlyFile::open(f_data).await.unwrap();
    let got_from_disk = sealed.read_at(0, payload.len()).await.unwrap();
    assert_eq!(got_from_disk.as_ref(), payload.as_slice());
}

#[tokio::test]
async fn test_update_vsize_after_write_preserves_close_seal_descriptor() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, _f_index, lsmt) = create_lsmt_env(&temp_dir, 8 * 1024).await;

    let payload = vec![0x5a; 4096];
    lsmt.write_at(0, &payload).await.unwrap();
    lsmt.update_vsize(16 * 1024).await.unwrap();

    let descriptor = lsmt
        .close_seal()
        .await
        .unwrap()
        .expect("descriptor should remain available after vsize update");
    let data_size = f_data.size().await.unwrap();
    let sealed_bytes = f_data.read_at(0, data_size as usize).await.unwrap();

    assert_eq!(descriptor.size, data_size);
    assert_eq!(
        descriptor.digest,
        format!("sha256:{:x}", Sha256::digest(&sealed_bytes))
    );
}

#[tokio::test]
async fn test_sparse_close_seal_and_commit_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 4 * 1024 * 1024;
    let (f_data, lsmt) = create_sparse_lsmt_env(&temp_dir, vsize).await;

    let data_block = vec![0xAB; 4096];
    lsmt.write_at(8192, &data_block).await.unwrap();
    lsmt.close_seal().await.unwrap();
    drop(lsmt);

    let ro_layer = LSMTReadOnlyFile::open(f_data.clone()).await.unwrap();
    let read_back = ro_layer.read_at(8192, 4096).await.unwrap();
    assert_eq!(&read_back[..], &data_block[..]);

    let ring = test_io_ring();
    let commit_path = temp_dir.path().join("sparse_ro_commit.lsmt");
    let f_commit = Arc::new(LocalFile::new(&commit_path, ring).await.unwrap());
    ro_layer.commit(f_commit.clone()).await.unwrap();

    let final_ro = LSMTReadOnlyFile::open(f_commit).await.unwrap();
    let final_read_back = final_ro.read_at(8192, 4096).await.unwrap();
    assert_eq!(&final_read_back[..], &data_block[..]);

    let hole = final_ro.read_at(0, 4096).await.unwrap();
    assert_eq!(hole.iter().sum::<u8>(), 0);
}

#[tokio::test]
async fn test_log_discard_range_persists_via_index() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, f_index, lsmt) = create_lsmt_env(&temp_dir, 1024 * 1024).await;

    let payload = vec![0x5Au8; 4096];
    lsmt.write_at(0, &payload).await.unwrap();
    <LSMTFile as VirtualFile>::discard(&lsmt, 0, payload.len() as u64)
        .await
        .unwrap();

    let got = lsmt.read_at(0, payload.len()).await.unwrap();
    assert!(got.iter().all(|&b| b == 0));

    drop(lsmt);
    let reopened = open_lsmt_env(f_data, f_index, vec![]).await.unwrap();
    let reopened_got = reopened.read_at(0, payload.len()).await.unwrap();
    assert!(reopened_got.iter().all(|&b| b == 0));
}

async fn create_counting_log_lsmt_env(
    dir: &TempDir,
    vsize: u64,
) -> (Arc<CountingSizeFile>, Arc<CountingSizeFile>, LSMTFile) {
    let ring = test_io_ring();
    let data_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(dir.path().join("count-log-data.lsmt"), ring.clone())
            .await
            .unwrap(),
    );
    let index_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(dir.path().join("count-log-index.lsmt"), ring)
            .await
            .unwrap(),
    );
    let counted_data = CountingSizeFile::new(data_file);
    let counted_index = CountingSizeFile::new(index_file);
    let lsmt = LSMTFile::create(
        counted_data.clone() as Arc<dyn VirtualFile>,
        Some(counted_index.clone() as Arc<dyn VirtualFile>),
        vsize,
        false,
    )
    .await
    .unwrap();
    counted_data.reset_size_calls();
    counted_index.reset_size_calls();
    (counted_data, counted_index, lsmt)
}

async fn create_counting_hybrid_lsmt_env(
    dir: &TempDir,
    vsize: u64,
) -> (Arc<CountingSizeFile>, Arc<CountingSizeFile>, LSMTFile) {
    let ring = test_io_ring();
    let data_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(dir.path().join("count-hybrid-data.lsmt"), ring.clone())
            .await
            .unwrap(),
    );
    let index_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(dir.path().join("count-hybrid-index.lsmt"), ring)
            .await
            .unwrap(),
    );
    let counted_data = CountingSizeFile::new(data_file);
    let counted_index = CountingSizeFile::new(index_file);
    let mut args = LayerInfo::new(
        counted_data.clone() as Arc<dyn VirtualFile>,
        Some(counted_index.clone() as Arc<dyn VirtualFile>),
        vsize,
    );
    args.rw_layout = RwLayout::HybridLogStructured;
    let lsmt = create_file_rw(args).await.unwrap();
    counted_data.reset_size_calls();
    counted_index.reset_size_calls();
    (counted_data, counted_index, lsmt)
}

fn assert_no_size_calls(data: &CountingSizeFile, index: &CountingSizeFile) {
    assert_eq!(
        data.size_calls(),
        0,
        "rw data size() should not be in the write hot path"
    );
    assert_eq!(
        index.size_calls(),
        0,
        "rw index size() should not be in the write hot path"
    );
}

#[tokio::test]
async fn test_log_write_hot_path_avoids_size_calls() {
    let temp_dir = TempDir::new().unwrap();
    let (data, index, lsmt) = create_counting_log_lsmt_env(&temp_dir, 1024 * 1024).await;

    lsmt.write_at(0, &[0xA5; 4096]).await.unwrap();

    assert_no_size_calls(&data, &index);
}

#[tokio::test]
async fn test_hybrid_first_append_hot_path_avoids_size_calls() {
    let temp_dir = TempDir::new().unwrap();
    let (data, index, lsmt) = create_counting_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;

    lsmt.write_at(0, &[0xB6; 4096]).await.unwrap();

    assert_no_size_calls(&data, &index);
}

#[tokio::test]
async fn test_hybrid_mixed_write_hot_path_avoids_size_calls() {
    let temp_dir = TempDir::new().unwrap();
    let (data, index, lsmt) = create_counting_hybrid_lsmt_env(&temp_dir, 1024 * 1024).await;

    lsmt.write_at(0, &[0xC7; 4096]).await.unwrap();
    data.reset_size_calls();
    index.reset_size_calls();
    lsmt.write_at(0, &[0xD8; 8192]).await.unwrap();

    assert_no_size_calls(&data, &index);
}

#[tokio::test]
async fn test_log_index_group_commit_flush_avoids_size_calls() {
    let temp_dir = TempDir::new().unwrap();
    let (data, index, lsmt) = create_counting_log_lsmt_env(&temp_dir, 1024 * 1024).await;
    lsmt.set_index_group_commit(size_of::<DiskSegmentMapping>() * 4)
        .unwrap();

    lsmt.write_at(0, &[0xE9; 4096]).await.unwrap();
    lsmt.write_at(4096, &[0xEA; 4096]).await.unwrap();
    lsmt.sync().await.unwrap();

    assert_no_size_calls(&data, &index);
}

#[tokio::test]
async fn test_log_discard_hot_path_avoids_size_calls() {
    let temp_dir = TempDir::new().unwrap();
    let (data, index, lsmt) = create_counting_log_lsmt_env(&temp_dir, 1024 * 1024).await;

    lsmt.write_at(0, &[0xF1; 4096]).await.unwrap();
    data.reset_size_calls();
    index.reset_size_calls();
    <LSMTFile as VirtualFile>::discard(&lsmt, 0, 4096)
        .await
        .unwrap();

    assert_no_size_calls(&data, &index);
}

#[tokio::test]
async fn test_log_append_cursor_survives_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let ring = test_io_ring();
    let data_path = temp_dir.path().join("reopen-count-log-data.lsmt");
    let index_path = temp_dir.path().join("reopen-count-log-index.lsmt");

    {
        let data_file = Arc::new(
            LocalFile::new(&data_path, ring.clone())
                .await
                .expect("create log data file"),
        );
        let index_file = Arc::new(
            LocalFile::new(&index_path, ring.clone())
                .await
                .expect("create log index file"),
        );
        let lsmt = LSMTFile::create(data_file, Some(index_file), 1024 * 1024, false)
            .await
            .expect("create log LSMTFile");

        lsmt.write_at(0, &[0xA1; 4096]).await.unwrap();
        lsmt.write_at(4096, &[0xA2; 4096]).await.unwrap();
        lsmt.sync().await.unwrap();
    }

    let data_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_rw(&data_path, false, ring.clone())
            .await
            .expect("reopen log data file"),
    );
    let index_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_rw(&index_path, false, ring)
            .await
            .expect("reopen log index file"),
    );
    let counted_data = CountingSizeFile::new(data_file);
    let counted_index = CountingSizeFile::new(index_file);
    let lsmt = LSMTFile::open(
        counted_data.clone() as Arc<dyn VirtualFile>,
        Some(counted_index.clone() as Arc<dyn VirtualFile>),
        None,
        vec![],
    )
    .await
    .expect("open log LSMTFile");

    counted_data.reset_size_calls();
    counted_index.reset_size_calls();

    lsmt.write_at(8192, &[0xA3; 4096]).await.unwrap();

    assert_no_size_calls(&counted_data, &counted_index);
    assert_eq!(lsmt.read_at(0, 4096).await.unwrap()[0], 0xA1);
    assert_eq!(lsmt.read_at(4096, 4096).await.unwrap()[0], 0xA2);
    assert_eq!(lsmt.read_at(8192, 4096).await.unwrap()[0], 0xA3);
}

#[tokio::test]
async fn test_hybrid_append_cursor_survives_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let ring = test_io_ring();
    let data_path = temp_dir.path().join("reopen-count-hybrid-data.lsmt");
    let index_path = temp_dir.path().join("reopen-count-hybrid-index.lsmt");

    {
        let data_file = Arc::new(
            LocalFile::new(&data_path, ring.clone())
                .await
                .expect("create hybrid data file"),
        );
        let index_file = Arc::new(
            LocalFile::new(&index_path, ring.clone())
                .await
                .expect("create hybrid index file"),
        );
        let mut args = LayerInfo::new(data_file, Some(index_file), 1024 * 1024);
        args.rw_layout = RwLayout::HybridLogStructured;
        let lsmt = create_file_rw(args).await.expect("create hybrid LSMTFile");

        lsmt.write_at(0, &[0xB1; 4096]).await.unwrap();
        lsmt.write_at(4096, &[0xB2; 4096]).await.unwrap();
        lsmt.write_at(0, &[0xB3; 4096]).await.unwrap();
        lsmt.sync().await.unwrap();
    }

    let data_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_rw(&data_path, false, ring.clone())
            .await
            .expect("reopen hybrid data file"),
    );
    let index_file: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_rw(&index_path, false, ring)
            .await
            .expect("reopen hybrid index file"),
    );
    let counted_data = CountingSizeFile::new(data_file);
    let counted_index = CountingSizeFile::new(index_file);
    let lsmt = LSMTFile::open(
        counted_data.clone() as Arc<dyn VirtualFile>,
        Some(counted_index.clone() as Arc<dyn VirtualFile>),
        None,
        vec![],
    )
    .await
    .expect("open hybrid LSMTFile");

    counted_data.reset_size_calls();
    counted_index.reset_size_calls();

    lsmt.write_at(8192, &[0xB4; 4096]).await.unwrap();

    assert_no_size_calls(&counted_data, &counted_index);
    assert_eq!(lsmt.read_at(0, 4096).await.unwrap()[0], 0xB3);
    assert_eq!(lsmt.read_at(4096, 4096).await.unwrap()[0], 0xB2);
    assert_eq!(lsmt.read_at(8192, 4096).await.unwrap()[0], 0xB4);
}

#[tokio::test]
async fn test_sparse_discard_range_persists_via_hole_punch() {
    let temp_dir = TempDir::new().unwrap();
    let (f_data, lsmt) = create_sparse_lsmt_env(&temp_dir, 1024 * 1024).await;

    let payload = vec![0x5Au8; 4096];
    lsmt.write_at(0, &payload).await.unwrap();
    <LSMTFile as VirtualFile>::discard(&lsmt, 0, payload.len() as u64)
        .await
        .unwrap();

    let got = lsmt.read_at(0, payload.len()).await.unwrap();
    assert!(got.iter().all(|&b| b == 0));

    let mut segs = Vec::new();
    let seg_count = lsmt
        .seek_data(0, payload.len() as u64, &mut segs)
        .await
        .unwrap();
    assert_eq!(
        seg_count, 0,
        "discarded sparse region should not be reported as data"
    );

    drop(lsmt);
    let reopened = open_sparse_lsmt_env(f_data.clone(), vec![]).await.unwrap();
    let reopened_got = reopened.read_at(0, payload.len()).await.unwrap();
    assert!(reopened_got.iter().all(|&b| b == 0));

    let sparse_mappings =
        create_mappings_from_sparse(&(f_data as Arc<dyn VirtualFile>), HEADER_SIZE)
            .await
            .unwrap();
    assert!(
        sparse_mappings.is_empty(),
        "discarded sparse data should be removed from the punched file extents"
    );
}

#[tokio::test]
async fn test_ro_layer_commit() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 4 * 1024 * 1024;
    let (f_data, _f_index, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    let data_block = vec![0xAB; 4096];
    lsmt.write_at(8192, &data_block).await.unwrap();
    lsmt.close_seal().await.unwrap();
    drop(lsmt);

    let ro_layer = LSMTReadOnlyFile::open(f_data.clone()).await.unwrap();

    let ring = test_io_ring();
    let commit_path = temp_dir.path().join("ro_commit.lsmt");
    let f_commit = Arc::new(LocalFile::new(&commit_path, ring).await.unwrap());
    ro_layer.commit(f_commit.clone()).await.unwrap();

    let final_ro = LSMTReadOnlyFile::open(f_commit).await.unwrap();
    let read_back = final_ro.read_at(8192, 4096).await.unwrap();
    assert_eq!(&read_back[..], &data_block[..]);

    let hole = final_ro.read_at(0, 4096).await.unwrap();
    assert_eq!(hole.iter().sum::<u8>(), 0);
}

#[tokio::test]
async fn test_prevent_rw_open_on_sealed() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 1024 * 1024;
    let (f_data, f_index, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    lsmt.write_at(0, &[0xCC; 512]).await.unwrap();

    lsmt.close_seal().await.unwrap();

    let rw_reopen_result =
        LSMTFile::open(f_data.clone(), Some(f_index.clone()), None, vec![]).await;

    assert!(
        rw_reopen_result.is_err(),
        "Should forbid opening a sealed file in RW mode"
    );
}

#[tokio::test]
async fn test_commit_close_seal() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 16 * 1024 * 1024;
    let (f_data, _f_index, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    let mut rng = StdRng::seed_from_u64(42);
    let mut shadow_mem = vec![0u8; vsize as usize];

    for _ in 0..20 {
        let offset = rng.random_range(0..vsize / 512) * 512;
        let len = (rng.random_range(1..10) * 512) as usize;
        if offset + len as u64 > vsize {
            continue;
        }
        let mut buf = vec![0u8; len];
        rng.fill(&mut buf[..]);
        shadow_mem[offset as usize..offset as usize + len].copy_from_slice(&buf);
        lsmt.write_at(offset, &buf).await.unwrap();
    }

    let ring = test_io_ring();
    let commit_path = temp_dir.path().join("commit.lsmt");
    let f_commit = Arc::new(LocalFile::new(&commit_path, ring).await.unwrap());
    lsmt.commit(f_commit.clone()).await.unwrap();

    let ro_commit = LSMTReadOnlyFile::open(f_commit).await.unwrap();
    for _ in 0..20 {
        let offset = rng.random_range(0..vsize / 512) * 512;
        let len = 512;
        if offset + len as u64 > vsize {
            continue;
        }
        let read_data = ro_commit.read_at(offset, len).await.unwrap();
        let expected = &shadow_mem[offset as usize..offset as usize + len];
        assert_eq!(
            &read_data[..],
            expected,
            "Commit file mismatch at offset {}",
            offset
        );
    }

    for _ in 0..10 {
        let offset = rng.random_range(0..vsize / 512) * 512;
        let len = 512;
        if offset + len as u64 > vsize {
            continue;
        }
        let mut buf = vec![0u8; len];
        rng.fill(&mut buf[..]);
        shadow_mem[offset as usize..offset as usize + len].copy_from_slice(&buf);
        lsmt.write_at(offset, &buf).await.unwrap();
    }

    lsmt.close_seal().await.unwrap();

    let write_err = lsmt.write_at(0, &[0u8; 512]).await;
    assert!(
        write_err.is_err(),
        "Should not be able to write after close_seal"
    );

    drop(lsmt);

    let ro_sealed = LSMTReadOnlyFile::open(f_data.clone()).await.unwrap();

    for _i in 0..50 {
        let offset = rng.random_range(0..vsize / 512) * 512;
        let len = 512;
        if offset + len as u64 > vsize {
            continue;
        }
        let read_data = ro_sealed.read_at(offset, len).await.unwrap();
        let expected = &shadow_mem[offset as usize..offset as usize + len];
        assert_eq!(
            &read_data[..],
            expected,
            "Sealed file mismatch at offset {}",
            offset
        );
    }
}

#[tokio::test]
async fn test_rand_rw_correctness() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 16 * 1024 * 1024;
    let (_f_data, _f_index, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    let mut rng = StdRng::seed_from_u64(12345);
    let mut shadow_mem = vec![0u8; vsize as usize];

    for _ in 0..50 {
        let offset = rng.random_range(0..vsize / 512) * 512;
        let len = (rng.random_range(1..10) * 512) as usize;
        if offset + len as u64 > vsize {
            continue;
        }

        let mut buf = vec![0u8; len];
        rng.fill(&mut buf[..]);

        shadow_mem[offset as usize..offset as usize + len].copy_from_slice(&buf);
        lsmt.write_at(offset, &buf).await.unwrap();
    }

    for i in 0..50 {
        let offset = rng.random_range(0..vsize / 512) * 512;
        let len = 512;
        if offset + len as u64 > vsize {
            continue;
        }

        let lsmt_data = lsmt.read_at(offset, len).await.unwrap();
        let shadow_data = &shadow_mem[offset as usize..offset as usize + len];

        assert_eq!(
            &lsmt_data[..],
            shadow_data,
            "Mismatch at iter {}, offset {}",
            i,
            offset
        );
    }
}

#[tokio::test]
async fn test_large_write_chunking() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 32 * 1024 * 1024; // 32MB
    let (_f_data, _f_index, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    let write_size = 10 * 1024 * 1024; // 一次性写入 10MB
    let mut large_data = vec![0xDD; write_size];

    large_data[0] = 0xAA;
    large_data[write_size - 1] = 0xBB;

    let written = lsmt.write_at(1024 * 1024, &large_data).await.unwrap();
    assert_eq!(written, write_size);

    {
        let idx = lsmt.index.read().await;
        assert!(
            idx.upper_len() >= 3,
            "Large write should be chunked into multiple mappings"
        );
    }

    let read_back = lsmt.read_at(1024 * 1024, write_size).await.unwrap();
    assert_eq!(read_back.len(), write_size);
    assert_eq!(read_back[0], 0xAA);
    assert_eq!(read_back[write_size - 1], 0xBB);
    assert!(read_back[1..write_size - 1].iter().all(|&b| b == 0xDD));
}

#[tokio::test]
async fn test_lsmt_factory_methods() {
    let temp_dir = TempDir::new().unwrap();
    let ring = test_io_ring();
    let data = Arc::new(
        LocalFile::new(temp_dir.path().join("rw.data"), ring.clone())
            .await
            .unwrap(),
    );
    let index = Arc::new(
        LocalFile::new(temp_dir.path().join("rw.index"), ring)
            .await
            .unwrap(),
    );
    let mut args = LayerInfo::new(data.clone(), Some(index.clone()), 8 * 1024 * 1024);
    args.uuid = Uuid::new_v4();

    let lsmt = create_file_rw(args.clone()).await.unwrap();
    lsmt.set_max_io_size(512 * 1024).unwrap();
    assert_eq!(lsmt.get_max_io_size(), 512 * 1024);
    assert!(lsmt.set_max_io_size(511 * 1024).is_err());
    assert_eq!(lsmt.get_uuid(0).unwrap(), args.uuid);

    let buf = vec![0xAB; 4096];
    lsmt.write_at(4096, &buf).await.unwrap();

    let mut segs = Vec::new();
    let n = lsmt.seek_data(0, 64 * 1024, &mut segs).await.unwrap();
    assert!(n > 0);
    assert!(!segs.is_empty());

    let stat = lsmt.data_stat().await.unwrap();
    assert!(stat.valid_data_size >= 4096);

    lsmt.update_vsize(4 * 1024 * 1024).await.unwrap();
    assert_eq!(lsmt.size().await.unwrap(), 4 * 1024 * 1024);
}

#[tokio::test]
async fn test_open_merge_stack_flatten() {
    let temp_dir = TempDir::new().unwrap();
    let ring = test_io_ring();
    let vsize = 4 * 1024 * 1024;

    let data0 = Arc::new(
        LocalFile::new(temp_dir.path().join("l0.data"), ring.clone())
            .await
            .unwrap(),
    );
    let index0 = Arc::new(
        LocalFile::new(temp_dir.path().join("l0.index"), ring.clone())
            .await
            .unwrap(),
    );
    let l0 = LSMTFile::create(data0.clone(), Some(index0), vsize, false)
        .await
        .unwrap();
    l0.write_at(4096, &[0x11; 4096]).await.unwrap();
    l0.close_seal().await.unwrap();
    drop(l0);

    let data1 = Arc::new(
        LocalFile::new(temp_dir.path().join("l1.data"), ring.clone())
            .await
            .unwrap(),
    );
    let index1 = Arc::new(
        LocalFile::new(temp_dir.path().join("l1.index"), ring.clone())
            .await
            .unwrap(),
    );
    let l1 = LSMTFile::create(data1.clone(), Some(index1), vsize, false)
        .await
        .unwrap();
    l1.write_at(8192, &[0x22; 4096]).await.unwrap();
    l1.close_seal().await.unwrap();
    drop(l1);

    let lower = open_files_ro(&[data0.clone(), data1.clone()])
        .await
        .unwrap();
    assert_eq!(lower.read_at(4096, 4096).await.unwrap()[0], 0x11);
    assert_eq!(lower.read_at(8192, 4096).await.unwrap()[0], 0x22);

    let merged = Arc::new(
        LocalFile::new(temp_dir.path().join("merged.lsmt"), ring.clone())
            .await
            .unwrap(),
    );
    merge_files_ro(
        &[data0.clone(), data1.clone()],
        CommitArgs::new(merged.clone()),
    )
    .await
    .unwrap();
    let merged_ro = open_file_ro(merged.clone()).await.unwrap();
    assert_eq!(merged_ro.read_at(4096, 4096).await.unwrap()[0], 0x11);
    assert_eq!(merged_ro.read_at(8192, 4096).await.unwrap()[0], 0x22);

    let up_data = Arc::new(
        LocalFile::new(temp_dir.path().join("up.data"), ring.clone())
            .await
            .unwrap(),
    );
    let up_index = Arc::new(
        LocalFile::new(temp_dir.path().join("up.index"), ring.clone())
            .await
            .unwrap(),
    );
    let upper = LSMTFile::create(up_data, Some(up_index), vsize, false)
        .await
        .unwrap();
    let stacked = stack_files(&upper, &lower, true).await.unwrap();
    stacked.write_at(4096, &[0x33; 4096]).await.unwrap();
    assert_eq!(stacked.read_at(4096, 4096).await.unwrap()[0], 0x33);

    let flat = Arc::new(
        LocalFile::new(temp_dir.path().join("flat.lsmt"), ring)
            .await
            .unwrap(),
    );
    stacked.flatten(flat.clone()).await.unwrap();
    let flat_ro = open_file_ro(flat).await.unwrap();
    assert_eq!(flat_ro.read_at(4096, 4096).await.unwrap()[0], 0x33);
    assert_eq!(flat_ro.read_at(8192, 4096).await.unwrap()[0], 0x22);
}

#[tokio::test]
async fn test_flatten_keeps_zero_written_blocks_as_data_segments() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 4096;
    let (data, _index, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    lsmt.write_at(0, &[0u8; 4096]).await.unwrap();
    lsmt.close_seal().await.unwrap();
    drop(lsmt);

    let ro = open_file_ro(data.clone()).await.unwrap();
    let source_mappings = ro
        .index_view
        .as_ref()
        .expect("readonly source index view")
        .mappings()
        .to_vec();
    assert!(
        source_mappings.iter().all(|m| !m.zeroed),
        "source image should keep zero-written blocks as data mappings before compact"
    );
    let ring = test_io_ring();
    let flat = Arc::new(
        LocalFile::new(temp_dir.path().join("flat-zero.lsmt"), ring)
            .await
            .unwrap(),
    );
    ro.flatten(flat.clone()).await.unwrap();

    let flat_ro = open_file_ro(flat).await.unwrap();
    let mappings = flat_ro
        .index_view
        .as_ref()
        .expect("readonly index view")
        .mappings();
    assert!(
        !mappings.is_empty(),
        "flattened image should contain mappings"
    );
    assert!(
        mappings.iter().all(|m| !m.zeroed),
        "upstream compact currently materializes zero-written blocks as data mappings"
    );
    assert_eq!(
        flat_ro.read_at(0, 4096).await.unwrap().as_ref(),
        &[0u8; 4096]
    );
}

#[tokio::test]
async fn test_compact_to_rejects_short_reads() {
    let temp_dir = TempDir::new().unwrap();
    let ring = test_io_ring();
    let src: Arc<dyn VirtualFile> = Arc::new(ShortReadFile::new(vec![0x5a; 4096], 1));
    let dest: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(temp_dir.path().join("short-read-flat.lsmt"), ring)
            .await
            .unwrap(),
    );
    let args = CommitArgs::new(dest);
    let err = compact_to(
        &[src],
        &[SegmentMapping::new(0, 8, 0, false, 0)],
        4096,
        args,
    )
    .await
    .expect_err("short reads during compact should fail");
    assert_err_contains(&err, "failed to read");
}

#[tokio::test]
async fn test_sparse_factory_without_index() {
    let temp_dir = TempDir::new().unwrap();
    let ring = test_io_ring();
    let data = Arc::new(
        LocalFile::new(temp_dir.path().join("sparse.data"), ring)
            .await
            .unwrap(),
    );

    let mut args = LayerInfo::new(data.clone(), None, 8 * 1024 * 1024);
    args.rw_layout = RwLayout::Sparse;
    let lsmt = create_file_rw(args).await.unwrap();

    let payload = vec![0x5A; 4096];
    lsmt.write_at(4096, &payload).await.unwrap();
    drop(lsmt);

    let reopened = open_file_rw(data.clone(), None).await.unwrap();
    let out = reopened.read_at(4096, 4096).await.unwrap();
    assert_eq!(&out[..], &payload[..]);
}

fn test_layer_metadata(uuid: Uuid, index_size: u64) -> ReadOnlyLayerMetadata {
    ReadOnlyLayerMetadata {
        uuid,
        file_size: 1024 * 1024,
        virtual_size: 512 * 1024,
        index_offset: 4096,
        index_size,
        header_version: 1,
        header_sub_version: 1,
        trailer_version: 1,
        trailer_sub_version: 1,
    }
}

#[test]
fn test_premerged_index_cache_key_metadata_identity() {
    let base_uuid = Uuid::new_v4();
    let top_uuid = Uuid::new_v4();
    let metadata = vec![
        test_layer_metadata(base_uuid, 3),
        test_layer_metadata(top_uuid, 4),
    ];

    let key0 = PremergedIndexCacheKey::from_metadata(&metadata).unwrap();
    let key1 = PremergedIndexCacheKey::from_metadata(&metadata).unwrap();
    assert_eq!(key0.digest, key1.digest);

    let mut reversed = metadata.clone();
    reversed.reverse();
    let reversed_key = PremergedIndexCacheKey::from_metadata(&reversed).unwrap();
    assert_ne!(key0.digest, reversed_key.digest);

    let mut changed = metadata.clone();
    changed[0].index_size += 1;
    let changed_key = PremergedIndexCacheKey::from_metadata(&changed).unwrap();
    assert_ne!(key0.digest, changed_key.digest);

    let mut nil_uuid = metadata;
    nil_uuid[0].uuid = Uuid::nil();
    assert!(PremergedIndexCacheKey::from_metadata(&nil_uuid).is_none());
}

#[test]
fn test_premerged_artifact_header_has_expected_wire_len() {
    let metadata = vec![
        test_layer_metadata(Uuid::new_v4(), 3),
        test_layer_metadata(Uuid::new_v4(), 4),
    ];
    let key = PremergedIndexCacheKey::from_metadata(&metadata).unwrap();
    let header = build_premerged_artifact_header(&key, 0, 0, [0; 32]);

    assert_eq!(header.len(), PREMERGED_INDEX_HEADER_LEN);
    assert_eq!(
        read_u32(&header, 16).unwrap() as usize,
        PREMERGED_INDEX_HEADER_LEN
    );
}

#[test]
fn test_premerged_artifact_rejects_invalid_mapping_order_and_tag() {
    let body = serialize_premerged_mappings(&[
        SegmentMapping::new(10, 1, 20, false, 0),
        SegmentMapping::new(9, 1, 30, false, 0),
    ]);
    let err = deserialize_premerged_mappings(&body, 2, 1).unwrap_err();
    assert_err_contains(&err, "not sorted or overlap");

    let body = serialize_premerged_mappings(&[SegmentMapping::new(0, 1, 20, false, 1)]);
    let err = deserialize_premerged_mappings(&body, 1, 1).unwrap_err();
    assert_err_contains(&err, "tag out of range");
}

#[tokio::test]
async fn test_premerged_index_lock_map_drops_idle_entries() {
    let key = format!("test-lock-{}", Uuid::new_v4());
    let lock = acquire_premerged_index_lock(&key).await;
    assert_eq!(Arc::strong_count(&lock), 1);
    {
        let locks = premerged_index_locks().lock().await;
        assert!(locks.get(&key).and_then(Weak::upgrade).is_some());
    }

    release_premerged_index_lock(&key, &lock).await;
    {
        let locks = premerged_index_locks().lock().await;
        assert!(!locks.contains_key(&key));
    }

    let stale_key = format!("test-stale-lock-{}", Uuid::new_v4());
    let stale = acquire_premerged_index_lock(&stale_key).await;
    drop(stale);
    {
        let locks = premerged_index_locks().lock().await;
        assert!(locks
            .get(&stale_key)
            .is_some_and(|lock| lock.upgrade().is_none()));
    }

    let replacement = acquire_premerged_index_lock(&stale_key).await;
    {
        let locks = premerged_index_locks().lock().await;
        assert!(locks
            .get(&stale_key)
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, &replacement)));
    }
    release_premerged_index_lock(&stale_key, &replacement).await;
}

fn premerged_artifact_count(cache_dir: &std::path::Path) -> usize {
    let dir = cache_dir.join(PREMERGED_INDEX_DIR);
    match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension().and_then(|v| v.to_str()) == Some(PREMERGED_INDEX_EXT)
            })
            .count(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => panic!("read artifact dir: {err}"),
    }
}

async fn wait_premerged_artifact_count(cache_dir: &std::path::Path, expected: usize) {
    for _ in 0..100 {
        if premerged_artifact_count(cache_dir) == expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(premerged_artifact_count(cache_dir), expected);
}

async fn wait_readable_premerged_artifact(
    path: &std::path::Path,
    key: &PremergedIndexCacheKey,
) -> Arc<ReadOnlyIndex> {
    let mut last_err = None;
    for _ in 0..100 {
        match tokio::fs::read(path).await {
            Ok(bytes) => match decode_premerged_index_artifact(&bytes, key) {
                Ok(index) => return index,
                Err(err) => last_err = Some(err),
            },
            Err(err) => last_err = Some(err.into()),
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("premerged artifact did not become readable: {:?}", last_err);
}

#[tokio::test]
async fn test_premerged_index_artifact_hit_skips_layer_index_load() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 4 * 1024 * 1024;
    let base = create_sealed_layer(&temp_dir, "pm-base", vsize, &[(4096, 0x11)]).await;
    let top = create_sealed_layer(&temp_dir, "pm-top", vsize, &[(8192, 0x22)]).await;
    let files: Vec<Arc<dyn VirtualFile>> = vec![
        base.clone() as Arc<dyn VirtualFile>,
        top.clone() as Arc<dyn VirtualFile>,
    ];
    let cache_dir = temp_dir.path().join("cache");
    let policy = PremergedIndexCachePolicy::hybrid(1);

    let lower = open_files_ro_with_premerged_cache(&files, &cache_dir, policy)
        .await
        .unwrap();
    assert_eq!(lower.read_at(4096, 4096).await.unwrap()[0], 0x11);
    assert_eq!(lower.read_at(8192, 4096).await.unwrap()[0], 0x22);
    wait_premerged_artifact_count(&cache_dir, 1).await;
    assert_eq!(premerged_artifact_count(&cache_dir), 1);

    let metadata = load_readonly_layers_metadata(&files).await.unwrap();
    let wrapped: Vec<Arc<dyn VirtualFile>> = files
        .iter()
        .cloned()
        .zip(metadata.iter())
        .map(|(file, layer)| {
            Arc::new(FailingIndexReadFile::new(
                file,
                layer.index_offset,
                layer.index_size,
            )) as Arc<dyn VirtualFile>
        })
        .collect();

    let hit_policy = PremergedIndexCachePolicy {
        read: true,
        write: false,
        max_dir_bytes: policy.max_dir_bytes,
    };
    let lower = open_files_ro_with_premerged_cache(&wrapped, &cache_dir, hit_policy)
        .await
        .expect("artifact hit should avoid reading per-layer indexes");
    assert_eq!(lower.read_at(4096, 4096).await.unwrap()[0], 0x11);
    assert_eq!(lower.read_at(8192, 4096).await.unwrap()[0], 0x22);
}

#[tokio::test]
async fn test_premerged_index_artifact_corruption_falls_back_and_rewrites() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 4 * 1024 * 1024;
    let base = create_sealed_layer(&temp_dir, "bad-base", vsize, &[(4096, 0x31)]).await;
    let top = create_sealed_layer(&temp_dir, "bad-top", vsize, &[(8192, 0x32)]).await;
    let files: Vec<Arc<dyn VirtualFile>> = vec![
        base.clone() as Arc<dyn VirtualFile>,
        top.clone() as Arc<dyn VirtualFile>,
    ];
    let cache_dir = temp_dir.path().join("cache");
    let policy = PremergedIndexCachePolicy::hybrid(1);

    open_files_ro_with_premerged_cache(&files, &cache_dir, policy)
        .await
        .unwrap();
    wait_premerged_artifact_count(&cache_dir, 1).await;
    let metadata = load_readonly_layers_metadata(&files).await.unwrap();
    let key = PremergedIndexCacheKey::from_metadata(&metadata).unwrap();
    let artifact_path = key.artifact_path(&cache_dir);
    tokio::fs::write(&artifact_path, b"corrupt").await.unwrap();

    let lower = open_files_ro_with_premerged_cache(&files, &cache_dir, policy)
        .await
        .unwrap();
    assert_eq!(lower.read_at(4096, 4096).await.unwrap()[0], 0x31);
    assert_eq!(lower.read_at(8192, 4096).await.unwrap()[0], 0x32);
    wait_premerged_artifact_count(&cache_dir, 1).await;

    let rewritten = wait_readable_premerged_artifact(&artifact_path, &key).await;
    assert!(!rewritten.mappings().is_empty());
}

#[tokio::test]
async fn test_open_files_ro_uses_topmost_nonzero_vsize() {
    let temp_dir = TempDir::new().unwrap();
    let ring = test_io_ring();
    let base_data = Arc::new(
        LocalFile::new(temp_dir.path().join("base.data"), ring.clone())
            .await
            .unwrap(),
    );
    let base_index = Arc::new(
        LocalFile::new(temp_dir.path().join("base.index"), ring.clone())
            .await
            .unwrap(),
    );
    let base = LSMTFile::create(base_data.clone(), Some(base_index), 2 * 1024 * 1024, false)
        .await
        .unwrap();
    base.close_seal().await.unwrap();
    drop(base);

    let mid_data = Arc::new(
        LocalFile::new(temp_dir.path().join("mid.data"), ring.clone())
            .await
            .unwrap(),
    );
    let mid_index = Arc::new(
        LocalFile::new(temp_dir.path().join("mid.index"), ring.clone())
            .await
            .unwrap(),
    );
    let mid = LSMTFile::create(mid_data.clone(), Some(mid_index), 4 * 1024 * 1024, false)
        .await
        .unwrap();
    mid.close_seal().await.unwrap();
    drop(mid);

    let top_data = Arc::new(
        LocalFile::new(temp_dir.path().join("top.data"), ring.clone())
            .await
            .unwrap(),
    );
    let top_index = Arc::new(
        LocalFile::new(temp_dir.path().join("top.index"), ring)
            .await
            .unwrap(),
    );
    let top = LSMTFile::create(top_data.clone(), Some(top_index), 0, false)
        .await
        .unwrap();
    top.close_seal().await.unwrap();
    drop(top);

    let lower = open_files_ro(&[base_data, mid_data, top_data])
        .await
        .unwrap();
    assert_eq!(lower.size().await.unwrap(), 4 * 1024 * 1024);
}

#[tokio::test]
async fn test_stack_files_check_order_detects_uuid_chain_mismatch() {
    let temp_dir = TempDir::new().unwrap();
    let ring = test_io_ring();
    let vsize = 4 * 1024 * 1024;

    let base_uuid = Uuid::new_v4();
    let mid_uuid = Uuid::new_v4();
    let top_uuid = Uuid::new_v4();
    let wrong_parent = Uuid::new_v4();
    let base_data = Arc::new(
        LocalFile::new(temp_dir.path().join("chain.base.data"), ring.clone())
            .await
            .unwrap(),
    );
    let base_index = Arc::new(
        LocalFile::new(temp_dir.path().join("chain.base.index"), ring.clone())
            .await
            .unwrap(),
    );
    let mut base_args = LayerInfo::new(base_data.clone(), Some(base_index), vsize);
    base_args.uuid = base_uuid;
    let base = create_file_rw(base_args).await.unwrap();
    base.write_at(4096, &[0xA5; 4096]).await.unwrap();
    base.close_seal().await.unwrap();
    drop(base);

    let mid_data = Arc::new(
        LocalFile::new(temp_dir.path().join("chain.mid.data"), ring.clone())
            .await
            .unwrap(),
    );
    let mid_index = Arc::new(
        LocalFile::new(temp_dir.path().join("chain.mid.index"), ring.clone())
            .await
            .unwrap(),
    );
    let mut mid_args = LayerInfo::new(mid_data.clone(), Some(mid_index), vsize);
    mid_args.uuid = mid_uuid;
    mid_args.parent_uuid = Some(wrong_parent);
    let mid = create_file_rw(mid_args).await.unwrap();
    mid.write_at(4096, &[0xA5; 4096]).await.unwrap();
    mid.close_seal().await.unwrap();
    drop(mid);

    let top_data = Arc::new(
        LocalFile::new(temp_dir.path().join("chain.top.data"), ring.clone())
            .await
            .unwrap(),
    );
    let top_index = Arc::new(
        LocalFile::new(temp_dir.path().join("chain.top.index"), ring.clone())
            .await
            .unwrap(),
    );
    let mut top_args = LayerInfo::new(top_data.clone(), Some(top_index), vsize);
    top_args.uuid = top_uuid;
    top_args.parent_uuid = Some(mid_uuid);
    let top = create_file_rw(top_args).await.unwrap();
    top.write_at(4096, &[0xA5; 4096]).await.unwrap();
    top.close_seal().await.unwrap();
    drop(top);

    let base = base_data;
    let mid = mid_data;
    let top = top_data;
    let lower = open_files_ro(&[base, mid, top]).await.unwrap();

    let upper_data = Arc::new(
        LocalFile::new(temp_dir.path().join("upper.data"), ring.clone())
            .await
            .unwrap(),
    );
    let upper_index = Arc::new(
        LocalFile::new(temp_dir.path().join("upper.index"), ring)
            .await
            .unwrap(),
    );
    let upper = LSMTFile::create(upper_data, Some(upper_index), vsize, false)
        .await
        .unwrap();

    let stacked = stack_files(&upper, &lower, true).await;
    assert!(stacked.is_err(), "check_order should reject UUID mismatch");
}

#[tokio::test]
async fn test_export_upper_as_sealed() {
    let temp_dir = TempDir::new().unwrap();
    let vsize: u64 = 10 * 1024 * 1024;

    // 1. Create and seal a base (lower) layer
    let (base_data, _base_index, base_lsmt) = create_lsmt_env(&temp_dir, vsize).await;
    let base_content = vec![0xAAu8; 4096];
    base_lsmt.write_at(0, &base_content).await.unwrap();
    base_lsmt.write_at(8192, &base_content).await.unwrap();
    base_lsmt.close_seal().await.unwrap();
    drop(base_lsmt);

    // 2. Open base as RO, create upper RW layer, stack them
    let ring = test_io_ring();
    let base_ro = LSMTReadOnlyFile::open(base_data.clone()).await.unwrap();

    let upper_data_path = temp_dir.path().join("upper_data.lsmt");
    let upper_index_path = temp_dir.path().join("upper_index.lsmt");
    let upper_data = Arc::new(
        LocalFile::new(&upper_data_path, ring.clone())
            .await
            .unwrap(),
    );
    let upper_index = Arc::new(
        LocalFile::new(&upper_index_path, ring.clone())
            .await
            .unwrap(),
    );
    let upper_rw = LSMTFile::create(upper_data.clone(), Some(upper_index.clone()), vsize, false)
        .await
        .unwrap();
    let stacked = stack_files(&upper_rw, &base_ro, false).await.unwrap();

    // 3. Write dirty blocks to the stacked file
    let dirty1 = vec![0xBBu8; 4096];
    stacked.write_at(0, &dirty1).await.unwrap(); // overwrite a base block
    let dirty2 = vec![0xCCu8; 4096];
    stacked.write_at(4096, &dirty2).await.unwrap(); // new block (not in base)

    // 4. Call export_upper_as_sealed — writes only upper dirty blocks to a new file
    let snap_path = temp_dir.path().join("snapshot.lsmt");
    let snap_file: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::new(&snap_path, ring.clone()).await.unwrap());
    let snap_uuid = Uuid::new_v4();
    stacked
        .export_upper_as_sealed(CommitArgs {
            writer: Arc::new(VirtualFileWriter::new(snap_file.clone())),
            uuid: Some(snap_uuid),
            parent_uuid: None,
            user_tag: None,
            concurrency: 1,
        })
        .await
        .unwrap();

    // 5. Verify snapshot's parent_uuid was auto-set to the base layer's UUID
    let base_size = base_data.size().await.unwrap();
    let base_trailer = verify_ht(
        &(base_data.clone() as Arc<dyn VirtualFile>),
        true,
        base_size,
    )
    .await
    .unwrap();
    let base_uuid = parse_uuid_field(&base_trailer.uuid).unwrap();

    let snap_size = snap_file.size().await.unwrap();
    let snap_header = verify_ht(
        &(snap_file.clone() as Arc<dyn VirtualFile>),
        false,
        snap_size,
    )
    .await
    .unwrap();
    let snap_parent_uuid = parse_uuid_field(&snap_header.parent_uuid);
    assert_eq!(
        snap_parent_uuid,
        Some(base_uuid),
        "snapshot parent_uuid should be auto-set to the base layer's UUID"
    );

    // 6. Verify snapshot is a valid sealed LSMT
    let snap_ro = LSMTReadOnlyFile::open(snap_file).await.unwrap();

    // 7. Verify snapshot contains only upper dirty blocks
    let read0 = snap_ro.read_at(0, 4096).await.unwrap();
    assert_eq!(
        &read0[..],
        &dirty1[..],
        "snapshot should have dirty1 at offset 0"
    );

    let read4k = snap_ro.read_at(4096, 4096).await.unwrap();
    assert_eq!(
        &read4k[..],
        &dirty2[..],
        "snapshot should have dirty2 at offset 4096"
    );

    // offset 8192 was written only in base, not in upper -> zeros in snapshot
    let read8k = snap_ro.read_at(8192, 4096).await.unwrap();
    assert!(
        read8k.iter().all(|&b| b == 0),
        "snapshot should have zeros at offset 8192 (base-only data not included)"
    );

    // 8. Verify the source LSMTFile is NOT sealed — further writes succeed
    let dirty3 = vec![0xDDu8; 4096];
    stacked
        .write_at(12288, &dirty3)
        .await
        .expect("source should still accept writes after snapshot_to");

    // 9. Verify source reads correctly (upper takes priority over lower)
    let read0_src = stacked.read_at(0, 4096).await.unwrap();
    assert_eq!(
        &read0_src[..],
        &dirty1[..],
        "source should read upper dirty1 at 0"
    );

    let read8k_src = stacked.read_at(8192, 4096).await.unwrap();
    assert_eq!(
        &read8k_src[..],
        &base_content[..],
        "source should fall through to base at 8192"
    );

    // 10. Second snapshot includes ALL accumulated dirty blocks (dirty1 + dirty2 + dirty3)
    let snap2_path = temp_dir.path().join("snapshot2.lsmt");
    let snap2_file: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::new(&snap2_path, ring.clone()).await.unwrap());
    stacked
        .export_upper_as_sealed(CommitArgs {
            writer: Arc::new(VirtualFileWriter::new(snap2_file.clone())),
            uuid: Some(Uuid::new_v4()),
            parent_uuid: None,
            user_tag: None,
            concurrency: 1,
        })
        .await
        .unwrap();

    let snap2_ro = LSMTReadOnlyFile::open(snap2_file).await.unwrap();

    let read12k_snap2 = snap2_ro.read_at(12288, 4096).await.unwrap();
    assert_eq!(
        &read12k_snap2[..],
        &dirty3[..],
        "second snapshot must include dirty3"
    );

    let read0_snap2 = snap2_ro.read_at(0, 4096).await.unwrap();
    assert_eq!(
        &read0_snap2[..],
        &dirty1[..],
        "second snapshot must still have dirty1"
    );

    let read4k_snap2 = snap2_ro.read_at(4096, 4096).await.unwrap();
    assert_eq!(
        &read4k_snap2[..],
        &dirty2[..],
        "second snapshot must still have dirty2"
    );
}

#[tokio::test]
async fn test_export_upper_as_sealed_parent_uuid() {
    let temp_dir = TempDir::new().unwrap();
    let vsize: u64 = 10 * 1024 * 1024;

    // 1. Create and seal a base (lower) layer
    let (base_data, _base_index, base_lsmt) = create_lsmt_env(&temp_dir, vsize).await;
    let base_content = vec![0xAAu8; 4096];
    base_lsmt.write_at(0, &base_content).await.unwrap();
    base_lsmt.write_at(8192, &base_content).await.unwrap();
    base_lsmt.close_seal().await.unwrap();
    drop(base_lsmt);

    let mut lower_layers: Vec<Arc<dyn VirtualFile>> = vec![base_data];
    let ring = test_io_ring();

    for id in 1..=5 {
        // 2. Open base as RO, create upper RW layer, stack them
        let base_ro = open_files_ro(&lower_layers).await.unwrap();

        let upper_data_path = temp_dir.path().join(format!("upper_data{id}.lsmt"));
        let upper_index_path = temp_dir.path().join(format!("upper_index{id}.lsmt"));
        let upper_data = Arc::new(
            LocalFile::new(&upper_data_path, ring.clone())
                .await
                .unwrap(),
        );
        let upper_index = Arc::new(
            LocalFile::new(&upper_index_path, ring.clone())
                .await
                .unwrap(),
        );
        let upper_rw =
            LSMTFile::create(upper_data.clone(), Some(upper_index.clone()), vsize, false)
                .await
                .unwrap();
        let stacked = stack_files(&upper_rw, &base_ro, false).await.unwrap();

        // 3. Write dirty blocks to the stacked file
        let dirty1 = vec![0xBBu8; 4096];
        stacked.write_at(2 * id * 4096, &dirty1).await.unwrap(); // overwrite a base block
        let dirty2 = vec![0xCCu8; 4096];
        stacked
            .write_at((2 * id + 1) * 4096, &dirty2)
            .await
            .unwrap(); // new block (not in base)

        // 4. Call export_upper_as_sealed — writes only upper dirty blocks to a new file
        let snap_path = temp_dir.path().join(format!("snapshot{id}.lsmt"));
        let snap_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&snap_path, ring.clone()).await.unwrap());
        let snap_uuid = Uuid::new_v4();
        stacked
            .export_upper_as_sealed(CommitArgs {
                writer: Arc::new(VirtualFileWriter::new(snap_file.clone())),
                uuid: Some(snap_uuid),
                parent_uuid: None,
                user_tag: None,
                concurrency: 1,
            })
            .await
            .unwrap();
        lower_layers.push(snap_file);
    }

    let mut headers = vec![];
    for layer in lower_layers.iter() {
        let header = verify_ht(layer, true, layer.size().await.unwrap())
            .await
            .unwrap();
        headers.push(header);
    }
    for (curr, next) in headers.iter().zip(headers.iter().skip(1)) {
        assert_eq!(curr.uuid, next.parent_uuid);
    }
}

/// Helper: assert read_at and read_at_into return the same data.
async fn assert_read_at_into_matches(file: &dyn VirtualFile, offset: u64, len: usize) {
    let bytes_result = file.read_at(offset, len).await.expect("read_at failed");
    let mut into_buf = vec![0u8; len];
    let n = file
        .read_at_into(offset, &mut into_buf)
        .await
        .expect("read_at_into failed");
    assert_eq!(
        n,
        bytes_result.len(),
        "read_at_into length mismatch at offset={offset} len={len}"
    );
    assert_eq!(
        &into_buf[..n],
        &bytes_result[..],
        "read_at_into data mismatch at offset={offset} len={len}"
    );
}

#[tokio::test]
async fn test_lsmt_read_at_into_all_holes() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 64 * 1024u64;
    let (_, _, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    // Read from a region with no writes — should be all zeros.
    assert_read_at_into_matches(&lsmt, 0, 4096).await;
    assert_read_at_into_matches(&lsmt, 512, 512).await;
    assert_read_at_into_matches(&lsmt, 0, vsize as usize).await;
}

#[tokio::test]
async fn test_lsmt_read_at_into_single_write() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 64 * 1024u64;
    let (_, _, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    let data = vec![0xAA; 4096];
    lsmt.write_at(4096, &data).await.unwrap();

    // Exact read on written region
    assert_read_at_into_matches(&lsmt, 4096, 4096).await;
    // Read spanning hole + data
    assert_read_at_into_matches(&lsmt, 0, 8192).await;
    // Read spanning data + hole
    assert_read_at_into_matches(&lsmt, 4096, 8192).await;
}

#[tokio::test]
async fn test_lsmt_read_at_into_multiple_writes() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 64 * 1024u64;
    let (_, _, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    // Write several non-contiguous regions
    let data_a = vec![0xAA; 512];
    let data_b = vec![0xBB; 1024];
    let data_c = vec![0xCC; 4096];
    lsmt.write_at(512, &data_a).await.unwrap();
    lsmt.write_at(2048, &data_b).await.unwrap();
    lsmt.write_at(8192, &data_c).await.unwrap();

    // Read spanning all three writes with holes in between
    assert_read_at_into_matches(&lsmt, 0, 16384).await;
    // Read individual segments
    assert_read_at_into_matches(&lsmt, 512, 512).await;
    assert_read_at_into_matches(&lsmt, 2048, 1024).await;
    assert_read_at_into_matches(&lsmt, 8192, 4096).await;
}

#[tokio::test]
async fn test_lsmt_read_at_into_beyond_eof() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 4096u64;
    let (_, _, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    // Read beyond virtual size — should return 0 bytes
    let mut buf = vec![0xFFu8; 512];
    let n = lsmt.read_at_into(vsize, &mut buf).await.unwrap();
    assert_eq!(n, 0);

    // Read straddling the end
    assert_read_at_into_matches(&lsmt, 0, vsize as usize + 512).await;
}

#[tokio::test]
async fn test_lsmt_read_at_into_empty_buffer() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 4096u64;
    let (_, _, lsmt) = create_lsmt_env(&temp_dir, vsize).await;

    let mut buf = [];
    let n = lsmt.read_at_into(0, &mut buf).await.unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn test_lsmt_readonly_read_at_into() {
    // Create and seal a base layer with some data
    let base_dir = TempDir::new().unwrap();
    let vsize = 64 * 1024u64;
    let (f_data, _f_index, lsmt) = create_lsmt_env(&base_dir, vsize).await;

    let data_a = vec![0xAA; 4096];
    let data_b = vec![0xBB; 512];
    lsmt.write_at(0, &data_a).await.unwrap();
    lsmt.write_at(8192, &data_b).await.unwrap();
    lsmt.close_seal().await.unwrap();
    drop(lsmt);

    let base_ro_index = load_base_index(f_data.clone()).await;

    // Create a raw file that serves as the base layer's data
    let ring = test_io_ring();
    let raw_base_path = base_dir.path().join("base.raw");
    let raw_base_file = Arc::new(LocalFile::new(&raw_base_path, ring).await.unwrap());
    raw_base_file.write_at(0, &data_a).await.unwrap();
    raw_base_file.write_at(8192, &data_b).await.unwrap();

    // Create a proper upper LSMT layer (in a separate dir to avoid name collision)
    let upper_dir = TempDir::new().unwrap();
    let (upper_data, upper_index, upper_lsmt) = create_lsmt_env(&upper_dir, vsize).await;
    drop(upper_lsmt);

    let stacked = LSMTFile::open(
        upper_data,
        Some(upper_index),
        Some(Arc::new(base_ro_index)),
        vec![raw_base_file],
    )
    .await
    .unwrap();

    // Reads go through the base (LSMTReadOnlyFile) layer
    assert_read_at_into_matches(&stacked, 0, 4096).await;
    assert_read_at_into_matches(&stacked, 8192, 512).await;
    assert_read_at_into_matches(&stacked, 0, 16384).await;
    // Hole
    assert_read_at_into_matches(&stacked, 4096, 4096).await;
}

async fn open_mapped_read_stacks(
    mapped_data_len: usize,
    trim: usize,
) -> (LSMTReadOnlyFile, LSMTFile) {
    let base_dir = TempDir::new().unwrap();
    let vsize = 64 * 1024u64;
    let (f_data, _f_index, lsmt) = create_lsmt_env(&base_dir, vsize).await;

    let data = vec![0xEE; 4096];
    lsmt.write_at(0, &data).await.unwrap();
    lsmt.close_seal().await.unwrap();
    drop(lsmt);

    let base_ro_index = Arc::new(load_base_index(f_data.clone()).await);
    let opened_readonly = open_files_ro(&[f_data.clone() as Arc<dyn VirtualFile>])
        .await
        .unwrap();
    let merged_ro_index = opened_readonly.index_view().unwrap();
    let mapping = base_ro_index
        .mappings()
        .first()
        .expect("test layer should contain a mapped extent");
    let physical_offset = (mapping.moffset * ALIGNMENT) as usize;
    let mut layer_data = vec![0u8; physical_offset];
    layer_data.extend(mapped_pattern(mapped_data_len));
    let layer: Arc<dyn VirtualFile> = Arc::new(ShortReadFile::new(layer_data, trim));
    let readonly = LSMTReadOnlyFile::from_merged_layers(
        vec![layer.clone()],
        merged_ro_index,
        vsize,
        vec![Uuid::nil()],
        LSMTFileType::ReadOnly,
    );

    let upper_dir = TempDir::new().unwrap();
    let (upper_data, upper_index, upper_lsmt) = create_lsmt_env(&upper_dir, vsize).await;
    drop(upper_lsmt);

    let writable = LSMTFile::open(
        upper_data,
        Some(upper_index),
        Some(base_ro_index),
        vec![layer],
    )
    .await
    .unwrap();

    (readonly, writable)
}

fn mapped_pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

async fn assert_mapped_read_equals(file: &dyn VirtualFile, expected: &[u8]) {
    let data = file
        .read_at(0, expected.len())
        .await
        .expect("mapped read_at");
    assert_eq!(data.as_ref(), expected);

    let mut dst = vec![0u8; expected.len()];
    let n = file
        .read_at_into(0, &mut dst)
        .await
        .expect("mapped read_at_into");
    assert_eq!(n, expected.len());
    assert_eq!(dst, expected);
}

async fn assert_truncated_mapped_read_fails(file: &dyn VirtualFile) {
    let err = file.read_at(0, 4096).await.unwrap_err();
    assert_err_contains(&err, "unexpected EOF");

    let mut dst = vec![0u8; 4096];
    let err = file.read_at_into(0, &mut dst).await.unwrap_err();
    assert_err_contains(&err, "unexpected EOF");
}

#[tokio::test]
async fn test_lsmt_retries_short_mapped_reads() {
    let expected = mapped_pattern(4096);
    let (readonly, writable) = open_mapped_read_stacks(4096, 128).await;

    assert_mapped_read_equals(&readonly, &expected).await;
    assert_mapped_read_equals(&writable, &expected).await;
}

#[tokio::test]
async fn test_lsmt_rejects_truncated_mapped_reads() {
    let (readonly, writable) = open_mapped_read_stacks(4096 - 128, 0).await;

    assert_truncated_mapped_read_fails(&readonly).await;
    assert_truncated_mapped_read_fails(&writable).await;
}

#[tokio::test]
async fn test_read_partial_block() {
    let dir = TempDir::new().unwrap();
    let (_, _, lsmt) = create_lsmt_env(&dir, 16384).await;

    let data = vec![0xAB; 4096];
    lsmt.write_at(0, &data).await.unwrap();

    let mut buf = vec![0u8; 512];
    lsmt.read_at_into(512, &mut buf).await.unwrap();
    assert_eq!(buf[0], 0xAB);
}

#[tokio::test]
async fn test_write_multiple_blocks() {
    let dir = TempDir::new().unwrap();
    let (_, _, lsmt) = create_lsmt_env(&dir, 16384).await;

    let data = vec![0xCD; 8192];
    lsmt.write_at(0, &data).await.unwrap();

    let mut buf = vec![0u8; 8192];
    lsmt.read_at_into(0, &mut buf).await.unwrap();
    assert_eq!(buf, data);
}

#[tokio::test]
async fn test_read_at_vsize_boundary() {
    let dir = TempDir::new().unwrap();
    let (_, _, lsmt) = create_lsmt_env(&dir, 4096).await;

    let mut buf = vec![0u8; 512];
    let n = lsmt.read_at_into(4096, &mut buf).await.unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn test_lsmt_size() {
    let dir = TempDir::new().unwrap();
    let (_, _, lsmt) = create_lsmt_env(&dir, 8192).await;
    assert_eq!(lsmt.size().await.unwrap(), 8192);
}

#[tokio::test]
async fn test_sparse_write_read() {
    let dir = TempDir::new().unwrap();
    let (_, lsmt) = create_sparse_lsmt_env(&dir, 16384).await;
    lsmt.write_at(4096, &[0xAA; 512]).await.unwrap();
    let mut buf = vec![0u8; 512];
    lsmt.read_at_into(4096, &mut buf).await.unwrap();
    assert_eq!(buf[0], 0xAA);
}

#[tokio::test]
async fn test_sparse_commit_matches_logical_content_of_log_structured() {
    let temp_dir = TempDir::new().unwrap();
    let vsize = 64 * 1024u64;
    let (_log_data, _log_index, log_lsmt) = create_lsmt_env(&temp_dir, vsize).await;
    let (_sparse_data, sparse_lsmt) = create_sparse_lsmt_env(&temp_dir, vsize).await;

    let write_a = vec![0xAA; 4096];
    let write_b = vec![0xBB; 4096];
    log_lsmt.write_at(0, &write_a).await.unwrap();
    sparse_lsmt.write_at(0, &write_a).await.unwrap();
    log_lsmt.write_at(8192, &write_b).await.unwrap();
    sparse_lsmt.write_at(8192, &write_b).await.unwrap();
    <LSMTFile as VirtualFile>::discard(&log_lsmt, 0, 4096)
        .await
        .unwrap();
    <LSMTFile as VirtualFile>::discard(&sparse_lsmt, 0, 4096)
        .await
        .unwrap();

    let ring = test_io_ring();
    let log_commit_path = temp_dir.path().join("commit-log.lsmt");
    let sparse_commit_path = temp_dir.path().join("commit-sparse.lsmt");
    let log_commit: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::new(&log_commit_path, ring.clone())
            .await
            .unwrap(),
    );
    let sparse_commit: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::new(&sparse_commit_path, ring).await.unwrap());
    log_lsmt.commit(log_commit.clone()).await.unwrap();
    sparse_lsmt.commit(sparse_commit.clone()).await.unwrap();

    let log_ro = LSMTReadOnlyFile::open(log_commit).await.unwrap();
    let sparse_ro = LSMTReadOnlyFile::open(sparse_commit).await.unwrap();
    let log_bytes = log_ro.read_at(0, vsize as usize).await.unwrap();
    let sparse_bytes = sparse_ro.read_at(0, vsize as usize).await.unwrap();
    assert_eq!(log_bytes, sparse_bytes);
}

// -----------------------------------------------------------------------
// O_DIRECT data-file tests
//
// These mirror the buffered tests above but open the RW *data* file with
// O_DIRECT enabled.  The index file intentionally remains buffered (as
// required by the design: only the data file is direct-IO).
// -----------------------------------------------------------------------

/// Create an LSMTFile whose data file is O_DIRECT; index file stays buffered.
async fn create_lsmt_env_direct_io(
    dir: &TempDir,
    vsize: u64,
) -> (Arc<LocalFile>, Arc<LocalFile>, LSMTFile) {
    let ring = test_io_ring();
    let data_path = dir.path().join("data.lsmt");
    let index_path = dir.path().join("index.lsmt");

    let data_file = Arc::new(
        LocalFile::builder(ring.clone())
            .direct_io(true)
            .truncate(true)
            .open(&data_path)
            .await
            .expect("create direct-io data file"),
    );
    // Index file stays buffered — direct-IO is only for the data file.
    let index_file = Arc::new(
        LocalFile::new(&index_path, ring)
            .await
            .expect("create buffered index file"),
    );

    let lsmt = LSMTFile::create(data_file.clone(), Some(index_file.clone()), vsize, false)
        .await
        .expect("create direct-io LSMTFile failed");

    (data_file, index_file, lsmt)
}

/// `LSMTFile::create` must succeed when the data file is O_DIRECT and the
/// resulting file must be re-openable with the correct virtual size.
#[tokio::test]
async fn test_direct_io_create_open() {
    let temp = TempDir::new().unwrap();
    let vsize = 64 * 1024 * 1024u64;

    let (f_data, f_index, lsmt) = create_lsmt_env_direct_io(&temp, vsize).await;
    assert_eq!(lsmt.size().await.unwrap(), vsize);
    drop(lsmt);

    // Re-open; `open` calls `read_at_into` on the data file which goes
    // through `read_direct_at` for O_DIRECT files.
    let lsmt2 = LSMTFile::open(f_data, Some(f_index), None, vec![])
        .await
        .expect("re-open direct-io LSMTFile");
    assert_eq!(lsmt2.size().await.unwrap(), vsize);
}

/// Write one aligned block via `LSMTFile::write_at` on a direct-IO data
/// file and read it back through `LSMTFile`.  This exercises
/// `write_internal` → `LocalFile::write_direct_at` for actual data blocks.
#[tokio::test]
async fn test_direct_io_write_and_read() {
    let temp = TempDir::new().unwrap();
    let vsize = 64 * 1024 * 1024u64;

    let (_f_data, _f_index, lsmt) = create_lsmt_env_direct_io(&temp, vsize).await;

    let pattern = vec![0xAB_u8; 512];
    lsmt.write_at(0, &pattern).await.expect("direct-io write");

    let got = lsmt.read_at(0, 512).await.expect("direct-io read");
    assert_eq!(got.as_ref(), pattern.as_slice());
}

/// Write several blocks, then call `close_seal`.  The seal writes the index
/// and a 4096-byte trailer to the O_DIRECT data file — both already
/// 512-aligned — and must succeed.  Afterwards, attempting to re-open the
/// sealed file in RW mode must fail (sealed guard), confirming the trailer
/// was written and readable via the direct-IO path.
#[tokio::test]
async fn test_direct_io_close_seal() {
    let temp = TempDir::new().unwrap();
    let vsize = 64 * 1024 * 1024u64;

    let (f_data, f_index, lsmt) = create_lsmt_env_direct_io(&temp, vsize).await;

    // Write a few 512-byte blocks at different offsets.
    lsmt.write_at(0, &vec![0x11_u8; 512])
        .await
        .expect("write block 0");
    lsmt.write_at(512, &vec![0x22_u8; 512])
        .await
        .expect("write block 1");
    lsmt.write_at(4096, &vec![0x33_u8; 512])
        .await
        .expect("write block 2");

    lsmt.close_seal()
        .await
        .expect("close_seal on direct-io file");
    drop(lsmt);

    // The sealed file must be rejected when re-opened in RW mode.
    match LSMTFile::open(f_data, Some(f_index), None, vec![]).await {
        Err(e) => assert_err_contains(&e, "Cannot open a Sealed LSMT file in RW mode"),
        Ok(_) => panic!("sealed file must not open in RW mode"),
    }
}

/// `LSMTFile::create` with `sparse_rw = true` on an O_DIRECT data file
/// (no index file required for sparse mode).  Verifies that the
/// 4096-byte aligned header write path works for sparse files too, and
/// that `truncate` (which is not an I/O write) succeeds on O_DIRECT files.
#[tokio::test]
async fn test_direct_io_create_sparse() {
    let temp = TempDir::new().unwrap();
    let vsize = 64 * 1024 * 1024u64;
    let ring = test_io_ring();
    let data_path = temp.path().join("sparse_data.lsmt");

    let data_file = Arc::new(
        LocalFile::builder(ring)
            .direct_io(true)
            .truncate(true)
            .open(&data_path)
            .await
            .expect("create direct-io data file for sparse"),
    );

    // sparse_rw = true; no index file needed.
    let lsmt = LSMTFile::create(data_file, None, vsize, true)
        .await
        .expect("create sparse direct-io LSMTFile");

    assert_eq!(lsmt.size().await.unwrap(), vsize);
    assert_eq!(lsmt.file_type(), LSMTFileType::SparseReadWrite);
}
