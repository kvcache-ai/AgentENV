use crate::backend::local::LocalFile;
#[cfg(feature = "io-uring")]
use crate::io::vfile_io::CtxRead;
use crate::io::vfile_io::{read_exact, DirectRead, FileReader};
use crate::io::virtual_file::VirtualFile;
#[cfg(feature = "io-uring")]
use crate::io::virtual_file::{IoCtx, LocalBoxFuture};
use crate::metrics::{ZFileCodec, ZFileReadMetrics, ZFileReadStats, ZFileReadStatus};
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use std::cell::{Cell, RefCell};
use std::cmp::min;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const MAX_READ_SIZE: usize = 65_536;

/// Maximum combined encoded span read from the backing file in one
/// coalesced batch. Semantically separate from [`MAX_READ_SIZE`] (the
/// maximum *logical* block size): a batch only ever covers complete
/// encoded blocks whose combined span fits this budget, and a single
/// encoded block larger than the budget still forms its own one-block
/// batch so 64 KiB incompressible blocks stay readable.
const MAX_COALESCED_READ_SIZE: usize = 64 * 1024;

/// One logical pread in this many per thread is sampled for the duration
/// histograms. The counter is thread-local, so an unsampled pread pays only
/// a `Cell` increment for observability: no shared atomic, no
/// `Instant::now()`, no tracing span, no label work.
const ZFILE_PREAD_SAMPLE_INTERVAL: u64 = 1024;

std::thread_local! {
    /// Per-thread logical-pread counter driving [`sample_zfile_pread`].
    static ZFILE_PREAD_COUNTER: Cell<u64> = const { Cell::new(0) };
}

/// Return true for one pread in [`ZFILE_PREAD_SAMPLE_INTERVAL`] on each
/// thread. Histograms fed only by sampled preads export a `_count` that is
/// a SAMPLE COUNT, not an operation count.
fn sample_zfile_pread() -> bool {
    ZFILE_PREAD_COUNTER.with(|counter| {
        let next = counter.get().wrapping_add(1);
        counter.set(next);
        next % ZFILE_PREAD_SAMPLE_INTERVAL == 0
    })
}

/// Observation collected over one logical [`ZFileRO::pread`]: the exact
/// counters plus, for sampled preads only, the durations. Handed to the
/// metrics recorder exactly once per pread by [`ZFileRO::pread_generic`].
#[derive(Debug, Default)]
struct ZFilePreadObservation {
    stats: ZFileReadStats,
    /// Total pread wall time; `Some` only when this pread was sampled.
    pread_elapsed: Option<Duration>,
    /// Sum of all per-block decode times within this pread (retry decodes
    /// included). Stays `Duration::ZERO` for unsampled preads, whose block
    /// loop never calls `Instant::now()`.
    decompress_elapsed: Duration,
}

std::thread_local! {
    /// Per-thread free list of merged-batch buffers. Buffers move out by
    /// value, so no pool borrow is ever held across an `.await`.
    static COMPRESSED_BUFFER_POOL: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
}

/// Merged-batch buffer checked out of [`COMPRESSED_BUFFER_POOL`]; returns
/// to the pool on drop (error exits included), so steady-state preads on
/// one thread neither allocate nor re-zero.
struct PooledBatchBuffer(Vec<u8>);

impl PooledBatchBuffer {
    fn take() -> Self {
        Self(
            COMPRESSED_BUFFER_POOL
                .with(|pool| pool.borrow_mut().pop())
                .unwrap_or_default(),
        )
    }
}

impl std::ops::Deref for PooledBatchBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for PooledBatchBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for PooledBatchBuffer {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.0);
        // Retain only a few reasonably-sized buffers per thread.
        if buf.capacity() == 0 || buf.capacity() > 2 * MAX_COALESCED_READ_SIZE {
            return;
        }
        COMPRESSED_BUFFER_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < 4 {
                pool.push(buf);
            }
        });
    }
}

const HEADER_TRAILER_SPACE: usize = 512;
const HEADER_TRAILER_BODY_SIZE: usize = 96;
const BUF_SIZE: usize = 512;
const NOI_WELL_KNOWN_PRIME: u32 = 100_007;
const ZSTD_C_LEVEL: i32 = 3;
const JUMP_TABLE_READ_CHUNK_SIZE: usize = 1 << 20;
const JUMP_TABLE_READ_PARALLELISM: usize = 32;

// ZFile checksums use the "raw" CRC-32C variant (poly 0x1EDC6F41, reflected,
// init 0, no output xor), matching upstream overlaybd's crc32c_extend().
// The `crc32c` crate computes hardware-accelerated *standard* CRC-32C, whose
// `crc32c_append(seed, data)` complements the seed on entry and the state on
// exit; complementing at both ends therefore chains the raw internal state
// directly: raw(state, data) == !crc32c_append(!state, data).

fn crc32c(data: &[u8]) -> u32 {
    !::crc32c::crc32c_append(!0, data)
}

fn crc32c_salt(data: &[u8]) -> u32 {
    // Upstream crc32c_extend() seeds the running CRC state with the salt
    // prime directly, so chain from that raw state.
    !::crc32c::crc32c_append(!NOI_WELL_KNOWN_PRIME, data)
}

/// Thin wrapper around [`read_exact`] preserving the historical
/// `(file, buf, offset)` argument order used by zfile parsing helpers.
async fn read_exact_at(file: &dyn VirtualFile, buf: &mut [u8], offset: u64) -> Result<()> {
    read_exact(&DirectRead, file, offset, buf).await
}

async fn write_all_at(
    file: &(dyn VirtualFile + Send + Sync),
    mut buf: &[u8],
    mut offset: u64,
) -> Result<()> {
    while !buf.is_empty() {
        let n = file.write_at(offset, buf).await?;
        ensure!(n != 0, "short write");
        buf = &buf[n..];
        offset = offset.checked_add(n as u64).context("offset overflow")?;
    }
    Ok(())
}

async fn read_exact_chunks_at(
    file: Arc<dyn VirtualFile>,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if len <= JUMP_TABLE_READ_CHUNK_SIZE {
        let mut buf = vec![0u8; len];
        read_exact_at(file.as_ref(), &mut buf, offset).await?;
        return Ok(buf);
    }

    let chunk_count = len.div_ceil(JUMP_TABLE_READ_CHUNK_SIZE);
    let chunks = stream::iter(0..chunk_count)
        .map(|chunk_index| {
            let file = file.clone();
            async move {
                let chunk_offset = chunk_index * JUMP_TABLE_READ_CHUNK_SIZE;
                let chunk_len = min(JUMP_TABLE_READ_CHUNK_SIZE, len - chunk_offset);
                let mut buf = vec![0u8; chunk_len];
                read_exact_at(file.as_ref(), &mut buf, offset + chunk_offset as u64).await?;
                Ok::<_, anyhow::Error>((chunk_index, buf))
            }
        })
        .buffer_unordered(chunk_count.min(JUMP_TABLE_READ_PARALLELISM))
        .collect::<Vec<_>>()
        .await;

    let mut out = vec![0u8; len];
    for chunk in chunks {
        let (chunk_index, buf) = chunk?;
        let chunk_offset = chunk_index * JUMP_TABLE_READ_CHUNK_SIZE;
        out[chunk_offset..chunk_offset + buf.len()].copy_from_slice(&buf);
    }
    Ok(out)
}

fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
    let end = offset.checked_add(4).context("offset overflow")?;
    let bytes = buf.get(offset..end).context("buffer too small")?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 length")))
}

fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
    let end = offset.checked_add(8).context("offset overflow")?;
    let bytes = buf.get(offset..end).context("buffer too small")?;
    Ok(u64::from_le_bytes(bytes.try_into().expect("u64 length")))
}

fn write_u32_le(buf: &mut [u8], offset: usize, value: u32) {
    let end = offset + 4;
    buf[offset..end].copy_from_slice(&value.to_le_bytes());
}

fn write_u64_le(buf: &mut [u8], offset: usize, value: u64) {
    let end = offset + 8;
    buf[offset..end].copy_from_slice(&value.to_le_bytes());
}

fn u64_to_usize(value: u64, field: &str) -> Result<usize> {
    usize::try_from(value).with_context(|| format!("{field} cannot fit in usize"))
}

fn mul_u64_to_usize(a: u64, b: u64, field: &str) -> Result<usize> {
    let product = a
        .checked_mul(b)
        .with_context(|| format!("{field} multiplication overflow"))?;
    u64_to_usize(product, field)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompressOptions {
    pub block_size: u32,
    pub algo: u8,
    pub level: u8,
    pub use_dict: u8,
    pub reserved: u32,
    pub dict_size: u32,
    pub verify: u8,
}

impl CompressOptions {
    pub const LZ4: u8 = 1;
    pub const ZSTD: u8 = 2;
    pub const DEFAULT_BLOCK_SIZE: u32 = 4096;

    pub fn new(algo: u8, block_size: u32, verify: u8) -> Self {
        Self {
            block_size,
            algo,
            verify,
            ..Self::default()
        }
    }

    fn decode_from(buf: &[u8]) -> Result<Self> {
        ensure!(buf.len() >= 24, "compress options buffer too small");
        Ok(Self {
            block_size: read_u32_le(buf, 0)?,
            algo: buf[4],
            level: buf[5],
            use_dict: buf[6],
            reserved: read_u32_le(buf, 8)?,
            dict_size: read_u32_le(buf, 12)?,
            verify: buf[16],
        })
    }

    fn encode_into(&self, buf: &mut [u8]) -> Result<()> {
        ensure!(buf.len() >= 24, "compress options buffer too small");
        buf[..24].fill(0);
        write_u32_le(buf, 0, self.block_size);
        buf[4] = self.algo;
        buf[5] = self.level;
        buf[6] = self.use_dict;
        write_u32_le(buf, 8, self.reserved);
        write_u32_le(buf, 12, self.dict_size);
        buf[16] = self.verify;
        Ok(())
    }
}

impl Default for CompressOptions {
    fn default() -> Self {
        Self {
            block_size: Self::DEFAULT_BLOCK_SIZE,
            algo: Self::LZ4,
            level: 0,
            use_dict: 0,
            reserved: 0,
            dict_size: 0,
            verify: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CompressArgs {
    pub opt: CompressOptions,
    pub overwrite_header: bool,
    pub workers: usize,
}

impl CompressArgs {
    pub fn new(opt: CompressOptions) -> Self {
        Self {
            opt,
            overwrite_header: false,
            workers: 1,
        }
    }
}

impl Default for CompressArgs {
    fn default() -> Self {
        Self::new(CompressOptions::default())
    }
}

/// Block compressor for the zfile format.
///
/// Thread-safety contract: the decode side ([`Self::decompress`]) takes
/// `&self` and the trait is `Send + Sync`, so one shared instance must decode
/// blocks concurrently — keep decoders stateless, with any per-call scratch on
/// the stack or in caller-provided buffers. The encode side
/// ([`Self::compress`] / [`Self::compress_batch`]) stays `&mut self` and is
/// only used on the single-owner write path.
pub trait Compressor: Send + Sync {
    fn max_dst_size(&self) -> usize;
    fn src_block_size(&self) -> usize;

    fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize>;
    fn decompress(&self, src: &[u8], dst: &mut [u8]) -> Result<usize>;

    fn nbatch(&self) -> usize {
        1
    }

    fn compress_batch(
        &mut self,
        src: &[u8],
        src_chunk_len: &[usize],
        dst: &mut [u8],
        dst_chunk_len: &mut [usize],
        n: usize,
    ) -> Result<()> {
        if n == 0 {
            return Ok(());
        }
        ensure!(
            src_chunk_len.len() >= n && dst_chunk_len.len() >= n,
            "chunk length arrays are smaller than n"
        );
        let each_dst_capacity = dst.len() / n;
        ensure!(
            each_dst_capacity >= self.max_dst_size(),
            "destination chunk capacity ({each_dst_capacity}) smaller than max_dst_size ({})",
            self.max_dst_size()
        );

        let mut src_offset = 0usize;
        for i in 0..n {
            let chunk_len = src_chunk_len[i];
            let end = src_offset
                .checked_add(chunk_len)
                .context("source offset overflow")?;
            ensure!(end <= src.len(), "source chunk exceeds source buffer");
            let dst_begin = i
                .checked_mul(each_dst_capacity)
                .context("destination offset overflow")?;
            let dst_end = dst_begin
                .checked_add(each_dst_capacity)
                .context("destination offset overflow")?;

            let nout = self.compress(&src[src_offset..end], &mut dst[dst_begin..dst_end])?;
            dst_chunk_len[i] = nout;
            src_offset = end;
        }
        Ok(())
    }
}

fn lz4_compress_bound(src_size: usize) -> usize {
    let size = i32::try_from(src_size).expect("lz4 block size exceeds c_int");
    let bound = unsafe { lz4_sys::LZ4_compressBound(size) };
    assert!(bound > 0, "lz4_compressBound failed for size {size}");
    bound as usize
}

fn zstd_compress_bound(src_size: usize) -> usize {
    src_size
        + (src_size >> 8)
        + if src_size < (128 << 10) {
            ((128 << 10) - src_size) >> 11
        } else {
            0
        }
}

struct Lz4Compressor {
    max_dst_size: usize,
    src_blk_size: usize,
}

impl Lz4Compressor {
    fn new(args: &CompressArgs) -> Result<Self> {
        ensure!(
            args.opt.algo == CompressOptions::LZ4,
            "compression type invalid, expected LZ4"
        );
        let src_blk_size = usize::try_from(args.opt.block_size).context("invalid block_size")?;
        ensure!(src_blk_size != 0, "block_size must be > 0");
        ensure!(
            src_blk_size <= i32::MAX as usize,
            "block_size {src_blk_size} exceeds LZ4 i32 limit"
        );

        Ok(Self {
            max_dst_size: lz4_compress_bound(src_blk_size),
            src_blk_size,
        })
    }
}

impl Compressor for Lz4Compressor {
    fn max_dst_size(&self) -> usize {
        self.max_dst_size
    }

    fn src_block_size(&self) -> usize {
        self.src_blk_size
    }

    fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        ensure!(
            dst.len() >= self.max_dst_size,
            "destination buffer too small ({}), expected at least {}",
            dst.len(),
            self.max_dst_size
        );
        // C liblz4 block format (identical to lz4_flex and upstream OverlayBD).
        // Block sizes are 4KiB-scale, so the c_int casts cannot overflow.
        let n = unsafe {
            lz4_sys::LZ4_compress_default(
                src.as_ptr().cast(),
                dst.as_mut_ptr().cast(),
                src.len() as i32,
                dst.len() as i32,
            )
        };
        ensure!(n > 0, "lz4 compression failed");
        Ok(n as usize)
    }

    fn decompress(&self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        // Returns the decompressed size, or a negative value on failure.
        let n = unsafe {
            lz4_sys::LZ4_decompress_safe(
                src.as_ptr().cast(),
                dst.as_mut_ptr().cast(),
                src.len() as i32,
                dst.len() as i32,
            )
        };
        ensure!(n > 0, "lz4 decompress failed");
        Ok(n as usize)
    }
}

struct ZstdCompressor {
    max_dst_size: usize,
    src_blk_size: usize,
}

impl ZstdCompressor {
    fn new(args: &CompressArgs) -> Result<Self> {
        ensure!(
            args.opt.algo == CompressOptions::ZSTD,
            "compression type invalid, expected ZSTD"
        );

        let src_blk_size = usize::try_from(args.opt.block_size).context("invalid block_size")?;
        ensure!(src_blk_size != 0, "block_size must be > 0");

        Ok(Self {
            max_dst_size: zstd_compress_bound(src_blk_size),
            src_blk_size,
        })
    }
}

thread_local! {
    /// Per-thread reusable ZSTD contexts. Creating a context costs more than
    /// (de)compressing one small block, and `decompress(&self)` must stay
    /// shareable across concurrent readers, so contexts are thread-local
    /// rather than compressor-instance fields (which would also break the
    /// `Compressor: Send + Sync` contract — the raw contexts are not `Sync`).
    static ZSTD_CCTX: RefCell<Option<zstd::bulk::Compressor<'static>>> = const { RefCell::new(None) };
    static ZSTD_DCTX: RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { RefCell::new(None) };
}

impl Compressor for ZstdCompressor {
    fn max_dst_size(&self) -> usize {
        self.max_dst_size
    }

    fn src_block_size(&self) -> usize {
        self.src_blk_size
    }

    fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        ensure!(
            dst.len() >= self.max_dst_size,
            "destination buffer too small ({}), expected at least {}",
            dst.len(),
            self.max_dst_size
        );
        let compressed_len = ZSTD_CCTX.with(|cell| -> Result<usize> {
            let mut guard = cell.borrow_mut();
            if guard.is_none() {
                *guard = Some(
                    zstd::bulk::Compressor::new(ZSTD_C_LEVEL)
                        .context("create zstd compression context")?,
                );
            }
            let ctx = guard.as_mut().expect("context initialized above");
            ctx.compress_to_buffer(src, dst)
                .context("zstd compress failed")
        })?;
        ensure!(compressed_len != 0, "zstd returned empty output");
        Ok(compressed_len)
    }

    fn decompress(&self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        ensure!(
            dst.len() >= self.src_blk_size,
            "destination buffer too small ({}), expected at least {}",
            dst.len(),
            self.src_blk_size
        );
        let out_len = ZSTD_DCTX.with(|cell| -> Result<usize> {
            let mut guard = cell.borrow_mut();
            if guard.is_none() {
                *guard = Some(
                    zstd::bulk::Decompressor::new().context("create zstd decompression context")?,
                );
            }
            let ctx = guard.as_mut().expect("context initialized above");
            ctx.decompress_to_buffer(src, dst)
                .context("zstd decompress failed")
        })?;
        ensure!(out_len != 0, "zstd decompress returned zero bytes");
        ensure!(
            out_len <= self.src_blk_size,
            "zstd decompress returned more than the block size: {out_len}"
        );
        Ok(out_len)
    }
}

pub fn create_compressor(args: &CompressArgs) -> Result<Box<dyn Compressor>> {
    match args.opt.algo {
        CompressOptions::LZ4 => Ok(Box::new(Lz4Compressor::new(args)?)),
        CompressOptions::ZSTD => Ok(Box::new(ZstdCompressor::new(args)?)),
        _ => bail!("invalid compression algorithm"),
    }
}

#[derive(Clone, Debug)]
struct HeaderTrailer {
    magic0: u64,
    magic1: [u8; 16],
    size: u32,
    digest: u32,
    flags: u64,
    index_offset: u64,
    index_size: u64,
    original_file_size: u64,
    index_crc: u32,
    reserved_0: u32,
    opt: CompressOptions,
}

impl HeaderTrailer {
    const MAGIC0: u64 = u64::from_le_bytes(*b"ZFile\0\x01\0");
    const MAGIC1: [u8; 16] = [
        0x74, 0x75, 0x6a, 0x69, 0x2e, 0x79, 0x79, 0x66, 0x40, 0x41, 0x6c, 0x69, 0x62, 0x61, 0x62,
        0x61,
    ];

    const FLAG_SHIFT_HEADER: u32 = 0;
    const FLAG_SHIFT_TYPE: u32 = 1;
    const FLAG_SHIFT_SEALED: u32 = 2;
    const FLAG_SHIFT_HEADER_OVERWRITE: u32 = 3;
    const FLAG_SHIFT_CALC_DIGEST: u32 = 4;

    fn new() -> Self {
        Self {
            magic0: Self::MAGIC0,
            magic1: Self::MAGIC1,
            size: HEADER_TRAILER_BODY_SIZE as u32,
            digest: 0,
            flags: 0,
            index_offset: 0,
            index_size: 0,
            original_file_size: 0,
            index_crc: 0,
            reserved_0: 0,
            opt: CompressOptions::default(),
        }
    }

    fn decode_from_512(buf: &[u8]) -> Result<Self> {
        ensure!(
            buf.len() >= HEADER_TRAILER_SPACE,
            "header/trailer buffer too small"
        );
        Ok(Self {
            magic0: read_u64_le(buf, 0)?,
            magic1: buf[8..24].try_into().context("invalid magic1")?,
            size: read_u32_le(buf, 24)?,
            digest: read_u32_le(buf, 28)?,
            flags: read_u64_le(buf, 32)?,
            index_offset: read_u64_le(buf, 40)?,
            index_size: read_u64_le(buf, 48)?,
            original_file_size: read_u64_le(buf, 56)?,
            index_crc: read_u32_le(buf, 64)?,
            reserved_0: read_u32_le(buf, 68)?,
            opt: CompressOptions::decode_from(&buf[72..96])?,
        })
    }

    fn encode_512(&self) -> Result<[u8; HEADER_TRAILER_SPACE]> {
        let mut buf = [0u8; HEADER_TRAILER_SPACE];
        write_u64_le(&mut buf, 0, self.magic0);
        buf[8..24].copy_from_slice(&self.magic1);
        write_u32_le(&mut buf, 24, self.size);
        write_u32_le(&mut buf, 28, self.digest);
        write_u64_le(&mut buf, 32, self.flags);
        write_u64_le(&mut buf, 40, self.index_offset);
        write_u64_le(&mut buf, 48, self.index_size);
        write_u64_le(&mut buf, 56, self.original_file_size);
        write_u32_le(&mut buf, 64, self.index_crc);
        write_u32_le(&mut buf, 68, self.reserved_0);
        self.opt.encode_into(&mut buf[72..96])?;
        Ok(buf)
    }

    fn verify_magic(&self) -> bool {
        self.magic0 == Self::MAGIC0 && self.magic1 == Self::MAGIC1
    }

    fn get_flag_bit(&self, shift: u32) -> bool {
        (self.flags & (1u64 << shift)) != 0
    }

    fn set_flag_bit(&mut self, shift: u32) {
        self.flags |= 1u64 << shift;
    }

    fn clr_flag_bit(&mut self, shift: u32) {
        self.flags &= !(1u64 << shift);
    }

    fn is_header(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_HEADER)
    }

    fn is_header_overwrite(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_HEADER_OVERWRITE)
    }

    fn is_trailer(&self) -> bool {
        !self.is_header()
    }

    fn is_data_file(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_TYPE)
    }

    fn is_sealed(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_SEALED)
    }

    fn is_digest_enabled(&self) -> bool {
        self.get_flag_bit(Self::FLAG_SHIFT_CALC_DIGEST)
    }

    fn is_valid(&self, bytes: &[u8; HEADER_TRAILER_SPACE]) -> bool {
        if !self.is_digest_enabled() {
            return true;
        }
        let mut tmp = *bytes;
        tmp[28..32].fill(0);
        crc32c(&tmp) == self.digest
    }

    fn set_header(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_HEADER);
    }

    fn set_trailer(&mut self) {
        self.clr_flag_bit(Self::FLAG_SHIFT_HEADER);
    }

    fn set_data_file(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_TYPE);
    }

    fn set_index_file(&mut self) {
        self.clr_flag_bit(Self::FLAG_SHIFT_TYPE);
    }

    fn set_sealed(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_SEALED);
    }

    fn clr_sealed(&mut self) {
        self.clr_flag_bit(Self::FLAG_SHIFT_SEALED);
    }

    fn set_header_overwrite(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_HEADER_OVERWRITE);
    }

    fn set_digest_enable(&mut self) {
        self.set_flag_bit(Self::FLAG_SHIFT_CALC_DIGEST);
    }

    fn set_compress_option(&mut self, opt: CompressOptions) {
        self.opt = opt;
    }
}

#[derive(Clone, Debug, Default)]
struct JumpTable {
    group_size: usize,
    partial_offset: Vec<u64>,
    deltas: Vec<u16>,
}

impl JumpTable {
    fn offset_at(&self, idx: usize) -> Result<u64> {
        ensure!(self.group_size != 0, "jump table not initialized");
        ensure!(
            idx < self.deltas.len(),
            "jump table index out of range: idx={idx}, size={}",
            self.deltas.len()
        );

        let part_idx = idx / self.group_size;
        let inner_idx = idx % self.group_size;
        let part_offset = *self.partial_offset.get(part_idx).with_context(|| {
            format!(
                "partial_offset index out of range: idx={part_idx}, size={}",
                self.partial_offset.len()
            )
        })?;

        if inner_idx == 0 {
            return Ok(part_offset);
        }
        Ok(part_offset + u64::from(self.deltas[idx]))
    }

    fn build(
        &mut self,
        ibuf: &[u32],
        offset_begin: u64,
        block_size: u32,
        enable_crc: bool,
    ) -> Result<()> {
        ensure!(block_size != 0, "block_size cannot be zero");

        let block_size = block_size as usize;
        let group_size = ((u16::MAX as usize) + 1) / block_size;
        ensure!(
            group_size != 0,
            "block_size too large for jump table grouping: {block_size}"
        );

        self.group_size = group_size;
        self.partial_offset.clear();
        self.deltas.clear();

        self.partial_offset.reserve(ibuf.len() / group_size + 1);
        self.deltas.reserve(ibuf.len() + 1);

        let mut raw_offset = offset_begin;
        self.partial_offset.push(raw_offset);
        self.deltas.push(0);

        let min_block_size = if enable_crc {
            std::mem::size_of::<u32>()
        } else {
            0
        };
        for i in 1..=ibuf.len() {
            let blk = ibuf[i - 1] as usize;
            ensure!(
                blk > min_block_size,
                "unexpected block size: idx={}, size={}",
                i - 1,
                ibuf[i - 1]
            );

            raw_offset = raw_offset
                .checked_add(ibuf[i - 1] as u64)
                .context("jump table offset overflow")?;

            if i % group_size == 0 {
                self.partial_offset.push(raw_offset);
                self.deltas.push(0);
                continue;
            }

            let prev = u64::from(self.deltas[i - 1]);
            let next = prev
                .checked_add(ibuf[i - 1] as u64)
                .context("delta overflow")?;
            ensure!(
                next < u16::MAX as u64,
                "build block length failed idx={} delta={} blk={} exceeds {}",
                i - 1,
                prev,
                ibuf[i - 1],
                u16::MAX
            );
            self.deltas.push(next as u16);
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValidMode {
    Normal,
    CrcOnly,
}

pub struct ZFileRO {
    file: Arc<dyn VirtualFile>,
    ht: HeaderTrailer,
    jump_table: JumpTable,
    compressor: Box<dyn Compressor>,
    valid_mode: ValidMode,
    data_verify: bool,
    metrics: ZFileReadMetrics,
}

impl ZFileRO {
    pub fn original_size(&self) -> u64 {
        self.ht.original_file_size
    }

    pub fn options(&self) -> CompressOptions {
        self.ht.opt
    }

    pub fn set_crc_check_only(&mut self) {
        self.valid_mode = ValidMode::CrcOnly;
    }

    pub async fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.pread_generic(&DirectRead, buf, offset).await
    }

    /// Same as [`Self::pread`], but compressed-block reads from the underlying
    /// file go through `read_at_into_with_ctx`, allowing a ublk queue to drive
    /// the disk IO submission on its own io_uring thread. The decompression
    /// path is unchanged.
    #[cfg(feature = "io-uring")]
    pub async fn pread_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize> {
        self.pread_generic(&CtxRead { ctx }, buf, offset).await
    }

    /// Plan the coalesced read batch starting at block `begin_idx`: return
    /// the exclusive end index of the longest run of *complete* encoded
    /// blocks whose combined encoded span fits within
    /// [`MAX_COALESCED_READ_SIZE`]. The first block always belongs to the
    /// batch, even when its encoded span alone exceeds the budget, so
    /// oversized blocks (e.g. 64 KiB incompressible) stay readable. Unlike
    /// upstream's `min(MAX_READ_SIZE, full span)` C++ read, a batch never
    /// extends into part of the next block, so no byte is read twice.
    fn plan_coalesced_batch(&self, begin_idx: usize, end_idx: usize) -> Result<usize> {
        let batch_begin = self.jump_table.offset_at(begin_idx)?;
        let mut next = begin_idx + 1;
        while next < end_idx {
            let candidate_end = self.jump_table.offset_at(next + 1)?;
            let span = candidate_end
                .checked_sub(batch_begin)
                .context("invalid block offsets")?;
            if span > MAX_COALESCED_READ_SIZE as u64 {
                break;
            }
            next += 1;
        }
        Ok(next)
    }

    /// Body shared by [`Self::pread`] and [`Self::pread_with_ctx`]; the
    /// backing read is provided by `reader`. Send-ness of the returned
    /// future is monomorphised from `R`: [`DirectRead`] yields a `Send`
    /// future for background callers; [`CtxRead`] yields a `!Send`
    /// future for ublk-queue callers.
    ///
    /// This is the observation wrapper around [`Self::pread_observed`]: it
    /// maps the codec label, decides duration sampling, and records the
    /// exact counters (plus, for sampled preads, the duration histograms)
    /// exactly once per logical pread — on success and on every error exit
    /// alike. The block loop itself only ever touches plain stack integers.
    async fn pread_generic<R: FileReader>(
        &self,
        reader: &R,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<usize> {
        let sampled = sample_zfile_pread();
        let (result, observation) = self.pread_observed(reader, buf, offset, sampled).await;
        self.metrics.record(&observation.stats);
        if let Some(pread_elapsed) = observation.pread_elapsed {
            let status = if result.is_ok() {
                ZFileReadStatus::Ok
            } else {
                ZFileReadStatus::Error
            };
            self.metrics
                .record_durations(status, pread_elapsed, observation.decompress_elapsed);
        }
        result
    }

    /// Run one logical pread and collect its [`ZFilePreadObservation`].
    /// Split from [`Self::pread_generic`] — with an explicit `sampled` flag
    /// instead of the thread-local sampler — so tests can drive
    /// success/retry/error flows and assert the aggregated counters without
    /// installing a metrics recorder.
    async fn pread_observed<R: FileReader>(
        &self,
        reader: &R,
        buf: &mut [u8],
        offset: u64,
        sampled: bool,
    ) -> (Result<usize>, ZFilePreadObservation) {
        let pread_start = sampled.then(Instant::now);
        let mut observation = ZFilePreadObservation::default();
        let result = self
            .pread_inner(
                reader,
                buf,
                offset,
                &mut observation.stats,
                if sampled {
                    Some(&mut observation.decompress_elapsed)
                } else {
                    None
                },
            )
            .await;
        if let Ok(n) = &result {
            // Only bytes actually returned to the caller count as output.
            observation.stats.output_bytes = *n as u64;
        }
        observation.pread_elapsed = pread_start.map(|start| start.elapsed());
        (result, observation)
    }

    /// Read implementation behind [`Self::pread_observed`].
    ///
    /// Encoded blocks are fetched in coalesced batches planned by
    /// [`Self::plan_coalesced_batch`]: one merged backing read per batch,
    /// then each block is CRC-checked, decoded, and copied out
    /// individually. A block that fails CRC or decompression is evicted at
    /// its exact encoded range and re-read on its own (at most 3 retries,
    /// as before); blocks already consumed from the same batch are never
    /// re-read.
    ///
    /// Observability stays on the stack: the loop only increments plain
    /// `u64` fields of `stats`, and only when `decompress_clock` is `Some`
    /// (a sampled pread) does it time decodes into that pread-level sum.
    async fn pread_inner<R: FileReader>(
        &self,
        reader: &R,
        buf: &mut [u8],
        offset: u64,
        stats: &mut ZFileReadStats,
        mut decompress_clock: Option<&mut Duration>,
    ) -> Result<usize> {
        let block_size = usize::try_from(self.ht.opt.block_size).context("invalid block_size")?;
        ensure!(
            block_size <= MAX_READ_SIZE,
            "block_size {block_size} > MAX_READ_SIZE {MAX_READ_SIZE}"
        );

        if offset >= self.ht.original_file_size {
            return Ok(0);
        }

        let clamped = min(buf.len() as u64, self.ht.original_file_size - offset) as usize;
        if clamped == 0 {
            return Ok(0);
        }

        let begin_idx = (offset / block_size as u64) as usize;
        let end = offset + clamped as u64 - 1;
        let end_idx = (end / block_size as u64) as usize + 1;
        // The initial request touches exactly these logical blocks; retry
        // re-reads never re-increment this.
        stats.blocks = (end_idx - begin_idx) as u64;

        let mut written = 0usize;
        // Merged encoded-block buffer covering one coalesced batch; pooled
        // per thread, so steady-state preads neither allocate nor re-zero.
        let mut compressed = PooledBatchBuffer::take();
        // Single-block buffer backing CRC/decompress retries, allocated
        // lazily on the first failed block so clean reads never pay for it.
        let mut retry_block: Option<Vec<u8>> = None;
        // Scratch for partial first/last blocks only; full-block decodes
        // write straight into the caller's buffer. Allocated lazily so
        // block-aligned reads pay no per-request scratch allocation.
        let mut raw: Option<Vec<u8>> = None;

        let mut batch_begin_idx = begin_idx;
        while batch_begin_idx < end_idx {
            let batch_end_idx = self.plan_coalesced_batch(batch_begin_idx, end_idx)?;
            let batch_begin = self.jump_table.offset_at(batch_begin_idx)?;
            let batch_end = self.jump_table.offset_at(batch_end_idx)?;
            let batch_len = u64_to_usize(
                batch_end
                    .checked_sub(batch_begin)
                    .context("invalid block offsets")?,
                "batch_len",
            )?;

            compressed.clear();
            compressed.reserve(batch_len);
            // SAFETY: `read_exact` fills the whole `batch_len` span before
            // the buffer is read below; a short read errors out and the
            // contents are never observed.
            unsafe { compressed.set_len(batch_len) };
            // One merged-range exact read per batch: counted at submission,
            // so a failed read still accounts its submitted bytes.
            stats.read_batches += 1;
            stats.encoded_bytes += batch_len as u64;
            read_exact(reader, self.file.as_ref(), batch_begin, &mut compressed).await?;

            for idx in batch_begin_idx..batch_end_idx {
                let block_begin = self.jump_table.offset_at(idx)?;
                let block_end = self.jump_table.offset_at(idx + 1)?;
                let encoded_len_u64 = block_end
                    .checked_sub(block_begin)
                    .context("invalid block offsets")?;
                let encoded_len = u64_to_usize(encoded_len_u64, "encoded_len")?;
                let merged_begin = u64_to_usize(
                    block_begin
                        .checked_sub(batch_begin)
                        .context("invalid block offsets")?,
                    "merged_begin",
                )?;

                let compressed_size = if self.data_verify {
                    encoded_len
                        .checked_sub(std::mem::size_of::<u32>())
                        .context("encoded block smaller than crc32 trailer")?
                } else {
                    encoded_len
                };

                let cp_begin = if idx == begin_idx {
                    (offset % block_size as u64) as usize
                } else {
                    0
                };
                let mut cp_len = if idx == end_idx - 1 {
                    (end % block_size as u64) as usize + 1
                } else {
                    block_size
                };
                cp_len = cp_len.checked_sub(cp_begin).context("invalid cp_len")?;

                let mut retry = 3;
                // The first attempt decodes from the merged batch buffer;
                // after a CRC/decompress failure the block is evicted and
                // re-read on its own into `retry_block`, which backs every
                // subsequent attempt of this block.
                let mut from_retry_buffer = false;
                loop {
                    if self.data_verify {
                        let (expected, got) = {
                            let block: &[u8] = if from_retry_buffer {
                                retry_block.as_deref().expect("retry buffer allocated")
                            } else {
                                &compressed[merged_begin..merged_begin + encoded_len]
                            };
                            (
                                u32::from_le_bytes(
                                    block[compressed_size..compressed_size + 4]
                                        .try_into()
                                        .expect("crc32 size"),
                                ),
                                crc32c_salt(&block[..compressed_size]),
                            )
                        };
                        if got != expected {
                            if retry > 0 {
                                retry -= 1;
                                self.file.evict_range(block_begin, encoded_len_u64).await?;
                                let slot = retry_block.get_or_insert_with(Vec::new);
                                slot.resize(encoded_len, 0);
                                // A retry counts only once its exact re-read
                                // is submitted (a failed evict above records
                                // nothing).
                                stats.checksum_retries += 1;
                                stats.read_batches += 1;
                                stats.encoded_bytes += encoded_len_u64;
                                read_exact(reader, self.file.as_ref(), block_begin, slot).await?;
                                from_retry_buffer = true;
                                continue;
                            }
                            bail!(
                                "checksum verification failed: idx={idx}, got={got:#010x}, expected={expected:#010x}"
                            );
                        }
                    }

                    if self.valid_mode == ValidMode::CrcOnly {
                        break;
                    }

                    // Sampled preads only: time this block's whole decode
                    // (either copy-out shape) and add it to the pread-level
                    // sum. Unsampled preads never call `Instant::now()`.
                    let decode_start = decompress_clock.is_some().then(Instant::now);
                    let decomp = {
                        let block: &[u8] = if from_retry_buffer {
                            retry_block.as_deref().expect("retry buffer allocated")
                        } else {
                            &compressed[merged_begin..merged_begin + encoded_len]
                        };
                        if cp_begin == 0 && cp_len == block_size {
                            let dst_slice = &mut buf[written..written + cp_len];
                            let n = self
                                .compressor
                                .decompress(&block[..compressed_size], dst_slice)?;
                            if n != cp_len {
                                Err(anyhow::anyhow!(
                                    "unexpected decompressed size: got {n}, expected {cp_len}"
                                ))
                            } else {
                                Ok(())
                            }
                        } else {
                            let raw = raw.get_or_insert_with(|| vec![0u8; block_size]);
                            let n = self.compressor.decompress(&block[..compressed_size], raw)?;
                            let end_pos = cp_begin
                                .checked_add(cp_len)
                                .context("copy range overflow")?;
                            if end_pos > n {
                                Err(anyhow::anyhow!(
                                    "decompressed range out of bounds: n={n}, begin={cp_begin}, len={cp_len}"
                                ))
                            } else {
                                buf[written..written + cp_len]
                                    .copy_from_slice(&raw[cp_begin..end_pos]);
                                Ok(())
                            }
                        }
                    };
                    if let (Some(clock), Some(start)) =
                        (decompress_clock.as_deref_mut(), decode_start)
                    {
                        *clock += start.elapsed();
                    }

                    match decomp {
                        Ok(()) => break,
                        Err(e) => {
                            if retry > 0 {
                                retry -= 1;
                                self.file.evict_range(block_begin, encoded_len_u64).await?;
                                let slot = retry_block.get_or_insert_with(Vec::new);
                                slot.resize(encoded_len, 0);
                                // Same accounting rule as the checksum
                                // branch: count once the re-read is
                                // submitted.
                                stats.decompress_retries += 1;
                                stats.read_batches += 1;
                                stats.encoded_bytes += encoded_len_u64;
                                read_exact(reader, self.file.as_ref(), block_begin, slot).await?;
                                from_retry_buffer = true;
                                continue;
                            }
                            return Err(e);
                        }
                    }
                }

                written += cp_len;
            }

            batch_begin_idx = batch_end_idx;
        }

        Ok(written)
    }
}

#[async_trait]
impl VirtualFile for ZFileRO {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let mut buf = vec![0u8; len];
        let n = self.pread(&mut buf, offset).await?;
        buf.truncate(n);
        Ok(Bytes::from(buf))
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.pread(dst, offset).await
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("zfile reader is read-only")
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.original_size())
    }

    #[cfg(feature = "io-uring")]
    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let mut buf = vec![0u8; len];
            let n = self.pread_with_ctx(ctx, &mut buf, offset).await?;
            buf.truncate(n);
            Ok(Bytes::from(buf))
        })
    }

    #[cfg(feature = "io-uring")]
    fn read_at_into_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move { self.pread_with_ctx(ctx, dst, offset).await })
    }
}

async fn load_jump_table(file: Arc<dyn VirtualFile>) -> Result<(HeaderTrailer, JumpTable)> {
    let mut buf = [0u8; HEADER_TRAILER_SPACE];
    read_exact_at(file.as_ref(), &mut buf, 0).await?;
    let header = HeaderTrailer::decode_from_512(&buf)?;

    ensure!(
        header.verify_magic() && header.is_header(),
        "header magic/type mismatch"
    );
    ensure!(header.is_valid(&buf), "header digest validation failed");

    let st_size = file.size().await?;
    let mut selected = header.clone();
    let index_bytes: usize;

    if !header.is_header_overwrite() {
        ensure!(header.is_data_file(), "unsupported zfile type");
        ensure!(
            st_size >= HEADER_TRAILER_SPACE as u64,
            "file too small for trailer"
        );

        let trailer_offset = st_size - HEADER_TRAILER_SPACE as u64;
        read_exact_at(file.as_ref(), &mut buf, trailer_offset).await?;
        let trailer = HeaderTrailer::decode_from_512(&buf)?;
        ensure!(
            trailer.verify_magic()
                && trailer.is_trailer()
                && trailer.is_data_file()
                && trailer.is_sealed(),
            "trailer magic/type/sealed mismatch"
        );

        index_bytes = mul_u64_to_usize(trailer.index_size, 4, "index_size")?;
        ensure!(
            trailer.index_offset <= trailer_offset,
            "invalid index_offset beyond trailer"
        );
        let max_index_bytes = trailer_offset - trailer.index_offset;
        ensure!(index_bytes as u64 <= max_index_bytes, "invalid index bytes");
        selected = trailer;
    } else {
        index_bytes = mul_u64_to_usize(header.index_size, 4, "index_size")?;
    }

    let index_raw = read_exact_chunks_at(file.clone(), selected.index_offset, index_bytes).await?;

    if selected.is_digest_enabled() {
        let crc = crc32c(&index_raw);
        ensure!(
            crc == selected.index_crc,
            "jump table checksum mismatch: got {crc:#010x}, expected {:#010x}",
            selected.index_crc
        );
    }

    let nindex = u64_to_usize(selected.index_size, "index_size")?;
    ensure!(nindex != 0, "empty index is not a valid zfile");

    ensure!(
        index_raw.len() == nindex * std::mem::size_of::<u32>(),
        "index byte length mismatch"
    );

    let mut ibuf = Vec::with_capacity(nindex);
    for chunk in index_raw.as_chunks::<4>().0 {
        ibuf.push(u32::from_le_bytes(*chunk));
    }

    let mut jump_table = JumpTable::default();
    jump_table.build(
        &ibuf,
        HEADER_TRAILER_SPACE as u64 + selected.opt.dict_size as u64,
        selected.opt.block_size,
        selected.opt.verify != 0,
    )?;

    Ok((selected, jump_table))
}

pub async fn zfile_open_ro(file: Arc<dyn VirtualFile>, verify: bool) -> Result<ZFileRO> {
    let mut retry = 2;
    let (ht, jump_table) = loop {
        match load_jump_table(file.clone()).await {
            Ok(v) => break v,
            Err(e) => {
                if verify && retry > 0 {
                    retry -= 1;
                    file.evict_all().await?;
                    continue;
                }
                return Err(e);
            }
        }
    };
    let args = CompressArgs::new(ht.opt);
    let compressor = create_compressor(&args)?;
    let data_verify = ht.opt.verify != 0;
    // `create_compressor` accepts only LZ4 and ZSTD, so an open reader
    // always maps to one of the two `codec` label values.
    let codec = match ht.opt.algo {
        CompressOptions::LZ4 => ZFileCodec::Lz4,
        _ => ZFileCodec::Zstd,
    };

    Ok(ZFileRO {
        file,
        ht,
        jump_table,
        compressor,
        valid_mode: ValidMode::Normal,
        data_verify,
        metrics: ZFileReadMetrics::new(codec),
    })
}

pub async fn zfile_open_ro_vfile(
    file: Arc<dyn VirtualFile>,
    verify: bool,
) -> Result<Arc<dyn VirtualFile>> {
    let ro = zfile_open_ro(file, verify).await?;
    Ok(Arc::new(ro))
}

async fn write_header_trailer(
    file: &(dyn VirtualFile + Send + Sync),
    is_header: bool,
    is_sealed: bool,
    is_data_file: bool,
    overwrite_header: bool,
    ht: &mut HeaderTrailer,
    offset: u64,
) -> Result<()> {
    if is_header {
        ht.set_header();
    } else {
        ht.set_trailer();
    }

    if is_sealed {
        ht.set_sealed();
    } else {
        ht.clr_sealed();
    }

    if is_data_file {
        ht.set_data_file();
    } else {
        ht.set_index_file();
    }

    if overwrite_header {
        ht.set_header_overwrite();
    }

    ht.set_digest_enable();
    ht.digest = 0;
    let mut encoded = ht.encode_512()?;
    encoded[28..32].fill(0);
    ht.digest = crc32c(&encoded);
    write_u32_le(&mut encoded, 28, ht.digest);

    write_all_at(file, &encoded, offset).await
}

fn compress_data(
    compressor: &mut dyn Compressor,
    src: &[u8],
    dst: &mut [u8],
    gen_crc: bool,
) -> Result<usize> {
    let mut compressed_len = compressor.compress(src, dst)?;
    ensure!(compressed_len != 0, "compress returned zero");
    if gen_crc {
        let end = compressed_len
            .checked_add(std::mem::size_of::<u32>())
            .context("crc length overflow")?;
        ensure!(
            end <= dst.len(),
            "destination buffer cannot hold crc32 trailer"
        );
        let crc = crc32c_salt(&dst[..compressed_len]);
        write_u32_le(dst, compressed_len, crc);
        compressed_len = end;
    }
    Ok(compressed_len)
}

struct CompressedSegment {
    bytes: Vec<u8>,
    block_lengths: Vec<u32>,
}

/// Worst-case encoded size of one logical block: the compressor's maximum
/// output plus the optional CRC32 trailer.
fn max_compressed_block_size(compressor: &dyn Compressor, gen_crc: bool) -> Result<usize> {
    compressor
        .max_dst_size()
        .checked_add(if gen_crc {
            std::mem::size_of::<u32>()
        } else {
            0
        })
        .context("buffer size overflow")
}

fn compress_segment(
    compressor: &mut dyn Compressor,
    source: &[u8],
    block_size: usize,
    max_compressed_block_size: usize,
    gen_crc: bool,
) -> Result<CompressedSegment> {
    ensure!(block_size != 0, "block_size must be > 0");
    ensure!(
        source.len().is_multiple_of(block_size),
        "source must contain a whole number of blocks"
    );

    let block_count = source.len() / block_size;
    let output_capacity = block_count
        .checked_mul(max_compressed_block_size)
        .context("compressed segment size overflow")?;
    let mut bytes = vec![0u8; output_capacity];
    let mut block_lengths = Vec::with_capacity(block_count);
    let mut output_offset = 0usize;

    for block in source.chunks_exact(block_size) {
        let output_end = output_offset
            .checked_add(max_compressed_block_size)
            .context("compressed segment offset overflow")?;
        let compressed_len = compress_data(
            compressor,
            block,
            &mut bytes[output_offset..output_end],
            gen_crc,
        )?;
        block_lengths.push(
            u32::try_from(compressed_len)
                .context("compressed_len cannot fit in u32 jump table entry")?,
        );
        output_offset = output_offset
            .checked_add(compressed_len)
            .context("compressed segment offset overflow")?;
    }
    bytes.truncate(output_offset);

    Ok(CompressedSegment {
        bytes,
        block_lengths,
    })
}

/// One segment of full blocks handed to a persistent compression worker.
/// `seq` is the submission order so out-of-order completions can be
/// reassembled back into source order by the consumer.
struct WorkItem {
    seq: usize,
    source: Vec<u8>,
}

/// Completion returned by a persistent compression worker; `seq` matches the
/// originating [`WorkItem`].
struct CompressedBatch {
    seq: usize,
    result: Result<CompressedSegment>,
}

/// Persistent pool of compression worker threads.
///
/// Replaces per-call `spawn_blocking` fan-out: each worker owns one
/// compressor (zstd contexts are expensive to create) and loops on a shared
/// bounded work queue, returning one [`CompressedBatch`] per [`WorkItem`].
/// Both channels are bounded at `workers * 2` so submission backpressures
/// instead of queueing unbounded segment buffers.
struct WorkerPool {
    work_tx: crossbeam_channel::Sender<WorkItem>,
    result_rx: crossbeam_channel::Receiver<CompressedBatch>,
    handles: Vec<std::thread::JoinHandle<()>>,
    /// Test-only per-pool kill switch: while set, this pool's workers panic
    /// instead of compressing. Instance-scoped so concurrent tests with
    /// their own pools never observe the injection.
    #[cfg(test)]
    panic_on_work: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl WorkerPool {
    /// Maximum worker threads. Prevents absurd configs from spawning
    /// thousands of OS threads or overflowing channel capacity arithmetic.
    const MAX_WORKERS: usize = 256;

    fn new(
        workers: usize,
        opt: CompressOptions,
        gen_crc: bool,
        block_size: usize,
        max_compressed_block_size: usize,
    ) -> Self {
        let workers = workers.clamp(1, Self::MAX_WORKERS);
        let (work_tx, work_rx) = crossbeam_channel::bounded::<WorkItem>(workers * 2);
        let (result_tx, result_rx) = crossbeam_channel::bounded::<CompressedBatch>(workers * 2);
        #[cfg(test)]
        let panic_on_work = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let work_rx = work_rx.clone();
            let result_tx = result_tx.clone();
            #[cfg(test)]
            let panic_on_work = panic_on_work.clone();
            handles.push(std::thread::spawn(move || {
                let mut compressor = match create_compressor(&CompressArgs::new(opt)) {
                    Ok(compressor) => compressor,
                    Err(error) => {
                        // Construction is config-driven and already validated
                        // by ZFileBuilder::new; if it still fails, fail every
                        // item so the seq-matched protocol stays intact until
                        // the pool shuts down.
                        let message = error.to_string();
                        while let Ok(item) = work_rx.recv() {
                            if result_tx
                                .send(CompressedBatch {
                                    seq: item.seq,
                                    result: Err(anyhow::anyhow!(
                                        "zfile worker compressor init failed: {message}"
                                    )),
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                        return;
                    }
                };
                while let Ok(item) = work_rx.recv() {
                    // A panicking compression must still answer its item:
                    // converting the panic into this seq's Err result keeps
                    // the seq-matched protocol intact, so the consumer's
                    // drain loop always receives exactly one result per
                    // submitted item instead of blocking forever.
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        #[cfg(test)]
                        {
                            if panic_on_work.load(std::sync::atomic::Ordering::Relaxed) {
                                panic!("injected zfile worker panic");
                            }
                        }
                        compress_segment(
                            compressor.as_mut(),
                            &item.source,
                            block_size,
                            max_compressed_block_size,
                            gen_crc,
                        )
                    }))
                    .unwrap_or_else(|_payload| {
                        // The panic hook already logged message and location;
                        // payloads are an opaque type on modern toolchains,
                        // so keep the error generic.
                        Err(anyhow::anyhow!("zfile worker panicked during compression"))
                    });
                    if result_tx
                        .send(CompressedBatch {
                            seq: item.seq,
                            result,
                        })
                        .is_err()
                    {
                        // Consumer is gone (pool dropped): exit quietly.
                        break;
                    }
                }
            }));
        }

        Self {
            work_tx,
            result_rx,
            handles,
            #[cfg(test)]
            panic_on_work,
        }
    }

    /// Number of persistent worker threads in the pool.
    fn worker_count(&self) -> usize {
        self.handles.len()
    }

    /// Queue one segment for compression. Blocks while `workers * 2` items
    /// are already in flight (bounded-channel backpressure).
    fn submit(&self, item: WorkItem) -> Result<()> {
        self.work_tx
            .send(item)
            .map_err(|_| anyhow::anyhow!("zfile worker pool is shut down"))
    }

    /// Take the next completed batch, in completion order (not `seq` order).
    fn recv(&self) -> Result<CompressedBatch> {
        self.result_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("zfile worker pool result channel closed"))
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        // Swap in a disconnected sender and drop the real one: with no
        // senders left, every worker's `recv()` returns Err and the thread
        // exits its loop, so the joins below cannot hang.
        let (disconnected_tx, disconnected_rx) = crossbeam_channel::bounded::<WorkItem>(1);
        drop(disconnected_rx);
        drop(std::mem::replace(&mut self.work_tx, disconnected_tx));
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

const ZFILE_PHYSICAL_WRITE_CHUNK_SIZE: usize = 4096;

pub struct ZFileBuilder {
    dest: Arc<dyn VirtualFile>,
    args: CompressArgs,
    opt: CompressOptions,
    compressor: Box<dyn Compressor>,
    /// Persistent compression workers, created when `args.workers > 1`.
    /// `None` means multi-block writes fall back to the in-line compressor.
    pool: Option<WorkerPool>,
    block_len: Vec<u32>,
    compressed_data: Vec<u8>,
    reserved_buf: Vec<u8>,
    reserved_size: usize,
    raw_data_size: u64,
    moffset: u64,
    ht: HeaderTrailer,
    finished: bool,
}

impl ZFileBuilder {
    pub async fn new(file: Arc<dyn VirtualFile>, args: &CompressArgs) -> Result<Self> {
        let mut ht = HeaderTrailer::new();
        ht.set_compress_option(args.opt);
        write_header_trailer(file.as_ref(), true, false, true, false, &mut ht, 0).await?;

        let block_size = usize::try_from(args.opt.block_size).context("invalid block_size")?;
        ensure!(block_size != 0, "block_size must be > 0");

        let buf_size = block_size
            .checked_add(BUF_SIZE)
            .context("buffer size overflow")?;

        let compressor = create_compressor(args)?;
        let pool = if args.workers > 1 {
            let gen_crc = args.opt.verify != 0;
            Some(WorkerPool::new(
                args.workers,
                args.opt,
                gen_crc,
                block_size,
                max_compressed_block_size(compressor.as_ref(), gen_crc)?,
            ))
        } else {
            None
        };

        Ok(Self {
            dest: file,
            args: *args,
            opt: args.opt,
            compressor,
            pool,
            block_len: Vec::new(),
            compressed_data: vec![0u8; buf_size],
            reserved_buf: vec![0u8; buf_size],
            reserved_size: 0,
            raw_data_size: 0,
            moffset: HEADER_TRAILER_SPACE as u64 + args.opt.dict_size as u64,
            ht,
            finished: false,
        })
    }

    async fn write_buffer(&mut self, src: &[u8]) -> Result<()> {
        let compressed_len = compress_data(
            self.compressor.as_mut(),
            src,
            &mut self.compressed_data,
            self.opt.verify != 0,
        )?;

        write_all_at(
            self.dest.as_ref(),
            &self.compressed_data[..compressed_len],
            self.moffset,
        )
        .await?;
        self.block_len.push(
            u32::try_from(compressed_len)
                .context("compressed_len cannot fit in u32 jump table entry")?,
        );
        self.moffset = self
            .moffset
            .checked_add(compressed_len as u64)
            .context("moffset overflow")?;
        Ok(())
    }

    async fn commit_compressed_batch(&mut self, batch: CompressedSegment) -> Result<()> {
        let mut encoded_size = 0usize;
        for &block_length in &batch.block_lengths {
            ensure!(block_length != 0, "compressed block length cannot be zero");
            encoded_size = encoded_size
                .checked_add(block_length as usize)
                .context("compressed batch length overflow")?;
        }
        ensure!(
            encoded_size == batch.bytes.len(),
            "compressed block lengths do not match batch size"
        );

        let next_offset = self
            .moffset
            .checked_add(batch.bytes.len() as u64)
            .context("moffset overflow")?;
        // Large batches destined for a local file bypass io_uring with one
        // synchronous pwrite: the builder owns the destination exclusively
        // and the bytes land in local page cache, so a single blocked
        // syscall is cheaper than per-4 KiB submit/completion round trips.
        // Non-local destinations take the normal async write path.
        let local_dest = if batch.bytes.len() > ZFILE_PHYSICAL_WRITE_CHUNK_SIZE {
            self.dest
                .as_any()
                .and_then(|any| any.downcast_ref::<LocalFile>())
        } else {
            None
        };
        if let Some(local) = local_dest {
            let written = local.write_at_sync_pwrite(self.moffset, &batch.bytes)?;
            ensure!(written == batch.bytes.len(), "short sync pwrite");
        } else {
            write_all_at(self.dest.as_ref(), &batch.bytes, self.moffset).await?;
        }
        self.block_len.extend(batch.block_lengths);
        self.moffset = next_offset;
        Ok(())
    }

    /// In-line (non-pooled) compression shared by the borrowed and owned
    /// full-block write paths: compress `source` with the builder's own
    /// compressor and commit the resulting batch.
    async fn compress_and_commit(&mut self, source: &[u8], block_size: usize) -> Result<()> {
        let max_block_size =
            max_compressed_block_size(self.compressor.as_ref(), self.opt.verify != 0)?;
        let batch = compress_segment(
            self.compressor.as_mut(),
            source,
            block_size,
            max_block_size,
            self.opt.verify != 0,
        )?;
        self.commit_compressed_batch(batch).await
    }

    async fn write_full_blocks_borrowed(&mut self, source: &[u8]) -> Result<()> {
        let block_size = usize::try_from(self.opt.block_size).context("invalid block_size")?;
        ensure!(
            source.len().is_multiple_of(block_size),
            "source must contain a whole number of blocks"
        );
        if source.is_empty() {
            return Ok(());
        }

        let block_count = source.len() / block_size;
        if self.args.workers <= 1 || block_count == 1 {
            return self.compress_and_commit(source, block_size).await;
        }

        self.write_full_blocks_owned(source.to_vec()).await
    }

    async fn write_full_blocks_owned(&mut self, source: Vec<u8>) -> Result<()> {
        let block_size = usize::try_from(self.opt.block_size).context("invalid block_size")?;
        ensure!(
            source.len().is_multiple_of(block_size),
            "source must contain a whole number of blocks"
        );
        if source.is_empty() {
            return Ok(());
        }

        let block_count = source.len() / block_size;
        if self.args.workers <= 1 || block_count == 1 || self.pool.is_none() {
            return self.compress_and_commit(&source, block_size).await;
        }

        // Persistent-pool fan-out: split the batch into per-worker segments,
        // submit them all, then collect every result before committing in
        // `seq` order. No `.await` happens while pool results are
        // outstanding, so dropping this future mid-write can never strand
        // stale results in the pool for the next call (cancellation safety).
        // `submit` never blocks here: a batch submits at most `workers`
        // items into a `workers * 2` channel that the previous round drained.
        // TODO: `pool.recv()` below blocks the calling async task for the
        // duration of one batch's compression (bounded and deadlock-free,
        // but it parks a current-thread runtime). If server-side latency
        // matters, wrap the submit/recv collection in
        // `tokio::task::spawn_blocking`.
        let (first_error, segments) = {
            let pool = self.pool.as_ref().expect("checked is_some above");
            let workers = pool.worker_count().min(block_count);
            let segment_blocks = block_count.div_ceil(workers);
            let mut segments: std::collections::BTreeMap<usize, CompressedSegment> =
                Default::default();
            let mut pending = 0usize;
            let mut first_error: Option<anyhow::Error> = None;

            for segment_begin_block in (0..block_count).step_by(segment_blocks) {
                let segment_end_block = min(segment_begin_block + segment_blocks, block_count);
                let segment_begin = segment_begin_block
                    .checked_mul(block_size)
                    .context("source offset overflow")?;
                let segment_end = segment_end_block
                    .checked_mul(block_size)
                    .context("source offset overflow")?;
                if let Err(error) = pool.submit(WorkItem {
                    seq: pending,
                    source: source[segment_begin..segment_end].to_vec(),
                }) {
                    first_error = Some(error);
                    break;
                }
                pending += 1;
            }

            // Drain exactly the submitted results, even after an error, so
            // the pool stays clean for the next round. `recv` only fails
            // when every worker is gone, in which case no more results can
            // arrive and the drain stops early.
            while pending > 0 {
                pending -= 1;
                match pool.recv() {
                    Ok(batch) => match batch.result {
                        Ok(segment) => {
                            segments.insert(batch.seq, segment);
                        }
                        Err(error) => {
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                        }
                    },
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                        break;
                    }
                }
            }
            (first_error, segments)
        };

        if let Some(error) = first_error {
            // Nothing was committed; drop the pool so a later write falls
            // back to the in-line compressor instead of risking stale
            // results from a half-drained queue.
            self.pool = None;
            return Err(error);
        }

        for segment in segments.into_values() {
            self.commit_compressed_batch(segment).await?;
        }
        Ok(())
    }

    pub async fn write(&mut self, mut buf: &[u8]) -> Result<usize> {
        ensure!(!self.finished, "builder already closed");

        let next_raw_data_size = self
            .raw_data_size
            .checked_add(buf.len() as u64)
            .context("raw_data_size overflow")?;

        let expected = buf.len();
        let block_size = self.opt.block_size as usize;
        let batch_size = write_batch_size(block_size)?;

        if self.reserved_size != 0 {
            let needed = block_size - self.reserved_size;
            if buf.len() < needed {
                self.reserved_buf[self.reserved_size..self.reserved_size + buf.len()]
                    .copy_from_slice(buf);
                self.reserved_size += buf.len();
                self.raw_data_size = next_raw_data_size;
                return Ok(expected);
            }

            self.reserved_buf[self.reserved_size..self.reserved_size + needed]
                .copy_from_slice(&buf[..needed]);
            // Borrow the completed block so the builder state stays
            // consistent even if the await below is cancelled.
            let completed_block = self.reserved_buf[..block_size].to_vec();
            self.write_full_blocks_borrowed(&completed_block).await?;
            self.reserved_size = 0;
            buf = &buf[needed..];
        }

        while buf.len() >= block_size {
            let full_bytes = (buf.len() / block_size)
                .checked_mul(block_size)
                .context("batch_bytes overflow")?;
            let batch_bytes = min(full_bytes, batch_size);
            let (head, tail) = buf.split_at(batch_bytes);
            self.write_full_blocks_borrowed(head).await?;
            buf = tail;
        }

        if !buf.is_empty() {
            self.reserved_buf[..buf.len()].copy_from_slice(buf);
            self.reserved_size = buf.len();
        }

        self.raw_data_size = next_raw_data_size;
        Ok(expected)
    }

    async fn write_owned(&mut self, mut buffer: Vec<u8>, len: usize) -> Result<usize> {
        ensure!(len <= buffer.len(), "write length exceeds buffer");
        ensure!(!self.finished, "builder already closed");

        let block_size = self.opt.block_size as usize;
        if self.reserved_size != 0 || !len.is_multiple_of(block_size) {
            return self.write(&buffer[..len]).await;
        }

        let next_raw_data_size = self
            .raw_data_size
            .checked_add(len as u64)
            .context("raw_data_size overflow")?;
        buffer.truncate(len);

        // The compact writer only hands over buffers of at most
        // ZFILE_COMPACT_WRITER_BUFFER_SIZE and `len` covers a whole number
        // of blocks, so a single batch always spans the whole buffer.
        self.write_full_blocks_owned(buffer).await?;

        self.raw_data_size = next_raw_data_size;
        Ok(len)
    }

    pub async fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        if self.reserved_size != 0 {
            let chunk = self.reserved_buf[..self.reserved_size].to_vec();
            self.write_buffer(&chunk).await?;
            self.reserved_size = 0;
        }

        let index_offset = self.moffset;
        let index_size = self.block_len.len() as u64;
        let mut index_raw = Vec::with_capacity(self.block_len.len() * std::mem::size_of::<u32>());
        for len in &self.block_len {
            index_raw.extend_from_slice(&len.to_le_bytes());
        }

        write_all_at(self.dest.as_ref(), &index_raw, index_offset).await?;

        self.ht.index_crc = crc32c(&index_raw);
        self.ht.index_offset = index_offset;
        self.ht.index_size = index_size;
        self.ht.original_file_size = self.raw_data_size;

        let trailer_offset = index_offset
            .checked_add(index_raw.len() as u64)
            .context("trailer offset overflow")?;
        write_header_trailer(
            self.dest.as_ref(),
            false,
            true,
            true,
            false,
            &mut self.ht,
            trailer_offset,
        )
        .await?;
        if self.args.overwrite_header {
            write_header_trailer(self.dest.as_ref(), true, false, true, true, &mut self.ht, 0)
                .await?;
        }

        self.finished = true;
        Ok(())
    }

    pub async fn close(mut self) -> Result<Arc<dyn VirtualFile>> {
        self.finish().await?;
        Ok(self.dest.clone())
    }
}

const ZFILE_COMPACT_WRITER_BUFFER_SIZE: usize = 512 * 1024;

fn write_batch_size(block_size: usize) -> Result<usize> {
    let blocks = (ZFILE_COMPACT_WRITER_BUFFER_SIZE / block_size).max(1);
    blocks
        .checked_mul(block_size)
        .context("write batch size overflow")
}

pub struct ZFileCompactWriter {
    state: tokio::sync::Mutex<ZFileCompactWriterState>,
}

struct ZFileCompactWriterState {
    builder: ZFileBuilder,
    next_offset: u64,
    finalized: bool,
    failed: bool,
}

impl ZFileCompactWriter {
    pub async fn new(destination: Arc<dyn VirtualFile>, args: &CompressArgs) -> Result<Self> {
        Ok(Self {
            state: tokio::sync::Mutex::new(ZFileCompactWriterState {
                builder: ZFileBuilder::new(destination, args).await?,
                next_offset: 0,
                finalized: false,
                failed: false,
            }),
        })
    }

    async fn write_slice_at(&self, data: &[u8], offset: u64) -> Result<()> {
        let mut state = self.state.lock().await;
        ensure!(!state.finalized, "zfile compact writer already finalized");
        ensure!(
            !state.failed,
            "zfile compact writer failed after previous write error"
        );
        ensure!(
            offset == state.next_offset,
            "zfile compact writer received offset {offset}, expected {}",
            state.next_offset
        );
        let next_offset = offset
            .checked_add(data.len() as u64)
            .context("zfile compact writer logical offset overflow")?;
        state.failed = true;
        let written = match state.builder.write(data).await {
            Ok(written) => written,
            Err(error) => return Err(error),
        };
        if written != data.len() {
            bail!("zfile builder performed a short write");
        }
        state.next_offset = next_offset;
        state.failed = false;
        Ok(())
    }
}

#[async_trait]
impl storage_util::CompactWriter for ZFileCompactWriter {
    async fn alloc_buffer(&self) -> Result<Box<dyn storage_util::CompactBuffer>> {
        Ok(Box::new(vec![0u8; ZFILE_COMPACT_WRITER_BUFFER_SIZE]))
    }

    fn buffer_size(&self) -> usize {
        ZFILE_COMPACT_WRITER_BUFFER_SIZE
    }

    fn requires_ordered_writes(&self) -> bool {
        true
    }

    async fn write(
        &self,
        buf: Box<dyn storage_util::CompactBuffer>,
        offset: u64,
        len: usize,
    ) -> Result<()> {
        let buffer = buf
            .into_any()
            .downcast::<Vec<u8>>()
            .map_err(|_| anyhow::anyhow!("zfile compact writer requires Vec<u8> buffers"))?;
        let mut state = self.state.lock().await;
        ensure!(!state.finalized, "zfile compact writer already finalized");
        ensure!(
            !state.failed,
            "zfile compact writer failed after previous write error"
        );
        ensure!(
            offset == state.next_offset,
            "zfile compact writer received offset {offset}, expected {}",
            state.next_offset
        );
        let next_offset = offset
            .checked_add(len as u64)
            .context("zfile compact writer logical offset overflow")?;
        state.failed = true;
        let written = match state.builder.write_owned(*buffer, len).await {
            Ok(written) => written,
            Err(error) => return Err(error),
        };
        if written != len {
            bail!("zfile builder performed a short write");
        }
        state.next_offset = next_offset;
        state.failed = false;
        Ok(())
    }

    async fn write_all_at(&self, data: &[u8], offset: u64) -> Result<()> {
        if data.is_empty() {
            return self.write_slice_at(data, offset).await;
        }

        let mut offset = offset;
        for chunk in data.chunks(ZFILE_COMPACT_WRITER_BUFFER_SIZE) {
            self.write_slice_at(chunk, offset).await?;
            offset = offset
                .checked_add(chunk.len() as u64)
                .context("zfile compact writer logical offset overflow")?;
        }
        Ok(())
    }

    async fn finalize(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.finalized {
            return Ok(());
        }
        ensure!(
            !state.failed,
            "zfile compact writer failed after previous write error"
        );
        state.failed = true;
        state.builder.finish().await?;
        state.finalized = true;
        state.failed = false;
        Ok(())
    }
}

pub async fn new_zfile_builder(
    file: Arc<dyn VirtualFile>,
    args: &CompressArgs,
) -> Result<ZFileBuilder> {
    ZFileBuilder::new(file, args).await
}

pub async fn zfile_compress(
    src: Arc<dyn VirtualFile>,
    dst: Arc<dyn VirtualFile>,
    args: &CompressArgs,
) -> Result<()> {
    let opt = args.opt;
    let mut compressor = create_compressor(args)?;

    let mut ht = HeaderTrailer::new();
    ht.set_compress_option(opt);
    write_header_trailer(dst.as_ref(), true, false, true, false, &mut ht, 0).await?;

    let block_size = usize::try_from(opt.block_size).context("invalid block_size")?;
    ensure!(block_size != 0, "block_size must be > 0");

    let buf_size = block_size
        .checked_add(BUF_SIZE)
        .context("buffer size overflow")?;

    let nbatch = compressor.nbatch().max(1);

    let mut block_len: Vec<u32> = Vec::new();
    let mut moffset = HEADER_TRAILER_SPACE as u64 + opt.dict_size as u64;
    let mut infile_size = 0u64;
    let src_size = src.size().await?;
    let mut src_offset = 0u64;

    let mut raw_data = vec![0u8; block_size * nbatch];
    let mut compressed_data = vec![0u8; buf_size * nbatch];

    while src_offset < src_size {
        let remain = (src_size - src_offset) as usize;
        let to_read = min(remain, raw_data.len());
        let mut readn = 0usize;
        while readn < to_read {
            let chunk = src
                .read_at(src_offset + readn as u64, to_read - readn)
                .await?;
            if chunk.is_empty() {
                break;
            }
            let n = chunk.len();
            raw_data[readn..readn + n].copy_from_slice(&chunk);
            readn += n;
        }
        if readn == 0 {
            break;
        }
        src_offset = src_offset
            .checked_add(readn as u64)
            .context("src_offset overflow")?;
        infile_size = infile_size
            .checked_add(readn as u64)
            .context("infile_size overflow")?;

        let mut raw_chunk_len = Vec::with_capacity(nbatch);
        let mut remain = readn;
        while remain > 0 {
            let chunk = min(remain, block_size);
            raw_chunk_len.push(chunk);
            remain -= chunk;
        }
        let n = raw_chunk_len.len();

        let mut compressed_len = vec![0usize; n];
        compressor.compress_batch(
            &raw_data[..readn],
            &raw_chunk_len,
            &mut compressed_data[..n * buf_size],
            &mut compressed_len,
            n,
        )?;

        for (j, compressed) in compressed_len.iter().copied().enumerate() {
            let begin = j * buf_size;
            let end = begin + compressed;
            write_all_at(dst.as_ref(), &compressed_data[begin..end], moffset).await?;
            let mut written = compressed;

            if opt.verify != 0 {
                let crc = crc32c_salt(&compressed_data[begin..end]);
                write_all_at(
                    dst.as_ref(),
                    &crc.to_le_bytes(),
                    moffset
                        .checked_add(written as u64)
                        .context("crc offset overflow")?,
                )
                .await?;
                written += std::mem::size_of::<u32>();
            }

            block_len.push(u32::try_from(written).context("compressed length cannot fit in u32")?);
            moffset = moffset
                .checked_add(written as u64)
                .context("moffset overflow")?;
        }
    }

    let index_offset = moffset;
    let index_size = block_len.len() as u64;
    let mut index_raw = Vec::with_capacity(block_len.len() * std::mem::size_of::<u32>());
    for len in &block_len {
        index_raw.extend_from_slice(&len.to_le_bytes());
    }

    write_all_at(dst.as_ref(), &index_raw, index_offset).await?;

    ht.index_crc = crc32c(&index_raw);
    ht.index_offset = index_offset;
    ht.index_size = index_size;
    ht.original_file_size = infile_size;

    let trailer_offset = index_offset
        .checked_add(index_raw.len() as u64)
        .context("trailer offset overflow")?;
    write_header_trailer(
        dst.as_ref(),
        false,
        true,
        true,
        false,
        &mut ht,
        trailer_offset,
    )
    .await?;
    if args.overwrite_header {
        write_header_trailer(dst.as_ref(), true, false, true, true, &mut ht, 0).await?;
    }

    Ok(())
}

pub async fn zfile_decompress(src: Arc<dyn VirtualFile>, dst: Arc<dyn VirtualFile>) -> Result<()> {
    let zf = zfile_open_ro(src, true).await?;
    let raw_data_size = zf.original_size();
    let block_size = zf.options().block_size as usize;

    let mut raw_buf = vec![0u8; block_size];
    let mut offset = 0u64;
    while offset < raw_data_size {
        let len = min(block_size as u64, raw_data_size - offset) as usize;
        let readn = zf.pread(&mut raw_buf[..len], offset).await?;
        ensure!(
            readn == len,
            "failed to read decompressed data: offset={offset}, expect={len}, got={readn}"
        );
        write_all_at(dst.as_ref(), &raw_buf[..readn], offset).await?;
        offset += len as u64;
    }

    Ok(())
}

pub async fn zfile_validation_check(src: Arc<dyn VirtualFile>) -> Result<()> {
    let mut zf = zfile_open_ro(src, true).await?;
    ensure!(
        zf.options().verify != 0,
        "source file does not have data checksum"
    );

    zf.set_crc_check_only();

    let raw_data_size = zf.original_size();
    let block_size = zf.options().block_size as usize;
    let mut scratch = vec![0u8; block_size];

    let mut offset = 0u64;
    while offset < raw_data_size {
        let len = min(block_size as u64, raw_data_size - offset) as usize;
        let readn = zf.pread(&mut scratch[..len], offset).await?;
        ensure!(
            readn == len,
            "crc check failed in block {}",
            offset / block_size as u64
        );
        offset += len as u64;
    }

    Ok(())
}

pub async fn is_zfile(file: Arc<dyn VirtualFile>) -> Result<i32> {
    let mut buf = [0u8; HEADER_TRAILER_SPACE];
    read_exact_at(file.as_ref(), &mut buf, 0).await?;

    let ht = HeaderTrailer::decode_from_512(&buf)?;
    if !ht.verify_magic() || !ht.is_header() {
        return Ok(0);
    }
    ensure!(
        ht.is_valid(&buf),
        "file is zfile but header digest is invalid"
    );
    Ok(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::local::LocalFile;
    use rand::{rngs::StdRng, Rng, RngExt, SeedableRng};
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    const VERIFY_DATA_SIZE: usize = 1024 * 1024 + 333;

    async fn write_all(file: &Arc<dyn VirtualFile>, data: &[u8]) {
        write_all_at(file.as_ref(), data, 0)
            .await
            .expect("write source");
    }

    async fn read_all(file: &Arc<dyn VirtualFile>) -> Vec<u8> {
        let size = file.size().await.expect("size read") as usize;
        let mut out = vec![0u8; size];
        if size > 0 {
            read_exact_at(file.as_ref(), &mut out, 0)
                .await
                .expect("read all");
        }
        out
    }

    async fn new_local_vfile() -> Arc<dyn VirtualFile> {
        let tmp = NamedTempFile::new().expect("create temp file");
        let path = tmp.path().to_path_buf();
        drop(tmp);
        Arc::new(LocalFile::new(path).expect("create local vfile"))
    }

    fn sample_data(size: usize) -> Vec<u8> {
        let mut rng = StdRng::seed_from_u64(0x5a5a_1122_7788_ccdd);
        let mut data = vec![0u8; size];
        rng.fill_bytes(&mut data);
        data
    }

    async fn seqread_compare(src: &[u8], ro: &ZFileRO) {
        let mut buf = vec![0u8; 16 * 1024];
        let mut offset = 0usize;
        while offset < src.len() {
            let len = min(buf.len(), src.len() - offset);
            let readn = ro
                .pread(&mut buf[..len], offset as u64)
                .await
                .expect("sequential pread");
            assert_eq!(readn, len, "sequential read length mismatch at {offset}");
            assert_eq!(
                &buf[..len],
                &src[offset..offset + len],
                "sequential read content mismatch at {offset}"
            );
            offset += len;
        }
    }

    async fn randread_compare(src: &[u8], ro: &ZFileRO, seed: u64) {
        if src.len() < 512 {
            return;
        }
        let mut rng = StdRng::seed_from_u64(seed);
        let counts = src.len() / 512;
        let mut small_buf = vec![0u8; 16 * 1024];

        for _ in 0..400 {
            let offset_blk = (rng.next_u32() as usize) % counts;
            let mut len_blk = min(counts - offset_blk, (rng.next_u32() as usize) % 32);
            if len_blk == 0 {
                len_blk = 1;
            }

            let offset = offset_blk * 512;
            let len = min(len_blk * 512, src.len() - offset);
            let readn = ro
                .pread(&mut small_buf[..len], offset as u64)
                .await
                .expect("random pread");
            assert_eq!(readn, len, "random read length mismatch at {offset}");
            assert_eq!(
                &small_buf[..len],
                &src[offset..offset + len],
                "random read content mismatch at {offset}"
            );
        }

        let large_len = MAX_READ_SIZE * 2;
        if src.len() <= large_len {
            return;
        }
        let mut large_buf = vec![0u8; large_len];
        for _ in 0..120 {
            let max_offset = src.len() - large_len;
            let offset = rng.random_range(0..=max_offset);
            let readn = ro
                .pread(&mut large_buf, offset as u64)
                .await
                .expect("large pread");
            assert_eq!(readn, large_len, "large read length mismatch at {offset}");
            assert_eq!(
                &large_buf[..],
                &src[offset..offset + large_len],
                "large read content mismatch at {offset}"
            );
        }
    }

    async fn build_zfile(data: &[u8], workers: usize, seed: u64) -> Arc<dyn VirtualFile> {
        let dst = new_local_vfile().await;
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers,
        };
        let mut rng = StdRng::seed_from_u64(seed);

        {
            let mut builder = new_zfile_builder(dst.clone(), &args)
                .await
                .expect("create builder");
            let mut offset = 0usize;
            while offset < data.len() {
                let step = ((rng.next_u32() as usize) % 8192) + 1;
                let chunk = min(step, data.len() - offset);
                let n = builder
                    .write(&data[offset..offset + chunk])
                    .await
                    .expect("builder write");
                assert_eq!(n, chunk);
                offset += chunk;
            }
            builder.finish().await.expect("builder finish");
        }

        dst
    }

    #[tokio::test]
    async fn test_verify_compression_matrix() {
        let data = sample_data(VERIFY_DATA_SIZE);

        for enable_crc in [0u8, 1u8] {
            for algo in [CompressOptions::LZ4, CompressOptions::ZSTD] {
                for bs_shift in 12..=16 {
                    let block_size = 1u32 << bs_shift;
                    let case_tag = format!("crc={enable_crc},algo={algo},bs={block_size}");

                    let src = new_local_vfile().await;
                    let dst = new_local_vfile().await;
                    let dec = new_local_vfile().await;
                    write_all(&src, &data).await;

                    let args = CompressArgs {
                        opt: CompressOptions::new(algo, block_size, enable_crc),
                        overwrite_header: false,
                        workers: 1,
                    };
                    zfile_compress(src.clone(), dst.clone(), &args)
                        .await
                        .unwrap_or_else(|e| panic!("compress failed for {case_tag}: {e}"));

                    assert_eq!(
                        is_zfile(dst.clone()).await.expect("is_zfile"),
                        1,
                        "dst should be zfile for {case_tag}"
                    );

                    let ro = zfile_open_ro(dst.clone(), enable_crc != 0)
                        .await
                        .unwrap_or_else(|e| panic!("open ro failed for {case_tag}: {e}"));
                    seqread_compare(&data, &ro).await;
                    randread_compare(&data, &ro, 0x77aa_5511 + block_size as u64 + algo as u64)
                        .await;

                    zfile_decompress(dst.clone(), dec.clone())
                        .await
                        .unwrap_or_else(|e| panic!("decompress failed for {case_tag}: {e}"));
                    assert_eq!(
                        read_all(&dec).await,
                        data,
                        "decompress compare failed for {case_tag}"
                    );
                    assert_eq!(
                        is_zfile(dec.clone()).await.expect("is_zfile decompressed"),
                        0,
                        "decompressed file should not be zfile for {case_tag}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_validation_check() {
        let data = sample_data(2 * 1024 * 1024 + 517);

        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;

        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src.clone(), dst.clone(), &args)
            .await
            .expect("compress");
        zfile_validation_check(dst.clone())
            .await
            .expect("validation before corruption");

        let corruption = vec![0xEEu8; 8192];
        write_all_at(dst.as_ref(), &corruption, 8192)
            .await
            .expect("corrupt data blocks");
        assert!(zfile_validation_check(dst.clone()).await.is_err());
    }

    #[tokio::test]
    async fn test_ht_check() {
        let data = sample_data(1024 * 1024 + 257);

        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;

        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };
        zfile_compress(src.clone(), dst.clone(), &args)
            .await
            .expect("compress");

        let bad_header = 2324u32.to_le_bytes();
        write_all_at(dst.as_ref(), &bad_header, 400)
            .await
            .expect("corrupt header");
        assert!(zfile_validation_check(dst.clone()).await.is_err());
        assert!(is_zfile(dst.clone()).await.is_err());
    }

    #[tokio::test]
    async fn test_verify_builder() {
        let data = sample_data(1024 * 1024 + 99);

        let zfile_mp = build_zfile(&data, 4, 0x4567_9001).await;
        let zfile_sp = build_zfile(&data, 1, 0x9753_1110).await;

        let bytes_mp = read_all(&zfile_mp).await;
        let bytes_sp = read_all(&zfile_sp).await;
        assert_eq!(bytes_mp, bytes_sp, "builder output byte mismatch");

        let ro = zfile_open_ro(zfile_mp.clone(), true)
            .await
            .expect("open mp zfile");
        seqread_compare(&data, &ro).await;
        randread_compare(&data, &ro, 0x9abc_dd01).await;
    }

    #[tokio::test]
    async fn compact_writer_fallback_writes_full_batch() {
        let data = sample_data(2 * ZFILE_COMPACT_WRITER_BUFFER_SIZE);
        let backing = Arc::new(CountingMemoryFile::new(Vec::new()));
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 4,
        };
        let writer = ZFileCompactWriter::new(backing.clone(), &args)
            .await
            .expect("create compact writer");
        backing.reset_max_write_len();
        storage_util::CompactWriter::write_all_at(&writer, &data, 0)
            .await
            .expect("write bounded batches");
        let max_write_len = backing.max_write_len();
        assert!(max_write_len > 0, "expected at least one physical write");
        // Non-LocalFile fallback writes the full batch in one async call.
        assert!(
            max_write_len > ZFILE_PHYSICAL_WRITE_CHUNK_SIZE as u64,
            "fallback should write full batch, got max_write_len={max_write_len}"
        );
    }

    #[tokio::test]
    async fn commit_compressed_batch_uses_sync_pwrite_for_large_batches() {
        // Three incompressible 4 KiB blocks compress to slightly more than
        // 4 KiB each, so the single committed batch is well above the 4 KiB
        // physical-write chunk size.
        let data = sample_data(3 * 4096);
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 1,
        };

        // A non-LocalFile destination cannot take the sync-pwrite path: the
        // batch falls back to a single async write_all_at call.
        let backing = Arc::new(CountingMemoryFile::new(Vec::new()));
        let mut builder = ZFileBuilder::new(backing.clone(), &args)
            .await
            .expect("create builder over memory file");
        backing.reset_max_write_len();
        let n = builder.write(&data).await.expect("builder write");
        assert_eq!(n, data.len());
        builder.finish().await.expect("builder finish");
        let max_write_len = backing.max_write_len();
        assert!(max_write_len > 0, "expected at least one physical write");
        assert!(
            max_write_len > ZFILE_PHYSICAL_WRITE_CHUNK_SIZE as u64,
            "fallback should write full batch, got max_write_len={max_write_len}"
        );
        let ro = zfile_open_ro(backing.clone(), true)
            .await
            .expect("open memory zfile");
        seqread_compare(&data, &ro).await;

        // A LocalFile destination commits the >4 KiB batch with one
        // synchronous pwrite instead of 4 KiB slices.
        let tmp = NamedTempFile::new().expect("create temp file");
        let path = tmp.path().to_path_buf();
        drop(tmp);
        let local = Arc::new(LocalFile::new(&path).expect("create local file"));
        let mut builder = ZFileBuilder::new(local.clone(), &args)
            .await
            .expect("create builder over local file");
        let before = local.write_calls();
        let n = builder.write(&data).await.expect("builder write");
        assert_eq!(n, data.len());
        assert_eq!(
            local.write_calls() - before,
            1,
            "large batch must land in one synchronous pwrite"
        );
        builder.finish().await.expect("builder finish");
        let local_vfile: Arc<dyn VirtualFile> = local;
        let ro = zfile_open_ro(local_vfile, true)
            .await
            .expect("open local zfile");
        seqread_compare(&data, &ro).await;
    }

    #[tokio::test]
    async fn compact_writer_validates_empty_write_all_at() {
        let backing = Arc::new(CountingMemoryFile::new(Vec::new()));
        let writer = ZFileCompactWriter::new(backing, &CompressArgs::default())
            .await
            .expect("create compact writer");

        let offset_error = storage_util::CompactWriter::write_all_at(&writer, &[], 1)
            .await
            .expect_err("empty write at wrong offset must fail");
        assert!(offset_error.to_string().contains("expected 0"));

        storage_util::CompactWriter::finalize(&writer)
            .await
            .expect("finalize compact writer");
        let finalized_error = storage_util::CompactWriter::write_all_at(&writer, &[], 0)
            .await
            .expect_err("empty write after finalize must fail");
        assert!(finalized_error.to_string().contains("already finalized"));
    }

    /// A poisoned compact writer rejects every later write and finalize
    /// with the poison error instead of running it.
    async fn assert_writer_poisoned(writer: &ZFileCompactWriter) {
        let retry_error = storage_util::CompactWriter::write(
            writer,
            Box::new(sample_data(ZFILE_COMPACT_WRITER_BUFFER_SIZE)),
            0,
            ZFILE_COMPACT_WRITER_BUFFER_SIZE,
        )
        .await
        .expect_err("write after poisoning must be rejected");
        assert!(retry_error
            .to_string()
            .contains("failed after previous write error"));

        let finalize_error = storage_util::CompactWriter::finalize(writer)
            .await
            .expect_err("finalize after poisoning must be rejected");
        assert!(finalize_error
            .to_string()
            .contains("failed after previous write error"));
    }

    #[tokio::test]
    async fn compact_writer_poisoned_after_data_write_failure() {
        let backing = Arc::new(FailNextWriteFile::new());
        let writer = ZFileCompactWriter::new(backing.clone(), &CompressArgs::default())
            .await
            .expect("create compact writer");
        let data = sample_data(ZFILE_COMPACT_WRITER_BUFFER_SIZE);

        backing.arm_write_failure();
        let write_error =
            storage_util::CompactWriter::write(&writer, Box::new(data.clone()), 0, data.len())
                .await
                .expect_err("armed destination write must fail");
        assert!(write_error
            .to_string()
            .contains("injected data write failure"));

        assert_writer_poisoned(&writer).await;
    }

    #[tokio::test]
    async fn compact_writer_poisoned_after_cancelled_data_write() {
        let backing = Arc::new(ParkSecondDataWriteFile::new());
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: false,
            workers: 4,
        };
        let writer = Arc::new(
            ZFileCompactWriter::new(backing.clone(), &args)
                .await
                .expect("create compact writer"),
        );
        let data = sample_data(ZFILE_COMPACT_WRITER_BUFFER_SIZE);
        let data_len = data.len();
        backing.arm_data_writes();

        let task_writer = writer.clone();
        let write_task = tokio::spawn(async move {
            storage_util::CompactWriter::write(task_writer.as_ref(), Box::new(data), 0, data_len)
                .await
        });
        backing.wait_for_second_data_write().await;
        write_task.abort();
        assert!(
            write_task
                .await
                .expect_err("write task must be cancelled")
                .is_cancelled(),
            "write task ended without cancellation"
        );

        assert_writer_poisoned(writer.as_ref()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compact_writer_worker_panic_returns_error_instead_of_hanging() {
        // A worker that panics mid-round must not hang the builder: the pool
        // converts the panic into that seq's Err result, the round drains,
        // and the write surfaces the error. The timeout only fires if the
        // drain deadlocks (multi_thread runtime so the timer can run while
        // the write future blocks a worker thread in `pool.recv()`).
        let backing = Arc::new(CountingMemoryFile::new(Vec::new()));
        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::ZSTD, 4096, 1),
            overwrite_header: false,
            workers: 4,
        };
        let mut builder = ZFileBuilder::new(backing, &args)
            .await
            .expect("create builder");
        let batch = sample_data(ZFILE_COMPACT_WRITER_BUFFER_SIZE);

        // Arm only this builder's pool so concurrent tests are unaffected;
        // the pool is dropped on the error path, so no disarm is needed.
        builder
            .pool
            .as_ref()
            .expect("workers > 1 must create a pool")
            .panic_on_work
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let error = tokio::time::timeout(Duration::from_secs(30), builder.write(&batch))
            .await
            .expect("write must not hang on worker panic")
            .expect_err("injected worker panic must surface as an error");
        assert!(
            error
                .to_string()
                .contains("zfile worker panicked during compression"),
            "unexpected error: {error}"
        );

        // The failed round committed nothing and dropped the pool, so the
        // builder falls back to the in-line compressor and stays usable.
        assert!(builder.pool.is_none(), "pool must be dropped after panic");
        builder
            .write(&batch)
            .await
            .expect("serial fallback write after panic");
    }

    #[tokio::test]
    async fn test_overwrite_header_path() {
        let data = sample_data(1024 * 1024 + 123);
        let src = new_local_vfile().await;
        let dst = new_local_vfile().await;
        write_all(&src, &data).await;

        let args = CompressArgs {
            opt: CompressOptions::new(CompressOptions::LZ4, 4096, 1),
            overwrite_header: true,
            workers: 1,
        };
        zfile_compress(src.clone(), dst.clone(), &args)
            .await
            .expect("compress with overwrite_header");

        let size = dst.size().await.expect("zfile size");
        assert!(size > HEADER_TRAILER_SPACE as u64);
        let trailer_offset = size - HEADER_TRAILER_SPACE as u64;
        // Corrupt trailer deliberately: open path should still rely on overwritten header.
        write_all_at(dst.as_ref(), &[0xEE; 32], trailer_offset)
            .await
            .expect("corrupt trailer");

        let ro = zfile_open_ro(dst.clone(), true)
            .await
            .expect("open zfile by overwritten header");
        seqread_compare(&data, &ro).await;
        randread_compare(&data, &ro, 0x5566_aabb).await;
    }

    #[tokio::test]
    async fn test_crc32c_consistency() {
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"123456789"), 0x58e3_fa20);
        assert_eq!(crc32c_salt(b"123456789"), 0x3c9b_d4e0);

        let mut rng = StdRng::seed_from_u64(0x8000_22dd_3388_12ab);
        let mut buf = [0u8; 1024];
        for _ in 0..3000 {
            rng.fill_bytes(&mut buf);
            let direct = crc32c(&buf);

            // Chaining the raw CRC state across an arbitrary split must
            // reproduce the one-shot value; this pins the same
            // complement-in/complement-out identity that crc32c_salt()
            // relies on for its seeded initial state.
            let split = (rng.next_u32() as usize) % buf.len();
            let first = crc32c(&buf[..split]);
            let incremental = !::crc32c::crc32c_append(!first, &buf[split..]);

            assert_eq!(direct, incremental);
        }
    }

    // -----------------------------------------------------------------------
    // CountingMemoryFile — deterministic write probe for zfile unit tests:
    // an in-memory VirtualFile recording the largest single physical write.
    // Test-module local on purpose.
    // -----------------------------------------------------------------------

    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::Notify;

    /// Write-size counters shared by the probe files below. Atomics keep
    /// resets usable through a shared reference.
    #[derive(Debug, Default)]
    struct ProbeCounters {
        max_write_len: AtomicU64,
    }

    impl ProbeCounters {
        fn reset_max_write_len(&self) {
            self.max_write_len.store(0, Ordering::SeqCst);
        }

        fn max_write_len(&self) -> u64 {
            self.max_write_len.load(Ordering::SeqCst)
        }
    }

    /// In-memory [`VirtualFile`][crate::io::virtual_file::VirtualFile] that
    /// records the largest single write. Writes grow the buffer.
    struct CountingMemoryFile {
        data: Mutex<Vec<u8>>,
        counters: ProbeCounters,
    }

    impl CountingMemoryFile {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data: Mutex::new(data),
                counters: ProbeCounters::default(),
            }
        }

        fn reset_max_write_len(&self) {
            self.counters.reset_max_write_len();
        }

        fn max_write_len(&self) -> u64 {
            self.counters.max_write_len()
        }
    }

    #[async_trait::async_trait]
    impl VirtualFile for CountingMemoryFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
            let data = self.data.lock().expect("data mutex poisoned");
            if len == 0 || offset >= data.len() as u64 {
                return Ok(Bytes::new());
            }
            let end = offset.saturating_add(len as u64).min(data.len() as u64) as usize;
            Ok(Bytes::copy_from_slice(&data[offset as usize..end]))
        }

        async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
            let bytes = self.read_at(offset, dst.len()).await?;
            dst[..bytes.len()].copy_from_slice(&bytes);
            Ok(bytes.len())
        }

        async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
            self.counters
                .max_write_len
                .fetch_max(data.len() as u64, Ordering::SeqCst);
            let mut inner = self.data.lock().expect("data mutex poisoned");
            let end = usize::try_from(offset)
                .context("write offset overflow")?
                .checked_add(data.len())
                .context("write offset overflow")?;
            if end > inner.len() {
                inner.resize(end, 0);
            }
            inner[offset as usize..end].copy_from_slice(data);
            Ok(data.len())
        }

        async fn size(&self) -> Result<u64> {
            Ok(self.data.lock().expect("data mutex poisoned").len() as u64)
        }
    }

    /// Implement `VirtualFile` for a write-interposing probe file: reads and
    /// size delegate to its inner [`CountingMemoryFile`], and the invocation
    /// supplies only the instrumented `write_at` method.
    macro_rules! impl_read_delegating_vfile {
        ($ty:ty; $($write_at:tt)*) => {
            #[async_trait::async_trait]
            impl VirtualFile for $ty {
                async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
                    self.inner.read_at(offset, len).await
                }

                async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
                    self.inner.read_at_into(offset, dst).await
                }

                $($write_at)*

                async fn size(&self) -> Result<u64> {
                    self.inner.size().await
                }
            }
        };
    }

    struct ParkSecondDataWriteFile {
        inner: CountingMemoryFile,
        armed: AtomicBool,
        data_writes: AtomicU64,
        second_write_started: Notify,
    }

    impl ParkSecondDataWriteFile {
        fn new() -> Self {
            Self {
                inner: CountingMemoryFile::new(Vec::new()),
                armed: AtomicBool::new(false),
                data_writes: AtomicU64::new(0),
                second_write_started: Notify::new(),
            }
        }

        fn arm_data_writes(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }

        async fn wait_for_second_data_write(&self) {
            self.second_write_started.notified().await;
        }
    }

    impl_read_delegating_vfile!(ParkSecondDataWriteFile;
        async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
            if self.armed.load(Ordering::SeqCst) {
                let data_write = self.data_writes.fetch_add(1, Ordering::SeqCst) + 1;
                if data_write == 2 {
                    self.second_write_started.notify_one();
                    std::future::pending::<()>().await;
                }
            }
            self.inner.write_at(offset, data).await
        }
    );

    struct FailNextWriteFile {
        inner: CountingMemoryFile,
        fail_next_write: AtomicBool,
    }

    impl FailNextWriteFile {
        fn new() -> Self {
            Self {
                inner: CountingMemoryFile::new(Vec::new()),
                fail_next_write: AtomicBool::new(false),
            }
        }

        fn arm_write_failure(&self) {
            self.fail_next_write.store(true, Ordering::SeqCst);
        }
    }

    impl_read_delegating_vfile!(FailNextWriteFile;
        async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
            if self.fail_next_write.swap(false, Ordering::SeqCst) {
                bail!("injected data write failure");
            }
            self.inner.write_at(offset, data).await
        }
    );
}
