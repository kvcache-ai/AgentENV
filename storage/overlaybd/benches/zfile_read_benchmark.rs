//! Criterion benchmark harness for the zfile read and compress paths.
//!
//! Read-path groups measure steady-state `read_at_into` throughput of the
//! shared handle from [`zfile_open_ro_vfile`] over an instrumented in-memory
//! backing file that records lower-file read calls, requested/returned bytes,
//! and the in-flight high-water mark (`max_in_flight`) — so serialization
//! shows up as data. The `compress_throughput` group times the full
//! `zfile_compress` pipeline, and a post-run table reports compression ratio
//! (raw bytes vs. the honest zfile artifact size: header + data + jump table
//! + trailer).
//!
//! Determinism: source data and offsets come from fixed seeds; all data,
//! buffers, and the zfile artifact are built outside the timed region;
//! backing counters are reset after open so header/index reads never pollute
//! steady-state numbers. Only non-timing invariants are asserted, never
//! wall-clock thresholds. All benchmarks use a current-thread Tokio runtime
//! and go through `read_at_into` to avoid the `read_at` `Bytes` allocation.

use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use criterion::{Criterion, Throughput};
use futures_util::future::join_all;
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::{zfile_compress, zfile_open_ro_vfile, CompressArgs, CompressOptions};
use rand::{rngs::StdRng, Rng, RngExt, SeedableRng};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const DATASET_SIZE: usize = 8 * 1024 * 1024;
const COMPRESS_DATASET_SIZE: usize = 16 * 1024 * 1024;
const MIXED_CHUNK_SIZE: usize = 4096;
const SEED_SOURCE: u64 = 0x5eed_5eed_0000_0001;
const SEED_OFFSETS: u64 = 0x5eed_5eed_0000_0002;
const RANDOM_OFFSET_COUNT: usize = 256;
const RANDOM_OFFSET_ALIGN: u64 = 4096;

// ---------------------------------------------------------------------------
// InstrumentedMemoryFile — in-memory VirtualFile with read-path counters.
// Bench-local on purpose: the zfile unit tests keep their own minimal probes
// inside the zfile test module.
// ---------------------------------------------------------------------------

/// Read-path counters of [`InstrumentedMemoryFile`]. Everything is atomic so
/// [`InstrumentedMemoryFile::reset_counters`] works through the shared `Arc`
/// while zfile holds its own `Arc<dyn VirtualFile>` reference.
#[derive(Debug, Default)]
struct ReadCounters {
    read_calls: AtomicU64,
    requested_bytes: AtomicU64,
    returned_bytes: AtomicU64,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
}

/// RAII guard that keeps `in_flight` balanced across awaits and early
/// returns, and raises the `max_in_flight` high-water mark on entry.
struct InFlightGuard<'a> {
    counters: &'a ReadCounters,
}

impl<'a> InFlightGuard<'a> {
    fn enter(counters: &'a ReadCounters, requested: u64) -> Self {
        counters.read_calls.fetch_add(1, Ordering::SeqCst);
        counters
            .requested_bytes
            .fetch_add(requested, Ordering::SeqCst);
        let current = counters.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        counters.max_in_flight.fetch_max(current, Ordering::SeqCst);
        Self { counters }
    }

    fn record_returned(&self, returned: u64) {
        self.counters
            .returned_bytes
            .fetch_add(returned, Ordering::SeqCst);
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.counters.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// In-memory [`VirtualFile`] that counts lower-file reads: read calls,
/// requested/returned bytes, and the in-flight high-water mark. Writes grow
/// the in-memory buffer so `zfile_compress` can build the fixture artifact
/// directly into it.
#[derive(Debug)]
struct InstrumentedMemoryFile {
    data: Mutex<Vec<u8>>,
    counters: ReadCounters,
}

impl InstrumentedMemoryFile {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(Vec::new()),
            counters: ReadCounters::default(),
        })
    }

    fn reset_counters(&self) {
        let counters = &self.counters;
        counters.read_calls.store(0, Ordering::SeqCst);
        counters.requested_bytes.store(0, Ordering::SeqCst);
        counters.returned_bytes.store(0, Ordering::SeqCst);
        counters.in_flight.store(0, Ordering::SeqCst);
        counters.max_in_flight.store(0, Ordering::SeqCst);
    }

    fn read_vec(&self, offset: u64, len: usize) -> Vec<u8> {
        let data = self.data.lock().expect("data mutex poisoned");
        if len == 0 || offset >= data.len() as u64 {
            return Vec::new();
        }
        let end = offset.saturating_add(len as u64).min(data.len() as u64) as usize;
        data[offset as usize..end].to_vec()
    }

    fn read_into(&self, offset: u64, dst: &mut [u8]) -> usize {
        let data = self.data.lock().expect("data mutex poisoned");
        if dst.is_empty() || offset >= data.len() as u64 {
            return 0;
        }
        let end = offset
            .saturating_add(dst.len() as u64)
            .min(data.len() as u64) as usize;
        let n = end - offset as usize;
        dst[..n].copy_from_slice(&data[offset as usize..end]);
        n
    }
}

#[async_trait]
impl VirtualFile for InstrumentedMemoryFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let guard = InFlightGuard::enter(&self.counters, len as u64);
        let bytes = self.read_vec(offset, len);
        guard.record_returned(bytes.len() as u64);
        Ok(Bytes::from(bytes))
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let guard = InFlightGuard::enter(&self.counters, dst.len() as u64);
        let n = self.read_into(offset, dst);
        guard.record_returned(n as u64);
        Ok(n)
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
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

// ---------------------------------------------------------------------------
// Workload matrix.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum DataKind {
    /// Seeded pseudo-random bytes (effectively incompressible).
    Random,
    /// Highly compressible repeating byte pattern.
    Compressible,
    /// 50/50 mix: alternating 4 KiB chunks of compressible pattern and
    /// seeded random bytes. Used by the compression benchmarks only.
    Mixed,
}

#[derive(Clone, Copy, Debug)]
enum Pattern {
    /// Random 4 KiB-aligned reads of `len` bytes, `qd` in flight per op on
    /// one shared zfile handle.
    Random { len: usize, qd: usize },
    /// Sequential full-chunk reads of `len` bytes, one in flight per op.
    Sequential { len: usize },
}

impl Pattern {
    const fn read_len(&self) -> usize {
        match *self {
            Pattern::Random { len, .. } => len,
            Pattern::Sequential { len } => len,
        }
    }

    const fn qd(&self) -> usize {
        match *self {
            Pattern::Random { qd, .. } => qd,
            Pattern::Sequential { .. } => 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct CaseSpec {
    /// Criterion case label; combines with the group name into the
    /// `BenchmarkId` used as baseline key.
    label: &'static str,
    algo: u8,
    data: DataKind,
    block_size: u32,
    pattern: Pattern,
}

const fn spec(
    label: &'static str,
    algo: u8,
    data: DataKind,
    block_size: u32,
    pattern: Pattern,
) -> CaseSpec {
    CaseSpec {
        label,
        algo,
        data,
        block_size,
        pattern,
    }
}

/// `throughput` group: LZ4/ZSTD x {4 KiB random QD1, 128 KiB sequential QD1,
/// QD16 on one shared handle}, plus one compressible-dataset contrast case
/// per algorithm. All with 4 KiB blocks. Labels are fixed literals so they
/// stay stable as baseline keys.
fn throughput_specs() -> Vec<CaseSpec> {
    let mut specs = Vec::new();
    for (algo, name) in [
        (CompressOptions::LZ4, "lz4"),
        (CompressOptions::ZSTD, "zstd"),
    ] {
        let random4k_qd1: &'static str = match name {
            "lz4" => "lz4_random4k_qd1",
            _ => "zstd_random4k_qd1",
        };
        let seq128k_qd1: &'static str = match name {
            "lz4" => "lz4_seq128k_qd1",
            _ => "zstd_seq128k_qd1",
        };
        let random4k_qd16: &'static str = match name {
            "lz4" => "lz4_random4k_qd16_shared",
            _ => "zstd_random4k_qd16_shared",
        };
        let compressible_qd1: &'static str = match name {
            "lz4" => "lz4_compressible_random4k_qd1",
            _ => "zstd_compressible_random4k_qd1",
        };
        specs.push(spec(
            random4k_qd1,
            algo,
            DataKind::Random,
            4096,
            Pattern::Random { len: 4096, qd: 1 },
        ));
        specs.push(spec(
            seq128k_qd1,
            algo,
            DataKind::Random,
            4096,
            Pattern::Sequential { len: 128 * 1024 },
        ));
        specs.push(spec(
            random4k_qd16,
            algo,
            DataKind::Random,
            4096,
            Pattern::Random { len: 4096, qd: 16 },
        ));
        specs.push(spec(
            compressible_qd1,
            algo,
            DataKind::Compressible,
            4096,
            Pattern::Random { len: 4096, qd: 1 },
        ));
    }
    specs
}

/// `block_size` group: 4 KiB vs 64 KiB zfile blocks on the same base case.
fn block_size_specs() -> Vec<CaseSpec> {
    vec![
        spec(
            "lz4_random4k_qd1_bs4k",
            CompressOptions::LZ4,
            DataKind::Random,
            4096,
            Pattern::Random { len: 4096, qd: 1 },
        ),
        spec(
            "lz4_random4k_qd1_bs64k",
            CompressOptions::LZ4,
            DataKind::Random,
            64 * 1024,
            Pattern::Random { len: 4096, qd: 1 },
        ),
    ]
}

/// Compression case: times the full `zfile_compress` pipeline and feeds the
/// post-run compression-ratio table.
#[derive(Clone, Copy, Debug)]
struct CompressCaseSpec {
    /// Criterion case label; combines with the group name into the
    /// `BenchmarkId` used as baseline key.
    label: &'static str,
    algo: u8,
    data: DataKind,
    block_size: u32,
}

/// `compress_throughput` group: LZ4/ZSTD x {compressible, random} x
/// {4 KiB, 64 KiB blocks}, plus one 50/50 mixed-dataset case per algorithm
/// at 4 KiB blocks (mixed skips 64 KiB to keep the matrix small). Labels are
/// fixed literals so they stay stable as baseline keys.
fn compress_specs() -> Vec<CompressCaseSpec> {
    vec![
        CompressCaseSpec {
            label: "lz4_compressible_bs4k",
            algo: CompressOptions::LZ4,
            data: DataKind::Compressible,
            block_size: 4096,
        },
        CompressCaseSpec {
            label: "lz4_compressible_bs64k",
            algo: CompressOptions::LZ4,
            data: DataKind::Compressible,
            block_size: 64 * 1024,
        },
        CompressCaseSpec {
            label: "lz4_random_bs4k",
            algo: CompressOptions::LZ4,
            data: DataKind::Random,
            block_size: 4096,
        },
        CompressCaseSpec {
            label: "lz4_random_bs64k",
            algo: CompressOptions::LZ4,
            data: DataKind::Random,
            block_size: 64 * 1024,
        },
        CompressCaseSpec {
            label: "lz4_mixed_bs4k",
            algo: CompressOptions::LZ4,
            data: DataKind::Mixed,
            block_size: 4096,
        },
        CompressCaseSpec {
            label: "zstd_compressible_bs4k",
            algo: CompressOptions::ZSTD,
            data: DataKind::Compressible,
            block_size: 4096,
        },
        CompressCaseSpec {
            label: "zstd_compressible_bs64k",
            algo: CompressOptions::ZSTD,
            data: DataKind::Compressible,
            block_size: 64 * 1024,
        },
        CompressCaseSpec {
            label: "zstd_random_bs4k",
            algo: CompressOptions::ZSTD,
            data: DataKind::Random,
            block_size: 4096,
        },
        CompressCaseSpec {
            label: "zstd_random_bs64k",
            algo: CompressOptions::ZSTD,
            data: DataKind::Random,
            block_size: 64 * 1024,
        },
        CompressCaseSpec {
            label: "zstd_mixed_bs4k",
            algo: CompressOptions::ZSTD,
            data: DataKind::Mixed,
            block_size: 4096,
        },
    ]
}

// ---------------------------------------------------------------------------
// Fixture construction and validation (all outside Criterion timed regions).
// ---------------------------------------------------------------------------

fn source_bytes(kind: DataKind, size: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(SEED_SOURCE);
    match kind {
        DataKind::Random => {
            let mut data = vec![0u8; size];
            rng.fill_bytes(&mut data);
            data
        }
        DataKind::Compressible => {
            // 64-byte period seeded tile repeated over the whole buffer.
            let mut tile = [0u8; 64];
            rng.fill_bytes(&mut tile);
            (0..size).map(|i| tile[i % tile.len()]).collect()
        }
        DataKind::Mixed => {
            // Random base, with every even 4 KiB chunk overwritten by the
            // repeating tile: half the logical blocks compress well, half
            // do not.
            let mut data = vec![0u8; size];
            rng.fill_bytes(&mut data);
            let mut tile = [0u8; 64];
            rng.fill_bytes(&mut tile);
            for (chunk_idx, chunk) in data.chunks_mut(MIXED_CHUNK_SIZE).enumerate() {
                if chunk_idx % 2 == 0 {
                    for (i, byte) in chunk.iter_mut().enumerate() {
                        *byte = tile[i % tile.len()];
                    }
                }
            }
            data
        }
    }
}

fn make_offsets(pattern: &Pattern, source_len: usize) -> Vec<u64> {
    match *pattern {
        Pattern::Random { len, .. } => {
            let mut rng = StdRng::seed_from_u64(SEED_OFFSETS);
            let slots = ((source_len - len) / RANDOM_OFFSET_ALIGN as usize) as u64;
            (0..RANDOM_OFFSET_COUNT)
                .map(|_| rng.random_range(0..=slots) * RANDOM_OFFSET_ALIGN)
                .collect()
        }
        Pattern::Sequential { len } => {
            let chunks = source_len / len;
            (0..chunks).map(|i| (i * len) as u64).collect()
        }
    }
}

struct Fixture {
    /// Shared zfile handle under test (`zfile_open_ro_vfile` output).
    vf: Arc<dyn VirtualFile>,
    /// Instrumented backing file holding the compressed artifact.
    backing: Arc<InstrumentedMemoryFile>,
    /// Decompressed source bytes for correctness comparison.
    source: Vec<u8>,
    /// Pre-generated offset sequence replayed by the measured ops.
    offsets: Vec<u64>,
}

async fn build_fixture(spec: &CaseSpec) -> Result<Fixture> {
    let source = source_bytes(spec.data, DATASET_SIZE);

    let src_file: Arc<dyn VirtualFile> = InstrumentedMemoryFile::new();
    let written = src_file
        .write_at(0, &source)
        .await
        .context("write source")?;
    ensure!(written == source.len(), "short source write");

    let backing = InstrumentedMemoryFile::new();
    let backing_file: Arc<dyn VirtualFile> = backing.clone();
    let args = CompressArgs {
        opt: CompressOptions::new(spec.algo, spec.block_size, 1),
        overwrite_header: false,
        workers: 1,
    };
    zfile_compress(src_file, backing_file.clone(), &args)
        .await
        .context("compress fixture")?;
    let vf = zfile_open_ro_vfile(backing_file, false)
        .await
        .context("open fixture zfile")?;
    let offsets = make_offsets(&spec.pattern, source.len());
    Ok(Fixture {
        vf,
        backing,
        source,
        offsets,
    })
}

/// Non-timing correctness invariant: every replayed offset must return the
/// exact source bytes. Runs before the counter reset, so these validation
/// reads never pollute steady-state numbers.
async fn validate_fixture(fixture: &Fixture, read_len: usize) -> Result<()> {
    let mut buf = vec![0u8; read_len];
    for &offset in &fixture.offsets {
        let n = fixture
            .vf
            .read_at_into(offset, &mut buf)
            .await
            .with_context(|| format!("validation read at {offset}"))?;
        ensure!(n == read_len, "validation short read at {offset}: {n}");
        let begin = offset as usize;
        ensure!(
            buf[..] == fixture.source[begin..begin + read_len],
            "validation data mismatch at {offset}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Measured section and reporting.
// ---------------------------------------------------------------------------

struct CaseSummary {
    id: String,
    ops: u64,
    read_calls: u64,
    requested_bytes: u64,
    returned_bytes: u64,
    max_in_flight: u64,
    in_flight_end: u64,
}

fn run_group(
    criterion: &mut Criterion,
    rt: &Runtime,
    group_name: &str,
    specs: &[CaseSpec],
    summaries: &mut Vec<CaseSummary>,
) {
    let mut group = criterion.benchmark_group(group_name);
    for spec in specs {
        let fixture = rt.block_on(build_fixture(spec)).expect("build fixture");
        let read_len = spec.pattern.read_len();
        let qd = spec.pattern.qd();
        rt.block_on(validate_fixture(&fixture, read_len))
            .expect("validate fixture");
        fixture.backing.reset_counters();

        let ops = Rc::new(Cell::new(0u64));
        group.throughput(Throughput::Bytes((read_len * qd) as u64));
        group.bench_function(spec.label, |b| {
            let vf = fixture.vf.clone();
            let offsets = Rc::new(fixture.offsets.clone());
            let ops = ops.clone();
            b.iter_custom(move |iters| {
                // Per-invocation setup: fresh output buffers, allocated
                // before the timed span starts.
                let mut bufs: Vec<Vec<u8>> = (0..qd).map(|_| vec![0u8; read_len]).collect();
                let mut offs: Vec<u64> = Vec::with_capacity(qd);
                let mut cursor = 0usize;
                let vf = vf.clone();
                let offsets = offsets.clone();
                let ops = ops.clone();
                rt.block_on(async move {
                    let start = Instant::now();
                    for _ in 0..iters {
                        offs.clear();
                        offs.extend((0..bufs.len()).map(|k| offsets[(cursor + k) % offsets.len()]));
                        cursor = (cursor + bufs.len()) % offsets.len();
                        let results = join_all(
                            bufs.iter_mut()
                                .zip(offs.iter())
                                .map(|(buf, &off)| vf.read_at_into(off, &mut buf[..])),
                        )
                        .await;
                        for (result, buf) in results.into_iter().zip(bufs.iter()) {
                            let n = result.expect("zfile read_at_into");
                            assert_eq!(n, buf.len(), "fixture short read");
                        }
                        ops.set(ops.get() + 1);
                    }
                    start.elapsed()
                })
            });
        });

        let counters = &fixture.backing.counters;
        let summary = CaseSummary {
            id: format!("{group_name}/{}", spec.label),
            ops: ops.get(),
            read_calls: counters.read_calls.load(Ordering::SeqCst),
            requested_bytes: counters.requested_bytes.load(Ordering::SeqCst),
            returned_bytes: counters.returned_bytes.load(Ordering::SeqCst),
            max_in_flight: counters.max_in_flight.load(Ordering::SeqCst),
            in_flight_end: counters.in_flight.load(Ordering::SeqCst),
        };
        if summary.ops == 0 {
            // Analysis-only criterion modes (e.g. `--load-baseline`
            // comparison runs) never execute a measured iteration, so the
            // probe counters stay at zero. Skip the invariant assertions
            // and the summary row for such cases instead of panicking.
            continue;
        }
        // Non-timing invariants only; no wall-clock thresholds.
        assert!(
            summary.max_in_flight >= 1,
            "{}: max_in_flight must be >= 1",
            summary.id
        );
        assert_eq!(
            summary.in_flight_end, 0,
            "{}: in_flight not drained after case",
            summary.id
        );
        summaries.push(summary);
    }
    group.finish();
}

fn print_summary(summaries: &[CaseSummary]) {
    println!();
    println!(
        "=== zfile read-path lower-file probe summary (post-run, counters sampled outside timing) ==="
    );
    println!(
        "{:<44} {:>8} {:>9} {:>11} {:>11} {:>10}",
        "case", "ops", "calls/op", "req B/op", "ret B/op", "max_inflt"
    );
    for s in summaries {
        println!(
            "{:<44} {:>8} {:>9.2} {:>11.1} {:>11.1} {:>10}",
            s.id,
            s.ops,
            s.read_calls as f64 / s.ops as f64,
            s.requested_bytes as f64 / s.ops as f64,
            s.returned_bytes as f64 / s.ops as f64,
            s.max_in_flight,
        );
    }
}

// ---------------------------------------------------------------------------
// Compression throughput and compression ratio.
// ---------------------------------------------------------------------------

struct CompressSummary {
    id: String,
    raw_bytes: u64,
    artifact_bytes: u64,
}

impl CompressSummary {
    fn ratio(&self) -> f64 {
        self.artifact_bytes as f64 / self.raw_bytes as f64
    }
}

/// Times the full `zfile_compress` pipeline per case and records one untimed
/// ratio probe per case. The timed span covers exactly one `zfile_compress`
/// call per iteration; the destination file is allocated outside the timed
/// span so allocation of the output buffer never distorts compress numbers.
fn run_compress_group(
    criterion: &mut Criterion,
    rt: &Runtime,
    specs: &[CompressCaseSpec],
    ratios: &mut Vec<CompressSummary>,
) {
    let mut group = criterion.benchmark_group("compress_throughput");
    for spec in specs {
        let source = source_bytes(spec.data, COMPRESS_DATASET_SIZE);
        let src_file: Arc<dyn VirtualFile> = InstrumentedMemoryFile::new();
        let written = rt
            .block_on(src_file.write_at(0, &source))
            .expect("write compress source");
        assert_eq!(written, source.len(), "short compress source write");
        let args = CompressArgs {
            opt: CompressOptions::new(spec.algo, spec.block_size, 1),
            overwrite_header: false,
            workers: 1,
        };

        // Untimed ratio probe: honest artifact size, i.e. the whole zfile
        // (header + compressed data + jump table + trailer).
        let artifact_bytes = rt
            .block_on(async {
                let dst: Arc<dyn VirtualFile> = InstrumentedMemoryFile::new();
                zfile_compress(src_file.clone(), dst.clone(), &args).await?;
                dst.size().await
            })
            .expect("ratio probe compress");
        let summary = CompressSummary {
            id: format!("compress_throughput/{}", spec.label),
            raw_bytes: source.len() as u64,
            artifact_bytes,
        };
        // Sanity guard on the ratio accounting itself, not a perf threshold:
        // the highly compressible dataset must actually shrink.
        if matches!(spec.data, DataKind::Compressible) {
            assert!(
                summary.ratio() < 1.0,
                "{}: compressible dataset did not shrink (ratio {:.4})",
                summary.id,
                summary.ratio()
            );
        }
        ratios.push(summary);

        group.throughput(Throughput::Bytes(source.len() as u64));
        group.bench_function(spec.label, |b| {
            let src_file = src_file.clone();
            b.iter_custom(move |iters| {
                let src_file = src_file.clone();
                rt.block_on(async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Fresh destination per iteration, allocated outside
                        // the timed span.
                        let dst: Arc<dyn VirtualFile> = InstrumentedMemoryFile::new();
                        let start = Instant::now();
                        zfile_compress(src_file.clone(), dst.clone(), &args)
                            .await
                            .expect("bench zfile_compress");
                        total += start.elapsed();
                    }
                    total
                })
            });
        });
    }
    group.finish();
}

fn print_compression_ratios(rows: &[CompressSummary]) {
    println!();
    println!(
        "=== zfile compression ratio (post-run, artifact = full zfile incl. header/index/trailer) ==="
    );
    println!(
        "{:<44} {:>12} {:>14} {:>8}",
        "case", "raw bytes", "artifact bytes", "ratio"
    );
    for r in rows {
        println!(
            "{:<44} {:>12} {:>14} {:>8.4}",
            r.id,
            r.raw_bytes,
            r.artifact_bytes,
            r.ratio(),
        );
    }
}

fn main() {
    // All benchmarks run on a current-thread Tokio runtime.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");
    let mut criterion = Criterion::default().configure_from_args();
    let mut summaries = Vec::new();
    run_group(
        &mut criterion,
        &rt,
        "throughput",
        &throughput_specs(),
        &mut summaries,
    );
    run_group(
        &mut criterion,
        &rt,
        "block_size",
        &block_size_specs(),
        &mut summaries,
    );
    let mut ratios = Vec::new();
    run_compress_group(&mut criterion, &rt, &compress_specs(), &mut ratios);
    print_summary(&summaries);
    print_compression_ratios(&ratios);
}
