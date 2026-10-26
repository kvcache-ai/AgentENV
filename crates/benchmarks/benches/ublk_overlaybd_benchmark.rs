#![allow(clippy::unwrap_used, clippy::expect_used)] // Test and benchmark code may unwrap or expect.
use anyhow::{bail, Context, Result};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use overlaybd::config::UpperMode;
use overlaybd::helper::prepare_runtime_upper;
use serde::Deserialize;
use std::cell::RefCell;
use std::fs;
use std::future::Future;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::task::LocalSet;
use uvm_ublk::{IOBuffer, OverlaybdTarget, UVMUblkTarget, UserBuffer};

use storage_util::io_ring::{AsyncIoRing, AsyncIoRingBuilder};

const KB: usize = 1024;
const MIB: usize = 1024 * KB;
const GIB: usize = 1024 * MIB;

const PRIME_IO_SIZE_BYTES: usize = 256 * KB;
const DEFAULT_DEVICE_SIZE_BYTES: usize = 32 * MIB;
const FULL_DEVICE_SIZE_BYTES: usize = GIB;
const MAX_IO_DEPTH: usize = 64;
const DEFAULT_SAMPLE_SIZE: usize = 10;
const DEFAULT_WARM_UP_TIME: Duration = Duration::from_millis(100);
const DEFAULT_MEASUREMENT_TIME: Duration = Duration::from_millis(500);
const FULL_SAMPLE_SIZE: usize = 50;
const FULL_MEASUREMENT_TIME: Duration = Duration::from_secs(12);
const SEED: u64 = 0xabcdbeefcafe1234;
const BENCH_UPPER_MODE_ENV: &str = "AENV_BENCH_UPPER_MODE";

fn bench_upper_mode() -> UpperMode {
    match std::env::var(BENCH_UPPER_MODE_ENV) {
        Ok(value) => match value.as_str() {
            "sparse" => UpperMode::Sparse,
            "log" => UpperMode::LogStructured,
            "hybrid" => UpperMode::HybridLogStructured,
            other => {
                panic!("{BENCH_UPPER_MODE_ENV} must be one of sparse, log, or hybrid; got {other}")
            }
        },
        Err(_) => UpperMode::LogStructured,
    }
}

#[derive(Debug, Clone, Copy)]
struct BenchParams {
    io_size: usize,
    io_depth: usize,
}

impl BenchParams {
    fn op_sectors(self) -> u32 {
        (self.io_size / 512) as u32
    }
}

#[derive(Debug, Clone, Copy)]
enum AccessPattern {
    SequentialRead,
    RandomRead,
    RandomWrite,
}

#[derive(Debug, Clone, Copy)]
struct BenchCase {
    name: &'static str,
    params: BenchParams,
    access: AccessPattern,
}

const BENCH_CASES: &[BenchCase] = &[
    BenchCase {
        name: "read_seq_1m_qd8",
        params: BenchParams {
            io_size: MIB,
            io_depth: 8,
        },
        access: AccessPattern::SequentialRead,
    },
    BenchCase {
        name: "read_seq_128k_qd32",
        params: BenchParams {
            io_size: 128 * KB,
            io_depth: 32,
        },
        access: AccessPattern::SequentialRead,
    },
    BenchCase {
        name: "read_rand_4k_qd1",
        params: BenchParams {
            io_size: 4 * KB,
            io_depth: 1,
        },
        access: AccessPattern::RandomRead,
    },
    BenchCase {
        name: "read_rand_4k_qd64",
        params: BenchParams {
            io_size: 4 * KB,
            io_depth: 64,
        },
        access: AccessPattern::RandomRead,
    },
    BenchCase {
        name: "write_rand_4k_qd1",
        params: BenchParams {
            io_size: 4 * KB,
            io_depth: 1,
        },
        access: AccessPattern::RandomWrite,
    },
    BenchCase {
        name: "write_rand_4k_qd64",
        params: BenchParams {
            io_size: 4 * KB,
            io_depth: 64,
        },
        access: AccessPattern::RandomWrite,
    },
];

const UBLK_OVERLAYBD_ORDER: [&str; 6] = [
    "read_seq_1m_qd8",
    "read_seq_128k_qd32",
    "read_rand_4k_qd1",
    "read_rand_4k_qd64",
    "write_rand_4k_qd1",
    "write_rand_4k_qd64",
];

thread_local! {
    static BENCH_RING: RefCell<Option<AsyncIoRing>> = const { RefCell::new(None) };
}

struct BenchRuntime {
    rt: Runtime,
    local_set: LocalSet,
    ring: AsyncIoRing,
}

impl BenchRuntime {
    fn new() -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .on_thread_park(|| {
                BENCH_RING.with(|slot| {
                    if let Some(ring) = slot.borrow().as_ref() {
                        let _ = ring.borrow().submit();
                    }
                });
            })
            .build()
            .context("build tokio runtime for benchmark")?;

        let ring = AsyncIoRingBuilder::new()
            .nr_sparse_buffer(MAX_IO_DEPTH + 2)
            .nr_sparse_file(16)
            .sqe_entries(256)
            .cqe_entries(512)
            .build()
            .context("build io_uring for benchmark")?;

        BENCH_RING.with(|slot| {
            *slot.borrow_mut() = Some(ring.clone());
        });

        let local_set = LocalSet::new();
        local_set.spawn_local({
            let ring = ring.clone();
            async move {
                if let Err(err) = ring.handle_completion().await {
                    panic!("benchmark completion task exited: {err:#}");
                }
            }
        });

        Ok(Self {
            rt,
            local_set,
            ring,
        })
    }

    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.local_set.block_on(&self.rt, fut)
    }

    fn ring(&self) -> AsyncIoRing {
        self.ring.clone()
    }
}

struct BenchEnv {
    _temp_dir: TempDir,
    upper_data_fd: std::fs::File,
    ring: AsyncIoRing,
    target: Arc<OverlaybdTarget>,
    buf: IOBuffer,
    prime_buf: IOBuffer,
    depth_bufs: Vec<IOBuffer>,
    dev_size_sectors: u64,
    next_read_sector: u64,
    io_depth: usize,
}

fn full_bench_mode() -> bool {
    std::env::var_os("AENV_BENCH_FULL").is_some()
}

fn device_size_bytes() -> usize {
    if full_bench_mode() {
        FULL_DEVICE_SIZE_BYTES
    } else {
        DEFAULT_DEVICE_SIZE_BYTES
    }
}

fn criterion_config() -> Criterion {
    if full_bench_mode() {
        Criterion::default()
            .sample_size(FULL_SAMPLE_SIZE)
            .measurement_time(FULL_MEASUREMENT_TIME)
    } else {
        Criterion::default()
            .sample_size(DEFAULT_SAMPLE_SIZE)
            .warm_up_time(DEFAULT_WARM_UP_TIME)
            .measurement_time(DEFAULT_MEASUREMENT_TIME)
    }
}

struct RandomBenchState {
    runtime: BenchRuntime,
    env: BenchEnv,
    params: BenchParams,
    prime: bool,
    sector_order: Vec<u64>,
    next_idx: usize,
}

#[derive(Debug, Deserialize)]
struct CriterionBenchmarkMeta {
    function_id: Option<String>,
    throughput: Option<CriterionThroughput>,
}

#[derive(Debug, Deserialize)]
struct CriterionThroughput {
    #[serde(rename = "Bytes")]
    bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CriterionEstimates {
    mean: CriterionEstimate,
}

#[derive(Debug, Deserialize)]
struct CriterionEstimate {
    point_estimate: f64,
}

#[derive(Debug, Clone)]
struct BenchSummary {
    name: String,
    mean_us: f64,
    throughput_gib_s: Option<f64>,
}

impl RandomBenchState {
    fn new(params: BenchParams, prime: bool) -> Self {
        let runtime = BenchRuntime::new().expect("create benchmark runtime");
        let mut env = runtime
            .block_on(BenchEnv::new(
                runtime.ring(),
                params.io_size,
                params.io_depth,
            ))
            .expect("create benchmark env");
        if prime {
            runtime
                .block_on(env.prime_all())
                .expect("prime all sectors for random read");
        }
        let ops_total = (env.dev_size_sectors / params.op_sectors() as u64) as usize;
        let mut sector_order: Vec<u64> = (0..ops_total as u64)
            .map(|i| i * params.op_sectors() as u64)
            .collect();

        // Fixed seed is intentional: deterministic shuffle ensures reproducible access
        // patterns across runs, which is desirable for benchmark comparability.
        let mut rng: u64 = SEED;
        for i in (1..sector_order.len()).rev() {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let j = (rng as usize) % (i + 1);
            sector_order.swap(i, j);
        }
        Self {
            runtime,
            env,
            params,
            prime,
            sector_order,
            next_idx: 0,
        }
    }

    fn new_primed(params: BenchParams) -> Self {
        Self::new(params, true)
    }

    fn new_unprimed(params: BenchParams) -> Self {
        Self::new(params, false)
    }

    fn alloc_random_sector(&mut self) -> u64 {
        if self.next_idx >= self.sector_order.len() {
            *self = Self::new(self.params, self.prime);
        }
        let s = self.sector_order[self.next_idx];
        self.next_idx += 1;
        s
    }
}

fn write_overlaybd_global_config(dir: &Path) -> Result<PathBuf> {
    let path = dir.join("global.json");
    let cache_dir = dir.join("cache");
    fs::create_dir_all(&cache_dir).context("create overlaybd cache dir")?;
    let json = serde_json::json!({
        "registryFsVersion": "v2",
        "nrIoRings": 1,
        "cacheConfig": {
            "cacheType": "file",
            "cacheDir": cache_dir,
            "cacheSizeGB": 1,
            "refillSize": 262144
        },
        "download": { "enable": false }
    });
    fs::write(
        &path,
        serde_json::to_vec_pretty(&json).context("serialize overlaybd global config")?,
    )
    .context("write overlaybd global config")?;
    Ok(path)
}

fn write_overlaybd_image_config(
    dir: &Path,
    upper_data: &Path,
    upper_index: Option<&Path>,
    upper_mode: UpperMode,
) -> Result<PathBuf> {
    let path = dir.join("image.json");
    let upper = if let Some(upper_index) = upper_index {
        serde_json::json!({
            "mode": upper_mode,
            "data": upper_data,
            "index": upper_index
        })
    } else {
        serde_json::json!({
            "mode": upper_mode,
            "data": upper_data
        })
    };
    let json = serde_json::json!({
        "lowers": [],
        "upper": upper,
        "resultFile": dir.join("result.txt")
    });
    fs::write(
        &path,
        serde_json::to_vec_pretty(&json).context("serialize overlaybd image config")?,
    )
    .context("write overlaybd image config")?;
    Ok(path)
}

impl BenchEnv {
    async fn new(ring: AsyncIoRing, io_size: usize, io_depth: usize) -> Result<Self> {
        let temp_dir = tempfile::tempdir().context("create tempdir for benchmark")?;

        let global_config = write_overlaybd_global_config(temp_dir.path())
            .context("write overlaybd global config")?;

        let upper_data = temp_dir.path().join("upper.data");
        let upper_mode = bench_upper_mode();
        let upper_index = match upper_mode {
            UpperMode::Sparse => None,
            UpperMode::LogStructured | UpperMode::HybridLogStructured => {
                Some(temp_dir.path().join("upper.index"))
            }
        };
        prepare_runtime_upper(
            &upper_data,
            upper_index.as_deref(),
            device_size_bytes() as u64,
            upper_mode,
        )
        .context("prepare overlaybd upper")?;
        let image_config = write_overlaybd_image_config(
            temp_dir.path(),
            &upper_data,
            upper_index.as_deref(),
            upper_mode,
        )
        .context("write overlaybd image config")?;

        let target = Arc::new(
            OverlaybdTarget::open(&global_config, &image_config)
                .await
                .context("open OverlaybdTarget")?,
        );
        target
            .init(0, &ring)
            .await
            .context("init OverlaybdTarget")?;

        let mut buf = IOBuffer::User(
            UserBuffer::new(ring.clone(), io_size, 512).context("create benchmark IO buffer")?,
        );
        fill_write_pattern(&mut buf, io_size);

        let mut prime_buf = IOBuffer::User(
            UserBuffer::new(ring.clone(), PRIME_IO_SIZE_BYTES, 512)
                .context("create prime IO buffer")?,
        );
        fill_write_pattern(&mut prime_buf, PRIME_IO_SIZE_BYTES);

        let depth_bufs = if io_depth > 1 {
            let mut bufs = Vec::with_capacity(io_depth);
            for _ in 0..io_depth {
                bufs.push(IOBuffer::User(
                    UserBuffer::new(ring.clone(), io_size, 512)
                        .context("create depth IO buffer")?,
                ));
            }
            bufs
        } else {
            Vec::new()
        };

        Ok(Self {
            _temp_dir: temp_dir,
            upper_data_fd: std::fs::File::open(&upper_data)
                .context("open upper.data for cache eviction")?,
            ring,
            target,
            buf,
            prime_buf,
            depth_bufs,
            dev_size_sectors: (device_size_bytes() / 512) as u64,
            next_read_sector: 0,
            io_depth,
        })
    }

    async fn prime_all(&mut self) -> Result<()> {
        self.prime_region(0, self.dev_size_sectors).await
    }

    async fn prime_region(&mut self, start: u64, end: u64) -> Result<()> {
        let op_sectors = (PRIME_IO_SIZE_BYTES / 512) as u32;
        let expected = (op_sectors as i32) * 512;
        let mut sector = start;
        while sector + op_sectors as u64 <= end {
            let io_desc = ublk_sys::ublksrv_io_desc {
                op_flags: ublk_sys::UBLK_IO_OP_WRITE,
                nr_sectors: op_sectors,
                start_sector: sector,
                addr: self.prime_buf.uring_buf_idx().unwrap() as u64,
            };
            let ret = self
                .target
                .handle_io_request(0, 0, io_desc, &mut self.prime_buf, None, &self.ring)
                .await;
            if ret != expected {
                bail!("prime_region write failed at sector {sector}: expect {expected}, got {ret}");
            }
            sector += op_sectors as u64;
        }
        Ok(())
    }

    fn evict_sector_cache(&self) {
        // overlaybd stores sector data at physical offsets determined by its internal LSMT index,
        // not at start_sector*512. Evict the entire upper.data file (offset=0, len=0 → whole file)
        // so the next read truly hits disk regardless of which sectors are accessed.
        let rc = unsafe {
            libc::posix_fadvise(
                self.upper_data_fd.as_raw_fd(),
                0,
                0,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        debug_assert_eq!(rc, 0, "posix_fadvise(FADV_DONTNEED) failed: {rc}");
    }

    async fn issue_request(&mut self, op: u32, start_sector: u64, nr_sectors: u32) -> i32 {
        let io_desc = ublk_sys::ublksrv_io_desc {
            op_flags: op,
            nr_sectors,
            start_sector,
            addr: self.buf.uring_buf_idx().unwrap() as u64,
        };
        self.target
            .handle_io_request(0, 0, io_desc, &mut self.buf, None, &self.ring)
            .await
    }

    async fn issue_concurrent(&mut self, op: u32, starts: &[u64], nr_sectors: u32) -> i32 {
        debug_assert!(
            !starts.is_empty(),
            "issue_concurrent: starts slice must not be empty"
        );
        if self.io_depth == 1 {
            return self.issue_request(op, starts[0], nr_sectors).await;
        }
        let bufs = std::mem::take(&mut self.depth_bufs);
        assert_eq!(
            bufs.len(),
            starts.len(),
            "issue_concurrent: depth_bufs.len() ({}) != starts.len() ({}); zip would silently drop extras",
            bufs.len(),
            starts.len(),
        );
        let target = Arc::clone(&self.target);
        let ring = self.ring.clone();
        let handles: Vec<_> = bufs
            .into_iter()
            .zip(starts.iter().copied())
            .map(|(mut buf, start)| {
                let target = Arc::clone(&target);
                let ring = ring.clone();
                tokio::task::spawn_local(async move {
                    let io_desc = ublk_sys::ublksrv_io_desc {
                        op_flags: op,
                        nr_sectors,
                        start_sector: start,
                        addr: buf.uring_buf_idx().unwrap() as u64,
                    };
                    let ret = target
                        .handle_io_request(0, 0, io_desc, &mut buf, None, &ring)
                        .await;
                    (ret, buf)
                })
            })
            .collect();
        let mut total = 0i32;
        let mut recovered = Vec::with_capacity(handles.len());
        for h in handles {
            let (ret, buf) = h.await.unwrap();
            total += ret;
            recovered.push(buf);
        }
        self.depth_bufs = recovered;
        total
    }

    fn next_read_sector(&mut self, nr_sectors: u32) -> u64 {
        if self.next_read_sector + nr_sectors as u64 > self.dev_size_sectors {
            self.next_read_sector = 0;
        }
        let start = self.next_read_sector;
        self.next_read_sector += nr_sectors as u64;
        start
    }
}

fn fill_write_pattern(buf: &mut IOBuffer, io_size: usize) {
    match buf {
        IOBuffer::User(user_buf) => {
            let data = user_buf.subslice_mut(0, io_size);
            for (idx, b) in data.iter_mut().enumerate() {
                *b = (idx as u8).wrapping_mul(31).wrapping_add(17);
            }
        }
        IOBuffer::AutoReg(_) => panic!(
            "fill_write_pattern: unsupported IOBuffer variant; benchmark would silently write zeroes"
        ),
    }
}

fn criterion_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        PathBuf::from(dir).join("criterion")
    } else {
        PathBuf::from("target").join("criterion")
    }
}

fn bench_order(name: &str) -> usize {
    UBLK_OVERLAYBD_ORDER
        .iter()
        .position(|candidate| candidate == &name)
        .unwrap_or(usize::MAX)
}

fn escape_xml(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\"', "&quot;")
}

fn build_bar_chart_svg<F>(
    title: &str,
    x_axis_label: &str,
    rows: &[BenchSummary],
    value_of: F,
    tick_formatter: impl Fn(f64) -> String,
    value_formatter: impl Fn(f64) -> String,
    bar_color: &str,
) -> String
where
    F: Fn(&BenchSummary) -> f64,
{
    let left: f64 = 320.0;
    let right: f64 = 140.0;
    let top: f64 = 72.0;
    let bottom: f64 = 78.0;
    let row_h: f64 = 46.0;
    let bar_h: f64 = 24.0;
    let chart_w: f64 = 660.0;
    let ticks = 5usize;

    let max_value = rows.iter().map(&value_of).fold(0.0_f64, f64::max).max(1.0);

    let width = (left + chart_w + right).round() as i32;
    let height = (top + bottom + row_h * (rows.len() as f64)).round() as i32;

    let mut svg = String::new();
    svg.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    svg.push('\n');
    svg.push_str(&format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">"#
    ));
    svg.push('\n');
    svg.push_str(
        r#"<style>
.title { font: 24px sans-serif; fill: #1f2937; }
.axis-label { font: 13px sans-serif; fill: #4b5563; }
.tick { font: 12px sans-serif; fill: #6b7280; }
.label { font: 13px sans-serif; fill: #111827; }
.value { font: 12px sans-serif; fill: #374151; }
.grid { stroke: #e5e7eb; stroke-width: 1; }
.axis { stroke: #9ca3af; stroke-width: 1.2; }
</style>"#,
    );
    svg.push('\n');
    svg.push_str(&format!(
        r#"<text class="title" x="{:.1}" y="38">{}</text>"#,
        left,
        escape_xml(title)
    ));
    svg.push('\n');

    for tick in 0..=ticks {
        let ratio = tick as f64 / ticks as f64;
        let x = left + chart_w * ratio;
        let tick_value = tick_formatter(max_value * ratio);
        svg.push_str(&format!(
            r#"<line class="grid" x1="{x:.2}" y1="{top:.2}" x2="{x:.2}" y2="{:.2}"/>"#,
            height as f64 - bottom
        ));
        svg.push('\n');
        svg.push_str(&format!(
            r#"<text class="tick" x="{x:.2}" y="{:.2}" text-anchor="middle">{}</text>"#,
            height as f64 - bottom + 24.0,
            escape_xml(&tick_value)
        ));
        svg.push('\n');
    }

    let y_bottom = height as f64 - bottom;
    svg.push_str(&format!(
        r#"<line class="axis" x1="{left:.2}" y1="{y_bottom:.2}" x2="{:.2}" y2="{y_bottom:.2}"/>"#,
        left + chart_w
    ));
    svg.push('\n');
    svg.push_str(&format!(
        r#"<text class="axis-label" x="{:.2}" y="{:.2}" text-anchor="middle">{}</text>"#,
        left + chart_w / 2.0,
        height as f64 - 20.0,
        escape_xml(x_axis_label)
    ));
    svg.push('\n');

    for (idx, row) in rows.iter().enumerate() {
        let y = top + (idx as f64) * row_h + (row_h - bar_h) / 2.0;
        let value = value_of(row).max(0.0);
        let bar_w = (value / max_value) * chart_w;
        let label_y = y + bar_h / 2.0 + 4.5;
        svg.push_str(&format!(
            r#"<text class="label" x="{:.2}" y="{label_y:.2}" text-anchor="end">{}</text>"#,
            left - 12.0,
            escape_xml(&row.name)
        ));
        svg.push('\n');
        svg.push_str(&format!(
            r#"<rect x="{left:.2}" y="{y:.2}" width="{bar_w:.2}" height="{bar_h:.2}" rx="4" fill="{bar_color}"/>"#
        ));
        svg.push('\n');
        svg.push_str(&format!(
            r#"<text class="value" x="{:.2}" y="{label_y:.2}" text-anchor="start">{}</text>"#,
            left + bar_w + 8.0,
            escape_xml(&value_formatter(value))
        ));
        svg.push('\n');
    }

    svg.push_str("</svg>\n");
    svg
}

fn rewrite_summary_index_to_bar_charts(index_path: &Path) -> Result<()> {
    let index = fs::read_to_string(index_path)
        .with_context(|| format!("read criterion summary index: {}", index_path.display()))?;

    let updated = index
        .replace("<h3>Line Chart</h3>", "<h3>Bar Chart</h3>")
        .replace("<img src=\"lines.svg\" alt=\"Line Chart\" />", "<img src=\"lines.svg\" alt=\"Bar Chart\" />")
        .replace(
            "This chart shows the mean measured time for each function as the input (or the size of the input) increases.",
            "This bar chart shows the mean measured time for each function.",
        )
        .replace("<h3>Throughput Chart</h3>", "<h3>Throughput Bar Chart</h3>")
        .replace(
            "<img src=\"lines_throughput.svg\" alt=\"Line Chart\" />",
            "<img src=\"lines_throughput.svg\" alt=\"Throughput Bar Chart\" />",
        )
        .replace(
            "This chart shows the mean measured throughput for each function as the input (or the size of the input) increases.",
            "This bar chart shows the mean measured throughput for each function.",
        );

    fs::write(index_path, updated)
        .with_context(|| format!("write criterion summary index: {}", index_path.display()))?;
    Ok(())
}

fn rewrite_criterion_summary_with_bar_charts(group_name: &str) -> Result<()> {
    let group_dir = criterion_dir().join(group_name);
    if !group_dir.exists() {
        return Ok(());
    }

    let mut rows = Vec::new();
    for entry in fs::read_dir(&group_dir)
        .with_context(|| format!("list criterion group dir: {}", group_dir.display()))?
    {
        let entry = entry.with_context(|| format!("read dir entry in {}", group_dir.display()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let dir_name = entry.file_name();
        if dir_name == "report" {
            continue;
        }

        let benchmark_path = path.join("new").join("benchmark.json");
        let estimates_path = path.join("new").join("estimates.json");
        if !benchmark_path.exists() || !estimates_path.exists() {
            continue;
        }

        let benchmark: CriterionBenchmarkMeta =
            serde_json::from_slice(&fs::read(&benchmark_path).with_context(|| {
                format!(
                    "read criterion benchmark metadata: {}",
                    benchmark_path.display()
                )
            })?)
            .with_context(|| format!("parse criterion metadata: {}", benchmark_path.display()))?;

        let estimates: CriterionEstimates =
            serde_json::from_slice(&fs::read(&estimates_path).with_context(|| {
                format!("read criterion estimates: {}", estimates_path.display())
            })?)
            .with_context(|| format!("parse criterion estimates: {}", estimates_path.display()))?;

        let mean_ns = estimates.mean.point_estimate;
        if mean_ns <= 0.0 {
            continue;
        }

        let name = benchmark
            .function_id
            .unwrap_or_else(|| dir_name.to_string_lossy().to_string());
        let bytes_per_iter = benchmark.throughput.and_then(|t| t.bytes);

        let mean_us = mean_ns / 1000.0;
        let throughput_gib_s = bytes_per_iter
            .map(|b| b as f64 * 1_000_000_000.0 / mean_ns / (1024.0 * 1024.0 * 1024.0));
        rows.push(BenchSummary {
            name,
            mean_us,
            throughput_gib_s,
        });
    }

    if rows.is_empty() {
        return Ok(());
    }

    rows.sort_by(|a, b| {
        bench_order(&a.name)
            .cmp(&bench_order(&b.name))
            .then_with(|| a.name.cmp(&b.name))
    });

    let report_dir = group_dir.join("report");
    fs::create_dir_all(&report_dir)
        .with_context(|| format!("create criterion report dir: {}", report_dir.display()))?;

    let mean_svg = build_bar_chart_svg(
        &format!("{group_name}: Mean Latency"),
        "Average time (us)",
        &rows,
        |row| row.mean_us,
        |value| format!("{value:.0}"),
        |value| format!("{value:.2} us"),
        "#2563eb",
    );
    fs::write(report_dir.join("lines.svg"), mean_svg)
        .with_context(|| format!("write {}", report_dir.join("lines.svg").display()))?;

    let throughput_rows: Vec<BenchSummary> = rows
        .iter()
        .filter(|r| r.throughput_gib_s.is_some())
        .cloned()
        .collect();
    if !throughput_rows.is_empty() {
        let throughput_svg = build_bar_chart_svg(
            &format!("{group_name}: Throughput"),
            "Average throughput (GiB/s)",
            &throughput_rows,
            |row| row.throughput_gib_s.unwrap(),
            |value| format!("{value:.2}"),
            |value| format!("{value:.2} GiB/s"),
            "#059669",
        );
        fs::write(report_dir.join("lines_throughput.svg"), throughput_svg).with_context(|| {
            format!(
                "write {}",
                report_dir.join("lines_throughput.svg").display()
            )
        })?;
    }

    rewrite_summary_index_to_bar_charts(&report_dir.join("index.html"))?;
    Ok(())
}

fn bench_overlaybd(c: &mut Criterion) {
    let mut group = c.benchmark_group("ublk_overlaybd");
    if !full_bench_mode() {
        group.sampling_mode(SamplingMode::Flat);
    }

    for &case in BENCH_CASES {
        let params = case.params;
        let expected_bytes = params.io_size as i32 * params.io_depth as i32;

        group.throughput(Throughput::Bytes((params.io_size * params.io_depth) as u64));
        group.bench_with_input(case.name, &case, |b, &case| match case.access {
            AccessPattern::SequentialRead => {
                let params = case.params;
                let op_sectors = params.op_sectors();
                let runtime = BenchRuntime::new().expect("create benchmark runtime");
                let mut env = runtime
                    .block_on(BenchEnv::new(
                        runtime.ring(),
                        params.io_size,
                        params.io_depth,
                    ))
                    .expect("create benchmark env");
                runtime
                    .block_on(env.prime_all())
                    .expect("prime all sectors for cold sequential read");

                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let mut starts = vec![0u64; params.io_depth];
                        for s in &mut starts {
                            *s = env.next_read_sector(op_sectors);
                        }
                        // Evict before every read, including after wrap-around, so all
                        // iterations measure a true cache miss regardless of sector reuse.
                        env.evict_sector_cache();

                        let begin = Instant::now();
                        let total = runtime.block_on(env.issue_concurrent(
                            ublk_sys::UBLK_IO_OP_READ,
                            &starts,
                            op_sectors,
                        ));
                        elapsed += begin.elapsed();
                        assert_eq!(
                            total, expected_bytes,
                            "cold sequential read operation returned unexpected size"
                        );
                    }
                    elapsed
                });
            }
            AccessPattern::RandomRead => {
                let params = case.params;
                let op_sectors = params.op_sectors();
                let mut state = RandomBenchState::new_primed(params);

                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let mut starts = vec![0u64; params.io_depth];
                        for s in &mut starts {
                            *s = state.alloc_random_sector();
                        }
                        state.env.evict_sector_cache();

                        let runtime = &state.runtime;
                        let env = &mut state.env;

                        let begin = Instant::now();
                        let total = runtime.block_on(env.issue_concurrent(
                            ublk_sys::UBLK_IO_OP_READ,
                            &starts,
                            op_sectors,
                        ));
                        elapsed += begin.elapsed();
                        assert_eq!(
                            total, expected_bytes,
                            "cold random read operation returned unexpected size"
                        );
                    }
                    elapsed
                });
            }
            AccessPattern::RandomWrite => {
                let params = case.params;
                let op_sectors = params.op_sectors();
                let mut state = RandomBenchState::new_unprimed(params);

                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let mut starts = vec![0u64; params.io_depth];
                        for s in &mut starts {
                            *s = state.alloc_random_sector();
                        }

                        let runtime = &state.runtime;
                        let env = &mut state.env;

                        let begin = Instant::now();
                        let total = runtime.block_on(env.issue_concurrent(
                            ublk_sys::UBLK_IO_OP_WRITE,
                            &starts,
                            op_sectors,
                        ));
                        elapsed += begin.elapsed();
                        assert_eq!(
                            total, expected_bytes,
                            "random write operation returned unexpected size"
                        );
                    }
                    elapsed
                });
            }
        });
    }

    group.finish();

    if let Err(err) = rewrite_criterion_summary_with_bar_charts("ublk_overlaybd") {
        eprintln!("failed to rewrite criterion summary as bar charts: {err:#}");
    }
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_overlaybd
}
criterion_main!(benches);
