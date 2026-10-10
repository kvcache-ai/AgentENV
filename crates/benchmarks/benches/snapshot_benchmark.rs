use agentenv::image::ImageResolver;
use agentenv::sandbox::{
    CapturedSandboxSnapshot, FirecrackerSandbox, FirecrackerSandboxConfig,
    FirecrackerSnapshotConfig, OverlaybdConfig, SandboxBackend, SandboxExecutor, UblkDeviceManager,
};
use anyhow::{Context, Result};
use criterion::{Criterion, SamplingMode, Throughput};
use overlaybd::config::UpperMode;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::OnceCell;

static DEFAULT_ROOTFS_IMAGE_CONFIG: OnceCell<PathBuf> = OnceCell::const_new();

const FULL_SAMPLE_SIZE: usize = 10;
const FULL_WARM_UP_TIME: Duration = Duration::from_secs(3);
const FULL_MEASUREMENT_TIME: Duration = Duration::from_secs(20);
const DEFAULT_SAMPLE_COUNT: usize = 10;
const DEFAULT_CLEANUP_SETTLE_TIME: Duration = Duration::from_millis(25);
const FULL_CLEANUP_SETTLE_TIME: Duration = Duration::from_millis(500);
const CONCURRENCY: usize = 50;
// Keep the automatic batch no larger than the existing concurrent-resume benchmark.
const MAX_AUTO_THROUGHPUT_CONCURRENCY: usize = CONCURRENCY;
const BENCH_CONCURRENCY_ENV: &str = "AENV_BENCH_CONCURRENCY";
const FULL_THROUGHPUT_MEASUREMENT_TIME: Duration = Duration::from_secs(30);
const HEAVY_DATA_SIZE_MIB: u32 = 1024;
const HEAVY_MEM_SIZE_MIB: u32 = HEAVY_DATA_SIZE_MIB + 512;
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

fn full_bench_mode() -> bool {
    std::env::var_os("AENV_BENCH_FULL").is_some()
}

fn throughput_concurrency() -> Result<usize> {
    match std::env::var(BENCH_CONCURRENCY_ENV) {
        Ok(value) => {
            let concurrency = value.parse::<usize>().with_context(|| {
                format!("{BENCH_CONCURRENCY_ENV} must be a positive integer, got {value:?}")
            })?;
            anyhow::ensure!(
                concurrency > 0,
                "{BENCH_CONCURRENCY_ENV} must be greater than zero"
            );
            Ok(concurrency)
        }
        Err(std::env::VarError::NotPresent) => thread::available_parallelism()
            .context("determine available CPU parallelism for throughput benchmark")?
            .get()
            .checked_mul(2)
            .map(|concurrency| concurrency.min(MAX_AUTO_THROUGHPUT_CONCURRENCY))
            .context("default throughput concurrency overflowed"),
        Err(err) => Err(err).context(format!("read {BENCH_CONCURRENCY_ENV}")),
    }
}

fn cleanup_settle_time() -> Duration {
    if full_bench_mode() {
        FULL_CLEANUP_SETTLE_TIME
    } else {
        DEFAULT_CLEANUP_SETTLE_TIME
    }
}

fn should_run(name: &str, filters: &[String]) -> bool {
    filters.is_empty() || filters.iter().any(|filter| name.contains(filter))
}

fn filtered_benchmark_names() -> Result<Option<Vec<String>>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--list") {
        println!("snapshot_creation");
        println!("snapshot_creation_1gdisk");
        println!("snapshot_creation_1gmem");
        println!("snapshot_resume_cold");
        println!("snapshot_resume");
        println!("concurrent_resume");
        println!("concurrent_resume_throughput");
        println!("concurrent_snapshot_capture_throughput");
        println!("concurrent_pause_throughput");
        return Ok(None);
    }

    Ok(Some(
        args.into_iter()
            .filter(|arg| !arg.starts_with('-'))
            .collect(),
    ))
}

fn format_duration(duration: Duration) -> String {
    format!("{:.2} ms", duration.as_secs_f64() * 1_000.0)
}

fn print_samples(name: &str, samples: &[Duration]) {
    let total: Duration = samples.iter().copied().sum();
    let mean = total / samples.len() as u32;
    let min = samples.iter().copied().min().unwrap_or_default();
    let max = samples.iter().copied().max().unwrap_or_default();

    println!(
        "{name:<28} mean {:>10}  min {:>10}  max {:>10}  samples {}",
        format_duration(mean),
        format_duration(min),
        format_duration(max),
        samples.len()
    );

    if std::env::var_os("AENV_BENCH_PRINT_SAMPLES").is_some() {
        let samples = samples
            .iter()
            .map(|duration| format_duration(*duration))
            .collect::<Vec<_>>()
            .join(", ");
        println!("{name:<28} samples [{samples}]");
    }
}

fn print_throughput_samples(name: &str, samples: &[Duration], concurrency: usize) -> Result<()> {
    anyhow::ensure!(!samples.is_empty(), "{name} produced no samples");
    let total: Duration = samples.iter().copied().sum();
    anyhow::ensure!(!total.is_zero(), "{name} measured zero elapsed time");
    let operations = concurrency
        .checked_mul(samples.len())
        .context("throughput operation count overflowed")?;
    let throughput = operations as f64 / total.as_secs_f64();
    let mean = total / samples.len() as u32;
    let min = samples.iter().copied().min().unwrap_or_default();
    let max = samples.iter().copied().max().unwrap_or_default();

    println!(
        "{name:<40} throughput {throughput:>10.2} ops/s  mean batch {:>10}  min {:>10}  max {:>10}  samples {}  concurrency {concurrency}",
        format_duration(mean),
        format_duration(min),
        format_duration(max),
        samples.len()
    );

    if std::env::var_os("AENV_BENCH_PRINT_SAMPLES").is_some() {
        let samples = samples
            .iter()
            .map(|duration| format_duration(*duration))
            .collect::<Vec<_>>()
            .join(", ");
        println!("{name:<40} batch samples [{samples}]");
    }
    Ok(())
}

fn run_default_benchmark<F>(name: &str, filters: &[String], mut run: F) -> bool
where
    F: FnMut() -> Result<Vec<Duration>>,
{
    if !should_run(name, filters) {
        return false;
    }

    match run() {
        Ok(samples) => print_samples(name, &samples),
        Err(err) => eprintln!("Skipping {name}: {err:#}"),
    }
    true
}

fn run_default_throughput_benchmark<F>(
    name: &str,
    filters: &[String],
    concurrency: usize,
    mut run: F,
) -> Result<bool>
where
    F: FnMut() -> Result<Vec<Duration>>,
{
    if !should_run(name, filters) {
        return Ok(false);
    }
    let samples = run().with_context(|| format!("run {name}"))?;
    print_throughput_samples(name, &samples, concurrency)?;
    Ok(true)
}

async fn setup_sandbox() -> Result<FirecrackerSandbox> {
    setup_sandbox_inner(128).await
}

async fn setup_sandbox_inner(mem_size_mib: u32) -> Result<FirecrackerSandbox> {
    let app_config = agentenv::cfg::ConfigManager::init_global()?.config();
    let image_config_path = DEFAULT_ROOTFS_IMAGE_CONFIG
        .get_or_try_init(|| async {
            let image_resolver = ImageResolver::new(app_config);
            image_resolver
                .resolve(image_resolver.default_image())
                .await
                .map(|resolved| resolved.overlaybd_config_path)
        })
        .await?
        .clone();
    UblkDeviceManager::init_global_from_config(app_config)
        .await
        .expect("init global UblkDeviceManager for benchmark");

    let mut config =
        FirecrackerSandboxConfig::from_global_config_with_user_image(OverlaybdConfig {
            image_config_path,
            read_only: false,
            runtime_upper_mode: bench_upper_mode(),
        })
        .context("load sandbox config for benchmark setup")?;
    config.mem_size_mib = mem_size_mib;
    config.vcpu_count = 1;
    config.common.runtime_policy.socket_timeout = Duration::from_secs(30);

    let mut sandbox = FirecrackerSandbox::new(config)?;
    sandbox.start().await?;
    Ok(sandbox)
}

async fn write_1g_disk(sandbox: &FirecrackerSandbox) -> Result<()> {
    let count = format!("count={HEAVY_DATA_SIZE_MIB}");
    sandbox
        .executor()?
        .run_command(
            "dd",
            &[
                "if=/dev/zero",
                "of=/tmp/bench_1g",
                "bs=1M",
                &count,
                "oflag=direct",
            ],
        )
        .await?;
    sandbox.executor()?.run_command("sync", &[]).await?;
    sandbox
        .executor()?
        .run_command("sh", &["-c", "echo 3 > /proc/sys/vm/drop_caches"])
        .await?;
    Ok(())
}

async fn dirty_1g_mem(sandbox: &FirecrackerSandbox) -> Result<()> {
    let count = format!("count={HEAVY_DATA_SIZE_MIB}");
    sandbox
        .executor()?
        .run_command(
            "dd",
            &["if=/dev/zero", "of=/dev/shm/bench_1g", "bs=1M", &count],
        )
        .await?;
    Ok(())
}

fn bench_snapshot_creation_inner(
    name: &str,
    c: &mut Criterion,
    mem_size_mib: u32,
    prepare: impl Fn(&Runtime, &FirecrackerSandbox) -> Result<()>,
) {
    let rt = Runtime::new().unwrap();

    // Smoke test
    println!("Smoke-testing {name}...");
    match rt.block_on(setup_sandbox_inner(mem_size_mib)) {
        Ok(mut sandbox) => {
            if let Err(e) = prepare(&rt, &sandbox) {
                eprintln!("Skipping {name}: prepare step failed: {e:?}");
                rt.block_on(async {
                    let _ = sandbox.stop().await;
                });
                return;
            }
            rt.block_on(async {
                let _ = sandbox.stop().await;
                tokio::time::sleep(cleanup_settle_time()).await;
            });
        }
        Err(e) => {
            eprintln!("Skipping {name}: sandbox setup failed: {e:?}");
            return;
        }
    }

    c.bench_function(name, |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                let mut sandbox = rt.block_on(setup_sandbox_inner(mem_size_mib)).unwrap();
                prepare(&rt, &sandbox).unwrap();

                let start = std::time::Instant::now();
                let _snapshot = rt.block_on(async { sandbox.pause().await.unwrap() });
                total += start.elapsed();

                rt.block_on(async {
                    let _ = sandbox.stop().await;
                    tokio::time::sleep(cleanup_settle_time()).await;
                });
            }

            total
        });
    });
}

fn bench_snapshot_creation(c: &mut Criterion) {
    bench_snapshot_creation_inner("snapshot_creation", c, 128, |_, _| Ok(()));
}

fn bench_snapshot_creation_1gdisk(c: &mut Criterion) {
    bench_snapshot_creation_inner("snapshot_creation_1gdisk", c, 128, |rt, sandbox| {
        rt.block_on(write_1g_disk(sandbox))
    });
}

fn bench_snapshot_creation_1gmem(c: &mut Criterion) {
    bench_snapshot_creation_inner(
        "snapshot_creation_1gmem",
        c,
        HEAVY_MEM_SIZE_MIB,
        |rt, sandbox| rt.block_on(dirty_1g_mem(sandbox)),
    );
}

async fn prepare_snapshot() -> Result<agentenv::sandbox::FirecrackerSnapshotConfig> {
    let mut sandbox = setup_sandbox().await?;
    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;
    Ok(snapshot)
}

fn bench_snapshot_resume(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let snapshot = match rt.block_on(prepare_snapshot()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to prepare snapshot: {:?}", e);
            eprintln!("Skipping snapshot_resume benchmark due to setup failure");
            return;
        }
    };

    // Keep one instance alive so the benchmark matches the hot template path:
    // memory ublk devices are shared while at least one handle is live.
    let mut warm_sandbox = match rt
        .block_on(async { FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await })
    {
        Ok(sandbox) => sandbox,
        Err(e) => {
            eprintln!("Failed to warm snapshot resume path: {:?}", e);
            eprintln!("Skipping snapshot_resume benchmark due to setup failure");
            return;
        }
    };

    c.bench_function("snapshot_resume", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                let start = std::time::Instant::now();
                let mut sandbox = rt.block_on(async {
                    FirecrackerSandbox::resume_from_snapshot_config(&snapshot)
                        .await
                        .unwrap()
                });
                total += start.elapsed();

                rt.block_on(async {
                    let _ = sandbox.stop().await;
                    tokio::time::sleep(cleanup_settle_time()).await;
                });
            }

            total
        });
    });

    rt.block_on(async {
        let _ = warm_sandbox.stop().await;
    });
}

fn bench_snapshot_resume_cold(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let snapshot = match rt.block_on(prepare_snapshot()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to prepare snapshot: {:?}", e);
            eprintln!("Skipping snapshot_resume_cold benchmark due to setup failure");
            return;
        }
    };

    c.bench_function("snapshot_resume_cold", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                let start = std::time::Instant::now();
                let mut sandbox = rt.block_on(async {
                    FirecrackerSandbox::resume_from_snapshot_config(&snapshot)
                        .await
                        .unwrap()
                });
                total += start.elapsed();

                rt.block_on(async {
                    let _ = sandbox.stop().await;
                    tokio::time::sleep(cleanup_settle_time()).await;
                });
            }

            total
        });
    });
}

fn bench_snapshot_concurrent_resume(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let base_snapshot = match rt.block_on(prepare_snapshot()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to prepare snapshot: {:?}", e);
            eprintln!("Skipping concurrent_resume benchmark due to setup failure");
            return;
        }
    };

    // Keep the shared memory device hot while measuring concurrent resume.
    let mut warm_sandbox = match rt
        .block_on(async { FirecrackerSandbox::resume_from_snapshot_config(&base_snapshot).await })
    {
        Ok(sandbox) => sandbox,
        Err(e) => {
            eprintln!("Failed to warm concurrent resume path: {:?}", e);
            eprintln!("Skipping concurrent_resume benchmark due to setup failure");
            return;
        }
    };

    c.bench_function("concurrent_resume", |b| {
        b.iter_custom(|iters| {
            let next_request = Arc::new(AtomicU64::new(0));
            let start_barrier = Arc::new(Barrier::new(CONCURRENCY));
            let handles: Vec<_> = (0..CONCURRENCY)
                .map(|_| {
                    let snapshot = base_snapshot.clone();
                    let handle = rt.handle().clone();
                    let next_request = Arc::clone(&next_request);
                    let start_barrier = Arc::clone(&start_barrier);

                    thread::spawn(move || {
                        let mut resume_latency_total = Duration::ZERO;

                        start_barrier.wait();
                        loop {
                            let request = next_request.fetch_add(1, Ordering::Relaxed);
                            if request >= iters * CONCURRENCY as u64 {
                                break;
                            }

                            let start = std::time::Instant::now();
                            let mut sandbox = handle.block_on(async {
                                FirecrackerSandbox::resume_from_snapshot_config(&snapshot)
                                    .await
                                    .unwrap()
                            });
                            resume_latency_total += start.elapsed();

                            handle.block_on(async {
                                let _ = sandbox.stop().await;
                                tokio::time::sleep(cleanup_settle_time()).await;
                            });
                        }

                        resume_latency_total
                    })
                })
                .collect();

            let duration: Duration = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .sum();
            duration / (CONCURRENCY as u32)
        });
    });

    rt.block_on(async {
        let _ = warm_sandbox.stop().await;
    });
}

fn criterion_config() -> Criterion {
    if full_bench_mode() {
        Criterion::default()
            .sample_size(FULL_SAMPLE_SIZE)
            .warm_up_time(FULL_WARM_UP_TIME)
            .measurement_time(FULL_MEASUREMENT_TIME)
    } else {
        Criterion::default()
    }
}

fn default_snapshot_creation_inner(
    rt: &Runtime,
    mem_size_mib: u32,
    prepare: impl Fn(&Runtime, &FirecrackerSandbox) -> Result<()>,
) -> Result<Vec<Duration>> {
    let mut samples = Vec::with_capacity(DEFAULT_SAMPLE_COUNT);
    for _ in 0..DEFAULT_SAMPLE_COUNT {
        let mut sandbox = rt.block_on(setup_sandbox_inner(mem_size_mib))?;
        prepare(rt, &sandbox)?;

        let start = std::time::Instant::now();
        rt.block_on(async { sandbox.pause().await })?;
        samples.push(start.elapsed());

        rt.block_on(async {
            let _ = sandbox.stop().await;
            tokio::time::sleep(cleanup_settle_time()).await;
        });
    }
    Ok(samples)
}

fn default_snapshot_creation(rt: &Runtime) -> Result<Vec<Duration>> {
    default_snapshot_creation_inner(rt, 128, |_, _| Ok(()))
}

fn default_snapshot_creation_1gdisk(rt: &Runtime) -> Result<Vec<Duration>> {
    default_snapshot_creation_inner(rt, 128, |rt, sandbox| rt.block_on(write_1g_disk(sandbox)))
}

fn default_snapshot_creation_1gmem(rt: &Runtime) -> Result<Vec<Duration>> {
    default_snapshot_creation_inner(rt, HEAVY_MEM_SIZE_MIB, |rt, sandbox| {
        rt.block_on(dirty_1g_mem(sandbox))
    })
}

fn default_snapshot_resume_cold(rt: &Runtime) -> Result<Vec<Duration>> {
    let snapshot = rt.block_on(prepare_snapshot())?;
    let mut samples = Vec::with_capacity(DEFAULT_SAMPLE_COUNT);

    for _ in 0..DEFAULT_SAMPLE_COUNT {
        let start = std::time::Instant::now();
        let mut sandbox = rt
            .block_on(async { FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await })?;
        samples.push(start.elapsed());

        rt.block_on(async {
            let _ = sandbox.stop().await;
            tokio::time::sleep(cleanup_settle_time()).await;
        });
    }

    Ok(samples)
}

fn default_snapshot_resume(rt: &Runtime) -> Result<Vec<Duration>> {
    let snapshot = rt.block_on(prepare_snapshot())?;
    let mut warm_sandbox =
        rt.block_on(async { FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await })?;
    let mut samples = Vec::with_capacity(DEFAULT_SAMPLE_COUNT);

    for _ in 0..DEFAULT_SAMPLE_COUNT {
        let start = std::time::Instant::now();
        let mut sandbox = rt
            .block_on(async { FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await })?;
        samples.push(start.elapsed());

        rt.block_on(async {
            let _ = sandbox.stop().await;
            tokio::time::sleep(cleanup_settle_time()).await;
        });
    }

    rt.block_on(async {
        let _ = warm_sandbox.stop().await;
    });
    Ok(samples)
}

fn run_concurrent_resume_samples(
    rt: &Runtime,
    base_snapshot: &FirecrackerSnapshotConfig,
    sample_count: usize,
) -> Result<Vec<Duration>> {
    let start_barrier = Arc::new(Barrier::new(CONCURRENCY));
    let handles: Vec<_> = (0..CONCURRENCY)
        .map(|_| {
            let snapshot = base_snapshot.clone();
            let handle = rt.handle().clone();
            let start_barrier = Arc::clone(&start_barrier);
            let settle_time = cleanup_settle_time();

            thread::spawn(move || -> Result<Vec<Duration>> {
                let mut samples = Vec::with_capacity(sample_count);

                start_barrier.wait();
                for _ in 0..sample_count {
                    let start = std::time::Instant::now();
                    let mut sandbox = handle.block_on(async {
                        FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await
                    })?;
                    samples.push(start.elapsed());

                    handle.block_on(async {
                        let _ = sandbox.stop().await;
                        tokio::time::sleep(settle_time).await;
                    });
                }

                Ok(samples)
            })
        })
        .collect();

    let mut samples = vec![Duration::ZERO; sample_count];
    let mut first_error = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(worker_samples)) => {
                for (sample, duration) in samples.iter_mut().zip(worker_samples) {
                    *sample += duration;
                }
            }
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(_) => {
                first_error
                    .get_or_insert_with(|| anyhow::anyhow!("concurrent resume worker panicked"));
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }

    for sample in &mut samples {
        *sample /= CONCURRENCY as u32;
    }
    Ok(samples)
}

fn default_concurrent_resume(rt: &Runtime) -> Result<Vec<Duration>> {
    let base_snapshot = rt.block_on(prepare_snapshot())?;
    let mut warm_sandbox = rt.block_on(async {
        FirecrackerSandbox::resume_from_snapshot_config(&base_snapshot).await
    })?;

    let benchmark_result = (|| {
        // The bounded runner does not get Criterion's warm-up phase. Prime the
        // adaptive network, block-device, and Firecracker pools with the same
        // burst shape before collecting samples.
        run_concurrent_resume_samples(rt, &base_snapshot, 1)?;
        run_concurrent_resume_samples(rt, &base_snapshot, DEFAULT_SAMPLE_COUNT)
    })();

    rt.block_on(async {
        let _ = warm_sandbox.stop().await;
    });
    benchmark_result
}

// Each worker owns a separate sandbox. The timer starts when the whole batch is
// released and stops after every operation has completed; setup and cleanup are
// deliberately outside this interval.
fn measure_concurrent_batch<F, T>(work: Vec<F>) -> (Duration, Vec<thread::Result<T>>)
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let start_barrier = Arc::new(Barrier::new(work.len() + 1));
    let handles: Vec<_> = work
        .into_iter()
        .map(|run| {
            let start_barrier = Arc::clone(&start_barrier);
            thread::spawn(move || {
                start_barrier.wait();
                run()
            })
        })
        .collect();

    let start = Instant::now();
    start_barrier.wait();
    let results = handles.into_iter().map(|handle| handle.join()).collect();
    (start.elapsed(), results)
}

fn stop_batch(rt: &Runtime, sandboxes: Vec<FirecrackerSandbox>) -> Result<()> {
    if sandboxes.is_empty() {
        return Ok(());
    }
    let mut first_error = None;
    let mut failed = Vec::new();
    for mut sandbox in sandboxes {
        if let Err(error) = rt.block_on(sandbox.stop()) {
            first_error.get_or_insert(error);
            failed.push(sandbox);
        }
    }
    // A failed stop retains its device handles; retry before dropping the sandbox.
    if !failed.is_empty() {
        rt.block_on(async { tokio::time::sleep(cleanup_settle_time()).await });
        for mut sandbox in failed {
            if let Err(error) = rt.block_on(sandbox.stop()) {
                eprintln!("Retrying benchmark sandbox cleanup failed: {error:#}");
            }
        }
    }
    rt.block_on(async { tokio::time::sleep(cleanup_settle_time()).await });
    if let Some(error) = first_error {
        return Err(error).context("stop benchmark sandboxes");
    }
    Ok(())
}

fn prepare_running_batch(rt: &Runtime, concurrency: usize) -> Result<Vec<FirecrackerSandbox>> {
    let mut sandboxes = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        match rt.block_on(setup_sandbox()) {
            Ok(sandbox) => sandboxes.push(sandbox),
            Err(error) => {
                if let Err(cleanup_error) = stop_batch(rt, sandboxes) {
                    return Err(error).context(format!(
                        "prepare running sandbox batch; cleanup also failed: {cleanup_error:#}"
                    ));
                }
                return Err(error).context("prepare running sandbox batch");
            }
        }
    }
    Ok(sandboxes)
}

fn capture_running_sandbox(
    handle: &Handle,
    sandbox: &mut FirecrackerSandbox,
) -> Result<CapturedSandboxSnapshot> {
    Ok(handle.block_on(SandboxBackend::snapshot(sandbox))?)
}

fn pause_running_sandbox(
    handle: &Handle,
    sandbox: &mut FirecrackerSandbox,
) -> Result<FirecrackerSnapshotConfig> {
    handle.block_on(sandbox.pause())
}

fn run_running_batch<T, F>(rt: &Runtime, concurrency: usize, operation: F) -> Result<Duration>
where
    T: Send + 'static,
    F: Fn(&Handle, &mut FirecrackerSandbox) -> Result<T> + Copy + Send + 'static,
{
    let sandboxes = prepare_running_batch(rt, concurrency)?;
    let work = sandboxes
        .into_iter()
        .map(|mut sandbox| {
            let handle = rt.handle().clone();
            move || {
                let result = catch_unwind(AssertUnwindSafe(|| operation(&handle, &mut sandbox)));
                (sandbox, result)
            }
        })
        .collect();
    let (elapsed, results) = measure_concurrent_batch(work);

    let mut sandboxes = Vec::with_capacity(concurrency);
    let mut artifacts = Vec::with_capacity(concurrency);
    let mut first_error = None;
    for result in results {
        match result {
            Ok((sandbox, Ok(Ok(artifact)))) => {
                sandboxes.push(sandbox);
                artifacts.push(artifact);
            }
            Ok((sandbox, Ok(Err(error)))) => {
                sandboxes.push(sandbox);
                first_error.get_or_insert(error);
            }
            Ok((sandbox, Err(_))) => {
                sandboxes.push(sandbox);
                first_error.get_or_insert_with(|| anyhow::anyhow!("benchmark worker panicked"));
            }
            Err(_) => {
                first_error.get_or_insert_with(|| anyhow::anyhow!("benchmark worker panicked"));
            }
        }
    }
    let cleanup = stop_batch(rt, sandboxes);
    drop(artifacts);
    if let Some(error) = first_error {
        if let Err(cleanup_error) = cleanup {
            return Err(error).context(format!(
                "running sandbox batch failed; cleanup also failed: {cleanup_error:#}"
            ));
        }
        return Err(error).context("running sandbox batch failed");
    }
    cleanup?;
    Ok(elapsed)
}

fn run_resume_batch(
    rt: &Runtime,
    snapshot: &FirecrackerSnapshotConfig,
    concurrency: usize,
) -> Result<Duration> {
    let work = (0..concurrency)
        .map(|_| {
            let snapshot = snapshot.clone();
            let handle = rt.handle().clone();
            move || handle.block_on(FirecrackerSandbox::resume_from_snapshot_config(&snapshot))
        })
        .collect();
    let (elapsed, results) = measure_concurrent_batch(work);

    let mut sandboxes = Vec::with_capacity(concurrency);
    let mut first_error = None;
    for result in results {
        match result {
            Ok(Ok(sandbox)) => sandboxes.push(sandbox),
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(_) => {
                first_error.get_or_insert_with(|| anyhow::anyhow!("resume worker panicked"));
            }
        }
    }
    let cleanup = stop_batch(rt, sandboxes);
    if let Some(error) = first_error {
        if let Err(cleanup_error) = cleanup {
            return Err(error).context(format!(
                "concurrent resume batch failed; cleanup also failed: {cleanup_error:#}"
            ));
        }
        return Err(error).context("concurrent resume batch failed");
    }
    cleanup?;
    Ok(elapsed)
}

fn default_throughput_samples(
    mut run_batch: impl FnMut() -> Result<Duration>,
) -> Result<Vec<Duration>> {
    run_batch().context("warm up throughput benchmark")?;
    (0..DEFAULT_SAMPLE_COUNT).map(|_| run_batch()).collect()
}

fn default_resume_throughput(rt: &Runtime, concurrency: usize) -> Result<Vec<Duration>> {
    let snapshot = rt.block_on(prepare_snapshot())?;
    let mut warm_sandbox =
        rt.block_on(FirecrackerSandbox::resume_from_snapshot_config(&snapshot))?;
    let samples = default_throughput_samples(|| run_resume_batch(rt, &snapshot, concurrency));
    rt.block_on(warm_sandbox.stop())?;
    samples
}

fn bench_snapshot_throughput(c: &mut Criterion) {
    let concurrency = throughput_concurrency().expect("valid throughput concurrency");
    let rt = Runtime::new().expect("create Tokio runtime for throughput benchmarks");
    let mut group = c.benchmark_group("snapshot_throughput");
    group.throughput(Throughput::Elements(concurrency as u64));
    group.sample_size(FULL_SAMPLE_SIZE);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(FULL_THROUGHPUT_MEASUREMENT_TIME);

    let snapshot = rt
        .block_on(prepare_snapshot())
        .expect("prepare resume snapshot");
    let mut warm_sandbox = rt
        .block_on(FirecrackerSandbox::resume_from_snapshot_config(&snapshot))
        .expect("warm resume snapshot");
    let resume_result = catch_unwind(AssertUnwindSafe(|| {
        group.bench_function("concurrent_resume_throughput", |b| {
            b.iter_custom(|iters| {
                (0..iters)
                    .map(|_| run_resume_batch(&rt, &snapshot, concurrency).expect("resume batch"))
                    .sum()
            });
        });
    }));
    rt.block_on(warm_sandbox.stop()).expect("stop warm sandbox");
    if let Err(panic) = resume_result {
        resume_unwind(panic);
    }

    group.bench_function("concurrent_snapshot_capture_throughput", |b| {
        b.iter_custom(|iters| {
            (0..iters)
                .map(|_| {
                    run_running_batch(&rt, concurrency, capture_running_sandbox)
                        .expect("snapshot capture batch")
                })
                .sum()
        });
    });
    group.bench_function("concurrent_pause_throughput", |b| {
        b.iter_custom(|iters| {
            (0..iters)
                .map(|_| {
                    run_running_batch(&rt, concurrency, pause_running_sandbox).expect("pause batch")
                })
                .sum()
        });
    });
    group.finish();
}

fn run_default_snapshot_benchmarks() -> Result<()> {
    let Some(filters) = filtered_benchmark_names()? else {
        return Ok(());
    };

    println!("Running bounded snapshot benchmarks (set AENV_BENCH_FULL=1 for Criterion sampling)");
    let rt = Runtime::new().context("create Tokio runtime for snapshot benchmarks")?;

    let mut ran = false;
    ran |= run_default_benchmark("snapshot_creation", &filters, || {
        default_snapshot_creation(&rt)
    });
    ran |= run_default_benchmark("snapshot_creation_1gdisk", &filters, || {
        default_snapshot_creation_1gdisk(&rt)
    });
    ran |= run_default_benchmark("snapshot_creation_1gmem", &filters, || {
        default_snapshot_creation_1gmem(&rt)
    });
    ran |= run_default_benchmark("snapshot_resume_cold", &filters, || {
        default_snapshot_resume_cold(&rt)
    });
    ran |= run_default_benchmark("snapshot_resume", &filters, || default_snapshot_resume(&rt));
    ran |= run_default_benchmark("concurrent_resume", &filters, || {
        default_concurrent_resume(&rt)
    });
    let throughput_names = [
        "concurrent_resume_throughput",
        "concurrent_snapshot_capture_throughput",
        "concurrent_pause_throughput",
    ];
    if throughput_names
        .iter()
        .any(|name| should_run(name, &filters))
    {
        let concurrency = throughput_concurrency()?;
        ran |= run_default_throughput_benchmark(
            "concurrent_resume_throughput",
            &filters,
            concurrency,
            || default_resume_throughput(&rt, concurrency),
        )?;
        ran |= run_default_throughput_benchmark(
            "concurrent_snapshot_capture_throughput",
            &filters,
            concurrency,
            || {
                default_throughput_samples(|| {
                    run_running_batch(&rt, concurrency, capture_running_sandbox)
                })
            },
        )?;
        ran |= run_default_throughput_benchmark(
            "concurrent_pause_throughput",
            &filters,
            concurrency,
            || {
                default_throughput_samples(|| {
                    run_running_batch(&rt, concurrency, pause_running_sandbox)
                })
            },
        )?;
    }

    if !ran {
        eprintln!(
            "No snapshot benchmarks matched filter(s): {}",
            filters.join(", ")
        );
    }

    Ok(())
}

fn run_full_snapshot_benchmarks() {
    let mut criterion = criterion_config().configure_from_args();
    bench_snapshot_creation(&mut criterion);
    bench_snapshot_creation_1gdisk(&mut criterion);
    bench_snapshot_creation_1gmem(&mut criterion);
    bench_snapshot_resume_cold(&mut criterion);
    bench_snapshot_resume(&mut criterion);
    bench_snapshot_concurrent_resume(&mut criterion);
    bench_snapshot_throughput(&mut criterion);
    criterion.final_summary();
}

fn main() {
    if full_bench_mode() {
        run_full_snapshot_benchmarks();
    } else if let Err(err) = run_default_snapshot_benchmarks() {
        eprintln!("snapshot benchmark failed: {err:#}");
        std::process::exit(1);
    }
}
