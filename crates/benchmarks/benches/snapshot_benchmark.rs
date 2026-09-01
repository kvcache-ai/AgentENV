use agentenv::cfg::ConfigManager;
use agentenv::image::ImageResolver;
use agentenv::sandbox::{
    FirecrackerSandbox, FirecrackerSandboxConfig, FirecrackerSnapshotConfig, GuestMemoryRange,
    GuestMemoryWorkingSet, GuestMemoryWorkingSetLimits, OverlaybdConfig, PrefaultCompletionStats,
    SandboxExecutor, SnapshotPrefaultCandidate, UblkDeviceManager,
};
use anyhow::{Context, Result};
use criterion::Criterion;
use overlaybd::config::UpperMode;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::OnceCell;

static DEFAULT_ROOTFS_IMAGE_CONFIG: OnceCell<PathBuf> = OnceCell::const_new();

const FULL_SAMPLE_SIZE: usize = 10;
const FULL_WARM_UP_TIME: Duration = Duration::from_secs(3);
const FULL_MEASUREMENT_TIME: Duration = Duration::from_secs(20);
const DEFAULT_SAMPLE_COUNT: usize = 10;
const DEFAULT_CLEANUP_SETTLE_TIME: Duration = Duration::from_millis(25);
const FULL_CLEANUP_SETTLE_TIME: Duration = Duration::from_millis(500);
const CONCURRENCY: usize = 50;
const PREFAULT_MULTI_VCPU_COUNTS: [u32; 3] = [2, 4, 8];
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

#[derive(Clone, Debug, Default)]
struct BenchmarkCliOptions {
    max_prefault_bytes: Option<u64>,
    prefault_vcpu_count: Option<u32>,
    firecracker_binary: Option<PathBuf>,
}

fn benchmark_cli_options() -> Result<BenchmarkCliOptions> {
    let mut options = BenchmarkCliOptions::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--max-prefault-bytes" => {
                anyhow::ensure!(
                    options.max_prefault_bytes.is_none(),
                    "--max-prefault-bytes may be specified only once"
                );
                let value = args
                    .next()
                    .context("--max-prefault-bytes requires a positive integer value")?
                    .parse::<u64>()
                    .context("parse --max-prefault-bytes")?;
                anyhow::ensure!(value > 0, "--max-prefault-bytes must be positive");
                options.max_prefault_bytes = Some(value);
            }
            "--firecracker-binary" => {
                anyhow::ensure!(
                    options.firecracker_binary.is_none(),
                    "--firecracker-binary may be specified only once"
                );
                let path = PathBuf::from(
                    args.next()
                        .context("--firecracker-binary requires an executable path")?,
                );
                anyhow::ensure!(
                    path.is_file(),
                    "--firecracker-binary does not name a file: {}",
                    path.display()
                );
                options.firecracker_binary = Some(path);
            }
            "--prefault-vcpus" => {
                anyhow::ensure!(
                    options.prefault_vcpu_count.is_none(),
                    "--prefault-vcpus may be specified only once"
                );
                let value = args
                    .next()
                    .context("--prefault-vcpus requires one of 1, 2, 4, or 8")?
                    .parse::<u32>()
                    .context("parse --prefault-vcpus")?;
                anyhow::ensure!(
                    matches!(value, 1 | 2 | 4 | 8),
                    "--prefault-vcpus must be one of 1, 2, 4, or 8"
                );
                options.prefault_vcpu_count = Some(value);
            }
            _ => {}
        }
    }
    Ok(options)
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
        println!("snapshot_mincore_stages");
        println!("snapshot_prefault_e2e");
        println!("snapshot_prefault_phase_e2e");
        println!("snapshot_prefault_multivcpu_e2e");
        println!("snapshot_prefault_size_sanity");
        println!("snapshot_prefault_workload");
        println!("snapshot_prefault_fixed512");
        println!("snapshot_prefault_fixed512_sanity");
        println!("snapshot_prefault_fixed512_scaling");
        return Ok(None);
    }

    let mut filters = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if matches!(
            arg.as_str(),
            "--max-prefault-bytes" | "--prefault-vcpus" | "--firecracker-binary"
        ) {
            args.next().context("benchmark option requires a value")?;
        } else if !arg.starts_with('-') {
            filters.push(arg);
        }
    }
    Ok(Some(filters))
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

async fn setup_sandbox() -> Result<FirecrackerSandbox> {
    setup_sandbox_inner(128).await
}

async fn setup_sandbox_with_vcpu(vcpu_count: u32) -> Result<FirecrackerSandbox> {
    anyhow::ensure!(vcpu_count > 0, "benchmark vCPU count must be positive");
    setup_sandbox_inner_with_vcpu(128, vcpu_count).await
}

async fn setup_sandbox_inner(mem_size_mib: u32) -> Result<FirecrackerSandbox> {
    setup_sandbox_inner_with_vcpu(mem_size_mib, 1).await
}

async fn setup_sandbox_inner_with_vcpu(
    mem_size_mib: u32,
    vcpu_count: u32,
) -> Result<FirecrackerSandbox> {
    anyhow::ensure!(vcpu_count > 0, "benchmark vCPU count must be positive");
    let app_config = agentenv::cfg::ConfigManager::init_global()?.config();
    let image_config_path = if let Some(path) = std::env::var_os("AENV_BENCH_IMAGE_CONFIG") {
        let path = PathBuf::from(path);
        anyhow::ensure!(
            path.is_file(),
            "AENV_BENCH_IMAGE_CONFIG does not name a readable image config: {}",
            path.display()
        );
        path
    } else {
        DEFAULT_ROOTFS_IMAGE_CONFIG
            .get_or_try_init(|| async {
                let image_resolver = ImageResolver::new(app_config);
                image_resolver
                    .resolve(image_resolver.default_image())
                    .await
                    .map(|resolved| resolved.overlaybd_config_path)
            })
            .await?
            .clone()
    };
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
    config.vcpu_count = vcpu_count;
    if let Some(firecracker_binary) = benchmark_cli_options()?.firecracker_binary {
        config.common.firecracker_binary = firecracker_binary;
    }
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

async fn guest_vcpu_count(sandbox: &FirecrackerSandbox) -> Result<u32> {
    let output = sandbox
        .executor()?
        .run_command("nproc", &[])
        .await
        .context("read guest vCPU count")?;
    anyhow::ensure!(
        output.exit_code == 0,
        "guest nproc failed with exit code {}: {}",
        output.exit_code,
        output.stderr
    );
    output
        .stdout
        .trim()
        .parse::<u32>()
        .context("parse guest nproc output")
}

async fn prepare_snapshot_with_vcpu(
    vcpu_count: u32,
) -> Result<agentenv::sandbox::FirecrackerSnapshotConfig> {
    let mut sandbox = setup_sandbox_with_vcpu(vcpu_count).await?;
    let reported_vcpu_count = guest_vcpu_count(&sandbox).await?;
    anyhow::ensure!(
        reported_vcpu_count == vcpu_count,
        "guest reports {reported_vcpu_count} vCPUs; benchmark requested {vcpu_count}"
    );
    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;
    Ok(snapshot)
}

async fn run_mincore_stage_workload(sandbox: &FirecrackerSandbox) -> Result<()> {
    let output = sandbox
        .executor()?
        .run_command("sh", &["-lc", "sha256sum /etc/os-release >/dev/null"])
        .await
        .context("run mincore diagnostic workload")?;
    anyhow::ensure!(
        output.exit_code == 0,
        "mincore diagnostic workload failed with exit code {}: {}",
        output.exit_code,
        output.stderr
    );
    Ok(())
}

fn run_mincore_stage_diagnostic(rt: &Runtime) -> Result<()> {
    let snapshot = rt.block_on(prepare_snapshot())?;
    let stages = rt.block_on(FirecrackerSandbox::profile_snapshot_mincore_stages(
        &snapshot,
        |sandbox| Box::pin(run_mincore_stage_workload(sandbox)),
    ))?;
    for stage in stages {
        println!(
            "mincore_stage phase={} total_ranges={} total_bytes={} delta_ranges={} delta_bytes={}",
            stage.phase,
            stage.total_ranges,
            stage.total_bytes,
            stage.newly_resident_ranges,
            stage.newly_resident_bytes,
        );
    }
    Ok(())
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

const PREFAULT_ARM_ORDER: [bool; 4] = [false, true, true, false];
const PREFAULT_WORKLOAD_SETUP: &str = "dd if=/dev/zero of=/tmp/aenv-prefault-workload.bin bs=1M count=8 conv=fsync status=none && sync";
const PREFAULT_WORKLOAD_REQUEST: &str = "sha256sum /tmp/aenv-prefault-workload.bin >/dev/null";

#[derive(Clone, Debug)]
struct PrefaultMeasurement {
    restore_setup: Duration,
    snapshot_load: Duration,
    prefault_stats: Option<PrefaultCompletionStats>,
    prefault: Duration,
    firecracker_resume: Duration,
    envd_ready: Duration,
    guest_command: Duration,
    total: Duration,
}

fn prefault_working_set_limits() -> GuestMemoryWorkingSetLimits {
    let profiling = &ConfigManager::global_config().template_profiling;
    GuestMemoryWorkingSetLimits {
        max_bytes: profiling.max_prefault_bytes,
        max_ranges: profiling.max_range_count,
        max_guest_memory_ratio_percent: profiling.max_guest_memory_ratio_percent,
    }
}

fn attach_profiled_working_set(
    mut snapshot: FirecrackerSnapshotConfig,
    working_set: agentenv::sandbox::GuestMemoryWorkingSet,
) -> Result<(FirecrackerSnapshotConfig, usize, u64)> {
    let range_count = working_set.ranges.len();
    let byte_count = working_set
        .total_bytes()
        .context("sum benchmark working-set bytes")?;
    anyhow::ensure!(
        range_count > 0 && byte_count > 0,
        "profiled benchmark snapshot has an empty working set"
    );
    snapshot.restore_working_set = Some(working_set);
    Ok((snapshot, range_count, byte_count))
}

async fn prepare_profiled_snapshot() -> Result<(FirecrackerSnapshotConfig, usize, u64)> {
    let snapshot = prepare_snapshot().await?;
    let working_set =
        FirecrackerSandbox::profile_snapshot_working_set(&snapshot, prefault_working_set_limits())
            .await
            .context("profile benchmark snapshot working set")?;
    attach_profiled_working_set(snapshot, working_set)
}

async fn run_guest_shell(
    sandbox: &FirecrackerSandbox,
    command: &str,
    description: &str,
) -> Result<()> {
    let output = sandbox
        .executor()?
        .run_command("sh", &["-lc", command])
        .await
        .with_context(|| format!("run {description}"))?;
    anyhow::ensure!(
        output.exit_code == 0,
        "{description} failed with exit code {}: {}",
        output.exit_code,
        output.stderr
    );
    Ok(())
}

async fn prepare_profiled_workload_snapshot() -> Result<(FirecrackerSnapshotConfig, usize, u64)> {
    let mut sandbox = setup_sandbox().await?;
    run_guest_shell(
        &sandbox,
        PREFAULT_WORKLOAD_SETUP,
        "prefault workload fixture setup",
    )
    .await?;
    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    let working_set = FirecrackerSandbox::profile_snapshot_working_set_with_workload(
        &snapshot,
        prefault_working_set_limits(),
        |sandbox| {
            Box::pin(run_guest_shell(
                sandbox,
                PREFAULT_WORKLOAD_REQUEST,
                "profile workload",
            ))
        },
    )
    .await
    .context("profile workload benchmark snapshot working set")?;
    attach_profiled_working_set(snapshot, working_set)
}

async fn resume_and_measure(
    snapshot: &FirecrackerSnapshotConfig,
    prefault_enabled: bool,
    command: &str,
    description: &str,
    expected_vcpu_count: Option<u32>,
) -> Result<PrefaultMeasurement> {
    let total_started = Instant::now();
    let (mut sandbox, timings) =
        FirecrackerSandbox::resume_from_snapshot_config_with_prefault_and_timings(
            snapshot,
            prefault_enabled,
        )
        .await
        .context("resume snapshot")?;

    let prefault_stats_result: Result<Option<PrefaultCompletionStats>> = if prefault_enabled {
        (|| {
            let stats = timings
                .prefault_stats
                .clone()
                .context("benchmark requires Firecracker pre-fault completion stats")?;
            let expected = snapshot
                .restore_working_set
                .as_ref()
                .context("enabled pre-fault benchmark snapshot lacks working-set metadata")?;
            let expected_bytes = expected.total_bytes()?;
            anyhow::ensure!(
                stats.requested_bytes == expected_bytes,
                "benchmark pre-fault requested {} bytes; expected {expected_bytes}",
                stats.requested_bytes
            );
            anyhow::ensure!(
                stats.completed_bytes == stats.requested_bytes,
                "benchmark rejects incomplete pre-fault: requested={}, completed={}",
                stats.requested_bytes,
                stats.completed_bytes
            );
            anyhow::ensure!(
                stats.remaining_bytes == 0,
                "benchmark rejects pre-fault with {} remaining bytes",
                stats.remaining_bytes
            );
            Ok(Some(stats))
        })()
    } else {
        Ok(None)
    };
    let prefault_stats = match prefault_stats_result {
        Ok(stats) => stats,
        Err(error) => {
            sandbox
                .stop()
                .await
                .context("stop rejected benchmark sample")?;
            return Err(error);
        }
    };

    let guest_command_started = Instant::now();
    let command_result = run_guest_shell(&sandbox, command, description).await;
    let guest_command = guest_command_started.elapsed();
    let total = total_started.elapsed();
    let vcpu_result: Result<()> = match expected_vcpu_count {
        Some(expected) => {
            let reported = guest_vcpu_count(&sandbox).await?;
            anyhow::ensure!(
                reported == expected,
                "restored guest reports {reported} vCPUs; expected {expected}"
            );
            Ok(())
        }
        None => Ok(()),
    };
    let stop_result = sandbox.stop().await;

    command_result?;
    vcpu_result?;
    stop_result.context("stop measured sandbox")?;
    Ok(PrefaultMeasurement {
        restore_setup: timings.restore_setup,
        snapshot_load: timings.snapshot_load,
        prefault: timings.prefault,
        prefault_stats,
        firecracker_resume: timings.firecracker_resume,
        envd_ready: timings.envd_ready,
        guest_command,
        total,
    })
}

async fn prefault_measurement_samples(
    snapshot: &FirecrackerSnapshotConfig,
    hot: bool,
    prefault_enabled: bool,
    command: &str,
    description: &str,
    expected_vcpu_count: Option<u32>,
) -> Result<Vec<PrefaultMeasurement>> {
    let mut warm_sandbox = if hot {
        let sandbox = FirecrackerSandbox::resume_from_snapshot_config_with_prefault(
            snapshot,
            prefault_enabled,
        )
        .await
        .context("start hot-path holder")?;
        run_guest_shell(&sandbox, "true", "hot-path holder readiness").await?;
        Some(sandbox)
    } else {
        None
    };

    let result = async {
        let mut samples = Vec::with_capacity(DEFAULT_SAMPLE_COUNT);
        for _ in 0..DEFAULT_SAMPLE_COUNT {
            samples.push(
                resume_and_measure(
                    snapshot,
                    prefault_enabled,
                    command,
                    description,
                    expected_vcpu_count,
                )
                .await?,
            );
            tokio::time::sleep(cleanup_settle_time()).await;
        }
        Ok(samples)
    }
    .await;

    if let Some(mut sandbox) = warm_sandbox.take() {
        sandbox.stop().await.context("stop hot-path holder")?;
    }
    result
}

fn print_prefault_measurements(name: &str, samples: &[PrefaultMeasurement]) {
    for (index, sample) in samples.iter().enumerate() {
        println!(
            "{name}_sample sample={} total_us={} restore_setup_us={} snapshot_load_us={} prefault_us={} firecracker_resume_us={} envd_ready_us={} guest_command_us={}",
            index + 1,
            sample.total.as_micros(),
            sample.restore_setup.as_micros(),
            sample.snapshot_load.as_micros(),
            sample.prefault.as_micros(),
            sample.firecracker_resume.as_micros(),
            sample.envd_ready.as_micros(),
            sample.guest_command.as_micros(),
        );

        if let Some(stats) = &sample.prefault_stats {
            println!(
                "{name}_prefault_stats sample={} requested_bytes={} completed_bytes={} remaining_bytes={} range_count={} ioctl_count={} wall_time_us={} workers={:?}",
                index + 1,
                stats.requested_bytes,
                stats.completed_bytes,
                stats.remaining_bytes,
                stats.range_count,
                stats.ioctl_count,
                stats.wall_time_us,
                stats.workers
            );
        }
    }

    print_samples(
        name,
        &samples
            .iter()
            .map(|sample| sample.total)
            .collect::<Vec<_>>(),
    );
    print_samples(
        &format!("{name}_restore_setup"),
        &samples
            .iter()
            .map(|sample| sample.restore_setup)
            .collect::<Vec<_>>(),
    );
    print_samples(
        &format!("{name}_snapshot_load"),
        &samples
            .iter()
            .map(|sample| sample.snapshot_load)
            .collect::<Vec<_>>(),
    );
    print_samples(
        &format!("{name}_prefault"),
        &samples
            .iter()
            .map(|sample| sample.prefault)
            .collect::<Vec<_>>(),
    );
    print_samples(
        &format!("{name}_firecracker_resume"),
        &samples
            .iter()
            .map(|sample| sample.firecracker_resume)
            .collect::<Vec<_>>(),
    );
    print_samples(
        &format!("{name}_envd_ready"),
        &samples
            .iter()
            .map(|sample| sample.envd_ready)
            .collect::<Vec<_>>(),
    );
    print_samples(
        &format!("{name}_guest_command"),
        &samples
            .iter()
            .map(|sample| sample.guest_command)
            .collect::<Vec<_>>(),
    );
}

fn run_prefault_e2e_benchmark(rt: &Runtime) -> Result<()> {
    let (snapshot, range_count, byte_count) = rt.block_on(prepare_profiled_snapshot())?;
    println!(
        "prefault_e2e invariant: one profiled snapshot; working-set ranges {range_count}, bytes {byte_count}; only the in-process pre-fault boolean differs between arms"
    );

    for (mode, hot) in [("resource_cold", false), ("hot", true)] {
        for (run, enabled) in PREFAULT_ARM_ORDER.into_iter().enumerate() {
            let samples = rt.block_on(prefault_measurement_samples(
                &snapshot,
                hot,
                enabled,
                "true",
                "guest readiness command",
                None,
            ))?;
            let arm = if enabled { "enabled" } else { "disabled" };
            print_prefault_measurements(
                &format!("prefault_e2e_{mode}_{arm}_run{}", run + 1),
                &samples,
            );
        }
    }
    Ok(())
}

#[derive(Clone)]
struct PrefaultPhaseArm {
    name: &'static str,
    snapshot: FirecrackerSnapshotConfig,
    prefault_enabled: bool,
    range_count: usize,
    byte_count: u64,
}

fn prefault_phase_arm(
    snapshot: &FirecrackerSnapshotConfig,
    candidate: SnapshotPrefaultCandidate,
) -> Result<PrefaultPhaseArm> {
    let range_count = candidate.working_set.ranges.len();
    let byte_count = candidate
        .working_set
        .total_bytes()
        .context("sum phase pre-fault candidate bytes")?;
    anyhow::ensure!(
        range_count > 0 && byte_count > 0,
        "empty {} pre-fault candidate",
        candidate.phase
    );
    let mut snapshot = snapshot.clone();
    snapshot.restore_working_set = Some(candidate.working_set);
    Ok(PrefaultPhaseArm {
        name: candidate.phase,
        snapshot,
        prefault_enabled: true,
        range_count,
        byte_count,
    })
}

async fn prepare_prefault_phase_arms() -> Result<Vec<PrefaultPhaseArm>> {
    let snapshot = prepare_snapshot().await?;
    let mut arms = vec![PrefaultPhaseArm {
        name: "none",
        snapshot: snapshot.clone(),
        prefault_enabled: false,
        range_count: 0,
        byte_count: 0,
    }];
    for candidate in FirecrackerSandbox::profile_snapshot_prefault_candidates(
        &snapshot,
        prefault_working_set_limits(),
    )
    .await
    .context("profile phase pre-fault candidates")?
    {
        arms.push(prefault_phase_arm(&snapshot, candidate)?);
    }
    Ok(arms)
}

fn run_prefault_phase_e2e_benchmark(rt: &Runtime) -> Result<()> {
    let arms = rt.block_on(prepare_prefault_phase_arms())?;
    anyhow::ensure!(
        arms.len() == 4,
        "expected no-prefault plus three phase candidates"
    );
    for arm in &arms {
        println!(
            "prefault_phase candidate={} ranges={} bytes={}",
            arm.name, arm.range_count, arm.byte_count
        );
    }
    let order = [3usize, 2, 1, 0, 0, 1, 2, 3];
    for (run, index) in order.into_iter().enumerate() {
        let arm = &arms[index];
        let samples = rt.block_on(prefault_measurement_samples(
            &arm.snapshot,
            false,
            arm.prefault_enabled,
            "true",
            "guest readiness command",
            None,
        ))?;
        print_prefault_measurements(
            &format!("prefault_phase_cold_{}_run{}", arm.name, run + 1),
            &samples,
        );
    }
    Ok(())
}

async fn prepare_prefault_ready_arms(vcpu_count: u32) -> Result<Vec<PrefaultPhaseArm>> {
    let snapshot = prepare_snapshot_with_vcpu(vcpu_count).await?;
    let working_set =
        FirecrackerSandbox::profile_snapshot_working_set(&snapshot, prefault_working_set_limits())
            .await
            .context("profile envd-ready pre-fault working set")?;
    let (prefault_snapshot, range_count, byte_count) =
        attach_profiled_working_set(snapshot.clone(), working_set)?;
    Ok(vec![
        PrefaultPhaseArm {
            name: "none",
            snapshot,
            prefault_enabled: false,
            range_count: 0,
            byte_count: 0,
        },
        PrefaultPhaseArm {
            name: "envd_ready",
            snapshot: prefault_snapshot,
            prefault_enabled: true,
            range_count,
            byte_count,
        },
    ])
}

fn run_prefault_multivcpu_e2e_benchmark(rt: &Runtime) -> Result<()> {
    for vcpu_count in PREFAULT_MULTI_VCPU_COUNTS {
        let arms = rt.block_on(prepare_prefault_ready_arms(vcpu_count))?;
        anyhow::ensure!(
            arms.len() == 2,
            "expected no-prefault and envd-ready arms for {vcpu_count} vCPUs"
        );
        for arm in &arms {
            println!(
                "prefault_multivcpu vcpu={} candidate={} ranges={} bytes={}",
                vcpu_count, arm.name, arm.range_count, arm.byte_count
            );
        }
        for (run, index) in [1usize, 0, 0, 1].into_iter().enumerate() {
            let arm = &arms[index];
            let samples = rt.block_on(prefault_measurement_samples(
                &arm.snapshot,
                false,
                arm.prefault_enabled,
                "true",
                "guest readiness command",
                Some(vcpu_count),
            ))?;
            print_prefault_measurements(
                &format!(
                    "prefault_multivcpu_cold_vcpu{}_{}_run{}",
                    vcpu_count,
                    arm.name,
                    run + 1
                ),
                &samples,
            );
        }
    }
    Ok(())
}
const FIXED_WORKING_SET_MIB: u32 = 512;
const FIXED_WORKING_SET_MEM_MIB: u32 = 1024;
const FIXED_PREFAULT_GPA_START: u64 = 128 * 1024 * 1024;
const FIXED_WORKING_SET_SAMPLES: usize = 2;
const FIXED_WORKING_SET_HELPER: &str = r#"nohup perl -e 'use Fcntl qw(SEEK_SET);
my ($P,$N)=(4096,512*1024*1024);
my $addr=syscall(9,0,$N,3,0x22,-1,0); die "mmap failed: $!
" if $addr == -1;
open my $mem,"+<","/proc/self/mem" or die "open mem: $!
";
for (my $i=0;$i<$N;$i+=$P) { sysseek($mem,$addr+$i,SEEK_SET) or die "seek mem: $!
"; syswrite($mem,chr(($i/$P)%251+1)) == 1 or die "write mem: $!
"; }
open my $pm,"<:raw","/proc/self/pagemap" or die "open pagemap: $!
";
open my $out,">","/tmp/aenv-fixed512-gpas" or die "open gpas: $!
";
my ($start,$prev);
for (my $i=0;$i<$N;$i+=$P) {
 sysseek($pm,(($addr+$i)/$P)*8,SEEK_SET) or die "seek pagemap: $!
";
 sysread($pm,my $b,8)==8 or die "read pagemap: $!
";
 my $v=unpack("Q<",$b); my $p=$v & ((1<<55)-1);
 die "guest pagemap PFN unavailable
" if !($v & (1<<63)) || !$p;
 if (!defined $start) { $start=$prev=$p; next; }
 if ($p==$prev+1) { $prev=$p; next; }
 print $out ($start*$P)." ".(($prev-$start+1)*$P)."
"; $start=$prev=$p;
}
print $out ($start*$P)." ".(($prev-$start+1)*$P)."
"; close $out;
open my $ready,">","/tmp/aenv-fixed512-ready" or die "ready: $!
"; close $ready;
select(undef,undef,undef,0.005) while !-e "/tmp/aenv-fixed512-go";
open my $workers,"<","/tmp/aenv-fixed512-workers" or die "workers: $!
"; my $n=<$workers>; chomp $n;
for my $w (0..$n-1) { my $pid=fork(); die "fork: $!
" if !defined $pid; if (!$pid) { open my $r,"<","/proc/self/mem" or die "read mem: $!
"; my $sum=0; for (my $i=$w*$P;$i<$N;$i+=$n*$P) { sysseek($r,$addr+$i,SEEK_SET) or die "scan seek: $!
"; sysread($r,my $b,1)==1 or die "scan read: $!
"; $sum+=ord($b); } exit($sum&255); } }
for (1..$n) { wait(); }
open my $result,">","/tmp/aenv-fixed512-result" or die "result: $!
"; print $result "done
"; close $result;' </dev/null >/tmp/aenv-fixed512-helper.log 2>&1 &"#;

async fn run_guest_shell_stdout(sandbox: &FirecrackerSandbox, command: &str) -> Result<String> {
    let output = sandbox
        .executor()?
        .run_command("sh", &["-lc", command])
        .await?;
    anyhow::ensure!(
        output.exit_code == 0,
        "guest command failed: {}",
        output.stderr
    );
    Ok(output.stdout)
}

fn fixed_working_set_helper(working_set_mib: u32) -> String {
    FIXED_WORKING_SET_HELPER.replace("512*1024*1024", &format!("{working_set_mib}*1024*1024"))
}

async fn prepare_fixed_working_set_snapshot(
    vcpu_count: u32,
    working_set_mib: u32,
) -> Result<FirecrackerSnapshotConfig> {
    let expected_bytes = u64::from(working_set_mib) * 1024 * 1024;
    let mut sandbox = setup_sandbox_inner_with_vcpu(FIXED_WORKING_SET_MEM_MIB, vcpu_count).await?;
    anyhow::ensure!(
        guest_vcpu_count(&sandbox).await? == vcpu_count,
        "guest vCPU mismatch"
    );
    run_guest_shell(
        &sandbox,
        "command -v perl >/dev/null",
        "check fixed-working-set helper dependency",
    )
    .await?;
    run_guest_shell(
        &sandbox,
        &fixed_working_set_helper(working_set_mib),
        "start fixed-working-set helper",
    )
    .await?;
    run_guest_shell(
        &sandbox,
        "if ! timeout 90 sh -c 'while [ ! -e /tmp/aenv-fixed512-ready ]; do sleep .01; done'; then echo fixed-working-set-helper-failed >&2; cat /tmp/aenv-fixed512-helper.log >&2 || true; exit 1; fi",
        "wait fixed-working-set helper",
    )
    .await?;
    let raw = run_guest_shell_stdout(&sandbox, "cat /tmp/aenv-fixed512-gpas").await?;
    let mut ranges = raw
        .lines()
        .map(|line| -> Result<GuestMemoryRange> {
            let mut f = line.split_whitespace();
            Ok(GuestMemoryRange {
                gpa: f.next().context("missing GPA")?.parse()?,
                size: f.next().context("missing size")?.parse()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // `/proc/self/pagemap` is traversed in virtual-address order, while the
    // backing guest PFNs need not be monotonic. Persist canonical GPA order so
    // the working set remains valid for Firecracker's all-or-nothing RAM-range
    // validation and adjacent guest pages form stable ranges.
    ranges.sort_by_key(|range| range.gpa);
    let mut canonical_ranges: Vec<GuestMemoryRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = canonical_ranges.last_mut() {
            let previous_end = previous
                .gpa
                .checked_add(previous.size)
                .context("compute previous fixed-working-set GPA end")?;
            anyhow::ensure!(
                range.gpa >= previous_end,
                "fixed-working-set helper reported overlapping GPA ranges"
            );
            if range.gpa == previous_end {
                previous.size = previous
                    .size
                    .checked_add(range.size)
                    .context("merge adjacent fixed-working-set GPA ranges")?;
                continue;
            }
        }
        canonical_ranges.push(range);
    }
    let working_set = GuestMemoryWorkingSet::new(canonical_ranges);
    anyhow::ensure!(
        working_set.total_bytes()? == expected_bytes,
        "GPA bytes differ from requested {working_set_mib} MiB"
    );
    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;
    let mut snapshot = snapshot;
    snapshot.restore_working_set = Some(working_set);
    Ok(snapshot)
}

async fn prepare_fixed512_snapshot(vcpu_count: u32) -> Result<FirecrackerSnapshotConfig> {
    prepare_fixed_working_set_snapshot(vcpu_count, FIXED_WORKING_SET_MIB).await
}

/// The pre-fault-only experiment intentionally uses the exact same canonical
/// GPA range for every vCPU configuration. Helper-derived GPA ranges are
/// valuable for the later workload experiment, but vary with guest allocator
/// placement across independently created snapshots and would confound this
/// mechanism-only comparison.
async fn prepare_fixed512_prefault_microbenchmark_snapshot(
    vcpu_count: u32,
) -> Result<FirecrackerSnapshotConfig> {
    let mut snapshot = prepare_fixed512_snapshot(vcpu_count).await?;
    let bytes = u64::from(FIXED_WORKING_SET_MIB) * 1024 * 1024;
    let working_set = GuestMemoryWorkingSet::new(vec![GuestMemoryRange {
        gpa: FIXED_PREFAULT_GPA_START,
        size: bytes,
    }]);
    anyhow::ensure!(
        working_set.total_bytes()? == bytes,
        "fixed pre-fault microbenchmark working-set size changed unexpectedly"
    );
    snapshot.restore_working_set = Some(working_set);
    Ok(snapshot)
}
async fn fixed512_sample(
    snapshot: &FirecrackerSnapshotConfig,
    enabled: bool,
    workers: u32,
) -> Result<PrefaultMeasurement> {
    let command = format!(
        "echo {workers} > /tmp/aenv-fixed512-workers; touch /tmp/aenv-fixed512-go; while [ ! -e /tmp/aenv-fixed512-result ]; do sleep .005; done; cat /tmp/aenv-fixed512-result"
    );
    resume_and_measure(
        snapshot,
        enabled,
        &command,
        "fixed-512 MiB helper scan",
        None,
    )
    .await
}

fn require_fixed512_prefault_limit(options: &BenchmarkCliOptions) -> Result<u64> {
    let limit = options.max_prefault_bytes.context(
        "snapshot_prefault_fixed512_{sanity,scaling} requires an explicit \
         --max-prefault-bytes 536870912 (or larger) benchmark-only override",
    )?;
    let required = u64::from(FIXED_WORKING_SET_MIB) * 1024 * 1024;
    anyhow::ensure!(
        limit >= required,
        "--max-prefault-bytes {limit} is smaller than the fixed {FIXED_WORKING_SET_MIB} MiB working set"
    );
    Ok(limit)
}

async fn fixed512_prefault_only_sample(
    snapshot: &FirecrackerSnapshotConfig,
    vcpu_count: u32,
    max_prefault_bytes: u64,
) -> Result<(PrefaultCompletionStats, Duration, Duration)> {
    let total_started = Instant::now();
    let (mut sandbox, timings) =
        FirecrackerSandbox::resume_from_snapshot_config_with_prefault_and_timings_for_benchmark(
            snapshot,
            true,
            Some(max_prefault_bytes),
        )
        .await
        .context("resume fixed 512 MiB snapshot for pre-fault-only measurement")?;
    let stats_result: Result<PrefaultCompletionStats> = (|| {
        let stats = timings
            .prefault_stats
            .context("pre-fault-only benchmark requires Firecracker completion stats")?;
        let expected_bytes = snapshot
            .restore_working_set
            .as_ref()
            .context("pre-fault-only benchmark snapshot lacks working-set metadata")?
            .total_bytes()?;
        anyhow::ensure!(
            stats.requested_bytes == expected_bytes,
            "pre-fault-only requested {} bytes; expected {expected_bytes}",
            stats.requested_bytes
        );
        anyhow::ensure!(
            stats.completed_bytes == stats.requested_bytes,
            "pre-fault-only rejects incomplete pre-fault: requested={}, completed={}",
            stats.requested_bytes,
            stats.completed_bytes
        );
        anyhow::ensure!(
            stats.remaining_bytes == 0,
            "pre-fault-only rejects {} remaining bytes",
            stats.remaining_bytes
        );
        anyhow::ensure!(
            stats.workers.len() == vcpu_count as usize,
            "pre-fault-only expected {vcpu_count} worker stats; received {}",
            stats.workers.len()
        );
        Ok(stats)
    })();
    let total_wall = total_started.elapsed();
    let stop_result = sandbox.stop().await;
    let stats = stats_result?;
    stop_result.context("stop pre-fault-only measured sandbox")?;
    Ok((stats, timings.prefault, total_wall))
}

fn print_fixed512_prefault_only(
    name: &str,
    vcpu_count: u32,
    stats: &PrefaultCompletionStats,
    api_wall: Duration,
    total_wall: Duration,
) -> Result<()> {
    println!(
        "{name} vcpu={vcpu_count} total_wall_time_us={} api_wall_time_us={} requested_bytes={} completed_bytes={} remaining_bytes={} range_count={} ioctl_count={} firecracker_wall_time_us={}",
        total_wall.as_micros(),
        api_wall.as_micros(),
        stats.requested_bytes,
        stats.completed_bytes,
        stats.remaining_bytes,
        stats.range_count,
        stats.ioctl_count,
        stats.wall_time_us,
    );
    for worker in &stats.workers {
        println!(
            "{name}_worker vcpu={} assigned_range_count={} assigned_bytes={} completed_bytes={} remaining_bytes={} ioctl_count={} wall_time_us={}",
            worker.vcpu_id,
            worker.range_count,
            worker.requested_bytes,
            worker.completed_bytes,
            worker.remaining_bytes,
            worker.ioctl_count,
            worker.wall_time_us,
        );
    }
    Ok(())
}

fn run_prefault_fixed512_sanity_benchmark(
    rt: &Runtime,
    options: &BenchmarkCliOptions,
) -> Result<()> {
    let max_prefault_bytes = require_fixed512_prefault_limit(options)?;
    let snapshot = rt.block_on(prepare_fixed512_prefault_microbenchmark_snapshot(1))?;
    let (stats, api_wall, total_wall) = rt.block_on(fixed512_prefault_only_sample(
        &snapshot,
        1,
        max_prefault_bytes,
    ))?;
    print_fixed512_prefault_only("fixed512_prefault_sanity", 1, &stats, api_wall, total_wall)?;
    Ok(())
}

fn run_prefault_fixed512_scaling_benchmark(
    rt: &Runtime,
    options: &BenchmarkCliOptions,
) -> Result<()> {
    let max_prefault_bytes = require_fixed512_prefault_limit(options)?;
    let vcpu_counts: &[u32] = options
        .prefault_vcpu_count
        .as_ref()
        .map_or(&[1, 2, 4, 8], std::slice::from_ref);
    for &vcpu_count in vcpu_counts {
        let snapshot = rt.block_on(prepare_fixed512_prefault_microbenchmark_snapshot(
            vcpu_count,
        ))?;
        let (stats, api_wall, total_wall) = rt.block_on(fixed512_prefault_only_sample(
            &snapshot,
            vcpu_count,
            max_prefault_bytes,
        ))?;
        print_fixed512_prefault_only(
            "fixed512_prefault_scaling",
            vcpu_count,
            &stats,
            api_wall,
            total_wall,
        )?;
    }
    Ok(())
}

fn fixed512_env_u32_list(name: &str, default: &[u32]) -> Result<Vec<u32>> {
    let Some(raw) = std::env::var_os(name) else {
        return Ok(default.to_vec());
    };
    let values = raw
        .to_string_lossy()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::parse::<u32>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parse {name} as comma-separated positive integers"))?;
    anyhow::ensure!(
        !values.is_empty() && values.iter().all(|value| *value > 0),
        "{name} must contain positive integers"
    );
    Ok(values)
}
fn fixed512_sample_count() -> Result<usize> {
    match std::env::var("AENV_BENCH_FIXED512_SAMPLES") {
        Ok(raw) => {
            let value = raw
                .parse::<usize>()
                .context("parse AENV_BENCH_FIXED512_SAMPLES")?;
            anyhow::ensure!(value > 0, "AENV_BENCH_FIXED512_SAMPLES must be positive");
            Ok(value)
        }
        Err(_) => Ok(FIXED_WORKING_SET_SAMPLES),
    }
}
fn run_prefault_fixed512_benchmark(rt: &Runtime) -> Result<()> {
    let vcpu_counts = fixed512_env_u32_list("AENV_BENCH_FIXED512_VCPUS", &[1, 2, 4, 8])?;
    let worker_override = std::env::var_os("AENV_BENCH_FIXED512_WORKERS")
        .map(|_| fixed512_env_u32_list("AENV_BENCH_FIXED512_WORKERS", &[]))
        .transpose()?;
    let sample_count = fixed512_sample_count()?;
    for vcpu_count in vcpu_counts {
        let snapshot = rt.block_on(prepare_fixed512_snapshot(vcpu_count))?;
        let ws = snapshot
            .restore_working_set
            .as_ref()
            .context("missing fixed working set")?;
        println!(
            "fixed512 vcpu={vcpu_count} ranges={} bytes={} samples={sample_count}",
            ws.ranges.len(),
            ws.total_bytes()?
        );
        let mut workers = worker_override
            .clone()
            .unwrap_or_else(|| vec![1, vcpu_count]);
        workers.sort_unstable();
        workers.dedup();
        anyhow::ensure!(
            workers.iter().all(|worker| *worker <= vcpu_count),
            "fixed512 worker count exceeds vCPU count"
        );
        for workers in workers {
            for (run, enabled) in [false, true, true, false].into_iter().enumerate() {
                let samples = rt.block_on(async {
                    let mut out = Vec::new();
                    for _ in 0..sample_count {
                        out.push(fixed512_sample(&snapshot, enabled, workers).await?);
                    }
                    Ok::<_, anyhow::Error>(out)
                })?;
                print_prefault_measurements(
                    &format!(
                        "fixed512_vcpu{vcpu_count}_workers{workers}_{}_run{}",
                        if enabled { "enabled" } else { "baseline" },
                        run + 1
                    ),
                    &samples,
                );
            }
        }
    }
    Ok(())
}
fn run_prefault_size_sanity_prefault_only(
    rt: &Runtime,
    options: &BenchmarkCliOptions,
) -> Result<()> {
    let max_prefault_bytes = require_fixed512_prefault_limit(options)?;
    for working_set_mib in [4_u32, 16, 64, 256, 512] {
        let mut snapshot = rt.block_on(prepare_fixed_working_set_snapshot(1, working_set_mib))?;
        let bytes = u64::from(working_set_mib) * 1024 * 1024;
        snapshot.restore_working_set = Some(GuestMemoryWorkingSet::new(vec![GuestMemoryRange {
            gpa: FIXED_PREFAULT_GPA_START,
            size: bytes,
        }]));
        let (stats, api_wall, total_wall) = rt.block_on(fixed512_prefault_only_sample(
            &snapshot,
            1,
            max_prefault_bytes,
        ))?;
        print_fixed512_prefault_only(
            &format!("prefault_size_sanity_mib{working_set_mib}"),
            1,
            &stats,
            api_wall,
            total_wall,
        )?;
    }
    Ok(())
}

fn run_prefault_workload_benchmark(rt: &Runtime) -> Result<()> {
    let (snapshot, range_count, byte_count) = rt.block_on(prepare_profiled_workload_snapshot())?;
    println!(
        "prefault_workload invariant: one profiled snapshot; workload is a 8 MiB guest file read plus sha256 via AgentENV envd; working-set ranges {range_count}, bytes {byte_count}; only the in-process pre-fault boolean differs between arms"
    );

    for (run, enabled) in PREFAULT_ARM_ORDER.into_iter().enumerate() {
        let samples = rt.block_on(prefault_measurement_samples(
            &snapshot,
            false,
            enabled,
            PREFAULT_WORKLOAD_REQUEST,
            "first guest workload request",
            None,
        ))?;
        let arm = if enabled { "enabled" } else { "disabled" };
        print_prefault_measurements(
            &format!("prefault_workload_resource_cold_{arm}_run{}", run + 1),
            &samples,
        );
    }
    Ok(())
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

fn run_default_snapshot_benchmarks() -> Result<()> {
    let options = benchmark_cli_options()?;
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
    if should_run("snapshot_prefault_e2e", &filters) {
        run_prefault_e2e_benchmark(&rt)?;
        ran = true;
    }
    if should_run("snapshot_prefault_phase_e2e", &filters) {
        run_prefault_phase_e2e_benchmark(&rt)?;
        ran = true;
    }
    if should_run("snapshot_prefault_size_sanity", &filters) {
        run_prefault_size_sanity_prefault_only(&rt, &options)?;
        ran = true;
    }
    if should_run("snapshot_prefault_fixed512", &filters) {
        run_prefault_fixed512_benchmark(&rt)?;
        ran = true;
    }
    if should_run("snapshot_prefault_fixed512_sanity", &filters) {
        run_prefault_fixed512_sanity_benchmark(&rt, &options)?;
        ran = true;
    }
    if should_run("snapshot_prefault_fixed512_scaling", &filters) {
        run_prefault_fixed512_scaling_benchmark(&rt, &options)?;
        ran = true;
    }
    if should_run("snapshot_prefault_multivcpu_e2e", &filters) {
        run_prefault_multivcpu_e2e_benchmark(&rt)?;
        ran = true;
    }
    if should_run("snapshot_prefault_workload", &filters) {
        run_prefault_workload_benchmark(&rt)?;
        ran = true;
    }

    if should_run("snapshot_mincore_stages", &filters) {
        run_mincore_stage_diagnostic(&rt)?;
        ran = true;
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
