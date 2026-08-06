use anyhow::{Context, Result};
use clap::Parser;
use nix::sys::resource::{setrlimit, Resource};
use serde::Deserialize;
use std::os::fd::{FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tracing_log::log::LevelFilter;

use overlaybd::image_service::ImageService;
use storage_util::io_ring::spawn_io_ring_worker;
use uvm_ublk_daemon::{server::UblkDaemonServer, ResizeToolSpec};

mod metrics_server;

#[derive(Debug, Parser)]
#[command(
    name = "uvm-ublk-daemon",
    about = "Centralized ublk device manager daemon. Manages all ublk devices in a single process."
)]
struct Cli {
    /// Path to the Unix domain socket for control communication.
    #[arg(long)]
    socket_path: PathBuf,

    /// Path to overlaybd global config JSON.
    #[arg(long)]
    global_config: PathBuf,

    /// Path to AgentENV TOML config. Pool settings are read from [pool.block].
    #[arg(long)]
    config: Option<PathBuf>,

    /// Log level: off, error, warn, info, debug, trace.
    #[arg(long, default_value = "info")]
    log_level: LevelFilter,

    /// Optional log file path. If omitted, logs to stderr.
    #[arg(long)]
    log_file: Option<PathBuf>,

    /// HTTP listen address for Prometheus metrics. Empty string disables it.
    #[arg(long, default_value = "0.0.0.0:9103")]
    metrics_listen_addr: String,

    /// Enable warm pool for overlaybd devices.
    #[arg(long)]
    enable_pool: bool,

    /// Override warm pool low watermark when --enable-pool is used.
    #[arg(long)]
    pool_low_watermark: Option<usize>,

    /// Override warm pool high watermark when --enable-pool is used.
    #[arg(long)]
    pool_high_watermark: Option<usize>,

    /// Override whether the overlaybd pool prewarms after first image use.
    #[arg(long)]
    pool_startup_prewarm: Option<bool>,

    /// Local HTTP endpoint used to publish completed overlaybd layers into P2P.
    #[arg(long)]
    p2p_publish_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DaemonTomlConfig {
    home_path: Option<PathBuf>,
    deps_path: Option<PathBuf>,
    pool: Option<DaemonPoolTomlConfig>,
    ublk: Option<DaemonUblkTomlConfig>,
}

#[derive(Debug, Deserialize)]
struct DaemonUblkTomlConfig {
    overlaybd: Option<DaemonUblkOverlaybdTomlConfig>,
}

#[derive(Debug, Deserialize)]
struct DaemonUblkOverlaybdTomlConfig {
    resize_timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DaemonPoolTomlConfig {
    low_watermark: Option<usize>,
    high_watermark: Option<usize>,
    block: Option<DaemonPoolComponentConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct DaemonPoolComponentConfig {
    enabled: Option<bool>,
    startup_prewarm: Option<bool>,
}

#[derive(Debug, Default)]
struct PoolConfigOverrides {
    low_watermark: Option<usize>,
    high_watermark: Option<usize>,
    startup_prewarm: Option<bool>,
}

fn load_pool_config(
    config: Option<&DaemonTomlConfig>,
    force_enable: bool,
    overrides: &PoolConfigOverrides,
) -> Result<Option<warm_pool::PoolConfig>> {
    let Some(config) = config else {
        return Ok(force_enable.then(|| apply_pool_overrides(default_pool_config(), overrides)));
    };

    let common = config.pool.as_ref();
    let pool = common.and_then(|pool| pool.block.as_ref());
    let enabled = pool.and_then(|pool| pool.enabled).unwrap_or(force_enable);
    if !enabled {
        return Ok(None);
    }
    Ok(Some(warm_pool::PoolConfig {
        low_watermark: overrides
            .low_watermark
            .or_else(|| common.and_then(|pool| pool.low_watermark))
            .unwrap_or(2),
        high_watermark: overrides
            .high_watermark
            .or_else(|| common.and_then(|pool| pool.high_watermark))
            .unwrap_or(64),
        // ublk-daemon refills overlaybd devices inline from acquire/release
        // requests because prewarming needs an async ublk control path and the
        // request's current overlaybd image. Do not enable the generic
        // synchronous background worker semantics for this pool.
        maintenance_enabled: false,
        startup_prewarm: overrides
            .startup_prewarm
            .or_else(|| pool.and_then(|pool| pool.startup_prewarm))
            .unwrap_or(true),
    }))
}

fn load_daemon_config(path: Option<&PathBuf>) -> Result<Option<DaemonTomlConfig>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read daemon config {}", path.display()))?;
    toml::from_str(&contents)
        .with_context(|| format!("parse daemon config {}", path.display()))
        .map(Some)
}

const HOME_PATH_PLACEHOLDER: &str = "$AENV_HOME";
const DEFAULT_HOME_PATH: &str = "/var/lib/aenv";
const DEFAULT_DEPS_PATH: &str = "./env";
const DEFAULT_RESIZE_TIMEOUT_SECS: u64 = 120;

fn load_resize_tool_config(
    config_path: Option<&PathBuf>,
    config: Option<&DaemonTomlConfig>,
) -> Result<Option<ResizeToolSpec>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let config_dir = config_path
        .and_then(|path| path.parent())
        .unwrap_or_else(|| Path::new("."));
    let home_path = resolve_relative_to(
        config_dir,
        &env_path("AENV_HOME_PATH")
            .or_else(|| config.home_path.clone())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_HOME_PATH)),
    );
    let deps_path = resolve_config_path(
        &home_path,
        config_dir,
        &env_path("AENV_DEPS_PATH")
            .or_else(|| config.deps_path.clone())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DEPS_PATH)),
    );
    let resize_timeout_secs = config
        .ublk
        .as_ref()
        .and_then(|ublk| ublk.overlaybd.as_ref())
        .and_then(|overlaybd| overlaybd.resize_timeout_secs)
        .unwrap_or(DEFAULT_RESIZE_TIMEOUT_SECS);
    anyhow::ensure!(
        resize_timeout_secs > 0,
        "invalid ublk.overlaybd config: resize_timeout_secs must be > 0"
    );
    Ok(Some(ResizeToolSpec {
        binary: deps_path.join("overlaybd/bin/overlaybd-resize"),
        lib_dir: None,
        timeout_secs: resize_timeout_secs,
    }))
}

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn resolve_config_path(home_path: &Path, config_dir: &Path, raw: &Path) -> PathBuf {
    let expanded = match raw.to_str() {
        Some(s) if s.contains(HOME_PATH_PLACEHOLDER) => {
            PathBuf::from(s.replace(HOME_PATH_PLACEHOLDER, &home_path.to_string_lossy()))
        }
        _ => raw.to_path_buf(),
    };
    resolve_relative_to(config_dir, &expanded)
}

fn resolve_relative_to(base_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

fn default_pool_config() -> warm_pool::PoolConfig {
    warm_pool::PoolConfig {
        low_watermark: 2,
        high_watermark: 64,
        maintenance_enabled: false,
        startup_prewarm: true,
    }
}

fn apply_pool_overrides(
    mut config: warm_pool::PoolConfig,
    overrides: &PoolConfigOverrides,
) -> warm_pool::PoolConfig {
    if let Some(value) = overrides.low_watermark {
        config.low_watermark = value;
    }
    if let Some(value) = overrides.high_watermark {
        config.high_watermark = value;
    }
    if let Some(value) = overrides.startup_prewarm {
        config.startup_prewarm = value;
    }
    config
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    uvm_ublk::setup_tracing(cli.log_file.clone(), cli.log_level).context("setup tracing")?;

    // Capture the parent PID before entering the async runtime so
    // getppid() is called on the main thread where the value is reliable.
    let parent_pid = nix::unistd::getppid();

    // Raise file descriptor limit.
    let target = 1_048_576;
    if let Err(err) = setrlimit(Resource::RLIMIT_NOFILE, target, target) {
        tracing::warn!(?err, target, "failed to raise RLIMIT_NOFILE");
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async {
        let (metrics_shutdown_tx, metrics_shutdown_rx) = tokio::sync::watch::channel(false);
        metrics_server::spawn(&cli.metrics_listen_addr, metrics_shutdown_rx)
            .await
            .context("start ublk daemon metrics server")?;

        // Create a shared ImageService from the global config.
        let image_service = ImageService::from_config_path_with_p2p_publish_url(
            &cli.global_config,
            cli.p2p_publish_url.clone(),
        )
        .await
        .with_context(|| {
            format!(
                "create ImageService from global config: {}",
                cli.global_config.display()
            )
        })?;

        // Create the shared ctrl io_uring for ublk control commands.
        let (ctrl_ring, _ctrl_ring_handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);

        let mut server = UblkDaemonServer::new_with_p2p_publish_url(
            cli.socket_path.clone(),
            ctrl_ring,
            image_service,
            cli.p2p_publish_url.clone(),
        );
        let daemon_config =
            load_daemon_config(cli.config.as_ref()).context("load daemon config")?;
        if let Some(resize_tool) =
            load_resize_tool_config(cli.config.as_ref(), daemon_config.as_ref())
                .context("load overlaybd resize tool config")?
        {
            server.set_resize_tool(resize_tool);
        }

        // Enable warm pool from the AgentENV TOML config when requested.
        let pool_overrides = PoolConfigOverrides {
            low_watermark: cli.pool_low_watermark,
            high_watermark: cli.pool_high_watermark,
            startup_prewarm: cli.pool_startup_prewarm,
        };
        if let Some(pool_config) =
            load_pool_config(daemon_config.as_ref(), cli.enable_pool, &pool_overrides)
                .context("load warm pool config")?
        {
            server
                .enable_pool(pool_config)
                .await
                .context("enable warm pool")?;
        }

        // Open a pidfd for the parent process. When the parent process
        // exits the fd becomes readable, allowing us to shut down
        // gracefully. This monitors process-level lifetime (not thread),
        // which avoids the pitfall of prctl(PR_SET_PDEATHSIG) firing when
        // the specific thread that called fork() exits.
        let parent_pidfd = pidfd_open(parent_pid).context("open parent pidfd")?;
        let async_parent_pidfd = AsyncFd::with_interest(parent_pidfd, Interest::READABLE)
            .context("register parent pidfd with tokio")?;

        let server = Arc::new(server);

        // Shut down on SIGTERM, SIGINT, or parent process exit.
        let shutdown_server = {
            let s = Arc::clone(&server);
            let metrics_shutdown_tx = metrics_shutdown_tx.clone();
            async move {
                let mut sigterm =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("install SIGTERM handler");
                let mut sigint =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                        .expect("install SIGINT handler");

                tokio::select! {
                    _ = sigterm.recv() => {
                        tracing::info!("received SIGTERM");
                    }
                    _ = sigint.recv() => {
                        tracing::info!("received SIGINT");
                    }
                    result = async_parent_pidfd.readable() => {
                        match result {
                            Ok(_guard) => tracing::info!("parent process exited, shutting down"),
                            Err(err) => tracing::warn!(?err, "parent pidfd error, shutting down"),
                        }
                    }
                }
                s.request_shutdown();
                let _ = metrics_shutdown_tx.send(true);
            }
        };

        tokio::spawn(shutdown_server);
        let result = server
            .run_with_ready_signal(|| {
                println!("ready");
                use std::io::Write;
                std::io::stdout().flush().context("flush ready signal")
            })
            .await;
        let _ = metrics_shutdown_tx.send(true);
        result
    })
}

/// Open a pidfd for the given process (Linux 5.3+).
///
/// The returned file descriptor becomes readable (`POLLIN`) when the
/// target process exits, making it suitable for async polling via
/// `tokio::io::unix::AsyncFd`.
fn pidfd_open(pid: nix::unistd::Pid) -> Result<OwnedFd> {
    let ret = unsafe { libc::syscall(libc::SYS_pidfd_open, pid.as_raw() as libc::c_int, 0u32) };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            tracing::warn!("parent process already exited before pidfd_open");
            std::process::exit(0);
        }
        return Err(err).context("pidfd_open");
    }
    Ok(unsafe { OwnedFd::from_raw_fd(ret as i32) })
}
