use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use storage_util::{abort_on_drop, fatal_exit};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::{Duration, Instant};
use warm_pool::PoolConfig;

use crate::protocol::{
    recv_message, AccessMode, DaemonRequest, DaemonResponse, DeviceRole, DeviceSpec,
    PackRecordingState, RestackSnapshotResult, SourceStateStrategy,
};
use overlaybd::{config::UpperMode, RestackSnapshotTerminalFailure};

/// Default timeout for daemon RPC calls.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for snapshot operations (may be slow for large images).
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(360);

/// Timeout for waiting for the daemon to signal readiness.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for a detached daemon to flush its images and exit.
const DETACH_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for a new daemon to recover every registered device.
const RECOVER_TIMEOUT: Duration = Duration::from_secs(120);

/// Attempts to start a replacement daemon before giving up on its devices.
const RESPAWN_ATTEMPTS: u32 = 3;

/// Base delay between replacement attempts; attempt `n` waits `n` times this.
const RESPAWN_BACKOFF: Duration = Duration::from_secs(1);

/// How long shutdown waits for in-flight RPCs and an in-progress restart.
const SHUTDOWN_GATE_TIMEOUT: Duration = Duration::from_secs(30);

fn configure_daemon_capabilities(command: &mut tokio::process::Command) -> Result<()> {
    let can_delegate = linux_cap::has_effective_capabilities(&[linux_cap::CAP_SYS_ADMIN])
        .context("inspect process capabilities before spawning ublk daemon")?;
    if !can_delegate {
        bail!("cannot scope ublk daemon: CAP_SYS_ADMIN is not effective in the server process");
    }
    // SAFETY: the pre-exec closure performs only prctl/capset syscalls. The
    // server validates the runtime capability contract before spawning the
    // daemon. Root may have an empty inheritable set, but can populate it in
    // the child before exec while CAP_SYS_ADMIN is effective.
    unsafe {
        command.pre_exec(|| {
            linux_cap::configure_current_process_capabilities(&[linux_cap::CAP_SYS_ADMIN])
        });
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct OverlaybdRuntimeDevice {
    pub dev_id: u32,
    pub device_path: PathBuf,
    pub actual_virtual_size: u64,
    pub runtime_image_config_path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub struct CreateOverlaybdRuntimeDeviceRequest<'a> {
    pub source_image_config: &'a Path,
    pub global_config: &'a Path,
    pub runtime_dir: &'a Path,
    pub read_only: bool,
    pub runtime_upper_mode: UpperMode,
    pub requested_virtual_size: Option<u64>,
    pub known_source_virtual_size: Option<u64>,
    pub allow_shrink: bool,
}

#[derive(Debug)]
pub struct InvalidRequestError {
    message: String,
}

impl InvalidRequestError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for InvalidRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for InvalidRequestError {}

/// Client for communicating with the `uvm-ublk-daemon` process.
///
/// Owns the daemon child process. Spawns a background watchdog task that
/// monitors whether the daemon is alive. If the daemon exits unexpectedly,
/// all subsequent RPC calls fail immediately with a clear error message
/// instead of attempting a socket connection to a dead process.
pub struct UblkDaemonClient {
    inner: Arc<UblkDaemonClientInner>,
}

pub struct UblkDaemonSpawnConfig<'a> {
    pub binary_path: &'a Path,
    pub socket_path: PathBuf,
    pub global_config: &'a Path,
    pub resize_global_config: &'a Path,
    pub app_config: Option<&'a Path>,
    pub log_file: Option<&'a Path>,
    pub metrics_listen_addr: &'a str,
    pub pool_config: Option<&'a PoolConfig>,
    pub p2p_publish_url: Option<&'a str>,
    pub runtime_device_timeout: Duration,
    /// Replace the daemon and recover its devices when it exits unexpectedly.
    pub recovery_enabled: bool,
}

/// Owned copy of [`UblkDaemonSpawnConfig`], kept to start replacement daemons.
struct DaemonCommand {
    binary_path: PathBuf,
    socket_path: PathBuf,
    global_config: PathBuf,
    resize_global_config: PathBuf,
    app_config: Option<PathBuf>,
    log_file: Option<PathBuf>,
    metrics_listen_addr: String,
    pool_config: Option<PoolConfig>,
    p2p_publish_url: Option<String>,
}

impl DaemonCommand {
    fn new(config: &UblkDaemonSpawnConfig<'_>) -> Self {
        Self {
            binary_path: config.binary_path.to_path_buf(),
            socket_path: config.socket_path.clone(),
            global_config: config.global_config.to_path_buf(),
            resize_global_config: config.resize_global_config.to_path_buf(),
            app_config: config.app_config.map(Path::to_path_buf),
            log_file: config.log_file.map(Path::to_path_buf),
            metrics_listen_addr: config.metrics_listen_addr.to_string(),
            pool_config: config.pool_config.cloned(),
            p2p_publish_url: config.p2p_publish_url.map(str::to_string),
        }
    }
}

struct UblkDaemonClientInner {
    socket_path: PathBuf,
    /// Set to `true` when the daemon process exits (expected or unexpected).
    daemon_dead: AtomicBool,
    /// Retains completion so shutdown callers cannot miss the watchdog's exit.
    daemon_exited: tokio::sync::watch::Sender<bool>,
    /// Set to `true` when `shutdown()` is called, so the watchdog task
    /// doesn't log the expected exit as an error.
    shutting_down: AtomicBool,
    /// Populated with the exit status description when the daemon dies.
    death_reason: Mutex<Option<String>>,
    runtime_device_timeout: Duration,
    /// How to start a replacement daemon; `None` for test clients.
    command: Option<DaemonCommand>,
    recovery_enabled: bool,
    /// Set while `restart()` replaces the daemon, so the old daemon's exit is
    /// logged as expected.
    restarting: AtomicBool,
    /// Pid of the current daemon process, or 0.
    child_pid: AtomicU32,
    /// Counts spawned daemons, so a watchdog can tell whether its daemon is
    /// still the current one.
    generation: AtomicU64,
    /// Every device this client holds, kept so a replacement daemon can
    /// recover them. Updated while the RPC holds `gate`.
    registry: Mutex<HashMap<u32, DeviceSpec>>,
    /// RPCs hold it shared; replacing the daemon holds it exclusively, so no
    /// RPC runs against a daemon that is going away.
    gate: tokio::sync::RwLock<()>,
}

impl UblkDaemonClient {
    /// Spawn the `uvm-ublk-daemon` process and wait for it to become ready.
    ///
    /// This is the primary constructor. It:
    /// 1. Spawns the daemon binary as a child process.
    /// 2. Waits for `"ready"` on stdout (with a 30s timeout).
    /// 3. Spawns a background watchdog task that monitors the child process
    ///    and sets `daemon_dead` if it exits unexpectedly.
    ///
    /// Returns `Arc<Self>` because the watchdog task needs a reference.
    pub async fn new(config: UblkDaemonSpawnConfig<'_>) -> Result<Arc<Self>> {
        tracing::info!(
            binary = %config.binary_path.display(),
            socket = %config.socket_path.display(),
            global_config = %config.global_config.display(),
            resize_global_config = %config.resize_global_config.display(),
            app_config = ?config.app_config,
            log_file = ?config.log_file,
            metrics_listen_addr = config.metrics_listen_addr,
            pool_enabled = config.pool_config.is_some(),
            p2p_publish_enabled = config.p2p_publish_url.is_some(),
            recovery_enabled = config.recovery_enabled,
            "spawning ublk daemon"
        );

        let command = DaemonCommand::new(&config);
        let child = Self::spawn_daemon(&command).await?;

        let client = Arc::new(Self {
            inner: Arc::new(UblkDaemonClientInner {
                socket_path: config.socket_path,
                daemon_dead: AtomicBool::new(false),
                daemon_exited: tokio::sync::watch::channel(false).0,
                shutting_down: AtomicBool::new(false),
                death_reason: Mutex::new(None),
                runtime_device_timeout: config.runtime_device_timeout,
                command: Some(command),
                recovery_enabled: config.recovery_enabled,
                restarting: AtomicBool::new(false),
                child_pid: AtomicU32::new(0),
                generation: AtomicU64::new(0),
                registry: Mutex::new(HashMap::new()),
                gate: tokio::sync::RwLock::new(()),
            }),
        });

        // Spawn the watchdog task to monitor the child process.
        Self::spawn_watchdog(client.inner.clone(), child);

        tracing::info!("ublk daemon ready");
        Ok(client)
    }

    /// Start a daemon process and wait until it is ready.
    async fn spawn_daemon(command: &DaemonCommand) -> Result<tokio::process::Child> {
        Self::wait_for_socket_available(&command.socket_path).await?;

        let mut cmd = tokio::process::Command::new(&command.binary_path);
        configure_daemon_capabilities(&mut cmd)?;
        cmd.arg("--socket-path")
            .arg(&command.socket_path)
            .arg("--global-config")
            .arg(&command.global_config)
            .arg("--resize-global-config")
            .arg(&command.resize_global_config)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        // Keep the daemon out of the server's foreground process group.
        // Otherwise a terminal Ctrl+C sends SIGINT to both agentenv and the
        // ublk daemon, interrupting snapshot/restack RPCs during graceful
        // shutdown.
        cmd.process_group(0);

        if let Some(path) = &command.app_config {
            cmd.arg("--config").arg(path);
        }

        if let Some(path) = &command.log_file {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).context("create parent dir for ublk daemon log")?;
            }
            cmd.arg("--log-file").arg(path);
        }

        cmd.arg("--metrics-listen-addr")
            .arg(&command.metrics_listen_addr);
        if let Some(url) = &command.p2p_publish_url {
            cmd.arg("--p2p-publish-url").arg(url);
        }

        if let Some(pool_config) = &command.pool_config {
            if command.app_config.is_none() {
                cmd.arg("--enable-pool")
                    .arg("--pool-low-watermark")
                    .arg(pool_config.low_watermark.to_string())
                    .arg("--pool-high-watermark")
                    .arg(pool_config.high_watermark.to_string())
                    .arg("--pool-startup-prewarm")
                    .arg(pool_config.startup_prewarm.to_string());
            }
        }

        let mut child = cmd
            .spawn()
            .with_context(|| format!("spawn ublk daemon: {}", command.binary_path.display()))?;

        // Wait for "ready" on stdout.
        let stdout = child
            .stdout
            .take()
            .context("ublk daemon child process has no stdout")?;
        Self::wait_for_ready(stdout, &mut child).await?;
        Ok(child)
    }

    async fn wait_for_socket_available(socket_path: &Path) -> Result<()> {
        let deadline = Instant::now() + READY_TIMEOUT;

        loop {
            if !socket_path.exists() {
                return Ok(());
            }

            match std::os::unix::net::UnixStream::connect(socket_path) {
                Ok(_) => {
                    if Instant::now() >= deadline {
                        bail!(
                            "daemon socket {} is still in use after {READY_TIMEOUT:?}",
                            socket_path.display()
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => {
                    std::fs::remove_file(socket_path).with_context(|| {
                        format!("remove stale daemon socket: {}", socket_path.display())
                    })?;
                    return Ok(());
                }
            }
        }
    }

    /// Wait for the daemon to print "ready" on stdout.
    async fn wait_for_ready(
        stdout: tokio::process::ChildStdout,
        child: &mut tokio::process::Child,
    ) -> Result<()> {
        let mut reader = tokio::io::BufReader::new(stdout);
        let mut line = String::new();

        let ready = tokio::time::timeout(READY_TIMEOUT, async {
            loop {
                line.clear();
                let n = reader
                    .read_line(&mut line)
                    .await
                    .context("read ublk daemon stdout")?;
                if n == 0 {
                    let status = child.wait().await.ok();
                    bail!("ublk daemon exited before ready (status={status:?})");
                }
                if line.trim() == "ready" {
                    return Ok(());
                }
            }
        })
        .await;

        match ready {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                let _ = child.kill().await;
                Err(e)
            }
            Err(_) => {
                let _ = child.kill().await;
                bail!("ublk daemon did not become ready within {READY_TIMEOUT:?}");
            }
        }
    }

    /// Spawn a background task that waits for the child process to exit.
    ///
    /// An exit during shutdown sets `daemon_dead` so that subsequent RPC
    /// calls fail immediately instead of trying to connect to a dead socket.
    /// Any other exit signals `daemon_exited`, then waits for a restart or
    /// recovery in progress. If nothing replaced the daemon meanwhile, the
    /// watchdog replaces it and recovers its devices when recovery is
    /// enabled, and otherwise marks it dead. A replacement that dies after
    /// recovering is thus replaced again, even mid-restart.
    fn spawn_watchdog(inner: Arc<UblkDaemonClientInner>, mut child: tokio::process::Child) {
        let pid = child.id().unwrap_or(0);
        inner.child_pid.store(pid, Ordering::Release);
        let generation = inner.generation.fetch_add(1, Ordering::AcqRel) + 1;
        tokio::spawn(async move {
            let status = child.wait().await;
            // The pid may be reused once reaped; never signal it again.
            let _ = inner
                .child_pid
                .compare_exchange(pid, 0, Ordering::AcqRel, Ordering::Acquire);

            let reason = match &status {
                Ok(exit) => format!("ublk daemon exited: {exit}"),
                Err(err) => format!("ublk daemon wait failed: {err}"),
            };

            if inner.shutting_down.load(Ordering::Acquire) {
                if !matches!(status, Ok(exit) if exit.success()) {
                    fatal_exit(&reason);
                }
                tracing::info!(%reason, "ublk daemon stopped (expected shutdown)");
                inner.mark_dead(reason);
                return;
            }
            if inner.restarting.load(Ordering::Acquire) {
                tracing::info!(%reason, "ublk daemon stopped for restart");
            } else {
                tracing::error!(%reason, "ublk daemon exited unexpectedly");
            }
            inner.daemon_exited.send_replace(true);

            let _gate = inner.gate.write().await;
            if inner.generation.load(Ordering::Acquire) != generation
                || inner.daemon_dead.load(Ordering::Acquire)
            {
                // Already replaced, or given up on.
                return;
            }
            if inner.shutting_down.load(Ordering::Acquire) {
                inner.mark_dead(reason);
                return;
            }
            if !inner.recovery_enabled || inner.command.is_none() {
                fatal_exit(&reason);
            }
            let result = inner.respawn_and_recover().await;
            if let Err(err) = result {
                inner
                    .fail_permanently(format!("{reason}; replacement failed: {err:#}"))
                    .await;
            }
        });
    }

    /// Replace the daemon with a new process of the same binary, keeping
    /// every device this client holds.
    ///
    /// Waits for in-flight RPCs, detaches the current daemon (which flushes
    /// its images and exits without stopping devices), starts a new daemon
    /// and has it recover the devices. Guest I/O stalls from the old daemon's
    /// exit until recovery. Devices the kernel cannot recover (for example
    /// ones created without user recovery) are deleted.
    ///
    /// If no replacement daemon can be started, the client is marked dead and
    /// its devices are deleted so guests see I/O errors instead of hanging.
    pub async fn restart(&self) -> Result<()> {
        let inner = &self.inner;
        if inner.command.is_none() {
            bail!("cannot restart a ublk daemon this client did not spawn");
        }
        let _gate = inner.gate.write().await;
        inner.ensure_alive()?;
        if inner.shutting_down.load(Ordering::Acquire) {
            bail!("ublk daemon is shutting down");
        }

        tracing::info!("restarting ublk daemon");
        inner.restarting.store(true, Ordering::Release);
        let mut exited = inner.daemon_exited.subscribe();
        match tokio::time::timeout(
            DETACH_TIMEOUT,
            inner.call_inner(DaemonRequest::Detach, &mut false, false),
        )
        .await
        {
            Ok(Ok(DaemonResponse::Ok)) => {}
            Ok(Ok(other)) => tracing::warn!(?other, "unexpected response to ublk daemon detach"),
            Ok(Err(err)) => {
                tracing::warn!(error = %format!("{err:#}"), "ublk daemon detach failed")
            }
            Err(_) => tracing::warn!("ublk daemon detach timed out"),
        }
        if tokio::time::timeout(DETACH_TIMEOUT, exited.wait_for(|exited| *exited))
            .await
            .is_err()
        {
            // Devices survive a killed daemon just as they survive a detach.
            tracing::warn!("ublk daemon did not exit after detach; killing it");
            inner.kill_child();
            exited
                .wait_for(|exited| *exited)
                .await
                .context("wait for ublk daemon exit")?;
        }
        inner.restarting.store(false, Ordering::Release);

        let result = inner.respawn_and_recover().await;
        if let Err(err) = &result {
            inner
                .fail_permanently(format!("ublk daemon restart failed: {err:#}"))
                .await;
        }
        result
    }

    /// The socket path this client connects to.
    pub fn socket_path(&self) -> &Path {
        &self.inner.socket_path
    }

    /// Create a raw overlaybd-backed ublk device.
    ///
    /// This opens an existing image config as-is. It does not materialize an
    /// OverlayBD runtime directory, create upper files, or rewrite paths. Use
    /// [`create_overlaybd_runtime_device`] for sandbox rootfs and extra drives.
    ///
    /// `global_config` specifies the overlaybd global config to use for this
    /// device's `ImageService`. The daemon lazily creates one `ImageService`
    /// per unique global config path.
    ///
    /// Returns the kernel-assigned device ID and path (e.g. `/dev/ublkb0`).
    pub async fn create_overlaybd(
        &self,
        image_config: &Path,
        global_config: &Path,
    ) -> Result<(u32, PathBuf)> {
        self.create_overlaybd_with_upper_access(image_config, global_config, false)
            .await
    }

    pub async fn create_overlaybd_with_upper_access(
        &self,
        image_config: &Path,
        global_config: &Path,
        read_only_upper: bool,
    ) -> Result<(u32, PathBuf)> {
        let request = DaemonRequest::CreateOverlaybd {
            image_config: image_config.to_path_buf(),
            global_config: global_config.to_path_buf(),
            read_only_upper,
        };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::DeviceCreated {
                dev_id,
                device_path,
            } => Ok((dev_id, device_path)),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Report that the sandbox owning `device_key` (the image.json path its
    /// memory device was opened with) finished booting, releasing any held
    /// background downloads. Best-effort: a failed notification only means the
    /// downloads start after the fallback timeout instead.
    pub async fn notify_sandbox_ready(&self, device_key: &str) -> Result<()> {
        let request = DaemonRequest::NotifySandboxReady {
            device_key: device_key.to_string(),
        };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Ok => Ok(()),
            response => bail!("unexpected ublk response: {response:?}"),
        }
    }

    /// `output` and reports the result through [`Self::pack_recording_status`].
    #[allow(clippy::too_many_arguments)]
    pub async fn start_pack_recording(
        &self,
        dev_id: u32,
        output: &Path,
        max_pages: u32,
        min_window_ms: u64,
        quiet_ms: u64,
        max_window_ms: u64,
    ) -> Result<()> {
        let request = DaemonRequest::StartPackRecording {
            dev_id,
            output: output.to_path_buf(),
            max_pages,
            min_window_ms,
            quiet_ms,
            max_window_ms,
        };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Ok => Ok(()),
            DaemonResponse::InvalidRequest { message } => {
                bail!("daemon: start pack recording rejected: {message}")
            }
            DaemonResponse::Error { message } => {
                bail!("daemon: start pack recording failed: {message}")
            }
            other => bail!("daemon: unexpected response for start pack recording: {other:?}"),
        }
    }

    /// Poll the state of the pack recording running on `dev_id`.
    pub async fn pack_recording_status(&self, dev_id: u32) -> Result<PackRecordingState> {
        let request = DaemonRequest::PackRecordingStatus { dev_id };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::PackRecording { state } => Ok(state),
            DaemonResponse::InvalidRequest { message } => {
                bail!("daemon: pack recording status rejected: {message}")
            }
            DaemonResponse::Error { message } => {
                bail!("daemon: pack recording status failed: {message}")
            }
            other => bail!("daemon: unexpected response for pack recording status: {other:?}"),
        }
    }

    /// Abort the pack recording on `dev_id` (idempotent): detach the
    /// recorder, stop the window task, and remove any partial pack output.
    pub async fn abort_pack_recording(&self, dev_id: u32) -> Result<()> {
        let request = DaemonRequest::AbortPackRecording { dev_id };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Ok => Ok(()),
            DaemonResponse::Error { message } => {
                bail!("daemon: abort pack recording failed: {message}")
            }
            other => bail!("daemon: unexpected response for abort pack recording: {other:?}"),
        }
    }

    /// Best-effort: register a startup pack prefetch for the memory image at
    /// `image_config`. The daemon deduplicates by pack identity and always
    /// answers `Ok`; failures are logged daemon-side only.
    #[allow(clippy::too_many_arguments)]
    pub async fn prefetch_startup_pack(
        &self,
        image_config: &Path,
        global_config: &Path,
        url: &str,
        pack_size: u64,
        index_sha256: &str,
        mem_virtual_size: u64,
        timeout_secs: u64,
    ) -> Result<()> {
        let request = DaemonRequest::PrefetchStartupPack {
            image_config: image_config.to_path_buf(),
            global_config: global_config.to_path_buf(),
            url: url.to_string(),
            pack_size,
            index_sha256: index_sha256.to_string(),
            mem_virtual_size,
            timeout_secs,
        };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Ok => Ok(()),
            DaemonResponse::Error { message } => {
                bail!("daemon: prefetch startup pack failed: {message}")
            }
            other => bail!("daemon: unexpected response for prefetch startup pack: {other:?}"),
        }
    }

    /// Create an OverlayBD runtime config and acquire a ublk device for it.
    pub async fn create_overlaybd_runtime_device(
        &self,
        request: CreateOverlaybdRuntimeDeviceRequest<'_>,
    ) -> Result<OverlaybdRuntimeDevice> {
        self.create_overlaybd_runtime_device_with_source_state_strategy(
            request,
            SourceStateStrategy::Reuse,
        )
        .await
    }

    /// Create an OverlayBD runtime while optionally consuming source-owned
    /// state into the new runtime directory before opening the device.
    pub async fn create_overlaybd_runtime_device_with_source_state_strategy(
        &self,
        request: CreateOverlaybdRuntimeDeviceRequest<'_>,
        source_state_strategy: SourceStateStrategy,
    ) -> Result<OverlaybdRuntimeDevice> {
        let request = DaemonRequest::CreateOverlaybdRuntimeDevice {
            source_image_config: request.source_image_config.to_path_buf(),
            global_config: request.global_config.to_path_buf(),
            runtime_dir: request.runtime_dir.to_path_buf(),
            read_only: request.read_only,
            runtime_upper_mode: request.runtime_upper_mode,
            requested_virtual_size: request.requested_virtual_size,
            known_source_virtual_size: request.known_source_virtual_size,
            allow_shrink: request.allow_shrink,
            source_state_strategy,
        };
        match self
            .call(request, self.inner.runtime_device_timeout)
            .await?
        {
            DaemonResponse::OverlaybdRuntimeDeviceCreated {
                dev_id,
                device_path,
                actual_virtual_size,
                runtime_image_config_path,
            } => Ok(OverlaybdRuntimeDevice {
                dev_id,
                device_path,
                actual_virtual_size,
                runtime_image_config_path,
            }),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Delete a ublk device.
    pub async fn delete(&self, dev_id: u32) -> Result<()> {
        let request = DaemonRequest::Delete { dev_id };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Deleted => Ok(()),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Request a restack-style snapshot of an overlaybd device's upper layer.
    pub async fn restack_snapshot(
        &self,
        dev_id: u32,
        output_layer_path: &Path,
        compact_from: Option<usize>,
    ) -> Result<RestackSnapshotResult> {
        let request = DaemonRequest::RestackSnapshot {
            dev_id,
            output_layer_path: output_layer_path.to_path_buf(),
            compact_from,
        };
        let response = self.call(request, SNAPSHOT_TIMEOUT).await?;
        match response {
            DaemonResponse::RestackSnapshotCreated {
                descriptor,
                lowers,
                data_stat,
                ext4_used_bytes,
            } => Ok(RestackSnapshotResult {
                descriptor,
                lowers,
                data_stat,
                ext4_used_bytes,
            }),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Flush a live image after its owning VM has stopped issuing I/O.
    pub async fn flush_for_checkpoint(&self, dev_id: u32) -> Result<()> {
        let request = DaemonRequest::FlushForCheckpoint { dev_id };
        match self.call(request, SNAPSHOT_TIMEOUT).await? {
            DaemonResponse::Ok => Ok(()),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Stop the daemon after all normal device owners have released their resources.
    pub async fn shutdown(&self) -> Result<()> {
        let shutdown = abort_on_drop((), "daemon shutdown was cancelled");
        // Let an in-progress restart or recovery finish before shutting down its replacement.
        let _gate = tokio::time::timeout(SHUTDOWN_GATE_TIMEOUT, self.inner.gate.write())
            .await
            .unwrap_or_else(|_| {
                fatal_exit("timed out waiting for ublk daemon recovery at shutdown")
            });
        self.inner.shutting_down.store(true, Ordering::Release);
        let stopped = tokio::time::timeout(Duration::from_secs(10), async {
            let response = tokio::time::timeout(
                Duration::from_secs(5),
                self.inner
                    .call_inner(DaemonRequest::Shutdown, &mut false, false),
            )
            .await;
            // The daemon may close its socket before replying; process exit is decisive.
            if let Ok(Ok(response)) = response {
                if !matches!(response, DaemonResponse::Ok) {
                    fatal_exit(&format!("unexpected ublk response: {response:?}"));
                }
            }
            self.inner
                .daemon_exited
                .subscribe()
                .wait_for(|exited| *exited)
                .await
                .expect("client owns exit sender");
        })
        .await;
        if stopped.is_err() {
            fatal_exit("ublk daemon shutdown did not complete within 10 seconds");
        }
        shutdown.complete();
        Ok(())
    }

    /// Query daemon capabilities.
    ///
    /// Returns a bitmask of feature flags (e.g., `UBLK_F_UPDATE_SIZE`).
    pub async fn get_features(&self) -> Result<u64> {
        let request = DaemonRequest::GetFeatures;
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Features { flags } => Ok(flags),
            response => bail!("unexpected ublk response: {response:?}"),
        }
    }

    /// Acquire a warm overlaybd device from the pool.
    ///
    /// Returns `(dev_id, device_path, actual_image_config_path)`.
    pub async fn acquire_overlaybd(
        &self,
        image_config: &Path,
        global_config: &Path,
        virtual_size: Option<u64>,
        access_mode: AccessMode,
    ) -> Result<(u32, PathBuf, PathBuf)> {
        self.acquire_overlaybd_with_upper_access(
            image_config,
            global_config,
            virtual_size,
            access_mode,
            false,
        )
        .await
    }

    pub async fn acquire_overlaybd_with_upper_access(
        &self,
        image_config: &Path,
        global_config: &Path,
        virtual_size: Option<u64>,
        access_mode: AccessMode,
        read_only_upper: bool,
    ) -> Result<(u32, PathBuf, PathBuf)> {
        let request = DaemonRequest::AcquireOverlaybd {
            image_config: image_config.to_path_buf(),
            global_config: global_config.to_path_buf(),
            virtual_size,
            access_mode,
            read_only_upper,
        };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::DeviceAcquired {
                dev_id,
                device_path,
                image_config_path,
            } => Ok((dev_id, device_path, image_config_path)),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Release an overlaybd device back to the pool.
    pub async fn release_overlaybd(&self, dev_id: u32) -> Result<()> {
        let request = DaemonRequest::ReleaseOverlaybd { dev_id };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Released => Ok(()),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Update the virtual size of a ublk device.
    ///
    /// Requires `UBLK_F_UPDATE_SIZE` capability.
    pub async fn update_size(&self, dev_id: u32, new_sectors: u64) -> Result<()> {
        let request = DaemonRequest::UpdateSize {
            dev_id,
            new_sectors,
        };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::SizeUpdated => Ok(()),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Fail stop when a live daemon may still own backing files after an uncertain request.
    async fn call(&self, request: DaemonRequest, timeout: Duration) -> Result<DaemonResponse> {
        let inner = &self.inner;
        let best_effort = matches!(
            request,
            DaemonRequest::GetFeatures
                | DaemonRequest::NotifySandboxReady { .. }
                | DaemonRequest::StartPackRecording { .. }
                | DaemonRequest::PackRecordingStatus { .. }
                | DaemonRequest::AbortPackRecording { .. }
                | DaemonRequest::PrefetchStartupPack { .. }
        );
        let creating = matches!(
            request,
            DaemonRequest::CreateOverlaybd { .. }
                | DaemonRequest::CreateOverlaybdRuntimeDevice { .. }
                | DaemonRequest::AcquireOverlaybd { .. }
        );
        let releasing = matches!(
            request,
            DaemonRequest::Delete { .. } | DaemonRequest::ReleaseOverlaybd { .. }
        );
        let is_restack = matches!(request, DaemonRequest::RestackSnapshot { .. });

        let _gate = inner.gate.read().await;
        if inner.shutting_down.load(Ordering::Acquire) {
            if releasing {
                fatal_exit("device release attempted after daemon shutdown began");
            }
            bail!("ublk daemon is shutting down");
        }
        inner.ensure_alive()?;

        let update = RegistryUpdate::for_request(&request);
        let mut request_started = false;
        let result = tokio::time::timeout(
            timeout,
            inner.call_inner(request, &mut request_started, !best_effort),
        )
        .await
        .with_context(|| format!("ublk control request timed out after {timeout:?}"))
        .and_then(|result| result);
        let response = match result {
            Ok(response) => response,
            Err(error) if best_effort || (creating && !request_started) => return Err(error),
            Err(error) => {
                // The watchdog announces exit before it takes the write gate. Wait
                // here so callers can only release files after the old daemon is gone.
                let mut exited = inner.daemon_exited.subscribe();
                if !inner.recovery_enabled
                    || !matches!(
                        tokio::time::timeout(DETACH_TIMEOUT, exited.wait_for(|exited| *exited))
                            .await,
                        Ok(Ok(_))
                    )
                {
                    fatal_exit(&format!("ublk control connection failed: {error:#}"));
                }
                if let Some(update) = update {
                    update.apply(&inner.registry, None);
                }
                if is_restack && request_started {
                    return Err(RestackSnapshotTerminalFailure::new(format!(
                        "daemon: restack snapshot outcome is unknown: {error:#}"
                    ))
                    .into());
                }
                return Err(error);
            }
        };
        if let Some(update) = update {
            update.apply(&inner.registry, Some(&response));
        }
        match response {
            DaemonResponse::TerminalError { message } if is_restack => {
                Err(RestackSnapshotTerminalFailure::new(message).into())
            }
            DaemonResponse::TerminalError { message } if !best_effort => fatal_exit(&message),
            DaemonResponse::Error { message } | DaemonResponse::InvalidRequest { message }
                if releasing =>
            {
                fatal_exit(&message)
            }
            DaemonResponse::Error { message } | DaemonResponse::TerminalError { message } => {
                bail!("daemon: {message}")
            }
            DaemonResponse::InvalidRequest { message } => {
                Err(InvalidRequestError::new(message).into())
            }
            response => Ok(response),
        }
    }
}

impl UblkDaemonClientInner {
    async fn call_inner(
        &self,
        request: DaemonRequest,
        request_started: &mut bool,
        protect: bool,
    ) -> Result<DaemonResponse> {
        // Encoding cannot have changed daemon state, so invalid input remains an error.
        let payload = serde_json::to_vec(&request).context("encode daemon request")?;
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .with_context(|| {
                format!(
                    "connect to ublk daemon socket: {}",
                    self.socket_path.display()
                )
            })?;
        *request_started = true;
        let pending =
            protect.then(|| abort_on_drop((), "device RPC was cancelled before confirmation"));
        let response = async {
            stream
                .write_all(&(payload.len() as u32).to_be_bytes())
                .await?;
            stream.write_all(&payload).await?;
            recv_message(&mut stream)
                .await?
                .context("daemon closed connection without response")
        }
        .await;
        if let Some(pending) = pending {
            pending.complete();
        }
        response
    }

    fn ensure_alive(&self) -> Result<()> {
        if self.daemon_dead.load(Ordering::Acquire) {
            let reason = self.death_reason.lock().unwrap();
            bail!(
                "ublk daemon is not running: {}",
                reason.as_deref().unwrap_or("unknown reason")
            );
        }
        Ok(())
    }

    fn mark_dead(&self, reason: String) {
        *self.death_reason.lock().unwrap() = Some(reason);
        self.daemon_dead.store(true, Ordering::Release);
        self.daemon_exited.send_replace(true);
    }

    fn kill_child(&self) {
        let pid = self.child_pid.load(Ordering::Acquire);
        if pid == 0 {
            return;
        }
        if let Err(err) = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGKILL,
        ) {
            tracing::warn!(pid, %err, "failed to kill ublk daemon");
        }
    }

    /// Start a replacement daemon and have it recover every registered
    /// device. The caller holds `gate` exclusively and has seen the previous
    /// daemon exit.
    async fn respawn_and_recover(self: &Arc<Self>) -> Result<()> {
        let mut last_error = None;
        for attempt in 1..=RESPAWN_ATTEMPTS {
            if attempt > 1 {
                tokio::time::sleep(RESPAWN_BACKOFF * (attempt - 1)).await;
            }
            match self.respawn_and_recover_once().await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    tracing::warn!(
                        attempt,
                        error = %format!("{err:#}"),
                        "failed to replace ublk daemon"
                    );
                    last_error = Some(err);
                }
            }
        }
        Err(last_error.expect("at least one respawn attempt"))
    }

    async fn respawn_and_recover_once(self: &Arc<Self>) -> Result<()> {
        let command = self
            .command
            .as_ref()
            .context("ublk daemon was not spawned by this client")?;
        let child = UblkDaemonClient::spawn_daemon(command).await?;
        let mut exited = self.daemon_exited.subscribe();
        self.daemon_exited.send_replace(false);
        UblkDaemonClient::spawn_watchdog(Arc::clone(self), child);

        let devices: Vec<DeviceSpec> = self.registry.lock().unwrap().values().cloned().collect();
        let count = devices.len();
        let response = tokio::time::timeout(
            RECOVER_TIMEOUT,
            self.call_inner(DaemonRequest::Recover { devices }, &mut false, false),
        )
        .await;
        let failed = match response {
            Ok(Ok(DaemonResponse::Recovered { failed })) => failed,
            other => {
                // The new daemon may hold some devices; stop it so the next
                // attempt can recover all of them.
                self.kill_child();
                let _ = exited.wait_for(|exited| *exited).await;
                match other {
                    Ok(Ok(response)) => bail!("unexpected response to recover: {response:?}"),
                    Ok(Err(err)) => return Err(err).context("recover ublk devices"),
                    Err(_) => bail!("recovering ublk devices timed out after {RECOVER_TIMEOUT:?}"),
                }
            }
        };

        let mut registry = self.registry.lock().unwrap();
        for failure in &failed {
            registry.remove(&failure.dev_id);
            tracing::error!(
                dev_id = failure.dev_id,
                error = %failure.message,
                "ublk device was lost while replacing the daemon"
            );
        }
        tracing::info!(
            devices = count,
            failed = failed.len(),
            "replaced ublk daemon and recovered devices"
        );
        Ok(())
    }

    /// Give up on the daemon: fail further RPCs and delete every registered
    /// device, so I/O queued for a daemon that will never come back fails
    /// instead of hanging.
    async fn fail_permanently(&self, reason: String) {
        tracing::error!(%reason, "ublk daemon could not be replaced; deleting its devices");
        let dev_ids: Vec<u32> = self
            .registry
            .lock()
            .unwrap()
            .drain()
            .map(|(id, _)| id)
            .collect();
        self.mark_dead(reason);
        if dev_ids.is_empty() {
            return;
        }
        let (ring, _ring_thread) =
            storage_util::io_ring::spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        for dev_id in dev_ids {
            match uvm_ublk::delete_dev(ring.clone(), dev_id).await {
                Ok(()) => tracing::warn!(dev_id, "deleted ublk device of failed daemon"),
                Err(err) => tracing::error!(
                    dev_id,
                    error = %format!("{err:#}"),
                    "failed to delete ublk device of failed daemon"
                ),
            }
        }
    }
}

/// How an RPC changes the devices a client holds.
enum RegistryUpdate {
    /// Recorded once the daemon hands the device over. Acquired and runtime
    /// devices take their actual image config from the response.
    Add {
        image_config: PathBuf,
        global_config: PathBuf,
        read_only_upper: bool,
        role: DeviceRole,
    },
    /// Dropped whatever the outcome: the caller stops using the device. If
    /// the daemon still has it, a restart's sweep deletes it.
    Delete(u32),
    /// Like `Delete`, but a shared device keeps its other references.
    Release(u32),
}

impl RegistryUpdate {
    fn for_request(request: &DaemonRequest) -> Option<Self> {
        match request {
            DaemonRequest::CreateOverlaybd {
                image_config,
                global_config,
                read_only_upper,
            } => Some(Self::Add {
                image_config: image_config.clone(),
                global_config: global_config.clone(),
                read_only_upper: *read_only_upper,
                role: DeviceRole::Raw,
            }),
            DaemonRequest::CreateOverlaybdRuntimeDevice { global_config, .. } => Some(Self::Add {
                image_config: PathBuf::new(),
                global_config: global_config.clone(),
                read_only_upper: false,
                role: DeviceRole::Exclusive,
            }),
            DaemonRequest::AcquireOverlaybd {
                image_config,
                global_config,
                access_mode,
                read_only_upper,
                ..
            } => Some(Self::Add {
                image_config: image_config.clone(),
                global_config: global_config.clone(),
                read_only_upper: *read_only_upper,
                role: match access_mode {
                    AccessMode::Exclusive => DeviceRole::Exclusive,
                    AccessMode::Shared => DeviceRole::Shared { refcount: 1 },
                },
            }),
            DaemonRequest::Delete { dev_id } => Some(Self::Delete(*dev_id)),
            DaemonRequest::ReleaseOverlaybd { dev_id } => Some(Self::Release(*dev_id)),
            _ => None,
        }
    }

    fn apply(self, registry: &Mutex<HashMap<u32, DeviceSpec>>, response: Option<&DaemonResponse>) {
        let mut registry = registry.lock().unwrap();
        match self {
            Self::Add {
                image_config,
                global_config,
                read_only_upper,
                role,
            } => {
                let (dev_id, image_config) = match response {
                    Some(DaemonResponse::DeviceCreated { dev_id, .. }) => (*dev_id, image_config),
                    Some(DaemonResponse::DeviceAcquired {
                        dev_id,
                        image_config_path,
                        ..
                    }) => (*dev_id, image_config_path.clone()),
                    Some(DaemonResponse::OverlaybdRuntimeDeviceCreated {
                        dev_id,
                        runtime_image_config_path,
                        ..
                    }) => (*dev_id, runtime_image_config_path.clone()),
                    _ => return,
                };
                // A shared acquire of a device already held adds a reference.
                if let Some(DeviceSpec {
                    role: DeviceRole::Shared { refcount },
                    ..
                }) = registry.get_mut(&dev_id)
                {
                    *refcount += 1;
                    return;
                }
                registry.insert(
                    dev_id,
                    DeviceSpec {
                        dev_id,
                        image_config,
                        global_config,
                        read_only_upper,
                        role,
                    },
                );
            }
            Self::Delete(dev_id) => {
                registry.remove(&dev_id);
            }
            Self::Release(dev_id) => {
                if let Some(DeviceSpec {
                    role: DeviceRole::Shared { refcount },
                    ..
                }) = registry.get_mut(&dev_id)
                {
                    if *refcount > 1 {
                        *refcount -= 1;
                        return;
                    }
                }
                registry.remove(&dev_id);
            }
        }
    }
}

impl std::fmt::Debug for UblkDaemonClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UblkDaemonClient")
            .field("socket_path", &self.inner.socket_path)
            .field(
                "daemon_dead",
                &self.inner.daemon_dead.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl UblkDaemonClient {
    /// Test-only constructor that creates a client pointing at a given socket
    /// path without spawning a real daemon process. Useful for testing RPC
    /// logic against a mock server or verifying dead-daemon fast-fail.
    #[doc(hidden)]
    pub fn new_for_test(socket_path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(UblkDaemonClientInner {
                socket_path,
                daemon_dead: AtomicBool::new(false),
                daemon_exited: tokio::sync::watch::channel(true).0,
                shutting_down: AtomicBool::new(false),
                death_reason: Mutex::new(if daemon_dead {
                    Some("test: daemon marked dead".into())
                } else {
                    None
                }),
                runtime_device_timeout: Duration::from_secs(360),
                command: None,
                recovery_enabled: false,
                restarting: AtomicBool::new(false),
                child_pid: AtomicU32::new(0),
                generation: AtomicU64::new(0),
                registry: Mutex::new(HashMap::new()),
                gate: tokio::sync::RwLock::new(()),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::send_message;

    #[tokio::test]
    async fn shutdown_waits_for_child_exit_after_rpc_ack() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("shutdown.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
        let client = UblkDaemonClient::new_for_test(sock_path);
        // The child cannot exit until the test releases its stdin gate.
        client.inner.daemon_exited.send_replace(false);
        let mut child = tokio::process::Command::new("/bin/sh")
            .args(["-c", "read line"])
            .stdin(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut gate = child.stdin.take().unwrap();
        UblkDaemonClient::spawn_watchdog(client.inner.clone(), child);
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
            assert!(matches!(request, DaemonRequest::Shutdown));
            send_message(&mut stream, &DaemonResponse::Ok)
                .await
                .unwrap();
            ack_tx.send(()).unwrap();
        });
        let shutdown_client = client.clone();
        let mut shutdown = tokio::spawn(async move { shutdown_client.shutdown().await });
        ack_rx.await.unwrap();
        let returned_early = tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
            .await
            .is_ok();
        gate.write_all(b"exit\n").await.unwrap();
        if !returned_early {
            tokio::time::timeout(Duration::from_secs(3), shutdown)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert!(
            !returned_early,
            "shutdown returned before daemon cleanup/exit"
        );
        assert!(client.inner.daemon_dead.load(Ordering::Acquire));
        // Repeated shutdown must also observe the already-completed exit.
        client.shutdown().await.unwrap();
    }

    /// Serve each connection's single request with `handler`.
    fn serve(
        listener: tokio::net::UnixListener,
        handler: impl Fn(DaemonRequest) -> DaemonResponse + Send + Sync + 'static,
    ) {
        let handler = Arc::new(handler);
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    let request: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
                    send_message(&mut stream, &handler(request)).await.unwrap();
                });
            }
        });
    }

    fn registry(client: &UblkDaemonClient) -> Vec<DeviceSpec> {
        let mut specs: Vec<_> = client
            .inner
            .registry
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        specs.sort_by_key(|spec| spec.dev_id);
        specs
    }

    #[tokio::test]
    async fn registry_tracks_handed_over_devices() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("registry.sock");
        serve(
            tokio::net::UnixListener::bind(&socket).unwrap(),
            |request| {
                let acquired = |dev_id, image_config_path: &str| DaemonResponse::DeviceAcquired {
                    dev_id,
                    device_path: PathBuf::from(format!("/dev/ublkb{dev_id}")),
                    image_config_path: image_config_path.into(),
                };
                match request {
                    DaemonRequest::CreateOverlaybd { .. } => DaemonResponse::DeviceCreated {
                        dev_id: 1,
                        device_path: PathBuf::from("/dev/ublkb1"),
                    },
                    DaemonRequest::CreateOverlaybdRuntimeDevice { .. } => {
                        DaemonResponse::OverlaybdRuntimeDeviceCreated {
                            dev_id: 2,
                            device_path: PathBuf::from("/dev/ublkb2"),
                            actual_virtual_size: 4096,
                            runtime_image_config_path: PathBuf::from("/run/rootfs/image.json"),
                        }
                    }
                    DaemonRequest::AcquireOverlaybd {
                        access_mode: AccessMode::Exclusive,
                        ..
                    } => acquired(3, "/reused/x.json"),
                    DaemonRequest::AcquireOverlaybd {
                        access_mode: AccessMode::Shared,
                        ..
                    } => acquired(4, "/reused/mem.json"),
                    DaemonRequest::ReleaseOverlaybd { .. } => DaemonResponse::Released,
                    DaemonRequest::Delete { .. } => DaemonResponse::Deleted,
                    other => panic!("unexpected request {other:?}"),
                }
            },
        );
        let client = UblkDaemonClient::new_for_test(socket);
        let global = Path::new("/global.json");

        client
            .create_overlaybd_with_upper_access(Path::new("/raw.json"), global, true)
            .await
            .unwrap();
        client
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: Path::new("/source.json"),
                global_config: global,
                runtime_dir: Path::new("/run/rootfs"),
                read_only: false,
                runtime_upper_mode: UpperMode::LogStructured,
                requested_virtual_size: None,
                known_source_virtual_size: None,
                allow_shrink: false,
            })
            .await
            .unwrap();
        client
            .acquire_overlaybd(Path::new("/x.json"), global, None, AccessMode::Exclusive)
            .await
            .unwrap();
        for _ in 0..2 {
            client
                .acquire_overlaybd_with_upper_access(
                    Path::new("/mem.json"),
                    global,
                    None,
                    AccessMode::Shared,
                    true,
                )
                .await
                .unwrap();
        }

        let spec = |dev_id, image_config: &str, role, read_only_upper| DeviceSpec {
            dev_id,
            image_config: PathBuf::from(image_config),
            global_config: global.to_path_buf(),
            read_only_upper,
            role,
        };
        assert_eq!(
            registry(&client),
            vec![
                spec(1, "/raw.json", DeviceRole::Raw, true),
                spec(2, "/run/rootfs/image.json", DeviceRole::Exclusive, false),
                spec(3, "/reused/x.json", DeviceRole::Exclusive, false),
                spec(
                    4,
                    "/reused/mem.json",
                    DeviceRole::Shared { refcount: 2 },
                    true
                ),
            ]
        );

        client.release_overlaybd(4).await.unwrap();
        assert_eq!(
            registry(&client)[3].role,
            DeviceRole::Shared { refcount: 1 }
        );
        client.release_overlaybd(4).await.unwrap();
        client.release_overlaybd(3).await.unwrap();
        client.delete(1).await.unwrap();
        assert_eq!(
            registry(&client),
            vec![spec(
                2,
                "/run/rootfs/image.json",
                DeviceRole::Exclusive,
                false
            )]
        );
    }

    #[tokio::test]
    async fn registry_skips_failed_handoffs() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("registry.sock");
        serve(
            tokio::net::UnixListener::bind(&socket).unwrap(),
            |request| match request {
                DaemonRequest::AcquireOverlaybd { .. } => DaemonResponse::DeviceAcquired {
                    dev_id: 5,
                    device_path: PathBuf::from("/dev/ublkb5"),
                    image_config_path: PathBuf::from("/image.json"),
                },
                _ => DaemonResponse::Error {
                    message: "boom".to_string(),
                },
            },
        );
        let client = UblkDaemonClient::new_for_test(socket);
        let global = Path::new("/global.json");

        assert!(client
            .create_overlaybd(Path::new("/raw.json"), global)
            .await
            .is_err());
        assert!(registry(&client).is_empty());
    }

    #[tokio::test]
    async fn restart_requires_a_spawned_daemon() {
        let client = UblkDaemonClient::new_for_test(PathBuf::from("/tmp/none.sock"));
        let err = client.restart().await.unwrap_err();
        assert!(format!("{err:#}").contains("did not spawn"), "{err:#}");
    }

    #[tokio::test]
    async fn daemon_dead_fails_immediately() {
        let client = UblkDaemonClient::new_for_test(PathBuf::from("/nonexistent/test.sock"));
        client.inner.mark_dead("test daemon exit".into());

        let err = client
            .create_overlaybd(Path::new("/img.json"), Path::new("/global.json"))
            .await;
        assert!(err.is_err());
        let msg = format!("{:#}", err.unwrap_err());
        assert!(
            msg.contains("not running"),
            "expected 'not running' in error: {msg}"
        );

        let err = client.delete(0).await;
        assert!(err.is_err());

        let err = client.restack_snapshot(0, Path::new("/out"), None).await;
        assert!(err.is_err());
    }

    #[test]
    fn debug_format_shows_socket_and_status() {
        let client = UblkDaemonClient::new_for_test(PathBuf::from("/tmp/test.sock"));
        let debug = format!("{:?}", client);
        assert!(
            debug.contains("UblkDaemonClient"),
            "debug should contain struct name"
        );
        assert!(
            debug.contains("/tmp/test.sock"),
            "debug should contain socket path"
        );
        assert!(
            debug.contains("false"),
            "debug should contain daemon_dead status"
        );
    }

    #[tokio::test]
    async fn connection_refused_when_no_server() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("nonexistent.sock");
        let client = UblkDaemonClient::new_for_test(sock_path);

        let err = client
            .create_overlaybd(Path::new("/img.json"), Path::new("/global.json"))
            .await;
        assert!(err.is_err(), "should fail when no server is listening");
    }

    #[tokio::test]
    async fn request_failures_leave_the_client_healthy() -> Result<()> {
        let root = tempfile::tempdir()?;
        let socket = root.path().join("daemon.sock");
        let client = UblkDaemonClient::new_for_test(socket.clone());
        assert!(client
            .create_overlaybd(Path::new("image"), Path::new("global"))
            .await
            .is_err());
        let listener = tokio::net::UnixListener::bind(&socket)?;
        for response in [
            DaemonResponse::InvalidRequest {
                message: "invalid size".into(),
            },
            DaemonResponse::Error {
                message: "invalid image".into(),
            },
        ] {
            let invalid = matches!(response, DaemonResponse::InvalidRequest { .. });
            let server = async {
                let (mut stream, _) = listener.accept().await?;
                recv_message::<DaemonRequest>(&mut stream).await?;
                send_message(&mut stream, &response).await
            };
            let (result, served) = tokio::join!(
                client.create_overlaybd(Path::new("image"), Path::new("global")),
                server
            );
            served?;
            assert_eq!(result.unwrap_err().is::<InvalidRequestError>(), invalid);
        }
        let server = async {
            let (mut stream, _) = listener.accept().await?;
            recv_message::<DaemonRequest>(&mut stream).await?;
            send_message(
                &mut stream,
                &DaemonResponse::TerminalError {
                    message: "upper already sealed".into(),
                },
            )
            .await
        };
        let (result, served) =
            tokio::join!(client.restack_snapshot(1, Path::new("layer"), None), server);
        assert!(result.unwrap_err().is::<RestackSnapshotTerminalFailure>());
        served?;
        let server = async {
            let (mut stream, _) = listener.accept().await?;
            assert!(matches!(
                recv_message::<DaemonRequest>(&mut stream).await?,
                Some(DaemonRequest::GetFeatures)
            ));
            send_message(&mut stream, &DaemonResponse::Features { flags: 0 }).await
        };
        let (health, served) = tokio::join!(client.get_features(), server);
        assert_eq!(health?, 0);
        served?;
        assert!(client
            .call(DaemonRequest::GetFeatures, Duration::from_millis(10))
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn intentional_shutdown_waits_for_the_child_and_keeps_the_owner_alive() -> Result<()> {
        let root = tempfile::tempdir()?;
        let socket = root.path().join("daemon.sock");
        let listener = tokio::net::UnixListener::bind(&socket)?;
        let client = UblkDaemonClient::new_for_test(socket);
        let mut child = tokio::process::Command::new("sh")
            .args(["-c", "read line; exit 0"])
            .stdin(std::process::Stdio::piped())
            .spawn()?;
        let input = child.stdin.take();
        UblkDaemonClient::spawn_watchdog(Arc::clone(&client.inner), child);
        let server = async {
            let (mut stream, _) = listener.accept().await?;
            assert!(matches!(
                recv_message::<DaemonRequest>(&mut stream).await?,
                Some(DaemonRequest::Shutdown)
            ));
            drop(input);
            Ok::<_, anyhow::Error>(())
        };
        let (shutdown, served) = tokio::join!(client.shutdown(), server);
        shutdown?;
        served?;
        assert!(*client.inner.daemon_exited.borrow());
        Ok(())
    }

    #[test]
    fn control_faults_exit_before_backing_files_or_exit_hooks() -> Result<()> {
        const CASE: &str = "AENV_TEST_FATAL_RPC";
        const ROOT: &str = "AENV_TEST_FATAL_ROOT";
        if let Ok(case) = std::env::var(CASE) {
            extern "C" fn exit_hook() {
                let _ = std::fs::write(
                    PathBuf::from(std::env::var("AENV_TEST_FATAL_ROOT").unwrap()).join("exit-hook"),
                    b"ran",
                );
            }
            unsafe {
                libc::atexit(exit_hook);
            }
            let root = PathBuf::from(std::env::var(ROOT)?);
            let backing = tempfile::Builder::new()
                .prefix("backing")
                .rand_bytes(0)
                .tempdir_in(&root)?;
            std::fs::write(backing.path().join("data"), b"must survive")?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let socket = root.join("daemon.sock");
                let client = UblkDaemonClient::new_for_test(socket.clone());
                if case == "watchdog" {
                    let child = tokio::process::Command::new("sh")
                        .args(["-c", "exit 23"])
                        .spawn()?;
                    UblkDaemonClient::spawn_watchdog(Arc::clone(&client.inner), child);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    panic!("watchdog did not terminate owner");
                }
                let listener = tokio::net::UnixListener::bind(&socket)?;
                let operation = case.clone();
                let request = tokio::spawn(async move {
                    let _backing = backing;
                    match operation.as_str() {
                        "shutdown" => {
                            client.shutdown().await?;
                        }
                        "release" => {
                            client.release_overlaybd(12).await?;
                        }
                        "response" => {
                            client
                                .create_overlaybd(Path::new("image"), Path::new("global"))
                                .await?;
                        }
                        _ => {
                            client
                                .call(
                                    DaemonRequest::CreateOverlaybd {
                                        image_config: "image".into(),
                                        global_config: "global".into(),
                                        read_only_upper: false,
                                    },
                                    Duration::from_millis(100),
                                )
                                .await?;
                        }
                    }
                    panic!("fatal operation returned to its owner");
                    #[allow(unreachable_code)]
                    Ok::<_, anyhow::Error>(())
                });
                let (mut stream, _) = listener.accept().await?;
                recv_message::<DaemonRequest>(&mut stream).await?;
                match case.as_str() {
                    "disconnect" | "shutdown" => drop(stream),
                    "cancel" => {
                        request.abort();
                    }
                    "release" => {
                        send_message(
                            &mut stream,
                            &DaemonResponse::Error {
                                message: "device still active".into(),
                            },
                        )
                        .await?
                    }
                    "response" => send_message(&mut stream, &DaemonResponse::Deleted).await?,
                    "timeout" => {
                        tokio::time::sleep(Duration::from_secs(4)).await;
                    }
                    _ => unreachable!(),
                }
                request.await??;
                Ok::<_, anyhow::Error>(())
            })?;
            panic!("fatal case returned");
        }
        for case in [
            "disconnect",
            "timeout",
            "cancel",
            "response",
            "release",
            "watchdog",
            "shutdown",
        ] {
            let root = tempfile::tempdir()?;
            let output = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "client::tests::control_faults_exit_before_backing_files_or_exit_hooks",
                    "--nocapture",
                ])
                .env(CASE, case)
                .env(ROOT, root.path())
                .output()?;
            assert_eq!(
                output.status.code(),
                Some(70),
                "{case}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                std::fs::read(root.path().join("backing/data"))?,
                b"must survive",
                "{case}"
            );
            assert!(!root.path().join("exit-hook").exists(), "{case}");
        }
        Ok(())
    }
}
