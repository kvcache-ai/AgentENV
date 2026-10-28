use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use tokio::io::AsyncBufReadExt;
use tokio::net::UnixStream;
use tokio::time::{Duration, Instant};
use warm_pool::PoolConfig;

use crate::protocol::{
    recv_message, send_message, AccessMode, DaemonRequest, DaemonResponse, DeviceRole, DeviceSpec,
    PackRecordingState, RestackSnapshotStats,
};
use overlaybd::config::UpperMode;

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

#[derive(Debug)]
pub struct RestackSnapshotTerminalFailure {
    message: String,
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

impl RestackSnapshotTerminalFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RestackSnapshotTerminalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RestackSnapshotTerminalFailure {}

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
            if inner.shutting_down.load(Ordering::Acquire)
                || !inner.recovery_enabled
                || inner.command.is_none()
            {
                inner.mark_dead(reason);
                return;
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
        match tokio::time::timeout(DETACH_TIMEOUT, inner.call_inner(DaemonRequest::Detach)).await {
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
        let request = DaemonRequest::CreateOverlaybd {
            image_config: image_config.to_path_buf(),
            global_config: global_config.to_path_buf(),
        };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::DeviceCreated {
                dev_id,
                device_path,
            } => Ok((dev_id, device_path)),
            DaemonResponse::TerminalError { message } => {
                bail!("daemon: create overlaybd failed terminally: {message}")
            }
            DaemonResponse::Error { message } => {
                bail!("daemon: create overlaybd failed: {message}")
            }
            other => bail!("daemon: unexpected response for create overlaybd: {other:?}"),
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
            DaemonResponse::TerminalError { message } => {
                bail!("daemon: notify sandbox ready failed terminally: {message}")
            }
            DaemonResponse::Error { message } => {
                bail!("daemon: notify sandbox ready failed: {message}")
            }
            other => bail!("daemon: unexpected response for notify sandbox ready: {other:?}"),
        }
    }

    /// Arm a startup-pack first-touch recorder on `dev_id`. When the
    /// recording window ends the daemon packages the recorded pages into
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
    ///
    /// This is the sandbox rootfs/extra-drive path. The daemon owns runtime
    /// materialization and returns the generated runtime image config path.
    pub async fn create_overlaybd_runtime_device(
        &self,
        request: CreateOverlaybdRuntimeDeviceRequest<'_>,
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
            DaemonResponse::TerminalError { message } => {
                bail!("daemon: create overlaybd runtime device failed terminally: {message}")
            }
            DaemonResponse::InvalidRequest { message } => {
                Err(InvalidRequestError::new(message).into())
            }
            DaemonResponse::Error { message } => {
                bail!("daemon: create overlaybd runtime device failed: {message}")
            }
            other => {
                bail!("daemon: unexpected response for create overlaybd runtime device: {other:?}")
            }
        }
    }

    /// Delete a ublk device.
    pub async fn delete(&self, dev_id: u32) -> Result<()> {
        let request = DaemonRequest::Delete { dev_id };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Deleted => Ok(()),
            DaemonResponse::TerminalError { message } => {
                bail!("daemon: delete dev_id={dev_id} failed terminally: {message}")
            }
            DaemonResponse::Error { message } => {
                bail!("daemon: delete dev_id={dev_id} failed: {message}")
            }
            other => bail!("daemon: unexpected response for delete: {other:?}"),
        }
    }

    /// Request a restack-style snapshot of an overlaybd device's upper layer.
    pub async fn restack_snapshot(
        &self,
        dev_id: u32,
        output_layer_path: &Path,
    ) -> Result<RestackSnapshotStats> {
        let request = DaemonRequest::RestackSnapshot {
            dev_id,
            output_layer_path: output_layer_path.to_path_buf(),
        };
        // A lost reply can follow a successful seal. Only an explicit Error
        // response establishes that the runtime is still safe to resume.
        let response = self.call(request, SNAPSHOT_TIMEOUT).await.context(
            RestackSnapshotTerminalFailure::new(format!(
                "daemon: restack snapshot dev_id={dev_id} outcome unknown"
            )),
        )?;
        match response {
            DaemonResponse::RestackSnapshotCreated {
                descriptor,
                data_stat,
                ext4_used_bytes,
            } => Ok(RestackSnapshotStats {
                descriptor,
                data_stat,
                ext4_used_bytes,
            }),
            DaemonResponse::TerminalError { message } => Err(
                RestackSnapshotTerminalFailure::new(format!(
                    "daemon: restack snapshot dev_id={dev_id} failed after mutating live state: {message}"
                ))
                .into(),
            ),
            DaemonResponse::Error { message } => {
                bail!("daemon: restack snapshot dev_id={dev_id} failed: {message}")
            }
            other => Err(RestackSnapshotTerminalFailure::new(format!(
                "daemon: unexpected response for restack snapshot: {other:?}"
            ))
            .into()),
        }
    }

    /// Request graceful daemon shutdown.
    ///
    /// Signals the watchdog that this is an expected exit so it does not
    /// log at error level.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner.shutting_down.store(true, Ordering::Release);
        // Let an in-progress restart or recovery finish, so the daemon it
        // leaves running is the one that shuts down.
        let _gate = tokio::time::timeout(SHUTDOWN_GATE_TIMEOUT, self.inner.gate.write())
            .await
            .ok();
        // Best-effort: daemon may close connection before responding.
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            self.inner.call_inner(DaemonRequest::Shutdown),
        )
        .await;
        self.inner
            .daemon_exited
            .subscribe()
            .wait_for(|exited| *exited)
            .await
            .context("wait for ublk daemon exit")?;
        Ok(())
    }

    /// Query daemon capabilities.
    ///
    /// Returns a bitmask of feature flags (e.g., `UBLK_F_UPDATE_SIZE`).
    pub async fn get_features(&self) -> Result<u64> {
        let request = DaemonRequest::GetFeatures;
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Features { flags } => Ok(flags),
            DaemonResponse::Error { message } => {
                bail!("daemon: get features failed: {message}")
            }
            other => bail!("daemon: unexpected response for get features: {other:?}"),
        }
    }

    /// Acquire a warm overlaybd device from the pool.
    ///
    /// Returns `(dev_id, device_path)`.
    pub async fn acquire_overlaybd(
        &self,
        image_config: &Path,
        global_config: &Path,
        virtual_size: Option<u64>,
        access_mode: AccessMode,
    ) -> Result<(u32, PathBuf)> {
        let request = DaemonRequest::AcquireOverlaybd {
            image_config: image_config.to_path_buf(),
            global_config: global_config.to_path_buf(),
            virtual_size,
            access_mode,
        };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::DeviceAcquired {
                dev_id,
                device_path,
            } => Ok((dev_id, device_path)),
            DaemonResponse::TerminalError { message } => {
                bail!("daemon: acquire overlaybd failed terminally: {message}")
            }
            DaemonResponse::Error { message } => {
                bail!("daemon: acquire overlaybd failed: {message}")
            }
            other => bail!("daemon: unexpected response for acquire overlaybd: {other:?}"),
        }
    }

    /// Release an overlaybd device back to the pool.
    pub async fn release_overlaybd(&self, dev_id: u32) -> Result<()> {
        let request = DaemonRequest::ReleaseOverlaybd { dev_id };
        match self.call(request, DEFAULT_TIMEOUT).await? {
            DaemonResponse::Released => Ok(()),
            DaemonResponse::TerminalError { message } => {
                bail!("daemon: release overlaybd dev_id={dev_id} failed terminally: {message}")
            }
            DaemonResponse::Error { message } => {
                bail!("daemon: release overlaybd dev_id={dev_id} failed: {message}")
            }
            other => bail!("daemon: unexpected response for release overlaybd: {other:?}"),
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
            DaemonResponse::TerminalError { message } => {
                bail!("daemon: update size dev_id={dev_id} failed terminally: {message}")
            }
            DaemonResponse::Error { message } => {
                bail!("daemon: update size dev_id={dev_id} failed: {message}")
            }
            other => bail!("daemon: unexpected response for update size: {other:?}"),
        }
    }

    /// Check if the daemon is alive, then send a request and wait for a response.
    async fn call(&self, request: DaemonRequest, timeout: Duration) -> Result<DaemonResponse> {
        // Fail fast if daemon is dead.
        self.inner.ensure_alive()?;

        let inner = &self.inner;
        let result = tokio::time::timeout(timeout, async {
            let _gate = inner.gate.read().await;
            // A failed restart may have given up on the daemon meanwhile.
            inner.ensure_alive()?;
            let update = RegistryUpdate::for_request(&request);
            let response = inner.call_inner(request).await;
            if let Some(update) = update {
                update.apply(&inner.registry, response.as_ref().ok());
            }
            response
        })
        .await;
        match result {
            Ok(inner) => inner,
            Err(_) => bail!(
                "daemon RPC timed out after {timeout:?} (socket={})",
                self.inner.socket_path.display()
            ),
        }
    }
}

impl UblkDaemonClientInner {
    async fn call_inner(&self, request: DaemonRequest) -> Result<DaemonResponse> {
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .with_context(|| {
                format!(
                    "connect to ublk daemon socket: {}",
                    self.socket_path.display()
                )
            })?;
        send_message(&mut stream, &request)
            .await
            .context("send daemon request")?;
        let response: DaemonResponse = recv_message(&mut stream)
            .await
            .context("receive daemon response")?
            .context("daemon closed connection without response")?;
        Ok(response)
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
            self.call_inner(DaemonRequest::Recover { devices }),
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
    /// Recorded once the daemon hands the device over. Runtime devices take
    /// their image config from the response.
    Add {
        image_config: PathBuf,
        global_config: PathBuf,
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
            } => Some(Self::Add {
                image_config: image_config.clone(),
                global_config: global_config.clone(),
                role: DeviceRole::Raw,
            }),
            DaemonRequest::CreateOverlaybdRuntimeDevice { global_config, .. } => Some(Self::Add {
                image_config: PathBuf::new(),
                global_config: global_config.clone(),
                role: DeviceRole::Exclusive,
            }),
            DaemonRequest::AcquireOverlaybd {
                image_config,
                global_config,
                access_mode,
                ..
            } => Some(Self::Add {
                image_config: image_config.clone(),
                global_config: global_config.clone(),
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
                role,
            } => {
                let (dev_id, image_config) = match response {
                    Some(
                        DaemonResponse::DeviceCreated { dev_id, .. }
                        | DaemonResponse::DeviceAcquired { dev_id, .. },
                    ) => (*dev_id, image_config),
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
    pub fn new_for_test(socket_path: PathBuf, daemon_dead: bool) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(UblkDaemonClientInner {
                socket_path,
                daemon_dead: AtomicBool::new(daemon_dead),
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

    #[tokio::test]
    async fn shutdown_waits_for_child_exit_after_rpc_ack() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("shutdown.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
        let client = UblkDaemonClient::new_for_test(sock_path, false);
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
                let acquired = |dev_id| DaemonResponse::DeviceAcquired {
                    dev_id,
                    device_path: PathBuf::from(format!("/dev/ublkb{dev_id}")),
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
                    } => acquired(3),
                    DaemonRequest::AcquireOverlaybd {
                        access_mode: AccessMode::Shared,
                        ..
                    } => acquired(4),
                    DaemonRequest::ReleaseOverlaybd { .. } => DaemonResponse::Released,
                    DaemonRequest::Delete { .. } => DaemonResponse::Deleted,
                    other => panic!("unexpected request {other:?}"),
                }
            },
        );
        let client = UblkDaemonClient::new_for_test(socket, false);
        let global = Path::new("/global.json");

        client
            .create_overlaybd(Path::new("/raw.json"), global)
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
                .acquire_overlaybd(Path::new("/mem.json"), global, None, AccessMode::Shared)
                .await
                .unwrap();
        }

        let spec = |dev_id, image_config: &str, role| DeviceSpec {
            dev_id,
            image_config: PathBuf::from(image_config),
            global_config: global.to_path_buf(),
            role,
        };
        assert_eq!(
            registry(&client),
            vec![
                spec(1, "/raw.json", DeviceRole::Raw),
                spec(2, "/run/rootfs/image.json", DeviceRole::Exclusive),
                spec(3, "/x.json", DeviceRole::Exclusive),
                spec(4, "/mem.json", DeviceRole::Shared { refcount: 2 }),
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
            vec![spec(2, "/run/rootfs/image.json", DeviceRole::Exclusive)]
        );
    }

    #[tokio::test]
    async fn registry_skips_failed_handoffs_and_drops_failed_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("registry.sock");
        serve(
            tokio::net::UnixListener::bind(&socket).unwrap(),
            |request| match request {
                DaemonRequest::AcquireOverlaybd { .. } => DaemonResponse::DeviceAcquired {
                    dev_id: 5,
                    device_path: PathBuf::from("/dev/ublkb5"),
                },
                _ => DaemonResponse::Error {
                    message: "boom".to_string(),
                },
            },
        );
        let client = UblkDaemonClient::new_for_test(socket, false);
        let global = Path::new("/global.json");

        assert!(client
            .create_overlaybd(Path::new("/raw.json"), global)
            .await
            .is_err());
        assert!(registry(&client).is_empty());

        client
            .acquire_overlaybd(Path::new("/x.json"), global, None, AccessMode::Exclusive)
            .await
            .unwrap();
        // The caller stops using a device whose delete failed, so a restart
        // must not recover it.
        assert!(client.delete(5).await.is_err());
        assert!(registry(&client).is_empty());
    }

    #[tokio::test]
    async fn restart_requires_a_spawned_daemon() {
        let client = UblkDaemonClient::new_for_test(PathBuf::from("/tmp/none.sock"), false);
        let err = client.restart().await.unwrap_err();
        assert!(format!("{err:#}").contains("did not spawn"), "{err:#}");
    }

    #[tokio::test]
    async fn daemon_dead_fails_immediately() {
        let client = UblkDaemonClient::new_for_test(
            PathBuf::from("/nonexistent/test.sock"),
            true, // daemon marked dead
        );

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

        let err = client.restack_snapshot(0, Path::new("/out")).await;
        assert!(err.is_err());
    }

    #[test]
    fn debug_format_shows_socket_and_status() {
        let client = UblkDaemonClient::new_for_test(PathBuf::from("/tmp/test.sock"), false);
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
        let client = UblkDaemonClient::new_for_test(sock_path, false);

        let err = client
            .create_overlaybd(Path::new("/img.json"), Path::new("/global.json"))
            .await;
        assert!(err.is_err(), "should fail when no server is listening");
    }

    #[tokio::test]
    async fn restack_lost_reply_is_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("restack.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let client = UblkDaemonClient::new_for_test(socket, false);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert!(matches!(
                recv_message::<DaemonRequest>(&mut stream).await.unwrap(),
                Some(DaemonRequest::RestackSnapshot { .. })
            ));
            // Consume the request, then close without replying: the client
            // cannot know whether the live upper was already sealed.
        });

        let error = client
            .restack_snapshot(0, Path::new("/snapshot.commit"))
            .await
            .unwrap_err();
        server.await.unwrap();
        assert!(error.is::<RestackSnapshotTerminalFailure>(), "{error:#}");
    }

    #[tokio::test]
    async fn shutdown_sets_shutting_down_flag() {
        // Create a mock server that accepts one connection and returns nothing,
        // to simulate the daemon receiving shutdown and closing.
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("shutdown.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let client = UblkDaemonClient::new_for_test(sock_path, false);

        // Accept connection in background (shutdown is best-effort).
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Read the request, don't send response — simulating daemon closing.
            let _: Option<DaemonRequest> =
                crate::protocol::recv_message(&mut stream).await.unwrap();
        });

        // shutdown() should not return an error even if the daemon doesn't respond.
        let result = client.shutdown().await;
        assert!(result.is_ok());
        assert!(client.inner.shutting_down.load(Ordering::Acquire));
    }
}
