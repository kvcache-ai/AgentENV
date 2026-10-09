use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use storage_util::{abort_on_drop, fatal_exit};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::{Duration, Instant};
use warm_pool::PoolConfig;

use crate::protocol::{
    recv_message, AccessMode, DaemonRequest, DaemonResponse, PackRecordingState,
    RestackSnapshotResult, SourceStateStrategy,
};
use overlaybd::{config::UpperMode, RestackSnapshotTerminalFailure};

/// Timeout for best-effort daemon RPC calls.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

const SLOW_REQUEST_WARNING_AFTER: Duration = Duration::from_secs(300);

/// Timeout for waiting for the daemon to signal readiness.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

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
/// Owns the daemon child process. Spawns a background watchdog thread that
/// monitors whether the daemon is alive. An unexpected exit terminates the
/// owner process before its destructors can remove device backing files.
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
}

struct UblkDaemonClientInner {
    socket_path: PathBuf,
    exited: tokio::sync::watch::Sender<bool>,
    /// Set to `true` when `shutdown()` is called, so the watchdog accepts a successful
    /// exit after all device owners have released their resources.
    shutting_down: AtomicBool,
}

impl UblkDaemonClient {
    /// Spawn the `uvm-ublk-daemon` process and wait for it to become ready.
    ///
    /// This is the primary constructor. It:
    /// 1. Spawns the daemon binary as a child process.
    /// 2. Waits for `"ready"` on stdout (with a 30s timeout).
    /// 3. Spawns a background watchdog thread that monitors the child process
    ///    and terminates the owner if it exits unexpectedly.
    ///
    /// Returns a shared client for device owners.
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
            "spawning ublk daemon"
        );

        Self::wait_for_socket_available(&config.socket_path).await?;

        let mut cmd = tokio::process::Command::new(config.binary_path);
        configure_daemon_capabilities(&mut cmd)?;
        cmd.arg("--socket-path")
            .arg(&config.socket_path)
            .arg("--global-config")
            .arg(config.global_config)
            .arg("--resize-global-config")
            .arg(config.resize_global_config)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());

        // Keep the daemon out of the server's foreground process group.
        // Otherwise a terminal Ctrl+C sends SIGINT to both agentenv and the
        // ublk daemon, interrupting snapshot/restack RPCs during graceful
        // shutdown.
        cmd.process_group(0);

        if let Some(path) = config.app_config {
            cmd.arg("--config").arg(path);
        }

        if let Some(path) = config.log_file {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).context("create parent dir for ublk daemon log")?;
            }
            cmd.arg("--log-file").arg(path);
        }

        cmd.arg("--metrics-listen-addr")
            .arg(config.metrics_listen_addr);
        if let Some(url) = config.p2p_publish_url {
            cmd.arg("--p2p-publish-url").arg(url);
        }

        if let Some(pool_config) = config.pool_config {
            if config.app_config.is_none() {
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
            .as_std_mut()
            .spawn()
            .with_context(|| format!("spawn ublk daemon: {}", config.binary_path.display()))?;

        // Wait for "ready" on stdout.
        let stdout = child
            .stdout
            .take()
            .context("ublk daemon child process has no stdout")?;
        Self::wait_for_ready(tokio::process::ChildStdout::from_std(stdout)?, &mut child).await?;

        let client = Arc::new(Self {
            inner: Arc::new(UblkDaemonClientInner {
                socket_path: config.socket_path,
                exited: tokio::sync::watch::channel(false).0,
                shutting_down: AtomicBool::new(false),
            }),
        });

        // Monitor daemon exit independently of the async runtime.
        Self::spawn_watchdog(client.inner.clone(), child);

        tracing::info!("ublk daemon ready");
        Ok(client)
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
        child: &mut std::process::Child,
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
                    let status = child.try_wait().ok().flatten();
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
            Ok(Err(e)) => Err(e),
            Err(_) => {
                let _ = child.kill();
                bail!("ublk daemon did not become ready within {READY_TIMEOUT:?}");
            }
        }
    }

    /// A blocking wait survives caller cancellation and Tokio runtime teardown.
    fn spawn_watchdog(inner: Arc<UblkDaemonClientInner>, mut child: std::process::Child) {
        std::thread::Builder::new()
            .name("ublk-watchdog".into())
            .spawn(move || {
                let status = child
                    .wait()
                    .unwrap_or_else(|error| fatal_exit(&format!("wait for ublk daemon: {error}")));
                if !inner.shutting_down.load(Ordering::Acquire) || !status.success() {
                    fatal_exit(&format!("ublk daemon exited unexpectedly: {status}"));
                }
                inner.exited.send_replace(true);
            })
            .unwrap_or_else(|error| fatal_exit(&format!("start ublk watchdog: {error}")));
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
        match self.call(request).await? {
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
        match self.call(request).await? {
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
        match self.call(request).await? {
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
        match self.call(request).await? {
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
        match self.call(request).await? {
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
        match self.call(request).await? {
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
        match self.call(request).await? {
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
        match self.call(request).await? {
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
        let response = self.call(request).await?;
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
        match self.call(request).await? {
            DaemonResponse::Ok => Ok(()),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Stop the daemon after all normal device owners have released their resources.
    pub async fn shutdown(&self) {
        let shutdown = abort_on_drop((), "daemon shutdown was cancelled");
        self.inner.shutting_down.store(true, Ordering::Release);
        let stopped = tokio::time::timeout(Duration::from_secs(10), async {
            // The daemon may close its socket before replying; process exit is decisive.
            match self.call(DaemonRequest::Shutdown).await {
                Ok(DaemonResponse::Ok) | Err(_) => {}
                Ok(response) => fatal_exit(&format!("unexpected ublk response: {response:?}")),
            }
            let mut exited = self.inner.exited.subscribe();
            exited
                .wait_for(|exited| *exited)
                .await
                .expect("client owns exit sender");
        })
        .await;
        if stopped.is_err() {
            fatal_exit("ublk daemon shutdown did not complete within 10 seconds");
        }
        shutdown.complete();
    }

    /// Query daemon capabilities.
    ///
    /// Returns a bitmask of feature flags (e.g., `UBLK_F_UPDATE_SIZE`).
    pub async fn get_features(&self) -> Result<u64> {
        let request = DaemonRequest::GetFeatures;
        match self.call(request).await? {
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
        match self.call(request).await? {
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
        match self.call(request).await? {
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
        match self.call(request).await? {
            DaemonResponse::SizeUpdated => Ok(()),
            response => fatal_exit(&format!("unexpected ublk response: {response:?}")),
        }
    }

    /// Fail stop on uncertain device changes; callers handle confirmed failures.
    async fn call(&self, request: DaemonRequest) -> Result<DaemonResponse> {
        let shutdown = matches!(request, DaemonRequest::Shutdown);
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
        if !shutdown && self.inner.shutting_down.load(Ordering::Acquire) {
            if releasing {
                fatal_exit("device release attempted after daemon shutdown began");
            }
            bail!("ublk daemon is shutting down");
        }
        // Encoding cannot have changed daemon state, so invalid input remains an error.
        let payload = serde_json::to_vec(&request).context("encode daemon request")?;
        let mut request_started = false;
        let exchange = async {
            let mut stream = UnixStream::connect(&self.inner.socket_path)
                .await
                .context("connect to ublk daemon")?;
            // A partial write can already leave the operation's outcome unknown.
            request_started = true;
            let response = async {
                stream
                    .write_all(&(payload.len() as u32).to_be_bytes())
                    .await?;
                stream.write_all(&payload).await?;
                recv_message(&mut stream)
                    .await?
                    .context("daemon closed without a response")
            };
            if shutdown || best_effort {
                response.await
            } else {
                let pending = abort_on_drop((), "device RPC was cancelled before confirmation");
                let response = response.await;
                pending.complete();
                response
            }
        };
        let result = if shutdown || best_effort {
            let timeout = if shutdown {
                Duration::from_secs(5)
            } else {
                DEFAULT_TIMEOUT
            };
            tokio::time::timeout(timeout, exchange)
                .await
                .context("ublk daemon request timed out")?
        } else {
            // Abandoning this wait cannot cancel daemon changes to backing files.
            let started = Instant::now();
            let mut exchange = std::pin::pin!(exchange);
            match tokio::time::timeout(SLOW_REQUEST_WARNING_AFTER, &mut exchange).await {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!(?request, elapsed = ?started.elapsed(), "ublk daemon request still pending");
                    exchange.await
                }
            }
        };
        let can_return = shutdown || best_effort || (creating && !request_started);
        let response = match result {
            Ok(response) => response,
            Err(error) if can_return => return Err(error),
            Err(error) => fatal_exit(&format!("ublk control connection failed: {error:#}")),
        };
        match response {
            DaemonResponse::TerminalError { message }
                if matches!(request, DaemonRequest::RestackSnapshot { .. }) =>
            {
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

impl std::fmt::Debug for UblkDaemonClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UblkDaemonClient")
            .field("socket_path", &self.inner.socket_path)
            .field(
                "shutting_down",
                &self.inner.shutting_down.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl UblkDaemonClient {
    /// Test-only constructor that creates a client pointing at a given socket
    /// path without spawning a real daemon process. Useful for testing RPC
    /// logic against a mock server without starting a daemon process.
    #[doc(hidden)]
    pub fn new_for_test(socket_path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(UblkDaemonClientInner {
                socket_path,
                exited: tokio::sync::watch::channel(false).0,
                shutting_down: AtomicBool::new(false),
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
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("shutdown.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
        let client = UblkDaemonClient::new_for_test(sock_path);
        // The child cannot exit until the test releases its stdin gate.
        client.inner.exited.send_replace(false);
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "read line"])
            .stdin(std::process::Stdio::piped())
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
        gate.write_all(b"exit\n").unwrap();
        if !returned_early {
            tokio::time::timeout(Duration::from_secs(3), shutdown)
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            !returned_early,
            "shutdown returned before daemon cleanup/exit"
        );
        assert!(*client.inner.exited.borrow());
        // Repeated shutdown must also observe the already-completed exit.
        client.shutdown().await;
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
        Ok(())
    }

    #[tokio::test]
    async fn intentional_shutdown_waits_for_the_child_and_keeps_the_owner_alive() -> Result<()> {
        let root = tempfile::tempdir()?;
        let socket = root.path().join("daemon.sock");
        let listener = tokio::net::UnixListener::bind(&socket)?;
        let client = UblkDaemonClient::new_for_test(socket);
        let mut child = std::process::Command::new("sh")
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
        let ((), served) = tokio::join!(client.shutdown(), server);
        served?;
        assert!(*client.inner.exited.borrow());
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
                    let child = std::process::Command::new("sh")
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
                            client.shutdown().await;
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
                                .call(DaemonRequest::CreateOverlaybd {
                                    image_config: "image".into(),
                                    global_config: "global".into(),
                                    read_only_upper: false,
                                })
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
                    _ => unreachable!(),
                }
                request.await??;
                Ok::<_, anyhow::Error>(())
            })?;
            panic!("fatal case returned");
        }
        for case in [
            "disconnect",
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
