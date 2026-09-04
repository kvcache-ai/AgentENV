use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use tokio::io::AsyncBufReadExt;
use tokio::net::UnixStream;
use tokio::time::{Duration, Instant};
use warm_pool::PoolConfig;

use crate::protocol::{
    recv_message, send_message, AccessMode, DaemonRequest, DaemonResponse, RestackSnapshotStats,
};
use overlaybd::config::UpperMode;

/// Default timeout for daemon RPC calls.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for snapshot operations (may be slow for large images).
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(360);

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
}

struct UblkDaemonClientInner {
    socket_path: PathBuf,
    /// Set to `true` when the daemon process exits (expected or unexpected).
    daemon_dead: AtomicBool,
    /// Set to `true` when `shutdown()` is called, so the watchdog task
    /// doesn't log the expected exit as an error.
    shutting_down: AtomicBool,
    /// Populated with the exit status description when the daemon dies.
    death_reason: Mutex<Option<String>>,
    runtime_device_timeout: Duration,
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
            .spawn()
            .with_context(|| format!("spawn ublk daemon: {}", config.binary_path.display()))?;

        // Wait for "ready" on stdout.
        let stdout = child
            .stdout
            .take()
            .context("ublk daemon child process has no stdout")?;
        Self::wait_for_ready(stdout, &mut child).await?;

        let client = Arc::new(Self {
            inner: Arc::new(UblkDaemonClientInner {
                socket_path: config.socket_path,
                daemon_dead: AtomicBool::new(false),
                shutting_down: AtomicBool::new(false),
                death_reason: Mutex::new(None),
                runtime_device_timeout: config.runtime_device_timeout,
            }),
        });

        // Spawn the watchdog task to monitor the child process.
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
            Ok(Err(e)) => Err(e),
            Err(_) => {
                let _ = child.kill().await;
                bail!("ublk daemon did not become ready within {READY_TIMEOUT:?}");
            }
        }
    }

    /// Spawn a background task that waits for the child process to exit.
    ///
    /// When the daemon dies, the task sets `daemon_dead` so that subsequent
    /// RPC calls fail immediately instead of trying to connect to a dead socket.
    fn spawn_watchdog(inner: Arc<UblkDaemonClientInner>, mut child: tokio::process::Child) {
        tokio::spawn(async move {
            let status = child.wait().await;
            let is_shutdown = inner.shutting_down.load(Ordering::Acquire);

            let reason = match &status {
                Ok(exit) => format!("ublk daemon exited: {exit}"),
                Err(err) => format!("ublk daemon wait failed: {err}"),
            };

            if is_shutdown {
                tracing::info!(%reason, "ublk daemon stopped (expected shutdown)");
            } else {
                tracing::error!(%reason, "ublk daemon exited unexpectedly");
            }

            *inner.death_reason.lock().unwrap() = Some(reason);
            inner.daemon_dead.store(true, Ordering::Release);
        });
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
        match self.call(request, SNAPSHOT_TIMEOUT).await? {
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
            other => bail!("daemon: unexpected response for restack snapshot: {other:?}"),
        }
    }

    /// Request graceful daemon shutdown.
    ///
    /// Signals the watchdog that this is an expected exit so it does not
    /// log at error level.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner.shutting_down.store(true, Ordering::Release);
        // Best-effort: daemon may close connection before responding.
        let _ = self
            .call(DaemonRequest::Shutdown, Duration::from_secs(5))
            .await;
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
        virtual_size: u64,
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
        if self.inner.daemon_dead.load(Ordering::Acquire) {
            let reason = self.inner.death_reason.lock().unwrap();
            bail!(
                "ublk daemon is not running: {}",
                reason.as_deref().unwrap_or("unknown reason")
            );
        }

        let result = tokio::time::timeout(timeout, self.call_inner(request)).await;
        match result {
            Ok(inner) => inner,
            Err(_) => bail!(
                "daemon RPC timed out after {timeout:?} (socket={})",
                self.inner.socket_path.display()
            ),
        }
    }

    async fn call_inner(&self, request: DaemonRequest) -> Result<DaemonResponse> {
        let mut stream = UnixStream::connect(&self.inner.socket_path)
            .await
            .with_context(|| {
                format!(
                    "connect to ublk daemon socket: {}",
                    self.inner.socket_path.display()
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
                shutting_down: AtomicBool::new(false),
                death_reason: Mutex::new(if daemon_dead {
                    Some("test: daemon marked dead".into())
                } else {
                    None
                }),
                runtime_device_timeout: Duration::from_secs(360),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
