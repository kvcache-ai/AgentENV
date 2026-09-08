use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{bail, Context, Result};
use firecracker_client::models::drive::IoEngine;
use firecracker_client::models::instance_action_info::ActionType;
use firecracker_client::models::vm::State as VmState;
use firecracker_client::models::{mmds_config::Version as MmdsVersion, MmdsConfig, PartialDrive};
use firecracker_client::models::{
    Balloon, BootSource, DirtyMemoryRanges, Drive, InstanceActionInfo, Logger,
    MachineConfiguration, MemoryBackend, NetworkInterface, NetworkOverride, RateLimiter,
    SnapshotCreateParams, SnapshotLoadParams, Vm,
};
use hyper::Method;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use serde_json::value::RawValue;
use tokio::process::{Child, Command};
use tokio::time::{self, Duration, Instant};
use tracing::{debug, trace, warn};

use super::mmds::MmdsMetadata;
use super::socket::UnixSocketClient;

/// Maximum size (in bytes) of the MMDS data store. The Firecracker default is
/// 51200 (50 KiB); we raise it to 1 MiB to accommodate raw image configs
/// injected via the extra MMDS metadata pass-through.
const MMDS_SIZE_LIMIT: usize = 1_048_576;
const HTTP_API_MAX_PAYLOAD_SIZE: usize = MMDS_SIZE_LIMIT;

pub(crate) struct FirecrackerInstance {
    client: UnixSocketClient,
    work_dir: PathBuf,
    socket_path: PathBuf,
    stderr_path: Option<PathBuf>,
    process: Option<Child>,
}

impl FirecrackerInstance {
    pub fn new(work_dir: PathBuf) -> Self {
        let socket_path = work_dir.join("firecracker.socket");
        Self {
            client: UnixSocketClient::new(socket_path.clone()),
            work_dir,
            socket_path,
            stderr_path: None,
            process: None,
        }
    }

    pub fn pid(&self) -> Result<Pid> {
        let raw_pid = self
            .process
            .as_ref()
            .and_then(|child| child.id())
            .context("firecracker process is not running")?;
        let raw_pid = i32::try_from(raw_pid).context("firecracker pid does not fit in i32")?;
        Ok(Pid::from_raw(raw_pid))
    }

    /// Return the PID only if the owned Firecracker child has not exited.
    pub fn running_pid(&mut self) -> Result<Pid> {
        let child = self
            .process
            .as_mut()
            .context("firecracker process is not running")?;
        if let Some(status) = child
            .try_wait()
            .context("failed to check Firecracker process status")?
        {
            bail!("firecracker process exited with status {status}");
        }
        self.pid()
    }

    pub async fn spawn_with_netns(
        &mut self,
        firecracker_binary: &Path,
        stdout_path: Option<&Path>,
        stderr_path: Option<&Path>,
        netns: Option<&Path>,
    ) -> Result<()> {
        if self.process.is_some() {
            bail!("firecracker process already started");
        }

        if self.socket_path.exists() {
            let _ = fs::remove_file(&self.socket_path);
        }

        let firecracker_binary = fs::canonicalize(firecracker_binary)
            .unwrap_or_else(|_| firecracker_binary.to_path_buf());
        trace!(
            binary = %firecracker_binary.display(),
            socket_path = %self.socket_path.display(),
            netns = ?netns,
            "spawning firecracker process"
        );

        let netns = netns
            .map(|netns_path| {
                fs::File::open(netns_path).with_context(|| {
                    format!(
                        "open Firecracker network namespace {}",
                        netns_path.display()
                    )
                })
            })
            .transpose()?;

        let mut cmd = Command::new(&firecracker_binary);
        cmd.process_group(0);

        cmd.arg("--api-sock")
            .arg(&self.socket_path)
            .arg("--mmds-size-limit")
            .arg(MMDS_SIZE_LIMIT.to_string())
            .arg("--http-api-max-payload-size")
            .arg(HTTP_API_MAX_PAYLOAD_SIZE.to_string())
            .stdin(Stdio::null());

        // Set Current Working Directory to the workspace
        // This is crucial for relative paths to work
        cmd.current_dir(&self.work_dir);

        let stdout = open_log_stdio(stdout_path)?;
        let stderr = open_log_stdio(stderr_path)?;
        self.stderr_path = stderr_path.map(Path::to_path_buf);

        cmd.stdout(stdout).stderr(stderr);

        let child = crate::privileges::spawn_tokio_command_scoped(cmd, &[], move || {
            if let Some(netns) = netns {
                // SAFETY: `netns` is an open network namespace descriptor and
                // this short-lived launcher thread has not spawned a child yet.
                let rc = unsafe { libc::setns(netns.as_raw_fd(), libc::CLONE_NEWNET) };
                if rc != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        })
        .await
        .context("failed to spawn firecracker")?;
        trace!("firecracker process spawned");

        // Set oom_score_adj to maximum (1000) so the firecracker process is
        // the first candidate for OOM kill, protecting current agentenv server.
        if let Some(pid) = child.id() {
            let oom_path = format!("/proc/{pid}/oom_score_adj");
            if let Err(e) = fs::write(&oom_path, b"1000") {
                warn!(pid, oom_path, err = %e, "failed to set oom_score_adj for firecracker");
            }
        }

        self.process = Some(child);
        Ok(())
    }

    /// Waits for the Firecracker API socket to become available.
    pub async fn wait_for_ready(
        &mut self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<()> {
        trace!(
            socket_path = %self.socket_path.display(),
            timeout_ms = timeout.as_millis(),
            poll_interval_ms = poll_interval.as_millis(),
            "waiting for firecracker api socket"
        );
        let start = Instant::now();
        while !self.socket_path.exists() {
            if let Some(process) = self.process.as_mut() {
                if let Some(status) = process
                    .try_wait()
                    .context("check firecracker process status")?
                {
                    bail!(
                        "firecracker exited before its API socket appeared at {} (status: {status}); stderr: {}",
                        self.socket_path.display(),
                        self.stderr_summary()
                    );
                }
            }
            if start.elapsed() > timeout {
                bail!(
                    "firecracker api socket did not appear at {} within {} ms; stderr: {}",
                    self.socket_path.display(),
                    timeout.as_millis(),
                    self.stderr_summary()
                );
            }
            time::sleep(poll_interval).await;
        }
        trace!(socket_path = %self.socket_path.display(), "firecracker api socket is ready");
        Ok(())
    }

    fn stderr_summary(&self) -> String {
        let Some(path) = self.stderr_path.as_deref() else {
            return "not captured".to_string();
        };
        match Self::read_stderr_tail(path) {
            Ok(stderr) if stderr.is_empty() => "empty".to_string(),
            Ok(stderr) => stderr,
            Err(err) => format!("unavailable ({err})"),
        }
    }

    fn read_stderr_tail(path: &Path) -> std::io::Result<String> {
        const MAX_BYTES: u64 = 4096;

        let mut file = fs::File::open(path)?;
        let bytes_to_read = file.metadata()?.len().min(MAX_BYTES);
        file.seek(SeekFrom::End(-(bytes_to_read as i64)))?;
        let mut bytes = Vec::with_capacity(bytes_to_read as usize);
        file.read_to_end(&mut bytes)?;
        Ok(String::from_utf8_lossy(&bytes).trim().to_owned())
    }

    /// Gracefully stops the Firecracker process, with a timeout for forced kill.
    /// The socket file will be cleaned up after stopping the process.
    ///
    /// This method first tries to send `SIGTERM` to allow for graceful stop,
    /// but if the process does not exit within the specified timeout, it will send `SIGKILL` to force termination.
    #[tracing::instrument(skip(self))]
    pub async fn stop(&mut self, timeout: Duration) -> Result<()> {
        debug!(
            timeout_ms = timeout.as_millis(),
            "stopping firecracker process"
        );

        if let Some(mut child) = self.process.take() {
            // First try a graceful stop with SIGTERM
            if let Some(pid) = child.id() {
                trace!(pid, "sending SIGTERM to firecracker");
                let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
            }

            // Wait for the process to exit, but if it doesn't within the timeout, force kill it with SIGKILL
            if time::timeout(timeout, child.wait()).await.is_err() {
                warn!("firecracker stop timed out; sending SIGKILL");
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
        }

        if self.socket_path.exists() {
            let _ = fs::remove_file(&self.socket_path);
        }
        debug!("firecracker process stopped");
        Ok(())
    }

    /// Applies a CPU configuration template.
    /// Pre-boot only.
    pub async fn set_cpu_config(&self, json: &str) -> Result<()> {
        let raw: Box<RawValue> =
            serde_json::from_str(json).context("Failed to parse cpu_config_json")?;
        self.client
            .request_no_content(Method::PUT, "/cpu-config", Some(raw.as_ref()))
            .await
            .context("Failed to set CPU configuration")
    }

    /// Configures the Firecracker logger to write human-readable logs to a file.
    ///
    /// Pre-boot only and can only be set once. `level` is case-insensitive and
    /// must be one of `Error`, `Warning`, `Info`, `Debug`, `Trace`.
    pub async fn set_logger(&self, log_path: &Path, level: &str) -> Result<()> {
        let level = parse_log_level(level)?;
        let logger = Logger {
            level: Some(level),
            log_path: Some(log_path.to_string_lossy().into_owned()),
            show_level: Some(true),
            show_log_origin: Some(true),
            module: None,
        };
        self.client
            .request_no_content(Method::PUT, "/logger", Some(&logger))
            .await
            .context("Failed to configure Firecracker logger")
    }

    /// Configures the machine (vCPUs, memory, etc.)
    /// Pre-boot only.
    pub async fn set_machine_config(
        &self,
        mem_size_mib: u32,
        vcpu_count: u32,
        smt: bool,
        track_dirty_pages: bool,
    ) -> Result<()> {
        let mut machine_config = MachineConfiguration::new(mem_size_mib as i32, vcpu_count as i32);
        machine_config.smt = Some(smt);
        machine_config.track_dirty_pages = Some(track_dirty_pages);
        self.client
            .request_no_content(Method::PUT, "/machine-config", Some(&machine_config))
            .await
            .context("Failed to set machine configuration")
    }

    /// Configures the boot source (kernel, initrd, boot args).
    /// Pre-boot only.
    pub async fn set_boot_source(
        &self,
        kernel_image: &Path,
        initrd_path: Option<&Path>,
        boot_args: Option<&str>,
    ) -> Result<()> {
        let mut boot_source = BootSource::new(kernel_image.to_string_lossy().into_owned());
        boot_source.boot_args = boot_args.map(str::to_string);
        boot_source.initrd_path = initrd_path.map(|p| p.to_string_lossy().into_owned());
        self.client
            .request_no_content(Method::PUT, "/boot-source", Some(&boot_source))
            .await
            .context("Failed to set boot source")
    }

    /// Adds or updates a virtio-blk drive in the Firecracker device model.
    ///
    /// `is_root_device` must be `true` for the boot rootfs drive and `false`
    /// for any non-root extra drive. This API is only used during the
    /// pre-boot configuration phase. `rate_limiter` attaches a pre-boot limiter
    /// so throttling is in force from the guest's first I/O (a post-start PATCH
    /// would leave a brief unthrottled window).
    #[allow(clippy::too_many_arguments)]
    pub async fn add_drive(
        &self,
        drive_id: &str,
        path_on_host: &Path,
        is_root_device: bool,
        read_only: bool,
        direct: bool,
        io_engine: IoEngine,
        rate_limiter: Option<Box<RateLimiter>>,
    ) -> Result<()> {
        let mut drive = Drive::new(drive_id.to_string(), is_root_device);
        drive.path_on_host = Some(path_on_host.to_string_lossy().into_owned());
        drive.is_read_only = Some(read_only);
        drive.direct = Some(direct);
        drive.io_engine = Some(io_engine);
        drive.rate_limiter = rate_limiter;
        let path = format!("/drives/{}", drive_id);
        self.client
            .request_no_content(Method::PUT, &path, Some(&drive))
            .await
            .with_context(|| format!("Failed to add/update drive {}", drive_id))
    }

    /// Updates the rate limiter on a running VM's drive via PATCH.
    pub async fn patch_drive_rate_limiter(
        &self,
        drive_id: &str,
        rate_limiter: Box<RateLimiter>,
    ) -> Result<()> {
        let mut partial = PartialDrive::new(drive_id.to_string());
        partial.rate_limiter = Some(rate_limiter);
        let path = format!("/drives/{}", drive_id);
        self.client
            .request_no_content(Method::PATCH, &path, Some(&partial))
            .await
            .with_context(|| format!("Failed to patch rate limiter on drive {}", drive_id))
    }

    /// Updates the host backing path for a drive on a loaded snapshot.
    pub async fn patch_drive_path(&self, drive_id: &str, path_on_host: &Path) -> Result<()> {
        let mut partial = PartialDrive::new(drive_id.to_string());
        partial.path_on_host = Some(path_on_host.to_string_lossy().into_owned());
        let path = format!("/drives/{}", drive_id);
        self.client
            .request_no_content(Method::PATCH, &path, Some(&partial))
            .await
            .with_context(|| format!("Failed to patch host path on drive {}", drive_id))
    }

    /// Adds a network interface.
    /// Pre-boot only.
    pub async fn add_network_interface(
        &self,
        iface_id: &str,
        guest_mac: Option<String>,
        host_dev_name: String,
        rx_rate_limiter: Option<Box<RateLimiter>>,
        tx_rate_limiter: Option<Box<RateLimiter>>,
    ) -> Result<()> {
        let path = format!("/network-interfaces/{}", iface_id);
        let iface = NetworkInterface {
            guest_mac,
            host_dev_name: host_dev_name.clone(),
            iface_id: iface_id.to_string(),
            rx_rate_limiter,
            tx_rate_limiter,
        };
        self.client
            .request_no_content(Method::PUT, &path, Some(&iface))
            .await
            .with_context(|| format!("Failed to add network interface {}", iface_id))
    }

    /// Configures Firecracker MMDS V2 for a network interface.
    /// Pre-boot only.
    pub async fn set_mmds_config(&self, iface_id: &str) -> Result<()> {
        let mut config = MmdsConfig::new(vec![iface_id.to_string()]);
        config.version = Some(MmdsVersion::V2);
        self.client
            .request_no_content(Method::PUT, "/mmds/config", Some(&config))
            .await
            .context("Failed to set MMDS configuration")
    }

    /// Creates or replaces the Firecracker MMDS data store.
    pub async fn set_mmds(&self, metadata: &MmdsMetadata) -> Result<()> {
        let payload_size = serde_json::to_vec(metadata)
            .context("Failed to serialize MMDS metadata for size validation")?
            .len();
        if payload_size > MMDS_SIZE_LIMIT {
            bail!(
                "MMDS metadata payload is {payload_size} bytes, exceeds Firecracker MMDS size limit {MMDS_SIZE_LIMIT}; imageConfigs may be too large"
            );
        }
        self.client
            .request_no_content(Method::PUT, "/mmds", Some(metadata))
            .await
            .context("Failed to set MMDS data")
    }

    /// Configures the virtio-balloon device with free page reporting enabled.
    ///
    /// Pre-boot only. Sets `amount_mib = 0` (no target inflation) and
    /// `deflate_on_oom = true` as a safety net. The primary purpose is to
    /// enable free page reporting so the guest kernel can return unused pages
    /// to the host via `MADV_DONTNEED`, reducing host memory pressure.
    pub async fn set_balloon(&self) -> Result<()> {
        let balloon = Balloon {
            amount_mib: 0,
            deflate_on_oom: true,
            stats_polling_interval_s: None,
            free_page_hinting: None,
            free_page_reporting: Some(true),
        };
        self.client
            .request_no_content(Method::PUT, "/balloon", Some(&balloon))
            .await
            .context("Failed to set balloon configuration")
    }

    /// Starts the microVM.
    pub async fn start(&self) -> Result<()> {
        let action = InstanceActionInfo::new(ActionType::InstanceStart);
        self.client
            .request_no_content(Method::PUT, "/actions", Some(&action))
            .await
            .context("Failed to start microVM")
    }

    /// Pauses the microVM.
    pub async fn pause(&self) -> Result<()> {
        let body = Vm::new(VmState::Paused);
        self.client
            .request_no_content(Method::PATCH, "/vm", Some(&body))
            .await
            .context("Failed to pause microVM")
    }

    /// Resumes the microVM.
    pub async fn resume(&self) -> Result<()> {
        let body = Vm::new(VmState::Resumed);
        self.client
            .request_no_content(Method::PATCH, "/vm", Some(&body))
            .await
            .context("Failed to resume microVM")
    }

    /// Creates a diff snapshot containing VM state only.
    ///
    /// The memory data path is handled by AgentENV through dirty memory ranges.
    #[tracing::instrument(skip(self), fields(snapshot_path = %snapshot_path.display()))]
    pub async fn create_state_only_snapshot(&self, snapshot_path: &Path) -> Result<()> {
        let mut params = SnapshotCreateParams::new(snapshot_path.to_string_lossy().into_owned());
        params.snapshot_type =
            Some(firecracker_client::models::snapshot_create_params::SnapshotType::Diff);
        self.client
            .request_no_content(Method::PUT, "/snapshot/create", Some(&params))
            .await
            .context("Failed to create state-only snapshot")
    }

    /// Returns Firecracker's dirty memory ranges for direct memory snapshot packaging.
    #[tracing::instrument(skip(self))]
    pub async fn get_dirty_memory_ranges(&self) -> Result<DirtyMemoryRanges> {
        self.client
            .request::<(), DirtyMemoryRanges>(Method::GET, "/vm/dirty-memory-ranges", None)
            .await
            .context("Failed to get dirty memory ranges")
    }

    /// Loads a snapshot with uffd memory backend.
    /// Pre-boot only.
    #[allow(dead_code)]
    #[tracing::instrument(skip(self, network_overrides), fields(snapshot_path = %snapshot_path.display(), uffd_uds_path = %uffd_uds_path.display(), network_override_count = network_overrides.len()))]
    pub async fn load_snapshot_uffd(
        &self,
        snapshot_path: &Path,
        uffd_uds_path: &Path,
        network_overrides: &[(String, String)],
    ) -> Result<()> {
        let snapshot_path = self.resolve_host_path(snapshot_path);
        let mut params = SnapshotLoadParams::new(snapshot_path.to_string_lossy().into_owned());
        params.mem_backend = Some(Box::new(MemoryBackend::new(
            firecracker_client::models::memory_backend::BackendType::Uffd,
            uffd_uds_path.to_string_lossy().into_owned(),
        )));
        params.resume_vm = Some(true);
        if !network_overrides.is_empty() {
            params.network_overrides = Some(
                network_overrides
                    .iter()
                    .map(|(iface_id, host_dev_name)| {
                        NetworkOverride::new(iface_id.clone(), host_dev_name.clone())
                    })
                    .collect(),
            );
        }
        self.client
            .request_no_content(Method::PUT, "/snapshot/load", Some(&params))
            .await
            .context("Failed to load snapshot with uffd backend")
    }

    /// Loads a snapshot with a file-backed memory backend.
    /// Pre-boot only.
    #[tracing::instrument(skip(self, network_overrides), fields(snapshot_path = %snapshot_path.display(), mem_backend_path = %mem_backend_path.display(), network_override_count = network_overrides.len()))]
    pub async fn load_snapshot_file(
        &self,
        snapshot_path: &Path,
        mem_backend_path: &Path,
        network_overrides: &[(&str, &str)],
        resume_vm: bool,
        track_dirty_pages: bool,
    ) -> Result<()> {
        let snapshot_path = self.resolve_host_path(snapshot_path);
        let mut params = SnapshotLoadParams::new(snapshot_path.to_string_lossy().into_owned());
        params.mem_backend = Some(Box::new(MemoryBackend::new(
            firecracker_client::models::memory_backend::BackendType::File,
            mem_backend_path.to_string_lossy().into_owned(),
        )));
        params.resume_vm = Some(resume_vm);
        params.track_dirty_pages = Some(track_dirty_pages);
        if !network_overrides.is_empty() {
            params.network_overrides = Some(
                network_overrides
                    .iter()
                    .map(|(iface_id, host_dev_name)| {
                        NetworkOverride::new(iface_id.to_string(), host_dev_name.to_string())
                    })
                    .collect(),
            );
        }
        self.client
            .request_no_content(Method::PUT, "/snapshot/load", Some(&params))
            .await
            .context("Failed to load snapshot with file backend")
    }

    fn resolve_host_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.work_dir.join(path)
        }
    }
}

impl Drop for FirecrackerInstance {
    fn drop(&mut self) {
        let mut had_child = false;
        if let Some(mut child) = self.process.take() {
            had_child = true;
            if let Err(err) = child.start_kill() {
                warn!(error = %err, "failed to kill sandbox process on drop");
            }
        }

        // Only remove the socket if we actually spawned/owned the process.
        if had_child {
            if let Err(err) = fs::remove_file(&self.socket_path) {
                // Ignore NotFound error as the file might have been removed already
                if err.kind() != std::io::ErrorKind::NotFound {
                    warn!(
                        path = %self.socket_path.display(),
                        error = %err,
                        "failed to remove sandbox socket on drop"
                    );
                }
            }
        }
    }
}

fn open_log_stdio(path: Option<&Path>) -> Result<Stdio> {
    let Some(path) = path else {
        return Ok(Stdio::null());
    };

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create log directory {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open firecracker log {}", path.display()))?;
    Ok(Stdio::from(file))
}

/// Parse a case-insensitive Firecracker log level string.
fn parse_log_level(level: &str) -> Result<firecracker_client::models::logger::Level> {
    use firecracker_client::models::logger::Level;
    match level.trim().to_ascii_lowercase().as_str() {
        "error" => Ok(Level::Error),
        "warning" | "warn" => Ok(Level::Warning),
        "info" => Ok(Level::Info),
        "debug" => Ok(Level::Debug),
        "trace" => Ok(Level::Trace),
        other => bail!(
            "invalid firecracker log level '{other}'; expected one of Error, Warning, Info, Debug, Trace"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;
    use tempfile::tempdir;

    #[tokio::test]
    async fn wait_for_ready_times_out_when_socket_never_appears() {
        let temp = tempdir().expect("tempdir");
        let mut instance = FirecrackerInstance::new(temp.path().to_path_buf());

        let err = instance
            .wait_for_ready(Duration::from_millis(30), Duration::from_millis(10))
            .await
            .expect_err("missing socket should time out");

        assert!(err.to_string().contains("did not appear"));
    }

    #[tokio::test]
    async fn wait_for_ready_succeeds_when_socket_file_appears() -> Result<()> {
        let temp = tempdir()?;
        let mut instance = FirecrackerInstance::new(temp.path().to_path_buf());
        let socket_path = instance.socket_path.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            fs::write(&socket_path, b"socket").expect("create socket marker");
        });

        instance
            .wait_for_ready(Duration::from_millis(100), Duration::from_millis(10))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn stop_without_process_removes_stale_socket_file() -> Result<()> {
        let temp = tempdir()?;
        let mut instance = FirecrackerInstance::new(temp.path().to_path_buf());
        fs::write(&instance.socket_path, b"stale socket")?;

        instance.stop(Duration::from_millis(10)).await?;

        assert!(!instance.socket_path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn spawn_with_netns_rejects_duplicate_start() -> Result<()> {
        let temp = tempdir()?;
        let mut instance = FirecrackerInstance::new(temp.path().to_path_buf());

        instance
            .spawn_with_netns(Path::new("/bin/true"), None, None, None)
            .await?;

        let err = instance
            .spawn_with_netns(Path::new("/bin/true"), None, None, None)
            .await
            .expect_err("second spawn should fail");

        assert!(err.to_string().contains("already started"));
        instance.stop(Duration::from_millis(10)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn pid_rejects_an_exited_process() -> Result<()> {
        let temp = tempdir()?;
        let mut instance = FirecrackerInstance::new(temp.path().to_path_buf());
        instance.process = Some(Command::new("/bin/true").spawn()?);

        let error = time::timeout(Duration::from_secs(2), async {
            loop {
                match instance.running_pid() {
                    Ok(_) => time::sleep(Duration::from_millis(10)).await,
                    Err(error) => break error,
                }
            }
        })
        .await
        .context("Firecracker test process did not exit")?;

        assert!(error.to_string().contains("exited with status"));
        Ok(())
    }

    #[tokio::test]
    async fn drop_after_spawn_removes_owned_socket_file() -> Result<()> {
        let temp = tempdir()?;
        let socket_path = {
            let mut instance = FirecrackerInstance::new(temp.path().to_path_buf());
            instance
                .spawn_with_netns(Path::new("/bin/true"), None, None, None)
                .await?;
            fs::write(&instance.socket_path, b"owned socket")?;
            instance.socket_path.clone()
        };

        assert!(!socket_path.exists());
        Ok(())
    }

    #[test]
    fn drop_without_child_keeps_unowned_socket_file() -> Result<()> {
        let temp = tempdir()?;
        let socket_path = {
            let instance = FirecrackerInstance::new(temp.path().to_path_buf());
            fs::write(&instance.socket_path, b"external socket")?;
            instance.socket_path.clone()
        };

        assert!(socket_path.exists());
        Ok(())
    }

    #[test]
    fn open_log_stdio_creates_parent_dirs_and_appends_existing_files() -> Result<()> {
        let temp = tempdir()?;
        let log_path = temp.path().join("logs").join("firecracker.log");

        let status = StdCommand::new("sh")
            .arg("-c")
            .arg("printf first")
            .stdout(open_log_stdio(Some(&log_path))?)
            .status()?;
        assert!(status.success());
        assert_eq!(fs::read_to_string(&log_path)?, "first");

        let status = StdCommand::new("sh")
            .arg("-c")
            .arg("printf second")
            .stdout(open_log_stdio(Some(&log_path))?)
            .status()?;
        assert!(status.success());
        assert_eq!(fs::read_to_string(&log_path)?, "firstsecond");

        Ok(())
    }

    #[test]
    fn resolve_host_path_uses_firecracker_work_dir_for_relative_paths() -> Result<()> {
        let temp = tempdir()?;
        let instance = FirecrackerInstance::new(temp.path().to_path_buf());

        assert_eq!(
            instance.resolve_host_path(Path::new("vm_state.bin")),
            temp.path().join("vm_state.bin")
        );
        assert_eq!(
            instance.resolve_host_path(Path::new("/tmp/vm_state.bin")),
            PathBuf::from("/tmp/vm_state.bin")
        );

        Ok(())
    }
}
