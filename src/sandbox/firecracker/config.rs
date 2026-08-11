//! Firecracker sandbox config.
//!
//! This module defines the configuration structures for creating and managing Firecracker sandboxes,
//! including [`FirecrackerSandboxConfig`] for new instances and
//! [`FirecrackerSnapshotConfig`] for snapshot-based instances.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::mmds::MmdsMetadata;
use crate::cfg::{AppConfig, ConfigManager, EnvdConfig, ToolsConfig};
use crate::sandbox::ublk::UblkConfig;
use crate::sandbox::SandboxNetworkPolicy;
use crate::sandbox::UblkBackend;
use crate::sandbox::{validate_drive_id, ExtraDrive, OverlaybdConfig, SandboxLaunchConfig};
use crate::snapshot::RunnableSnapshot;
use anyhow::{bail, Context, Result};
use overlaybd::config::UpperMode;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::time::Duration;

// ── Constants ────────────────────────────────────────────────────────────────

/// Fallback boot arguments when `config.firecracker.boot_args` is not set.
/// Keep DAMON parameters in sync with `config/default.toml`.
///
/// DAMON reclaim parameters (conservative initial values — TODO: tune per workload):
///   min_age        = 60 000 000 ns (60 s) — page must be cold for 60 s before reclaim
///   quota_ms       = 100           — spend at most 100 ms per quota interval reclaiming
///   quota_sz       = 1 GiB         — reclaim at most 1 GiB per quota interval
///   quota_reset_interval_ms = 1000 — reset quota counters every 1 s
///   wmarks_high    = 900 (‰)      — stop reclaim when free pages > 90 %
///   wmarks_mid     = 700 (‰)      — start reclaim when free pages < 70 %
///   wmarks_low     = 200 (‰)      — aggressive reclaim below 20 %
///   wmarks_interval= 5 000 000 us  — check watermarks every 5 s
///   skip_anon      = Y             — only reclaim file-backed (pagecache) pages
const DEFAULT_BOOT_ARGS: &str = "\
    console=ttyS0 reboot=k panic=1 pci=off \
    damon_reclaim.enabled=Y \
    damon_reclaim.min_age=60000000 \
    damon_reclaim.quota_ms=100 \
    damon_reclaim.quota_sz=1073741824 \
    damon_reclaim.quota_reset_interval_ms=1000 \
    damon_reclaim.wmarks_high=900 \
    damon_reclaim.wmarks_mid=700 \
    damon_reclaim.wmarks_low=200 \
    damon_reclaim.skip_anon=Y \
    damon_reclaim.wmarks_interval=5000000";
pub(super) const MAX_EXTRA_DRIVES: usize = (b'z' - b'c' + 1) as usize;

#[derive(Debug)]
pub(crate) struct PersistentSnapshotRootGuard {
    path: PathBuf,
}

impl PersistentSnapshotRootGuard {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) async fn prepare(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.path)
            .await
            .with_context(|| format!("create managed snapshot root {}", self.path.display()))
    }
}

impl Drop for PersistentSnapshotRootGuard {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_dir_all(&self.path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    snapshot_root = %self.path.display(),
                    error = %err,
                    "failed to clean up managed snapshot root"
                );
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct FirecrackerRuntimePolicy {
    pub socket_timeout: Duration,
    pub socket_poll_interval: Duration,
    pub envd_timeout: Duration,
    pub envd_poll_interval: Duration,
}

impl FirecrackerRuntimePolicy {
    fn from_app_config(config: &AppConfig) -> Self {
        Self {
            socket_timeout: Duration::from_secs(config.firecracker.socket_timeout_secs),
            socket_poll_interval: Duration::from_millis(config.firecracker.socket_poll_ms),
            envd_timeout: Duration::from_secs(config.envd.init_timeout_secs),
            envd_poll_interval: Duration::from_millis(config.envd.poll_ms),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FirecrackerCommonConfig {
    pub firecracker_binary: PathBuf,
    /// Immutable release version of the complete tools drive.
    #[serde(default)]
    pub tools_drive_version: String,
    /// Optional parent directory for this sandbox's Firecracker runtime working directory.
    /// This is a host-side directory containing Firecracker sockets/symlinks/logs and
    /// OverlayBD writable upper layer files; it is unrelated to the guest default workdir.
    /// When omitted, the system temp directory is used.
    pub firecracker_work_base_dir: Option<PathBuf>,
    /// Base directory for Firecracker serial output.
    /// The actual serial output files will be created under `{serial_output_base_dir}/{sandbox_id}/`.
    ///
    /// Overriden by `stdout_path` and `stderr_path` if they are set.
    pub serial_output_base_dir: Option<PathBuf>,
    pub stdout_path: Option<PathBuf>,
    pub stderr_path: Option<PathBuf>,
    /// Optional Firecracker log level. When set (non-empty), Firecracker logging
    /// is enabled and written to a `firecracker.log` file alongside the stdout log.
    pub firecracker_log_level: Option<String>,
    pub runtime_policy: FirecrackerRuntimePolicy,
    /// Enable Firecracker KVM dirty-page tracking for memory snapshot capture.
    /// The serde default keeps older persisted snapshot configs compatible.
    #[serde(default)]
    pub track_dirty_pages: bool,
    pub envd_version: String,
    /// Control plane port inside the VM (default: 49983).
    pub control_plane_port: u16,
    pub env_vars: Option<HashMap<String, String>>,
    pub default_workdir: Option<String>,
    pub default_user: Option<String>,
    pub mmds_metadata: Option<MmdsMetadata>,
    /// OverlayBD config for the sandbox user rootfs drive.
    pub rootfs_image_config: Option<OverlaybdConfig>,
    /// Virtual size of the sandbox user rootfs block device in bytes.
    pub rootfs_virtual_size: Option<u64>,
    #[serde(default)]
    pub rootfs_allow_shrink: bool,
    pub extra_drives: Vec<ExtraDrive>,
    pub ublk_config: Option<UblkConfig>,
    /// Cluster-wide CPU intersection received from the scheduler.
    /// When set, applied via `PUT /cpu-config` before the VM boots.
    pub cpu_config_json: Option<String>,
    pub network_policy: Option<SandboxNetworkPolicy>,
    /// Opaque user-provided JSON passed through to the custom extension
    /// start-fresh / start-resume hooks. Persisted with snapshot configs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<crate::sandbox::CustomExtensionParams>,
    /// Effective disk I/O rate limit for the user rootfs drive. Carried in the
    /// common config so fresh boot and snapshot resume apply the same config
    /// and it persists across pause/resume, instead of each path reaching for
    /// the process-global config.
    #[serde(default)]
    pub disk_rate_limit: crate::cfg::DiskRateLimitConfig,
}

impl FirecrackerCommonConfig {
    pub fn new(
        firecracker_binary: PathBuf,
        tools_drive_version: String,
        runtime_policy: FirecrackerRuntimePolicy,
    ) -> Self {
        Self {
            firecracker_binary,
            tools_drive_version,
            firecracker_work_base_dir: None,
            serial_output_base_dir: None,
            stdout_path: None,
            stderr_path: None,
            firecracker_log_level: None,
            runtime_policy,
            track_dirty_pages: false,
            envd_version: EnvdConfig::default().version,
            control_plane_port: ToolsConfig::default().control_plane_port,
            env_vars: None,
            default_workdir: None,
            default_user: None,
            mmds_metadata: None,
            rootfs_image_config: None,
            rootfs_virtual_size: None,
            rootfs_allow_shrink: false,
            extra_drives: Vec::new(),
            ublk_config: None,
            cpu_config_json: None,
            network_policy: None,
            custom_extension_params: None,
            disk_rate_limit: crate::cfg::DiskRateLimitConfig::default(),
        }
    }

    pub fn from_app_config(config: &AppConfig) -> Result<Self> {
        let runtime_policy = FirecrackerRuntimePolicy::from_app_config(config);
        let firecracker_binary = config.resolved_firecracker_binary_path();
        let tools_drive_version = config.resolved_tools_version().to_string();

        let mut common = Self::new(firecracker_binary, tools_drive_version, runtime_policy);
        common.envd_version = config.envd.version.clone();
        common.disk_rate_limit = config.machine.disk_rate_limit.clone();
        common.track_dirty_pages = config.memory_snapshot.track_dirty_pages;
        common.rootfs_allow_shrink = config.ublk.overlaybd.allow_shrink;
        common.control_plane_port = config.tools.control_plane_port;
        common.firecracker_work_base_dir = config.firecracker.work_dir.clone();
        common.serial_output_base_dir =
            Self::resolve_serial_output_dir(config.firecracker.serial_dir.clone())?;
        common.firecracker_log_level = config
            .firecracker
            .log_level
            .as_ref()
            .map(|level| level.trim().to_string())
            .filter(|level| !level.is_empty());

        Ok(common)
    }

    pub fn from_global_config() -> Result<Self> {
        Self::from_app_config(ConfigManager::global_config())
    }

    pub fn validate(&self) -> Result<()> {
        let config = ConfigManager::global_config();
        let tools_drive_path = self.resolved_tools_drive_path(config)?;
        if !cfg!(target_os = "linux") {
            anyhow::bail!("Firecracker requires a Linux host");
        }
        if !self.firecracker_binary.exists() {
            anyhow::bail!(
                "firecracker binary not found at {}",
                self.firecracker_binary.display()
            );
        }
        if !tools_drive_path.exists() {
            anyhow::bail!(
                "tools drive version '{}' is not installed on this node; resolved path: {}; dependency root: {}; install this immutable release before launching the sandbox",
                self.tools_drive_version,
                tools_drive_path.display(),
                config.deps_path.display()
            );
        }
        if !tools_drive_path.is_file() {
            anyhow::bail!(
                "tools drive version '{}' resolved to a non-file path: {}",
                self.tools_drive_version,
                tools_drive_path.display()
            );
        }
        self.validate_persisted_artifacts()
    }

    pub(super) fn resolved_tools_drive_path(&self, config: &AppConfig) -> Result<PathBuf> {
        if self.tools_drive_version.trim().is_empty() {
            anyhow::bail!(
                "sandbox state does not record a tools drive version; migrate its persisted metadata before resuming it"
            );
        }
        config
            .resolved_tools_drive_path_for_version(&self.tools_drive_version)
            .with_context(|| {
                format!(
                    "resolve tools drive version '{}' under dependency root {}",
                    self.tools_drive_version,
                    config.deps_path.display()
                )
            })
    }

    fn validate_persisted_artifacts(&self) -> Result<()> {
        validate_overlaybd_extra_drive_set(&self.extra_drives)?;
        if matches!(self.rootfs_virtual_size, Some(0)) {
            anyhow::bail!("rootfs virtual size must be non-zero");
        }
        if let Some(ublk_config) = &self.ublk_config {
            let UblkBackend::Overlaybd(overlaybd_cfg) = &ublk_config.backend;
            if !overlaybd_cfg.image_config_path.exists() {
                anyhow::bail!(
                    "overlaybd image config not found at {}",
                    overlaybd_cfg.image_config_path.display()
                );
            }
        }
        Ok(())
    }

    fn resolve_serial_output_dir(serial_output_dir: Option<PathBuf>) -> Result<Option<PathBuf>> {
        let Some(output_dir) = serial_output_dir else {
            return Ok(None);
        };

        let output_dir = if output_dir.is_absolute() {
            output_dir
        } else {
            std::env::current_dir()
                .context("resolve serial output dir")?
                .join(output_dir)
        };

        Ok(Some(output_dir))
    }
}

pub(crate) fn create_firecracker_work_dir(work_dir: Option<&Path>) -> Result<TempDir> {
    match work_dir {
        Some(parent) => {
            fs::create_dir_all(parent)
                .with_context(|| format!("create firecracker work_dir {}", parent.display()))?;
            TempDir::with_prefix_in("agentenv-fc-", parent).with_context(|| {
                format!(
                    "create firecracker sandbox work directory under {}",
                    parent.display()
                )
            })
        }
        None => TempDir::with_prefix("agentenv-fc-")
            .context("create firecracker sandbox work directory"),
    }
}

// ── FirecrackerSandboxConfig ────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct FirecrackerSandboxConfig {
    pub common: FirecrackerCommonConfig,
    pub kernel_image: PathBuf,
    pub boot_args: Option<String>,
    pub vcpu_count: u32,
    pub mem_size_mib: u32,
}

impl FirecrackerSandboxConfig {
    pub fn new(
        firecracker_binary: PathBuf,
        kernel_image: PathBuf,
        tools_drive_version: String,
        user_image_config_path: PathBuf,
    ) -> Self {
        let app_config = AppConfig::default();
        let mut common = FirecrackerCommonConfig::new(
            firecracker_binary,
            tools_drive_version,
            FirecrackerRuntimePolicy::from_app_config(&app_config),
        );
        common.rootfs_image_config = Some(OverlaybdConfig {
            image_config_path: user_image_config_path,
            read_only: false,
            runtime_upper_mode: UpperMode::LogStructured,
        });
        common.disk_rate_limit = app_config.machine.disk_rate_limit;
        Self {
            common,
            kernel_image,
            boot_args: None,
            vcpu_count: app_config.machine.vcpu_count,
            mem_size_mib: app_config.machine.mem_size_mib,
        }
    }

    pub fn from_app_config_with_user_image(
        config: &AppConfig,
        mut user_image_config: OverlaybdConfig,
    ) -> Result<Self> {
        let kernel_image = config.resolved_kernel_image_path();

        let ublk = &config.ublk;
        let runtime_upper_mode = ublk.overlaybd.runtime_upper_mode;
        user_image_config.runtime_upper_mode = runtime_upper_mode;

        let mut common = FirecrackerCommonConfig::from_app_config(config)?;
        common.rootfs_image_config = Some(user_image_config.clone());
        common.ublk_config = Some(UblkConfig::overlaybd_with_runtime_upper_mode(
            user_image_config.image_config_path.clone(),
            user_image_config.read_only,
            runtime_upper_mode,
        ));

        Ok(Self {
            common,
            kernel_image,
            boot_args: config
                .firecracker
                .boot_args
                .clone()
                .or_else(|| Some(DEFAULT_BOOT_ARGS.to_string())),
            vcpu_count: config.machine.vcpu_count,
            mem_size_mib: config.machine.mem_size_mib,
        })
    }

    pub fn from_global_config_with_user_image(user_image_config: OverlaybdConfig) -> Result<Self> {
        Self::from_app_config_with_user_image(ConfigManager::global_config(), user_image_config)
    }

    pub fn apply_launch_config(mut self, launch_config: &SandboxLaunchConfig) -> Self {
        if let Some(env_vars) = launch_config
            .env_vars
            .as_ref()
            .filter(|env_vars| !env_vars.is_empty())
        {
            let env = self.common.env_vars.get_or_insert_with(Default::default);
            env.extend(env_vars.clone());
        }
        self.common.mmds_metadata = Some(
            MmdsMetadata::new(launch_config.sandbox_id, launch_config.snapshot_id.clone())
                .with_extra(launch_config.extra_mmds.clone()),
        );
        self.common.network_policy = launch_config.network.clone();
        self.common.custom_extension_params = launch_config.custom_extension_params.clone();
        self
    }

    pub fn validate(&self) -> Result<()> {
        self.common.validate()?;
        if !self.kernel_image.exists() {
            anyhow::bail!("kernel image not found at {}", self.kernel_image.display());
        }
        let rootfs_image_config = self
            .common
            .rootfs_image_config
            .as_ref()
            .context("user image overlaybd config is missing")?;
        if !rootfs_image_config.image_config_path.exists() {
            anyhow::bail!(
                "user image overlaybd config not found at {}",
                rootfs_image_config.image_config_path.display()
            );
        }
        if !rootfs_image_config.image_config_path.is_file() {
            anyhow::bail!(
                "user image overlaybd config path is not a file: {}",
                rootfs_image_config.image_config_path.display()
            );
        }
        Ok(())
    }
}

// ── FirecrackerSnapshotConfig ───────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FirecrackerSnapshotConfig {
    pub common: FirecrackerCommonConfig,
    /// The path to the Firecracker VM state snapshot file (for example, `vm_state.bin`).
    pub vm_state_path: PathBuf,
    /// The memory overlaybd image config (image config path + read_only).
    pub mem_overlaybd_config: OverlaybdConfig,
    /// Virtual size of the memory image in bytes.
    pub mem_virtual_size: u64,
    /// If `None`, the caller is responsible for managing the snapshot directory lifecycle.
    /// `Some` is used for keeping the snapshot directory alive across multiple pause/resume cycles.
    #[serde(skip)]
    pub(super) managed_snapshot_root: Option<Arc<PersistentSnapshotRootGuard>>,
}

impl FirecrackerSnapshotConfig {
    pub fn from_runnable_snapshot(snapshot: &RunnableSnapshot) -> Result<Self> {
        let mut base_common =
            FirecrackerCommonConfig::from_global_config().context("load sandbox common config")?;
        let manifest = snapshot.manifest();

        let app_config = ConfigManager::global_config();
        let snapshot_mode = snapshot.committed().virtualization_mode;
        if snapshot_mode != app_config.virtualization_mode {
            bail!(
                "snapshot '{}' uses virtualization mode '{}', but this node runs in mode '{}'",
                snapshot.record().id,
                snapshot_mode,
                app_config.virtualization_mode
            );
        }
        let tools_drive_version = &snapshot.committed().runtime_versions.tools_drive_version;
        if tools_drive_version.trim().is_empty() {
            bail!(
                "snapshot does not record a tools drive version; migrate its metadata before launching it"
            );
        }
        base_common.tools_drive_version = tools_drive_version.clone();
        let rootfs_image_config = OverlaybdConfig {
            image_config_path: manifest.rootfs.image_config_path.clone(),
            read_only: app_config.ublk.overlaybd.read_only,
            runtime_upper_mode: app_config.ublk.overlaybd.runtime_upper_mode,
        };
        let overlaybd_ublk_config = UblkConfig::overlaybd_with_runtime_upper_mode(
            rootfs_image_config.image_config_path.clone(),
            rootfs_image_config.read_only,
            rootfs_image_config.runtime_upper_mode,
        );

        let build_context = &snapshot.committed().context;
        let env_vars = build_context.env_vars.clone();
        // cpu_config_json is intentionally not set here. It is a pre-boot Firecracker
        // API call (PUT /cpu-config) that configures the virtual CPU before the VM
        // starts for the first time. When resuming from a snapshot, the full CPU state
        // is already serialised inside vm_state.bin, so re-applying a template would
        // be incorrect and is rejected by Firecracker anyway.
        let snapshot_common = FirecrackerCommonConfig {
            ublk_config: Some(overlaybd_ublk_config),
            envd_version: snapshot.committed().runtime_versions.envd_version.clone(),
            env_vars: (!env_vars.is_empty()).then_some(env_vars),
            default_workdir: Some(build_context.workdir.clone()),
            default_user: build_context.user.clone(),
            rootfs_image_config: Some(rootfs_image_config),
            rootfs_virtual_size: Some(manifest.rootfs.virtual_size),
            extra_drives: manifest.extra_drives(),
            ..base_common
        };

        let mem_overlaybd_config = OverlaybdConfig {
            image_config_path: manifest.memory.image_config_path.clone(),
            read_only: true,
            runtime_upper_mode: UpperMode::LogStructured,
        };

        Ok(Self {
            common: snapshot_common,
            vm_state_path: manifest.vm_state.path.clone(),
            mem_overlaybd_config,
            mem_virtual_size: manifest.memory.virtual_size,
            managed_snapshot_root: None,
        })
    }

    pub fn validate(&self) -> Result<()> {
        self.common.validate()?;
        self.validate_persisted_artifacts()
    }

    /// Validates stored artifacts while deferring node launch dependencies until resume.
    pub(super) fn validate_persisted(&self) -> Result<()> {
        self.common.validate_persisted_artifacts()?;
        self.validate_persisted_artifacts()
    }

    fn validate_persisted_artifacts(&self) -> Result<()> {
        let rootfs_virtual_size = self
            .common
            .rootfs_virtual_size
            .context("rootfs virtual size must be non-zero")?;
        let rootfs_image_config = self
            .common
            .rootfs_image_config
            .as_ref()
            .context("base rootfs image config is missing")?;
        anyhow::ensure!(
            rootfs_virtual_size > 0,
            "rootfs virtual size must be non-zero"
        );
        if !self.vm_state_path.exists() {
            anyhow::bail!(
                "vm state snapshot not found at {}",
                self.vm_state_path.display()
            );
        }
        if !self.mem_overlaybd_config.image_config_path.exists() {
            anyhow::bail!(
                "mem overlaybd image config not found at {}",
                self.mem_overlaybd_config.image_config_path.display()
            );
        }
        let rootfs_path = &rootfs_image_config.image_config_path;
        if !rootfs_path.exists() {
            anyhow::bail!("base rootfs not found at {}", rootfs_path.display());
        }
        if !self.mem_overlaybd_config.image_config_path.is_file() {
            anyhow::bail!(
                "mem overlaybd image config path is not a file: {}",
                self.mem_overlaybd_config.image_config_path.display()
            );
        }
        if !rootfs_path.is_file() {
            anyhow::bail!("base rootfs path is not a file: {}", rootfs_path.display());
        }

        Ok(())
    }
}

fn validate_overlaybd_extra_drive_set(extra_drives: &[ExtraDrive]) -> Result<()> {
    if extra_drives.len() > MAX_EXTRA_DRIVES {
        bail!(
            "too many extra drives: {} configured, but at most {} are supported (/dev/vdc..=/dev/vdz)",
            extra_drives.len(),
            MAX_EXTRA_DRIVES
        );
    }
    let mut drive_ids = HashSet::new();
    let mut mount_paths = HashSet::new();
    extra_drives.iter().try_for_each(|drive| {
        validate_overlaybd_extra_drive(drive, &mut drive_ids, &mut mount_paths)
    })
}

fn validate_overlaybd_extra_drive(
    drive: &ExtraDrive,
    drive_ids: &mut HashSet<String>,
    mount_paths: &mut HashSet<PathBuf>,
) -> Result<()> {
    let drive_id = drive.drive_id();
    let image_config_path = drive.image_config_path();

    validate_drive_id(drive_id)?;
    if !drive_ids.insert(drive_id.to_string()) {
        anyhow::bail!("duplicate extra drive id: {}", drive_id);
    }
    crate::sandbox::validate_mount_path(drive.mount_path())?;
    if !mount_paths.insert(drive.mount_path().to_path_buf()) {
        anyhow::bail!(
            "duplicate extra drive mount path: {}",
            drive.mount_path().display()
        );
    }
    if matches!(drive.virtual_size(), Some(0)) {
        anyhow::bail!("extra drive virtual size must be non-zero: {}", drive_id);
    }
    if !image_config_path.exists() {
        anyhow::bail!(
            "overlaybd image config not found at {}",
            image_config_path.display()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{UblkOverlaybdTomlConfig, UblkTomlConfig};
    use crate::snapshot::{CommittedSnapshot, SnapshotRecord};
    use std::fs;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    struct CurrentDirGuard(PathBuf);

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.0);
        }
    }

    fn cwd_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn write_file(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, b"test").expect("write temp file");
        path
    }

    fn base_app_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.firecracker.binary_path = Some("firecracker".into());
        config.kernel.image_path = Some("vmlinux.bin".into());
        config.tools.drive_path = Some("tools.ext4".into());
        config
    }

    fn test_runtime_policy() -> FirecrackerRuntimePolicy {
        FirecrackerRuntimePolicy::from_app_config(&base_app_config())
    }

    #[test]
    fn resolve_serial_output_path_resolves_relative_directory() -> Result<()> {
        let _guard = cwd_lock().lock().expect("cwd lock");
        let temp = tempdir()?;
        let previous = std::env::current_dir()?;
        let _restore = CurrentDirGuard(previous);
        std::env::set_current_dir(temp.path())?;

        let output = FirecrackerCommonConfig::resolve_serial_output_dir(Some(PathBuf::from(
            "serial-output",
        )))?
        .expect("serial path should be present");

        assert_eq!(output, temp.path().join("serial-output"));
        Ok(())
    }

    #[test]
    fn from_app_config_does_not_bind_ublk_without_user_image() -> Result<()> {
        let mut config = base_app_config();
        config.ublk = UblkTomlConfig {
            daemon_binary_path: None,
            daemon_log_path: None,
            overlaybd: UblkOverlaybdTomlConfig {
                global_config_path: "overlaybd_global.json".into(),
                read_only: true,
                ..Default::default()
            },
            ..UblkTomlConfig::default()
        };

        let common_config = FirecrackerCommonConfig::from_app_config(&config)?;
        assert!(common_config.ublk_config.is_none());
        Ok(())
    }

    #[test]
    fn sandbox_config_binds_overlaybd_to_user_image() -> Result<()> {
        let mut config = base_app_config();
        config.ublk = UblkTomlConfig {
            daemon_binary_path: None,
            daemon_log_path: None,
            overlaybd: UblkOverlaybdTomlConfig {
                global_config_path: "overlaybd_global.json".into(),
                read_only: false,
                runtime_upper_mode: UpperMode::Sparse,
                ..Default::default()
            },
            ..UblkTomlConfig::default()
        };

        let sandbox_config = FirecrackerSandboxConfig::from_app_config_with_user_image(
            &config,
            OverlaybdConfig {
                image_config_path: PathBuf::from("custom-image.json"),
                read_only: false,
                runtime_upper_mode: UpperMode::LogStructured,
            },
        )?;
        match sandbox_config
            .common
            .ublk_config
            .as_ref()
            .map(|cfg| &cfg.backend)
        {
            Some(UblkBackend::Overlaybd(overlaybd)) => {
                assert_eq!(
                    overlaybd.image_config_path,
                    PathBuf::from("custom-image.json")
                );
                assert!(!overlaybd.read_only);
                assert_eq!(overlaybd.runtime_upper_mode, UpperMode::Sparse);
            }
            other => panic!("expected overlaybd backend, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn validate_rejects_duplicate_extra_drive_ids() {
        let temp = tempdir().expect("tempdir");
        let firecracker_binary = write_file(temp.path(), "firecracker");
        let first_image = write_file(temp.path(), "overlaybd-image-1.json");
        let second_image = write_file(temp.path(), "overlaybd-image-2.json");

        let mut config = FirecrackerCommonConfig::new(
            firecracker_binary,
            "0.1.0".to_string(),
            test_runtime_policy(),
        );
        config.extra_drives = vec![
            ExtraDrive::Overlaybd {
                drive_id: "data".to_string(),
                image_config_path: first_image,
                read_only: true,
                mount_path: ExtraDrive::default_mount_path("data"),
                virtual_size: None,
                sub_path: None,
            },
            ExtraDrive::Overlaybd {
                drive_id: "data".to_string(),
                image_config_path: second_image,
                read_only: true,
                mount_path: ExtraDrive::default_mount_path("data"),
                virtual_size: None,
                sub_path: None,
            },
        ];

        let err = config
            .validate_persisted_artifacts()
            .expect_err("duplicate extra drive ids should fail");
        assert!(err.to_string().contains("duplicate extra drive id"));
    }

    #[test]
    fn validate_rejects_too_many_extra_drives() {
        let temp = tempdir().expect("tempdir");
        let firecracker_binary = write_file(temp.path(), "firecracker");

        let mut config = FirecrackerCommonConfig::new(
            firecracker_binary,
            "0.1.0".to_string(),
            test_runtime_policy(),
        );
        config.extra_drives = (0..=MAX_EXTRA_DRIVES)
            .map(|i| {
                let drive_id = format!("data-{i}");
                ExtraDrive::Overlaybd {
                    mount_path: ExtraDrive::default_mount_path(&drive_id),
                    drive_id,
                    image_config_path: write_file(
                        temp.path(),
                        &format!("overlaybd-image-{i}.json"),
                    ),
                    read_only: true,
                    virtual_size: None,
                    sub_path: None,
                }
            })
            .collect();

        let err = config
            .validate_persisted_artifacts()
            .expect_err("too many extra drives should fail");
        assert!(err.to_string().contains("too many extra drives"));
    }

    #[test]
    fn snapshot_persisted_validation_checks_each_required_artifact() -> Result<()> {
        let temp = tempdir()?;
        let vm_state_path = temp.path().join("vm_state.bin");
        let image_config_path = temp.path().join("mem_image.json");
        let rootfs_path = temp.path().join("rootfs.ext4");
        let mut common = FirecrackerSandboxConfig::new(
            "firecracker".into(),
            "vmlinux".into(),
            "0.1.0".to_string(),
            rootfs_path.clone(),
        )
        .common;
        common.rootfs_virtual_size = Some(0);
        let mut snapshot = FirecrackerSnapshotConfig {
            common,
            vm_state_path: vm_state_path.clone(),
            mem_overlaybd_config: OverlaybdConfig {
                image_config_path: image_config_path.clone(),
                read_only: true,
                runtime_upper_mode: UpperMode::LogStructured,
            },
            mem_virtual_size: 4096,
            managed_snapshot_root: None,
        };

        let err = snapshot
            .validate_persisted()
            .expect_err("rootfs virtual size must be non-zero");
        assert!(err
            .to_string()
            .contains("rootfs virtual size must be non-zero"));

        snapshot.common.rootfs_virtual_size = Some(4096);
        let err = snapshot
            .validate_persisted()
            .expect_err("missing vm state snapshot");
        assert!(err.to_string().contains("vm state snapshot not found"));

        fs::write(&vm_state_path, b"snapshot")?;
        let err = snapshot
            .validate_persisted()
            .expect_err("missing mem image config");
        assert!(err
            .to_string()
            .contains("mem overlaybd image config not found"));

        fs::write(&image_config_path, b"{}")?;
        let err = snapshot.validate_persisted().expect_err("missing rootfs");
        assert!(err.to_string().contains("base rootfs not found"));

        fs::write(&rootfs_path, b"rootfs")?;
        snapshot.validate_persisted()?;
        Ok(())
    }

    #[test]
    fn runnable_snapshot_uses_its_tools_drive_version() -> Result<()> {
        let snapshot_version = "9.9.9";
        let mut committed = CommittedSnapshot::mock();
        committed.runtime_versions.tools_drive_version = snapshot_version.to_string();
        let snapshot =
            RunnableSnapshot::from_test_manifest(SnapshotRecord::mock_ready(committed), Vec::new());

        let config = FirecrackerSnapshotConfig::from_runnable_snapshot(&snapshot)?;

        assert_eq!(config.common.tools_drive_version, snapshot_version);
        Ok(())
    }

    #[test]
    fn runnable_snapshot_without_tools_drive_version_is_not_launchable() {
        let mut committed = CommittedSnapshot::mock();
        committed.runtime_versions.tools_drive_version.clear();
        let snapshot =
            RunnableSnapshot::from_test_manifest(SnapshotRecord::mock_ready(committed), Vec::new());

        let err = FirecrackerSnapshotConfig::from_runnable_snapshot(&snapshot)
            .expect_err("legacy snapshot must not launch without a tools drive version");

        assert!(err
            .to_string()
            .contains("snapshot does not record a tools drive version"));
    }

    #[test]
    fn runnable_snapshot_rejects_cross_virtualization_mode() {
        use crate::virtualization::VirtualizationMode;

        let node_mode = ConfigManager::global_config().virtualization_mode;
        let snapshot_mode = match node_mode {
            VirtualizationMode::Kvm => VirtualizationMode::Pvm,
            VirtualizationMode::Pvm => VirtualizationMode::Kvm,
        };
        let mut committed = CommittedSnapshot::mock();
        committed.virtualization_mode = snapshot_mode;
        let snapshot =
            RunnableSnapshot::from_test_manifest(SnapshotRecord::mock_ready(committed), Vec::new());

        let err = FirecrackerSnapshotConfig::from_runnable_snapshot(&snapshot)
            .expect_err("node must reject a snapshot from the other virtualization mode");

        assert!(err
            .to_string()
            .contains(&format!("uses virtualization mode '{snapshot_mode}'")));
    }
}
