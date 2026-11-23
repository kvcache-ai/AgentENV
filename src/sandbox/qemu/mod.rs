//! Linux cold-start backend. Shared storage helpers intentionally remain in
//! firecracker until the independent backend-neutral refactor lands.
mod metadata;
mod qmp;

use super::{
    envd::EnvdInstance,
    extra_drive::{prepare_extra_drives, ExtraDrivePrepareMode},
    firecracker::resources::SandboxResources,
    ublk::{load_runtime_source, UblkCreateSpec},
    *,
};
use crate::{
    cfg::ConfigManager,
    snapshot::{SnapshotHandle, SnapshotId, SnapshotPlacement},
    types::SandboxId,
};
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use std::{
    any::Any,
    fs,
    net::Ipv4Addr,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use uvm_ublk_daemon::CreateOverlaybdRuntimeDeviceRequest;

pub(crate) struct QemuSandbox {
    config: FirecrackerSandboxConfig,
    work_dir: Arc<TempDir>,
    /// Service-owned stable directory; OverlayBD runtime state lives here.
    storage_root: PathBuf,
    log_dir: PathBuf,
    process: Option<Child>,
    metadata: Option<metadata::MetadataServer>,
    resources: SandboxResources,
    envd: Option<EnvdInstance>,
    rootfs_size: Option<u64>,
    started: bool,
}

impl QemuSandbox {
    pub(crate) fn new(
        config: FirecrackerSandboxConfig,
        id: SandboxId,
        storage_root: &Path,
    ) -> Result<Self> {
        let global = ConfigManager::global_config();
        ensure!(
            cfg!(target_arch = "x86_64")
                && global.virtualization_mode == crate::virtualization::VirtualizationMode::Kvm,
            "QEMU cold starts currently require an x86_64 KVM host"
        );
        ensure!(
            !config.common.extra_drives.iter().any(ExtraDrive::is_volume),
            "QEMU persistent volumes are not supported yet"
        );
        ensure!(
            !config.common.disk_rate_limit.enabled,
            "QEMU disk rate limits are not supported yet"
        );
        ensure!(
            config
                .common
                .custom_extension_params
                .as_ref()
                .is_none_or(|p| p.is_empty())
                && CustomExtensionClient::global().is_none(),
            "QEMU custom extension hooks are not supported yet"
        );
        // Same parent as Firecracker so startup recovery reclaims stale dirs.
        let base = config
            .common
            .firecracker_work_base_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        fs::create_dir_all(&base)?;
        let work_dir = Arc::new(TempDir::with_prefix_in("agentenv-qemu-", base)?);
        let log_dir = config
            .common
            .serial_output_base_dir
            .as_ref()
            .map(|dir| dir.join(id.to_string()))
            .unwrap_or_else(|| work_dir.path().join("logs"));
        fs::create_dir_all(&log_dir)?;
        Ok(Self {
            config,
            work_dir,
            storage_root: storage_root.to_owned(),
            log_dir,
            process: None,
            metadata: None,
            resources: Default::default(),
            envd: None,
            rootfs_size: None,
            started: false,
        })
    }

    fn socket(&self) -> PathBuf {
        self.work_dir.path().join("qemu.socket")
    }

    async fn boot(&mut self) -> Result<()> {
        ensure!(!self.started, "QEMU sandbox has already been started");
        self.started = true;
        let global = ConfigManager::global_config();
        crate::deps::ensure_qemu_kernel(global).await?;
        validate_kernel(&self.config.kernel_image)?;
        crate::deps::ensure_qemu(global).await?;
        let binary = global.resolved_qemu_binary_path();
        let common = &self.config.common;
        let tools_path = if let Some(image_config) =
            resolve_tools_image(global, &common.tools_drive_version).await?
        {
            let device = UblkDeviceManager::global()
                .get_or_create_shared_tools(&UblkCreateSpec::Overlaybd {
                    image_config,
                    global_config: global.ublk.overlaybd.global_config_path.clone(),
                    read_only_upper: false,
                })
                .await?;
            let path = device.device_path().to_owned();
            self.resources.tools_ublk_device = Some(device);
            path
        } else {
            common.resolved_tools_drive_path(global)?
        };
        fs::write(self.work_dir.path().join("backend"), b"qemu\n")?;
        std::os::unix::fs::symlink(tools_path, self.work_dir.path().join("rootfs.ext4"))?;
        let image = common
            .rootfs_image_config
            .as_ref()
            .context("QEMU rootfs missing")?;
        let layers = image.resolve_layers().await?;
        let (source, source_root) = load_runtime_source(&image.image_config_path)?;
        let runtime = UblkDeviceManager::global()
            .create_overlaybd_runtime_device(
                CreateOverlaybdRuntimeDeviceRequest {
                    image: source,
                    source_root: &source_root,
                    global_config: &global.ublk.overlaybd.global_config_path,
                    runtime_dir: &self.storage_root.join("overlaybd"),
                    read_only: false,
                    runtime_upper_mode: image.runtime_upper_mode,
                    requested_virtual_size: common.rootfs_virtual_size,
                    known_source_virtual_size: None,
                    allow_shrink: common.rootfs_allow_shrink,
                },
                layers,
            )
            .await?;
        self.rootfs_size = Some(runtime.actual_virtual_size);
        let rootfs_device = runtime.device.device_path().to_owned();
        self.resources.rootfs_runtime = Some(runtime);
        std::os::unix::fs::symlink(rootfs_device, self.work_dir.path().join("user-rootfs"))?;
        let drives = prepare_extra_drives(
            &common.extra_drives,
            &global.ublk.overlaybd.global_config_path,
            self.work_dir.path(),
            &self.storage_root,
            global.ublk.overlaybd.runtime_upper_mode,
            |_| ExtraDrivePrepareMode::Fresh {
                allow_shrink: common.rootfs_allow_shrink,
            },
            &mut self.resources.extra_drive_runtimes,
        )
        .await?;
        let slot = self.resources.network_slot.insert(
            NetworkManager::global()
                .allocate_configured(common.network_policy.clone())
                .await?,
        );
        let netns = slot.namespace_path();
        let interaction_ip = slot.host_interaction_ip;
        let ip_arg = slot.build_ip_boot_arg();
        self.metadata = Some(metadata::MetadataServer::start(
            &netns,
            serde_json::to_value(
                common
                    .mmds_metadata
                    .as_ref()
                    .context("QEMU metadata missing")?,
            )?,
        )?);
        let mut args = launch_args(
            &self.config,
            &ip_arg,
            self.resources.tools_ublk_device.is_some(),
        );
        for (index, drive) in drives.iter().enumerate() {
            add_drive(
                &mut args,
                &format!("extra{index}"),
                &drive.attachment_path,
                drive.read_only,
                true,
            );
        }
        let ns = fs::File::open(&netns)?;
        let mut command = Command::new(binary);
        command
            .args(&args)
            .current_dir(self.work_dir.path())
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::from(fs::File::create(
                self.log_dir.join("qemu-stdout.log"),
            )?))
            .stderr(Stdio::from(fs::File::create(
                self.log_dir.join("qemu-stderr.log"),
            )?));
        let child = crate::privileges::spawn_tokio_command_scoped(command, &[], move || {
            if unsafe { libc::setns(ns.as_raw_fd(), libc::CLONE_NEWNET) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        })
        .await
        .context("spawn QEMU")?;
        if let Some(pid) = child.id() {
            let _ = fs::write(format!("/proc/{pid}/oom_score_adj"), "1000");
        }
        self.process = Some(child);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.process.as_mut().unwrap().try_wait()? {
                bail!(
                    "QEMU exited ({status}): {}",
                    fs::read_to_string(self.log_dir.join("qemu-stderr.log")).unwrap_or_default()
                );
            }
            if qmp::execute(&self.socket(), "query-status").await.is_ok() {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "QEMU QMP socket did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.envd = Some(EnvdInstance::new(
            format!("http://{interaction_ip}:{}", common.control_plane_port),
            common.envd_access_token.clone(),
        ));
        qmp::execute(&self.socket(), "cont").await?;
        Ok(())
    }
}

/// Known Firecracker kernels embed their config. Fail before allocating host
/// resources when that config proves q35 cannot see its boot disks.
fn validate_kernel(path: &Path) -> Result<()> {
    use std::io::Read;
    let bytes =
        fs::read(path).with_context(|| format!("read QEMU Linux kernel {}", path.display()))?;
    if let Some(start) = bytes.windows(8).position(|w| w == b"IKCFG_ST") {
        if let Some(end) = bytes[start + 8..].windows(8).position(|w| w == b"IKCFG_ED") {
            let mut config = String::new();
            flate2::read::GzDecoder::new(&bytes[start + 8..start + 8 + end])
                .take(1024 * 1024)
                .read_to_string(&mut config)?;
            for option in [
                "CONFIG_PCI",
                "CONFIG_VIRTIO_PCI",
                "CONFIG_VIRTIO_BLK",
                "CONFIG_VIRTIO_NET",
            ] {
                ensure!(config.lines().any(|line| line == format!("{option}=y")),
                    "QEMU q35 requires {option}=y built into {}; configure kernel.image_path with a q35-compatible Linux kernel", path.display());
            }
        }
    }
    Ok(())
}

fn launch_args(
    config: &FirecrackerSandboxConfig,
    ip_arg: &str,
    tools_block_device: bool,
) -> Vec<String> {
    // Firecracker's pci=off and DAMON physical ranges are invalid for q35.
    let mut boot: Vec<&str> = config
        .boot_args
        .as_deref()
        .unwrap_or_default()
        .split_whitespace()
        .filter(|arg| {
            !arg.starts_with("pci=")
                && !arg.starts_with("damon_reclaim.")
                && !arg.starts_with("root=")
                && !arg.starts_with("init=")
                && !arg.starts_with("ip=")
        })
        .collect();
    boot.extend([
        "root=/dev/vda",
        "ro",
        "rootwait",
        "init=/init",
        "net.ifnames=0",
        "sysctl.net.ipv4.conf.all.arp_notify=1",
        ip_arg,
    ]);
    let mut args = vec![
        "-machine".into(),
        "q35".into(),
        "-accel".into(),
        "kvm".into(),
        "-cpu".into(),
        "host".into(),
        "-smp".into(),
        config.vcpu_count.to_string(),
        "-m".into(),
        config.mem_size_mib.to_string(),
        "-kernel".into(),
        config.kernel_image.to_string_lossy().into_owned(),
        "-append".into(),
        boot.join(" "),
        "-nodefaults".into(),
        "-display".into(),
        "none".into(),
        "-no-reboot".into(),
        "-S".into(),
        "-qmp".into(),
        "unix:qemu.socket,server=on,wait=off".into(),
        "-serial".into(),
        "stdio".into(),
        "-netdev".into(),
        "tap,id=net0,ifname=tap0,script=no,downscript=no".into(),
        "-device".into(),
        "virtio-net-pci,netdev=net0,romfile=".into(),
    ];
    add_drive(
        &mut args,
        "tools",
        Path::new("rootfs.ext4"),
        true,
        tools_block_device,
    );
    add_drive(&mut args, "rootfs", Path::new("user-rootfs"), false, true);
    args
}
fn add_drive(args: &mut Vec<String>, id: &str, path: &Path, read_only: bool, block_device: bool) {
    // JSON blockdev options avoid QEMU's comma-delimited path escaping.
    args.extend([
        "-blockdev".into(),
        serde_json::json!({"driver":"raw", "node-name": id,
        "read-only":read_only, "file":{"driver":if block_device { "host_device" } else { "file" }, "filename":path}})
        .to_string(),
        "-device".into(),
        format!("virtio-blk-pci,drive={id}"),
    ]);
}
fn unsupported<T>(operation: &str) -> SandboxCaptureResult<T> {
    Err(SandboxCaptureError::recoverable(anyhow::anyhow!(
        "QEMU {operation} is not supported yet; only Linux cold starts are available"
    )))
}

#[async_trait]
impl SandboxBackend for QemuSandbox {
    fn envd(&self) -> Option<Arc<dyn EnvdApi>> {
        self.envd
            .clone()
            .map(|envd| Arc::new(envd) as Arc<dyn EnvdApi>)
    }
    async fn start(&mut self) -> Result<()> {
        self.start_nowait().await?;
        if let Err(error) = self.wait_for_ready().await {
            let _ = self.stop().await;
            return Err(error);
        }
        Ok(())
    }
    async fn start_nowait(&mut self) -> Result<()> {
        if let Err(error) = self.boot().await {
            let _ = self.stop().await;
            return Err(error);
        }
        Ok(())
    }
    async fn wait_for_ready(&self) -> Result<()> {
        let global = ConfigManager::global_config();
        let envd = self.envd.as_ref().context("QEMU envd not connected")?;
        envd.wait_for_ready(Duration::from_secs(global.envd.init_timeout_secs), Duration::from_millis(global.envd.poll_ms)).await
            .context("QEMU guest not ready; q35 requires built-in CONFIG_PCI, CONFIG_VIRTIO_PCI, CONFIG_VIRTIO_BLK and CONFIG_VIRTIO_NET; inspect qemu-stdout.log")?;
        let common = &self.config.common;
        envd.init(
            common.env_vars.clone(),
            common.default_workdir.clone(),
            common.default_user.clone(),
        )
        .await?;
        if !common.extra_drives.is_empty() {
            super::guest_volumes::mount_initial_guest_drives(
                envd.clone(),
                common.extra_drives.iter().cloned().enumerate().collect(),
            )
            .await?;
        }
        if let Some(tools) = &self.resources.tools_ublk_device {
            UblkDeviceManager::global()
                .notify_sandbox_ready(&[tools.image_config_path()])
                .await;
        }
        if let (Some(source), Some(runtime)) = (
            &self.config.common.rootfs_image_config,
            &self.resources.rootfs_runtime,
        ) {
            UblkDeviceManager::global()
                .notify_sandbox_ready(&[&source.image_config_path, &runtime.image_config_path])
                .await;
        }
        Ok(())
    }
    async fn stop(&mut self) -> Result<()> {
        if let Some(envd) = self.envd.take() {
            envd.invalidate();
        }
        if self.process.is_some() {
            let _ = qmp::execute(&self.socket(), "quit").await;
        }
        if let Some(mut process) = self.process.take() {
            if tokio::time::timeout(Duration::from_secs(3), process.wait())
                .await
                .is_err()
            {
                process
                    .kill()
                    .await
                    .context("kill QEMU after quit timeout")?;
            }
        }
        if let Some(mut metadata) = self.metadata.take() {
            metadata.shutdown().await;
        }
        self.resources.release(Duration::from_secs(1)).await?;
        let _ = fs::remove_file(self.socket());
        Ok(())
    }
    async fn pause(&mut self) -> SandboxCaptureResult<Arc<dyn PausedSandboxState>> {
        unsupported("pause")
    }
    async fn resume(&mut self) -> Result<()> {
        bail!("QEMU resume is not supported yet")
    }
    async fn snapshot(
        &mut self,
        _: SnapshotId,
        _: SnapshotPlacement,
    ) -> SandboxCaptureResult<Arc<SnapshotHandle>> {
        unsupported("snapshot")
    }
    async fn snapshot_volumes(&mut self) -> SandboxCaptureResult<()> {
        unsupported("volume snapshots")
    }
    async fn fork(
        &mut self,
        _: &[SandboxForkSpec],
    ) -> SandboxCaptureResult<Vec<SandboxForkResult>> {
        unsupported("fork")
    }
    async fn capture_to_dir(
        &mut self,
        _: &Path,
    ) -> SandboxCaptureResult<(SnapshotRuntimeManifest, Option<Box<dyn Any + Send>>)> {
        unsupported("capture")
    }
    async fn freeze_and_snapshot_volumes(&mut self) -> SandboxCaptureResult<()> {
        Ok(())
    } // volumes rejected before launch
    async fn thaw_volumes(&mut self) -> Result<()> {
        Ok(())
    }
    fn host_interaction_ip(&self) -> Option<Ipv4Addr> {
        self.resources
            .network_slot
            .as_ref()
            .map(|s| s.host_interaction_ip)
    }
    fn runtime_info(&self) -> SandboxRuntimeInfo {
        let mut paths = self.image_paths();
        if let Some(runtime) = &self.resources.rootfs_runtime {
            paths.push(runtime.image_config_path.clone());
        }
        paths.extend(
            self.resources
                .extra_drive_runtimes
                .iter()
                .map(|r| r.image_config_path.clone()),
        );
        if let Some(tools) = &self.resources.tools_ublk_device {
            paths.push(tools.image_config_path().to_owned());
        }
        SandboxRuntimeInfo {
            rootfs_virtual_size: self.rootfs_size,
            runtime_artifacts: RuntimeArtifactSet::from_overlaybd_image_configs(paths),
        }
    }
    fn startup_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::from_overlaybd_image_configs(self.image_paths())
    }
    async fn update_network_policy(&mut self, policy: Option<SandboxNetworkPolicy>) -> Result<()> {
        self.resources
            .network_slot
            .as_mut()
            .context("QEMU is not running")?
            .set_egress_policy(policy.as_ref())?;
        self.config.common.network_policy = policy;
        Ok(())
    }
    fn update_custom_extension_params(&mut self, params: Option<CustomExtensionParams>) {
        self.config.common.custom_extension_params = params;
    }
}
impl QemuSandbox {
    fn image_paths(&self) -> Vec<PathBuf> {
        let mut paths: Vec<_> = self
            .config
            .common
            .rootfs_image_config
            .iter()
            .map(|i| i.image_config_path.clone())
            .collect();
        paths.extend(
            self.config
                .common
                .extra_drives
                .iter()
                .map(|d| d.image_config_path().to_owned()),
        );
        paths
    }
}
impl Drop for QemuSandbox {
    fn drop(&mut self) {
        if let Some(envd) = self.envd.take() {
            envd.invalidate();
        }
        let metadata = self.metadata.take();
        let mut child = self.process.take();
        if let Some(child) = child.as_mut() {
            let _ = child.start_kill();
        }
        let mut resources = std::mem::take(&mut self.resources);
        let work_dir = self.work_dir.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(mut child) = child {
                    let _ = child.wait().await;
                }
                if let Some(mut metadata) = metadata {
                    metadata.shutdown().await;
                }
                if let Err(error) = resources.release(Duration::from_secs(1)).await {
                    tracing::warn!(%error, "QEMU cleanup failed");
                }
                drop(work_dir);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn q35_boot_keeps_tools_layout_but_removes_firecracker_memory_options() {
        let mut config = FirecrackerSandboxConfig::new(
            "unused".into(),
            "/kernel".into(),
            "test".into(),
            "/image.json".into(),
        );
        config.boot_args = Some("console=ttyS0 pci=off damon_reclaim.enabled=Y init=/wrong root=/wrong aenv-custom.foo=1".into());
        config.vcpu_count = 3;
        config.mem_size_mib = 2048;
        let args = launch_args(
            &config,
            "ip=169.254.0.21::169.254.0.22:255.255.255.252:guest:eth0:off",
            false,
        );
        let option = |flag| args[args.iter().position(|a| a == flag).unwrap() + 1].clone();
        assert_eq!(option("-machine"), "q35");
        assert_eq!(option("-smp"), "3");
        assert_eq!(option("-m"), "2048");
        let boot = option("-append");
        assert!(boot.contains("root=/dev/vda") && boot.contains("init=/init"));
        assert!(boot.contains("aenv-custom.foo=1"));
        assert!(
            !boot.contains("pci=off")
                && !boot.contains("damon_reclaim")
                && !boot.contains("/wrong")
        );
        let blocks: Vec<serde_json::Value> = args
            .windows(2)
            .filter(|a| a[0] == "-blockdev")
            .map(|a| serde_json::from_str(&a[1]).unwrap())
            .collect();
        assert_eq!(blocks[0]["read-only"], true);
        assert_eq!(blocks[1]["read-only"], false);
        assert_eq!(blocks[0]["file"]["driver"], "file");
        assert_eq!(blocks[1]["file"]["driver"], "host_device");
    }
    #[test]
    fn qemu_disk_paths_are_json_encoded() {
        let mut args = vec![];
        add_drive(&mut args, "disk", Path::new("a,b\"c"), true, false);
        let block: serde_json::Value = serde_json::from_str(&args[1]).unwrap();
        assert_eq!(block["file"]["filename"], "a,b\"c");
    }
    #[test]
    fn unsupported_capture_is_recoverable() {
        assert!(!unsupported::<()>("pause").unwrap_err().is_terminal());
    }
}

#[cfg(test)]
mod kernel_tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn qemu_rejects_known_non_pci_kernel_before_boot() {
        let dir = tempfile::tempdir().unwrap();
        let kernel = dir.path().join("kernel");
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(b"# CONFIG_PCI is not set\n").unwrap();
        let mut bytes = b"ELF-test-IKCFG_ST".to_vec();
        bytes.extend(gzip.finish().unwrap());
        bytes.extend(b"IKCFG_ED");
        fs::write(&kernel, bytes).unwrap();
        let error = validate_kernel(&kernel).unwrap_err().to_string();
        assert!(error.contains("CONFIG_PCI=y") && error.contains("kernel.image_path"));
    }
}
