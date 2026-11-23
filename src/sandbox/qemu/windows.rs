//! Windows disk-template boot path. Windows system disks are materialized into
//! an OverlayBD image before they are exposed to QEMU through ublk.
use super::*;
use std::net::TcpListener;
use std::sync::atomic::{AtomicU32, Ordering};

impl QemuSandbox {
    pub(super) async fn boot_windows(&mut self) -> Result<()> {
        let disk = self.windows_test_disk.as_ref().unwrap();
        let global = ConfigManager::global_config();
        let common = &self.config.common;
        ensure!(
            common.extra_drives.is_empty(),
            "Windows test disks do not support attached drives"
        );
        ensure!(
            common.network_policy.is_none(),
            "Windows test disks do not implement egress policies"
        );
        ensure!(
            common.envd_access_token.is_none(),
            "Windows test disks do not support secure mode yet"
        );
        let control_plane_port = common.control_plane_port;
        let binary = global.resolved_qemu_binary_path();
        let qemu_img = binary
            .parent()
            .context("QEMU binary has no parent")?
            .join("qemu-img");
        let info = Command::new(&qemu_img)
            .args(["info", "--output=json", "-f", "qcow2"])
            .arg(&disk.disk_path)
            .output()
            .await?;
        ensure!(
            info.status.success(),
            "read Windows disk: {}",
            String::from_utf8_lossy(&info.stderr)
        );
        let info: serde_json::Value = serde_json::from_slice(&info.stdout)?;
        ensure!(
            info.get("backing-filename").is_none(),
            "Windows base disk must be standalone, not a backing chain"
        );
        self.rootfs_size = info["virtual-size"].as_u64();
        let overlaybd_config = convert_qcow2_to_overlaybd(
            &disk.disk_path,
            global,
            self.work_dir.path(),
            self.rootfs_size.unwrap(),
        )
        .await?;
        let layers = crate::sandbox::ublk::resolve_layer_chain(&overlaybd_config, None).await?;
        let (source, source_root) = load_runtime_source(&overlaybd_config)?;
        let runtime = UblkDeviceManager::global()
            .create_overlaybd_runtime_device(
                CreateOverlaybdRuntimeDeviceRequest {
                    image: source,
                    source_root: &source_root,
                    global_config: &global.ublk.overlaybd.global_config_path,
                    runtime_dir: &self.storage_root.join("overlaybd"),
                    read_only: false,
                    runtime_upper_mode: overlaybd::config::UpperMode::LogStructured,
                    requested_virtual_size: self.rootfs_size,
                    known_source_virtual_size: self.rootfs_size,
                    allow_shrink: false,
                },
                layers,
            )
            .await?;
        // The API leaves the Linux rootfs slot empty; record the converted disk.
        if let Some(rootfs) = self.config.common.rootfs_image_config.as_mut() {
            rootfs.image_config_path = overlaybd_config;
        }
        self.resources.rootfs_runtime = Some(runtime);
        fs::copy(
            &disk.firmware_vars,
            self.work_dir.path().join("uefi-vars.fd"),
        )?;
        std::os::unix::fs::symlink(
            &disk.firmware_code,
            self.work_dir.path().join("uefi-code.fd"),
        )?;
        fs::write(self.work_dir.path().join("backend"), b"qemu-windows-test\n")?;

        // Existing AgentENV routes are IP + guest port. Give each test VM a
        // different loopback IP, preserving the envd port and proxy interface.
        static NEXT_IP: AtomicU32 = AtomicU32::new(1);
        let (ip, reservation) = (0..256)
            .find_map(|_| {
                let n = NEXT_IP.fetch_add(1, Ordering::Relaxed);
                let ip = Ipv4Addr::new(127, 77, ((n >> 8) & 255) as u8, (n & 255) as u8);
                TcpListener::bind((ip, control_plane_port))
                    .ok()
                    .map(|s| (ip, s))
            })
            .context("no free Windows test envd loopback address")?;
        let args = vec![
            "-machine".into(),
            "q35".into(),
            "-accel".into(),
            "kvm".into(),
            "-cpu".into(),
            "host".into(),
            "-smp".into(),
            self.config.vcpu_count.to_string(),
            "-m".into(),
            self.config.mem_size_mib.to_string(),
            "-drive".into(),
            "if=pflash,format=raw,readonly=on,file=uefi-code.fd".into(),
            "-drive".into(),
            "if=pflash,format=raw,file=uefi-vars.fd".into(),
            "-drive".into(),
            format!(
                "if=none,id=os,format=raw,file={}",
                self.resources
                    .rootfs_runtime
                    .as_ref()
                    .expect("Windows rootfs runtime must exist")
                    .device
                    .device_path()
                    .display()
            ),
            "-device".into(),
            "virtio-blk-pci,drive=os,bootindex=1".into(),
            "-netdev".into(),
            format!(
                "user,id=net0,hostfwd=tcp:{ip}:{}-:49983",
                control_plane_port
            ),
            "-device".into(),
            "virtio-net-pci,netdev=net0,romfile=".into(),
            "-display".into(),
            "none".into(),
            "-vga".into(),
            "std".into(),
            "-qmp".into(),
            "unix:qemu.socket,server=on,wait=off".into(),
            "-serial".into(),
            "stdio".into(),
            "-monitor".into(),
            "none".into(),
        ];
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
        drop(reservation);
        self.process =
            Some(crate::privileges::spawn_tokio_command_scoped(command, &[], || Ok(())).await?);
        self.windows_ip = Some(ip);
        self.envd = Some(EnvdInstance::new(
            format!("http://{ip}:{}", control_plane_port),
            None,
        ));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.process.as_mut().unwrap().try_wait()? {
                bail!(
                    "Windows QEMU exited ({status}): {}",
                    fs::read_to_string(self.log_dir.join("qemu-stderr.log")).unwrap_or_default()
                );
            }
            if qmp::execute(&self.socket(), "query-status").await.is_ok() {
                return Ok(());
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "Windows QMP startup timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

async fn convert_qcow2_to_overlaybd(
    source: &std::path::Path,
    config: &crate::cfg::AppConfig,
    work_dir: &std::path::Path,
    virtual_size: u64,
) -> Result<std::path::PathBuf> {
    let overlaybd_root = config.deps_path.join("overlaybd");
    let apply = overlaybd_root.join("bin/overlaybd-apply");
    let create = overlaybd_root.join("bin/overlaybd-create");
    let commit = overlaybd_root.join("bin/overlaybd-commit");
    ensure!(
        apply.is_file() && create.is_file() && commit.is_file(),
        "OverlayBD conversion tools are not installed"
    );

    let cache_root = config.home_path.join("windows-overlaybd");
    let key =
        crate::digest::sha256_digest(format!("{}:{}", source.display(), virtual_size).as_bytes());
    let cached = cache_root.join(&key).join("image.json");
    if cached.is_file() {
        return Ok(cached);
    }

    let conversion_dir = work_dir.join("windows-overlaybd-convert");
    tokio::fs::create_dir_all(&conversion_dir).await?;
    let data = conversion_dir.join("upper.data");
    let index = conversion_dir.join("upper.index");
    let result = conversion_dir.join("result.txt");
    let layer = conversion_dir.join("windows.commit");
    let image = conversion_dir.join("image.json");
    let size_gib = virtual_size.div_ceil(1024 * 1024 * 1024).max(1);
    run_overlaybd_command(
        Command::new(&create)
            .arg(&data)
            .arg(&index)
            .arg(size_gib.to_string()),
        "create Windows OverlayBD layer",
    )
    .await?;
    tokio::fs::write(
        &image,
        serde_json::to_vec_pretty(&serde_json::json!({
            "lowers": [],
            "upper": {"data": data, "index": index},
            "resultFile": result,
        }))?,
    )
    .await?;
    let convert_config = config.resolved_overlaybd_convert_global_config_path();
    run_overlaybd_command(
        Command::new(&apply)
            .arg("--from_qcow2")
            .arg("--no-extract-rootfs")
            .arg(format!(
                "--service_config_path={}",
                convert_config.display()
            ))
            .arg(source)
            .arg(&image),
        "convert Windows QCOW2 to OverlayBD",
    )
    .await?;
    run_overlaybd_command(
        Command::new(&commit).arg(&data).arg(&index).arg(&layer),
        "seal Windows OverlayBD layer",
    )
    .await?;

    let target_dir = cache_root.join(&key);
    tokio::fs::create_dir_all(&target_dir).await?;
    let final_layer = target_dir.join("windows.commit");
    tokio::fs::copy(&layer, &final_layer).await?;
    let final_image = target_dir.join("image.json");
    tokio::fs::write(
        &final_image,
        serde_json::to_vec_pretty(&serde_json::json!({
            "repoBlobUrl": "",
            "lowers": [{"file": final_layer, "size": tokio::fs::metadata(&final_layer).await?.len()}],
            "upper": {"index": "", "data": "", "target": "", "gzipIndex": ""},
            "resultFile": ""
        }))?,
    )
    .await?;
    Ok(final_image)
}

async fn run_overlaybd_command(command: &mut Command, operation: &str) -> Result<()> {
    let output = command.output().await.context(operation.to_owned())?;
    ensure!(
        output.status.success(),
        "{operation} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
