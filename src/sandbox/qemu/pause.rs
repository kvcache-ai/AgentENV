//! Durable pause/resume. `vm-state.mig` carries CPU, device and RAM state;
//! the writable rootfs upper is flushed and reopened in place on resume.
use super::*;
use crate::sandbox::FirecrackerCommonConfig;
use serde::{Deserialize, Serialize};

pub(crate) const QEMU_BACKEND: &str = "qemu";
const VM_STATE_FILE_NAME: &str = "vm-state.mig";
const UEFI_VARS_FILE_NAME: &str = "uefi-vars.fd";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QemuPausedState {
    /// Always [`QEMU_BACKEND`]; lets the factory tell this record apart from
    /// a Firecracker paused state.
    backend: String,
    vm_state: PathBuf,
    uefi_vars: Option<PathBuf>,
    windows_test_disk: Option<WindowsTestDisk>,
    vcpu_count: u32,
    memory_mib: u32,
    kernel_image: PathBuf,
    boot_args: Option<String>,
    /// Rootfs points at the live runtime image config and its layers.
    common: FirecrackerCommonConfig,
}

/// Artifacts a resumed sandbox loads instead of cold booting.
#[derive(Clone, Debug)]
pub(super) struct QemuResume {
    pub(super) vm_state: PathBuf,
    pub(super) uefi_vars: Option<PathBuf>,
}

impl QemuPausedState {
    pub(crate) fn is_qemu_state(state: &serde_json::Value) -> bool {
        state.get("backend").and_then(serde_json::Value::as_str) == Some(QEMU_BACKEND)
    }

    pub(crate) fn decode(state: serde_json::Value) -> Result<Self> {
        let state: Self = serde_json::from_value(state).context("deserialize QEMU paused state")?;
        ensure!(state.backend == QEMU_BACKEND, "not a QEMU paused state");
        let rootfs = state
            .common
            .rootfs_image_config
            .as_ref()
            .context("QEMU paused rootfs missing")?;
        for path in [&state.vm_state, &rootfs.image_config_path]
            .into_iter()
            .chain(state.uefi_vars.as_ref())
        {
            ensure!(
                path.is_file(),
                "QEMU paused artifact missing: {}",
                path.display()
            );
        }
        Ok(state)
    }
}

impl PausedSandboxState for QemuPausedState {
    fn encode(&self) -> Result<serde_json::Value> {
        serde_json::to_value(self).context("serialize QEMU paused state")
    }

    fn runtime_artifacts(&self) -> RuntimeArtifactSet {
        RuntimeArtifactSet::from_overlaybd_image_configs(
            self.common
                .rootfs_image_config
                .iter()
                .map(|rootfs| rootfs.image_config_path.clone())
                .collect(),
        )
    }

    fn control_plane_port(&self) -> Option<u16> {
        let port = self.common.control_plane_port;
        (port != 0).then_some(port)
    }
}

impl QemuSandbox {
    pub(crate) fn from_paused_state(
        state: &QemuPausedState,
        id: SandboxId,
        envd_access_token: Option<EnvdAccessToken>,
        storage_root: &Path,
    ) -> Result<Self> {
        let mut common = state.common.clone();
        common.envd_access_token = envd_access_token;
        let metadata = common
            .mmds_metadata
            .get_or_insert_with(|| crate::sandbox::firecracker::MmdsMetadata::new(id, "unknown"));
        metadata.sandbox_id = id.to_string();
        metadata.set_access_token(common.envd_access_token.as_ref());
        let config = FirecrackerSandboxConfig {
            common,
            kernel_image: state.kernel_image.clone(),
            boot_args: state.boot_args.clone(),
            vcpu_count: state.vcpu_count,
            mem_size_mib: state.memory_mib,
        };
        let mut sandbox = Self::new(config, id, storage_root)?;
        sandbox.windows_test_disk = state.windows_test_disk.clone();
        sandbox.resume_from = Some(QemuResume {
            vm_state: state.vm_state.clone(),
            uefi_vars: state.uefi_vars.clone(),
        });
        Ok(sandbox)
    }

    /// Stop the guest, write `vm-state.mig`, then flush the rootfs upper so it
    /// can be reopened in place. The VM is left stopped; the orchestrator
    /// calls `stop` afterwards.
    pub(super) async fn pause_to_storage(&mut self) -> Result<QemuPausedState> {
        ensure!(
            self.config.common.extra_drives.is_empty(),
            "QEMU pause does not support attached drives yet"
        );
        ensure!(self.process.is_some(), "QEMU is not running");
        let root = &self.storage_root;
        let vm_state = root.join(VM_STATE_FILE_NAME);
        // Its pause record was consumed before this VM could run again.
        match tokio::fs::remove_file(&vm_state).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        // `stop` drains and flushes block I/O before returning.
        qmp::execute(&self.socket(), "stop").await?;
        qmp::execute_with_args(
            &self.socket(),
            "migrate",
            Some(serde_json::json!({"uri": format!("file:{}", vm_state.display())})),
        )
        .await?;
        self.wait_migration("migration").await?;

        let uefi_vars = if self.windows_test_disk.is_some() {
            let path = root.join(UEFI_VARS_FILE_NAME);
            tokio::fs::copy(self.work_dir.path().join(UEFI_VARS_FILE_NAME), &path)
                .await
                .context("save UEFI vars")?;
            Some(path)
        } else {
            None
        };

        let runtime = self
            .resources
            .rootfs_runtime
            .as_ref()
            .context("QEMU pause requires an OverlayBD rootfs runtime")?;
        UblkDeviceManager::global()
            .flush_for_checkpoint(&runtime.device)
            .await
            .context("flush QEMU rootfs")?;
        let mut common = self.config.common.clone();
        let source = common
            .rootfs_image_config
            .as_mut()
            .context("QEMU rootfs image config missing")?;
        source.image_config_path = runtime.image_config_path.clone();
        source.layers = Some(runtime.layers.clone());
        common.rootfs_virtual_size = Some(runtime.actual_virtual_size);

        Ok(QemuPausedState {
            backend: QEMU_BACKEND.into(),
            vm_state,
            uefi_vars,
            windows_test_disk: self.windows_test_disk.clone(),
            vcpu_count: self.config.vcpu_count,
            memory_mib: self.config.mem_size_mib,
            kernel_image: self.config.kernel_image.clone(),
            boot_args: self.config.boot_args.clone(),
            common,
        })
    }

    /// Load `vm-state.mig` into a QEMU started with `-S -incoming defer`.
    /// The guest stays stopped; callers issue `cont`.
    pub(super) async fn load_vm_state(&self) -> Result<()> {
        let Some(resume) = &self.resume_from else {
            return Ok(());
        };
        qmp::execute_with_args(
            &self.socket(),
            "migrate-incoming",
            Some(serde_json::json!({"uri": format!("file:{}", resume.vm_state.display())})),
        )
        .await?;
        self.wait_migration("incoming migration").await
    }

    async fn wait_migration(&self, operation: &str) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        loop {
            let status = qmp::execute(&self.socket(), "query-migrate").await?;
            match status.get("status").and_then(serde_json::Value::as_str) {
                Some("completed") => return Ok(()),
                Some("failed" | "cancelled") => bail!(
                    "QEMU {operation} failed: {}",
                    status
                        .get("error-desc")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown error")
                ),
                _ => {}
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "QEMU {operation} timed out"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paused_state_round_trips_and_requires_artifacts() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let rootfs = dir.path().join("image.json");
        let vm_state = dir.path().join(VM_STATE_FILE_NAME);
        fs::write(&rootfs, b"{}")?;
        let config = FirecrackerSandboxConfig::new(
            "unused".into(),
            "/kernel".into(),
            "test".into(),
            rootfs.clone(),
        );
        let state = QemuPausedState {
            backend: QEMU_BACKEND.into(),
            vm_state: vm_state.clone(),
            uefi_vars: None,
            windows_test_disk: None,
            vcpu_count: 2,
            memory_mib: 1024,
            kernel_image: "/kernel".into(),
            boot_args: None,
            common: config.common,
        };
        let encoded = state.encode()?;
        assert!(QemuPausedState::is_qemu_state(&encoded));
        assert!(!QemuPausedState::is_qemu_state(
            &serde_json::json!({"common": {}})
        ));
        assert!(QemuPausedState::decode(encoded.clone()).is_err());
        fs::write(&vm_state, b"")?;
        let decoded = QemuPausedState::decode(encoded)?;
        assert_eq!(
            decoded.runtime_artifacts(),
            RuntimeArtifactSet::from_overlaybd_image_configs(vec![rootfs])
        );
        Ok(())
    }
}
