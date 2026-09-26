use std::time::Duration;

use agentenv::sandbox::{FirecrackerSandbox, SandboxBackend, SandboxLaunchConfig};
use agentenv::snapshot::{
    ResolvedStartupPackSource, SnapshotAlias, SnapshotId, SnapshotPublishMetadata,
    SnapshotPublishSource, SnapshotRuntimeVersions,
};
use agentenv::types::{SandboxId, SandboxResources};
use anyhow::{bail, Context, Result};

use crate::common;

/// Hardware-gated coverage of capture, asynchronous descriptor attachment,
/// the normal POSIX resolver, and a real restore through LocalPath.
#[tokio::test]
#[ignore = "requires enabled startup-pack config plus KVM/ublk"]
async fn posix_startup_pack_publish_resolve_and_resume() -> Result<()> {
    common::setup().await;
    let config = agentenv::cfg::ConfigManager::global_config();
    if !config.snapshot.memory_startup_pack.enabled
        || !config.snapshot.memory_startup_pack.consume_enabled
    {
        bail!("enable snapshot.memory_startup_pack record and consume for this test");
    }

    let root = tempfile::tempdir()?;
    let (_, manager, _) = common::snapshot_test_parts(root.path());
    let sandbox_config = common::default_sandbox_config()?;
    let cpu_count = sandbox_config.vcpu_count;
    let memory_mib = sandbox_config.mem_size_mib;
    let vmm_binary = sandbox_config.common.firecracker_binary.clone();
    let tools_drive_version = sandbox_config.common.tools_drive_version.clone();
    let mut original = FirecrackerSandbox::new(sandbox_config)?;
    let source_id = original.sandbox_id();
    let result = async {
        original.start().await?;
        let runtime_versions =
            SnapshotRuntimeVersions::probe(&original, vmm_binary, tools_drive_version).await?;
        let rootfs_bytes = SandboxBackend::runtime_info(&original)
            .rootfs_virtual_size
            .context("source sandbox has no rootfs virtual size")?;
        let captured = SandboxBackend::snapshot(&mut original).await?;
        let record = manager
            .publish_captured(
                SnapshotPublishMetadata {
                    id: SnapshotId::generate(),
                    alias: Some(SnapshotAlias::parse(&format!(
                        "posix-startup-pack-{}",
                        std::process::id()
                    ))?),
                    source: SnapshotPublishSource::Sandbox {
                        source_sandbox_id: source_id.to_string(),
                    },
                    context: agentenv::snapshot::CommandContext::default(),
                    startup: None,
                    resources: SandboxResources {
                        cpu_count,
                        memory_mib,
                        disk_size_mib: rootfs_bytes.div_ceil(1 << 20).try_into()?,
                    },
                    runtime_versions,
                    virtualization_mode: config.virtualization_mode,
                    image_configs: agentenv::types::ImageConfigs::new(),
                    volume_snapshots: Vec::new(),
                    custom_extension_params: None,
                },
                captured,
            )
            .await?;

        let attached = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let current = manager
                    .get(&record.id.to_string())
                    .await?
                    .context("published POSIX snapshot disappeared before descriptor attach")?;
                if current
                    .committed
                    .as_ref()
                    .and_then(|committed| committed.memory_startup.as_ref())
                    .is_some()
                {
                    return Ok::<_, anyhow::Error>(current);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .context("POSIX startup descriptor did not attach")??;

        let runnable = manager.resolve_runnable(attached).await?;
        let pack = runnable
            .manifest()
            .memory_startup_pack
            .as_ref()
            .context("POSIX resolver did not enable startup-pack consumption")?;
        let ResolvedStartupPackSource::LocalPath(path) = &pack.source else {
            bail!("POSIX resolver selected a non-local startup-pack source");
        };
        anyhow::ensure!(
            path.is_file(),
            "LocalPath manifest must exist: {}",
            path.display()
        );
        // The formal hardware smoke in scripts/verify-posix-startup-pack-smoke.py
        // also verifies a positive read from this restore's memory device.
        let manifest_bytes = std::fs::read(path)?;
        let manifest = overlaybd::startup_manifest::decode_manifest(&manifest_bytes)?;
        anyhow::ensure!(
            !manifest.prefix_pages.is_empty() || !manifest.ranges.is_empty(),
            "recorded startup manifest is empty"
        );

        let launch = SandboxLaunchConfig {
            sandbox_id: SandboxId::new(),
            snapshot_id: runnable.record().id.to_string(),
            env_vars: None,
            network: None,
            extra_mmds: serde_json::Map::new(),
            extra_drives: Vec::new(),
            extra_drives_in_snapshot: false,
            custom_extension_params: None,
            envd_access_token: None,
        };
        let mut restored = FirecrackerSandbox::from_snapshot(&runnable, &launch)?;
        let resumed = restored.start().await;
        let stopped = restored.stop().await;
        resumed?;
        stopped?;
        Ok(())
    }
    .await;
    let stopped = original.stop().await;
    result.and(stopped)
}
