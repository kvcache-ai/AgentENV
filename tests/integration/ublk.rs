use crate::common;
use agentenv::snapshot::SnapshotPlacement;
use agentenv::types::SandboxId;

use agentenv::sandbox::{
    FirecrackerSandbox, FirecrackerSandboxConfig, SandboxExecutor, UblkBackend, UblkConfig,
};
use anyhow::{Context, Result};

async fn sandbox_config_with_overlaybd_ublk() -> Result<FirecrackerSandboxConfig> {
    let mut config = common::default_sandbox_config()?;
    if matches!(
        config.common.ublk_config.as_ref().map(|cfg| &cfg.backend),
        Some(UblkBackend::Overlaybd(_))
    ) {
        return Ok(config);
    }

    let image_config_path = config
        .common
        .rootfs_image_config
        .as_ref()
        .context("default rootfs image config is missing")?
        .image_config_path
        .clone();

    if !image_config_path.exists() {
        anyhow::bail!(
            "missing default user image overlaybd config {}. Resolve the default template image first",
            image_config_path.display()
        );
    }

    config.common.ublk_config = Some(UblkConfig::overlaybd(image_config_path, false));
    Ok(config)
}

#[tokio::test]
async fn overlaybd_ublk_lifecycle() -> Result<()> {
    common::setup().await;
    let config = sandbox_config_with_overlaybd_ublk().await?;
    let mut sandbox = FirecrackerSandbox::new(config, SandboxId::new(), None)?;
    sandbox.start().await?;

    let output = sandbox.run_command("echo", &["hello-overlaybd"]).await?;
    assert_eq!(output.exit_code, 0);
    assert!(output.stdout.trim().contains("hello-overlaybd"));

    let rootfs_path = sandbox.work_rootfs_path();
    assert!(
        rootfs_path.exists(),
        "expected overlaybd-backed user image path to exist: {}",
        rootfs_path.display()
    );

    let output = sandbox
        .run_command(
            "sh",
            &["-c", "echo overlaybd-test-data > /tmp/overlaybd_test.txt"],
        )
        .await?;
    assert_eq!(output.exit_code, 0);

    let snapshot = sandbox
        .capture_snapshot(SnapshotPlacement::Distributed)
        .await?;
    sandbox.stop().await?;

    let mut resumed = FirecrackerSandbox::from_snapshot_config(&snapshot, SandboxId::new(), None)?;
    resumed.start().await?;

    let output = resumed
        .run_command("cat", &["/tmp/overlaybd_test.txt"])
        .await?;
    assert_eq!(output.exit_code, 0);
    assert!(
        output.stdout.trim().contains("overlaybd-test-data"),
        "expected file content after overlaybd resume, got: {}",
        output.stdout
    );

    resumed.stop().await?;
    Ok(())
}
