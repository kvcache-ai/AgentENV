use crate::common;
use agentenv::snapshot::SnapshotPlacement;
use agentenv::types::SandboxId;

use agentenv::sandbox::{FirecrackerSandbox, SandboxExecutor};
use anyhow::Result;

#[tokio::test]
async fn overlaybd_ublk_lifecycle() -> Result<()> {
    common::setup().await;
    let config = common::default_sandbox_config()?;
    let mut invalid_config = config.clone();
    invalid_config.common.rootfs_virtual_size = Some(1);
    invalid_config.common.rootfs_allow_shrink = false;
    let mut rejected = FirecrackerSandbox::new(invalid_config, SandboxId::new(), None)?;
    let rejected_workspace = rejected.work_rootfs_path().parent().unwrap().to_path_buf();
    let error = rejected
        .start()
        .await
        .expect_err("a non-GiB-aligned size must fail validation");
    assert!(
        error.is::<uvm_ublk_daemon::InvalidRequestError>(),
        "{error:#}"
    );
    rejected.stop().await?;
    drop(rejected);
    assert!(
        !rejected_workspace.exists(),
        "a confirmed rejection must not retain the workspace"
    );

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
        .capture_snapshot(
            agentenv::snapshot::SnapshotId::generate(),
            SnapshotPlacement::Distributed,
        )
        .await?;
    sandbox.stop().await?;

    let mut resumed = FirecrackerSandbox::from_snapshot_files(
        std::sync::Arc::clone(&snapshot),
        agentenv::sandbox::FirecrackerCommonConfig::from_global_config()
            .expect("snapshot launch config"),
        SandboxId::new(),
        None,
    )?;
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
