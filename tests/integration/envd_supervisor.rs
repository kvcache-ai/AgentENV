use crate::common;
use agentenv::types::SandboxId;

use agentenv::sandbox::{FirecrackerSandbox, SandboxBackend, SandboxExecutor};
use anyhow::Result;
use tokio::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(90);

/// Kill envd inside a running sandbox and verify that runsv restarts it
/// automatically, restoring full sandbox operability without a recreate.
///
/// Covers acceptance criterion: "If envd is killed inside a running sandbox,
/// it comes back automatically and /health becomes healthy again."
#[tokio::test]
async fn envd_recovers_after_sigkill() -> Result<()> {
    common::setup().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let sandbox_config = common::default_sandbox_config()?;
        let mut sandbox = FirecrackerSandbox::new(sandbox_config, SandboxId::new(), None)?;
        sandbox.start().await?;

        // Record the envd PID that runsv is currently supervising.
        let pid_before = sandbox
            .run_command("sh", &["-c", "cat /run/sv/envd/supervise/pid"])
            .await?
            .stdout
            .trim()
            .to_string();
        assert!(!pid_before.is_empty(), "supervise/pid should be non-empty before kill");

        // SIGKILL the specific PID we just read rather than re-reading the
        // supervise file, which closes the race window between recording
        // pid_before and sending SIGKILL. The run_command call will likely
        // return a connection error because the envd gRPC socket closes when
        // the process is killed — that's expected, so the result is discarded.
        let _ = sandbox
            .run_command("sh", &["-c", &format!("kill -9 {pid_before}")])
            .await;

        // runsv restarts envd (enforcing a 1-second minimum between attempts).
        // Use the existing host-side readiness path — the same health-poll +
        // init sequence used during fresh boot and resume — to wait for it.
        SandboxBackend::wait_for_ready(&sandbox).await?;

        // A new envd process should be running under a different PID.
        let pid_after = sandbox
            .run_command("sh", &["-c", "cat /run/sv/envd/supervise/pid"])
            .await?
            .stdout
            .trim()
            .to_string();
        assert_ne!(
            pid_before, pid_after,
            "envd PID should change after SIGKILL + restart (before={pid_before}, after={pid_after})"
        );

        // exec, file I/O, and other sandbox operations must work without
        // recreating the sandbox.
        let out = sandbox.run_command("echo", &["recovered"]).await?;
        assert_eq!(out.exit_code, 0);
        assert!(
            out.stdout.contains("recovered"),
            "expected 'recovered' in stdout after restart, got: {:?}",
            out.stdout
        );

        sandbox.stop().await?;
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out after {:?}", TEST_TIMEOUT))?
}
