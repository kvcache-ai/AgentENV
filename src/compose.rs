//! Compose preparation and the one-shot guest initialization contract.
use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

pub use aenv_compose_runtime::{ComposePlan, ComposeService, MAX_PLAN_BYTES};

const MAX_PLANNER_STDERR_BYTES: usize = 64 * 1024;

/// Kept only in a fresh launch plan, never persisted or replayed on restore.
#[derive(Debug)]
pub struct ComposeBootstrap {
    pub plan: ComposePlan,
    pub deadline: Instant,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct InvalidCompose(pub String);

/// Serialize before allocating a VM, and again at the guest protocol boundary.
pub(crate) fn encode_plan(plan: &ComposePlan) -> Result<Vec<u8>> {
    struct Frame(Vec<u8>);
    impl std::io::Write for Frame {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > (MAX_PLAN_BYTES - 1).saturating_sub(self.0.len()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Compose startup plan exceeds 4 MiB",
                ));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut frame = Frame(Vec::new());
    serde_json::to_writer(&mut frame, plan).map_err(|error| InvalidCompose(error.to_string()))?;
    frame.0.push(b'\n');
    Ok(frame.0)
}

async fn read_limited(
    reader: impl AsyncRead + Unpin,
    limit: usize,
    stream: &str,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    reader
        .take((limit + 1) as u64)
        .read_to_end(&mut output)
        .await?;
    if output.len() > limit {
        return Err(
            InvalidCompose(format!("Compose planner {stream} exceeds {limit} bytes")).into(),
        );
    }
    Ok(output)
}

pub(crate) async fn prepare(binary: &str, request: serde_json::Value) -> Result<ComposePlan> {
    let mut child = tokio::process::Command::new(binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("start Compose planner (install aenv-compose-plan)")?;
    let mut stdin = child.stdin.take().context("planner stdin unavailable")?;
    let stdout = child.stdout.take().context("planner stdout unavailable")?;
    let stderr = child.stderr.take().context("planner stderr unavailable")?;
    let input = serde_json::to_vec(&request)?;
    // Drain output concurrently with writing input to avoid pipe backpressure.
    let write = async move {
        stdin.write_all(&input).await?;
        stdin.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    };
    let result = tokio::try_join!(
        write,
        read_limited(stdout, MAX_PLAN_BYTES, "stdout"),
        read_limited(stderr, MAX_PLANNER_STDERR_BYTES, "stderr"),
        async { child.wait().await.context("wait for Compose planner") },
    );
    let ((), stdout, stderr, status) = match result {
        Ok(output) => output,
        Err(error) => {
            // kill() also reaps the child; dropping the read futures alone does
            // not stop a planner that is still expanding or writing its plan.
            let _ = child.kill().await;
            return Err(error);
        }
    };
    if !status.success() {
        let message = String::from_utf8_lossy(&stderr).trim().to_owned();
        if status.code() == Some(2) {
            return Err(InvalidCompose(message).into());
        }
        anyhow::bail!("Compose planner failed: {message}");
    }
    serde_json::from_slice(&stdout).context("invalid Compose planner output")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn final_frame_limit_includes_image_configs_and_newline() {
        let mut plan = ComposePlan {
            compose: json!({"services": {"app": {"image": "local"}}}),
            services: vec![ComposeService {
                name: "app".into(),
                image: "busybox".into(),
                local_image: "local".into(),
                drive_id: "compose_0".into(),
                mount_path: "/mnt/compose_0".into(),
                config: json!({"Env": [""]}),
            }],
        };
        let empty_size = encode_plan(&plan).unwrap().len();
        plan.services[0].config["Env"][0] = json!("x".repeat(MAX_PLAN_BYTES - empty_size));
        let frame = encode_plan(&plan).unwrap();
        assert_eq!(frame.len(), MAX_PLAN_BYTES);
        assert_eq!(frame.last(), Some(&b'\n'));
        plan.services[0].config["Env"][0] = json!("x".repeat(MAX_PLAN_BYTES - empty_size + 1));
        assert!(encode_plan(&plan).unwrap_err().is::<InvalidCompose>());
    }

    #[tokio::test]
    async fn planner_output_limits_kill_a_writer_that_never_exits() {
        use std::os::unix::fs::PermissionsExt;

        for stream in ["stdout", "stderr"] {
            let directory = tempfile::tempdir().unwrap();
            let script = directory.path().join("planner");
            let redirect = if stream == "stderr" { " >&2" } else { "" };
            std::fs::write(
                &script,
                format!(
                    "#!/bin/sh\ncat >/dev/null\nwhile :; do printf '%s' '{}'{redirect}; done\n",
                    "x".repeat(1024)
                ),
            )
            .unwrap();
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
            let error = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                prepare(script.to_str().unwrap(), json!({})),
            )
            .await
            .expect("oversized output must not wait for process exit")
            .unwrap_err();
            assert!(error.is::<InvalidCompose>(), "{error:#}");
            assert!(error.to_string().contains(stream), "{error:#}");
        }
    }
}
