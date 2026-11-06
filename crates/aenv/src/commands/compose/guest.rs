//! Starts a planned Compose project inside the guest through envd.
//!
//! The guest image is built from images/compose/Dockerfile and must provide
//! /usr/bin/env, /bin/sh, mkdir, tar with gzip support, curl,
//! /usr/local/bin/docker and /usr/local/bin/plain-snapshotter.

use crate::grpc::{run_with_stdin, ProcessOutput, Transport};
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{sleep, Instant};

/// Extra time for `docker compose --wait` to report its own timeout.
const PROCESS_GRACE: Duration = Duration::from_secs(5);
const DOCKER_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// The server publishes each attached drive's image configuration in MMDS.
const READ_IMAGE_CONFIGS: &str = "token=$(curl -fsS -X PUT \
-H 'X-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token) && \
curl -fsS -H 'Accept: application/json' -H \"X-metadata-token: $token\" \
http://169.254.169.254/imageConfigs";

/// One service image drive as attached to the sandbox.
pub(super) struct Service {
    pub name: String,
    pub drive_id: String,
    pub mount_path: String,
    /// Placeholder image referenced by the rendered project.
    pub local_image: String,
}

#[derive(Debug, Deserialize)]
struct ImageConfigEntry {
    #[serde(rename = "driveId")]
    drive_id: Option<String>,
    config: serde_json::Value,
}

/// Waits for Docker, registers every service image and runs
/// `docker compose up --wait` within `budget`.
pub(super) async fn start(
    transport: &Transport,
    project_name: &str,
    compose: &serde_json::Value,
    services: &[Service],
    budget: Duration,
) -> Result<()> {
    let deadline = Instant::now() + budget;
    tokio::time::timeout(budget + PROCESS_GRACE, async {
        wait_for_docker(transport, deadline).await?;
        let metadata = run(transport, &["/bin/sh", "-c", READ_IMAGE_CONFIGS], &[]).await?;
        let configs = image_configs(&metadata)?;
        // Registration updates the snapshotter's shared mapping and must remain serial.
        for service in services {
            let config = configs
                .get(&service.drive_id)
                .with_context(|| format!("service {} image config is missing", service.name))?;
            check(
                run(
                    transport,
                    &[
                        "/usr/local/bin/plain-snapshotter",
                        "register",
                        "--image-metadata",
                        "/dev/stdin",
                        service.local_image.as_str(),
                        service.mount_path.as_str(),
                    ],
                    &serde_json::to_vec(config)?,
                )
                .await?,
            )
            .with_context(|| format!("register service {} image", service.name))?;
        }
        let wait_seconds = deadline
            .saturating_duration_since(Instant::now())
            .as_secs()
            .max(1)
            .to_string();
        check(
            run(
                transport,
                &[
                    "/usr/local/bin/docker",
                    "compose",
                    "--project-name",
                    project_name,
                    "--env-file",
                    "/dev/null",
                    "--file",
                    "-",
                    "up",
                    "--detach",
                    "--no-build",
                    "--pull",
                    "never",
                    "--wait",
                    "--wait-timeout",
                    wait_seconds.as_str(),
                ],
                &serde_json::to_vec(compose)?,
            )
            .await?,
        )
        .context("start Compose services")
    })
    .await
    .context("Compose startup deadline exceeded")?
}

/// Extracts the uploaded archive of project files at `/` and removes it. The
/// archive holds only the bound paths, so their parent directories are
/// created first.
pub(super) async fn extract(
    transport: &Transport,
    archive: &str,
    parents: &[String],
) -> Result<()> {
    let mut command = vec![
        "/bin/sh",
        "-c",
        "archive=$1; shift; mkdir -p -- \"$@\" && tar -xzpf \"$archive\" -C / && rm -f -- \"$archive\"",
        "sh",
        archive,
    ];
    command.extend(parents.iter().map(String::as_str));
    check(run(transport, &command, &[]).await?).context("extract the project files")
}

async fn wait_for_docker(transport: &Transport, deadline: Instant) -> Result<()> {
    loop {
        let output = run(
            transport,
            &["/usr/local/bin/docker", "info", "--format", "{{.Driver}}"],
            &[],
        )
        .await?;
        if output.exit_code == 0 {
            ensure!(
                String::from_utf8_lossy(&output.stdout).trim() == "plain",
                "Compose base image must run Docker with the plain snapshotter"
            );
            return Ok(());
        }
        ensure!(
            deadline.saturating_duration_since(Instant::now()) > DOCKER_POLL_INTERVAL,
            "Docker did not become ready (see /var/log/agentenv-compose in the sandbox): {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        sleep(DOCKER_POLL_INTERVAL).await;
    }
}

/// Indexes the MMDS `imageConfigs` document by attached drive ID.
fn image_configs(output: &ProcessOutput) -> Result<HashMap<String, serde_json::Value>> {
    ensure!(
        output.exit_code == 0,
        "read image configurations from instance metadata: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let entries: Vec<ImageConfigEntry> =
        serde_json::from_slice(&output.stdout).context("invalid image configurations")?;
    Ok(entries
        .into_iter()
        .filter_map(|entry| Some((entry.drive_id?, entry.config)))
        .collect())
}

/// Runs a guest command with a clean environment, so sandbox and envd variables
/// cannot affect Compose interpolation or its `COMPOSE_*` settings.
async fn run(transport: &Transport, command: &[&str], stdin: &[u8]) -> Result<ProcessOutput> {
    let mut args = vec![
        "-i",
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME=/root",
        "COMPOSE_ANSI=never",
    ];
    args.extend_from_slice(command);
    run_with_stdin(transport, "/usr/bin/env", &args, stdin).await
}

fn check(output: ProcessOutput) -> Result<()> {
    ensure!(
        output.exit_code == 0,
        "{}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn output(exit_code: i32, stdout: &str, stderr: &str) -> ProcessOutput {
        ProcessOutput {
            exit_code,
            stdout: stdout.into(),
            stderr: stderr.into(),
        }
    }

    #[test]
    fn indexes_attached_drive_image_configs() {
        let document = json!([
            {"mountPath": "/", "config": {"Cmd": ["/init"]}},
            {"driveId": "compose_0", "mountPath": "/mnt/compose_0", "config": {"Cmd": ["sh"]}},
            {"driveId": "compose_1", "mountPath": "/mnt/compose_1",
             "config": {"Entrypoint": ["docker-entrypoint.sh"], "WorkingDir": "/data"}},
        ]);
        let configs = image_configs(&output(0, &document.to_string(), "")).unwrap();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs["compose_0"], json!({"Cmd": ["sh"]}));
        assert_eq!(configs["compose_1"]["WorkingDir"], "/data");
    }

    #[test]
    fn reports_metadata_failures() {
        let error = image_configs(&output(22, "", "curl: (22) 401")).unwrap_err();
        assert!(error.to_string().contains("curl: (22) 401"), "{error}");
        assert!(image_configs(&output(0, "not json", "")).is_err());
    }

    #[test]
    fn check_reports_stderr() {
        check(output(0, "", "")).unwrap();
        assert_eq!(
            check(output(1, "", " unhealthy \n"))
                .unwrap_err()
                .to_string(),
            "unhealthy"
        );
    }
}
