//! Guest startup for image-only Compose sandboxes.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Instant};

use crate::sandbox::{EnvdApi, ProcessOutput};

/// Budget for Docker readiness, image registration and Compose health checks.
const STARTUP_BUDGET: Duration = Duration::from_secs(300);
/// Extra envd time for `docker compose --wait` to report its own timeout.
const PROCESS_GRACE: Duration = Duration::from_secs(5);
const DOCKER_POLL_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Debug, Deserialize, Serialize)]
pub struct ComposeService {
    pub name: String,
    pub image: String,
    #[serde(rename = "localImage")]
    pub local_image: String,
    #[serde(default, rename = "driveID")]
    pub drive_id: String,
    #[serde(default, rename = "mountPath")]
    pub mount_path: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

/// Planner output, completed by the API with drive placement and image configs.
#[derive(Debug, Deserialize, Serialize)]
pub struct ComposePlan {
    pub compose: serde_json::Value,
    pub services: Vec<ComposeService>,
}

/// Runs once on fresh creation, before the sandbox is published as Running.
pub(super) struct ComposeBootstrap {
    pub(super) plan: ComposePlan,
}

impl ComposeBootstrap {
    pub(super) async fn initialize(&self, envd: Option<Arc<dyn EnvdApi>>) -> Result<()> {
        let envd = envd.context("sandbox guest agent is not connected")?;
        let deadline = Instant::now() + STARTUP_BUDGET;
        loop {
            let output = run(
                &*envd,
                deadline,
                &["/usr/local/bin/docker", "info", "--format", "{{.Driver}}"],
                &[],
            )
            .await?;
            if output.exit_code == 0 {
                ensure!(
                    output.stdout.trim() == "plain",
                    "Compose base image must run Docker with the plain snapshotter"
                );
                break;
            }
            ensure!(
                deadline.saturating_duration_since(Instant::now()) > DOCKER_POLL_INTERVAL,
                "Docker did not become ready (see /var/log/agentenv-compose): {}",
                output.stderr
            );
            sleep(DOCKER_POLL_INTERVAL).await;
        }
        // Registration updates the snapshotter's shared mapping and must remain serial.
        for service in &self.plan.services {
            check(
                run(
                    &*envd,
                    deadline,
                    &[
                        "/usr/local/bin/plain-snapshotter",
                        "register",
                        "--image-metadata",
                        "/dev/stdin",
                        service.local_image.as_str(),
                        service.mount_path.as_str(),
                    ],
                    &serde_json::to_vec(&service.config)?,
                )
                .await?,
            )?;
        }
        let wait_seconds = deadline
            .saturating_duration_since(Instant::now())
            .as_secs()
            .max(1)
            .to_string();
        check(
            run(
                &*envd,
                deadline,
                &[
                    "/usr/local/bin/docker",
                    "compose",
                    "--project-name",
                    "aenv",
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
                &serde_json::to_vec(&self.plan.compose)?,
            )
            .await?,
        )
    }
}

/// Run a guest command with a clean environment, so sandbox and envd variables
/// cannot affect Compose interpolation or its `COMPOSE_*` settings.
async fn run(
    envd: &dyn EnvdApi,
    deadline: Instant,
    command: &[&str],
    stdin: &[u8],
) -> Result<ProcessOutput> {
    let mut args = vec![
        "-i",
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME=/root",
        "COMPOSE_ANSI=never",
    ];
    args.extend_from_slice(command);
    let timeout = deadline.saturating_duration_since(Instant::now()) + PROCESS_GRACE;
    envd.run_with_stdin("/usr/bin/env", &args, stdin, timeout)
        .await
}

fn check(output: ProcessOutput) -> Result<()> {
    ensure!(
        output.exit_code == 0,
        "Compose startup failed: {}",
        output.stderr
    );
    Ok(())
}
