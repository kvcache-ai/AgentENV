use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::sandbox::SandboxExecutor;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRuntimeVersions {
    pub kernel_version: String,
    pub firecracker_version: String,
    pub envd_version: String,
    #[serde(default)]
    pub tools_drive_version: String,
}

impl SnapshotRuntimeVersions {
    pub fn new(
        kernel_version: String,
        firecracker_version: String,
        envd_version: String,
        tools_drive_version: String,
    ) -> Self {
        Self {
            kernel_version,
            firecracker_version,
            envd_version,
            tools_drive_version,
        }
    }

    /// Read the versions a sandbox is running under. `vmm_binary` is the
    /// VMM the sandbox runs on, asked for its version, and
    /// `tools_drive_version` is the one the sandbox was given.
    #[tracing::instrument(skip(sandbox))]
    pub async fn probe(
        sandbox: &impl SandboxExecutor,
        vmm_binary: PathBuf,
        tools_drive_version: String,
    ) -> Result<Self> {
        let (kernel_version, firecracker_version, envd_version) = tokio::join!(
            probe_kernel_version(sandbox),
            probe_vmm_version(vmm_binary),
            probe_envd_version(sandbox),
        );

        let kernel_version = version_or_unknown("kernel", kernel_version);
        let firecracker_version = version_or_unknown("vmm", firecracker_version);
        let envd_version = envd_version?;

        debug!(
            kernel_version,
            firecracker_version, envd_version, tools_drive_version, "probed runtime versions"
        );

        Ok(Self::new(
            kernel_version,
            firecracker_version,
            envd_version,
            tools_drive_version,
        ))
    }
}

async fn probe_envd_version(sandbox: &impl SandboxExecutor) -> Result<String> {
    let output = sandbox
        .run_command("sh", &["-lc", "envd --version 2>&1 || envd -version 2>&1"])
        .await
        .context("run envd version probe in snapshot build sandbox")?;

    if output.exit_code != 0 {
        bail!(
            "envd version probe failed: exit_code={}, stdout='{}', stderr='{}'",
            output.exit_code,
            output.stdout.trim(),
            output.stderr.trim()
        );
    }

    parse_envd_version(&output.stdout)
}

async fn probe_kernel_version(sandbox: &impl SandboxExecutor) -> Result<String> {
    let output = sandbox
        .run_command("uname", &["-r"])
        .await
        .context("run kernel version probe in snapshot build sandbox")?;

    if output.exit_code != 0 {
        bail!(
            "kernel version probe failed: exit_code={}, stdout='{}', stderr='{}'",
            output.exit_code,
            output.stdout.trim(),
            output.stderr.trim()
        );
    }

    parse_kernel_version(&output.stdout)
}

async fn probe_vmm_version(vmm_binary: PathBuf) -> Result<String> {
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            std::process::Command::new(&vmm_binary)
                .arg("--version")
                .output()
                .with_context(|| {
                    format!(
                        "run firecracker version probe using {}",
                        vmm_binary.display()
                    )
                })
        }),
    )
    .await
    .context("timed out waiting for firecracker version probe task")?
    .context("join firecracker version probe task")??;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = if stderr.trim().is_empty() {
        stdout.into_owned()
    } else if stdout.trim().is_empty() {
        stderr.into_owned()
    } else {
        format!("{}\n{}", stdout.trim(), stderr.trim())
    };

    if !output.status.success() {
        bail!(
            "firecracker version probe failed: status={}, output='{}'",
            output.status.code().map_or_else(
                || "terminated by signal".to_string(),
                |code| code.to_string()
            ),
            combined.trim()
        );
    }

    parse_firecracker_version(&combined)
}

fn parse_envd_version(output: &str) -> Result<String> {
    parse_runtime_component_version(output, "envd")
}

fn parse_kernel_version(output: &str) -> Result<String> {
    let version = output.trim();
    if version.is_empty() {
        bail!("kernel version output is empty");
    }
    Ok(version.to_string())
}

fn parse_firecracker_version(output: &str) -> Result<String> {
    parse_runtime_component_version(output, "firecracker")
}

fn parse_runtime_component_version(output: &str, component: &str) -> Result<String> {
    for token in output
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_'))
        .filter(|token| !token.is_empty())
    {
        if let Some(version) = normalize_version_token(token) {
            return Ok(version);
        }
    }

    bail!(
        "failed to parse {} version from output: {}",
        component,
        output.trim()
    )
}

fn normalize_version_token(token: &str) -> Option<String> {
    let candidate = token.strip_prefix('v').unwrap_or(token);
    if candidate.is_empty() || !candidate.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    if !candidate.contains('.') {
        return None;
    }

    Some(candidate.to_string())
}

fn version_or_unknown(component: &str, result: Result<String>) -> String {
    match result {
        Ok(version) => version,
        Err(err) => {
            warn!(component, error = %err, "failed to probe runtime version; falling back to unknown");
            unknown_version()
        }
    }
}

fn unknown_version() -> String {
    "unknown".to_string()
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;

    #[test]
    fn parse_plain_semver() {
        assert_eq!(parse_envd_version("0.5.15\n").unwrap(), "0.5.15");
    }

    #[test]
    fn parse_version_with_prefix_words() {
        assert_eq!(
            parse_envd_version("envd version 0.5.15\n").unwrap(),
            "0.5.15"
        );
    }

    #[test]
    fn parse_version_with_v_prefix() {
        assert_eq!(parse_envd_version("envd v0.5.15").unwrap(), "0.5.15");
    }

    #[test]
    fn reject_unparseable_output() {
        assert!(parse_envd_version("envd release").is_err());
    }

    #[test]
    fn parse_plain_kernel_version() {
        assert_eq!(
            parse_kernel_version("6.1.12-agentenv\n").unwrap(),
            "6.1.12-agentenv"
        );
    }

    #[test]
    fn reject_empty_kernel_version() {
        assert!(parse_kernel_version("\n").is_err());
    }

    #[test]
    fn parse_firecracker_version_with_prefix() {
        assert_eq!(
            parse_firecracker_version("Firecracker v1.8.0").unwrap(),
            "1.8.0"
        );
    }

    #[test]
    fn fallback_to_unknown_when_optional_probe_fails() {
        assert_eq!(
            version_or_unknown("kernel", Err(anyhow!("boom"))),
            "unknown"
        );
    }
}
