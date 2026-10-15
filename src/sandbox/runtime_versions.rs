use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tracing::{debug, warn};

use crate::sandbox::SandboxExecutor;

use crate::snapshot::SnapshotRuntimeVersions;

/// Read the guest versions and the identity of its actual VMM executable.
pub async fn probe_runtime_versions(
    sandbox: &impl SandboxExecutor,
    vmm_binary: PathBuf,
    tools_drive_version: String,
) -> Result<SnapshotRuntimeVersions> {
    SnapshotRuntimeVersions::probe(sandbox, vmm_binary, tools_drive_version).await
}

impl SnapshotRuntimeVersions {
    /// Refresh the VMM version for a new capture without changing captured guest assets.
    pub(crate) async fn refresh_firecracker(
        &mut self,
        binary: PathBuf,
        snapshot: PathBuf,
    ) -> Result<()> {
        let (version, digest, format) = tokio::join!(
            probe_vmm_version(binary.clone()),
            crate::digest::FileDigest::describe(&binary),
            probe_snapshot_format(binary.clone(), Some(snapshot)),
        );
        let digest = digest.context("read Firecracker binary identity")?;
        let format = format?;
        self.firecracker_version = version_or_unknown("vmm", version);
        self.firecracker_binary_sha256 = Some(digest.sha256);
        self.firecracker_snapshot_version = Some(format);
        Ok(())
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
            probe_vmm_version(vmm_binary.clone()),
            probe_envd_version(sandbox),
        );

        let kernel_version = version_or_unknown("kernel", kernel_version);
        let firecracker_version = version_or_unknown("vmm", firecracker_version);
        let envd_version = envd_version?;

        debug!(
            kernel_version,
            firecracker_version, envd_version, tools_drive_version, "probed runtime versions"
        );

        let mut versions = Self::new(
            kernel_version,
            firecracker_version,
            envd_version,
            tools_drive_version,
        );
        versions.firecracker_binary_sha256 = Some(
            crate::digest::FileDigest::describe(&vmm_binary)
                .await
                .context("read template runtime identity")?
                .sha256,
        );
        Ok(versions)
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

async fn probe_vmm_version(binary: PathBuf) -> Result<String> {
    parse_firecracker_version(&probe_vmm_output(binary, "--version", None).await?)
}

async fn probe_snapshot_format(binary: PathBuf, snapshot: Option<PathBuf>) -> Result<String> {
    let flag = if snapshot.is_some() {
        "--describe-snapshot"
    } else {
        "--snapshot-version"
    };
    let output = probe_vmm_output(binary, flag, snapshot).await?;
    output
        .lines()
        .find_map(|line| {
            let version = line.trim().strip_prefix('v')?;
            let parts: Vec<_> = version.split('.').collect();
            (parts.len() == 3 && parts.iter().all(|part| part.parse::<u64>().is_ok()))
                .then(|| version.to_string())
        })
        .context("Firecracker did not report a snapshot format")
}

async fn probe_vmm_output(
    vmm_binary: PathBuf,
    flag: &'static str,
    snapshot: Option<PathBuf>,
) -> Result<String> {
    use tokio::io::AsyncReadExt;
    let mut command = tokio::process::Command::new(&vmm_binary);
    command
        .arg(flag)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if let Some(snapshot) = snapshot {
        command.arg(snapshot);
    }
    let mut child = command.spawn().with_context(|| {
        format!(
            "run firecracker version probe using {}",
            vmm_binary.display()
        )
    })?;
    let mut stdout_pipe = child.stdout.take().context("missing probe stdout pipe")?;
    let mut stderr_pipe = child.stderr.take().context("missing probe stderr pipe")?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let collect = async {
        let (status, _, _) = tokio::try_join!(
            child.wait(),
            stdout_pipe.read_to_end(&mut stdout),
            stderr_pipe.read_to_end(&mut stderr)
        )?;
        Ok::<_, std::io::Error>(status)
    };
    let status = match tokio::time::timeout(Duration::from_secs(5), collect).await {
        Ok(result) => result.context("wait for firecracker version probe")?,
        Err(_) => {
            // kill() also waits: a timeout never leaves a running or zombie child.
            child
                .kill()
                .await
                .context("terminate timed out firecracker version probe")?;
            bail!("timed out waiting for firecracker version probe");
        }
    };
    let stdout = String::from_utf8_lossy(&stdout);
    let stderr = String::from_utf8_lossy(&stderr);
    let combined = if stderr.trim().is_empty() {
        stdout.into_owned()
    } else if stdout.trim().is_empty() {
        stderr.into_owned()
    } else {
        format!("{}\n{}", stdout.trim(), stderr.trim())
    };

    if !status.success() {
        bail!(
            "firecracker version probe failed: status={}, output='{}'",
            status.code().map_or_else(
                || "terminated by signal".to_string(),
                |code| code.to_string()
            ),
            combined.trim()
        );
    }

    Ok(combined)
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
    fn legacy_metadata_has_no_invented_runtime_identity() {
        let legacy = serde_json::json!({"kernel_version":"6.1.175", "firecracker_version":"source", "envd_version":"0.5.15", "tools_drive_version":"0.1.1"});
        let metadata: SnapshotRuntimeVersions = serde_json::from_value(legacy).unwrap();
        assert!(metadata.firecracker_binary_sha256.is_none());
        assert!(metadata.firecracker_snapshot_version.is_none());
    }

    #[tokio::test]
    async fn refresh_runtime_identity_preserves_guest_versions() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let binary = dir.path().join("firecracker");
        std::fs::write(&binary, "#!/bin/sh\ncase $1 in --version) echo 'Firecracker v1.15.1-patch-v1';; --snapshot-version|--describe-snapshot) echo v1000.0.0;; esac\n")?;
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))?;
        let mut metadata = SnapshotRuntimeVersions::new(
            "6.1.175".into(),
            "source-only".into(),
            "captured-envd".into(),
            "captured-tools".into(),
        );
        metadata
            .refresh_firecracker(binary.clone(), dir.path().join("state.bin"))
            .await?;
        assert_eq!(metadata.firecracker_version, "1.15.1-patch-v1");
        assert_eq!(
            metadata.firecracker_binary_sha256,
            Some(crate::digest::FileDigest::describe(&binary).await?.sha256)
        );
        assert_eq!(
            metadata.firecracker_snapshot_version.as_deref(),
            Some("1000.0.0")
        );
        assert_eq!(metadata.kernel_version, "6.1.175");
        assert_eq!(metadata.envd_version, "captured-envd");
        assert_eq!(metadata.tools_drive_version, "captured-tools");
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn timed_out_and_cancelled_probes_reap_their_processes() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        for cancel in [false, true] {
            let dir = tempfile::tempdir()?;
            let binary = dir.path().join("probe");
            let pid_file = dir.path().join("pid");
            std::fs::write(
                &binary,
                format!(
                    "#!/bin/sh\necho $$ > '{}'\nexec sleep 60\n",
                    pid_file.display()
                ),
            )?;
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))?;
            let task = tokio::spawn(probe_vmm_output(binary, "--version", None));
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while !pid_file.exists() {
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "probe did not start"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let pid = std::fs::read_to_string(&pid_file)?.trim().to_string();
            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                let error = task.await?.unwrap_err();
                assert!(error.to_string().contains("timed out"), "{error:#}");
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while PathBuf::from(format!("/proc/{pid}")).exists() {
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "probe process was not reaped"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Ok(())
    }

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
