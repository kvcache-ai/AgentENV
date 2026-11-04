//! Runs the Compose planner (`services/compose`) on the client.

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

/// Installed next to `aenv`.
pub(super) const PLANNER: &str = "aenv-compose-plan";
const PLAN_TIMEOUT: Duration = Duration::from_secs(30);
/// Must match exitInvalid in services/compose/cmd/main.go.
const EXIT_INVALID: i32 = 65;

/// `Request` in services/compose/plan.go.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Request<'a> {
    /// `Some("build")` returns a `BuildPlan` instead of a `Plan`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<&'a str>,
    pub compose: &'a str,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub compose_env: HashMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<String>,
}

/// `Plan` in services/compose/plan.go.
#[derive(Debug, Deserialize)]
pub(super) struct Plan {
    pub compose: serde_json::Value,
    pub services: Vec<Service>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Service {
    pub name: String,
    pub image: String,
    pub local_image: String,
}

pub(super) async fn prepare<T: DeserializeOwned>(
    planner: &Path,
    request: &Request<'_>,
) -> Result<T> {
    let mut child = tokio::process::Command::new(planner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| {
            format!(
                "run {}: rerun the aenv installer to install the Compose planner, or set --planner",
                planner.display()
            )
        })?;
    let mut stdin = child.stdin.take().context("planner stdin unavailable")?;
    let input = serde_json::to_vec(request)?;
    // Write concurrently while wait_with_output drains both output streams.
    let write = async move {
        stdin.write_all(&input).await?;
        stdin.shutdown().await
    };
    let (written, output) = tokio::time::timeout(PLAN_TIMEOUT, async {
        tokio::join!(write, child.wait_with_output())
    })
    .await
    .context("Compose planning timed out")?;
    let output = output.context("wait for Compose planner")?;
    if !output.status.success() {
        let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        if output.status.code() == Some(EXIT_INVALID) {
            bail!("invalid Compose project: {message}");
        }
        bail!("Compose planner failed: {message}");
    }
    written.context("write Compose planner input")?;
    serde_json::from_slice(&output.stdout).context("invalid Compose planner output")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_matches_go_request_json() {
        let request = Request {
            mode: None,
            compose: "services: {}",
            compose_env: [("TAG".to_owned(), "1".to_owned())].into(),
            profiles: vec!["worker".to_owned()],
        };
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            json!({"compose": "services: {}", "composeEnv": {"TAG": "1"}, "profiles": ["worker"]})
        );
        let request = Request {
            mode: Some("build"),
            compose: "services: {}",
            compose_env: HashMap::new(),
            profiles: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            json!({"mode": "build", "compose": "services: {}"})
        );
        let request = Request {
            mode: None,
            compose: "services: {}",
            compose_env: HashMap::new(),
            profiles: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            json!({"compose": "services: {}"})
        );
    }

    #[test]
    fn plan_matches_go_plan_json() {
        let plan: Plan = serde_json::from_value(json!({
            "compose": {"name": "aenv", "services": {}},
            "services": [{"name": "app", "image": "busybox", "localImage": "local"}],
        }))
        .unwrap();
        assert_eq!(plan.compose["name"], "aenv");
        assert_eq!(plan.services[0].name, "app");
        assert_eq!(plan.services[0].image, "busybox");
        assert_eq!(plan.services[0].local_image, "local");
    }

    #[cfg(unix)]
    fn executable_busy(error: &anyhow::Error) -> bool {
        let error = error.downcast_ref::<std::io::Error>();
        error.and_then(std::io::Error::raw_os_error) == Some(libc::ETXTBSY)
    }

    /// Runs the planner, retrying while another test thread's fork still holds
    /// the just-written script open, which makes exec fail with ETXTBSY.
    #[cfg(unix)]
    async fn prepare_written(planner: &Path, request: &Request<'_>) -> Result<Plan> {
        for _ in 0..100 {
            match prepare(planner, request).await {
                Err(error) if executable_busy(&error) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                result => return result,
            }
        }
        prepare(planner, request).await
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn maps_planner_exit_status() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let planner = directory.path().join(PLANNER);
        let request = Request {
            mode: None,
            compose: "services: {}",
            compose_env: HashMap::new(),
            profiles: Vec::new(),
        };
        for (script, expected) in [
            (
                "cat >/dev/null; echo 'unsupported key' >&2; exit 65",
                "invalid Compose project: unsupported key",
            ),
            (
                "cat >/dev/null; echo crashed >&2; exit 2",
                "Compose planner failed: crashed",
            ),
        ] {
            std::fs::write(&planner, format!("#!/bin/sh\n{script}\n")).unwrap();
            std::fs::set_permissions(&planner, std::fs::Permissions::from_mode(0o755)).unwrap();
            let error = prepare_written(&planner, &request).await.unwrap_err();
            assert_eq!(error.to_string(), expected);
        }
        std::fs::write(
            &planner,
            "#!/bin/sh\ncat >/dev/null\necho '{\"compose\":{},\"services\":[]}'\n",
        )
        .unwrap();
        assert!(prepare_written(&planner, &request)
            .await
            .unwrap()
            .services
            .is_empty());
    }
}
