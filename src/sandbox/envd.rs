use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, LazyLock,
};

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use tokio::time::{sleep, Duration, Instant};
use tonic::Request;
use tracing::{debug, trace};

use crate::sandbox::EnvdAccessToken;
use envd::filesystem::FilesystemClient;
use envd::http_client::apis::{
    configuration::{ApiKey, Configuration},
    default_api,
};
use envd::http_client::models::InitPostRequest;
use envd::process::{process_event, ProcessClient, ProcessConfig, StartRequest};
use envd::reqwest::Client;

mod user;

use crate::cfg::{ConfigManager, EnvdConfig};
const BOOT_READY_PROBE: &str = r#"
pid1="$(cat /proc/1/comm 2>/dev/null)"
if [ "$pid1" != "systemd" ]; then
  exit 0
fi
[ -d /run/systemd/system ] || exit 1
systemctl=""
for candidate in /usr/bin/systemctl /bin/systemctl; do
  if [ -x "$candidate" ]; then
    systemctl="$candidate"
    break
  fi
done
if [ -z "$systemctl" ]; then
  exit 1
fi
load_state="$($systemctl show -p LoadState --value systemd-tmpfiles-setup.service 2>/dev/null)" || exit 1
if [ "$load_state" = "not-found" ]; then
  exit 0
fi
active_state="$($systemctl show -p ActiveState --value systemd-tmpfiles-setup.service 2>/dev/null)" || exit 1
sub_state="$($systemctl show -p SubState --value systemd-tmpfiles-setup.service 2>/dev/null)" || exit 1
basic_state="$($systemctl show -p ActiveState --value basic.target 2>/dev/null)" || exit 1
[ "$basic_state" = "active" ] || exit 1
case "$active_state:$sub_state" in
  active:exited|failed:failed) exit 0 ;;
  *) exit 1 ;;
esac
"#;

// Bootstrap addresses can be reused across sandbox runtime generations. Do not
// retain connections that may belong to the previous VM assigned the same IP.
static ENVD_BOOTSTRAP_HTTP_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build envd bootstrap HTTP client")
});

#[derive(Clone)]
pub(crate) struct EnvdInstance {
    config: Configuration,
    grpc_address: String,
    access_token: Option<EnvdAccessToken>,
    live: Arc<AtomicBool>,
    health_probe_timeout: Duration,
    boot_ready_probe_timeout: Duration,
}

impl EnvdInstance {
    pub(crate) async fn metrics(&self) -> Result<super::SandboxMetric> {
        self.ensure_live()?;
        let raw = default_api::metrics_get(&self.config).await?;
        raw.try_into()
    }

    pub(crate) fn new(base_path: String, access_token: Option<EnvdAccessToken>) -> Self {
        Self::new_with_config(
            base_path,
            access_token,
            &ConfigManager::global_config().envd,
        )
    }

    fn new_with_config(
        base_path: String,
        access_token: Option<EnvdAccessToken>,
        config: &EnvdConfig,
    ) -> Self {
        let grpc_address = base_path.clone();
        Self {
            // Share client configuration without retaining bootstrap TCP
            // connections across sandbox runtime generations.
            config: Configuration {
                base_path,
                user_agent: None,
                client: ENVD_BOOTSTRAP_HTTP_CLIENT.clone(),
                basic_auth: None,
                oauth_access_token: None,
                bearer_access_token: None,
                api_key: access_token.as_ref().map(|token| ApiKey {
                    prefix: None,
                    key: token.expose().to_owned(),
                }),
            },
            grpc_address,
            access_token,
            live: Arc::new(AtomicBool::new(true)),
            health_probe_timeout: Duration::from_millis(config.health_probe_timeout_ms),
            boot_ready_probe_timeout: Duration::from_millis(config.boot_ready_probe_timeout_ms),
        }
    }

    fn ensure_live(&self) -> Result<()> {
        if self.live.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(anyhow!("sandbox runtime is no longer active"))
        }
    }

    pub(crate) fn invalidate(&self) {
        self.live.store(false, Ordering::Release);
    }

    /// Create a new gRPC `ProcessClient` connected to the envd daemon.
    #[tracing::instrument(skip(self), fields(grpc_address = %self.grpc_address))]
    pub(crate) async fn process_client(&self) -> Result<ProcessClient> {
        self.ensure_live()?;
        trace!(grpc_address = %self.grpc_address, "connecting envd process client");
        let client = ProcessClient::connect(
            &self.grpc_address,
            self.access_token.as_ref().map(EnvdAccessToken::expose),
        )
        .await
        .context("failed to connect process client")?;
        trace!("connected to envd process client");
        Ok(client)
    }

    /// Create a new gRPC `FilesystemClient` connected to the envd daemon.
    #[tracing::instrument(skip(self), fields(grpc_address = %self.grpc_address))]
    pub(crate) async fn filesystem_client(&self) -> Result<FilesystemClient> {
        self.ensure_live()?;
        trace!(grpc_address = %self.grpc_address, "connecting envd filesystem client");
        let client = FilesystemClient::connect(
            &self.grpc_address,
            self.access_token.as_ref().map(EnvdAccessToken::expose),
        )
        .await
        .context("failed to connect filesystem client")?;
        trace!("connected to envd filesystem client");
        Ok(client)
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn wait_for_ready(
        &self,
        timeout: Duration,
        retry_interval: Duration,
    ) -> Result<()> {
        debug!(
            base_path = %self.config.base_path,
            timeout_ms = timeout.as_millis(),
            retry_interval_ms = retry_interval.as_millis(),
            "waiting for envd"
        );
        let start = std::time::Instant::now();

        loop {
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Err(anyhow!("timed out waiting for envd"));
            }

            let remaining = timeout - elapsed;
            let probe_timeout = std::cmp::min(self.health_probe_timeout, remaining);
            match tokio::time::timeout(probe_timeout, default_api::health_get(&self.config)).await {
                Ok(Ok(_)) => {
                    debug!(base_path = %self.config.base_path, "envd started successfully");
                    return Ok(());
                }
                Ok(Err(error)) => {
                    trace!(%error, "envd health probe failed");
                }
                Err(_) => {
                    trace!(
                        timeout_ms = probe_timeout.as_millis(),
                        "envd health probe timed out"
                    );
                }
            }

            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(anyhow!("timed out waiting for envd"));
            }
            sleep(std::cmp::min(retry_interval, remaining)).await;
        }
    }

    /// Wait until boot-time filesystem cleanup can no longer race the first command.
    pub(crate) fn wait_for_boot_ready(
        self,
        timeout: Duration,
        retry_interval: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<(), String>> + Send + 'static>,
    > {
        Box::pin(async move {
            let started = Instant::now();

            loop {
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Err("timed out waiting for guest boot services".to_string());
                }
                let probe_timeout = std::cmp::min(self.boot_ready_probe_timeout, remaining);
                match self.clone().probe_boot_ready(probe_timeout).await {
                    Ok(0) => {
                        debug!("guest boot services completed");
                        return Ok(());
                    }
                    Ok(exit_code) => {
                        trace!(exit_code, "guest boot readiness probe is not ready");
                    }
                    Err(error) => {
                        trace!(%error, "guest boot readiness probe failed");
                    }
                }

                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Err("timed out waiting for guest boot services".to_string());
                }
                sleep(std::cmp::min(retry_interval, remaining)).await;
            }
        })
    }

    fn probe_boot_ready(
        self,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<i32, String>> + Send + 'static>,
    > {
        Box::pin(async move {
            let probe = async move {
                self.ensure_live().map_err(|error| error.to_string())?;
                let request = Request::new(StartRequest {
                    process: Some(ProcessConfig {
                        cmd: "/agentenv/bin/busybox".to_string(),
                        args: vec![
                            "sh".to_string(),
                            "-c".to_string(),
                            BOOT_READY_PROBE.to_string(),
                        ],
                        envs: Default::default(),
                        cwd: Some("/".to_string()),
                    }),
                    pty: None,
                    tag: None,
                    stdin: Some(false),
                });
                let mut client = ProcessClient::connect_now(
                    &self.grpc_address,
                    self.access_token.as_ref().map(EnvdAccessToken::expose),
                )
                .map_err(|error| format!("connect guest boot readiness probe: {error}"))?;
                let mut stream = client
                    .start(request)
                    .await
                    .map_err(|error| format!("start guest boot readiness probe: {error}"))?
                    .into_inner();
                while let Some(response) = stream.next().await {
                    let response = response
                        .map_err(|error| format!("guest boot readiness probe stream: {error}"))?;
                    let Some(event) = response.event.and_then(|wrapper| wrapper.event) else {
                        continue;
                    };
                    if let process_event::Event::End(end) = event {
                        return Ok(end.exit_code);
                    }
                }
                Err("guest boot readiness probe ended without an exit event".to_string())
            };
            match tokio::time::timeout(timeout, probe).await {
                Ok(result) => result,
                Err(_) => Err("guest boot readiness probe timed out".to_string()),
            }
        })
    }

    #[tracing::instrument(skip(self, env_vars))]
    pub(crate) async fn init(
        &self,
        env_vars: Option<HashMap<String, String>>,
        default_workdir: Option<String>,
        default_user: Option<String>,
    ) -> Result<()> {
        let default_user = match default_user {
            Some(user) if user::needs_resolution(&user) => {
                // Authenticate this runtime first, including after restore.
                // Account setup runs before the sandbox becomes available.
                self.post_init(None, Some("/".to_owned()), Some("root".to_owned()))
                    .await?;
                Some(
                    tokio::time::timeout(Duration::from_secs(30), self.resolve_default_user(&user))
                        .await
                        .context("timed out resolving Dockerfile USER")??,
                )
            }
            user => user,
        };
        self.post_init(env_vars, default_workdir, default_user)
            .await
    }

    async fn post_init(
        &self,
        env_vars: Option<HashMap<String, String>>,
        default_workdir: Option<String>,
        default_user: Option<String>,
    ) -> Result<()> {
        debug!(has_env_vars = env_vars.is_some(), "initializing envd");
        let now = chrono::Utc::now().fixed_offset();
        let init_post_request = InitPostRequest {
            access_token: self
                .access_token
                .as_ref()
                .map(|token| token.expose().to_owned()),
            env_vars,
            default_workdir,
            default_user,
            timestamp: Some(now),
            ..Default::default()
        };
        default_api::init_post(&self.config, Some(init_post_request)).await?;
        debug!("envd initialized");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use axum::extract::{Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use super::*;

    async fn capture_init_request(
        State(sender): State<mpsc::Sender<(HeaderMap, Value)>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> StatusCode {
        sender.send((headers, body)).await.unwrap();
        StatusCode::NO_CONTENT
    }

    #[tokio::test]
    async fn sandbox_metrics_uses_guest_auth_and_rejects_invalidated_runtime() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let token = crate::sandbox::SandboxAccessTokenGenerator::new("metrics-test-seed")?
            .generate(crate::types::SandboxId::new());
        let expected_token = token.clone();
        let app = Router::new().route(
            "/metrics",
            get(move |headers: HeaderMap| async move {
                assert_eq!(headers["x-access-token"], expected_token.expose());
                Json(serde_json::json!({
                    "ts": 1700000000, "cpu_count": 2, "cpu_used_pct": 25.0,
                    "mem_used": 4000000000i64, "mem_total": 8000000000i64,
                    "mem_cache": 3000000000i64, "disk_used": 9000000000i64,
                    "disk_total": 20000000000i64
                }))
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let envd = EnvdInstance::new(format!("http://{address}"), Some(token));
        let sample = envd.metrics().await?;
        assert_eq!(sample.mem_total, 8000000000);
        assert_eq!(sample.disk_total, 20000000000);
        envd.invalidate();
        assert!(envd.metrics().await.is_err());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn init_sends_access_token_in_header_and_body() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (sender, mut receiver) = mpsc::channel(1);
        let app = Router::new()
            .route("/init", post(capture_init_request))
            .with_state(sender);
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let token = crate::sandbox::SandboxAccessTokenGenerator::new("envd-init-test-seed")?
            .generate(crate::types::SandboxId::new());
        let envd = EnvdInstance::new(format!("http://{address}"), Some(token.clone()));

        envd.init(None, None, None).await?;

        let (headers, body) = receiver.recv().await.expect("captured init request");
        assert_eq!(headers["x-access-token"], token.expose());
        assert_eq!(body["accessToken"], token.expose());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn init_resolves_numeric_default_user_and_preserves_image_environment() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (sender, mut receiver) = mpsc::channel(2);
        let token = crate::sandbox::SandboxAccessTokenGenerator::new("numeric-user-test-seed")?
            .generate(crate::types::SandboxId::new());
        let file_token = token.clone();
        let app = Router::new()
            .route("/init", post(capture_init_request))
            .route("/files", get(move |headers: HeaderMap, Query(query): Query<HashMap<String, String>>| async move {
                assert_eq!(headers["x-access-token"], file_token.expose());
                assert_eq!(query["path"], "/etc/passwd");
                assert_eq!(query["username"], "root");
                b"root:x:0:0:r\xffot:/root:/bin/sh\nother:x:1000:1000:\xff:/home/other:/bin/sh\n".to_vec()
            }))
            .with_state(sender);
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let envd = EnvdInstance::new(format!("http://{address}"), Some(token.clone()));
        envd.init(
            Some(HashMap::from([("HOME".to_owned(), "/app".to_owned())])),
            Some("/work".to_owned()),
            Some("0".to_owned()),
        )
        .await?;
        let (headers, bootstrap) = receiver.recv().await.unwrap();
        assert_eq!(headers["x-access-token"], token.expose());
        assert_eq!(bootstrap["accessToken"], token.expose());
        let (_, body) = receiver.recv().await.unwrap();
        assert_eq!(body["defaultUser"], "root");
        assert_eq!(body["defaultWorkdir"], "/work");
        assert_eq!(body["envVars"]["HOME"], "/app");
        server.abort();
        Ok(())
    }

    #[test]
    fn boot_readiness_accepts_completed_failure_but_not_ongoing_cleanup() {
        let check = BOOT_READY_PROBE
            .split("[ \"$basic_state\" = \"active\" ] || exit 1")
            .nth(1)
            .unwrap();
        for (active, sub, expected) in [
            ("active", "exited", true),
            ("failed", "failed", true),
            ("activating", "start", false),
            ("inactive", "dead", false),
        ] {
            let status = std::process::Command::new("sh")
                .args(["-c", check])
                .env("active_state", active)
                .env("sub_state", sub)
                .status()
                .unwrap();
            assert_eq!(status.success(), expected, "{active}:{sub}");
        }
    }

    #[tokio::test]
    async fn configured_health_probe_timeout_allows_retry_before_overall_deadline() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let config = EnvdConfig {
            health_probe_timeout_ms: 20,
            ..Default::default()
        };
        let envd = EnvdInstance::new_with_config(format!("http://{address}"), None, &config);
        let waiting = tokio::spawn(async move {
            envd.wait_for_ready(Duration::from_secs(3), Duration::from_millis(1))
                .await
        });
        let (_first, _) = listener.accept().await?;
        let retry = tokio::time::timeout(Duration::from_millis(500), listener.accept()).await;
        waiting.abort();
        retry
            .expect("configured probe budget should allow a retry before the default one second")?;
        Ok(())
    }

    #[tokio::test]
    async fn readiness_deadline_bounds_a_hung_health_probe() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await?;
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok::<_, anyhow::Error>(())
        });
        let envd = EnvdInstance::new(format!("http://{address}"), None);
        let deadline = Duration::from_millis(50);
        let started = Instant::now();

        let error = envd
            .wait_for_ready(deadline, Duration::from_millis(1))
            .await
            .expect_err("hung health probe should reach the readiness deadline");

        server.abort();
        assert!(error.to_string().contains("timed out waiting for envd"));
        assert!(started.elapsed() < Duration::from_millis(500));
        Ok(())
    }

    #[tokio::test]
    async fn invalidated_runtime_rejects_new_envd_clients() {
        let envd = EnvdInstance::new("http://127.0.0.1:1".to_owned(), None);
        let stale = envd.clone();
        envd.invalidate();

        assert!(stale.process_client().await.is_err());
        assert!(stale.filesystem_client().await.is_err());
        assert!(stale
            .probe_boot_ready(Duration::from_secs(1))
            .await
            .unwrap_err()
            .contains("no longer active"));
    }

    #[tokio::test]
    async fn boot_probe_deadline_bounds_a_hung_process_start() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await?;
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok::<_, anyhow::Error>(())
        });
        let envd = EnvdInstance::new(format!("http://{address}"), None);
        let deadline = Duration::from_millis(50);
        let started = Instant::now();

        let error = envd
            .probe_boot_ready(deadline)
            .await
            .expect_err("hung process start should reach the probe deadline");

        server.abort();
        assert!(error.contains("timed out"));
        assert!(started.elapsed() < Duration::from_millis(500));
        Ok(())
    }
}
