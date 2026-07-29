use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::{anyhow, Result};
use tokio::time::{sleep, Duration};
use tracing::{debug, trace};

use envd::http_client::apis::{configuration::Configuration, default_api};
use envd::http_client::models::InitPostRequest;
use envd::process::ProcessClient;
use envd::reqwest::Client;

const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

static ENVD_HTTP_CLIENT: LazyLock<Client> = LazyLock::new(Client::new);

pub(crate) struct EnvdInstance {
    config: Configuration,
    grpc_address: String,
}

impl EnvdInstance {
    pub(crate) fn new(base_path: String) -> Self {
        let grpc_address = base_path.clone();
        Self {
            // Use full construction here to ensure the shared `Client` is used and no new instances are created.
            config: Configuration {
                base_path,
                user_agent: None,
                client: ENVD_HTTP_CLIENT.clone(),
                basic_auth: None,
                oauth_access_token: None,
                bearer_access_token: None,
                api_key: None,
            },
            grpc_address,
        }
    }

    /// Create a new gRPC `ProcessClient` connected to the envd daemon.
    #[tracing::instrument(skip(self), fields(grpc_address = %self.grpc_address))]
    pub(crate) async fn process_client(&self) -> Result<ProcessClient> {
        trace!(grpc_address = %self.grpc_address, "connecting envd process client");
        let result = ProcessClient::connect(&self.grpc_address)
            .await
            .map_err(|e| anyhow!("failed to connect process client: {e}"));
        trace!("connected to envd process client");
        result
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
            let probe_timeout = std::cmp::min(HEALTH_PROBE_TIMEOUT, remaining);
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

    #[tracing::instrument(skip(self, env_vars))]
    pub(crate) async fn init(
        &self,
        env_vars: Option<HashMap<String, String>>,
        default_workdir: Option<String>,
        default_user: Option<String>,
    ) -> Result<()> {
        debug!(has_env_vars = env_vars.is_some(), "initializing envd");
        let now = chrono::Utc::now().fixed_offset();
        let init_post_request = InitPostRequest {
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

    use tokio::net::TcpListener;

    use super::*;

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
        let envd = EnvdInstance::new(format!("http://{address}"));
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
}
