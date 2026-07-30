use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::{anyhow, Context, Result};
use tokio::time::{sleep, Duration};
use tracing::{debug, trace};

use envd::filesystem::FilesystemClient;
use envd::http_client::apis::{configuration::Configuration, default_api};
use envd::http_client::models::InitPostRequest;
use envd::process::ProcessClient;
use envd::reqwest::Client;

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
        let client = ProcessClient::connect(&self.grpc_address)
            .await
            .context("failed to connect process client")?;
        trace!("connected to envd process client");
        Ok(client)
    }

    /// Create a new gRPC `FilesystemClient` connected to the envd daemon.
    #[tracing::instrument(skip(self), fields(grpc_address = %self.grpc_address))]
    pub(crate) async fn filesystem_client(&self) -> Result<FilesystemClient> {
        trace!(grpc_address = %self.grpc_address, "connecting envd filesystem client");
        let client = FilesystemClient::connect(&self.grpc_address)
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

            match default_api::health_get(&self.config).await {
                Ok(_) => {
                    debug!(base_path = %self.config.base_path, "envd started successfully");
                    return Ok(());
                }
                Err(_) => {
                    let remaining = timeout - elapsed;
                    sleep(std::cmp::min(retry_interval, remaining)).await;
                }
            }
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
