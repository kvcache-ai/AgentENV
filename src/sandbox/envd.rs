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

const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

// Bootstrap addresses can be reused across sandbox runtime generations. Do not
// retain connections that may belong to the previous VM assigned the same IP.
static ENVD_BOOTSTRAP_HTTP_CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build envd bootstrap HTTP client")
});

pub(crate) struct EnvdInstance {
    config: Configuration,
    grpc_address: String,
}

impl EnvdInstance {
    pub(crate) fn new(base_path: String) -> Self {
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

    /// Uploads a local file into the guest at `guest_path` via envd's files
    /// API. `username` selects the guest account envd writes as; template
    /// builds pass "root" because plain OCI images have no other account.
    #[tracing::instrument(skip(self, local_path))]
    pub(crate) async fn upload_file(
        &self,
        local_path: &std::path::Path,
        guest_path: &str,
        username: &str,
    ) -> Result<()> {
        use envd::http_client::apis::files_api;

        match files_api::files_post(
            &self.config,
            Some(guest_path),
            Some(username),
            None,
            None,
            Some(local_path.to_path_buf()),
        )
        .await
        {
            Ok(_) => Ok(()),
            // envd currently returns a successful text/plain response for
            // this endpoint, while the generated client attempts to decode
            // every 2xx response as JSON. The upload has completed by the
            // time this response-body error is produced.
            Err(envd::http_client::apis::Error::Serde(error)) => {
                debug!(%error, "ignoring envd upload response-body decoding error");
                Ok(())
            }
            Err(error) => Err(anyhow::Error::new(error).context("upload file to sandbox via envd")),
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
    use std::net::SocketAddr;
    use std::time::Instant;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;

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

    async fn read_request(socket: &mut TcpStream) -> Result<String> {
        let mut request = Vec::with_capacity(1024);
        let mut chunk = [0_u8; 1024];

        let (header_end, content_length) = loop {
            let bytes_read = socket.read(&mut chunk).await?;
            if bytes_read == 0 {
                return Err(anyhow!("connection closed before request headers"));
            }
            request.extend_from_slice(&chunk[..bytes_read]);

            if request.len() > 64 * 1024 {
                return Err(anyhow!("request headers exceed test limit"));
            }

            let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let header_text = std::str::from_utf8(&request[..header_end + 4])?;
            let content_length = header_text
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            break (header_end + 4, content_length);
        };

        while request.len() < header_end + content_length {
            let bytes_read = socket.read(&mut chunk).await?;
            if bytes_read == 0 {
                return Err(anyhow!("connection closed before request body"));
            }
            request.extend_from_slice(&chunk[..bytes_read]);
        }

        let request_line = std::str::from_utf8(&request)?
            .lines()
            .next()
            .ok_or_else(|| anyhow!("request line missing"))?;
        Ok(request_line.to_owned())
    }

    // The first accepted socket models an idle connection retained from the
    // previous runtime generation. A second accept models connecting to the
    // next VM assigned the same address; bytes sent on the first socket would
    // therefore be stale cross-generation reuse.
    async fn serve_two_bootstrap_requests(listener: TcpListener) -> Result<Vec<SocketAddr>> {
        const RESPONSE: &[u8] =
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
        let mut peer_addresses = Vec::with_capacity(2);
        let mut held_connections = Vec::with_capacity(2);

        for expected_prefix in ["GET /health ", "POST /init "] {
            let (mut socket, peer_address) = listener.accept().await?;
            let request_line = read_request(&mut socket).await?;
            if !request_line.starts_with(expected_prefix) {
                return Err(anyhow!(
                    "expected request prefix {expected_prefix:?}, got {request_line:?}"
                ));
            }
            socket.write_all(RESPONSE).await?;
            peer_addresses.push(peer_address);

            // Keep the first socket open so a pooling client would send init
            // there and never reach the second accept.
            held_connections.push(socket);
        }

        Ok(peer_addresses)
    }

    async fn run_bootstrap_no_reuse_interaction() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(serve_two_bootstrap_requests(listener));
        let envd = EnvdInstance::new(format!("http://{address}"));

        envd.wait_for_ready(Duration::from_secs(2), Duration::from_millis(10))
            .await?;
        timeout(Duration::from_secs(2), envd.init(None, None, None))
            .await
            .map_err(|_| {
                anyhow!("init reused the health connection instead of opening a new one")
            })??;

        let peer_addresses = timeout(Duration::from_secs(2), server)
            .await
            .map_err(|_| anyhow!("timed out waiting for two bootstrap connections"))?
            .map_err(|err| anyhow!("mock envd task failed: {err}"))??;
        assert_eq!(peer_addresses.len(), 2);
        assert_ne!(peer_addresses[0].port(), peer_addresses[1].port());
        Ok(())
    }

    #[tokio::test]
    async fn bootstrap_http_client_does_not_reuse_connections() -> Result<()> {
        timeout(Duration::from_secs(5), run_bootstrap_no_reuse_interaction())
            .await
            .map_err(|_| anyhow!("timed out running complete envd bootstrap interaction"))??;
        Ok(())
    }
}
