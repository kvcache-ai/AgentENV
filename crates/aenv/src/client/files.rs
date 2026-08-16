use anyhow::{anyhow, Context, Result};
use envd::filesystem::{
    EntryInfo, ListDirRequest, ListDirResponse, MakeDirRequest, MakeDirResponse, StatRequest,
    StatResponse,
};
use futures::TryStreamExt;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::io::ReaderStream;

use super::Client;
use crate::grpc::{RpcError, Transport, ENVD_PORT_STR};
use crate::progress::TransferProgress;

const SANDBOX_ID_HEADER: &str = "x-agentenv-sandbox-id";
const TARGET_PORT_HEADER: &str = "x-agentenv-target-port";
const ACCESS_TOKEN_HEADER: &str = "X-Access-Token";
const FILESYSTEM_SERVICE: &str = "filesystem.Filesystem";
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
pub(crate) const TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub struct EnvdFilesClient {
    base_url: String,
    http: reqwest::Client,
    transport: Transport,
}

impl EnvdFilesClient {
    fn new(base_url: &str, sandbox_id: &str, envd_access_token: Option<&str>) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            SANDBOX_ID_HEADER,
            HeaderValue::from_str(sandbox_id).context("invalid sandbox ID header value")?,
        );
        headers.insert(TARGET_PORT_HEADER, HeaderValue::from_static(ENVD_PORT_STR));
        if let Some(token) = envd_access_token {
            let mut token =
                HeaderValue::from_str(token).context("invalid envd access token header value")?;
            token.set_sensitive(true);
            headers.insert(ACCESS_TOKEN_HEADER, token);
        }
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&format!("aenv/{}", env!("CARGO_PKG_VERSION")))
                .context("invalid aenv user agent header value")?,
        );

        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .default_headers(headers);
        if crate::grpc::bypass_proxy_for_base_url(base_url) {
            builder = builder.no_proxy();
        }
        let client = builder.build().context("building envd files client")?;

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: client,
            transport: Transport::new(base_url, sandbox_id, envd_access_token)?,
        })
    }

    pub async fn upload(
        &self,
        local_path: &Path,
        remote_path: &str,
        username: Option<&str>,
        progress: &TransferProgress,
    ) -> Result<()> {
        // envd 0.5.13 returns a JSON body with a text/plain content type for
        // successful multipart uploads. The generated files_post helper
        // rejects that response before parsing it, so build the streaming
        // multipart request directly.
        let file = tokio::fs::File::open(local_path)
            .await
            .with_context(|| format!("opening local file {}", local_path.display()))?;
        let file_size = file
            .metadata()
            .await
            .with_context(|| format!("reading local file metadata {}", local_path.display()))?
            .len();
        let file_name = local_path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let (activity_tx, mut activity_rx) = watch::channel(());
        let progress = progress.clone();
        let stream = ReaderStream::new(file).map_ok(move |bytes| {
            let _ = activity_tx.send(());
            progress.inc(bytes.len() as u64);
            bytes
        });
        let part = Part::stream_with_length(reqwest::Body::wrap_stream(stream), file_size)
            .file_name(file_name);
        let form = Form::new().part("file", part);

        let mut request = self
            .http
            .post(format!("{}/files", self.base_url))
            .query(&[("path", remote_path)])
            .multipart(form);
        if let Some(username) = username {
            request = request.query(&[("username", username)]);
        }

        let operation = format!("uploading file to {remote_path}");
        let send = request.send();
        tokio::pin!(send);
        let idle = sleep(TRANSFER_IDLE_TIMEOUT);
        tokio::pin!(idle);
        let mut activity_open = true;
        let response = loop {
            tokio::select! {
                response = &mut send => {
                    break response.with_context(|| operation.clone())?;
                }
                changed = activity_rx.changed(), if activity_open => {
                    if changed.is_ok() {
                        idle.as_mut().reset(Instant::now() + TRANSFER_IDLE_TIMEOUT);
                    } else {
                        activity_open = false;
                    }
                }
                () = &mut idle => {
                    return Err(anyhow!(
                        "transfer made no progress for {} seconds",
                        TRANSFER_IDLE_TIMEOUT.as_secs()
                    ))
                    .context(operation);
                }
            }
        };
        ensure_http_success(response).await?;
        Ok(())
    }

    pub async fn download(
        &self,
        remote_path: &str,
        username: Option<&str>,
    ) -> Result<reqwest::Response> {
        let operation = format!("downloading file from {remote_path}");
        let mut request = self
            .http
            .get(format!("{}/files", self.base_url))
            .query(&[("path", remote_path)]);
        if let Some(username) = username {
            request = request.query(&[("username", username)]);
        }
        let response = timeout(TRANSFER_IDLE_TIMEOUT, request.send())
            .await
            .with_context(|| {
                format!(
                    "{operation}: no response for {} seconds",
                    TRANSFER_IDLE_TIMEOUT.as_secs()
                )
            })?
            .with_context(|| operation.clone())?;
        ensure_http_success(response).await
    }

    pub async fn stat(&self, path: &str, username: Option<&str>) -> Result<Option<EntryInfo>> {
        let response = self
            .transport
            .unary_service::<_, StatResponse>(
                FILESYSTEM_SERVICE,
                "Stat",
                StatRequest {
                    path: path.to_string(),
                },
                username,
            )
            .await;
        match response {
            Ok(response) => Ok(response.entry),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(error).with_context(|| format!("stating remote path {path}")),
        }
    }

    pub async fn list_dir(&self, path: &str) -> Result<Vec<EntryInfo>> {
        let response = self
            .transport
            .unary_service::<_, ListDirResponse>(
                FILESYSTEM_SERVICE,
                "ListDir",
                ListDirRequest {
                    path: path.to_string(),
                    depth: 1,
                },
                None,
            )
            .await
            .with_context(|| format!("listing remote directory {path}"))?;
        Ok(response.entries)
    }

    pub async fn make_dir(&self, path: &str) -> Result<()> {
        self.transport
            .unary_service::<_, MakeDirResponse>(
                FILESYSTEM_SERVICE,
                "MakeDir",
                MakeDirRequest {
                    path: path.to_string(),
                },
                None,
            )
            .await
            .with_context(|| format!("creating remote directory {path}"))?;
        Ok(())
    }
}

fn is_not_found(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<RpcError>()
        .is_some_and(|error| error.is_code("not_found"))
}

#[derive(Deserialize)]
struct EnvdErrorBody {
    message: String,
}

async fn ensure_http_success(response: reqwest::Response) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let content = read_error_body(response).await?;
    Err(format_envd_response_error(status, &content))
}

async fn read_error_body(mut response: reqwest::Response) -> Result<String> {
    let mut content = Vec::new();
    let mut truncated = false;
    loop {
        let chunk = timeout(TRANSFER_IDLE_TIMEOUT, response.chunk())
            .await
            .with_context(|| {
                format!(
                    "response body made no progress for {} seconds",
                    TRANSFER_IDLE_TIMEOUT.as_secs()
                )
            })?
            .context("reading response body")?;
        let Some(chunk) = chunk else {
            break;
        };
        let remaining = MAX_ERROR_BODY_BYTES.saturating_sub(content.len());
        if chunk.len() > remaining {
            content.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        content.extend_from_slice(&chunk);
        if content.len() == MAX_ERROR_BODY_BYTES {
            truncated = timeout(TRANSFER_IDLE_TIMEOUT, response.chunk())
                .await
                .with_context(|| {
                    format!(
                        "response body made no progress for {} seconds",
                        TRANSFER_IDLE_TIMEOUT.as_secs()
                    )
                })?
                .context("reading response body")?
                .is_some();
            break;
        }
    }

    let mut content = String::from_utf8_lossy(&content).into_owned();
    if truncated {
        content.push_str("\n[response body truncated]");
    }
    Ok(content)
}

fn format_envd_response_error(status: reqwest::StatusCode, content: &str) -> anyhow::Error {
    let detail = serde_json::from_str::<EnvdErrorBody>(content)
        .ok()
        .map(|error| error.message)
        .filter(|message| !message.trim().is_empty())
        .or_else(|| {
            let content = content.trim();
            (!content.is_empty()).then(|| content.to_string())
        });
    match detail {
        Some(detail) => anyhow!(detail),
        None => anyhow!("envd returned {status}"),
    }
}

impl Client {
    pub fn files(&self, sandbox_id: &str) -> Result<EnvdFilesClient> {
        let sandbox = self.get_sandbox(sandbox_id)?;
        EnvdFilesClient::new(&self.base, sandbox_id, sandbox.envd_access_token.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::format_envd_response_error;
    use reqwest::StatusCode;

    #[test]
    fn envd_json_error_displays_only_server_message_on_one_line() {
        let error = format_envd_response_error(
            StatusCode::BAD_REQUEST,
            r#"{"message":"path is a directory: /root","code":400}"#,
        );

        assert_eq!(error.to_string(), "path is a directory: /root");
        assert_eq!(format!("{error:?}"), "path is a directory: /root");
    }

    #[test]
    fn envd_plain_text_error_displays_only_server_message() {
        let error =
            format_envd_response_error(StatusCode::BAD_REQUEST, "invalid destination path\n");

        assert_eq!(error.to_string(), "invalid destination path");
    }

    #[test]
    fn envd_empty_error_falls_back_to_status() {
        let error = format_envd_response_error(StatusCode::BAD_GATEWAY, "");

        assert_eq!(error.to_string(), "envd returned 502 Bad Gateway");
    }
}
