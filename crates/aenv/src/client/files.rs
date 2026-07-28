use anyhow::{anyhow, Context, Result};
use envd::http_client::apis::{configuration::Configuration, files_api, Error as EnvdApiError};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

use super::Client;
use crate::grpc::ENVD_PORT_STR;

const API_KEY_HEADER: &str = "X-API-Key";
const SANDBOX_ID_HEADER: &str = "x-agentenv-sandbox-id";
const TARGET_PORT_HEADER: &str = "x-agentenv-target-port";

pub struct EnvdFilesClient {
    config: Configuration,
}

impl EnvdFilesClient {
    fn new(base_url: &str, api_key: &str, sandbox_id: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            API_KEY_HEADER,
            HeaderValue::from_str(api_key).context("invalid API key header value")?,
        );
        headers.insert(
            SANDBOX_ID_HEADER,
            HeaderValue::from_str(sandbox_id).context("invalid sandbox ID header value")?,
        );
        headers.insert(TARGET_PORT_HEADER, HeaderValue::from_static(ENVD_PORT_STR));

        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .default_headers(headers);
        if crate::grpc::bypass_proxy_for_base_url(base_url) {
            builder = builder.no_proxy();
        }
        let client = builder.build().context("building envd files client")?;

        Ok(Self {
            config: Configuration {
                base_path: base_url.trim_end_matches('/').to_string(),
                user_agent: Some(format!("aenv/{}", env!("CARGO_PKG_VERSION"))),
                client,
                basic_auth: None,
                oauth_access_token: None,
                bearer_access_token: None,
                api_key: None,
            },
        })
    }

    pub async fn upload(
        &self,
        local_path: &Path,
        remote_path: &str,
        username: Option<&str>,
    ) -> Result<()> {
        // envd 0.5.13 returns a JSON body with a text/plain content type for
        // successful multipart uploads. The generated files_post helper
        // rejects that response before parsing it, so issue only uploads
        // directly and keep using the generated helper for downloads.
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
        let part = Part::stream_with_length(file, file_size).file_name(file_name);
        let form = Form::new().part("file", part);

        let mut request = self
            .config
            .client
            .post(format!("{}/files", self.config.base_path))
            .query(&[("path", remote_path)])
            .multipart(form);
        if let Some(username) = username {
            request = request.query(&[("username", username)]);
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("uploading file to {remote_path}"))?;
        ensure_http_success(response).await?;
        Ok(())
    }

    pub async fn download(
        &self,
        remote_path: &str,
        username: Option<&str>,
    ) -> Result<reqwest::Response> {
        files_api::files_get(&self.config, Some(remote_path), username, None, None)
            .await
            .map_err(|error| {
                format_envd_api_error(error, &format!("downloading file from {remote_path}"))
            })
    }
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
    let content = response.text().await.unwrap_or_default();
    Err(format_envd_response_error(status, &content))
}

fn format_envd_api_error<T>(error: EnvdApiError<T>, operation: &str) -> anyhow::Error
where
    T: std::fmt::Debug,
{
    match error {
        EnvdApiError::ResponseError(response) => {
            format_envd_response_error(response.status, &response.content)
        }
        other => anyhow!(other.to_string()).context(operation.to_string()),
    }
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
        EnvdFilesClient::new(&self.base, &self.api_key, sandbox_id)
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
