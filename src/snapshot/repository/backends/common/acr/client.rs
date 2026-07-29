use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes as ReqwestBytes;
use reqwest::header::{
    HeaderValue, AUTHORIZATION, CONTENT_TYPE, LOCATION, RETRY_AFTER, WWW_AUTHENTICATE,
};
use reqwest::{Method, StatusCode};
use serde::Deserialize;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tokio::time::sleep;
use url::Url;

use crate::digest;
use crate::snapshot::repository::RepositoryError;

use super::manifest::OCI_IMAGE_MANIFEST_MEDIA_TYPE;

const DEFAULT_RETRY_COUNT: usize = 2;
const DEFAULT_UPLOAD_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(300);
const TOKEN_EXPIRY_SKEW: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub(crate) struct AcrClientOptions {
    pub(crate) timeout: Duration,
    pub(crate) retry_count: usize,
    pub(crate) upload_chunk_size: usize,
    pub(crate) retry_initial_backoff: Duration,
    #[cfg(test)]
    pub(crate) allow_insecure_http: bool,
}

impl Default for AcrClientOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            retry_count: DEFAULT_RETRY_COUNT,
            upload_chunk_size: DEFAULT_UPLOAD_CHUNK_SIZE,
            retry_initial_backoff: DEFAULT_RETRY_INITIAL_BACKOFF,
            #[cfg(test)]
            allow_insecure_http: false,
        }
    }
}

#[derive(Clone)]
struct RequestBody {
    bytes: ReqwestBytes,
    content_type: &'static str,
    content_range: Option<String>,
}

impl RequestBody {
    fn blob(bytes: impl Into<ReqwestBytes>) -> Self {
        Self {
            bytes: bytes.into(),
            content_type: "application/octet-stream",
            content_range: None,
        }
    }

    fn blob_range(bytes: impl Into<ReqwestBytes>, content_range: String) -> Self {
        Self {
            bytes: bytes.into(),
            content_type: "application/octet-stream",
            content_range: Some(content_range),
        }
    }

    fn manifest(bytes: impl Into<ReqwestBytes>) -> Self {
        Self {
            bytes: bytes.into(),
            content_type: OCI_IMAGE_MANIFEST_MEDIA_TYPE,
            content_range: None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AcrClient {
    http: reqwest::Client,
    credentials: Option<DockerRegistryCredentials>,
    options: AcrClientOptions,
    token_cache: Arc<Mutex<HashMap<String, CachedToken>>>,
    challenge_cache: Arc<Mutex<HashMap<String, String>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DockerRegistryCredentials {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct DockerCredentialHelperOutput {
    #[serde(rename = "Username", alias = "username")]
    username: String,
    #[serde(rename = "Secret", alias = "secret")]
    secret: String,
}

#[derive(Clone, Debug)]
struct CachedToken {
    token: String,
    expires_at: Instant,
}

#[derive(Debug, Error)]
pub(crate) enum AcrClientError {
    #[error("ACR credentials are missing for registry '{registry}'")]
    MissingCredentials { registry: String },

    #[error("ACR registry rejected push/pull credentials: {message}")]
    PermissionDenied { message: String },

    #[error("ACR manifest already exists for tag '{tag}'")]
    ManifestExists { tag: String },

    #[error("transient ACR registry error: {message}")]
    Transient { message: String },

    #[error("ACR registry error: {message}")]
    Registry { message: String },
}

impl AcrClient {
    pub(crate) fn from_docker_config(registry: &str) -> Result<Self, AcrClientError> {
        let credentials = load_docker_credentials(registry)?.ok_or_else(|| {
            AcrClientError::MissingCredentials {
                registry: registry.to_string(),
            }
        })?;
        Self::new(Some(credentials), AcrClientOptions::default())
    }

    fn new(
        credentials: Option<DockerRegistryCredentials>,
        options: AcrClientOptions,
    ) -> Result<Self, AcrClientError> {
        let http = reqwest::Client::builder()
            .timeout(options.timeout)
            .redirect(redirect_policy(&options))
            .build()
            .map_err(|e| AcrClientError::Registry {
                message: format!("build ACR HTTP client: {e}"),
            })?;
        Ok(Self {
            http,
            credentials,
            options,
            token_cache: Arc::new(Mutex::new(HashMap::new())),
            challenge_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub(crate) async fn ensure_manifest_absent(
        &self,
        manifest_url: &str,
        repository: &str,
        tag: &str,
    ) -> Result<(), AcrClientError> {
        let response = self
            .send_with_retries(Method::HEAD, manifest_url, repository, None)
            .await?;
        match response.status() {
            StatusCode::OK => Err(AcrClientError::ManifestExists {
                tag: tag.to_string(),
            }),
            StatusCode::NOT_FOUND => Ok(()),
            status if status.is_success() => Err(AcrClientError::ManifestExists {
                tag: tag.to_string(),
            }),
            status => Err(status_error(status, "check ACR manifest existence")),
        }
    }

    pub(crate) async fn blob_exists(
        &self,
        repo_blob_url: &str,
        repository: &str,
        digest: &str,
    ) -> Result<bool, AcrClientError> {
        let url = format!("{}/{}", repo_blob_url.trim_end_matches('/'), digest);
        let response = self
            .send_with_retries(Method::HEAD, &url, repository, None)
            .await?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status if status.is_success() => Ok(true),
            status => Err(status_error(status, "check ACR blob existence")),
        }
    }

    pub(crate) async fn upload_blob(
        &self,
        upload_url: &str,
        repository: &str,
        path: &Path,
        digest: &str,
    ) -> Result<(), AcrClientError> {
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|e| AcrClientError::Registry {
                message: format!("open ACR blob '{}': {e}", path.display()),
            })?;
        let size = file
            .metadata()
            .await
            .map_err(|e| AcrClientError::Registry {
                message: format!("stat ACR blob '{}': {e}", path.display()),
            })?
            .len();
        let mut session_url = self.start_blob_upload(upload_url, repository).await?;
        let chunk_size = self.options.upload_chunk_size.max(1);
        let mut offset = 0_u64;

        while offset < size {
            let remaining = (size - offset) as usize;
            let count = remaining.min(chunk_size);
            let mut buf = vec![0_u8; count];
            file.read_exact(&mut buf)
                .await
                .map_err(|e| AcrClientError::Registry {
                    message: format!("read ACR blob '{}': {e}", path.display()),
                })?;
            let end = offset + count as u64 - 1;
            // Registry upload PATCH retries reuse the same upload session and
            // byte range. OCI registries are expected to tolerate this
            // idempotent retry; if a registry does not, this path should be
            // upgraded to restart the whole upload session from offset 0.
            let response = self
                .send_with_retries(
                    Method::PATCH,
                    &session_url,
                    repository,
                    Some(RequestBody::blob_range(buf, format!("{offset}-{end}"))),
                )
                .await?;
            if response.status() != StatusCode::ACCEPTED && !response.status().is_success() {
                return Err(status_error(response.status(), "upload ACR blob chunk"));
            }
            if let Some(location) = response
                .headers()
                .get(LOCATION)
                .and_then(|v| v.to_str().ok())
            {
                session_url = absolutize_location(&session_url, location)?;
            }
            offset += count as u64;
        }

        self.complete_blob_upload(&session_url, repository, ReqwestBytes::new(), digest)
            .await
    }

    pub(crate) async fn upload_blob_with_descriptor(
        &self,
        upload_url: &str,
        repo_blob_url: &str,
        repository: &str,
        path: &Path,
        digest: &str,
        size: u64,
    ) -> Result<(String, u64), AcrClientError> {
        if digest.is_empty() {
            return Err(AcrClientError::Registry {
                message: format!("ACR blob '{}' is missing digest", path.display()),
            });
        }
        if size == 0 {
            return Err(AcrClientError::Registry {
                message: format!("ACR blob '{}' is empty", path.display()),
            });
        }
        let source_size = tokio::fs::metadata(path)
            .await
            .map_err(|e| AcrClientError::Registry {
                message: format!("stat ACR blob '{}': {e}", path.display()),
            })?
            .len();
        if source_size != size {
            return Err(AcrClientError::Registry {
                message: format!(
                    "ACR blob descriptor size mismatch for '{}': descriptor says {}, file has {}",
                    path.display(),
                    size,
                    source_size
                ),
            });
        }
        if self.blob_exists(repo_blob_url, repository, digest).await? {
            return Ok((digest.to_string(), size));
        }
        self.upload_blob(upload_url, repository, path, digest)
            .await?;
        Ok((digest.to_string(), size))
    }

    pub(crate) async fn upload_blob_bytes(
        &self,
        upload_url: &str,
        repository: &str,
        bytes: Vec<u8>,
        digest: &str,
    ) -> Result<(), AcrClientError> {
        let session_url = self.start_blob_upload(upload_url, repository).await?;
        self.complete_blob_upload(&session_url, repository, bytes.into(), digest)
            .await
    }

    async fn start_blob_upload(
        &self,
        upload_url: &str,
        repository: &str,
    ) -> Result<String, AcrClientError> {
        let init = self
            .send_with_retries(Method::POST, upload_url, repository, None)
            .await?;
        if init.status() != StatusCode::ACCEPTED && !init.status().is_success() {
            return Err(status_error(init.status(), "start ACR blob upload"));
        }
        let location = init
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| AcrClientError::Registry {
                message: "ACR blob upload response missing Location header".to_string(),
            })?;
        absolutize_location(upload_url, location)
    }

    async fn complete_blob_upload(
        &self,
        session_url: &str,
        repository: &str,
        bytes: ReqwestBytes,
        digest: &str,
    ) -> Result<(), AcrClientError> {
        let complete_url = append_digest_query(session_url, digest);
        let response = self
            .send_with_retries(
                Method::PUT,
                &complete_url,
                repository,
                Some(RequestBody::blob(bytes)),
            )
            .await?;
        if response.status() == StatusCode::CREATED || response.status().is_success() {
            Ok(())
        } else {
            Err(status_error(response.status(), "complete ACR blob upload"))
        }
    }

    pub(crate) async fn put_manifest(
        &self,
        manifest_url: &str,
        repository: &str,
        manifest_bytes: Vec<u8>,
    ) -> Result<String, AcrClientError> {
        let fallback_digest = digest::sha256_digest(&manifest_bytes);
        let response = self
            .send_with_retries(
                Method::PUT,
                manifest_url,
                repository,
                Some(RequestBody::manifest(manifest_bytes)),
            )
            .await?;
        if !(response.status() == StatusCode::CREATED || response.status().is_success()) {
            return Err(status_error(response.status(), "put ACR image manifest"));
        }
        Ok(response
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .unwrap_or(fallback_digest))
    }

    pub(crate) async fn delete_manifest_by_digest(
        &self,
        registry: &str,
        repository: &str,
        digest: &str,
    ) -> Result<(), AcrClientError> {
        if digest.is_empty() {
            return Err(AcrClientError::Registry {
                message: "cannot roll back ACR manifest because manifest digest is unknown"
                    .to_string(),
            });
        }
        let url = format!("https://{registry}/v2/{repository}/manifests/{digest}");
        self.delete_manifest_url(&url, repository).await
    }

    pub(crate) async fn delete_manifest_url(
        &self,
        manifest_digest_url: &str,
        repository: &str,
    ) -> Result<(), AcrClientError> {
        let response = self
            .send_with_retries(Method::DELETE, manifest_digest_url, repository, None)
            .await?;
        if response.status() == StatusCode::ACCEPTED
            || response.status() == StatusCode::NOT_FOUND
            || response.status().is_success()
        {
            Ok(())
        } else {
            Err(status_error(response.status(), "delete ACR image manifest"))
        }
    }

    async fn send_with_retries(
        &self,
        method: Method,
        url: &str,
        repository: &str,
        body: Option<RequestBody>,
    ) -> Result<reqwest::Response, AcrClientError> {
        let mut last_transient = None;
        let mut backoff = self.options.retry_initial_backoff;
        for attempt in 0..=self.options.retry_count {
            match self
                .send_once(method.clone(), url, repository, body.clone())
                .await
            {
                Ok(response) if is_retryable_status(response.status()) => {
                    let retry_after = retry_after_delay(response.headers().get(RETRY_AFTER));
                    last_transient = Some(AcrClientError::Transient {
                        message: format!("{} {} returned {}", method, url, response.status()),
                    });
                    if attempt < self.options.retry_count {
                        sleep(retry_after.unwrap_or(backoff)).await;
                        backoff = backoff.saturating_mul(2);
                    }
                    continue;
                }
                Ok(response) => return Ok(response),
                Err(error @ AcrClientError::Transient { .. }) => {
                    last_transient = Some(error);
                }
                Err(error) => return Err(error),
            }
            if attempt < self.options.retry_count {
                sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
        }
        Err(last_transient.unwrap_or_else(|| AcrClientError::Transient {
            message: format!("{} {} exhausted retry budget", method, url),
        }))
    }

    async fn send_once(
        &self,
        method: Method,
        url: &str,
        repository: &str,
        body: Option<RequestBody>,
    ) -> Result<reqwest::Response, AcrClientError> {
        let auth_header = self.cached_auth_header(url, repository).await?;
        let mut response = self
            .request(method.clone(), url, body.clone(), auth_header)
            .await?;
        if response.status() != StatusCode::UNAUTHORIZED {
            if response.status() == StatusCode::FORBIDDEN {
                return Err(AcrClientError::PermissionDenied {
                    message: format!("{} {} returned 403", method, url),
                });
            }
            return Ok(response);
        }

        let challenge = response
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| AcrClientError::PermissionDenied {
                message: "registry returned 401 without WWW-Authenticate".to_string(),
            })?
            .to_string();
        self.challenge_cache
            .lock()
            .await
            .insert(challenge_cache_key(url), challenge.clone());
        let auth_header = self.auth_header(url, repository, &challenge).await?;
        response = self
            .request(method.clone(), url, body, Some(auth_header))
            .await?;
        if response.status() == StatusCode::UNAUTHORIZED
            || response.status() == StatusCode::FORBIDDEN
        {
            return Err(AcrClientError::PermissionDenied {
                message: format!("{} {} returned {}", method, url, response.status()),
            });
        }
        Ok(response)
    }

    async fn request(
        &self,
        method: Method,
        url: &str,
        body: Option<RequestBody>,
        auth_header: Option<String>,
    ) -> Result<reqwest::Response, AcrClientError> {
        validate_https_url_str(url, "ACR registry request", &self.options)?;
        let mut request = self.http.request(method, url);
        if let Some(auth_header) = auth_header {
            request = request.header(AUTHORIZATION, auth_header);
        }
        if let Some(body) = body {
            request = request
                .header(CONTENT_TYPE, body.content_type)
                .header("Content-Length", body.bytes.len().to_string());
            if let Some(content_range) = body.content_range {
                request = request.header("Content-Range", content_range);
            }
            request = request.body(body.bytes);
        }
        request.send().await.map_err(|e| AcrClientError::Transient {
            message: format!("ACR HTTP request failed for {url}: {e}"),
        })
    }

    async fn auth_header(
        &self,
        registry_url: &str,
        repository: &str,
        challenge: &str,
    ) -> Result<String, AcrClientError> {
        let credentials =
            self.credentials
                .as_ref()
                .ok_or_else(|| AcrClientError::MissingCredentials {
                    registry: registry_host(registry_url)
                        .unwrap_or_else(|| registry_url.to_string()),
                })?;
        if starts_with_ignore_ascii_case(challenge, "basic ") {
            return Ok(build_basic_auth_header(
                &credentials.username,
                &credentials.password,
            ));
        }

        let bearer = parse_bearer_challenge(challenge)?;
        let scope = format!("repository:{repository}:push,pull");
        let cache_key = bearer.cache_key(&scope);
        if let Some(token) = self.cached_token(&cache_key).await {
            return Ok(format!("Bearer {token}"));
        }

        let mut url = Url::parse(&bearer.realm).map_err(|e| AcrClientError::Registry {
            message: format!("parse ACR token realm '{}': {e}", bearer.realm),
        })?;
        validate_https_url(&url, "ACR token realm", &self.options)?;
        url.query_pairs_mut()
            .append_pair("service", &bearer.service)
            .append_pair("scope", &scope);
        let response = self
            .http
            .get(url)
            .basic_auth(&credentials.username, Some(&credentials.password))
            .send()
            .await
            .map_err(|e| AcrClientError::Transient {
                message: format!("ACR token request failed: {e}"),
            })?;
        if response.status() == StatusCode::UNAUTHORIZED
            || response.status() == StatusCode::FORBIDDEN
        {
            return Err(AcrClientError::PermissionDenied {
                message: format!("ACR token service returned {}", response.status()),
            });
        }
        if response.status().is_server_error() {
            return Err(AcrClientError::Transient {
                message: format!("ACR token service returned {}", response.status()),
            });
        }
        if !response.status().is_success() {
            return Err(AcrClientError::Registry {
                message: format!("ACR token service returned {}", response.status()),
            });
        }
        let body: TokenResponse = response
            .json()
            .await
            .map_err(|e| AcrClientError::Registry {
                message: format!("parse ACR token response: {e}"),
            })?;
        let token = body
            .token
            .or(body.access_token)
            .ok_or_else(|| AcrClientError::Registry {
                message: "ACR token response did not contain token".to_string(),
            })?;
        let ttl = body
            .expires_in
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_TOKEN_TTL)
            .saturating_sub(TOKEN_EXPIRY_SKEW);
        self.token_cache.lock().await.insert(
            cache_key,
            CachedToken {
                token: token.clone(),
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(format!("Bearer {token}"))
    }

    async fn cached_auth_header(
        &self,
        registry_url: &str,
        repository: &str,
    ) -> Result<Option<String>, AcrClientError> {
        let challenge = self
            .challenge_cache
            .lock()
            .await
            .get(&challenge_cache_key(registry_url))
            .cloned();
        let Some(challenge) = challenge else {
            return Ok(None);
        };
        self.auth_header(registry_url, repository, &challenge)
            .await
            .map(Some)
    }

    async fn cached_token(&self, cache_key: &str) -> Option<String> {
        let mut cache = self.token_cache.lock().await;
        let cached = cache.get(cache_key)?;
        if cached.expires_at > Instant::now() {
            return Some(cached.token.clone());
        }
        cache.remove(cache_key);
        None
    }
}

impl From<AcrClientError> for RepositoryError {
    fn from(error: AcrClientError) -> Self {
        match error {
            AcrClientError::ManifestExists { tag } => RepositoryError::InvalidRequest {
                reason: format!("ACR snapshot tag '{tag}' already exists"),
            },
            other => RepositoryError::backend("ACR snapshot export", other),
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    token: Option<String>,
    access_token: Option<String>,
    expires_in: Option<u64>,
}

struct BearerChallenge {
    realm: String,
    service: String,
}

impl BearerChallenge {
    fn cache_key(&self, scope: &str) -> String {
        format!("{}|{}|{}", self.realm, self.service, scope)
    }
}

fn status_error(status: StatusCode, operation: &str) -> AcrClientError {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return AcrClientError::PermissionDenied {
            message: format!("{operation} returned {}", status.as_u16()),
        };
    }
    if status.is_server_error() {
        return AcrClientError::Transient {
            message: format!("{operation} returned {}", status.as_u16()),
        };
    }
    AcrClientError::Registry {
        message: format!("{operation} returned {}", status.as_u16()),
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

fn retry_after_delay(value: Option<&HeaderValue>) -> Option<Duration> {
    value?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

fn redirect_policy(options: &AcrClientOptions) -> reqwest::redirect::Policy {
    let allow_insecure_http = insecure_http_allowed(options);
    reqwest::redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() >= 10 {
            return attempt.error("too many ACR HTTP redirects");
        }
        let Some(origin) = attempt.previous().first() else {
            return attempt.error("ACR HTTP redirect is missing its origin URL");
        };
        if !same_origin(origin, attempt.url()) {
            return attempt.error("ACR HTTP redirect target must keep the original origin");
        }
        if attempt.url().scheme() != "https" && !allow_insecure_http {
            return attempt.error("ACR HTTP redirect target must use https");
        }
        attempt.follow()
    })
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn redirect_target_allowed(url: &Url, options: &AcrClientOptions) -> bool {
    url.scheme() == "https" || insecure_http_allowed(options)
}

fn insecure_http_allowed(options: &AcrClientOptions) -> bool {
    #[cfg(not(test))]
    {
        let _ = options;
        false
    }
    #[cfg(test)]
    {
        options.allow_insecure_http
    }
}

fn validate_https_url_str(
    url: &str,
    description: &str,
    options: &AcrClientOptions,
) -> Result<(), AcrClientError> {
    let parsed = Url::parse(url).map_err(|e| AcrClientError::Registry {
        message: format!("parse {description} URL '{url}': {e}"),
    })?;
    validate_https_url(&parsed, description, options)
}

fn validate_https_url(
    url: &Url,
    description: &str,
    options: &AcrClientOptions,
) -> Result<(), AcrClientError> {
    if redirect_target_allowed(url, options) {
        Ok(())
    } else {
        Err(AcrClientError::Registry {
            message: format!("{description} must use https: {url}"),
        })
    }
}

fn load_docker_credentials(
    registry: &str,
) -> Result<Option<DockerRegistryCredentials>, AcrClientError> {
    for path in docker_config_candidates() {
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).map_err(|e| AcrClientError::Registry {
            message: format!("read Docker config '{}': {e}", path.display()),
        })?;
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| AcrClientError::Registry {
                message: format!("parse Docker config '{}': {e}", path.display()),
            })?;
        if let Some(credentials) = parse_docker_auths(&value["auths"], registry) {
            return Ok(Some(credentials));
        }
        if let Some((helper, server_url)) = docker_credential_helper_for_registry(&value, registry)
        {
            if let Some(credentials) = load_docker_credential_helper(&helper, &server_url)? {
                return Ok(Some(credentials));
            }
        }
    }
    Ok(None)
}

fn docker_config_candidates() -> Vec<PathBuf> {
    [
        std::env::var_os("DOCKER_CONFIG")
            .map(PathBuf::from)
            .map(|path| path.join("config.json")),
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|path| path.join(".docker/config.json")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn parse_docker_auths(
    auths: &serde_json::Value,
    registry: &str,
) -> Option<DockerRegistryCredentials> {
    let obj = auths.as_object()?;
    for (address, value) in obj {
        if !docker_auth_address_matches(address, registry) {
            continue;
        }
        if let Some(auth) = value.get("auth").and_then(serde_json::Value::as_str) {
            if let Ok(decoded) = BASE64_STANDARD.decode(auth) {
                if let Ok(decoded) = String::from_utf8(decoded) {
                    if let Some((username, password)) = decoded.split_once(':') {
                        return Some(DockerRegistryCredentials {
                            username: username.to_string(),
                            password: password.to_string(),
                        });
                    }
                }
            }
        }
        if let (Some(username), Some(password)) = (
            value.get("username").and_then(serde_json::Value::as_str),
            value.get("password").and_then(serde_json::Value::as_str),
        ) {
            return Some(DockerRegistryCredentials {
                username: username.to_string(),
                password: password.to_string(),
            });
        }
    }
    None
}

fn docker_credential_helper_for_registry(
    docker_config: &serde_json::Value,
    registry: &str,
) -> Option<(String, String)> {
    if let Some(helpers) = docker_config
        .get("credHelpers")
        .and_then(serde_json::Value::as_object)
    {
        for (address, helper) in helpers {
            if docker_auth_address_matches(address, registry) {
                if let Some(helper) = helper.as_str().map(str::trim).filter(|h| !h.is_empty()) {
                    return Some((helper.to_string(), docker_helper_server_url(address)));
                }
            }
        }
    }

    let store = docker_config
        .get("credsStore")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|store| !store.is_empty())?;
    Some((
        store.to_string(),
        matching_docker_auth_address(&docker_config["auths"], registry)
            .map(|address| docker_helper_server_url(&address))
            .unwrap_or_else(|| registry.to_string()),
    ))
}

fn matching_docker_auth_address(auths: &serde_json::Value, registry: &str) -> Option<String> {
    auths.as_object()?.keys().find_map(|address| {
        docker_auth_address_matches(address, registry).then(|| address.to_string())
    })
}

fn docker_helper_server_url(address: &str) -> String {
    address.trim().trim_end_matches('/').to_string()
}

fn load_docker_credential_helper(
    helper: &str,
    server_url: &str,
) -> Result<Option<DockerRegistryCredentials>, AcrClientError> {
    let binary = format!("docker-credential-{helper}");
    load_docker_credential_helper_binary(&binary, server_url)
}

fn load_docker_credential_helper_binary(
    binary: &str,
    server_url: &str,
) -> Result<Option<DockerRegistryCredentials>, AcrClientError> {
    let mut child = Command::new(binary)
        .arg("get")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| AcrClientError::Registry {
            message: format!("run Docker credential helper '{binary} get': {e}"),
        })?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(server_url.as_bytes())
            .and_then(|_| stdin.write_all(b"\n"))
            .map_err(|e| AcrClientError::Registry {
                message: format!("write Docker credential helper request for '{server_url}': {e}"),
            })?;
    }

    let output = child
        .wait_with_output()
        .map_err(|e| AcrClientError::Registry {
            message: format!("wait for Docker credential helper '{binary} get': {e}"),
        })?;
    if !output.status.success() {
        return Ok(None);
    }
    parse_docker_credential_helper_output(&output.stdout)
}

fn parse_docker_credential_helper_output(
    output: &[u8],
) -> Result<Option<DockerRegistryCredentials>, AcrClientError> {
    let credentials: DockerCredentialHelperOutput =
        serde_json::from_slice(output).map_err(|e| AcrClientError::Registry {
            message: format!("parse Docker credential helper output: {e}"),
        })?;
    if credentials.username.is_empty() || credentials.secret.is_empty() {
        return Ok(None);
    }
    Ok(Some(DockerRegistryCredentials {
        username: credentials.username,
        password: credentials.secret,
    }))
}

fn docker_auth_address_matches(address: &str, registry: &str) -> bool {
    let trimmed = address
        .trim()
        .trim_end_matches('/')
        .trim_end_matches("/v1")
        .trim_end_matches("/v2");
    if trimmed == registry {
        return true;
    }
    Url::parse(trimmed)
        .ok()
        .and_then(|url| {
            let host = url.host_str()?;
            Some(match url.port() {
                Some(port) => format!("{host}:{port}"),
                None => host.to_string(),
            })
        })
        .as_deref()
        == Some(registry)
}

fn parse_bearer_challenge(challenge: &str) -> Result<BearerChallenge, AcrClientError> {
    if !starts_with_ignore_ascii_case(challenge, "bearer ") {
        return Err(AcrClientError::Registry {
            message: format!("unsupported registry auth challenge: {challenge}"),
        });
    }
    let fields = parse_auth_fields(&challenge[7..]);
    let realm = fields
        .iter()
        .find(|(key, _)| key == "realm")
        .map(|(_, value)| value.clone())
        .ok_or_else(|| AcrClientError::Registry {
            message: "bearer challenge missing realm".to_string(),
        })?;
    let service = fields
        .iter()
        .find(|(key, _)| key == "service")
        .map(|(_, value)| value.clone())
        .ok_or_else(|| AcrClientError::Registry {
            message: "bearer challenge missing service".to_string(),
        })?;
    Ok(BearerChallenge { realm, service })
}

fn parse_auth_fields(input: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_quotes = false;
    for ch in input.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                buf.push(ch);
            }
            ',' if !in_quotes => {
                push_auth_field(&mut out, &buf);
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }
    push_auth_field(&mut out, &buf);
    out
}

fn push_auth_field(out: &mut Vec<(String, String)>, raw: &str) {
    if let Some((key, value)) = raw.split_once('=') {
        out.push((
            key.trim().to_ascii_lowercase(),
            value.trim().trim_matches('"').to_string(),
        ));
    }
}

fn build_basic_auth_header(username: &str, password: &str) -> String {
    format!(
        "Basic {}",
        BASE64_STANDARD.encode(format!("{username}:{password}"))
    )
}

fn starts_with_ignore_ascii_case(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix)
}

fn append_digest_query(url: &str, digest: &str) -> String {
    let delimiter = if url.contains('?') { "&" } else { "?" };
    format!("{url}{delimiter}digest={digest}")
}

fn absolutize_location(base_url: &str, location: &str) -> Result<String, AcrClientError> {
    if Url::parse(location).is_ok() {
        return Ok(location.to_string());
    }
    Url::parse(base_url)
        .and_then(|base| base.join(location))
        .map(|url| url.to_string())
        .map_err(|e| AcrClientError::Registry {
            message: format!("resolve ACR upload Location '{location}': {e}"),
        })
}

fn registry_host(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    Some(match parsed.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

fn challenge_cache_key(url: &str) -> String {
    registry_host(url).unwrap_or_else(|| url.to_string())
}

#[cfg(test)]
pub(super) mod tests {
    use std::error::Error as _;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap as AxumHeaderMap, HeaderValue, StatusCode as AxumStatusCode};
    use axum::response::{IntoResponse, Redirect};
    use axum::routing::{delete, get, head, patch, post, put};
    use axum::Router;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Default)]
    pub(crate) struct FakeState {
        token_scopes: Vec<String>,
        uploads: Vec<Vec<u8>>,
        upload_completes: usize,
        pub(crate) manifest_puts: Vec<Vec<u8>>,
        deletes: Vec<String>,
        blob_exists: bool,
        blob_head_429s_remaining: usize,
        manifest_exists: bool,
        omit_manifest_digest: bool,
    }

    impl FakeState {
        pub(crate) fn with_existing_blobs() -> Self {
            Self {
                blob_exists: true,
                ..Default::default()
            }
        }
    }

    async fn serve(app: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    pub(crate) async fn fake_server(state: Arc<Mutex<FakeState>>) -> String {
        let app = Router::new()
            .route("/token", get(token))
            .route("/v2/ns/repo/blobs/{digest}", head(blob_head))
            .route("/v2/ns/repo/blobs/uploads/", post(upload_start))
            .route("/upload/session", patch(upload_chunk))
            .route("/upload/session", put(upload_complete))
            .route("/v2/ns/repo/manifests/{reference}", head(manifest_head))
            .route("/v2/ns/repo/manifests/{reference}", put(manifest_put))
            .route("/v2/ns/repo/manifests/{reference}", delete(manifest_delete))
            .with_state(state);
        serve(app).await
    }

    fn bearer_challenge(base: &str) -> String {
        format!(r#"Bearer realm="{base}/token",service="registry.test""#)
    }

    fn upload_url(base: &str) -> String {
        format!("{base}/v2/ns/repo/blobs/uploads/")
    }

    fn manifest_url(base: &str, tag: &str) -> String {
        format!("{base}/v2/ns/repo/manifests/{tag}")
    }

    fn repo_blob_url(base: &str) -> String {
        format!("{base}/v2/ns/repo/blobs")
    }

    pub(crate) fn client() -> AcrClient {
        client_with_retry_count(0)
    }

    fn client_with_retry_count(retry_count: usize) -> AcrClient {
        AcrClient::new(
            Some(DockerRegistryCredentials {
                username: "user".to_string(),
                password: "pass".to_string(),
            }),
            AcrClientOptions {
                timeout: Duration::from_secs(5),
                retry_count,
                upload_chunk_size: 4,
                retry_initial_backoff: Duration::from_millis(1),
                allow_insecure_http: true,
            },
        )
        .unwrap()
    }

    #[test]
    fn selects_docker_credential_helper_for_registry() {
        let config = serde_json::json!({
            "auths": {
                "https://registry.example/v2/": {}
            },
            "credsStore": "global",
            "credHelpers": {
                "registry.example": "specific"
            }
        });

        let (helper, server_url) =
            docker_credential_helper_for_registry(&config, "registry.example").unwrap();
        assert_eq!(helper, "specific");
        assert_eq!(server_url, "registry.example");
    }

    #[test]
    fn selects_global_docker_credential_store() {
        let config = serde_json::json!({
            "auths": {
                "https://registry.example/v2/": {}
            },
            "credsStore": "global"
        });

        let (helper, server_url) =
            docker_credential_helper_for_registry(&config, "registry.example").unwrap();
        assert_eq!(helper, "global");
        assert_eq!(server_url, "https://registry.example/v2");
    }

    #[test]
    #[cfg(unix)]
    fn loads_credentials_from_docker_credential_helper_binary() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let helper = temp.path().join("docker-credential-test");
        let staged_helper = temp.path().join("docker-credential-test.tmp");
        {
            let mut file = std::fs::File::create(&staged_helper).unwrap();
            file.write_all(
                br#"#!/bin/sh
read _server
printf '{"Username":"helper-user","Secret":"helper-secret"}'
"#,
            )
            .unwrap();
            file.sync_all().unwrap();
        }
        let mut permissions = std::fs::metadata(&staged_helper).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&staged_helper, permissions).unwrap();
        std::fs::rename(&staged_helper, &helper).unwrap();

        let credentials =
            load_docker_credential_helper_binary(helper.to_str().unwrap(), "registry.example")
                .unwrap()
                .unwrap();
        assert_eq!(
            credentials,
            DockerRegistryCredentials {
                username: "helper-user".to_string(),
                password: "helper-secret".to_string(),
            }
        );
    }

    async fn token(
        State(state): State<Arc<Mutex<FakeState>>>,
        axum::extract::Query(query): axum::extract::Query<
            std::collections::HashMap<String, String>,
        >,
        headers: AxumHeaderMap,
    ) -> impl IntoResponse {
        assert_eq!(headers.get("authorization").unwrap(), "Basic dXNlcjpwYXNz");
        state
            .lock()
            .unwrap()
            .token_scopes
            .push(query.get("scope").cloned().unwrap_or_default());
        (AxumStatusCode::OK, r#"{"token":"push-token"}"#)
    }

    async fn blob_head(
        State(state): State<Arc<Mutex<FakeState>>>,
        headers: AxumHeaderMap,
    ) -> impl IntoResponse {
        authorized_or_challenge(&headers, &state, |state| {
            if state.blob_head_429s_remaining > 0 {
                state.blob_head_429s_remaining -= 1;
                return (AxumStatusCode::TOO_MANY_REQUESTS, [("Retry-After", "0")]).into_response();
            }
            if state.blob_exists {
                AxumStatusCode::OK.into_response()
            } else {
                AxumStatusCode::NOT_FOUND.into_response()
            }
        })
    }

    async fn upload_start(
        State(state): State<Arc<Mutex<FakeState>>>,
        headers: AxumHeaderMap,
    ) -> impl IntoResponse {
        authorized_or_challenge(&headers, &state, |_state| {
            (AxumStatusCode::ACCEPTED, [("Location", "/upload/session")]).into_response()
        })
    }

    async fn upload_complete(
        State(state): State<Arc<Mutex<FakeState>>>,
        headers: AxumHeaderMap,
    ) -> impl IntoResponse {
        authorized_or_challenge(&headers, &state, |state| {
            state.upload_completes += 1;
            AxumStatusCode::CREATED.into_response()
        })
    }

    async fn upload_chunk(
        State(state): State<Arc<Mutex<FakeState>>>,
        headers: AxumHeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        authorized_or_challenge(&headers, &state, |state| {
            state.uploads.push(body.to_vec());
            (AxumStatusCode::ACCEPTED, [("Location", "/upload/session")]).into_response()
        })
    }

    async fn manifest_head(
        State(state): State<Arc<Mutex<FakeState>>>,
        headers: AxumHeaderMap,
    ) -> impl IntoResponse {
        authorized_or_challenge(&headers, &state, |state| {
            if state.manifest_exists {
                AxumStatusCode::OK.into_response()
            } else {
                AxumStatusCode::NOT_FOUND.into_response()
            }
        })
    }

    async fn manifest_put(
        State(state): State<Arc<Mutex<FakeState>>>,
        headers: AxumHeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        authorized_or_challenge(&headers, &state, |state| {
            state.manifest_puts.push(body.to_vec());
            let mut headers = AxumHeaderMap::new();
            if !state.omit_manifest_digest {
                headers.insert(
                    "Docker-Content-Digest",
                    HeaderValue::from_static("sha256:manifest"),
                );
            }
            (AxumStatusCode::CREATED, headers).into_response()
        })
    }

    async fn manifest_delete(
        State(state): State<Arc<Mutex<FakeState>>>,
        axum::extract::Path(reference): axum::extract::Path<String>,
        headers: AxumHeaderMap,
    ) -> impl IntoResponse {
        authorized_or_challenge(&headers, &state, |state| {
            state.deletes.push(reference);
            AxumStatusCode::ACCEPTED.into_response()
        })
    }

    fn authorized_or_challenge(
        headers: &AxumHeaderMap,
        state: &Arc<Mutex<FakeState>>,
        f: impl FnOnce(&mut FakeState) -> axum::response::Response,
    ) -> axum::response::Response {
        if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Bearer push-token") {
            let base = state_base_url(headers);
            let mut headers = AxumHeaderMap::new();
            headers.insert(
                "WWW-Authenticate",
                HeaderValue::from_str(&bearer_challenge(&base)).unwrap(),
            );
            return (AxumStatusCode::UNAUTHORIZED, headers).into_response();
        }
        f(&mut state.lock().unwrap())
    }

    fn state_base_url(headers: &AxumHeaderMap) -> String {
        let host = headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .unwrap();
        format!("http://{host}")
    }

    #[tokio::test]
    async fn blob_exists_skips_upload_decision() {
        let state = Arc::new(Mutex::new(FakeState {
            blob_exists: true,
            ..FakeState::default()
        }));
        let base = fake_server(Arc::clone(&state)).await;

        assert!(client()
            .blob_exists(&repo_blob_url(&base), "ns/repo", "sha256:delta")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn blob_exists_retries_too_many_requests() {
        let state = Arc::new(Mutex::new(FakeState {
            blob_exists: true,
            blob_head_429s_remaining: 1,
            ..FakeState::default()
        }));
        let base = fake_server(Arc::clone(&state)).await;

        assert!(client_with_retry_count(1)
            .blob_exists(&repo_blob_url(&base), "ns/repo", "sha256:delta")
            .await
            .unwrap());
        assert_eq!(state.lock().unwrap().blob_head_429s_remaining, 0);
    }

    #[tokio::test]
    async fn redirect_policy_rejects_cross_origin_body_replay() {
        async fn receive_body(
            State(reached): State<Arc<AtomicBool>>,
            _body: Bytes,
        ) -> AxumStatusCode {
            reached.store(true, Ordering::SeqCst);
            AxumStatusCode::OK
        }

        let reached = Arc::new(AtomicBool::new(false));
        let target = Router::new()
            .route("/upload", put(receive_body))
            .with_state(Arc::clone(&reached));
        let target_url = format!("{}/upload", serve(target).await);

        let redirect = Router::new().route(
            "/upload",
            put(move || {
                let target_url = target_url.clone();
                async move { Redirect::temporary(&target_url) }
            }),
        );
        let redirect_url = format!("{}/upload", serve(redirect).await);

        client()
            .http
            .put(redirect_url)
            .body("snapshot-data")
            .send()
            .await
            .expect_err("cross-origin redirect must be rejected");
        assert!(!reached.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn redirect_policy_limits_redirect_chain() {
        async fn redirect_to_self(headers: AxumHeaderMap) -> Redirect {
            let host = headers.get("host").unwrap().to_str().unwrap();
            Redirect::temporary(&format!("http://{host}/loop"))
        }

        let app = Router::new().route("/loop", get(redirect_to_self));
        let url = format!("{}/loop", serve(app).await);

        let error = client().http.get(url).send().await.unwrap_err();
        assert_eq!(
            error.source().map(ToString::to_string).as_deref(),
            Some("too many ACR HTTP redirects")
        );
    }

    #[tokio::test]
    async fn bearer_token_realm_must_use_https() {
        let client = AcrClient::new(
            Some(DockerRegistryCredentials {
                username: "user".to_string(),
                password: "pass".to_string(),
            }),
            AcrClientOptions {
                timeout: Duration::from_secs(5),
                retry_count: 0,
                upload_chunk_size: 4,
                retry_initial_backoff: Duration::from_millis(1),
                allow_insecure_http: false,
            },
        )
        .unwrap();

        let err = client
            .auth_header(
                "https://registry.test/v2/ns/repo/blobs/sha256:delta",
                "ns/repo",
                r#"Bearer realm="http://attacker.example.com/token",service="registry.test""#,
            )
            .await
            .expect_err("http token realms must be rejected before credentials are sent");

        assert!(err.to_string().contains("must use https"));
    }

    #[tokio::test]
    async fn registry_request_url_must_use_https() {
        let client = AcrClient::new(
            Some(DockerRegistryCredentials {
                username: "user".to_string(),
                password: "pass".to_string(),
            }),
            AcrClientOptions {
                timeout: Duration::from_secs(5),
                retry_count: 0,
                upload_chunk_size: 4,
                retry_initial_backoff: Duration::from_millis(1),
                allow_insecure_http: false,
            },
        )
        .unwrap();

        let err = client
            .blob_exists(
                "http://registry.test/v2/ns/repo/blobs",
                "ns/repo",
                "sha256:delta",
            )
            .await
            .expect_err("http registry URLs must be rejected before requests are sent");

        assert!(err.to_string().contains("must use https"));
    }

    #[tokio::test]
    async fn blob_upload_with_descriptor_streams_chunks_and_returns_declared_descriptor() {
        let state = Arc::new(Mutex::new(FakeState::default()));
        let base = fake_server(Arc::clone(&state)).await;
        let dir = TempDir::new().unwrap();
        let blob = dir.path().join("snapshot.commit");
        tokio::fs::write(&blob, b"0123456789").await.unwrap();
        let expected_digest = digest::sha256_digest(b"0123456789");

        let (digest, size) = client()
            .upload_blob_with_descriptor(
                &upload_url(&base),
                &repo_blob_url(&base),
                "ns/repo",
                &blob,
                &expected_digest,
                10,
            )
            .await
            .unwrap();

        assert_eq!(digest, expected_digest);
        assert_eq!(size, 10);
        let state = state.lock().unwrap();
        assert_eq!(
            state.uploads,
            vec![b"0123".to_vec(), b"4567".to_vec(), b"89".to_vec()]
        );
        assert_eq!(state.upload_completes, 1);
    }

    #[tokio::test]
    async fn blob_upload_with_descriptor_skips_existing_blob() {
        let state = Arc::new(Mutex::new(FakeState {
            blob_exists: true,
            ..FakeState::default()
        }));
        let base = fake_server(Arc::clone(&state)).await;
        let dir = TempDir::new().unwrap();
        let blob = dir.path().join("snapshot.commit");
        tokio::fs::write(&blob, b"0123456789").await.unwrap();
        let expected_digest = digest::sha256_digest(b"0123456789");

        let (digest, size) = client()
            .upload_blob_with_descriptor(
                &upload_url(&base),
                &repo_blob_url(&base),
                "ns/repo",
                &blob,
                &expected_digest,
                10,
            )
            .await
            .unwrap();

        assert_eq!(digest, expected_digest);
        assert_eq!(size, 10);
        let state = state.lock().unwrap();
        assert!(state.uploads.is_empty());
        assert_eq!(state.upload_completes, 0);
    }

    #[tokio::test]
    async fn blob_upload_with_descriptor_rejects_empty_blob() {
        let state = Arc::new(Mutex::new(FakeState::default()));
        let base = fake_server(Arc::clone(&state)).await;
        let dir = TempDir::new().unwrap();
        let blob = dir.path().join("snapshot.commit");
        tokio::fs::write(&blob, b"").await.unwrap();

        let err = client()
            .upload_blob_with_descriptor(
                &upload_url(&base),
                &repo_blob_url(&base),
                "ns/repo",
                &blob,
                "sha256:empty",
                0,
            )
            .await
            .expect_err("empty ACR blobs should be rejected before upload");

        assert!(err.to_string().contains("is empty"));
        let state = state.lock().unwrap();
        assert!(state.uploads.is_empty());
        assert_eq!(state.upload_completes, 0);
    }

    #[tokio::test]
    async fn ensure_manifest_absent_rejects_overwrite() {
        let state = Arc::new(Mutex::new(FakeState {
            manifest_exists: true,
            ..FakeState::default()
        }));
        let base = fake_server(state).await;

        assert!(matches!(
            client()
                .ensure_manifest_absent(&manifest_url(&base, "tag"), "ns/repo", "tag")
                .await,
            Err(AcrClientError::ManifestExists { .. })
        ));
    }

    #[tokio::test]
    async fn bearer_token_is_cached_for_same_scope() {
        let state = Arc::new(Mutex::new(FakeState::default()));
        let base = fake_server(Arc::clone(&state)).await;
        let client = client();

        for _ in 0..2 {
            let exists = client
                .blob_exists(&repo_blob_url(&base), "ns/repo", "sha256:delta")
                .await
                .unwrap();
            assert!(!exists);
        }

        assert_eq!(
            state.lock().unwrap().token_scopes,
            vec!["repository:ns/repo:push,pull"]
        );
    }

    #[tokio::test]
    async fn manifest_put_stores_payload_and_returns_digest() {
        let state = Arc::new(Mutex::new(FakeState::default()));
        let base = fake_server(Arc::clone(&state)).await;

        let digest = client()
            .put_manifest(&manifest_url(&base, "tag"), "ns/repo", b"manifest".to_vec())
            .await
            .unwrap();
        assert_eq!(digest, "sha256:manifest");
        assert_eq!(
            state.lock().unwrap().manifest_puts,
            vec![b"manifest".to_vec()]
        );
    }

    #[tokio::test]
    async fn rollback_deletes_known_digest() {
        let state = Arc::new(Mutex::new(FakeState::default()));
        let base = fake_server(Arc::clone(&state)).await;
        client()
            .delete_manifest_url(
                &format!("{base}/v2/ns/repo/manifests/sha256:manifest"),
                "ns/repo",
            )
            .await
            .unwrap();
        assert_eq!(state.lock().unwrap().deletes, vec!["sha256:manifest"]);
    }

    #[tokio::test]
    async fn rollback_impossible_when_digest_unknown() {
        assert!(matches!(
            client()
                .delete_manifest_by_digest("registry.example", "ns/repo", "")
                .await,
            Err(AcrClientError::Registry { message }) if message.contains("unknown")
        ));
    }
}
