use super::local::LocalFile;
use crate::config::{AuthConfig, CredentialConfig, GlobalConfig, ImageAuthResponse};
use crate::io::virtual_file::VirtualFile;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use dashmap::DashMap;
use reqwest::header::{
    AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, LOCATION, RANGE, WWW_AUTHENTICATE,
};
use reqwest::{Client, Identity, StatusCode, Url};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::cmp::min;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::{Mutex, Notify};

const MIN_TOKEN_LIFE: Duration = Duration::from_secs(30);
const MIN_URL_INFO_LIFE: Duration = Duration::from_secs(300);
const MIN_META_LIFE: Duration = Duration::from_secs(300);
const REGISTRY_AUTH_CHALLENGE_LIFE: Duration = Duration::from_secs(300);
const PROBE_PER_URL_LIFE: Duration = Duration::from_secs(30);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RETRY_COUNT: u32 = 3;
const DEFAULT_UPLOAD_CHUNK_SIZE: usize = 128 * 1024 * 1024;
const RETRY_BACKOFF: Duration = Duration::from_millis(1);
const HTTP_CLIENT_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone)]
pub struct RegistryFsV2Options {
    pub timeout: Duration,
    pub retry_count: u32,
    pub user_agent: String,
    pub credential: CredentialConfig,
    pub accelerate_address: String,
    pub cert_file: String,
    pub key_file: String,
}

impl Default for RegistryFsV2Options {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            retry_count: DEFAULT_RETRY_COUNT,
            user_agent: env!("CARGO_PKG_VERSION").to_string(),
            credential: CredentialConfig::default(),
            accelerate_address: String::new(),
            cert_file: String::new(),
            key_file: String::new(),
        }
    }
}

impl RegistryFsV2Options {
    pub fn from_global_config(cfg: &GlobalConfig) -> Self {
        let retry_count = if cfg.download.try_cnt <= 0 {
            DEFAULT_RETRY_COUNT
        } else {
            cfg.download.try_cnt as u32
        };
        let accelerate_address = if cfg.p2p_config.enable {
            cfg.p2p_config.address.clone()
        } else {
            String::new()
        };
        Self {
            timeout: DEFAULT_TIMEOUT,
            retry_count,
            user_agent: cfg.user_agent.clone(),
            credential: cfg.credential_config.clone(),
            accelerate_address,
            cert_file: cfg.cert_config.cert_file.clone(),
            key_file: cfg.cert_config.key_file.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum UrlMode {
    Redirect,
    SelfAuth,
}

#[derive(Debug, Clone)]
struct UrlInfo {
    mode: UrlMode,
    info: String,
    expire_at: Instant,
}

#[derive(Debug, Clone)]
struct CachedToken {
    token: String,
    expire_at: Instant,
}

#[derive(Debug, Clone)]
struct CachedMetaSize {
    size: u64,
    expire_at: Instant,
}

#[derive(Debug, Clone)]
enum CredentialMode {
    None,
    File(PathBuf),
    Http { endpoint: String, timeout: Duration },
    Static { username: String, password: String },
}

impl CredentialMode {
    fn from_config(cfg: &CredentialConfig) -> Result<Self> {
        match cfg.mode.as_str() {
            "" => Ok(Self::None),
            "file" => Ok(Self::File(PathBuf::from(&cfg.path))),
            "http" => {
                if cfg.path.is_empty() {
                    bail!("credentialConfig.path cannot be empty when mode=http");
                }
                let timeout = if cfg.timeout <= 0 {
                    Duration::from_secs(1)
                } else {
                    Duration::from_secs(cfg.timeout as u64)
                };
                Ok(Self::Http {
                    endpoint: cfg.path.clone(),
                    timeout,
                })
            }
            other => bail!("invalid credential mode {other}"),
        }
    }
}

#[derive(Debug, Clone)]
enum AuthChallenge {
    None,
    Basic,
    Bearer { auth_url: Url },
}

#[derive(Debug, Clone)]
enum RegistryAuthChallenge {
    None,
    Basic,
    Bearer { realm: String, service: String },
    ProbePerUrl,
}

#[derive(Debug, Clone)]
struct CachedRegistryAuthChallenge {
    challenge: RegistryAuthChallenge,
    expire_at: Instant,
}

#[derive(Debug, Clone)]
enum PreparedAuth {
    None,
    Basic { header: String },
    Bearer { header: String, cache_key: String },
    ProbePerUrl,
}

#[derive(Debug, Clone)]
struct BearerChallengeParts {
    realm: String,
    service: String,
    scope: Option<String>,
}

#[derive(Debug, Clone)]
struct BlobAuthContext {
    origin: String,
    ping_url: Url,
    scope: String,
}

#[derive(Debug)]
enum ActualUrlResponse {
    Success(UrlInfo),
    Unauthorized(StatusCode),
    Other(StatusCode),
}

impl PreparedAuth {
    fn header(&self) -> &str {
        match self {
            PreparedAuth::Basic { header } | PreparedAuth::Bearer { header, .. } => header,
            PreparedAuth::None | PreparedAuth::ProbePerUrl => "",
        }
    }

    fn bearer_cache_key(&self) -> Option<&str> {
        match self {
            PreparedAuth::Bearer { cache_key, .. } => Some(cache_key.as_str()),
            PreparedAuth::None | PreparedAuth::Basic { .. } | PreparedAuth::ProbePerUrl => None,
        }
    }
}

#[derive(Debug)]
struct RegistryFSImplV2 {
    client: Client,
    credential_mode: CredentialMode,
    timeout: Duration,
    retry_count: u32,
    accelerate_address: RwLock<String>,
    /// Cached bearer tokens keyed by the full token-endpoint URL
    /// (realm + service + scope), so that tokens for different registries or
    /// scopes never collide — even when the challenge omits `scope`.
    bearer_tokens: DashMap<String, CachedToken>,
    bearer_token_locks: DashMap<String, Arc<Mutex<()>>>,
    registry_auth_challenges: DashMap<String, CachedRegistryAuthChallenge>,
    registry_auth_locks: DashMap<String, Arc<Mutex<()>>>,
    url_infos: DashMap<String, UrlInfo>,
    meta_sizes: DashMap<String, CachedMetaSize>,
}

impl RegistryFSImplV2 {
    fn token_ttl() -> Duration {
        MIN_TOKEN_LIFE
    }

    fn meta_ttl() -> Duration {
        MIN_META_LIFE
    }

    fn registry_auth_challenge_ttl(challenge: &RegistryAuthChallenge) -> Duration {
        match challenge {
            RegistryAuthChallenge::ProbePerUrl => PROBE_PER_URL_LIFE,
            RegistryAuthChallenge::None
            | RegistryAuthChallenge::Basic
            | RegistryAuthChallenge::Bearer { .. } => REGISTRY_AUTH_CHALLENGE_LIFE,
        }
    }

    fn accelerate_address(&self) -> String {
        self.accelerate_address
            .read()
            .map(|x| x.clone())
            .unwrap_or_default()
    }

    fn invalidate_url_info(&self, url: &str) {
        self.url_infos.remove(url);
    }

    fn invalidate_scope_token(&self, cache_key: &str) {
        self.bearer_tokens.remove(cache_key);
    }

    fn mark_probe_per_url(&self, origin: &str) {
        let challenge = RegistryAuthChallenge::ProbePerUrl;
        self.registry_auth_challenges.insert(
            origin.to_string(),
            CachedRegistryAuthChallenge {
                expire_at: Instant::now() + Self::registry_auth_challenge_ttl(&challenge),
                challenge,
            },
        );
    }

    fn invalidate_meta_size(&self, url: &str) {
        self.meta_sizes.remove(url);
    }

    async fn resolve_credentials(&self, remote_url: &str) -> Result<(String, String)> {
        match &self.credential_mode {
            CredentialMode::None => Ok((String::new(), String::new())),
            CredentialMode::File(path) => {
                let raw = fs::read_to_string(path).context("read credential file")?;
                let cfg: AuthConfig =
                    serde_json::from_str(&raw).context("parse credential file json failed")?;
                resolve_docker_config_credentials(&cfg, remote_url, self.timeout).await
            }
            CredentialMode::Http { endpoint, timeout } => {
                let mut url = Url::parse(endpoint)
                    .context(format!("invalid credential http endpoint {endpoint}"))?;
                url.query_pairs_mut().append_pair("remote_url", remote_url);

                let resp = self
                    .client
                    .get(url)
                    .timeout(*timeout)
                    .send()
                    .await
                    .context("request credential http failed")?;
                if resp.status() != StatusCode::OK {
                    return Err(http_status_error(resp, "credential http request failed"));
                }
                let body = resp
                    .text()
                    .await
                    .context("read credential http response body failed")?;
                let auth_resp: ImageAuthResponse = serde_json::from_str(&body)
                    .context("parse credential http response json failed")?;
                if !auth_resp.success {
                    bail!(
                        "credential http response failed, traceId={}",
                        auth_resp.trace_id
                    );
                }
                Ok(parse_auths(&auth_resp.data.auths, remote_url))
            }
            CredentialMode::Static { username, password } => {
                Ok((username.clone(), password.clone()))
            }
        }
    }

    async fn get_scope_auth(&self, url: &str) -> Result<AuthChallenge> {
        self.get_scope_auth_for(url, false).await
    }

    async fn get_scope_auth_for(&self, url: &str, push: bool) -> Result<AuthChallenge> {
        let mut req = if push {
            self.client
                .post(url)
                .header("Content-Type", "application/octet-stream")
        } else {
            self.client.get(url).header(RANGE, "bytes=0-0")
        };
        req = req.timeout(self.timeout);
        let resp = req
            .send()
            .await
            .context(format!("challenge request failed for {url}"))?;
        let status = resp.status();
        if status != StatusCode::UNAUTHORIZED && status != StatusCode::FORBIDDEN {
            return Ok(AuthChallenge::None);
        }
        let challenge = resp
            .headers()
            .get(WWW_AUTHENTICATE)
            .ok_or_else(|| anyhow::anyhow!("no WWW-Authenticate in response"))?
            .to_str()
            .context("invalid WWW-Authenticate header")?;
        let parsed = parse_auth_challenge(challenge)?;
        Ok(parsed)
    }

    async fn get_token(&self, remote_url: &str, auth_url: &Url) -> Result<String> {
        let (username, password) = self.resolve_credentials(remote_url).await?;
        let has_credentials = !username.is_empty();
        // Per the Docker Distribution token auth spec, the `account` query
        // parameter identifies the user on whose behalf the token is requested.
        // Registries like Harbor require it in addition to the Basic-auth header.
        let mut token_url = auth_url.clone();
        if has_credentials {
            token_url
                .query_pairs_mut()
                .append_pair("account", &username);
        }
        let mut req = self.client.get(token_url).timeout(self.timeout);
        if has_credentials {
            req = req.basic_auth(&username, Some(password));
        }
        let resp = req
            .send()
            .await
            .context(format!("token request failed for {auth_url}"))?;
        if resp.status() != StatusCode::OK {
            return Err(http_status_error(
                resp,
                format!("token request returned non-200 for {auth_url}"),
            ));
        }
        let v: Value = resp
            .json()
            .await
            .context("parse token response json failed")?;
        let token = v
            .get("token")
            .and_then(Value::as_str)
            .or_else(|| v.get("access_token").and_then(Value::as_str))
            .ok_or_else(|| anyhow::anyhow!("token response missing token/access_token"))?;
        let token = token.to_string();
        Ok(token)
    }

    async fn get_or_create_token(&self, remote_url: &str, auth_url: &Url) -> Result<String> {
        // Use the full token-endpoint URL as cache key so that different
        // registries / scopes never share a cached token — even when the
        // WWW-Authenticate challenge omits the `scope` parameter.
        let cache_key = auth_url.as_str();

        if let Some(entry) = self.bearer_tokens.get(cache_key) {
            if entry.expire_at > Instant::now() {
                return Ok(entry.token.clone());
            }
        }

        let wait_lock = self
            .bearer_token_locks
            .entry(cache_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = wait_lock.lock().await;

        if let Some(entry) = self.bearer_tokens.get(cache_key) {
            if entry.expire_at > Instant::now() {
                return Ok(entry.token.clone());
            }
        }

        let token = self.get_token(remote_url, auth_url).await?;
        self.bearer_tokens.insert(
            cache_key.to_string(),
            CachedToken {
                token: token.clone(),
                expire_at: Instant::now() + Self::token_ttl(),
            },
        );
        Ok(token)
    }

    async fn get_or_create_registry_auth_challenge(
        &self,
        ctx: &BlobAuthContext,
    ) -> Result<RegistryAuthChallenge> {
        if let Some(entry) = self.registry_auth_challenges.get(&ctx.origin) {
            if entry.expire_at > Instant::now() {
                return Ok(entry.challenge.clone());
            }
        }

        let wait_lock = self
            .registry_auth_locks
            .entry(ctx.origin.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = wait_lock.lock().await;

        if let Some(entry) = self.registry_auth_challenges.get(&ctx.origin) {
            if entry.expire_at > Instant::now() {
                return Ok(entry.challenge.clone());
            }
        }

        let challenge = match self.ping_registry_auth(ctx).await {
            Ok(challenge) => challenge,
            Err(_) => RegistryAuthChallenge::ProbePerUrl,
        };
        self.registry_auth_challenges.insert(
            ctx.origin.clone(),
            CachedRegistryAuthChallenge {
                expire_at: Instant::now() + Self::registry_auth_challenge_ttl(&challenge),
                challenge: challenge.clone(),
            },
        );
        Ok(challenge)
    }

    async fn ping_registry_auth(&self, ctx: &BlobAuthContext) -> Result<RegistryAuthChallenge> {
        let resp = self
            .client
            .get(ctx.ping_url.clone())
            .timeout(self.timeout)
            .send()
            .await
            .context(format!("registry ping failed for {}", ctx.ping_url))?;
        let status = resp.status();
        if status != StatusCode::UNAUTHORIZED && status != StatusCode::FORBIDDEN {
            return Ok(RegistryAuthChallenge::None);
        }
        let challenge = resp
            .headers()
            .get(WWW_AUTHENTICATE)
            .ok_or_else(|| anyhow::anyhow!("no WWW-Authenticate in registry ping response"))?
            .to_str()
            .context("invalid WWW-Authenticate header in registry ping response")?;
        parse_registry_auth_challenge(challenge)
    }

    async fn prepare_cached_auth(&self, url: &str, ctx: &BlobAuthContext) -> Result<PreparedAuth> {
        match self.get_or_create_registry_auth_challenge(ctx).await? {
            RegistryAuthChallenge::None => Ok(PreparedAuth::None),
            RegistryAuthChallenge::Basic => {
                let (username, password) = self.resolve_credentials(url).await?;
                if username.is_empty() {
                    Ok(PreparedAuth::None)
                } else {
                    Ok(PreparedAuth::Basic {
                        header: build_basic_auth_header(&username, &password),
                    })
                }
            }
            RegistryAuthChallenge::Bearer { realm, service } => {
                let auth_url = build_bearer_auth_url(&realm, &service, Some(&ctx.scope))?;
                let cache_key = auth_url.to_string();
                let token = self.get_or_create_token(url, &auth_url).await?;
                Ok(PreparedAuth::Bearer {
                    header: format!("Bearer {token}"),
                    cache_key,
                })
            }
            RegistryAuthChallenge::ProbePerUrl => Ok(PreparedAuth::ProbePerUrl),
        }
    }

    async fn prepare_legacy_auth(&self, url: &str) -> Result<PreparedAuth> {
        match self.get_scope_auth(url).await? {
            AuthChallenge::None => Ok(PreparedAuth::None),
            AuthChallenge::Basic => {
                let (username, password) = self.resolve_credentials(url).await?;
                if username.is_empty() {
                    Ok(PreparedAuth::None)
                } else {
                    Ok(PreparedAuth::Basic {
                        header: build_basic_auth_header(&username, &password),
                    })
                }
            }
            AuthChallenge::Bearer { ref auth_url } => {
                let cache_key = auth_url.to_string();
                let token = self.get_or_create_token(url, auth_url).await?;
                Ok(PreparedAuth::Bearer {
                    header: format!("Bearer {token}"),
                    cache_key,
                })
            }
        }
    }

    async fn resolve_actual_url_once(
        &self,
        url: &str,
        auth_header: &str,
    ) -> Result<ActualUrlResponse> {
        let mut req = self.client.get(url).timeout(self.timeout);
        if !auth_header.is_empty() {
            req = req.header(AUTHORIZATION, auth_header);
        }
        let resp = req
            .send()
            .await
            .context(format!("resolve actual URL request failed for {url}"))?;
        let status = resp.status();
        if status.is_redirection() {
            let location = resp
                .headers()
                .get(LOCATION)
                .ok_or_else(|| anyhow::anyhow!("redirect without Location"))?
                .to_str()
                .context("invalid Location header")?;
            return Ok(ActualUrlResponse::Success(UrlInfo {
                mode: UrlMode::Redirect,
                info: location.to_string(),
                expire_at: Instant::now() + MIN_URL_INFO_LIFE,
            }));
        }
        if status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT {
            return Ok(ActualUrlResponse::Success(UrlInfo {
                mode: UrlMode::SelfAuth,
                info: auth_header.to_string(),
                expire_at: Instant::now() + MIN_URL_INFO_LIFE,
            }));
        }
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Ok(ActualUrlResponse::Unauthorized(status));
        }
        Ok(ActualUrlResponse::Other(status))
    }

    async fn get_actual_url_with_legacy_probe(
        &self,
        url: &str,
        ctx: Option<&BlobAuthContext>,
    ) -> Result<UrlInfo> {
        let auth = self.prepare_legacy_auth(url).await?;
        if matches!(auth, PreparedAuth::ProbePerUrl) {
            bail!("unexpected per-url auth sentinel for {url}");
        }
        match self.resolve_actual_url_once(url, auth.header()).await? {
            ActualUrlResponse::Success(info) => {
                if let Some(ctx) = ctx {
                    self.mark_probe_per_url(&ctx.origin);
                }
                Ok(info)
            }
            ActualUrlResponse::Unauthorized(status) => {
                if let Some(key) = auth.bearer_cache_key() {
                    self.invalidate_scope_token(key);
                }
                bail!(
                    "failed to resolve actual url for {url}: http status {}",
                    status.as_u16()
                )
            }
            ActualUrlResponse::Other(status) => {
                if let Some(key) = auth.bearer_cache_key() {
                    self.invalidate_scope_token(key);
                }
                bail!(
                    "failed to resolve actual url for {url}: http status {}",
                    status.as_u16()
                )
            }
        }
    }

    async fn refresh_token(&self, url: &str) -> Result<Option<String>> {
        match self.get_scope_auth_for(url, true).await? {
            AuthChallenge::None | AuthChallenge::Basic => Ok(None),
            AuthChallenge::Bearer { auth_url } => {
                let token = self.get_token(url, &auth_url).await?;
                if token.is_empty() {
                    bail!("failed to get token");
                }
                self.bearer_tokens.insert(
                    auth_url.to_string(),
                    CachedToken {
                        token: token.clone(),
                        expire_at: Instant::now() + Self::token_ttl(),
                    },
                );
                Ok(Some(token))
            }
        }
    }

    async fn get_actual_url(&self, url: &str) -> Result<UrlInfo> {
        let Some(ctx) = blob_auth_context(url) else {
            return self.get_actual_url_with_legacy_probe(url, None).await;
        };

        let auth = self.prepare_cached_auth(url, &ctx).await?;
        if matches!(auth, PreparedAuth::ProbePerUrl) {
            return self.get_actual_url_with_legacy_probe(url, Some(&ctx)).await;
        }

        match self.resolve_actual_url_once(url, auth.header()).await? {
            ActualUrlResponse::Success(info) => Ok(info),
            ActualUrlResponse::Other(status) => {
                if let Some(key) = auth.bearer_cache_key() {
                    self.invalidate_scope_token(key);
                }
                bail!(
                    "failed to resolve actual url for {url}: http status {}",
                    status.as_u16()
                );
            }
            ActualUrlResponse::Unauthorized(_status) => {
                if let Some(key) = auth.bearer_cache_key() {
                    self.invalidate_scope_token(key);
                }

                if matches!(auth, PreparedAuth::Bearer { .. }) {
                    let refreshed = self.prepare_cached_auth(url, &ctx).await?;
                    if !matches!(refreshed, PreparedAuth::ProbePerUrl) {
                        match self
                            .resolve_actual_url_once(url, refreshed.header())
                            .await?
                        {
                            ActualUrlResponse::Success(info) => return Ok(info),
                            ActualUrlResponse::Unauthorized(_status) => {
                                if let Some(key) = refreshed.bearer_cache_key() {
                                    self.invalidate_scope_token(key);
                                }
                            }
                            ActualUrlResponse::Other(status) => {
                                if let Some(key) = refreshed.bearer_cache_key() {
                                    self.invalidate_scope_token(key);
                                }
                                bail!(
                                    "failed to resolve actual url for {url}: http status {}",
                                    status.as_u16()
                                );
                            }
                        }
                    }
                }

                self.get_actual_url_with_legacy_probe(url, Some(&ctx)).await
            }
        }
    }

    async fn get_or_create_url_info(&self, url: &str) -> Result<UrlInfo> {
        if let Some(entry) = self.url_infos.get(url) {
            if entry.expire_at > Instant::now() {
                return Ok(entry.clone());
            }
        }
        let info = self.get_actual_url(url).await?;
        self.url_infos.insert(url.to_string(), info.clone());
        Ok(info)
    }

    fn with_accelerate_url(&self, actual_url: &str) -> Option<String> {
        let addr = self.accelerate_address();
        if addr.is_empty() {
            return None;
        }
        Some(format!("{}/{}", addr.trim_end_matches('/'), actual_url))
    }

    fn build_range_header(offset: u64, count: usize) -> Result<String> {
        let range_end = offset
            .checked_add(count as u64)
            .and_then(|x| x.checked_sub(1))
            .ok_or_else(|| anyhow::anyhow!("range overflow"))?;
        Ok(format!("bytes={offset}-{range_end}"))
    }

    async fn send_range_request(
        &self,
        request_url: &str,
        auth_header: Option<&str>,
        offset: u64,
        count: usize,
    ) -> Result<reqwest::Response> {
        let range_header = Self::build_range_header(offset, count)?;
        let mut req = self
            .client
            .get(request_url)
            .header(RANGE, range_header)
            .timeout(self.timeout);
        if let Some(auth_header) = auth_header {
            if !auth_header.is_empty() {
                req = req.header(AUTHORIZATION, auth_header);
            }
        }
        let resp = req
            .send()
            .await
            .context(format!("range request failed for {request_url}"))?;
        if resp.status() == StatusCode::OK || resp.status() == StatusCode::PARTIAL_CONTENT {
            return Ok(resp);
        }
        Err(http_status_error(resp, "range request returned non-2xx"))
    }

    async fn fetch_range_response(
        &self,
        url: &str,
        offset: u64,
        count: usize,
    ) -> Result<reqwest::Response> {
        let info = self.get_or_create_url_info(url).await?;
        let raw_actual_url = match info.mode {
            UrlMode::Redirect => info.info.clone(),
            UrlMode::SelfAuth => url.to_string(),
        };
        let auth_header = if matches!(info.mode, UrlMode::SelfAuth) {
            Some(info.info.as_str())
        } else {
            None
        };

        if let Some(accelerated) = self.with_accelerate_url(&raw_actual_url) {
            return match self
                .send_range_request(&accelerated, auth_header, offset, count)
                .await
            {
                Ok(resp) => Ok(resp),
                Err(err) => {
                    self.invalidate_url_info(url);
                    Err(err)
                }
            };
        }

        match self
            .send_range_request(&raw_actual_url, auth_header, offset, count)
            .await
        {
            Ok(resp) => Ok(resp),
            Err(err) => {
                self.invalidate_url_info(url);
                Err(err)
            }
        }
    }

    async fn read_range(&self, url: &str, offset: u64, count: usize) -> Result<Bytes> {
        let source = crate::metrics::registry_source(&self.accelerate_address(), url);
        let start = Instant::now();
        let result = self.read_range_inner(url, offset, count).await;
        crate::metrics::record_remote_read(
            &source,
            crate::metrics::RemoteReadOperation::ReadRange,
            &result,
            |body| body.len() as u64,
            start.elapsed(),
        );
        result
    }

    async fn read_range_inner(&self, url: &str, offset: u64, count: usize) -> Result<Bytes> {
        let deadline = Instant::now() + self.timeout;
        for attempt in 0..=self.retry_count {
            let remaining = remaining_timeout(deadline)?;
            let result =
                tokio::time::timeout(remaining, self.fetch_range_response(url, offset, count))
                    .await
                    .context(format!(
                        "timed out while reading range {offset}+{count} from {url}"
                    ))?;
            match result {
                Ok(resp) => {
                    let body = resp
                        .bytes()
                        .await
                        .context("read range response body failed")?;
                    return Ok(body);
                }
                Err(err) => {
                    if attempt == self.retry_count {
                        return Err(err);
                    }
                    tokio::time::sleep(RETRY_BACKOFF).await;
                }
            }
        }
        bail!("read_range retry loop exited unexpectedly");
    }

    async fn read_range_into(&self, url: &str, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let source = crate::metrics::registry_source(&self.accelerate_address(), url);
        let start = Instant::now();
        let result = self.read_range_into_inner(url, offset, dst).await;
        crate::metrics::record_remote_read(
            &source,
            crate::metrics::RemoteReadOperation::ReadRangeInto,
            &result,
            |filled| *filled as u64,
            start.elapsed(),
        );
        result
    }

    async fn read_range_into_inner(&self, url: &str, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let deadline = Instant::now() + self.timeout;
        for attempt in 0..=self.retry_count {
            let remaining = remaining_timeout(deadline)?;
            let result =
                tokio::time::timeout(remaining, self.fetch_range_response(url, offset, dst.len()))
                    .await
                    .context(format!(
                        "timed out while reading range {offset}+{} from {url}",
                        dst.len()
                    ))?;
            match result {
                Ok(mut resp) => {
                    let mut filled = 0usize;
                    while let Some(chunk) = resp
                        .chunk()
                        .await
                        .context("read range response body failed")?
                    {
                        let received = filled + chunk.len();
                        if received > dst.len() {
                            bail!("range response larger than requested for {url} at {offset}");
                        }

                        dst[filled..received].copy_from_slice(&chunk);
                        filled = received;
                    }
                    return Ok(filled);
                }
                Err(err) => {
                    if attempt == self.retry_count {
                        return Err(err);
                    }
                    tokio::time::sleep(RETRY_BACKOFF).await;
                }
            }
        }
        bail!("read_range_into retry loop exited unexpectedly");
    }

    async fn get_length(&self, url: &str) -> Result<u64> {
        let source = crate::metrics::registry_source(&self.accelerate_address(), url);
        let start = Instant::now();
        let result = self.get_length_inner(url).await;
        crate::metrics::record_remote_metadata(&source, &result, start.elapsed());
        result
    }

    async fn get_length_inner(&self, url: &str) -> Result<u64> {
        if let Some(entry) = self.meta_sizes.get(url) {
            if entry.expire_at > Instant::now() {
                return Ok(entry.size);
            }
        }

        let deadline = Instant::now() + self.timeout;
        for attempt in 0..=self.retry_count {
            let remaining = remaining_timeout(deadline)?;
            let result = tokio::time::timeout(remaining, self.fetch_range_response(url, 0, 1))
                .await
                .context(format!("timed out while getting length from {url}"))?;
            match result {
                Ok(resp) => {
                    let status = resp.status();
                    let headers = resp.headers().clone();
                    let body = resp
                        .bytes()
                        .await
                        .context("read length response body failed")?;
                    if let Some(total) = parse_content_range_total(&headers) {
                        self.meta_sizes.insert(
                            url.to_string(),
                            CachedMetaSize {
                                size: total,
                                expire_at: Instant::now() + Self::meta_ttl(),
                            },
                        );
                        return Ok(total);
                    }
                    if status == StatusCode::OK {
                        let content_len = headers
                            .get(CONTENT_LENGTH)
                            .and_then(|x| x.to_str().ok())
                            .and_then(|x| x.parse::<u64>().ok());
                        if let Some(v) = content_len {
                            self.meta_sizes.insert(
                                url.to_string(),
                                CachedMetaSize {
                                    size: v,
                                    expire_at: Instant::now() + Self::meta_ttl(),
                                },
                            );
                            return Ok(v);
                        }
                    }
                    let size = body.len() as u64;
                    self.meta_sizes.insert(
                        url.to_string(),
                        CachedMetaSize {
                            size,
                            expire_at: Instant::now() + Self::meta_ttl(),
                        },
                    );
                    return Ok(size);
                }
                Err(err) => {
                    if attempt == self.retry_count {
                        self.invalidate_meta_size(url);
                        return Err(err);
                    }
                    tokio::time::sleep(RETRY_BACKOFF).await;
                }
            }
        }
        bail!("get_length retry loop exited unexpectedly");
    }
}

#[derive(Debug, Clone)]
pub struct RegistryFsV2 {
    inner: Arc<RegistryFSImplV2>,
}

impl RegistryFsV2 {
    pub fn new() -> Self {
        Self::with_options(RegistryFsV2Options::default()).expect("default remote backend")
    }

    pub fn with_options(options: RegistryFsV2Options) -> Result<Self> {
        let credential_mode = CredentialMode::from_config(&options.credential)?;
        Self::with_credential_mode(options, credential_mode)
    }

    pub fn with_static_credentials(
        options: RegistryFsV2Options,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self> {
        let credential_mode = CredentialMode::Static {
            username: username.into(),
            password: password.into(),
        };
        Self::with_credential_mode(options, credential_mode)
    }

    fn with_credential_mode(
        options: RegistryFsV2Options,
        credential_mode: CredentialMode,
    ) -> Result<Self> {
        let client = build_http_client(&options)?;
        Ok(Self {
            inner: Arc::new(RegistryFSImplV2 {
                client,
                credential_mode,
                timeout: options.timeout,
                retry_count: options.retry_count,
                accelerate_address: RwLock::new(options.accelerate_address),
                bearer_tokens: DashMap::new(),
                bearer_token_locks: DashMap::new(),
                registry_auth_challenges: DashMap::new(),
                registry_auth_locks: DashMap::new(),
                url_infos: DashMap::new(),
                meta_sizes: DashMap::new(),
            }),
        })
    }

    pub fn from_global_config(cfg: &GlobalConfig) -> Result<Self> {
        Self::with_options(RegistryFsV2Options::from_global_config(cfg))
    }

    pub fn set_accelerate_address(&self, addr: impl Into<String>) {
        if let Ok(mut guard) = self.inner.accelerate_address.write() {
            *guard = addr.into();
        }
    }

    pub async fn open_file(&self, url: impl Into<String>) -> Result<Arc<RegistryFileImplV2>> {
        let file = Arc::new(RegistryFileImplV2::new(
            url.into(),
            self.inner.clone(),
            None,
        ));
        let _ = file.size().await?;
        Ok(file)
    }

    pub fn open_file_with_size_hint(
        &self,
        url: impl Into<String>,
        size_hint: Option<u64>,
    ) -> Arc<RegistryFileImplV2> {
        Arc::new(RegistryFileImplV2::new(
            url.into(),
            self.inner.clone(),
            size_hint,
        ))
    }

    pub async fn open(&self, url: impl Into<String>) -> Result<Arc<dyn VirtualFile>> {
        let file = self.open_file(url).await?;
        Ok(file)
    }

    pub fn open_with_size_hint(
        &self,
        url: impl Into<String>,
        size_hint: Option<u64>,
    ) -> Arc<dyn VirtualFile> {
        self.open_file_with_size_hint(url, size_hint)
    }

    pub async fn refresh_token(&self, url: &str) -> Result<Option<String>> {
        self.inner.refresh_token(url).await
    }
}

impl Default for RegistryFsV2 {
    fn default() -> Self {
        Self::new()
    }
}

// Backward-compatible aliases for previous module naming.
pub type RemoteBackendOptions = RegistryFsV2Options;
pub type RemoteBackend = RegistryFsV2;
pub type RemoteFile = RegistryFileImplV2;

#[derive(Debug, Clone)]
pub struct RegistryUploaderOptions {
    pub upload_url: String,
    pub username: String,
    pub password: String,
    pub timeout: Duration,
    pub upload_chunk_size: usize,
    pub cert_file: String,
    pub key_file: String,
    pub user_agent: String,
}

impl Default for RegistryUploaderOptions {
    fn default() -> Self {
        Self {
            upload_url: String::new(),
            username: String::new(),
            password: String::new(),
            timeout: DEFAULT_TIMEOUT,
            upload_chunk_size: DEFAULT_UPLOAD_CHUNK_SIZE,
            cert_file: String::new(),
            key_file: String::new(),
            user_agent: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

#[derive(Debug)]
pub struct RegistryUploader {
    local_file: Arc<LocalFile>,
    hasher: Sha256,
    write_pos: u64,
    digest: Option<String>,
    finished: bool,
    state: Arc<Mutex<UploadRuntimeState>>,
    notify: Arc<Notify>,
    worker: Option<tokio::task::JoinHandle<Result<()>>>,
}

#[derive(Debug, Default)]
struct UploadRuntimeState {
    write_pos: u64,
    upload_pos: u64,
    finished: bool,
    final_digest: Option<String>,
    failed_reason: Option<String>,
}

#[derive(Debug)]
struct UploadSession {
    local_file: Arc<LocalFile>,
    backend: RegistryFsV2,
    backend_options: RegistryFsV2Options,
    username: String,
    password: String,
    origin_upload_url: String,
    upload_url: String,
    token: Option<String>,
    upload_chunk_size: usize,
    retry_count: u32,
    upload_pos: u64,
    last_client_refresh: Instant,
}

impl UploadSession {
    async fn from_options(
        local_file: Arc<LocalFile>,
        options: RegistryUploaderOptions,
    ) -> Result<Self> {
        let chunk_size = if options.upload_chunk_size == 0 {
            DEFAULT_UPLOAD_CHUNK_SIZE
        } else {
            options.upload_chunk_size
        };

        let backend_options = RegistryFsV2Options {
            timeout: options.timeout,
            retry_count: DEFAULT_RETRY_COUNT,
            user_agent: options.user_agent,
            credential: CredentialConfig::default(),
            accelerate_address: String::new(),
            cert_file: options.cert_file,
            key_file: options.key_file,
        };
        let username = options.username;
        let password = options.password;
        let backend = RegistryFsV2::with_static_credentials(
            backend_options.clone(),
            username.clone(),
            password.clone(),
        )?;

        let mut session = Self {
            local_file,
            backend,
            backend_options,
            username,
            password,
            origin_upload_url: options.upload_url.clone(),
            upload_url: options.upload_url,
            token: None,
            upload_chunk_size: chunk_size,
            retry_count: DEFAULT_RETRY_COUNT,
            upload_pos: 0,
            last_client_refresh: Instant::now(),
        };
        session.init_upload().await?;
        Ok(session)
    }

    async fn refresh_backend_if_needed(&mut self) -> Result<()> {
        let elapsed = Instant::now().saturating_duration_since(self.last_client_refresh);
        if elapsed < HTTP_CLIENT_REFRESH_INTERVAL {
            return Ok(());
        }
        self.backend = RegistryFsV2::with_static_credentials(
            self.backend_options.clone(),
            self.username.clone(),
            self.password.clone(),
        )?;
        self.last_client_refresh = Instant::now();
        Ok(())
    }

    async fn mark_failed(state: &Arc<Mutex<UploadRuntimeState>>, err: &anyhow::Error) {
        let mut guard = state.lock().await;
        if guard.failed_reason.is_none() {
            guard.failed_reason = Some(err.to_string());
        }
    }

    async fn reset_session(&mut self, state: &Arc<Mutex<UploadRuntimeState>>) -> Result<()> {
        self.upload_pos = 0;
        self.upload_url = self.origin_upload_url.clone();
        self.token = None;
        self.init_upload().await?;
        let mut guard = state.lock().await;
        guard.upload_pos = 0;
        Ok(())
    }

    async fn run(
        mut self,
        state: Arc<Mutex<UploadRuntimeState>>,
        notify: Arc<Notify>,
    ) -> Result<()> {
        let mut session_retries = self.retry_count;

        loop {
            let (write_pos, finished, final_digest, failed_reason) = {
                let guard = state.lock().await;
                (
                    guard.write_pos,
                    guard.finished,
                    guard.final_digest.clone(),
                    guard.failed_reason.clone(),
                )
            };

            if let Some(reason) = failed_reason {
                bail!("upload worker stopped: {reason}");
            }

            if write_pos > self.upload_pos + self.upload_chunk_size as u64 {
                match self
                    .upload_chunk(self.upload_pos, self.upload_chunk_size, None)
                    .await
                {
                    Ok(new_pos) => {
                        self.upload_pos = new_pos;
                        let mut guard = state.lock().await;
                        guard.upload_pos = self.upload_pos;
                    }
                    Err(err) => {
                        if session_retries > 0 {
                            session_retries -= 1;
                            self.reset_session(&state).await?;
                            continue;
                        }
                        Self::mark_failed(&state, &err).await;
                        return Err(err);
                    }
                }
                continue;
            }

            if finished {
                if write_pos > self.upload_pos {
                    let remaining = write_pos
                        .checked_sub(self.upload_pos)
                        .ok_or_else(|| anyhow::anyhow!("invalid worker upload cursor"))?;
                    let count = min(remaining as usize, self.upload_chunk_size);
                    match self.upload_chunk(self.upload_pos, count, None).await {
                        Ok(new_pos) => {
                            self.upload_pos = new_pos;
                            let mut guard = state.lock().await;
                            guard.upload_pos = self.upload_pos;
                        }
                        Err(err) => {
                            if session_retries > 0 {
                                session_retries -= 1;
                                self.reset_session(&state).await?;
                                continue;
                            }
                            Self::mark_failed(&state, &err).await;
                            return Err(err);
                        }
                    }
                    continue;
                }

                let digest = final_digest.ok_or_else(|| {
                    anyhow::anyhow!("finish requested but final digest is missing")
                })?;

                if let Err(err) = self.upload_chunk(self.upload_pos, 0, Some(&digest)).await {
                    if session_retries > 0 {
                        session_retries -= 1;
                        self.reset_session(&state).await?;
                        continue;
                    }
                    Self::mark_failed(&state, &err).await;
                    return Err(err);
                }
                return Ok(());
            }

            notify.notified().await;
        }
    }

    async fn init_upload(&mut self) -> Result<()> {
        self.upload_url = self.origin_upload_url.clone();
        self.token = self.backend.refresh_token(&self.upload_url).await?;

        let mut req = self
            .backend
            .inner
            .client
            .post(&self.upload_url)
            .header("Content-Type", "application/octet-stream")
            .timeout(self.backend.inner.timeout);
        if let Some(token) = self.token.as_deref() {
            if !token.is_empty() {
                req = req.header(AUTHORIZATION, format!("Bearer {token}"));
            }
        }
        let resp = req
            .send()
            .await
            .context(format!("failed to init upload session {}", self.upload_url))?;
        let status = resp.status();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            bail!("upload init token invalid: http status {}", status.as_u16());
        }
        if !status.is_success() {
            bail!(
                "failed to get upload session: http status {}",
                status.as_u16()
            );
        }
        let location = resp
            .headers()
            .get(LOCATION)
            .ok_or_else(|| anyhow::anyhow!("upload init missing Location"))?
            .to_str()
            .context("invalid upload init Location header")?;
        self.upload_url = location.to_string();
        Ok(())
    }

    fn append_digest_query(url: &str, digest: &str) -> String {
        let delimiter = if url.contains('?') { "&" } else { "?" };
        format!("{url}{delimiter}digest={digest}")
    }

    async fn upload_chunk(
        &mut self,
        offset: u64,
        count: usize,
        digest: Option<&str>,
    ) -> Result<u64> {
        self.refresh_backend_if_needed().await?;

        let method = if digest.is_some() { "PUT" } else { "PATCH" };
        let base_url = self.upload_url.clone();
        let request_url = if let Some(digest) = digest {
            Self::append_digest_query(&base_url, digest)
        } else {
            base_url.clone()
        };
        let body = if count == 0 {
            Bytes::new()
        } else {
            let data = self.local_file.read_at(offset, count).await?;
            if data.len() != count {
                bail!(
                    "upload chunk short read: expected {count}, got {}",
                    data.len()
                );
            }
            data
        };

        for attempt in 0..=self.retry_count {
            let mut req = self
                .backend
                .inner
                .client
                .request(
                    reqwest::Method::from_bytes(method.as_bytes())
                        .context(format!("invalid upload method {method}"))?,
                    &request_url,
                )
                .header(CONTENT_LENGTH, count.to_string())
                .timeout(self.backend.inner.timeout);

            if digest.is_none() {
                let range_end = offset
                    .checked_add(count as u64)
                    .and_then(|x| x.checked_sub(1))
                    .ok_or_else(|| anyhow::anyhow!("upload range overflow"))?;
                req = req
                    .header("Content-Type", "application/octet-stream")
                    .header("Content-Range", format!("{offset}-{range_end}"))
                    .body(body.clone());
            }
            if let Some(token) = self.token.as_deref() {
                if !token.is_empty() {
                    req = req.header(AUTHORIZATION, format!("Bearer {token}"));
                }
            }
            if digest.is_some() {
                req = req.body(Bytes::new());
            }

            let resp = req
                .send()
                .await
                .context(format!("upload request failed for {request_url}"))?;
            let status = resp.status();
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                if attempt < self.retry_count {
                    self.token = self.backend.refresh_token(&self.upload_url).await?;
                    continue;
                }
                bail!("upload token invalid: http status {}", status.as_u16());
            }
            if !status.is_success() {
                bail!("upload request failed: http status {}", status.as_u16());
            }
            if let Some(location) = resp.headers().get(LOCATION).and_then(|x| x.to_str().ok()) {
                self.upload_url = location.to_string();
            }
            if count == 0 {
                return Ok(self.upload_pos);
            }
            let end = parse_upload_range_end(resp.headers())
                .ok_or_else(|| anyhow::anyhow!("upload response missing/invalid Range header"))?;
            return end
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("upload range overflow"));
        }

        bail!("upload chunk retry loop exited unexpectedly");
    }
}

impl RegistryUploader {
    pub async fn new(local_file: Arc<LocalFile>, options: RegistryUploaderOptions) -> Result<Self> {
        if options.upload_url.is_empty() {
            bail!("upload_url cannot be empty");
        }
        let state = Arc::new(Mutex::new(UploadRuntimeState::default()));
        let notify = Arc::new(Notify::new());
        let session = UploadSession::from_options(local_file.clone(), options).await?;
        let worker = tokio::spawn(session.run(state.clone(), notify.clone()));

        Ok(Self {
            local_file,
            hasher: Sha256::new(),
            write_pos: 0,
            digest: None,
            finished: false,
            state,
            notify,
            worker: Some(worker),
        })
    }

    async fn ensure_not_failed(&self) -> Result<()> {
        let guard = self.state.lock().await;
        if let Some(reason) = guard.failed_reason.as_ref() {
            bail!("uploader already failed: {reason}");
        }
        Ok(())
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.finished {
            bail!("uploader already finished");
        }
        if buf.is_empty() {
            return Ok(0);
        }
        self.ensure_not_failed().await?;

        let written = self.local_file.write_at(self.write_pos, buf).await?;
        if written > 0 {
            self.hasher.update(&buf[..written]);
            self.write_pos = self
                .write_pos
                .checked_add(written as u64)
                .ok_or_else(|| anyhow::anyhow!("write position overflow"))?;
            {
                let mut guard = self.state.lock().await;
                guard.write_pos = self.write_pos;
            }
            self.notify.notify_one();
        }

        Ok(written)
    }

    pub fn digest(&self) -> Option<&str> {
        self.digest.as_deref()
    }

    pub async fn finish(&mut self) -> Result<String> {
        if let Some(digest) = self.digest.clone() {
            return Ok(digest);
        }
        self.ensure_not_failed().await?;

        let digest_raw = std::mem::take(&mut self.hasher).finalize();
        let digest = format!("sha256:{}", encode_hex(&digest_raw));
        {
            let mut guard = self.state.lock().await;
            guard.write_pos = self.write_pos;
            guard.finished = true;
            guard.final_digest = Some(digest.clone());
        }
        self.notify.notify_one();

        if let Some(worker) = self.worker.take() {
            let result = worker.await.context("uploader worker join failed")?;
            result?;
        }

        self.ensure_not_failed().await?;
        self.finished = true;
        self.digest = Some(digest.clone());
        Ok(digest)
    }
}

impl Drop for RegistryUploader {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

pub async fn new_registry_uploader(
    local_file: Arc<LocalFile>,
    options: RegistryUploaderOptions,
) -> Result<RegistryUploader> {
    RegistryUploader::new(local_file, options).await
}

pub async fn registry_uploader_fini(uploader: &mut RegistryUploader) -> Result<String> {
    uploader.finish().await
}

#[derive(Debug)]
pub struct RegistryFileImplV2 {
    url: String,
    backend: Arc<RegistryFSImplV2>,
    size_cache: Mutex<Option<u64>>,
}

impl RegistryFileImplV2 {
    fn new(url: String, backend: Arc<RegistryFSImplV2>, size_hint: Option<u64>) -> Self {
        Self {
            url: normalize_url(&url),
            backend,
            size_cache: Mutex::new(size_hint),
        }
    }
}

#[async_trait]
impl VirtualFile for RegistryFileImplV2 {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let size = self.size().await?;
        if offset >= size {
            return Ok(Bytes::new());
        }
        let count = min((size - offset) as usize, len);
        self.backend.read_range(&self.url, offset, count).await
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }
        let size = self.size().await?;
        if offset >= size {
            return Ok(0);
        }
        let count = min((size - offset) as usize, dst.len());
        self.backend
            .read_range_into(&self.url, offset, &mut dst[..count])
            .await
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("remote file backend is read-only");
    }

    async fn size(&self) -> Result<u64> {
        let mut guard = self.size_cache.lock().await;
        if let Some(v) = *guard {
            return Ok(v);
        }
        let size = self.backend.get_length(&self.url).await?;
        *guard = Some(size);
        Ok(size)
    }
}

fn build_http_client(options: &RegistryFsV2Options) -> Result<Client> {
    let mut builder = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(options.timeout)
        .timeout(options.timeout)
        .user_agent(options.user_agent.clone());

    if !options.cert_file.is_empty() && !options.key_file.is_empty() {
        let cert = fs::read(&options.cert_file)
            .context(format!("read cert file {} failed", options.cert_file))?;
        let key = fs::read(&options.key_file)
            .context(format!("read key file {} failed", options.key_file))?;
        let mut pem = cert;
        pem.push(b'\n');
        pem.extend_from_slice(&key);
        let identity = Identity::from_pem(&pem).context("parse cert/key pem identity failed")?;
        builder = builder.identity(identity);
    }

    builder.build().context("build http client failed")
}

fn http_status_error(resp: reqwest::Response, context: impl Into<String>) -> anyhow::Error {
    match resp.error_for_status() {
        Ok(_) => anyhow::anyhow!(context.into()),
        Err(err) => anyhow::Error::from(err).context(context.into()),
    }
}

fn normalize_url(url: &str) -> String {
    url.trim_start_matches('/').to_string()
}

fn build_basic_auth_header(username: &str, password: &str) -> String {
    let token = BASE64_STANDARD.encode(format!("{username}:{password}"));
    format!("Basic {token}")
}

fn parse_auth_challenge(challenge: &str) -> Result<AuthChallenge> {
    if starts_with_ignore_ascii_case(challenge, "basic ") {
        return Ok(AuthChallenge::Basic);
    }
    let bearer = parse_bearer_challenge_parts(challenge)?;
    let auth_url = build_bearer_auth_url(&bearer.realm, &bearer.service, bearer.scope.as_deref())?;
    Ok(AuthChallenge::Bearer { auth_url })
}

fn parse_registry_auth_challenge(challenge: &str) -> Result<RegistryAuthChallenge> {
    if starts_with_ignore_ascii_case(challenge, "basic ") {
        return Ok(RegistryAuthChallenge::Basic);
    }
    let bearer = parse_bearer_challenge_parts(challenge)?;
    Ok(RegistryAuthChallenge::Bearer {
        realm: bearer.realm,
        service: bearer.service,
    })
}

fn parse_bearer_challenge_parts(challenge: &str) -> Result<BearerChallengeParts> {
    if !starts_with_ignore_ascii_case(challenge, "bearer ") {
        bail!("unsupported auth challenge: {challenge}");
    }

    let tail = challenge[7..].trim();
    let kv = parse_kv_map(tail);
    let realm = kv
        .get("realm")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("auth challenge missing realm"))?;
    let service = kv
        .get("service")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("auth challenge missing service"))?;
    let scope = kv.get("scope").filter(|s| !s.is_empty()).cloned();

    Ok(BearerChallengeParts {
        realm,
        service,
        scope,
    })
}

fn build_bearer_auth_url(realm: &str, service: &str, scope: Option<&str>) -> Result<Url> {
    let mut auth_url =
        Url::parse(realm).context(format!("invalid realm URL in auth challenge: {realm}"))?;
    auth_url.query_pairs_mut().append_pair("service", service);
    // per https://datatracker.ietf.org/doc/html/rfc6750#section-3, "scope" is optional.
    if let Some(scope) = scope.filter(|s| !s.is_empty()) {
        auth_url.query_pairs_mut().append_pair("scope", scope);
    }
    Ok(auth_url)
}

fn blob_auth_context(remote_url: &str) -> Option<BlobAuthContext> {
    let url = Url::parse(remote_url).ok()?;
    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let host = url.host_str()?;
    let origin = match url.port() {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    };

    let segments: Vec<&str> = url.path_segments()?.collect();
    if segments.len() < 4 || segments.first() != Some(&"v2") {
        return None;
    }
    let blobs_index = segments.len().checked_sub(2)?;
    if segments.get(blobs_index) != Some(&"blobs") {
        return None;
    }
    if segments.last().is_none_or(|digest| digest.is_empty()) {
        return None;
    }
    let repo_segments: Vec<String> = segments[1..blobs_index]
        .iter()
        .map(|segment| (*segment).to_string())
        .collect();
    if repo_segments.is_empty() || repo_segments.iter().any(|segment| segment.is_empty()) {
        return None;
    }
    let scope = format!("repository:{}:pull", repo_segments.join("/"));

    let mut ping_url = url;
    ping_url.set_path("/v2/");
    ping_url.set_query(None);
    ping_url.set_fragment(None);

    Some(BlobAuthContext {
        origin,
        ping_url,
        scope,
    })
}

fn starts_with_ignore_ascii_case(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix)
}

fn parse_kv_map(s: &str) -> HashMap<String, String> {
    let mut parts = Vec::new();
    let mut buf = String::new();
    let mut in_quotes = false;
    for ch in s.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                buf.push(ch);
            }
            ',' if !in_quotes => {
                if !buf.trim().is_empty() {
                    parts.push(buf.trim().to_string());
                }
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() {
        parts.push(buf.trim().to_string());
    }

    let mut kv = HashMap::new();
    for part in parts {
        if let Some((k, v)) = part.split_once('=') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().trim_matches('"').to_string();
            kv.insert(key, val);
        }
    }
    kv
}

fn parse_content_range_total(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let raw = headers.get(CONTENT_RANGE)?.to_str().ok()?;
    let (_, total) = raw.rsplit_once('/')?;
    if total == "*" {
        return None;
    }
    total.parse::<u64>().ok()
}

fn parse_upload_range_end(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let raw = headers.get(RANGE)?.to_str().ok()?.trim();
    let raw = raw.strip_prefix("bytes=").unwrap_or(raw);
    let (_, end) = raw.rsplit_once('-')?;
    end.parse::<u64>().ok()
}

fn remaining_timeout(deadline: Instant) -> Result<Duration> {
    let now = Instant::now();
    let left = deadline.saturating_duration_since(now);
    if left.is_zero() {
        bail!("operation timed out");
    }
    Ok(left)
}

fn encode_hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(TABLE[(b >> 4) as usize] as char);
        out.push(TABLE[(b & 0x0f) as usize] as char);
    }
    out
}

fn parse_auths(auths: &Value, remote_url: &str) -> (String, String) {
    let Some(obj) = auths.as_object() else {
        return (String::new(), String::new());
    };
    let seg = parse_blob_segments(remote_url);

    for (addr, val) in obj {
        if !address_matches(addr, &seg) {
            continue;
        }

        let mut cred: Option<(String, String)> = None;
        if let Some(auth) = val.get("auth").and_then(Value::as_str) {
            if let Ok(decoded) = BASE64_STANDARD.decode(auth) {
                if let Ok(decoded) = String::from_utf8(decoded) {
                    if let Some((u, p)) = decoded.split_once(':') {
                        cred = Some((u.to_string(), p.to_string()));
                    }
                }
            }
        }

        if cred.is_none() {
            if let (Some(u), Some(p)) = (
                val.get("username").and_then(Value::as_str),
                val.get("password").and_then(Value::as_str),
            ) {
                cred = Some((u.to_string(), p.to_string()));
            }
        }

        if let Some((u, p)) = cred {
            return (u, p);
        }
    }

    (String::new(), String::new())
}

/// Resolve Docker-compatible credentials from one config file.
///
/// Static `auths` entries win, matching Docker and regctl. When none matches,
/// invoke the registry-specific `credHelpers` entry or the global `credsStore`
/// helper. Invoking the helper on every resolution is deliberate: cloud
/// registry credentials such as ECR tokens expire, while the helper can use a
/// refreshable workload identity. The command is executed directly, never via
/// a shell, and the helper suffix cannot name a path.
async fn resolve_docker_config_credentials(
    config: &AuthConfig,
    remote_url: &str,
    timeout: Duration,
) -> Result<(String, String)> {
    let static_credentials = parse_auths(&config.auths, remote_url);
    if !static_credentials.0.is_empty() || !static_credentials.1.is_empty() {
        return Ok(static_credentials);
    }

    let registry = registry_server(remote_url)?;
    let helper = config
        .cred_helpers
        .get(&registry)
        .filter(|value| !value.is_empty())
        .or_else(|| (!config.creds_store.is_empty()).then_some(&config.creds_store));
    let Some(helper) = helper else {
        return Ok((String::new(), String::new()));
    };

    run_credential_helper(helper, &registry, timeout).await
}

fn registry_server(remote_url: &str) -> Result<String> {
    let url = Url::parse(remote_url).context("parse registry URL for credential helper")?;
    let host = url
        .host_str()
        .context("registry URL has no host for credential helper")?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

async fn run_credential_helper(
    helper: &str,
    registry: &str,
    timeout: Duration,
) -> Result<(String, String)> {
    if !helper
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("invalid Docker credential helper name {helper:?}");
    }

    let binary = format!("docker-credential-{helper}");
    run_credential_helper_command(&binary, registry, timeout).await
}

async fn run_credential_helper_command(
    binary: &str,
    registry: &str,
    timeout: Duration,
) -> Result<(String, String)> {
    let mut child = Command::new(binary)
        .arg("get")
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("start Docker credential helper {binary}"))?;
    child
        .stdin
        .take()
        .context("credential helper stdin was not piped")?
        .write_all(format!("{registry}\n").as_bytes())
        .await
        .context("write registry to Docker credential helper")?;

    let output = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .with_context(|| format!("Docker credential helper {binary} timed out"))??;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!("Docker credential helper {binary} failed: {stderr}");
    }

    let value: Value = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("parse Docker credential helper {binary} response"))?;
    let username = value
        .get("Username")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("Docker credential helper response has no Username")?;
    let secret = value
        .get("Secret")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("Docker credential helper response has no Secret")?;
    Ok((username.to_string(), secret.to_string()))
}

fn parse_blob_segments(remote_url: &str) -> Vec<String> {
    let without_scheme = if let Some(v) = remote_url.strip_prefix("http://") {
        v
    } else if let Some(v) = remote_url.strip_prefix("https://") {
        v
    } else {
        return Vec::new();
    };
    let words: Vec<&str> = without_scheme
        .split('/')
        .filter(|x| !x.is_empty())
        .collect();
    if words.is_empty() {
        return Vec::new();
    }
    let mut seg = vec![words[0].to_string()];
    if words.len() > 3 {
        for w in &words[2..words.len() - 1] {
            seg.push((*w).to_string());
        }
    }
    seg
}

fn address_matches(addr: &str, seg: &[String]) -> bool {
    let mut prefix = String::new();
    for s in seg {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(s);
        if addr == prefix {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::virtual_file::VirtualFile;
    use crate::test_utils::test_io_ring;
    use axum::body::Body;
    use axum::extract::{Query, State};
    use axum::http::header::{
        AUTHORIZATION as AUTHORIZATION_RAW, CONTENT_RANGE as CONTENT_RANGE_RAW,
    };
    use axum::http::{HeaderMap as HttpHeaderMap, Method, Response, StatusCode as HttpStatusCode};
    use axum::routing::{any, get, patch, post, put};
    use axum::{Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex as AsyncMutex;

    async fn spawn_server(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
        let addr = listener.local_addr().expect("server local addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("run server");
        });
        (format!("http://{addr}"), handle)
    }

    fn parse_request_range(headers: &HttpHeaderMap) -> Option<(u64, u64)> {
        let raw = headers.get(RANGE)?.to_str().ok()?.trim();
        let raw = raw.strip_prefix("bytes=")?;
        let (start, end) = raw.split_once('-')?;
        Some((start.parse().ok()?, end.parse().ok()?))
    }

    fn digest_of(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("sha256:{}", encode_hex(&hasher.finalize()))
    }

    fn bearer_challenge(host: &str, scope: &str) -> String {
        format!(r#"Bearer realm="http://{host}/token",service="registry.test",scope="{scope}""#)
    }

    fn bearer_challenge_without_scope(host: &str) -> String {
        format!(r#"Bearer realm="http://{host}/token",service="registry.test""#)
    }

    #[derive(Clone, Debug)]
    struct ReadTestState {
        blob: Arc<Vec<u8>>,
        data_hits: Arc<AtomicUsize>,
        token_hits: Arc<AtomicUsize>,
    }

    async fn handle_read_blob(
        State(_state): State<ReadTestState>,
        headers: HttpHeaderMap,
    ) -> Response<Body> {
        let host = headers
            .get("host")
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();
        let challenge = bearer_challenge(host, "repository:ns/repo:pull");
        let auth = headers
            .get(AUTHORIZATION_RAW)
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();

        if auth != "Bearer read-token" {
            return Response::builder()
                .status(HttpStatusCode::UNAUTHORIZED)
                .header(WWW_AUTHENTICATE, challenge)
                .body(Body::empty())
                .expect("401 response");
        }

        let location = format!("http://{host}/data");
        Response::builder()
            .status(HttpStatusCode::FOUND)
            .header(LOCATION, location)
            .body(Body::empty())
            .expect("302 response")
    }

    async fn handle_read_data(
        State(state): State<ReadTestState>,
        headers: HttpHeaderMap,
    ) -> Response<Body> {
        let (start, end) = parse_request_range(&headers).expect("range header");
        let len = state.blob.len() as u64;
        let start = start.min(len.saturating_sub(1));
        let end = end.min(len.saturating_sub(1));
        let body = state.blob[start as usize..=end as usize].to_vec();
        state.data_hits.fetch_add(1, Ordering::Relaxed);
        Response::builder()
            .status(HttpStatusCode::PARTIAL_CONTENT)
            .header(CONTENT_RANGE_RAW, format!("bytes {start}-{end}/{len}"))
            .header(CONTENT_LENGTH, body.len().to_string())
            .body(Body::from(body))
            .expect("206 response")
    }

    async fn handle_token(State(state): State<ReadTestState>) -> Json<Value> {
        state.token_hits.fetch_add(1, Ordering::Relaxed);
        Json(json!({"token": "read-token"}))
    }

    #[derive(Clone, Debug)]
    struct PingCacheState {
        blob: Arc<Vec<u8>>,
        data_hits: Arc<AtomicUsize>,
        ping_hits: Arc<AtomicUsize>,
        token_hits: Arc<AtomicUsize>,
        unauth_blob_hits: Arc<AtomicUsize>,
    }

    async fn handle_counting_ping_cache_v2(
        State(state): State<PingCacheState>,
        headers: HttpHeaderMap,
    ) -> Response<Body> {
        state.ping_hits.fetch_add(1, Ordering::Relaxed);
        let host = headers
            .get("host")
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();
        Response::builder()
            .status(HttpStatusCode::UNAUTHORIZED)
            .header(WWW_AUTHENTICATE, bearer_challenge_without_scope(host))
            .body(Body::empty())
            .expect("v2 ping response")
    }

    async fn handle_bad_ping_v2(State(state): State<PingCacheState>) -> Response<Body> {
        state.ping_hits.fetch_add(1, Ordering::Relaxed);
        Response::builder()
            .status(HttpStatusCode::UNAUTHORIZED)
            .body(Body::empty())
            .expect("bad v2 ping response")
    }

    async fn handle_ping_cache_blob(
        State(state): State<PingCacheState>,
        headers: HttpHeaderMap,
    ) -> Response<Body> {
        let host = headers
            .get("host")
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();
        let auth = headers
            .get(AUTHORIZATION_RAW)
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();

        if auth != "Bearer read-token" {
            state.unauth_blob_hits.fetch_add(1, Ordering::Relaxed);
            return Response::builder()
                .status(HttpStatusCode::UNAUTHORIZED)
                .header(
                    WWW_AUTHENTICATE,
                    bearer_challenge(host, "repository:ns/repo:pull"),
                )
                .body(Body::empty())
                .expect("401 response");
        }

        let location = format!("http://{host}/data");
        Response::builder()
            .status(HttpStatusCode::FOUND)
            .header(LOCATION, location)
            .body(Body::empty())
            .expect("302 response")
    }

    async fn handle_ping_cache_data(
        State(state): State<PingCacheState>,
        headers: HttpHeaderMap,
    ) -> Response<Body> {
        let (start, end) = parse_request_range(&headers).expect("range header");
        let len = state.blob.len() as u64;
        let start = start.min(len.saturating_sub(1));
        let end = end.min(len.saturating_sub(1));
        let body = state.blob[start as usize..=end as usize].to_vec();
        state.data_hits.fetch_add(1, Ordering::Relaxed);
        Response::builder()
            .status(HttpStatusCode::PARTIAL_CONTENT)
            .header(CONTENT_RANGE_RAW, format!("bytes {start}-{end}/{len}"))
            .header(CONTENT_LENGTH, body.len().to_string())
            .body(Body::from(body))
            .expect("206 response")
    }

    async fn handle_ping_cache_token(
        State(state): State<PingCacheState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<Value> {
        assert_eq!(
            query.get("scope").map(String::as_str),
            Some("repository:ns/repo:pull")
        );
        state.token_hits.fetch_add(1, Ordering::Relaxed);
        Json(json!({"token": "read-token"}))
    }

    fn registry_test_options() -> RegistryFsV2Options {
        RegistryFsV2Options {
            timeout: Duration::from_secs(3),
            retry_count: 2,
            user_agent: "registry-test".to_string(),
            credential: CredentialConfig::default(),
            accelerate_address: String::new(),
            cert_file: String::new(),
            key_file: String::new(),
        }
    }

    #[tokio::test]
    async fn test_registry_read_with_redirect() {
        let read_state = ReadTestState {
            blob: Arc::new(b"hello-registryfs-v2".to_vec()),
            data_hits: Arc::new(AtomicUsize::new(0)),
            token_hits: Arc::new(AtomicUsize::new(0)),
        };
        let source_app = Router::new()
            .route("/v2/blob", get(handle_read_blob))
            .route("/data", get(handle_read_data))
            .route("/token", get(handle_token))
            .with_state(read_state.clone());
        let (source_base, source_handle) = spawn_server(source_app).await;

        let backend = RegistryFsV2::with_static_credentials(
            RegistryFsV2Options {
                timeout: Duration::from_secs(3),
                retry_count: 2,
                user_agent: "registry-test".to_string(),
                credential: CredentialConfig::default(),
                accelerate_address: String::new(),
                cert_file: String::new(),
                key_file: String::new(),
            },
            "user",
            "password",
        )
        .expect("backend");
        let blob_url = format!("{source_base}/v2/blob");
        let file = backend.open_file(blob_url).await.expect("open remote file");
        let got = file.read_at(0, 5).await.expect("read remote data");
        let size = file.size().await.expect("size");

        source_handle.abort();

        assert_eq!(got.as_ref(), b"hello");
        assert_eq!(size, read_state.blob.len() as u64);
        assert!(read_state.data_hits.load(Ordering::Relaxed) > 0);
        assert!(read_state.token_hits.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_registry_token_fetch_is_coalesced_per_scope() {
        let read_state = ReadTestState {
            blob: Arc::new(b"hello-registryfs-v2".to_vec()),
            data_hits: Arc::new(AtomicUsize::new(0)),
            token_hits: Arc::new(AtomicUsize::new(0)),
        };
        let source_app = Router::new()
            .route("/v2/blob-a", get(handle_read_blob))
            .route("/v2/blob-b", get(handle_read_blob))
            .route("/data", get(handle_read_data))
            .route("/token", get(handle_token))
            .with_state(read_state.clone());
        let (source_base, source_handle) = spawn_server(source_app).await;

        let backend = RegistryFsV2::with_static_credentials(
            RegistryFsV2Options {
                timeout: Duration::from_secs(3),
                retry_count: 2,
                user_agent: "registry-test".to_string(),
                credential: CredentialConfig::default(),
                accelerate_address: String::new(),
                cert_file: String::new(),
                key_file: String::new(),
            },
            "user",
            "password",
        )
        .expect("backend");
        let blob_a = format!("{source_base}/v2/blob-a");
        let blob_b = format!("{source_base}/v2/blob-b");

        let (file_a, file_b) =
            tokio::try_join!(backend.open_file(blob_a), backend.open_file(blob_b))
                .expect("open files concurrently");
        let (got_a, got_b) = tokio::try_join!(file_a.read_at(0, 5), file_b.read_at(0, 5))
            .expect("read from both files");

        source_handle.abort();

        assert_eq!(got_a.as_ref(), b"hello");
        assert_eq!(got_b.as_ref(), b"hello");
        assert_eq!(read_state.token_hits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_registry_challenge_ping_is_cached_per_origin() {
        let state = PingCacheState {
            blob: Arc::new(b"hello-registryfs-v2".to_vec()),
            data_hits: Arc::new(AtomicUsize::new(0)),
            ping_hits: Arc::new(AtomicUsize::new(0)),
            token_hits: Arc::new(AtomicUsize::new(0)),
            unauth_blob_hits: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/v2/", get(handle_counting_ping_cache_v2))
            .route("/v2/ns/repo/blobs/{digest}", get(handle_ping_cache_blob))
            .route("/data", get(handle_ping_cache_data))
            .route("/token", get(handle_ping_cache_token))
            .with_state(state.clone());
        let (base, handle) = spawn_server(app).await;

        let backend =
            RegistryFsV2::with_static_credentials(registry_test_options(), "user", "password")
                .expect("backend");
        let blob_a = format!("{base}/v2/ns/repo/blobs/sha256:a");
        let blob_b = format!("{base}/v2/ns/repo/blobs/sha256:b");

        let (file_a, file_b) =
            tokio::try_join!(backend.open_file(blob_a), backend.open_file(blob_b))
                .expect("open files concurrently");
        let (got_a, got_b) = tokio::try_join!(file_a.read_at(0, 5), file_b.read_at(0, 5))
            .expect("read from both files");

        handle.abort();

        assert_eq!(got_a.as_ref(), b"hello");
        assert_eq!(got_b.as_ref(), b"hello");
        assert_eq!(state.ping_hits.load(Ordering::Relaxed), 1);
        assert_eq!(state.token_hits.load(Ordering::Relaxed), 1);
        assert_eq!(state.unauth_blob_hits.load(Ordering::Relaxed), 0);
        assert!(state.data_hits.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_registry_bad_ping_falls_back_to_per_url_probe() {
        let state = PingCacheState {
            blob: Arc::new(b"hello-registryfs-v2".to_vec()),
            data_hits: Arc::new(AtomicUsize::new(0)),
            ping_hits: Arc::new(AtomicUsize::new(0)),
            token_hits: Arc::new(AtomicUsize::new(0)),
            unauth_blob_hits: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/v2/", get(handle_bad_ping_v2))
            .route("/v2/ns/repo/blobs/{digest}", get(handle_ping_cache_blob))
            .route("/data", get(handle_ping_cache_data))
            .route("/token", get(handle_ping_cache_token))
            .with_state(state.clone());
        let (base, handle) = spawn_server(app).await;

        let backend =
            RegistryFsV2::with_static_credentials(registry_test_options(), "user", "password")
                .expect("backend");
        let blob_a = format!("{base}/v2/ns/repo/blobs/sha256:a");
        let blob_b = format!("{base}/v2/ns/repo/blobs/sha256:b");

        let file_a = backend.open_file(blob_a).await.expect("open first file");
        let got_a = file_a.read_at(0, 5).await.expect("read first file");
        let file_b = backend.open_file(blob_b).await.expect("open second file");
        let got_b = file_b.read_at(0, 5).await.expect("read second file");

        handle.abort();

        assert_eq!(got_a.as_ref(), b"hello");
        assert_eq!(got_b.as_ref(), b"hello");
        assert_eq!(state.ping_hits.load(Ordering::Relaxed), 1);
        assert_eq!(state.token_hits.load(Ordering::Relaxed), 1);
        assert_eq!(
            state.unauth_blob_hits.load(Ordering::Relaxed),
            2,
            "legacy fallback should probe each blob URL after /v2/ is marked unusable"
        );
        assert!(state.data_hits.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_registry_open_with_size_hint_skips_length_probe() {
        let read_state = ReadTestState {
            blob: Arc::new(b"hello-registryfs-v2".to_vec()),
            data_hits: Arc::new(AtomicUsize::new(0)),
            token_hits: Arc::new(AtomicUsize::new(0)),
        };
        let source_app = Router::new()
            .route("/v2/blob", get(handle_read_blob))
            .route("/data", get(handle_read_data))
            .route("/token", get(handle_token))
            .with_state(read_state.clone());
        let (source_base, source_handle) = spawn_server(source_app).await;

        let backend = RegistryFsV2::with_static_credentials(
            RegistryFsV2Options {
                timeout: Duration::from_secs(3),
                retry_count: 2,
                user_agent: "registry-test".to_string(),
                credential: CredentialConfig::default(),
                accelerate_address: String::new(),
                cert_file: String::new(),
                key_file: String::new(),
            },
            "user",
            "password",
        )
        .expect("backend");
        let blob_url = format!("{source_base}/v2/blob");
        let file = backend.open_file_with_size_hint(blob_url, Some(read_state.blob.len() as u64));

        assert_eq!(
            file.size().await.expect("size from hint"),
            read_state.blob.len() as u64
        );
        assert_eq!(
            read_state.data_hits.load(Ordering::Relaxed),
            0,
            "open with a known size should not probe remote data before the first read"
        );
        assert_eq!(
            read_state.token_hits.load(Ordering::Relaxed),
            0,
            "open with a known size should not challenge auth before the first read"
        );

        let got = file.read_at(0, 5).await.expect("read remote data");
        source_handle.abort();

        assert_eq!(got.as_ref(), b"hello");
        assert!(read_state.data_hits.load(Ordering::Relaxed) > 0);
        assert!(read_state.token_hits.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_range_response_allows_empty_chunk_after_dst_full() {
        let mut dst = [0u8; 5];
        let mut filled = 0usize;

        for chunk in [Bytes::from_static(b"hello"), Bytes::new()] {
            let received = filled + chunk.len();
            assert!(received <= dst.len());

            dst[filled..received].copy_from_slice(&chunk);
            filled = received;
        }

        assert_eq!(filled, dst.len());
        assert_eq!(&dst, b"hello");
    }

    #[tokio::test]
    async fn test_registry_read_with_accelerate_error_no_fallback() {
        let read_state = ReadTestState {
            blob: Arc::new(b"hello-registryfs-v2".to_vec()),
            data_hits: Arc::new(AtomicUsize::new(0)),
            token_hits: Arc::new(AtomicUsize::new(0)),
        };
        let source_app = Router::new()
            .route("/v2/blob", get(handle_read_blob))
            .route("/data", get(handle_read_data))
            .route("/token", get(handle_token))
            .with_state(read_state.clone());
        let (source_base, source_handle) = spawn_server(source_app).await;

        #[derive(Clone, Debug)]
        struct AccelerateState {
            hits: Arc<AtomicUsize>,
        }
        async fn handle_accelerate(
            State(state): State<AccelerateState>,
            _: Method,
        ) -> Response<Body> {
            state.hits.fetch_add(1, Ordering::Relaxed);
            Response::builder()
                .status(HttpStatusCode::BAD_GATEWAY)
                .body(Body::empty())
                .expect("502 response")
        }

        let accel_state = AccelerateState {
            hits: Arc::new(AtomicUsize::new(0)),
        };
        let accel_app = Router::new()
            .route("/{*path}", any(handle_accelerate))
            .with_state(accel_state.clone());
        let (accel_base, accel_handle) = spawn_server(accel_app).await;

        let backend = RegistryFsV2::with_static_credentials(
            RegistryFsV2Options {
                timeout: Duration::from_secs(3),
                retry_count: 2,
                user_agent: "registry-test".to_string(),
                credential: CredentialConfig::default(),
                accelerate_address: accel_base,
                cert_file: String::new(),
                key_file: String::new(),
            },
            "user",
            "password",
        )
        .expect("backend");
        let blob_url = format!("{source_base}/v2/blob");
        let err = backend
            .open_file(blob_url)
            .await
            .expect_err("accelerate error should fail");

        source_handle.abort();
        accel_handle.abort();

        assert!(
            format!("{err:?}").to_lowercase().contains("http status"),
            "error should mention http status"
        );
        assert!(accel_state.hits.load(Ordering::Relaxed) > 0);
        assert_eq!(read_state.data_hits.load(Ordering::Relaxed), 0);
        assert!(read_state.token_hits.load(Ordering::Relaxed) > 0);
    }

    #[derive(Clone, Debug)]
    struct RefreshState {
        blob: Arc<Vec<u8>>,
        token_hits: Arc<AtomicUsize>,
    }

    async fn handle_refresh_blob(headers: HttpHeaderMap) -> Response<Body> {
        let host = headers
            .get("host")
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();
        let challenge = bearer_challenge(host, "repository:ns/repo:pull");
        let auth = headers
            .get(AUTHORIZATION_RAW)
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();
        if auth.is_empty() {
            return Response::builder()
                .status(HttpStatusCode::UNAUTHORIZED)
                .header(WWW_AUTHENTICATE, challenge)
                .body(Body::empty())
                .expect("401 challenge response");
        }
        if auth == "Bearer old-token" {
            return Response::builder()
                .status(HttpStatusCode::UNAUTHORIZED)
                .body(Body::empty())
                .expect("401 old token response");
        }
        let location = format!("http://{host}/refresh-data");
        Response::builder()
            .status(HttpStatusCode::FOUND)
            .header(LOCATION, location)
            .body(Body::empty())
            .expect("302 response")
    }

    async fn handle_refresh_data(
        State(state): State<RefreshState>,
        headers: HttpHeaderMap,
    ) -> Response<Body> {
        let (start, end) = parse_request_range(&headers).expect("range header");
        let len = state.blob.len() as u64;
        let start = start.min(len.saturating_sub(1));
        let end = end.min(len.saturating_sub(1));
        let body = state.blob[start as usize..=end as usize].to_vec();
        Response::builder()
            .status(HttpStatusCode::PARTIAL_CONTENT)
            .header(CONTENT_RANGE_RAW, format!("bytes {start}-{end}/{len}"))
            .header(CONTENT_LENGTH, body.len().to_string())
            .body(Body::from(body))
            .expect("206 response")
    }

    async fn handle_refresh_token(State(state): State<RefreshState>) -> Json<Value> {
        let n = state.token_hits.fetch_add(1, Ordering::Relaxed);
        if n == 0 {
            Json(json!({"token": "old-token"}))
        } else {
            Json(json!({"token": "new-token"}))
        }
    }

    #[tokio::test]
    async fn test_registry_refresh_token_after_401() {
        let state = RefreshState {
            blob: Arc::new(b"token-refresh-success".to_vec()),
            token_hits: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/v2/blob", get(handle_refresh_blob))
            .route("/refresh-data", get(handle_refresh_data))
            .route("/token", get(handle_refresh_token))
            .with_state(state.clone());
        let (base, handle) = spawn_server(app).await;

        let backend = RegistryFsV2::with_static_credentials(
            RegistryFsV2Options {
                timeout: Duration::from_secs(3),
                retry_count: 3,
                user_agent: "registry-test".to_string(),
                credential: CredentialConfig::default(),
                accelerate_address: String::new(),
                cert_file: String::new(),
                key_file: String::new(),
            },
            "user",
            "password",
        )
        .expect("backend");
        let blob_url = format!("{base}/v2/blob");
        let file = backend.open_file(blob_url).await.expect("open remote file");
        let got = file.read_at(0, 5).await.expect("read data");

        handle.abort();

        assert_eq!(got.as_ref(), b"token");
        assert!(state.token_hits.load(Ordering::Relaxed) >= 2);
    }

    #[derive(Clone, Debug)]
    struct UploadState {
        patch_bytes: Arc<AsyncMutex<Vec<u8>>>,
        complete_digest: Arc<AsyncMutex<Option<String>>>,
        token_hits: Arc<AtomicUsize>,
    }

    async fn handle_upload_token(State(state): State<UploadState>) -> Json<Value> {
        state.token_hits.fetch_add(1, Ordering::Relaxed);
        Json(json!({"token": "upload-token"}))
    }

    async fn handle_upload_init(headers: HttpHeaderMap) -> Response<Body> {
        let host = headers
            .get("host")
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();
        let challenge = bearer_challenge(host, "repository:ns/repo:push,pull");
        let auth = headers
            .get(AUTHORIZATION_RAW)
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();

        if auth != "Bearer upload-token" {
            return Response::builder()
                .status(HttpStatusCode::UNAUTHORIZED)
                .header(WWW_AUTHENTICATE, challenge)
                .body(Body::empty())
                .expect("401 response");
        }
        let location = format!("http://{host}/upload/session");
        Response::builder()
            .status(HttpStatusCode::ACCEPTED)
            .header(LOCATION, location)
            .body(Body::empty())
            .expect("202 response")
    }

    async fn handle_upload_patch(
        State(state): State<UploadState>,
        headers: HttpHeaderMap,
        body: axum::body::Bytes,
    ) -> Response<Body> {
        let auth = headers
            .get(AUTHORIZATION_RAW)
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();
        if auth != "Bearer upload-token" {
            return Response::builder()
                .status(HttpStatusCode::UNAUTHORIZED)
                .body(Body::empty())
                .expect("401 response");
        }
        let content_range = headers
            .get(CONTENT_RANGE_RAW)
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let (_, end) = content_range
            .split_once('-')
            .and_then(|(s, e)| Some((s.parse::<u64>().ok()?, e.parse::<u64>().ok()?)))
            .expect("content-range format");

        {
            let mut guard = state.patch_bytes.lock().await;
            guard.extend_from_slice(&body);
        }
        let host = headers
            .get("host")
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();
        let location = format!("http://{host}/upload/session");
        Response::builder()
            .status(HttpStatusCode::ACCEPTED)
            .header(RANGE, format!("0-{end}"))
            .header(LOCATION, location)
            .body(Body::empty())
            .expect("202 patch response")
    }

    async fn handle_upload_complete(
        State(state): State<UploadState>,
        Query(query): Query<HashMap<String, String>>,
        headers: HttpHeaderMap,
    ) -> Response<Body> {
        let auth = headers
            .get(AUTHORIZATION_RAW)
            .and_then(|x| x.to_str().ok())
            .unwrap_or_default();
        if auth != "Bearer upload-token" {
            return Response::builder()
                .status(HttpStatusCode::UNAUTHORIZED)
                .body(Body::empty())
                .expect("401 response");
        }
        let digest = query.get("digest").cloned().unwrap_or_default();
        {
            let mut guard = state.complete_digest.lock().await;
            *guard = Some(digest.clone());
        }
        Response::builder()
            .status(HttpStatusCode::CREATED)
            .header("Docker-Content-Digest", digest)
            .body(Body::empty())
            .expect("201 put response")
    }

    #[tokio::test]
    async fn test_registry_uploader_end_to_end() {
        let upload_state = UploadState {
            patch_bytes: Arc::new(AsyncMutex::new(Vec::new())),
            complete_digest: Arc::new(AsyncMutex::new(None)),
            token_hits: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/token", get(handle_upload_token))
            .route("/v2/upload", post(handle_upload_init))
            .route("/upload/session", patch(handle_upload_patch))
            .route("/upload/session", put(handle_upload_complete))
            .with_state(upload_state.clone());
        let (base, handle) = spawn_server(app).await;

        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("uploader.data");
        let local = Arc::new(
            LocalFile::open_rw(&path, true, test_io_ring())
                .await
                .expect("open local upload staging file"),
        );
        let mut uploader = RegistryUploader::new(
            local,
            RegistryUploaderOptions {
                upload_url: format!("{base}/v2/upload"),
                username: "user".to_string(),
                password: "pass".to_string(),
                timeout: Duration::from_secs(3),
                upload_chunk_size: 4,
                cert_file: String::new(),
                key_file: String::new(),
                user_agent: "registry-test".to_string(),
            },
        )
        .await
        .expect("new uploader");

        uploader.write(b"hello ").await.expect("write 1");
        uploader.write(b"world").await.expect("write 2");
        let digest = uploader.finish().await.expect("finish upload");
        let expected = digest_of(b"hello world");

        handle.abort();

        let uploaded = upload_state.patch_bytes.lock().await.clone();
        let complete = upload_state.complete_digest.lock().await.clone();

        assert_eq!(uploaded.as_slice(), b"hello world");
        assert_eq!(digest, expected);
        assert_eq!(complete, Some(expected));
        assert!(upload_state.token_hits.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_parse_auth_challenge_bearer() {
        let line = r#"Bearer realm="https://auth.example/token",service="registry.example",scope="repository:ns/repo:pull""#;
        let challenge = parse_auth_challenge(line).expect("parse");
        match challenge {
            AuthChallenge::Bearer { auth_url } => {
                assert_eq!(
                    auth_url.as_str(),
                    "https://auth.example/token?service=registry.example&scope=repository%3Ans%2Frepo%3Apull"
                );
            }
            _ => panic!("unexpected auth type"),
        }
    }

    #[test]
    fn test_parse_auth_challenge_basic() {
        let line = r#"Basic realm="registry.example""#;
        let challenge = parse_auth_challenge(line).expect("parse");
        assert!(matches!(challenge, AuthChallenge::Basic));
    }

    #[test]
    fn test_blob_auth_context_for_nested_repo_with_port_and_query() {
        let ctx = blob_auth_context(
            "https://registry.example.com:5000/v2/ns/team/repo/blobs/sha256:abc?download=1",
        )
        .expect("blob auth context");

        assert_eq!(ctx.origin, "https://registry.example.com:5000");
        assert_eq!(
            ctx.ping_url.as_str(),
            "https://registry.example.com:5000/v2/"
        );
        assert_eq!(ctx.scope, "repository:ns/team/repo:pull");
    }

    #[test]
    fn test_blob_auth_context_for_single_segment_repo() {
        let ctx = blob_auth_context("https://registry.example.com/v2/ubuntu/blobs/sha256:abc")
            .expect("blob auth context");

        assert_eq!(ctx.origin, "https://registry.example.com");
        assert_eq!(ctx.ping_url.as_str(), "https://registry.example.com/v2/");
        assert_eq!(ctx.scope, "repository:ubuntu:pull");
    }

    #[test]
    fn test_blob_auth_context_rejects_non_blob_url() {
        assert!(
            blob_auth_context("https://registry.example.com/v2/ns/repo/manifests/latest").is_none()
        );
        assert!(blob_auth_context("s3://bucket/v2/ns/repo/blobs/sha256:abc").is_none());
    }

    #[test]
    fn test_parse_auths_select_prefix_entry() {
        let auths = json!({
            "registry.example.com/ns/repo": {
                "username": "u1",
                "password": "p1"
            },
            "registry.example.com": {
                "username": "u2",
                "password": "p2"
            }
        });

        let (u, p) = parse_auths(
            &auths,
            "https://registry.example.com/v2/ns/repo/blobs/sha256:abc",
        );
        assert_eq!(u, "u1");
        assert_eq!(p, "p1");
    }

    #[test]
    fn test_parse_auths_use_first_match_in_order() {
        let auths = json!({
            "registry.example.com": {
                "username": "u-first",
                "password": "p-first"
            },
            "registry.example.com/ns/repo": {
                "username": "u-second",
                "password": "p-second"
            }
        });

        let (u, p) = parse_auths(
            &auths,
            "https://registry.example.com/v2/ns/repo/blobs/sha256:abc",
        );
        assert_eq!(u, "u-first");
        assert_eq!(p, "p-first");
    }

    #[test]
    fn test_parse_auths_base64() {
        let encoded = BASE64_STANDARD.encode("alice:secret");
        let auths = json!({
            "registry.example.com/ns/repo": {
                "auth": encoded
            }
        });

        let (u, p) = parse_auths(
            &auths,
            "https://registry.example.com/v2/ns/repo/blobs/sha256:abc",
        );
        assert_eq!(u, "alice");
        assert_eq!(p, "secret");
    }

    #[tokio::test]
    async fn docker_credential_helper_receives_the_registry_and_returns_credentials() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("temp dir");
        let helper = temp.path().join("docker-credential-fixture");
        fs::write(
            &helper,
            "#!/bin/sh\n[ \"$1\" = get ] || exit 20\nIFS= read -r registry\n[ \"$registry\" = 123456789012.dkr.ecr.us-east-1.amazonaws.com ] || exit 21\nprintf '%s\\n' '{\"Username\":\"AWS\",\"Secret\":\"short-lived-token\"}'\n",
        )
        .expect("write helper");
        let mut permissions = fs::metadata(&helper)
            .expect("helper metadata")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&helper, permissions).expect("make helper executable");

        let (username, password) = run_credential_helper_command(
            helper.to_str().expect("UTF-8 helper path"),
            "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            Duration::from_secs(2),
        )
        .await
        .expect("credential helper result");

        assert_eq!(username, "AWS");
        assert_eq!(password, "short-lived-token");
    }

    #[tokio::test]
    async fn static_docker_auth_wins_over_an_unavailable_helper() {
        let config: AuthConfig = serde_json::from_value(json!({
            "auths": {
                "registry.example.com": {
                    "username": "static-user",
                    "password": "static-password"
                }
            },
            "credHelpers": {
                "registry.example.com": "this-helper-must-not-run"
            }
        }))
        .expect("Docker config");

        let credentials = resolve_docker_config_credentials(
            &config,
            "https://registry.example.com/v2/team/image/blobs/sha256:abc",
            Duration::from_millis(10),
        )
        .await
        .expect("static credentials");

        assert_eq!(
            credentials,
            ("static-user".to_string(), "static-password".to_string())
        );
    }

    #[test]
    fn docker_credential_helper_names_cannot_escape_to_a_path() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let error = runtime
            .block_on(run_credential_helper(
                "../../malicious",
                "registry.example.com",
                Duration::from_secs(1),
            ))
            .expect_err("path-like helper must be rejected");

        assert!(error
            .to_string()
            .contains("invalid Docker credential helper"));
    }

    #[test]
    fn registry_server_preserves_an_explicit_port() {
        assert_eq!(
            registry_server("https://registry.example.com:5443/v2/team/image/blobs/sha256:abc")
                .expect("registry server"),
            "registry.example.com:5443"
        );
    }

    #[test]
    fn test_parse_content_range_total() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            CONTENT_RANGE,
            "bytes 0-0/12345".parse().expect("header value"),
        );
        assert_eq!(parse_content_range_total(&headers), Some(12345));
    }
}
