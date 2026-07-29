use crate::config::OssConfig;
use crate::io::virtual_file::VirtualFile;
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store_operator::{
    build_object_store_operator, credential_source_from_fields, run_with_refresh, AddressingStyle,
    CachedCredentialSource, CredentialFields, CredentialSource, CredentialSourceOptions,
    ObjectStoreOperatorConfig, ObjectStoreOperatorError, OpenDalError, OpenDalErrorKind,
    OpenDalResult, Operator, OperatorWithCredential, ResolvedCredential,
};
use reqwest::Url;
use std::cmp::min;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RETRY_COUNT: u32 = 3;

#[derive(Clone, Debug)]
pub struct OssBackend {
    inner: Arc<OssBackendInner>,
}

#[derive(Debug)]
struct OssBackendInner {
    credentials: CachedCredentialSource,
    default_region: String,
    default_endpoint: String,
    addressing_override: Option<AddressingStyle>,
    timeout: Duration,
    retry_count: u32,
    cached_operators: RwLock<HashMap<OperatorCacheKey, OperatorWithCredential>>,
}

#[derive(Debug, Clone)]
struct ParsedOssUrl {
    bucket: String,
    key: String,
    region: String,
    endpoint: String,
    addressing_style: AddressingStyle,
}

#[derive(Debug)]
pub struct OssFile {
    backend: Arc<OssBackendInner>,
    location: ParsedOssUrl,
    size_cache: Mutex<Option<u64>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct OperatorCacheKey {
    bucket: String,
    region: String,
    endpoint: String,
    addressing_style: AddressingStyle,
}

impl OssBackend {
    pub fn new(config: &OssConfig) -> Result<Self> {
        let credential_source = credential_source_from_config(config)?;
        let addressing_override = parse_addressing_style(&config.default_addressing_style)?;
        let timeout = if config.timeout_secs == 0 {
            DEFAULT_TIMEOUT
        } else {
            Duration::from_secs(config.timeout_secs)
        };
        let retry_count = if config.retry_count == 0 {
            DEFAULT_RETRY_COUNT
        } else {
            config.retry_count
        };

        Ok(Self {
            inner: Arc::new(OssBackendInner {
                credentials: CachedCredentialSource::new(credential_source),
                default_region: config.default_region.clone(),
                default_endpoint: config.default_endpoint.clone(),
                addressing_override,
                timeout,
                retry_count,
                cached_operators: RwLock::new(HashMap::new()),
            }),
        })
    }

    pub fn open_file_with_size_hint(
        &self,
        url: impl AsRef<str>,
        size_hint: Option<u64>,
    ) -> Result<Arc<OssFile>> {
        let location = ParsedOssUrl::parse(
            url.as_ref(),
            &self.inner.default_endpoint,
            &self.inner.default_region,
            self.inner.addressing_override.clone(),
        )?;

        Ok(Arc::new(OssFile {
            backend: Arc::clone(&self.inner),
            location,
            size_cache: Mutex::new(size_hint),
        }))
    }

    pub fn open_with_size_hint(
        &self,
        url: impl AsRef<str>,
        size_hint: Option<u64>,
    ) -> Result<Arc<dyn VirtualFile>> {
        let file: Arc<dyn VirtualFile> = self.open_file_with_size_hint(url, size_hint)?;
        Ok(file)
    }

    pub async fn upload_path(&self, url: impl AsRef<str>, path: impl AsRef<Path>) -> Result<()> {
        let body = tokio::fs::read(path.as_ref())
            .await
            .with_context(|| format!("read staged oss upload file {}", path.as_ref().display()))?;
        self.upload_bytes(url, body).await
    }

    pub async fn upload_bytes(&self, url: impl AsRef<str>, body: Vec<u8>) -> Result<()> {
        let location = ParsedOssUrl::parse(
            url.as_ref(),
            &self.inner.default_endpoint,
            &self.inner.default_region,
            self.inner.addressing_override.clone(),
        )?;
        let key = location.key.clone();

        self.inner
            .run_with_operator(&location, |operator| {
                let key = key.clone();
                let body = body.clone();
                async move {
                    operator.write(&key, body).await?;
                    Ok(())
                }
            })
            .await
            .with_context(|| format!("upload oss object '{}'", url.as_ref()))
    }
}

impl ParsedOssUrl {
    fn parse(
        raw: &str,
        default_endpoint: &str,
        default_region: &str,
        addressing_override: Option<AddressingStyle>,
    ) -> Result<Self> {
        let url = Url::parse(raw).context(format!("invalid oss url {raw}"))?;
        ensure!(
            matches!(url.scheme(), "s3" | "oss"),
            "unsupported oss url scheme {}",
            url.scheme()
        );

        let bucket = url
            .host_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("oss url bucket is missing"))?
            .to_string();
        let key = url.path().trim_start_matches('/').to_string();
        ensure!(!key.is_empty(), "oss url object key is missing");

        let mut endpoint = None;
        let mut region = None;
        for (name, value) in url.query_pairs() {
            match name.as_ref() {
                "endpoint" => endpoint = Some(value.into_owned()),
                "region" => region = Some(value.into_owned()),
                _ => {}
            }
        }

        let endpoint = endpoint
            .or_else(|| {
                if default_endpoint.is_empty() {
                    None
                } else {
                    Some(default_endpoint.to_string())
                }
            })
            .ok_or_else(|| anyhow::anyhow!("oss endpoint is missing"))?;
        let region = region
            .or_else(|| {
                if default_region.is_empty() {
                    None
                } else {
                    Some(default_region.to_string())
                }
            })
            .ok_or_else(|| anyhow::anyhow!("oss region is missing"))?;
        // Detection also validates the endpoint URL, so it always runs; an
        // explicit override from the config then wins over the detected style.
        let detected_style = detect_addressing_style(&endpoint, &bucket)?;
        let addressing_style = addressing_override.unwrap_or(detected_style);

        Ok(Self {
            bucket,
            key,
            region,
            endpoint,
            addressing_style,
        })
    }

    fn cache_key(&self) -> OperatorCacheKey {
        OperatorCacheKey {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            addressing_style: self.addressing_style.clone(),
        }
    }

    fn operator_config(&self, timeout: Duration, retry_count: u32) -> ObjectStoreOperatorConfig {
        ObjectStoreOperatorConfig {
            bucket: self.bucket.clone(),
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            addressing_style: self.addressing_style.clone(),
            timeout: Some(timeout),
            max_retries: Some(retry_count as usize),
        }
    }
}

impl OssBackendInner {
    async fn run_with_operator<T, F, Fut>(&self, location: &ParsedOssUrl, operation: F) -> Result<T>
    where
        F: Fn(Operator) -> Fut,
        Fut: std::future::Future<Output = OpenDalResult<T>>,
    {
        let current = self.ensure_fresh_operator(location).await?;
        let config = location.operator_config(self.timeout, self.retry_count);
        let credentials = current.credential().is_some().then_some(&self.credentials);
        let (value, refreshed) = run_with_refresh(&current, credentials, &config, operation)
            .await
            .map_err(map_object_store_operator_error)?;
        if let Some(refreshed) = refreshed {
            self.cached_operators
                .write()
                .await
                .insert(location.cache_key(), refreshed);
        }
        Ok(value)
    }

    async fn ensure_fresh_operator(
        &self,
        location: &ParsedOssUrl,
    ) -> Result<OperatorWithCredential> {
        let credential = self.credentials.current().await?;
        let cache_key = location.cache_key();

        {
            let cached = self.cached_operators.read().await;
            if let Some(entry) = cached.get(&cache_key) {
                if entry.credential() == credential.as_ref() {
                    return Ok(entry.clone());
                }
            }
        }

        self.rebuild_operator(location, credential).await
    }

    async fn rebuild_operator(
        &self,
        location: &ParsedOssUrl,
        credential: Option<ResolvedCredential>,
    ) -> Result<OperatorWithCredential> {
        let entry = OperatorWithCredential::new(
            build_object_store_operator(
                &location.operator_config(self.timeout, self.retry_count),
                credential.as_ref(),
            )
            .map_err(map_object_store_operator_error)?,
            credential,
        );
        self.cached_operators
            .write()
            .await
            .insert(location.cache_key(), entry.clone());
        Ok(entry)
    }
}

#[async_trait]
impl VirtualFile for OssFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }

        let size = self.size().await?;
        if offset >= size {
            return Ok(Bytes::new());
        }

        let end = min(offset.saturating_add(len as u64), size);
        let key = self.location.key.clone();
        let start = Instant::now();
        let result = self
            .backend
            .run_with_operator(&self.location, |operator| {
                let key = key.clone();
                async move { operator.read_with(&key).range(offset..end).await }
            })
            .await
            .with_context(|| {
                format!(
                    "read range {offset}..{end} from oss object '{}'",
                    self.location.key
                )
            })
            .map(|data| data.to_bytes());
        crate::metrics::record_remote_read(
            &crate::metrics::RemoteSource::OssObject,
            crate::metrics::RemoteReadOperation::ReadRange,
            &result,
            |data| data.len() as u64,
            start.elapsed(),
        );

        result
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }

        let size = self.size().await?;
        if offset >= size {
            return Ok(0);
        }

        let end = min(offset.saturating_add(dst.len() as u64), size);
        let key = self.location.key.clone();
        let start = Instant::now();
        // Stream the response body straight into the caller's buffer. Unlike
        // read_at (whole-range Buffer + one memcpy), this holds at most a few
        // chunks of network data per in-flight read, halving peak memory and
        // removing the copy. The bounded channel forwards chunks from the
        // operator side (which may refresh credentials) to the plain write
        // loop here, so no &mut escapes any closure.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(4);
        let read_fut = self.backend.run_with_operator(&self.location, |operator| {
            let key = key.clone();
            let tx = tx.clone();
            async move {
                let reader = operator.reader(&key).await?;
                let mut stream = reader.into_bytes_stream(offset..end).await?;
                while let Some(chunk) = stream.try_next().await.map_err(|err| {
                    OpenDalError::new(
                        OpenDalErrorKind::Unexpected,
                        "stream read from oss object failed",
                    )
                    .set_source(err)
                })? {
                    if tx.send(chunk).await.is_err() {
                        break;
                    }
                }
                Ok(())
            }
        });
        let write_fut = async {
            let mut written = 0usize;
            while let Some(chunk) = rx.recv().await {
                let n = chunk.len().min(dst.len() - written);
                dst[written..written + n].copy_from_slice(&chunk[..n]);
                written += n;
                if n < chunk.len() || written == dst.len() {
                    break;
                }
            }
            Ok::<usize, anyhow::Error>(written)
        };
        let (read_result, write_result) = tokio::join!(read_fut, write_fut);
        let result = read_result.and(write_result).with_context(|| {
            format!(
                "stream range {offset}..{end} from oss object '{}'",
                self.location.key
            )
        });
        crate::metrics::record_remote_read(
            &crate::metrics::RemoteSource::OssObject,
            crate::metrics::RemoteReadOperation::ReadRangeInto,
            &result,
            |written| *written as u64,
            start.elapsed(),
        );

        result
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("oss file backend is read-only; use OssBackend::upload_bytes for writes")
    }

    async fn size(&self) -> Result<u64> {
        if let Some(size) = *self.size_cache.lock().await {
            return Ok(size);
        }

        let key = self.location.key.clone();
        let start = Instant::now();
        let result = self
            .backend
            .run_with_operator(&self.location, |operator| {
                let key = key.clone();
                async move { operator.stat(&key).await }
            })
            .await
            .with_context(|| format!("stat oss object '{}'", self.location.key));
        crate::metrics::record_remote_metadata(
            &crate::metrics::RemoteSource::OssObject,
            &result,
            start.elapsed(),
        );
        let metadata = result?;
        let size = metadata.content_length();
        *self.size_cache.lock().await = Some(size);
        Ok(size)
    }
}

fn map_object_store_operator_error(error: ObjectStoreOperatorError) -> anyhow::Error {
    match error {
        ObjectStoreOperatorError::OpenDal(error) => match error.kind() {
            OpenDalErrorKind::NotFound => anyhow::anyhow!("Object not found"),
            OpenDalErrorKind::PermissionDenied => anyhow::anyhow!("Access denied").context(error),
            _ => error.into(),
        },
        ObjectStoreOperatorError::CredentialRefresh(error) => {
            error.context("credential refresh failed")
        }
        ObjectStoreOperatorError::OperatorBuild(error) => error.context("operator build failed"),
    }
}

fn detect_addressing_style(endpoint: &str, bucket: &str) -> Result<AddressingStyle> {
    let url = Url::parse(endpoint).context("parse oss endpoint for addressing style")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("oss endpoint host is missing"))?;
    let bucket_host = format!("{bucket}.");
    let is_bucket_virtual_host = host.starts_with(&bucket_host);
    let is_aliyun_endpoint = host.ends_with(".aliyuncs.com") || host.ends_with(".aliyun-inc.com");

    if is_bucket_virtual_host {
        return Ok(AddressingStyle::Virtual);
    }
    if is_aliyun_endpoint {
        return Ok(AddressingStyle::Virtual);
    }
    Ok(AddressingStyle::Path)
}

/// Parse the config-level addressing style: `"virtual"`, `"path"`, or empty
/// for `None` (auto-detect per URL).
fn parse_addressing_style(value: &str) -> Result<Option<AddressingStyle>> {
    match value.trim() {
        "" => Ok(None),
        "virtual" => Ok(Some(AddressingStyle::Virtual)),
        "path" => Ok(Some(AddressingStyle::Path)),
        other => bail!(
            "invalid oss defaultAddressingStyle '{other}': expected 'virtual', 'path', or empty for auto-detection"
        ),
    }
}

fn credential_source_from_config(config: &OssConfig) -> Result<CredentialSource> {
    credential_source_from_fields(
        CredentialFields {
            access_key_id: Some(config.access_key_id.as_str()),
            secret_access_key: Some(config.secret_access_key.as_str()),
            security_token: Some(config.security_token.as_str()),
            credential_process: Some(config.credential_process.as_str()),
        },
        CredentialSourceOptions {
            scope: "oss",
            allow_anonymous: true,
            required_access_key_id_label: "oss access_key_id",
            required_secret_access_key_label: "oss secret_access_key",
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_addressing_style_promotes_virtual_for_aliyun_endpoint() {
        let style = detect_addressing_style("https://oss-cn-hangzhou.aliyuncs.com", "demo-bucket")
            .expect("detect style");

        assert_eq!(style, AddressingStyle::Virtual);
    }

    #[test]
    fn test_detect_addressing_style_keeps_path_for_generic_endpoint() {
        let style =
            detect_addressing_style("http://127.0.0.1:9000", "demo-bucket").expect("detect style");

        assert_eq!(style, AddressingStyle::Path);
    }

    #[test]
    fn test_detect_addressing_style_uses_cname_when_bucket_host_is_present() {
        let style = detect_addressing_style(
            "https://demo-bucket.s3.us-east-1.amazonaws.com",
            "demo-bucket",
        )
        .expect("detect style");

        assert_eq!(style, AddressingStyle::Virtual);
    }

    #[test]
    fn test_credential_source_from_config_rejects_mixed_sources() {
        let config = OssConfig {
            enable: true,
            access_key_id: "ak".to_string(),
            secret_access_key: "sk".to_string(),
            security_token: String::new(),
            credential_process: "echo '{}'".to_string(),
            default_region: "us-east-1".to_string(),
            default_endpoint: "https://s3.us-east-1.amazonaws.com".to_string(),
            default_addressing_style: String::new(),
            timeout_secs: 30,
            retry_count: 3,
        };

        let err = credential_source_from_config(&config).expect_err("mixed source should fail");
        assert!(err.to_string().contains("credential_process"));
    }

    #[test]
    fn test_parse_addressing_style_values() {
        assert_eq!(parse_addressing_style("").expect("empty"), None);
        assert_eq!(parse_addressing_style("  ").expect("blank"), None);
        assert_eq!(
            parse_addressing_style("virtual").expect("virtual"),
            Some(AddressingStyle::Virtual)
        );
        assert_eq!(
            parse_addressing_style("path").expect("path"),
            Some(AddressingStyle::Path)
        );
        parse_addressing_style("bogus").expect_err("invalid style must be rejected");
    }

    #[test]
    fn test_parsed_url_applies_addressing_override() {
        // A generic endpoint auto-detects to path style...
        let detected = ParsedOssUrl::parse(
            "s3://demo-bucket/layers/obj",
            "https://t3.storage.dev",
            "auto",
            None,
        )
        .expect("parse without override");
        assert_eq!(detected.addressing_style, AddressingStyle::Path);

        // ...and the config-level override switches remote layer reads to
        // virtual-host style for providers that require it.
        let overridden = ParsedOssUrl::parse(
            "s3://demo-bucket/layers/obj",
            "https://t3.storage.dev",
            "auto",
            Some(AddressingStyle::Virtual),
        )
        .expect("parse with override");
        assert_eq!(overridden.addressing_style, AddressingStyle::Virtual);
        assert_eq!(
            overridden
                .operator_config(Duration::from_secs(30), 3)
                .addressing_style,
            AddressingStyle::Virtual
        );
    }

    #[test]
    fn test_parsed_url_override_still_validates_endpoint() {
        ParsedOssUrl::parse(
            "s3://demo-bucket/layers/obj",
            "not a valid endpoint",
            "auto",
            Some(AddressingStyle::Virtual),
        )
        .expect_err("malformed endpoint must fail even with an explicit override");
    }
}
