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
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, RwLock};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RETRY_COUNT: u32 = 3;

/// Read granularity for the upload source.
///
/// Matches tokio's `DEFAULT_MAX_BUF_SIZE` (`tokio/src/io/blocking.rs:27`), which
/// caps what one `tokio::fs::File::read` returns regardless of the buffer handed
/// to it. A larger buffer here would sit resident with 97% of it never written to.
const UPLOAD_READ_SIZE: usize = 2 * 1024 * 1024;

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
            self.inner.addressing_override,
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

    /// Upload a local file as one object, streamed in `part_size` parts with
    /// `concurrency` of them in flight. Both figures carry consequences — peak
    /// memory and the largest uploadable object — documented on
    /// [`upload_file_streaming`], which is where to look before picking them.
    ///
    /// Streaming rather than reading the file in matters because the callers
    /// upload sealed LSMT layers, whose size is the guest's written data — GiB,
    /// not MiB. Note that the file is opened *inside* the retried closure: a
    /// credential refresh restarts the upload from byte zero rather than
    /// resuming a partly written multipart, which is the only outcome the object
    /// store guarantees is coherent.
    pub async fn upload_path(
        &self,
        url: impl AsRef<str>,
        path: impl AsRef<Path>,
        part_size: usize,
        concurrency: usize,
    ) -> Result<()> {
        let location = ParsedOssUrl::parse(
            url.as_ref(),
            &self.inner.default_endpoint,
            &self.inner.default_region,
            self.inner.addressing_override,
        )?;
        let key = location.key.clone();
        let path = path.as_ref().to_path_buf();

        self.inner
            .run_with_operator(&location, |operator| {
                let key = key.clone();
                let path = path.clone();
                async move {
                    upload_file_streaming(&operator, &key, &path, part_size, concurrency).await
                }
            })
            .await
            .with_context(|| {
                format!(
                    "upload oss object '{}' from {}",
                    url.as_ref(),
                    path.display()
                )
            })
    }

    /// Content length of an object, or `None` when it does not exist.
    ///
    /// Exists so a caller can skip re-uploading a content-addressed object, and
    /// so that deciding "absent" does not require the caller to name
    /// `opendal::Error`. Doing so would make the caller's `downcast_ref` depend on
    /// resolving to the same `opendal` version this crate does — a mismatch
    /// compiles, then silently never matches.
    ///
    /// `NotFound` is folded into `None` inside the closure, where `error.kind()`
    /// is still available directly. That keeps this independent of how faithfully
    /// [`map_object_store_operator_error`] preserves the kind afterwards.
    pub async fn object_size(&self, url: impl AsRef<str>) -> Result<Option<u64>> {
        let location = ParsedOssUrl::parse(
            url.as_ref(),
            &self.inner.default_endpoint,
            &self.inner.default_region,
            self.inner.addressing_override,
        )?;
        let key = location.key.clone();

        self.inner
            .run_with_operator(&location, |operator| {
                let key = key.clone();
                async move {
                    match operator.stat(&key).await {
                        Ok(metadata) => Ok(Some(metadata.content_length())),
                        Err(error) if error.kind() == OpenDalErrorKind::NotFound => Ok(None),
                        Err(error) => Err(error),
                    }
                }
            })
            .await
            .with_context(|| format!("stat oss object '{}'", url.as_ref()))
    }

    pub async fn upload_bytes(&self, url: impl AsRef<str>, body: Vec<u8>) -> Result<()> {
        let location = ParsedOssUrl::parse(
            url.as_ref(),
            &self.inner.default_endpoint,
            &self.inner.default_region,
            self.inner.addressing_override,
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
            addressing_style: self.addressing_style,
        }
    }

    fn operator_config(&self, timeout: Duration, retry_count: u32) -> ObjectStoreOperatorConfig {
        ObjectStoreOperatorConfig {
            bucket: self.bucket.clone(),
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            addressing_style: self.addressing_style,
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

/// Stream `path` into `key`, in parts of `part_size` with `concurrency` of them
/// in flight.
///
/// Shared by every caller that uploads a file to object storage, including
/// AgentENV's snapshot repository, which reaches it through its own `Operator`
/// rather than through [`OssBackend`]. **Deliberately no defaults for the two
/// figures**: each is a real trade with consequences that differ per caller — a
/// snapshot service that must handle huge images and a tool sharing a host with
/// running VMs should not land on the same numbers by inheriting them. Pick them
/// where the constraints are known and record the reasoning there.
///
/// # Part size
///
/// `part_size` is a request, not a guarantee: OpenDAL clamps it into the service's
/// `[write_multi_min_size, write_multi_max_size]`
/// (`opendal/src/types/context/write.rs:79-92`), which for S3 is
/// **[5 MiB, 5 GiB]** (`opendal/src/services/s3/backend.rs:927-938`, citing AWS's
/// rule that every part but the last is at least 5 MiB). Asking for less than the
/// floor is silently raised, so a caller's memory estimate would come out low
/// rather than the upload failing. That bound belongs to the service, which is why
/// it is not re-checked here.
///
/// **It also bounds the largest object that can be uploaded**, because S3 caps a
/// multipart upload at 10,000 parts:
///
/// | part size | largest object | peak memory at concurrency 4 |
/// |---|---|---|
/// | 5 MiB (the service floor) | 50 GiB | ~50 MiB |
/// | 16 MiB | 156 GiB | ~132 MiB |
/// | 32 MiB | 312 GiB | ~260 MiB |
/// | 64 MiB | 625 GiB | ~1040 MiB |
///
/// **Nothing checks that ceiling.** OpenDAL mentions it only in a comment next to
/// the code that shifts the part index from 0-based to 1-based
/// (`opendal/src/services/s3/writer.rs:115`), and its capability model has no field
/// for a part count at all. Part 10,001 is sent and the *endpoint* rejects it, so
/// exceeding it surfaces partway through an upload as a server-side
/// `InvalidArgument` rather than up front. A caller uploading something that could
/// exceed its ceiling has to raise the part size itself.
///
/// Short reads are forwarded as-is rather than being gathered into full parts:
/// the writer buffers up to the part size itself, so the part boundaries the
/// endpoint sees do not follow the read boundaries here.
///
/// # Memory
///
/// **Peak memory is roughly `(2 * concurrency + 2) * part_size`, not the product
/// of the two.** Measured against a live endpoint with a 1 GiB object: 1040 MiB at
/// 64 MiB × 8 and 132 MiB at 16 MiB × 4, against bounds of 1152 and 160. Where the
/// parts are held, top to bottom through OpenDAL 0.55:
///
/// - `WriteGenerator` accumulates writes into a `QueueBuf` until they reach the
///   part size, then hands them on via `take().collect()`. That collect is *not*
///   a copy: `FromIterator<Bytes> for Buffer` allocates only an `Arc<[Bytes]>`
///   index over the pieces already there, so the accumulating queue and the
///   collected buffer are the same memory, counted once.
/// - `MultipartWriter` holds one part in `cache`, dispatching it only when the
///   next one arrives — it cannot know a part is the last until another shows up,
///   and a lone part is uploaded with `write_once` (a single PutObject) instead of
///   paying three round trips for a multipart.
/// - `ConcurrentTasks` admits a new task while
///   `tasks.len() < concurrent + min(completed_but_unretrieved, prefetch)`, and
///   `MultipartWriter` hardcodes `prefetch` to 8192 while never draining results
///   before `close()`. A finished task keeps its input buffer alive for retry, so
///   every completed part raises that ceiling by one. It settles near twice
///   `concurrent` rather than growing with the file, but it is twice, not once.
pub async fn upload_file_streaming(
    operator: &Operator,
    key: &str,
    path: &Path,
    part_size: usize,
    concurrency: usize,
) -> OpenDalResult<()> {
    let mut writer = operator
        .writer_with(key)
        .chunk(part_size)
        .concurrent(concurrency)
        .await?;
    let mut file = tokio::fs::File::open(path).await.map_err(|err| {
        OpenDalError::new(OpenDalErrorKind::Unexpected, "open oss upload source file")
            .set_source(err)
    })?;

    let mut buf = vec![0u8; UPLOAD_READ_SIZE];
    loop {
        let read = file.read(&mut buf).await.map_err(|err| {
            OpenDalError::new(OpenDalErrorKind::Unexpected, "read oss upload source file")
                .set_source(err)
        })?;
        if read == 0 {
            break;
        }
        writer.write(buf[..read].to_vec()).await?;
    }

    // Only `close` completes the multipart upload. Dropping the writer abandons
    // it, so the object either appears whole or not at all.
    writer.close().await?;
    Ok(())
}

fn map_object_store_operator_error(error: ObjectStoreOperatorError) -> anyhow::Error {
    match error {
        ObjectStoreOperatorError::OpenDal(error) => match error.kind() {
            // Keep the `OpenDalError` on the chain rather than replacing it with a
            // fresh message. `NotFound` is the one kind a caller routinely needs to
            // act on — "absent" and "cannot tell" call for opposite decisions — and
            // discarding it left only the message text to match on. The top-level
            // `to_string()` is unchanged, so anything doing that still works.
            OpenDalErrorKind::NotFound => anyhow::Error::from(error).context("Object not found"),
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
}
