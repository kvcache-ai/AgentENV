use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{stream, TryStreamExt};
use object_store_operator::{
    build_object_store_operator, credential_source_from_fields, detect_addressing_style,
    normalized_credential, run_with_refresh, AddressingStyle, CachedCredentialSource,
    CredentialFields, CredentialSource, CredentialSourceOptions, ObjectStoreOperatorConfig,
    ObjectStoreOperatorError, ObjectStoreService, OperatorWithCredential,
};
use opendal::{Error as OpenDalError, ErrorKind as OpenDalErrorKind, Operator};
use overlaybd::backend::oss::upload_file_streaming;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::info;

use crate::cfg::{OssAddressingStyle, OssBackendConfig};
use crate::prometheus::MetricGuard;

/// Multipart part size for streaming file uploads. S3/OSS caps a multipart
/// upload at 10,000 parts, so this bounds the largest uploadable object
/// (~625 GiB at 64 MiB). Must be passed explicitly to opendal via
/// `writer_with().chunk()`: without it opendal falls back to the service's
/// minimum multipart part size (5 MiB), capping uploads at ~50 GiB.
///
/// Upload memory is bounded by `CHUNK_SIZE` and `UPLOAD_CONCURRENCY`, not by
/// the layer size. See `overlaybd::backend::oss::upload_file_streaming` for why.
/// Deliberately left as it was when the upload loop moved there: shrinking it
/// would also shrink the largest uploadable object.
const CHUNK_SIZE: usize = 64 * 1024 * 1024;
/// Number of multipart parts uploaded concurrently per file. A single
/// sequential stream tops out at roughly 100 MB/s to the OSS internal
/// endpoint; concurrent parts multiply effective throughput.
const UPLOAD_CONCURRENCY: usize = 8;
const OSS_OPERATION_DURATION: &str = "agentenv_snapshot_oss_operation_duration_seconds";
/// Maximum duration of a single non-retried delete attempt (GC sweeps).
const SINGLE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(4);

/// Snapshot artifacts uploaded to OSS. Used as the `artifact` label on upload
/// metrics and in upload completion logs so memory layers can be told apart
/// from rootfs/attached-drive layers.
#[derive(Clone, Copy, Debug)]
pub(crate) enum OssUploadArtifact {
    RootfsLayer,
    AttachedDriveLayer,
    MemoryLayer,
    VmState,
    FirecrackerManifest,
    StartupPack,
    CatalogRecord,
    BuildLog,
}

impl OssUploadArtifact {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RootfsLayer => "rootfs_layer",
            Self::AttachedDriveLayer => "attached_drive_layer",
            Self::MemoryLayer => "memory_layer",
            Self::VmState => "vm_state",
            Self::FirecrackerManifest => "manifest",
            Self::StartupPack => "startup_pack",
            Self::CatalogRecord => "record",
            Self::BuildLog => "build_log",
        }
    }
}

/// An operator configuration and the operator last built from it.
#[derive(Clone, Debug)]
struct CachedOperator {
    config: ObjectStoreOperatorConfig,
    operator: Arc<RwLock<Option<OperatorWithCredential>>>,
}

impl CachedOperator {
    fn new(config: ObjectStoreOperatorConfig) -> Self {
        Self {
            config,
            operator: Arc::new(RwLock::new(None)),
        }
    }
}

/// Thin wrapper around the OSS client used by the repository and resolver.
#[derive(Clone, Debug)]
pub(crate) struct OssClient {
    operator_config: ObjectStoreOperatorConfig,
    prefix: String,
    credentials: Arc<CachedCredentialSource>,
    cached_operator: Arc<RwLock<Option<OperatorWithCredential>>>,
    /// Non-retried operator for GC deletion calls bounded by [`SINGLE_ATTEMPT_TIMEOUT`].
    single_attempt: CachedOperator,
}

impl OssClient {
    pub(crate) fn from_config(config: &OssBackendConfig) -> Result<Self> {
        let bucket = normalized_credential(Some(config.bucket.as_str()))
            .context("backend.oss.bucket cannot be empty")?
            .to_string();
        let endpoint = normalized_credential(Some(config.endpoint.as_str()))
            .context("backend.oss.endpoint cannot be empty")?
            .to_string();
        // Region is only consumed later by overlaybd global-config generation.
        // The runtime backend keeps the validation here so incomplete OSS config
        // still fails fast during backend construction.
        let region = normalized_credential(config.region.as_deref())
            .context("backend.oss.region is required")?
            .to_string();
        let prefix = config
            .prefix
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        let credential_source = credential_source_from_fields(
            CredentialFields {
                access_key_id: config.access_key_id.as_deref(),
                secret_access_key: config.access_key_secret.as_deref(),
                security_token: config.security_token.as_deref(),
                credential_process: config.credential_process.as_deref(),
            },
            CredentialSourceOptions {
                scope: "backend.oss",
                allow_anonymous: false,
                required_access_key_id_label: "backend.oss.access_key_id",
                required_secret_access_key_label: "backend.oss.access_key_secret",
            },
        )?;
        let addressing_style = config.addressing_style.map(|style| match style {
            OssAddressingStyle::Path => AddressingStyle::Path,
            OssAddressingStyle::Virtual => AddressingStyle::Virtual,
        });
        Self::new(
            bucket,
            endpoint,
            region,
            prefix,
            credential_source,
            addressing_style,
        )
    }

    pub(crate) fn new(
        bucket: String,
        endpoint: String,
        region: String,
        prefix: String,
        credential_source: CredentialSource,
        addressing_override: Option<AddressingStyle>,
    ) -> Result<Self> {
        // Detection also validates the endpoint URL, so it always runs; an
        // explicit config override then wins over the detected style.
        let detected_style = detect_addressing_style(&endpoint, &bucket)?;
        let addressing_style = addressing_override.unwrap_or(detected_style);
        let operator_config = ObjectStoreOperatorConfig {
            service: ObjectStoreService::S3,
            addressing_style,
            bucket,
            endpoint,
            region,
            timeout: None,
            max_retries: None,
        };
        let single_attempt = CachedOperator::new(ObjectStoreOperatorConfig {
            timeout: Some(SINGLE_ATTEMPT_TIMEOUT),
            max_retries: Some(0),
            ..operator_config.clone()
        });
        Ok(Self {
            operator_config,
            prefix,
            credentials: Arc::new(CachedCredentialSource::new(credential_source)),
            cached_operator: Arc::new(RwLock::new(None)),
            single_attempt,
        })
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    /// The `s3://` URL overlaybd reads an object or prefix from. overlaybd
    /// expects S3-compatible URLs, including for Alibaba OSS.
    pub(crate) fn object_url(&self, key: &str) -> String {
        format!(
            "s3://{}/{}",
            self.operator_config.bucket,
            self.full_key(key)
        )
    }

    /// Read a small object entirely into memory.
    pub(crate) async fn get_bytes(&self, key: &str) -> Result<Bytes> {
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "get_bytes");
        let result = self
            .run_with_key(key, |operator, key| async move {
                operator.read(&key).await.map(|buffer| buffer.to_bytes())
            })
            .await
            .with_context(|| format!("oss get '{key}'"));
        metric.finish(&result);
        result
    }

    /// Download an object directly to a local file (atomic: temp + rename).
    pub(crate) async fn get_to_file(&self, key: &str, dest: &Path) -> Result<u64> {
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "get_to_file");
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create cache dir '{}'", parent.display()))?;
        }

        let dest = dest.to_path_buf();
        let oss_key = self.full_key(key);
        let result = self
            .run_with_operator(|operator| {
                let dest = dest.clone();
                let oss_key = oss_key.clone();
                async move { download_object_to_file(&operator, &oss_key, &dest).await }
            })
            .await
            .with_context(|| format!("oss download '{key}'"));
        metric.finish(&result);
        result
    }

    /// Check whether an object exists.
    pub(crate) async fn exists(&self, key: &str) -> Result<bool> {
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "exists");
        let result = self
            .run_with_key(
                key,
                |operator, key| async move { operator.exists(&key).await },
            )
            .await
            .with_context(|| format!("oss exists '{key}'"));
        metric.finish(&result);
        result
    }

    /// Return the byte size of an object, or `None` when absent.
    pub(crate) async fn stat_size(&self, key: &str) -> Result<Option<u64>> {
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "stat");
        let result = self
            .run_with_key(key, |operator, key| async move {
                match operator.stat(&key).await {
                    Ok(metadata) => Ok(Some(metadata.content_length())),
                    Err(error) if error.kind() == OpenDalErrorKind::NotFound => Ok(None),
                    Err(error) => Err(error),
                }
            })
            .await
            .with_context(|| format!("oss stat '{key}'"));
        metric.finish(&result);
        result
    }

    /// List all files recursively under a prefix.
    pub(crate) async fn list_keys_recursive(&self, prefix: &str) -> Result<Vec<String>> {
        let keys = self
            .run_with_key(prefix, |operator, prefix| async move {
                let entries = operator.list_with(&prefix).recursive(true).await?;

                Ok(entries
                    .into_iter()
                    .filter(|entry| !entry.metadata().mode().is_dir())
                    .map(|entry| entry.path().to_string())
                    .collect())
            })
            .await
            .with_context(|| format!("oss list '{prefix}'"))?;

        if self.prefix.is_empty() {
            return Ok(keys);
        }
        let strip = format!("{}/", self.prefix);
        Ok(keys
            .into_iter()
            .map(|p| p.strip_prefix(&strip).unwrap_or(&p).to_string())
            .collect())
    }

    /// Write small data (catalog JSON, alias JSON, etc.).
    pub(crate) async fn put_bytes(
        &self,
        key: &str,
        data: impl Into<Bytes>,
        artifact: OssUploadArtifact,
    ) -> Result<()> {
        let data = data.into();
        let size = data.len() as u64;
        let oss_key = self.full_key(key);
        let mut metric =
            MetricGuard::operation_artifact(OSS_OPERATION_DURATION, "put_bytes", artifact.as_str());
        let result = self
            .run_with_operator(|operator| {
                let data = data.clone();
                let oss_key = oss_key.clone();
                async move { write_bytes_to_operator(&operator, &oss_key, data).await }
            })
            .await
            .with_context(|| format!("oss put '{key}'"));
        metric.finish(&result);
        if result.is_ok() {
            metrics::counter!(
                "agentenv_snapshot_oss_upload_bytes_total",
                "operation" => "put_bytes",
                "artifact" => artifact.as_str(),
            )
            .increment(size);
        }
        result?;
        Ok(())
    }

    /// Upload a local file to OSS.
    pub(crate) async fn put_file(
        &self,
        key: &str,
        path: &Path,
        artifact: OssUploadArtifact,
    ) -> Result<()> {
        let oss_key = self.full_key(key);
        let path = path.to_path_buf();
        let mut metric =
            MetricGuard::operation_artifact(OSS_OPERATION_DURATION, "put_file", artifact.as_str());
        let start = Instant::now();
        let result: Result<u64> = async {
            let size = tokio::fs::metadata(&path)
                .await
                .with_context(|| format!("stat oss upload source file '{}'", path.display()))?
                .len();
            self.run_with_operator(|operator| {
                let oss_key = oss_key.clone();
                let path = path.clone();
                async move {
                    upload_file_streaming(
                        &operator,
                        &oss_key,
                        &path,
                        CHUNK_SIZE,
                        UPLOAD_CONCURRENCY,
                        None,
                    )
                    .await
                }
            })
            .await
            .with_context(|| format!("oss put file '{key}'"))?;
            Ok(size)
        }
        .await;
        metric.finish(&result);
        match result {
            Ok(size) => {
                metrics::counter!(
                    "agentenv_snapshot_oss_upload_bytes_total",
                    "operation" => "put_file",
                    "artifact" => artifact.as_str(),
                )
                .increment(size);
                info!(
                    key = %oss_key,
                    artifact = artifact.as_str(),
                    size_bytes = size,
                    // A u64, because the JSON log format writes a u128 as
                    // text, and text cannot be summed or averaged.
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "oss file uploaded"
                );
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Delete a single object. Idempotent – missing objects are not errors.
    pub(crate) async fn delete(&self, key: &str) -> Result<()> {
        self.run_with_key(key, |operator, key| async move {
            match operator.delete(&key).await {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == OpenDalErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            }
        })
        .await
        .with_context(|| format!("oss delete '{key}'"))
    }

    async fn delete_once(&self, key: &str) -> Result<()> {
        let operator = self
            .ensure_fresh_operator(&self.single_attempt.config, &self.single_attempt.operator)
            .await?;
        match operator.operator().delete(&self.full_key(key)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == OpenDalErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| format!("oss delete '{key}'")),
        }
    }

    /// Batch-delete keys without retries (so delayed retries cannot outlive the call),
    /// falling back to per-key `delete_once` on batch error so healthy keys still succeed.
    pub(crate) async fn delete_batch_once(&self, keys: &[String]) -> Vec<Result<()>> {
        if keys.is_empty() {
            return Vec::new();
        }
        if let Ok(operator) = self
            .ensure_fresh_operator(&self.single_attempt.config, &self.single_attempt.operator)
            .await
        {
            let full_keys: Vec<String> = keys.iter().map(|key| self.full_key(key)).collect();
            if operator.operator().delete_iter(full_keys).await.is_ok() {
                return (0..keys.len()).map(|_| Ok(())).collect();
            }
        }
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.delete_once(key).await);
        }
        results
    }

    /// Delete all objects under a prefix.
    pub(crate) async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        // `list_keys_recursive()` returns repository-relative keys with the
        // configured backend prefix stripped, while `delete()` expects that
        // same repository-relative form and re-applies the backend prefix.
        let keys = self.list_keys_recursive(prefix).await?;
        stream::iter(keys.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(16, |key| async move { self.delete(&key).await })
            .await
    }

    pub(crate) fn is_not_found_error(error: &anyhow::Error) -> bool {
        error.chain().any(|cause| {
            if let Some(opendal_error) = cause.downcast_ref::<OpenDalError>() {
                return opendal_error.kind() == OpenDalErrorKind::NotFound;
            }
            if let Some(ObjectStoreOperatorError::OpenDal(opendal_error)) =
                cause.downcast_ref::<ObjectStoreOperatorError>()
            {
                return opendal_error.kind() == OpenDalErrorKind::NotFound;
            }
            false
        })
    }

    async fn run_with_key<T, F, Fut>(&self, key: &str, operation: F) -> Result<T>
    where
        F: Fn(Operator, String) -> Fut,
        Fut: std::future::Future<Output = opendal::Result<T>>,
    {
        let key = self.full_key(key);
        self.run_with_operator(|operator| {
            let key = key.clone();
            operation(operator, key)
        })
        .await
    }

    async fn run_with_operator<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn(Operator) -> Fut,
        Fut: std::future::Future<Output = opendal::Result<T>>,
    {
        self.run_with(&self.operator_config, &self.cached_operator, operation)
            .await
    }

    async fn run_with<T, F, Fut>(
        &self,
        config: &ObjectStoreOperatorConfig,
        cached_operator: &RwLock<Option<OperatorWithCredential>>,
        operation: F,
    ) -> Result<T>
    where
        F: Fn(Operator) -> Fut,
        Fut: std::future::Future<Output = opendal::Result<T>>,
    {
        // Centralizes one-shot operator construction plus credential-refresh
        // retry semantics so individual OSS operations don't each have to
        // reason about cached credentials and operator replacement.
        let current = self.ensure_fresh_operator(config, cached_operator).await?;
        let (value, refreshed) =
            run_with_refresh(&current, Some(self.credentials.as_ref()), config, operation)
                .await
                .map_err(anyhow::Error::from)?;
        if let Some(refreshed) = refreshed {
            *cached_operator.write().await = Some(refreshed);
        }
        Ok(value)
    }

    async fn ensure_fresh_operator(
        &self,
        config: &ObjectStoreOperatorConfig,
        cached_operator: &RwLock<Option<OperatorWithCredential>>,
    ) -> Result<OperatorWithCredential> {
        let credential = self.credentials.current().await?.ok_or_else(|| {
            anyhow::anyhow!("snapshot OSS client requires non-anonymous credentials")
        })?;

        {
            let cached = cached_operator.read().await;
            if let Some(state) = cached.as_ref() {
                if state.credential() == Some(&credential) {
                    return Ok(state.clone());
                }
            }
        }

        let entry = OperatorWithCredential::new(
            build_object_store_operator(config, Some(&credential))?,
            Some(credential),
        );
        *cached_operator.write().await = Some(entry.clone());
        Ok(entry)
    }
}

async fn download_object_to_file(
    operator: &Operator,
    key: &str,
    dest: &Path,
) -> opendal::Result<u64> {
    // Keep the tempfile handle alive until the final rename so any early
    // return still benefits from `NamedTempFile`'s automatic cleanup.
    let tmp = tempfile::NamedTempFile::new_in(dest.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(|err| io_error_to_opendal(err, "create temporary download file"))?;
    let tmp_path = tmp.path().to_path_buf();
    let std_file = tmp
        .reopen()
        .map_err(|err| io_error_to_opendal(err, "reopen temporary download file"))?;
    let mut file = tokio::fs::File::from_std(std_file);
    let mut size = 0_u64;
    let mut stream = operator.reader(key).await?.into_stream(..).await?;
    while let Some(buffer) = stream.try_next().await? {
        for chunk in buffer {
            size += chunk.len() as u64;
            file.write_all(chunk.as_ref())
                .await
                .map_err(|err| io_error_to_opendal(err, "write downloaded object chunk"))?;
        }
    }

    file.flush()
        .await
        .map_err(|err| io_error_to_opendal(err, "flush downloaded object file"))?;
    file.sync_all()
        .await
        .map_err(|err| io_error_to_opendal(err, "sync downloaded object file"))?;
    drop(file);
    tokio::fs::rename(&tmp_path, dest)
        .await
        .map_err(|err| io_error_to_opendal(err, "rename downloaded object into place"))?;

    Ok(size)
}

async fn write_bytes_to_operator(
    operator: &Operator,
    key: &str,
    data: Bytes,
) -> opendal::Result<()> {
    operator.write(key, data).await.map(|_| ())
}

fn io_error_to_opendal(error: std::io::Error, message: &'static str) -> OpenDalError {
    OpenDalError::new(OpenDalErrorKind::Unexpected, message).set_source(error)
}

#[cfg(test)]
mod tests {
    use super::OssClient;
    use crate::cfg::OssBackendConfig;
    use object_store_operator::{AddressingStyle, CredentialSource};

    fn sample_config() -> OssBackendConfig {
        OssBackendConfig {
            endpoint: " https://oss-cn-hangzhou.aliyuncs.com ".to_string(),
            bucket: " demo-bucket ".to_string(),
            prefix: Some(" /snapshots/managed/ ".to_string()),
            credential_process: None,
            access_key_id: Some(" ak ".to_string()),
            access_key_secret: Some(" sk ".to_string()),
            security_token: Some(" token ".to_string()),
            region: Some(" cn-hangzhou ".to_string()),
            addressing_style: None,
            cache_max_size_gb: Some(8),
        }
    }

    #[test]
    fn from_config_and_addressing_validation() {
        let client = OssClient::from_config(&sample_config()).expect("build client from config");
        assert_eq!(client.operator_config.bucket, "demo-bucket");
        assert_eq!(
            client.operator_config.endpoint,
            "https://oss-cn-hangzhou.aliyuncs.com"
        );
        assert_eq!(client.operator_config.region, "cn-hangzhou");
        assert_eq!(client.prefix, "snapshots/managed");

        let mut mixed = sample_config();
        mixed.credential_process = Some("echo creds".to_string());
        assert!(OssClient::from_config(&mixed)
            .unwrap_err()
            .to_string()
            .contains("credential_process cannot be combined"));

        let mut blank_region = sample_config();
        blank_region.region = Some(" ".to_string());
        assert!(OssClient::from_config(&blank_region)
            .unwrap_err()
            .to_string()
            .contains("backend.oss.region is required"));

        let detected = OssClient::new(
            "snapshots".to_string(),
            "https://t3.storage.dev".to_string(),
            "auto".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("detected style");
        assert_eq!(
            detected.operator_config.addressing_style,
            AddressingStyle::Path
        );
        let overridden = OssClient::new(
            "snapshots".to_string(),
            "https://t3.storage.dev".to_string(),
            "auto".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            Some(AddressingStyle::Virtual),
        )
        .expect("override style");
        assert_eq!(
            overridden.operator_config.addressing_style,
            AddressingStyle::Virtual
        );
        assert!(OssClient::new(
            "snapshots".to_string(),
            "not a valid endpoint".to_string(),
            "auto".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            Some(AddressingStyle::Virtual),
        )
        .is_err());
    }
}
