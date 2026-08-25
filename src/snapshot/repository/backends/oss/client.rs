use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{stream, TryStreamExt};
use object_store_operator::{
    build_object_store_operator, run_with_refresh, AddressingStyle, CachedCredentialSource,
    CredentialSource, ObjectStoreOperatorConfig, ObjectStoreOperatorError, OperatorWithCredential,
};
use opendal::{Error as OpenDalError, ErrorKind as OpenDalErrorKind, Operator};
use overlaybd::backend::oss::upload_file_streaming;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::info;
use url::Url;

use crate::observability::prometheus::MetricGuard;

/// Multipart part size for streaming file uploads. S3/OSS caps a multipart
/// upload at 10,000 parts, so this bounds the largest uploadable object
/// (~625 GiB at 64 MiB). Must be passed explicitly to opendal via
/// `writer_with().chunk()`: without it opendal falls back to the service's
/// minimum multipart part size (5 MiB), capping uploads at ~50 GiB.
///
/// Note the memory cost, which is `(2 * UPLOAD_CONCURRENCY + 2) * CHUNK_SIZE`
/// rather than the product of the two — **measured at 1040 MiB for these
/// figures**. See `overlaybd::backend::oss::upload_file_streaming` for why.
/// Deliberately left as it was when the upload loop moved there: shrinking it
/// would also shrink the largest uploadable object.
const CHUNK_SIZE: usize = 64 * 1024 * 1024;
/// Number of multipart parts uploaded concurrently per file. A single
/// sequential stream tops out at roughly 100 MB/s to the OSS internal
/// endpoint; concurrent parts multiply effective throughput.
const UPLOAD_CONCURRENCY: usize = 8;
const OSS_OPERATION_DURATION: &str = "agentenv_snapshot_oss_operation_duration_seconds";

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
    CatalogRecord,
    Alias,
}

impl OssUploadArtifact {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RootfsLayer => "rootfs_layer",
            Self::AttachedDriveLayer => "attached_drive_layer",
            Self::MemoryLayer => "memory_layer",
            Self::VmState => "vm_state",
            Self::FirecrackerManifest => "manifest",
            Self::CatalogRecord => "record",
            Self::Alias => "alias",
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
}

impl OssClient {
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
        Ok(Self {
            operator_config: ObjectStoreOperatorConfig {
                addressing_style,
                bucket,
                endpoint,
                region,
                timeout: None,
                max_retries: None,
            },
            prefix,
            credentials: Arc::new(CachedCredentialSource::new(credential_source)),
            cached_operator: Arc::new(RwLock::new(None)),
        })
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    pub(crate) fn managed_layers_repo_blob_url(&self) -> String {
        // overlaybd expects an S3-compatible repo blob URL here, including for
        // Alibaba OSS, so the scheme remains `s3://` rather than `oss://`.
        if self.prefix.is_empty() {
            format!("s3://{}/managed-layers", self.operator_config.bucket)
        } else {
            format!(
                "s3://{}/{}/managed-layers",
                self.operator_config.bucket, self.prefix
            )
        }
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
                    elapsed_ms = start.elapsed().as_millis(),
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
        // Centralizes one-shot operator construction plus credential-refresh
        // retry semantics so individual OSS operations don't each have to
        // reason about cached credentials and operator replacement.
        let current = self.ensure_fresh_operator().await?;
        let (value, refreshed) = run_with_refresh(
            &current,
            Some(self.credentials.as_ref()),
            &self.operator_config,
            operation,
        )
        .await
        .map_err(anyhow::Error::from)?;
        if let Some(refreshed) = refreshed {
            *self.cached_operator.write().await = Some(refreshed);
        }
        Ok(value)
    }

    async fn ensure_fresh_operator(&self) -> Result<OperatorWithCredential> {
        let credential = self.credentials.current().await?.ok_or_else(|| {
            anyhow::anyhow!("snapshot OSS client requires non-anonymous credentials")
        })?;

        {
            let cached = self.cached_operator.read().await;
            if let Some(state) = cached.as_ref() {
                if state.credential() == Some(&credential) {
                    return Ok(state.clone());
                }
            }
        }

        let entry = OperatorWithCredential::new(
            build_object_store_operator(&self.operator_config, Some(&credential))?,
            Some(credential),
        );
        *self.cached_operator.write().await = Some(entry.clone());
        Ok(entry)
    }
}

fn detect_addressing_style(endpoint: &str, bucket: &str) -> Result<AddressingStyle> {
    let url = Url::parse(endpoint).context("parse snapshot OSS endpoint for addressing style")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("snapshot OSS endpoint host is missing"))?;
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
    use object_store_operator::{AddressingStyle, CredentialSource};

    #[test]
    fn explicit_override_takes_precedence_over_detection() {
        let detected = OssClient::new(
            "snapshots".to_string(),
            "https://t3.storage.dev".to_string(),
            "auto".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("build client with detected style");
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
        .expect("build client with override");
        assert_eq!(
            overridden.operator_config.addressing_style,
            AddressingStyle::Virtual
        );
    }

    #[test]
    fn explicit_override_still_validates_endpoint() {
        OssClient::new(
            "snapshots".to_string(),
            "not a valid endpoint".to_string(),
            "auto".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            Some(AddressingStyle::Virtual),
        )
        .expect_err("malformed endpoint must fail even with an explicit override");
    }
}
