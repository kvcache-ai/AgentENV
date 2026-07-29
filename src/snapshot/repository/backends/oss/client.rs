use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{stream, TryStreamExt};
use object_store_operator::{
    build_object_store_operator, run_with_refresh, AddressingStyle, CachedCredentialSource,
    CredentialSource, ObjectStoreOperatorConfig, ObjectStoreOperatorError, OperatorWithCredential,
};
use opendal::{Error as OpenDalError, ErrorKind as OpenDalErrorKind, Operator};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use url::Url;

use crate::observability::prometheus::MetricGuard;

const CHUNK_SIZE: usize = 8 * 1024 * 1024;
const OSS_OPERATION_DURATION: &str = "agentenv_snapshot_oss_operation_duration_seconds";

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
        // An explicit config override wins; otherwise fall back to
        // endpoint-based detection (Alibaba OSS / bucket-in-endpoint hosts).
        let addressing_style = match addressing_override {
            Some(style) => style,
            None => detect_addressing_style(&endpoint, &bucket)?,
        };
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
    pub(crate) async fn put_bytes(&self, key: &str, data: impl Into<Bytes>) -> Result<()> {
        let data = data.into();
        let size = data.len() as u64;
        let oss_key = self.full_key(key);
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "put_bytes");
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
            )
            .increment(size);
        }
        result?;
        Ok(())
    }

    /// Upload a local file to OSS.
    pub(crate) async fn put_file(&self, key: &str, path: &Path) -> Result<()> {
        let oss_key = self.full_key(key);
        let path = path.to_path_buf();
        let mut metric = MetricGuard::operation(OSS_OPERATION_DURATION, "put_file");
        let result: Result<u64> = async {
            let size = tokio::fs::metadata(&path)
                .await
                .with_context(|| format!("stat oss upload source file '{}'", path.display()))?
                .len();
            self.run_with_operator(|operator| {
                let oss_key = oss_key.clone();
                let path = path.clone();
                async move { upload_file_to_operator(&operator, &oss_key, &path).await }
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
                )
                .increment(size);
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

async fn upload_file_to_operator(
    operator: &Operator,
    key: &str,
    path: &Path,
) -> opendal::Result<()> {
    let mut writer = operator.writer(key).await?;
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|err| io_error_to_opendal(err, "open upload source file"))?;
    let mut buf = vec![0_u8; CHUNK_SIZE];
    loop {
        let read = file
            .read(&mut buf)
            .await
            .map_err(|err| io_error_to_opendal(err, "read upload source file"))?;
        if read == 0 {
            break;
        }
        writer.write(buf[..read].to_vec()).await?;
    }
    writer.close().await?;
    Ok(())
}

fn io_error_to_opendal(error: std::io::Error, message: &'static str) -> OpenDalError {
    OpenDalError::new(OpenDalErrorKind::Unexpected, message).set_source(error)
}

#[cfg(test)]
mod tests {
    use super::{detect_addressing_style, OssClient};
    use object_store_operator::{AddressingStyle, CredentialSource};

    #[test]
    fn detect_defaults_to_path_for_generic_endpoint() {
        // Virtual-host-only providers such as Tigris (t3.storage.dev) are not
        // matched by the heuristic and fall back to path style, which is why an
        // explicit `addressing_style` override exists.
        assert_eq!(
            detect_addressing_style("https://t3.storage.dev", "snapshots").unwrap(),
            AddressingStyle::Path
        );
    }

    #[test]
    fn detect_uses_virtual_for_aliyun_and_bucket_host() {
        assert_eq!(
            detect_addressing_style("https://oss-cn-hangzhou.aliyuncs.com", "b").unwrap(),
            AddressingStyle::Virtual
        );
        assert_eq!(
            detect_addressing_style("https://b.example.com", "b").unwrap(),
            AddressingStyle::Virtual
        );
    }

    #[test]
    fn explicit_override_takes_precedence_over_detection() {
        let client = OssClient::new(
            "snapshots".to_string(),
            "https://t3.storage.dev".to_string(),
            "auto".to_string(),
            String::new(),
            CredentialSource::Anonymous,
            Some(AddressingStyle::Virtual),
        )
        .expect("build client");
        assert_eq!(
            client.operator_config.addressing_style,
            AddressingStyle::Virtual
        );
    }
}
