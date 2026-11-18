use std::env;

use anyhow::{ensure, Context, Result};
use clap::Args;
use futures_util::TryStreamExt;
use object_store_operator::{
    build_object_store_operator, credential_source_from_fields, detect_addressing_style,
    CachedCredentialSource, CredentialFields, CredentialSourceOptions, ObjectStoreOperatorConfig,
    ObjectStoreService, OpenDalErrorKind, Operator, OperatorWithCredential,
};
use overlaybd::layer::LayerFile;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[derive(Args)]
pub struct Source {
    /// Object-store endpoint used by the legacy Node.
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    bucket: String,
    /// Repository prefix, or an empty string for the bucket root.
    #[arg(long)]
    prefix: String,
    #[arg(long)]
    region: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stamp {
    pub size: u64,
    etag: String,
}

impl Stamp {
    fn new(key: &str, size: u64, etag: Option<&str>) -> Result<Self> {
        let etag = etag
            .map(|value| value.trim_matches('"'))
            .filter(|value| !value.is_empty())
            .with_context(|| format!("{key} has no ETag"))?;
        Ok(Self {
            size,
            etag: etag.to_owned(),
        })
    }
}

pub struct Store {
    source: Source,
    config: ObjectStoreOperatorConfig,
    credentials: CachedCredentialSource,
    operator: Option<OperatorWithCredential>,
}

impl Store {
    pub fn new(mut source: Source) -> Result<Self> {
        source.prefix = source.prefix.trim().trim_matches('/').to_owned();
        let config = ObjectStoreOperatorConfig {
            service: ObjectStoreService::S3,
            bucket: source.bucket.clone(),
            endpoint: source.endpoint.clone(),
            region: source.region.clone(),
            addressing_style: detect_addressing_style(&source.endpoint, &source.bucket)?,
            timeout: None,
            max_retries: None,
        };
        let credentials = credential_source_from_fields(
            CredentialFields {
                access_key_id: env::var("AWS_ACCESS_KEY_ID").ok().as_deref(),
                secret_access_key: env::var("AWS_SECRET_ACCESS_KEY").ok().as_deref(),
                security_token: env::var("AWS_SESSION_TOKEN").ok().as_deref(),
                credential_process: env::var("SNAPSHOT_IMPORT_CREDENTIAL_PROCESS")
                    .ok()
                    .as_deref(),
            },
            CredentialSourceOptions {
                scope: "snapshot import",
                allow_anonymous: false,
                required_access_key_id_label: "AWS_ACCESS_KEY_ID",
                required_secret_access_key_label: "AWS_SECRET_ACCESS_KEY",
            },
        )?;
        Ok(Self {
            source,
            config,
            credentials: CachedCredentialSource::new(credentials),
            operator: None,
        })
    }

    async fn operator(&mut self) -> Result<Operator> {
        let credential = self.credentials.current().await?;
        if let Some(cached) = &self.operator {
            if cached.credential() == credential.as_ref() {
                return Ok(cached.operator().clone());
            }
        }
        let operator = build_object_store_operator(&self.config, credential.as_ref())?;
        self.operator = Some(OperatorWithCredential::new(operator.clone(), credential));
        Ok(operator)
    }

    pub fn layer_url(&self, isolated: bool) -> String {
        let prefix = if isolated {
            "snapshot-layers"
        } else {
            "managed-layers"
        };
        format!("s3://{}/{}", self.source.bucket, self.key(prefix))
    }

    fn key(&self, key: &str) -> String {
        if self.source.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.source.prefix)
        }
    }

    pub async fn read_checked(&mut self, key: &str, stamp: &Stamp) -> Result<Vec<u8>> {
        let full = self.key(key);
        let etag = format!("\"{}\"", stamp.etag);
        let bytes = self
            .operator()
            .await?
            .read_with(&full)
            .if_match(&etag)
            .await
            .with_context(|| format!("read audited object {key}"))?;
        ensure!(
            bytes.len() as u64 == stamp.size,
            "{key} changed during audit"
        );
        Ok(bytes.to_vec())
    }

    pub async fn read_optional(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let full = self.key(key);
        match self.operator().await?.read(&full).await {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(error) if error.kind() == OpenDalErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("read {key}")),
        }
    }

    pub async fn stamp(&mut self, key: &str) -> Result<Stamp> {
        let full = self.key(key);
        let metadata = self
            .operator()
            .await?
            .stat(&full)
            .await
            .with_context(|| format!("stat {key}"))?;
        Stamp::new(key, metadata.content_length(), metadata.etag())
    }

    pub async fn create_content(&mut self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let full = self.key(key);
        match self
            .operator()
            .await?
            .write_with(&full, bytes)
            .if_not_exists(true)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == OpenDalErrorKind::ConditionNotMatch => Ok(()),
            Err(error) => Err(error).with_context(|| format!("create {key}")),
        }
    }

    pub async fn layer_uuid(&mut self, key: &str, digest: &str, stamp: &Stamp) -> Result<Uuid> {
        let (file, path) = tempfile::NamedTempFile::new()
            .context("create temporary layer file")?
            .into_parts();
        let mut file = tokio::fs::File::from_std(file);
        let full = self.key(key);
        let mut stream = self
            .operator()
            .await?
            .reader(&full)
            .await?
            .into_bytes_stream(..)
            .await?;
        let mut hasher = Sha256::new();
        let mut size = 0;
        while let Some(chunk) = stream.try_next().await? {
            size += chunk.len() as u64;
            ensure!(size <= stamp.size, "{key} grew during download");
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        drop(file);
        ensure!(
            size == stamp.size && format!("sha256:{:x}", hasher.finalize()) == digest,
            "{key} does not match its recorded size and digest"
        );
        ensure!(
            self.stamp(key).await? == *stamp,
            "{key} changed during download"
        );
        let layer = LayerFile::from_path(path.to_path_buf(), None).await?;
        Ok(layer.uuid)
    }
}
