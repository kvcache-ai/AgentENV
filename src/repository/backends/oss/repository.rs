use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};

use super::client::{OssClient, OssUploadArtifact};
use super::config::NormalizedOssConfig;
use crate::cfg::{OssBackendConfig, SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
use crate::repository::interfaces::{LayerRole, PublishPolicy, Repository};
use crate::repository::{
    build_log_path, BlobBody, BlobKey, BlobLocation, BlobMetadata, LegacyDocumentCollection,
    LegacyDocumentKey, RepositoryError, RepositoryResult, SnapshotFile,
};
use crate::types::SnapshotId;

const LEGACY_READ_CONCURRENCY: usize = 16;

/// Manages the durable artifact layer of the OSS repository.
///
/// Objects sit under the configured prefix at the paths named in `layout`.
pub(crate) struct OssRepository {
    client: Arc<OssClient>,
    policy: PublishPolicy,
}

impl OssRepository {
    /// Connects to the bucket and prefix that `config` names.
    pub(crate) fn from_config(
        config: &OssBackendConfig,
        snapshot_image_storage: SnapshotImageStoragePolicy,
        publish_compression: &SnapshotPublishCompressionConfig,
    ) -> Result<Self> {
        let config = NormalizedOssConfig::new(config, snapshot_image_storage)?;
        let client = Arc::new(OssClient::new(
            config.bucket,
            config.endpoint,
            config.region,
            config.prefix,
            config.credential_source,
            config.addressing_style,
        )?);
        Ok(Self::new(
            client,
            config.snapshot_image_storage,
            publish_compression,
        ))
    }

    pub(crate) fn new(
        client: Arc<OssClient>,
        snapshot_image_storage: SnapshotImageStoragePolicy,
        publish_compression: &SnapshotPublishCompressionConfig,
    ) -> Self {
        Self {
            client,
            policy: PublishPolicy {
                compression: publish_compression.clone(),
                image_storage: snapshot_image_storage,
            },
        }
    }
}

fn upload_artifact(key: &BlobKey) -> OssUploadArtifact {
    match key {
        BlobKey::Layer { role, .. } => match role {
            LayerRole::Rootfs => OssUploadArtifact::RootfsLayer,
            LayerRole::AttachedDrive => OssUploadArtifact::AttachedDriveLayer,
            LayerRole::Memory => OssUploadArtifact::MemoryLayer,
        },
        BlobKey::Snapshot(_, SnapshotFile::VmState) => OssUploadArtifact::VmState,
        BlobKey::Snapshot(_, SnapshotFile::FirecrackerManifest) => {
            OssUploadArtifact::FirecrackerManifest
        }
        BlobKey::Snapshot(_, SnapshotFile::StartupManifest) => OssUploadArtifact::StartupPack,
        BlobKey::Snapshot(_, SnapshotFile::Record) => OssUploadArtifact::CatalogRecord,
    }
}

// ── Repository impl ────────────────────────────────────────────

#[async_trait]
impl Repository for OssRepository {
    fn publish_policy(&self) -> PublishPolicy {
        self.policy.clone()
    }

    fn managed_layers_repo_blob_url(&self) -> Option<String> {
        Some(self.client.object_url(agentenv_domain::LAYERS_DIR))
    }

    async fn delete_managed_layers(&self, digests: &[String]) -> Vec<RepositoryResult<()>> {
        let keys: Vec<String> = digests
            .iter()
            .map(|digest| {
                BlobKey::Layer {
                    digest: digest.clone(),
                    role: LayerRole::Rootfs,
                }
                .path()
            })
            .collect();
        self.client
            .delete_batch_once(&keys)
            .await
            .into_iter()
            .zip(keys)
            .map(|(result, key)| {
                result.map_err(|error| RepositoryError::backend(format!("delete '{key}'"), error))
            })
            .collect()
    }

    /// Layers are keyed by digest, so concurrent writers of one layer write
    /// identical bytes and the `exists → put` race is benign.
    ///
    /// The put is unconditional because Alibaba OSS rejects
    /// `x-oss-forbid-overwrite` on multipart uploads, which opendal uses for
    /// large files.
    async fn put_blob(&self, key: &BlobKey, body: BlobBody<'_>) -> RepositoryResult<()> {
        let object_key = key.path();
        if matches!(key, BlobKey::Layer { .. }) {
            let exists = self.client.exists(&object_key).await.map_err(|error| {
                RepositoryError::backend("check managed layer existence", error)
            })?;
            if exists {
                return Ok(());
            }
        }
        let artifact = upload_artifact(key);
        match body {
            BlobBody::File(source) => self.client.put_file(&object_key, source, artifact).await,
            BlobBody::Bytes(bytes) => self.client.put_bytes(&object_key, bytes, artifact).await,
        }
        .map_err(|error| RepositoryError::backend(format!("upload '{object_key}'"), error))
    }

    fn locate_blob(&self, key: &BlobKey) -> RepositoryResult<BlobLocation> {
        Ok(BlobLocation::Remote(self.client.object_url(&key.path())))
    }

    async fn has_blob(&self, key: &BlobKey) -> RepositoryResult<bool> {
        let object_key = key.path();
        self.client
            .exists(&object_key)
            .await
            .map_err(|error| RepositoryError::backend(format!("check '{object_key}'"), error))
    }

    async fn stat_blob(&self, key: &BlobKey) -> RepositoryResult<Option<BlobMetadata>> {
        let object_key = key.path();
        self.client
            .stat_size(&object_key)
            .await
            .map(|size| size.map(|size| BlobMetadata { size }))
            .map_err(|error| RepositoryError::backend(format!("stat '{object_key}'"), error))
    }

    async fn fetch_blob(&self, key: &BlobKey, destination: &Path) -> RepositoryResult<u64> {
        let object_key = key.path();
        match self.client.get_to_file(&object_key, destination).await {
            Ok(size) => Ok(size),
            Err(error) if OssClient::is_not_found_error(&error) => {
                Err(RepositoryError::ArtifactNotFound {
                    artifact: format!("'{object_key}'"),
                })
            }
            Err(error) => Err(RepositoryError::backend(
                format!("download '{object_key}'"),
                error,
            )),
        }
    }

    async fn read_blob(&self, key: &BlobKey) -> RepositoryResult<Vec<u8>> {
        let object_key = key.path();
        match self.client.get_bytes(&object_key).await {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(error) if OssClient::is_not_found_error(&error) => {
                Err(RepositoryError::ArtifactNotFound {
                    artifact: format!("'{object_key}'"),
                })
            }
            Err(error) => Err(RepositoryError::backend(
                format!("read '{object_key}'"),
                error,
            )),
        }
    }

    async fn delete_snapshot_blobs(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let prefix = format!("{}/", agentenv_domain::snapshot_dir(id));
        self.client
            .delete_prefix(&prefix)
            .await
            .map_err(|error| RepositoryError::backend(format!("delete '{prefix}'"), error))
    }

    async fn get_build_log(&self, id: &SnapshotId) -> RepositoryResult<Option<Vec<u8>>> {
        read_optional(&self.client, &build_log_path(id)).await
    }

    async fn put_build_log(&self, id: &SnapshotId, bytes: Vec<u8>) -> RepositoryResult<()> {
        let path = build_log_path(id);
        self.client
            .put_bytes(&path, bytes, OssUploadArtifact::BuildLog)
            .await
            .map_err(|error| RepositoryError::backend(format!("write '{path}'"), error))
    }

    async fn delete_build_log(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let path = build_log_path(id);
        self.client
            .delete(&path)
            .await
            .map_err(|error| RepositoryError::backend(format!("delete '{path}'"), error))
    }

    async fn get_legacy_document(
        &self,
        key: LegacyDocumentKey,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        read_optional(&self.client, &key.path()).await
    }

    async fn put_legacy_document(
        &self,
        key: LegacyDocumentKey,
        bytes: Vec<u8>,
    ) -> RepositoryResult<()> {
        let path = key.path();
        self.client
            .put_bytes(&path, bytes, OssUploadArtifact::CatalogRecord)
            .await
            .map_err(|error| RepositoryError::backend(format!("write '{path}'"), error))
    }

    async fn list_legacy_documents(
        &self,
        collection: LegacyDocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        let prefix = collection.prefix();
        let start_after = after
            .map(|id| {
                collection.key(id).map(|key| key.path()).ok_or_else(|| {
                    RepositoryError::InvalidRequest {
                        reason: format!("invalid document id '{id}'"),
                    }
                })
            })
            .transpose()?;
        let keys = self
            .client
            .list_keys_page(prefix, start_after.as_deref(), limit)
            .await
            .map_err(|error| RepositoryError::backend(format!("list '{prefix}'"), error))?;
        keys.iter()
            .map(|key| legacy_document_id(collection, key))
            .collect()
    }

    async fn read_legacy_documents(
        &self,
        collection: LegacyDocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        let prefix = collection.prefix();
        let keys = self
            .client
            .list_keys_recursive(prefix)
            .await
            .map_err(|error| RepositoryError::backend(format!("list '{prefix}'"), error))?;
        let client = &self.client;
        let read = stream::iter(keys)
            .map(|key| async move {
                let id = legacy_document_id(collection, &key)?;
                // Skips a document deleted since the listing.
                RepositoryResult::Ok(read_optional(client, &key).await?.map(|bytes| (id, bytes)))
            })
            .buffered(LEGACY_READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(read.into_iter().flatten().collect())
    }
}

async fn read_optional(client: &OssClient, path: &str) -> RepositoryResult<Option<Vec<u8>>> {
    match client.get_bytes(path).await {
        Ok(bytes) => Ok(Some(bytes.to_vec())),
        Err(error) if OssClient::is_not_found_error(&error) => Ok(None),
        Err(error) => Err(RepositoryError::backend(format!("read '{path}'"), error)),
    }
}

fn legacy_document_id(collection: LegacyDocumentCollection, key: &str) -> RepositoryResult<String> {
    key.strip_prefix(collection.prefix())
        .filter(|name| !name.contains('/'))
        .and_then(|name| collection.id_from_file_name(name))
        .map(str::to_owned)
        .ok_or_else(|| RepositoryError::InvalidRequest {
            reason: format!("invalid document key '{key}'"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store_operator::CredentialSource;

    #[test]
    fn publish_policy_carries_the_configured_compression_and_image_storage() {
        let client = OssClient::new(
            "bucket".to_string(),
            "https://oss.example.com".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("oss client");
        let repository = OssRepository::new(
            Arc::new(client),
            SnapshotImageStoragePolicy::SourceRegistry,
            &SnapshotPublishCompressionConfig {
                enabled: true,
                algorithm: crate::cfg::OverlaybdCompressionAlgorithm::Zstd,
                workers: 4,
            },
        );
        let policy = repository.publish_policy();
        assert_eq!(
            policy.image_storage,
            SnapshotImageStoragePolicy::SourceRegistry
        );
        assert!(policy.compression.enabled);
        assert_eq!(policy.compression.workers, 4);
        assert_eq!(
            repository.managed_layers_repo_blob_url().as_deref(),
            Some("s3://bucket/prefix/managed-layers")
        );
    }
}
