use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use super::client::{OssClient, OssUploadArtifact};
use super::documents;
use crate::cfg::{SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
use crate::repository::interfaces::{LayerRole, PublishPolicy, Repository};
use crate::repository::{
    BlobBody, BlobKey, BlobLocation, DocumentCollection, DocumentKey, DocumentUpdate,
    RepositoryError, RepositoryResult, SnapshotFile,
};
use crate::snapshot::SnapshotId;

/// Manages the committed‐state layer of the OSS snapshot repository.
///
/// Objects sit under the configured prefix at the paths
/// [`DocumentKey::path`] and [`BlobKey::path`] name.
pub(crate) struct OssRepository {
    client: Arc<OssClient>,
    policy: PublishPolicy,
}

impl OssRepository {
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
    }
}

// ── Repository impl ────────────────────────────────────────────

#[async_trait]
impl Repository for OssRepository {
    fn publish_policy(&self) -> PublishPolicy {
        self.policy.clone()
    }

    fn managed_layers_repo_blob_url(&self) -> Option<String> {
        Some(self.client.object_url(BlobKey::LAYERS_DIR))
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

    async fn fetch_blob(&self, key: &BlobKey, destination: &Path) -> RepositoryResult<u64> {
        let object_key = key.path();
        self.client
            .get_to_file(&object_key, destination)
            .await
            .map_err(|error| RepositoryError::backend(format!("download '{object_key}'"), error))
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
        let prefix = format!("{}/", BlobKey::snapshot_dir(id));
        self.client
            .delete_prefix(&prefix)
            .await
            .map_err(|error| RepositoryError::backend(format!("delete '{prefix}'"), error))
    }

    async fn get_document(&self, key: DocumentKey) -> RepositoryResult<Option<Vec<u8>>> {
        documents::get(&self.client, &key).await
    }

    async fn put_document(&self, key: DocumentKey, bytes: Vec<u8>) -> RepositoryResult<()> {
        documents::put(&self.client, &key, bytes).await
    }

    async fn update_document(
        &self,
        key: DocumentKey,
        update: DocumentUpdate,
    ) -> RepositoryResult<Option<Vec<u8>>> {
        documents::update(&self.client, &key, update).await
    }

    async fn delete_document(&self, key: DocumentKey) -> RepositoryResult<()> {
        documents::delete(&self.client, &key).await
    }

    async fn list_documents(
        &self,
        collection: DocumentCollection,
        after: Option<&str>,
        limit: usize,
    ) -> RepositoryResult<Vec<String>> {
        documents::list(&self.client, collection, after, limit).await
    }

    async fn read_documents(
        &self,
        collection: DocumentCollection,
    ) -> RepositoryResult<Vec<(String, Vec<u8>)>> {
        documents::read_all(&self.client, collection).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store_operator::CredentialSource;
    use overlaybd::config::ImageConfig;

    use crate::snapshot::{ExternalLayer, ManagedLayer, OverlaybdLayerRef};

    fn test_repository() -> OssRepository {
        let client = OssClient::new(
            "bucket".to_string(),
            "https://oss.example.com".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("oss client");
        OssRepository::new(
            Arc::new(client),
            SnapshotImageStoragePolicy::ObjectStorage,
            &SnapshotPublishCompressionConfig {
                enabled: false,
                ..Default::default()
            },
        )
    }

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

    #[tokio::test]
    async fn materializes_volume_backing_with_backend_layer_urls() {
        let temp = tempfile::tempdir().expect("tempdir");
        let repository = test_repository();
        let destination = temp.path().join("volume/image.json");
        let layers = vec![
            OverlaybdLayerRef::Managed(ManagedLayer {
                digest: "sha256:managed".to_string(),
                size: 12,
                uuid: None,
            }),
            OverlaybdLayerRef::External(ExternalLayer {
                digest: "sha256:external".to_string(),
                repo_blob_url: "https://registry.example/v2/data/blobs".to_string(),
                size: 24,
            }),
        ];

        crate::snapshot::resolver::materialize_volume_backing(&repository, &layers, &destination)
            .await
            .expect("materialize volume config");
        let config: ImageConfig =
            serde_json::from_slice(&tokio::fs::read(&destination).await.unwrap()).unwrap();
        assert_eq!(config.lowers.len(), 2);
        assert_eq!(
            config.lowers[0].repo_blob_url,
            "s3://bucket/prefix/managed-layers"
        );
        assert_eq!(
            config.lowers[1].repo_blob_url,
            "https://registry.example/v2/data/blobs"
        );
        assert!(config.lowers.iter().all(|layer| layer.file.is_empty()));
    }
}
