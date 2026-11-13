use std::path::{Path, PathBuf};

use overlaybd::config::{ImageConfig, LayerConfig};
use uuid::Uuid;

use super::{BlobKey, BlobLocation, LayerRole, Repository, RepositoryError, RepositoryResult};
use crate::types::{ManagedLayer, OverlaybdLayerRef};

/// Writes a volume's runtime image config for its stored backing layers.
pub(crate) async fn materialize_volume_backing(
    repository: &dyn Repository,
    layers: &[OverlaybdLayerRef],
    destination: &Path,
) -> RepositoryResult<PathBuf> {
    let managed_repo_blob_url = repository.managed_layers_repo_blob_url();
    let image_config = ImageConfig {
        lowers: layers
            .iter()
            .map(|layer| {
                Ok(match layer {
                    OverlaybdLayerRef::Managed(layer) => LayerConfig {
                        repo_blob_url: managed_repo_blob_url.clone().unwrap_or_default(),
                        ..managed_layer_config(repository, layer.clone(), LayerRole::Rootfs, false)?
                    },
                    OverlaybdLayerRef::External(layer) => LayerConfig {
                        repo_blob_url: layer.repo_blob_url.clone(),
                        digest: layer.digest.clone(),
                        size: layer.size,
                        ..LayerConfig::default()
                    },
                })
            })
            .collect::<RepositoryResult<_>>()?,
        ..ImageConfig::default()
    };
    let parent = destination
        .parent()
        .ok_or_else(|| RepositoryError::Backend {
            message: format!(
                "volume image config '{}' has no parent",
                destination.display()
            ),
            source: None,
        })?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        RepositoryError::backend(
            format!("create volume image config dir '{}'", parent.display()),
            error,
        )
    })?;
    let bytes = serde_json::to_vec_pretty(&image_config)
        .map_err(|error| RepositoryError::backend("serialize volume image config", error))?;
    let temporary = destination.with_extension(format!("tmp-{}", Uuid::now_v7().simple()));
    if let Err(error) = tokio::fs::write(&temporary, bytes).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(RepositoryError::backend(
            format!(
                "write temporary volume image config '{}'",
                temporary.display()
            ),
            error,
        ));
    }
    if let Err(error) = tokio::fs::rename(&temporary, destination).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(RepositoryError::backend(
            format!("install volume image config '{}'", destination.display()),
            error,
        ));
    }
    Ok(destination.to_path_buf())
}

/// A managed layer read in place when the repository allows it; otherwise
/// overlaybd reads it by digest from the repository's `repoBlobUrl`.
pub(crate) fn managed_layer_config(
    repository: &dyn Repository,
    layer: ManagedLayer,
    role: LayerRole,
    isolated: bool,
) -> RepositoryResult<LayerConfig> {
    let key = BlobKey::layer(layer.digest.clone(), role, isolated);
    let file = match repository.locate_blob(&key)? {
        BlobLocation::Local(path) => path.display().to_string(),
        BlobLocation::Remote(_) => String::new(),
    };
    Ok(LayerConfig {
        file,
        digest: layer.digest,
        size: layer.size,
        uuid: layer.uuid.unwrap_or_default(),
        ..LayerConfig::default()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store_operator::CredentialSource;

    use super::*;
    use crate::cfg::{SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
    use crate::repository::backends::oss::{OssClient, OssRepository};
    use crate::types::ExternalLayer;

    fn oss_repository() -> OssRepository {
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

    #[tokio::test]
    async fn materializes_volume_backing_with_backend_layer_urls() {
        let temp = tempfile::tempdir().expect("tempdir");
        let repository = oss_repository();
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

        materialize_volume_backing(&repository, &layers, &destination)
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
