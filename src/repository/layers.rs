//! Repository-level OverlayBD layer-chain resolution, materialization, and
//! content-addressed storage shared by snapshots, volumes, and published images.

use std::path::{Path, PathBuf};

use overlaybd::config::OverlaybdCompactOutput;

use super::{
    BlobBody, BlobKey, BlobLocation, LayerRole, Repository, RepositoryError, RepositoryResult,
};
use crate::layer::{
    load_layer_chain_from_config, prepare_layer_file_upload, write_image_config, LayerChain,
    LayerRef, LayerSource, ManagedLayerRef, PreparedLayerUpload,
};

pub(crate) fn same_repo_blob_url(left: &str, right: &str) -> bool {
    !left.is_empty() && left.trim_end_matches('/') == right.trim_end_matches('/')
}

/// Resolve committed [`LayerRef`] entries against `repository` into a
/// [`LayerChain`] carrying any in-place local paths and the repository's
/// remote `repoBlobUrl` for managed layers.
pub(crate) fn layer_chain_for_refs(
    repository: &dyn Repository,
    references: &[LayerRef],
    role: LayerRole,
) -> RepositoryResult<LayerChain> {
    let layers = references
        .iter()
        .cloned()
        .map(|reference| {
            let local_file = match &reference {
                LayerRef::Managed(layer) => match repository.locate_blob(&BlobKey::Layer {
                    digest: layer.digest.clone(),
                    role,
                })? {
                    BlobLocation::Local(path) => Some(path),
                    BlobLocation::Remote(_) => None,
                },
                LayerRef::External(_) => None,
            };
            Ok(LayerSource::Referenced {
                reference,
                local_file,
            })
        })
        .collect::<RepositoryResult<Vec<_>>>()?;
    Ok(LayerChain {
        layers,
        managed_repo_blob_url: repository.managed_layers_repo_blob_url(),
    })
}

/// Atomically writes a runtime OverlayBD image config at `destination` for
/// stored backing layers (used by volumes and repository-backed images).
pub(crate) async fn materialize_layer_config(
    repository: &dyn Repository,
    layers: &[LayerRef],
    destination: &Path,
) -> RepositoryResult<PathBuf> {
    let chain = layer_chain_for_refs(repository, layers, LayerRole::Rootfs)?;
    let image_config = chain.to_runtime_config();
    let parent = destination
        .parent()
        .ok_or_else(|| RepositoryError::Backend {
            message: format!(
                "layer image config '{}' has no parent",
                destination.display()
            ),
            source: None,
        })?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        RepositoryError::backend(
            format!("create layer image config dir '{}'", parent.display()),
            error,
        )
    })?;
    let dest = destination.to_path_buf();
    tokio::task::spawn_blocking(move || {
        write_image_config(&dest, &image_config, "layer image config")
    })
    .await
    .map_err(|error| RepositoryError::backend("write layer image config task", error))?
    .map_err(|error| RepositoryError::backend("write layer image config", error))?;
    Ok(destination.to_path_buf())
}

/// Stores the node-local layers described by an OverlayBD image config into
/// `repository`'s shared `managed-layers/` store and returns their references
/// alongside any temporary prepared upload files retained for restoration retry.
pub(crate) async fn store_disk_layers_with_uploads<R>(
    repository: &R,
    image_config_path: &Path,
) -> RepositoryResult<(Vec<LayerRef>, Vec<PreparedLayerUpload>)>
where
    R: Repository + ?Sized,
{
    let mut writer = RepositoryLayerWriter::new(repository);
    let layers = writer
        .store_disk_config(image_config_path, LayerRole::Rootfs)
        .await?;
    Ok((layers, writer.into_local_layers()))
}

/// Re-uploads any managed layers reported missing by the metadata directory
/// (e.g. reclaimed by a concurrent GC sweep between upload and SQL commit).
pub(crate) async fn restore_missing_managed_layers(
    repository: &dyn Repository,
    layers: &[&ManagedLayerRef],
    missing_uuids: &[String],
    local_layers: &[(&str, &Path)],
) -> RepositoryResult<()> {
    for &layer in layers {
        let uuid = layer
            .directory_uuid()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        if missing_uuids.contains(&uuid.to_string()) {
            let key = BlobKey::Layer {
                digest: layer.digest.clone(),
                role: LayerRole::Rootfs,
            };
            if !repository.has_blob(&key).await? {
                let (_, source) = local_layers
                    .iter()
                    .find(|(digest, _)| *digest == layer.digest)
                    .ok_or_else(|| RepositoryError::ManagedLayerNotFound {
                        digest: layer.digest.clone(),
                    })?;
                repository.put_blob(&key, BlobBody::File(source)).await?;
            }
        }
    }
    Ok(())
}

/// Stores the node-local layers described by an OverlayBD image config into
/// `repository`'s shared `managed-layers/` store and returns their references.
#[cfg(test)]
pub(crate) async fn store_disk_layers<R>(
    repository: &R,
    image_config_path: &Path,
) -> RepositoryResult<Vec<LayerRef>>
where
    R: Repository + ?Sized,
{
    store_disk_layers_with_uploads(repository, image_config_path)
        .await
        .map(|(layers, _)| layers)
}

/// Uploads and deduplicates OverlayBD [`LayerChain`] layers into a
/// [`Repository`], retaining any temporary prepared upload files for
/// post-commit consumers (such as P2P announcement).
pub(crate) struct RepositoryLayerWriter<'a, R: ?Sized> {
    repository: &'a R,
    compression: OverlaybdCompactOutput,
    managed_layers_repo_blob_url: Option<String>,
    local_layers: Vec<PreparedLayerUpload>,
}

impl<'a, R: Repository + ?Sized> RepositoryLayerWriter<'a, R> {
    pub(crate) fn new(repository: &'a R) -> Self {
        let policy = repository.publish_policy();
        Self {
            repository,
            compression: OverlaybdCompactOutput::from(&policy.compression),
            managed_layers_repo_blob_url: repository.managed_layers_repo_blob_url(),
            local_layers: Vec::new(),
        }
    }

    pub(crate) fn compression(&self) -> OverlaybdCompactOutput {
        self.compression
    }

    pub(crate) fn managed_layers_repo_blob_url(&self) -> Option<&str> {
        self.managed_layers_repo_blob_url.as_deref()
    }

    pub(crate) fn record_local_layers(
        &mut self,
        layers: impl IntoIterator<Item = PreparedLayerUpload>,
    ) {
        self.local_layers.extend(layers);
    }

    pub(crate) fn into_local_layers(self) -> Vec<PreparedLayerUpload> {
        self.local_layers
    }

    /// Volumes, built/pulled images, and legacy config inputs enter the same
    /// validated layer-chain adapter.
    pub(crate) async fn store_disk_config(
        &mut self,
        image_config_path: &Path,
        role: LayerRole,
    ) -> RepositoryResult<Vec<LayerRef>> {
        let layers = load_layer_chain_from_config(
            image_config_path,
            self.managed_layers_repo_blob_url.as_deref(),
        )
        .await
        .map_err(|error| RepositoryError::backend("import layer configuration", error))?;
        self.store_disk_chain(&layers, role).await
    }

    pub(crate) async fn store_disk_chain(
        &mut self,
        chain: &LayerChain,
        role: LayerRole,
    ) -> RepositoryResult<Vec<LayerRef>> {
        let mut layers = Vec::with_capacity(chain.layers.len());
        for layer in &chain.layers {
            match layer {
                LayerSource::Referenced {
                    reference: LayerRef::External(external),
                    ..
                } => {
                    layers.push(LayerRef::External(external.clone()));
                }
                layer => layers.push(LayerRef::Managed(
                    self.store_source(layer.clone(), role, chain.managed_repo_blob_url.as_deref())
                        .await?,
                )),
            }
        }
        Ok(layers)
    }

    pub(crate) async fn store_memory_chain(
        &mut self,
        chain: &LayerChain,
    ) -> RepositoryResult<Vec<ManagedLayerRef>> {
        let mut layers = Vec::with_capacity(chain.layers.len());
        for layer in &chain.layers {
            layers.push(
                self.store_source(
                    layer.clone(),
                    LayerRole::Memory,
                    chain.managed_repo_blob_url.as_deref(),
                )
                .await?,
            );
        }
        Ok(layers)
    }

    async fn store_source(
        &mut self,
        source: LayerSource,
        role: LayerRole,
        managed_repo: Option<&str>,
    ) -> RepositoryResult<ManagedLayerRef> {
        let file = match source {
            LayerSource::Referenced {
                reference: LayerRef::Managed(metadata),
                local_file,
            } => {
                let key = BlobKey::Layer {
                    digest: metadata.digest.clone(),
                    role,
                };
                match self.repository.locate_blob(&key)? {
                    BlobLocation::Remote(_) => {
                        let current = self
                            .repository
                            .managed_layers_repo_blob_url()
                            .unwrap_or_default();
                        if !managed_repo.is_some_and(|source| same_repo_blob_url(source, &current))
                        {
                            return Err(RepositoryError::Unsupported {
                                feature: "managed layer belongs to a different repository".into(),
                            });
                        }
                    }
                    BlobLocation::Local(destination) => {
                        if managed_repo.is_some() {
                            return Err(RepositoryError::Unsupported {
                                feature: "POSIX publication of a remote managed layer".into(),
                            });
                        }
                        let source = local_file.as_deref().unwrap_or(&destination);
                        if !source.is_file() {
                            return Err(RepositoryError::ArtifactNotFound {
                                artifact: format!("managed layer '{}'", metadata.digest),
                            });
                        }
                        match tokio::fs::metadata(&destination).await {
                            Ok(stored) if stored.len() != metadata.size => {
                                return Err(RepositoryError::IntegrityMismatch {
                                    artifact: format!("managed layer '{}'", metadata.digest),
                                    expected: metadata.size.to_string(),
                                    actual: stored.len().to_string(),
                                });
                            }
                            Ok(_) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => {
                                return Err(RepositoryError::backend(
                                    "read managed layer metadata",
                                    error,
                                ))
                            }
                        }
                        self.repository
                            .put_blob(&key, BlobBody::File(source))
                            .await?;
                    }
                }
                return Ok(metadata);
            }
            LayerSource::Referenced {
                reference: LayerRef::External(_),
                ..
            } => {
                return Err(RepositoryError::Unsupported {
                    feature: "external memory layers".into(),
                });
            }
            LayerSource::File(file) => file,
        };
        let staging_dir = self.repository.staging_dir();
        let (upload, uuid) =
            prepare_layer_file_upload(file, self.compression, move || match staging_dir {
                Some(dir) => {
                    std::fs::create_dir_all(&dir).map_err(|error| {
                        anyhow::Error::new(error)
                            .context(format!("create staging dir '{}'", dir.display()))
                    })?;
                    tempfile::NamedTempFile::new_in(&dir).map_err(|error| {
                        anyhow::Error::new(error)
                            .context(format!("create staging file in '{}'", dir.display()))
                    })
                }
                None => tempfile::NamedTempFile::new()
                    .map_err(|error| anyhow::Error::new(error).context("create staging file")),
            })
            .await
            .map_err(|error| RepositoryError::backend("prepare layer upload", error))?;
        self.put_prepared(upload, uuid, role).await
    }

    /// The layer's digest and size describe exactly the prepared (possibly
    /// recontainerized) bytes.
    async fn put_prepared(
        &mut self,
        upload: PreparedLayerUpload,
        uuid: Option<String>,
        role: LayerRole,
    ) -> RepositoryResult<ManagedLayerRef> {
        let layer = ManagedLayerRef {
            digest: upload.digest().to_string(),
            size: upload.size(),
            uuid,
        };
        let key = BlobKey::Layer {
            digest: layer.digest.clone(),
            role,
        };
        self.repository
            .put_blob(&key, BlobBody::File(upload.path()))
            .await?;
        self.local_layers.push(upload);
        Ok(layer)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store_operator::CredentialSource;
    use overlaybd::config::ImageConfig;
    use serde_json::json;

    use super::*;
    use crate::cfg::{SnapshotImageStoragePolicy, SnapshotPublishCompressionConfig};
    use crate::layer::ExternalLayer;
    use crate::repository::backends::oss::{OssClient, OssRepository};

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
    async fn materializes_layer_config_with_backend_layer_urls() {
        let temp = tempfile::tempdir().expect("tempdir");
        let repository = oss_repository();
        let destination = temp.path().join("volume/image.json");
        let layers = vec![
            LayerRef::Managed(ManagedLayerRef {
                digest: "sha256:managed".to_string(),
                size: 12,
                uuid: None,
            }),
            LayerRef::External(ExternalLayer {
                digest: "sha256:external".to_string(),
                repo_blob_url: "https://registry.example/v2/data/blobs".to_string(),
                size: 24,
            }),
        ];

        materialize_layer_config(&repository, &layers, &destination)
            .await
            .expect("materialize layer config");
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

    #[tokio::test]
    async fn derives_external_layers_from_layer_repo_blob_urls() {
        struct EmptyRepository;
        impl Repository for EmptyRepository {}

        let temp = tempfile::tempdir().expect("tempdir");
        let image = temp.path().join("image.json");
        std::fs::write(
            &image,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": "",
                "lowers": [
                    {
                        "digest": "sha256:base",
                        "size": 4096,
                        "repoBlobUrl": "https://registry.example/v2/ns/image/blobs"
                    },
                    {
                        "digest": "sha256:delta",
                        "size": 8192,
                        "repoBlobUrl": "s3://bucket/prefix/managed-layers"
                    }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }))
            .expect("serialize image config"),
        )
        .expect("write image config");

        let layers = store_disk_layers(&EmptyRepository, &image)
            .await
            .expect("derive layers");
        assert_eq!(
            layers,
            vec![
                LayerRef::External(ExternalLayer {
                    digest: "sha256:base".to_string(),
                    repo_blob_url: "https://registry.example/v2/ns/image/blobs".to_string(),
                    size: 4096,
                }),
                LayerRef::External(ExternalLayer {
                    digest: "sha256:delta".to_string(),
                    repo_blob_url: "s3://bucket/prefix/managed-layers".to_string(),
                    size: 8192,
                }),
            ]
        );
    }
}
