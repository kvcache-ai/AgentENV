use std::path::PathBuf;

use crate::digest::FileDigest;
use crate::layer::LayerChain;
use crate::repository::{RepositoryError, RepositoryResult};
use overlaybd::config::{ImageConfig, LayerConfig};
use overlaybd::layer::should_dense_export_layer;
use url::Url;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceRegistryImage {
    pub(crate) target: SourceRegistryRepository,
    pub(crate) layers: Vec<SourceRegistryLayer>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceRegistryRepository {
    pub(crate) registry: String,
    pub(crate) repository: String,
    pub(crate) repo_blob_url: String,
}

impl SourceRegistryRepository {
    pub(crate) fn parse(repo_blob_url: &str) -> RepositoryResult<Self> {
        let url = Url::parse(repo_blob_url).map_err(|e| RepositoryError::Unsupported {
            feature: format!("invalid ACR repoBlobUrl '{repo_blob_url}': {e}"),
        })?;
        let test_loopback_http = cfg!(test)
            && url.scheme() == "http"
            && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1"));
        if url.scheme() != "https" && !test_loopback_http {
            return Err(RepositoryError::Unsupported {
                feature: format!("ACR repoBlobUrl must use https: {repo_blob_url}"),
            });
        }
        let host = url.host_str().ok_or_else(|| RepositoryError::Unsupported {
            feature: format!("ACR repoBlobUrl is missing registry host: {repo_blob_url}"),
        })?;
        // Strip the default HTTPS :443 so equivalent source URLs normalize to
        // one registry; keep every other explicit port intact.
        let registry = match url.port() {
            Some(443) if url.scheme() == "https" => host.to_string(),
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };

        let segments: Vec<&str> = url
            .path_segments()
            .map(|segments| segments.collect())
            .unwrap_or_default();
        if segments.len() < 3
            || segments.first() != Some(&"v2")
            || segments.last() != Some(&"blobs")
        {
            return Err(RepositoryError::Unsupported {
                feature: format!(
                    "ACR repoBlobUrl must have shape https://<registry>/v2/<repo>/blobs: {repo_blob_url}"
                ),
            });
        }
        let repository = segments[1..segments.len() - 1].join("/");
        if repository.is_empty() {
            return Err(RepositoryError::Unsupported {
                feature: format!("ACR repoBlobUrl repository is empty: {repo_blob_url}"),
            });
        }
        let repo_blob_url = format!(
            "{}://{}{}",
            url.scheme(),
            registry,
            url.path().trim_end_matches('/')
        );

        Ok(Self {
            registry,
            repository,
            repo_blob_url,
        })
    }

    pub(crate) fn image_ref(&self, tag: &str) -> String {
        format!("{}/{}:{tag}", self.registry, self.repository)
    }

    fn registry_api_url(&self) -> String {
        let scheme = self
            .repo_blob_url
            .split_once("://")
            .map(|(scheme, _)| scheme)
            .unwrap_or("https");
        format!("{scheme}://{}", self.registry)
    }

    pub(crate) fn upload_url(&self) -> String {
        format!(
            "{}/v2/{}/blobs/uploads/",
            self.registry_api_url(),
            self.repository
        )
    }

    pub(crate) fn manifest_url(&self, tag: &str) -> String {
        format!(
            "{}/v2/{}/manifests/{tag}",
            self.registry_api_url(),
            self.repository
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SourceRegistryLayer {
    Remote { digest: String, size: u64 },
    LocalDelta(LocalSnapshotDelta),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalSnapshotDelta {
    pub(crate) path: PathBuf,
    pub(crate) descriptor: LocalSnapshotDeltaDescriptor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LocalSnapshotDeltaDescriptor {
    Raw { digest: String, size: u64 },
    DenseOverlaybd,
}

pub(crate) fn is_remote_reference(layer: &LayerConfig) -> bool {
    layer.file.is_empty() || !layer.repo_blob_url.is_empty()
}

pub(crate) fn source_registry_image(chain: &LayerChain) -> RepositoryResult<SourceRegistryImage> {
    let mut image = chain.to_runtime_config();
    if image.repo_blob_url.is_empty() {
        if let Some(url) = image
            .lowers
            .iter()
            .map(|layer| layer.repo_blob_url.as_str())
            .find(|url| !url.is_empty())
        {
            image.repo_blob_url = url.to_string();
        }
    }
    source_registry_image_from_config(image)
}

fn source_registry_image_from_config(
    image_config: ImageConfig,
) -> RepositoryResult<SourceRegistryImage> {
    if image_config.repo_blob_url.is_empty() {
        return Err(RepositoryError::Unsupported {
            feature:
                "ACR disk image export requires a single image-level source registry repoBlobUrl"
                    .to_string(),
        });
    }
    let repository_ref = SourceRegistryRepository::parse(&image_config.repo_blob_url)?;
    let mut layers = Vec::with_capacity(image_config.lowers.len());
    let mut saw_remote = false;
    let mut saw_local = false;

    for (index, layer) in image_config.lowers.iter().enumerate() {
        reject_unsupported_shape(index, layer)?;
        if !is_remote_reference(layer) {
            saw_local = true;
            let path = std::fs::canonicalize(&layer.file).map_err(|e| {
                RepositoryError::backend(
                    format!("canonicalize local ACR delta '{}'", layer.file),
                    e,
                )
            })?;
            let descriptor = if !layer.digest.is_empty() && layer.size > 0 {
                LocalSnapshotDeltaDescriptor::Raw {
                    digest: layer.digest.clone(),
                    size: layer.size,
                }
            } else if should_dense_export_layer(&path) {
                LocalSnapshotDeltaDescriptor::DenseOverlaybd
            } else {
                let descriptor = FileDigest::describe_blocking(&path).map_err(|e| {
                    RepositoryError::backend(
                        format!("describe local ACR delta '{}'", path.display()),
                        e,
                    )
                })?;
                LocalSnapshotDeltaDescriptor::Raw {
                    digest: descriptor.sha256,
                    size: descriptor.size,
                }
            };
            layers.push(SourceRegistryLayer::LocalDelta(LocalSnapshotDelta {
                path,
                descriptor,
            }));
            continue;
        }

        if !layer.repo_blob_url.is_empty()
            && SourceRegistryRepository::parse(&layer.repo_blob_url)? != repository_ref
        {
            return Err(RepositoryError::Unsupported {
                feature: format!(
                    "ACR layer {index} belongs to a different source registry repository: per-layer repoBlobUrl outside the source registry repository"
                ),
            });
        }
        if saw_local {
            return Err(RepositoryError::Unsupported {
                feature: "ACR disk image export requires all remote lowers before local deltas"
                    .to_string(),
            });
        }
        saw_remote = true;
        layers.push(SourceRegistryLayer::Remote {
            digest: layer.digest.clone(),
            size: layer.size,
        });
    }

    if !saw_remote {
        return Err(RepositoryError::Unsupported {
            feature: "ACR disk image export requires at least one remote source-registry lower"
                .to_string(),
        });
    }

    Ok(SourceRegistryImage {
        target: repository_ref,
        layers,
    })
}

fn reject_unsupported_shape(index: usize, layer: &LayerConfig) -> RepositoryResult<()> {
    if !layer.target_file.is_empty()
        || !layer.target_digest.is_empty()
        || !layer.gzip_index.is_empty()
    {
        return Err(RepositoryError::Unsupported {
            feature: format!(
                "ACR disk image export does not support complex overlaybd lower {index}"
            ),
        });
    }

    if !is_remote_reference(layer) {
        if !layer.dir.is_empty() {
            return Err(RepositoryError::Unsupported {
                feature: format!(
                    "ACR disk image export does not support local lower {index} with dir cache hint"
                ),
            });
        }
        return Ok(());
    }

    if layer.digest.is_empty() {
        return Err(RepositoryError::Unsupported {
            feature: format!("remote ACR lower {index} is missing digest"),
        });
    }
    if layer.size == 0 {
        return Err(RepositoryError::Unsupported {
            feature: format!("remote ACR lower {index} is missing size"),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use overlaybd::backend::local::LocalFile;
    use overlaybd::index_file::LSMTFile;
    use overlaybd::layer::LayerFile;
    use overlaybd::virtual_file::VirtualFile;
    use tempfile::TempDir;

    use super::*;
    use crate::layer::{LayerRef, LayerSource, ManagedLayerRef};

    fn local_delta(path: PathBuf, digest: &str, size: u64) -> LayerSource {
        LayerSource::File(LayerFile::dense(
            path,
            uuid::Uuid::nil(),
            Some(overlaybd::LayerDescriptor {
                digest: digest.into(),
                size,
            }),
        ))
    }

    #[test]
    fn rejects_source_without_remote_base() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        fs::write(&delta, b"delta").unwrap();
        let layers = LayerChain::from(vec![local_delta(delta, "sha256:delta", 5)]);

        assert!(matches!(
            source_registry_image(&layers),
            Err(RepositoryError::Unsupported { .. })
        ));
    }

    #[test]
    fn chained_snapshot_reuses_remote_delta_and_uploads_only_new_local_delta() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        fs::write(&delta, b"delta2").unwrap();
        let layers = LayerChain {
            layers: vec![
                LayerSource::Referenced {
                    reference: LayerRef::External(crate::layer::ExternalLayer {
                        digest: "sha256:base".into(),
                        size: 123,
                        repo_blob_url: "https://registry.example/v2/ns/base/blobs".into(),
                    }),
                    local_file: Some(dir.path().join("cached-base.commit")),
                },
                LayerSource::Referenced {
                    reference: LayerRef::Managed(ManagedLayerRef {
                        digest: "sha256:d1".into(),
                        size: 456,
                        uuid: None,
                    }),
                    local_file: Some(dir.path().join("cached-delta.commit")),
                },
                local_delta(delta.clone(), "sha256:d2", 6),
            ],
            managed_repo_blob_url: Some("https://registry.example/v2/ns/base/blobs".into()),
        };

        let source_image = source_registry_image(&layers).unwrap();
        assert_eq!(source_image.target.repository, "ns/base");
        assert_eq!(
            source_image.layers,
            vec![
                SourceRegistryLayer::Remote {
                    digest: "sha256:base".to_string(),
                    size: 123
                },
                SourceRegistryLayer::Remote {
                    digest: "sha256:d1".to_string(),
                    size: 456
                },
                SourceRegistryLayer::LocalDelta(LocalSnapshotDelta {
                    path: delta.canonicalize().unwrap(),
                    descriptor: LocalSnapshotDeltaDescriptor::Raw {
                        digest: "sha256:d2".to_string(),
                        size: 6,
                    },
                }),
            ]
        );
    }

    #[tokio::test]
    async fn plans_descriptorless_sparse_overlaybd_delta_for_dense_export() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        let data_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&delta).unwrap());
        let lsmt = LSMTFile::create(data_file, None, 64 * 1024, true)
            .await
            .unwrap();
        lsmt.write_at(0, &[0xAB; 4096]).await.unwrap();
        lsmt.close_seal().await.unwrap();
        drop(lsmt);

        let layers = LayerChain::from(vec![
            LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:base".into(),
                    size: 123,
                    repo_blob_url: "https://registry.example/v2/ns/base/blobs".into(),
                }),
                local_file: None,
            },
            LayerSource::File(LayerFile::from_path(delta.clone(), None).await.unwrap()),
        ]);

        let source_image = source_registry_image(&layers).unwrap();
        assert_eq!(
            source_image.layers,
            vec![
                SourceRegistryLayer::Remote {
                    digest: "sha256:base".to_string(),
                    size: 123
                },
                SourceRegistryLayer::LocalDelta(LocalSnapshotDelta {
                    path: delta.canonicalize().unwrap(),
                    descriptor: LocalSnapshotDeltaDescriptor::DenseOverlaybd,
                }),
            ]
        );
    }

    #[test]
    fn rejects_mixed_source_registry_repositories() {
        let layers = LayerChain::from(vec![
            LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:base".into(),
                    size: 123,
                    repo_blob_url: "https://registry.example/v2/ns/base/blobs".into(),
                }),
                local_file: None,
            },
            LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:other".into(),
                    size: 456,
                    repo_blob_url: "https://registry.example/v2/ns/other/blobs".into(),
                }),
                local_file: None,
            },
        ]);

        let err =
            source_registry_image(&layers).expect_err("mixed repositories must be unsupported");
        assert!(matches!(err, RepositoryError::Unsupported { .. }));
        assert!(err
            .to_string()
            .contains("different source registry repository"));
    }

    #[test]
    fn rejects_remote_lower_after_local_delta() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        fs::write(&delta, b"delta").unwrap();
        let layers = LayerChain::from(vec![
            LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:base".into(),
                    size: 123,
                    repo_blob_url: "https://registry.example/v2/ns/base/blobs".into(),
                }),
                local_file: None,
            },
            local_delta(delta, "sha256:delta", 5),
            LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:later".into(),
                    size: 456,
                    repo_blob_url: "https://registry.example/v2/ns/base/blobs".into(),
                }),
                local_file: None,
            },
        ]);

        assert!(matches!(
            source_registry_image(&layers),
            Err(RepositoryError::Unsupported { .. })
        ));
    }
}
