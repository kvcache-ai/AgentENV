use crate::layer::{ExternalLayer, LayerChain, LayerRef, LayerSource};
use crate::repository::{RepositoryError, RepositoryResult};
use overlaybd::layer::LayerFile;
use overlaybd::LayerDescriptor;
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
    LocalDelta(LayerFile),
}

pub(crate) fn source_registry_image(chain: &LayerChain) -> RepositoryResult<SourceRegistryImage> {
    let mut target: Option<SourceRegistryRepository> = None;
    let mut layers = Vec::with_capacity(chain.layers.len());
    let mut saw_remote = false;
    let mut saw_local = false;

    for (index, layer) in chain.layers.iter().enumerate() {
        match layer {
            LayerSource::File(file) => {
                saw_local = true;
                let mut local = file.clone();
                local.path = std::fs::canonicalize(&file.path).map_err(|e| {
                    RepositoryError::backend(
                        format!("canonicalize local ACR delta '{}'", file.path.display()),
                        e,
                    )
                })?;
                layers.push(SourceRegistryLayer::LocalDelta(local));
            }
            LayerSource::Referenced {
                reference:
                    LayerRef::External(ExternalLayer {
                        digest,
                        size,
                        repo_blob_url,
                    }),
                ..
            } => {
                validate_remote_lower(index, digest, *size, repo_blob_url, &mut target, saw_local)?;
                saw_remote = true;
                layers.push(SourceRegistryLayer::Remote {
                    digest: digest.clone(),
                    size: *size,
                });
            }
            LayerSource::Referenced {
                reference: LayerRef::Managed(managed),
                local_file,
            } => {
                if let Some(repo_blob_url) = chain
                    .managed_repo_blob_url
                    .as_deref()
                    .filter(|url| !url.is_empty())
                {
                    validate_remote_lower(
                        index,
                        &managed.digest,
                        managed.size,
                        repo_blob_url,
                        &mut target,
                        saw_local,
                    )?;
                    saw_remote = true;
                    layers.push(SourceRegistryLayer::Remote {
                        digest: managed.digest.clone(),
                        size: managed.size,
                    });
                } else if let Some(path) = local_file {
                    saw_local = true;
                    let canonical = std::fs::canonicalize(path).map_err(|e| {
                        RepositoryError::backend(
                            format!("canonicalize local ACR delta '{}'", path.display()),
                            e,
                        )
                    })?;
                    let uuid = managed
                        .uuid
                        .as_deref()
                        .and_then(|value| uuid::Uuid::parse_str(value).ok())
                        .unwrap_or_else(uuid::Uuid::nil);
                    let descriptor =
                        (!managed.digest.is_empty() && managed.size > 0).then(|| LayerDescriptor {
                            digest: managed.digest.clone(),
                            size: managed.size,
                        });
                    layers.push(SourceRegistryLayer::LocalDelta(LayerFile::dense(
                        canonical, uuid, descriptor,
                    )));
                } else {
                    return Err(RepositoryError::Unsupported {
                        feature:
                            "ACR disk image export requires a single image-level source registry repoBlobUrl"
                                .to_string(),
                    });
                }
            }
        }
    }

    let Some(target) = target.filter(|_| saw_remote) else {
        return Err(RepositoryError::Unsupported {
            feature: "ACR disk image export requires at least one remote source-registry lower"
                .to_string(),
        });
    };

    Ok(SourceRegistryImage { target, layers })
}

fn validate_remote_lower(
    index: usize,
    digest: &str,
    size: u64,
    repo_blob_url: &str,
    target: &mut Option<SourceRegistryRepository>,
    saw_local: bool,
) -> RepositoryResult<()> {
    if repo_blob_url.is_empty() {
        return Err(RepositoryError::Unsupported {
            feature:
                "ACR disk image export requires a single image-level source registry repoBlobUrl"
                    .to_string(),
        });
    }
    if digest.is_empty() {
        return Err(RepositoryError::Unsupported {
            feature: format!("remote ACR lower {index} is missing digest"),
        });
    }
    if size == 0 {
        return Err(RepositoryError::Unsupported {
            feature: format!("remote ACR lower {index} is missing size"),
        });
    }
    let parsed = SourceRegistryRepository::parse(repo_blob_url)?;
    match target {
        Some(existing) if existing != &parsed => {
            return Err(RepositoryError::Unsupported {
                feature: format!(
                    "ACR layer {index} belongs to a different source registry repository: per-layer repoBlobUrl outside the source registry repository"
                ),
            });
        }
        None => *target = Some(parsed),
        Some(_) => {}
    }
    if saw_local {
        return Err(RepositoryError::Unsupported {
            feature: "ACR disk image export requires all remote lowers before local deltas"
                .to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
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
                SourceRegistryLayer::LocalDelta(LayerFile::dense(
                    delta.canonicalize().unwrap(),
                    uuid::Uuid::nil(),
                    Some(LayerDescriptor {
                        digest: "sha256:d2".to_string(),
                        size: 6,
                    }),
                )),
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

        let inspected = LayerFile::from_path(delta.clone(), None).await.unwrap();
        assert!(inspected.is_sparse_rw());
        let layers = LayerChain::from(vec![
            LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:base".into(),
                    size: 123,
                    repo_blob_url: "https://registry.example/v2/ns/base/blobs".into(),
                }),
                local_file: None,
            },
            LayerSource::File(inspected.clone()),
        ]);

        let source_image = source_registry_image(&layers).unwrap();
        assert_eq!(source_image.layers.len(), 2);
        assert_eq!(
            source_image.layers[0],
            SourceRegistryLayer::Remote {
                digest: "sha256:base".to_string(),
                size: 123
            }
        );
        match &source_image.layers[1] {
            SourceRegistryLayer::LocalDelta(file) => {
                assert_eq!(file.path, delta.canonicalize().unwrap());
                assert_eq!(file.uuid, inspected.uuid);
                assert!(file.descriptor.is_none());
                assert!(file.is_sparse_rw());
            }
            other => panic!("expected local sparse delta, got {other:?}"),
        }
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
