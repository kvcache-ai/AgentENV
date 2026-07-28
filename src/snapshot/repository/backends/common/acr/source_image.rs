use std::path::{Path, PathBuf};

use crate::digest::FileDigest;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use overlaybd::config::{load_image_config as load_overlaybd_image_config, LayerConfig};
use overlaybd::dense_export;
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

pub(crate) fn load_source_registry_image(
    image_config_path: &Path,
) -> RepositoryResult<SourceRegistryImage> {
    let image_config = load_overlaybd_image_config(image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "load overlaybd image config '{}'",
                image_config_path.display()
            ),
            e,
        )
    })?;

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
        if !layer.file.is_empty() {
            saw_local = true;
            let path = std::fs::canonicalize(&layer.file).map_err(|e| {
                RepositoryError::backend(
                    format!("canonicalize local ACR delta '{}'", layer.file),
                    e,
                )
            })?;
            if !crate::image::local_layer::rootfs_layer_is_runtime_generated_delta(&path) {
                return Err(RepositoryError::Unsupported {
                    feature: format!(
                        "ACR disk image export only supports local runtime-generated delta lowers, got '{}'",
                        path.display()
                    ),
                });
            }
            let descriptor = if !layer.digest.is_empty() && layer.size > 0 {
                LocalSnapshotDeltaDescriptor::Raw {
                    digest: layer.digest.clone(),
                    size: layer.size,
                }
            } else if dense_export::should_dense_export_layer(&path) {
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

    if !layer.file.is_empty() {
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
    if !layer.repo_blob_url.is_empty() {
        return Err(RepositoryError::Unsupported {
            feature: format!(
                "ACR disk image export does not support per-layer repoBlobUrl on remote lower {index}"
            ),
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
    use overlaybd::transient_io_ring::shared_transient_io_ring;
    use overlaybd::virtual_file::VirtualFile;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    fn write_image(dir: &TempDir, lowers: serde_json::Value, repo_blob_url: &str) -> PathBuf {
        let path = dir.path().join("image.json");
        fs::write(
            &path,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": repo_blob_url,
                "lowers": lowers,
                "upper": {},
                "resultFile": ""
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    #[test]
    fn rejects_source_without_remote_base() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        fs::write(&delta, b"delta").unwrap();
        let image = write_image(
            &dir,
            json!([{"file": delta, "digest": "sha256:delta", "size": 5}]),
            "https://registry.example/v2/ns/base/blobs",
        );

        assert!(matches!(
            load_source_registry_image(&image),
            Err(RepositoryError::Unsupported { .. })
        ));
    }

    #[test]
    fn chained_snapshot_reuses_remote_delta_and_uploads_only_new_local_delta() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        fs::write(&delta, b"delta2").unwrap();
        let image = write_image(
            &dir,
            json!([
                {"digest":"sha256:base","size":123},
                {"digest":"sha256:d1","size":456},
                {"file": delta, "digest": "sha256:d2", "size": 6}
            ]),
            "https://registry.example/v2/ns/base/blobs",
        );

        let source_image = load_source_registry_image(&image).unwrap();
        assert_eq!(
            source_image.layers,
            vec![
                SourceRegistryLayer::Remote {
                    digest: "sha256:base".to_string(),
                    size: 123,
                },
                SourceRegistryLayer::Remote {
                    digest: "sha256:d1".to_string(),
                    size: 456,
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

    #[test]
    fn fills_descriptor_for_local_snapshot_delta() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        fs::write(&delta, b"delta2").unwrap();
        let image = write_image(
            &dir,
            json!([
                {"digest":"sha256:base","size":123},
                {"file": delta}
            ]),
            "https://registry.example/v2/ns/base/blobs",
        );
        let descriptor = FileDigest::describe_blocking(&delta).unwrap();

        let source_image = load_source_registry_image(&image).unwrap();
        assert_eq!(
            source_image.layers,
            vec![
                SourceRegistryLayer::Remote {
                    digest: "sha256:base".to_string(),
                    size: 123,
                },
                SourceRegistryLayer::LocalDelta(LocalSnapshotDelta {
                    path: delta.canonicalize().unwrap(),
                    descriptor: LocalSnapshotDeltaDescriptor::Raw {
                        digest: descriptor.sha256,
                        size: descriptor.size,
                    },
                }),
            ]
        );
    }

    #[tokio::test]
    async fn plans_descriptorless_sparse_overlaybd_delta_for_dense_export() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        let data_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&delta, shared_transient_io_ring())
                .await
                .unwrap(),
        );
        let lsmt = LSMTFile::create(data_file, None, 64 * 1024, true)
            .await
            .unwrap();
        lsmt.write_at(0, &[0xAB; 4096]).await.unwrap();
        lsmt.close_seal().await.unwrap();
        drop(lsmt);

        let image = write_image(
            &dir,
            json!([
                {"digest":"sha256:base","size":123},
                {"file": delta}
            ]),
            "https://registry.example/v2/ns/base/blobs",
        );

        let source_image = load_source_registry_image(&image).unwrap();
        assert_eq!(
            source_image.layers,
            vec![
                SourceRegistryLayer::Remote {
                    digest: "sha256:base".to_string(),
                    size: 123,
                },
                SourceRegistryLayer::LocalDelta(LocalSnapshotDelta {
                    path: delta.canonicalize().unwrap(),
                    descriptor: LocalSnapshotDeltaDescriptor::DenseOverlaybd,
                }),
            ]
        );
    }

    #[test]
    fn accepts_all_remote_source_registry_image_without_local_delta() {
        let dir = TempDir::new().unwrap();
        let image = write_image(
            &dir,
            json!([
                {"digest":"sha256:base","size":123,"dir":"/var/lib/agentenv/image-cache/commits/sha256-base"},
                {"digest":"sha256:d1","size":456,"dir":"/var/lib/agentenv/image-cache/commits/sha256-d1"}
            ]),
            "https://registry.example/v2/ns/base/blobs",
        );

        let source_image = load_source_registry_image(&image).unwrap();
        assert_eq!(
            source_image.layers,
            vec![
                SourceRegistryLayer::Remote {
                    digest: "sha256:base".to_string(),
                    size: 123,
                },
                SourceRegistryLayer::Remote {
                    digest: "sha256:d1".to_string(),
                    size: 456,
                },
            ]
        );
    }

    #[test]
    fn rejects_per_layer_repo_blob_url_for_source_registry_export() {
        let dir = TempDir::new().unwrap();
        let image = write_image(
            &dir,
            json!([
                {
                    "digest":"sha256:base",
                    "size":123,
                    "repoBlobUrl":"https://registry.example/v2/ns/other/blobs"
                }
            ]),
            "https://registry.example/v2/ns/base/blobs",
        );

        let err = load_source_registry_image(&image)
            .expect_err("per-layer repoBlobUrl should be unsupported");
        assert!(matches!(err, RepositoryError::Unsupported { .. }));
        assert!(err.to_string().contains("per-layer repoBlobUrl"));
    }

    #[test]
    fn rejects_missing_image_level_repo_blob_url_for_source_registry_export() {
        let dir = TempDir::new().unwrap();
        let image = write_image(
            &dir,
            json!([
                {
                    "digest":"sha256:base",
                    "size":123,
                    "repoBlobUrl":"https://registry.example/v2/ns/base/blobs"
                }
            ]),
            "",
        );

        let err = load_source_registry_image(&image)
            .expect_err("image-level repoBlobUrl should be required");
        assert!(matches!(err, RepositoryError::Unsupported { .. }));
        assert!(err.to_string().contains("single image-level"));
    }

    #[test]
    fn plans_multiple_local_snapshot_deltas_after_remote_lowers() {
        let dir = TempDir::new().unwrap();
        let first_dir = dir.path().join("first");
        let second_dir = dir.path().join("second");
        fs::create_dir_all(&first_dir).unwrap();
        fs::create_dir_all(&second_dir).unwrap();
        let first_delta = first_dir.join("snapshot.commit");
        let second_delta = second_dir.join("snapshot.commit");
        fs::write(&first_delta, b"delta1").unwrap();
        fs::write(&second_delta, b"delta2").unwrap();
        let image = write_image(
            &dir,
            json!([
                {"digest":"sha256:base","size":123},
                {"file": first_delta, "digest": "sha256:first", "size": 6},
                {"file": second_delta, "digest": "sha256:second", "size": 6}
            ]),
            "https://registry.example/v2/ns/base/blobs",
        );

        let source_image = load_source_registry_image(&image).unwrap();
        assert_eq!(
            source_image.layers,
            vec![
                SourceRegistryLayer::Remote {
                    digest: "sha256:base".to_string(),
                    size: 123,
                },
                SourceRegistryLayer::LocalDelta(LocalSnapshotDelta {
                    path: first_dir.join("snapshot.commit").canonicalize().unwrap(),
                    descriptor: LocalSnapshotDeltaDescriptor::Raw {
                        digest: "sha256:first".to_string(),
                        size: 6,
                    },
                }),
                SourceRegistryLayer::LocalDelta(LocalSnapshotDelta {
                    path: second_dir.join("snapshot.commit").canonicalize().unwrap(),
                    descriptor: LocalSnapshotDeltaDescriptor::Raw {
                        digest: "sha256:second".to_string(),
                        size: 6,
                    },
                }),
            ]
        );
    }

    #[test]
    fn rejects_remote_lower_after_local_delta() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        fs::write(&delta, b"delta").unwrap();
        let image = write_image(
            &dir,
            json!([
                {"digest":"sha256:base","size":123},
                {"file": delta, "digest": "sha256:delta", "size": 5},
                {"digest":"sha256:later","size":456}
            ]),
            "https://registry.example/v2/ns/base/blobs",
        );

        assert!(matches!(
            load_source_registry_image(&image),
            Err(RepositoryError::Unsupported { .. })
        ));
    }
}
