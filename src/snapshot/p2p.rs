use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::config::load_image_config as load_overlaybd_image_config;
use overlaybd::layer_metadata::read_overlaybd_layer_uuid;
use tracing::{debug, warn};
use uuid::Uuid;

use bytes::Bytes;

use crate::overlaybd::{layer_key_from_digest, layer_key_from_uuid, LayerMetadata};
use crate::p2p::{
    P2pArtifactKey, P2pPublishMode, P2pPublishRequest, P2pPublishSource, P2pTransport,
};
use crate::snapshot::SnapshotId;

const SNAPSHOT_P2P_KEY_PREFIX: &str = "snapshot/v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotP2pArtifact {
    pub(crate) key: P2pArtifactKey,
    pub(crate) source: P2pPublishSource,
    publish_mode: P2pPublishMode,
    metadata: serde_json::Value,
}

impl SnapshotP2pArtifact {
    pub(crate) fn fixed(
        snapshot_id: &SnapshotId,
        name: impl AsRef<str>,
        source: impl Into<PathBuf>,
    ) -> Self {
        let source = source.into();
        Self {
            key: fixed_artifact_key(snapshot_id, name),
            source: P2pPublishSource::Path(source),
            publish_mode: P2pPublishMode::Copy,
            metadata: serde_json::Value::Null,
        }
    }

    pub(crate) fn bytes(
        snapshot_id: &SnapshotId,
        name: impl AsRef<str>,
        source: impl Into<Bytes>,
    ) -> Self {
        Self {
            key: fixed_artifact_key(snapshot_id, name),
            source: P2pPublishSource::Bytes(source.into()),
            publish_mode: P2pPublishMode::Copy,
            metadata: serde_json::Value::Null,
        }
    }

    pub(crate) fn content_addressed_overlaybd_layer(
        source: impl Into<PathBuf>,
        sha256: impl Into<String>,
        size: u64,
    ) -> Self {
        let sha256 = sha256.into();
        let key = layer_key_from_digest(&sha256);
        let metadata = LayerMetadata::from_digest(sha256, Some(size), None).to_value();
        Self {
            key,
            source: P2pPublishSource::Path(source.into()),
            publish_mode: P2pPublishMode::Copy,
            metadata,
        }
    }

    pub(crate) fn uuid_overlaybd_layer(source: impl Into<PathBuf>, uuid: Uuid, size: u64) -> Self {
        let key = layer_key_from_uuid(&uuid);
        let metadata = LayerMetadata::from_uuid(uuid, Some(size)).to_value();
        Self {
            key,
            source: P2pPublishSource::Path(source.into()),
            publish_mode: P2pPublishMode::Copy,
            metadata,
        }
    }

    pub(crate) fn local_overlaybd_layers(
        image_config_path: &Path,
        committed_uuids: &HashSet<String>,
    ) -> Vec<Self> {
        let image_config = match load_overlaybd_image_config(image_config_path) {
            Ok(image_config) => image_config,
            Err(error) => {
                warn!(
                    path = %image_config_path.display(),
                    error = %error,
                    "skipping snapshot P2P layer publication because image config could not be loaded"
                );
                return Vec::new();
            }
        };

        image_config
            .lowers
            .into_iter()
            .flat_map(|layer| {
                if layer.file.is_empty() {
                    return Vec::new();
                }

                let mut artifacts = Vec::new();
                if !layer.digest.is_empty() && layer.size > 0 {
                    artifacts.push(Self::content_addressed_overlaybd_layer(
                        layer.file.clone(),
                        layer.digest,
                        layer.size,
                    ));
                }
                if committed_uuids.is_empty() {
                    return artifacts;
                }
                let path = PathBuf::from(&layer.file);
                let uuid = match read_overlaybd_layer_uuid(&path) {
                    Ok(uuid) if !uuid.is_nil() => uuid,
                    Ok(_) => {
                        warn!(
                            path = %path.display(),
                            "skipping snapshot P2P layer publication because overlaybd layer uuid is nil"
                        );
                        return artifacts;
                    }
                    Err(error) => {
                        warn!(
                            path = %path.display(),
                            error = %error,
                            "skipping snapshot P2P layer publication because overlaybd layer uuid could not be read"
                        );
                        return artifacts;
                    }
                };
                if !committed_uuids.contains(&uuid.to_string()) {
                    return artifacts;
                }
                let size = match std::fs::metadata(&path) {
                    Ok(metadata) if metadata.is_file() => metadata.len(),
                    Ok(_) => {
                        warn!(
                            path = %path.display(),
                            "skipping snapshot P2P layer publication because path is not a regular file"
                        );
                        return artifacts;
                    }
                    Err(error) => {
                        warn!(
                            path = %path.display(),
                            error = %error,
                            "skipping snapshot P2P layer publication because layer size could not be read"
                        );
                        return artifacts;
                    }
                };
                artifacts.push(Self::uuid_overlaybd_layer(path, uuid, size));
                artifacts
            })
            .collect()
    }

    pub(crate) async fn publish(&self, transport: &Arc<dyn P2pTransport>) -> Result<()> {
        let request = match &self.source {
            P2pPublishSource::Path(source) => {
                P2pPublishRequest::file(self.key.clone(), source.clone())
                    .with_publish_mode(self.publish_mode)
            }
            P2pPublishSource::Bytes(bytes) => {
                P2pPublishRequest::bytes(self.key.clone(), bytes.clone())
            }
        }
        .with_metadata(self.metadata.clone());

        transport
            .publish(&request)
            .await
            .with_context(|| format!("publish snapshot artifact '{}' to P2P", self.key))
    }
}

pub(crate) fn fixed_artifact_key(
    snapshot_id: &SnapshotId,
    name: impl AsRef<str>,
) -> P2pArtifactKey {
    format!(
        "{SNAPSHOT_P2P_KEY_PREFIX}/artifacts/{snapshot_id}/{}",
        name.as_ref()
    )
}

pub(crate) async fn fetch_artifact(
    transport: &Arc<dyn P2pTransport>,
    key: &P2pArtifactKey,
    destination: &Path,
) -> Result<u64> {
    let Some(descriptor) = transport.lookup(key).await? else {
        anyhow::bail!("snapshot P2P artifact '{key}' was not found");
    };
    let size = transport
        .fetch(&descriptor, destination)
        .await
        .with_context(|| format!("fetch snapshot P2P artifact '{key}'"))?;
    debug!(key, destination = %destination.display(), size, "fetched snapshot artifact from P2P");
    Ok(size)
}

pub(crate) async fn fetch_artifact_bytes(
    transport: &Arc<dyn P2pTransport>,
    key: &P2pArtifactKey,
) -> Result<Bytes> {
    let Some(descriptor) = transport.lookup(key).await? else {
        anyhow::bail!("snapshot P2P artifact '{key}' was not found");
    };
    let bytes = transport
        .fetch_bytes(&descriptor)
        .await
        .with_context(|| format!("fetch snapshot P2P artifact '{key}'"))?;
    debug!(
        key,
        size = bytes.len(),
        "fetched snapshot artifact from P2P"
    );
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlaybd::backend::local::LocalFile;
    use overlaybd::config::{ImageConfig, LayerConfig};
    use overlaybd::index_file::{create_file_rw, LayerInfo};
    use overlaybd::virtual_file::VirtualFile;
    use std::sync::Arc;

    async fn write_sealed_layer(path: &Path, uuid: Uuid) {
        let index_path = path.with_extension("index");
        let data_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path).expect("data file"));
        let index_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(index_path).expect("index file"));
        let mut info = LayerInfo::new(data_file, Some(index_file), 8192);
        info.uuid = uuid;
        let file = create_file_rw(info).await.expect("create rw layer");
        file.write_at(0, &[0x5a; 4096]).await.expect("write layer");
        file.close_seal().await.expect("seal layer");
    }

    #[tokio::test]
    async fn local_overlaybd_layers_publish_only_digest_layers_without_committed_uuid() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let descriptorless = temp.path().join("snapshot.commit");
        let described = temp.path().join("described.commit");
        let uuid = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        write_sealed_layer(&descriptorless, uuid).await;
        std::fs::write(&described, b"described").expect("write described layer");

        let image_config = ImageConfig {
            lowers: vec![
                LayerConfig {
                    file: descriptorless.display().to_string(),
                    ..Default::default()
                },
                LayerConfig {
                    file: described.display().to_string(),
                    digest:
                        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    size: 9,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let image_config_path = temp.path().join("image.json");
        std::fs::write(
            &image_config_path,
            serde_json::to_vec(&image_config).expect("serialize image config"),
        )
        .expect("write image config");

        let artifacts =
            SnapshotP2pArtifact::local_overlaybd_layers(&image_config_path, &HashSet::new());

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].publish_mode, P2pPublishMode::Copy);
        assert_eq!(
            artifacts[0].key,
            "overlaybd-layer/v1/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[tokio::test]
    async fn local_overlaybd_layers_publishes_uuid_alongside_digest_layers() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let committed_layer_path = temp.path().join("snapshot.commit");
        let skipped_layer_path = temp.path().join("skipped.commit");
        let committed_uuid = Uuid::parse_str("22222222-3333-4444-5555-666666666666").unwrap();
        let skipped_uuid = Uuid::parse_str("33333333-4444-5555-6666-777777777777").unwrap();
        write_sealed_layer(&committed_layer_path, committed_uuid).await;
        write_sealed_layer(&skipped_layer_path, skipped_uuid).await;
        let committed_descriptor = crate::digest::FileDigest::describe(&committed_layer_path)
            .await
            .expect("describe committed layer");
        let image_config = ImageConfig {
            lowers: vec![
                LayerConfig {
                    file: committed_layer_path.display().to_string(),
                    digest: committed_descriptor.sha256.clone(),
                    size: committed_descriptor.size,
                    ..Default::default()
                },
                LayerConfig {
                    file: skipped_layer_path.display().to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let image_config_path = temp.path().join("image.json");
        std::fs::write(
            &image_config_path,
            serde_json::to_vec(&image_config).expect("serialize image config"),
        )
        .expect("write image config");
        let committed_uuids = HashSet::from([committed_uuid.to_string()]);

        let artifacts =
            SnapshotP2pArtifact::local_overlaybd_layers(&image_config_path, &committed_uuids);

        assert_eq!(artifacts.len(), 2);
        assert!(artifacts
            .iter()
            .any(|artifact| artifact.key == layer_key_from_digest(&committed_descriptor.sha256)));
        assert!(artifacts.iter().any(|artifact| artifact.key
            == "overlaybd-layer/v1/uuid/22222222-3333-4444-5555-666666666666"));
    }
}
