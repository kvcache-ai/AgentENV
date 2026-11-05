use crate::layer::{LayerChain, LayerRef, LayerSource};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::layer_metadata::read_overlaybd_layer_uuid;
use tracing::{debug, info, warn};
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

    /// Collect the local overlaybd layers referenced by a snapshot image
    /// config into publishable P2P artifacts.
    ///
    /// `committed_digests` holds every layer digest the committed record
    /// carries for this image config's subject (memory, rootfs, or one
    /// attached drive). A local layer is only published under its
    /// descriptor digest when that digest is what the committed record
    /// references: publish-time compression recontainerizes raw local layers
    /// as zfile before upload, so the record names the compressed bytes and a
    /// raw-digest key would never be looked up by consumers.
    ///
    /// UUID publication is restricted to identities in the committed record.
    pub(crate) fn local_overlaybd_layers(
        layers: &LayerChain,
        committed_digests: &HashSet<String>,
        committed_uuids: &HashSet<String>,
    ) -> Vec<Self> {
        let mut artifacts = Vec::new();
        for layer in &layers.layers {
            let Some(path) = layer.path() else {
                continue;
            };
            let descriptor = match layer {
                LayerSource::File(file) => file
                    .descriptor
                    .as_ref()
                    .map(|descriptor| (descriptor.digest.as_str(), descriptor.size)),
                LayerSource::Referenced {
                    reference: LayerRef::Managed(reference),
                    ..
                } => Some((reference.digest.as_str(), reference.size)),
                LayerSource::Referenced {
                    reference: LayerRef::External(crate::layer::ExternalLayer { digest, size, .. }),
                    ..
                } => Some((digest.as_str(), *size)),
            };
            if let Some((digest, size)) = descriptor.filter(|(digest, size)| {
                !digest.is_empty() && *size > 0 && committed_digests.contains(*digest)
            }) {
                artifacts.push(Self::content_addressed_overlaybd_layer(path, digest, size));
            }
            if committed_uuids.is_empty() {
                continue;
            }
            let uuid = match read_overlaybd_layer_uuid(path) {
                Ok(uuid) if !uuid.is_nil() => uuid,
                Ok(_) => {
                    info!(
                        path = %path.display(),
                        "skipping snapshot P2P layer publication because overlaybd layer uuid is nil"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(
                        path = %path.display(),
                        error = %error,
                        "skipping snapshot P2P layer publication because overlaybd layer uuid could not be read"
                    );
                    continue;
                }
            };
            if !committed_uuids.contains(&uuid.to_string()) {
                continue;
            }
            let size = match std::fs::metadata(path) {
                Ok(metadata) if metadata.is_file() => metadata.len(),
                Ok(_) => {
                    warn!(
                        path = %path.display(),
                        "skipping snapshot P2P layer publication because path is not a regular file"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(
                        path = %path.display(),
                        error = %error,
                        "skipping snapshot P2P layer publication because layer size could not be read"
                    );
                    continue;
                }
            };
            artifacts.push(Self::uuid_overlaybd_layer(path, uuid, size));
        }
        artifacts
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
        std::fs::write(&descriptorless, b"snapshot").expect("write descriptorless layer");
        std::fs::write(&described, b"described").expect("write described layer");

        let described_digest =
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let layers = LayerChain::from(vec![
            LayerSource::File(overlaybd::layer::LayerFile::dense(
                descriptorless,
                Uuid::nil(),
                None,
            )),
            LayerSource::File(overlaybd::layer::LayerFile::dense(
                described,
                Uuid::nil(),
                Some(overlaybd::LayerDescriptor {
                    digest: described_digest.clone(),
                    size: 9,
                }),
            )),
        ]);

        let artifacts = SnapshotP2pArtifact::local_overlaybd_layers(
            &layers,
            &HashSet::from([described_digest]),
            &HashSet::new(),
        );

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].publish_mode, P2pPublishMode::Copy);
        assert_eq!(
            artifacts[0].key,
            "overlaybd-layer/v1/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    /// Publish-time compression recontainerizes a raw local layer before
    /// upload, so the committed record names the compressed digest and the
    /// raw descriptor digest must not be published: no consumer ever looks
    /// the raw key up. The uuid-keyed publication is unaffected by the
    /// digest guard.
    #[tokio::test]
    async fn local_overlaybd_layers_skips_digest_absent_from_committed_record() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let layer_path = temp.path().join("snapshot.commit");
        let uuid = Uuid::parse_str("44444444-5555-6666-7777-888888888888").unwrap();
        write_sealed_layer(&layer_path, uuid).await;
        let raw_descriptor = crate::digest::FileDigest::describe(&layer_path)
            .await
            .expect("describe raw layer");
        let layers = LayerChain::from(vec![LayerSource::File(overlaybd::layer::LayerFile::dense(
            layer_path,
            uuid,
            Some(overlaybd::LayerDescriptor {
                digest: raw_descriptor.sha256.clone(),
                size: raw_descriptor.size,
            }),
        ))]);
        // The committed record references the recontainerized (compressed)
        // bytes, not the raw local file.
        let committed_digests = HashSet::from([
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        ]);
        let committed_uuids = HashSet::from([uuid.to_string()]);

        let artifacts = SnapshotP2pArtifact::local_overlaybd_layers(
            &layers,
            &committed_digests,
            &committed_uuids,
        );

        assert_eq!(artifacts.len(), 1);
        assert_eq!(
            artifacts[0].key,
            "overlaybd-layer/v1/uuid/44444444-5555-6666-7777-888888888888"
        );
        assert!(!artifacts
            .iter()
            .any(|artifact| artifact.key == layer_key_from_digest(&raw_descriptor.sha256)));
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
        let layers = LayerChain::from(vec![
            LayerSource::File(overlaybd::layer::LayerFile::dense(
                committed_layer_path,
                committed_uuid,
                Some(overlaybd::LayerDescriptor {
                    digest: committed_descriptor.sha256.clone(),
                    size: committed_descriptor.size,
                }),
            )),
            LayerSource::File(overlaybd::layer::LayerFile::dense(
                skipped_layer_path,
                skipped_uuid,
                None,
            )),
        ]);
        let committed_digests = HashSet::from([committed_descriptor.sha256.clone()]);
        let committed_uuids = HashSet::from([committed_uuid.to_string()]);

        let artifacts = SnapshotP2pArtifact::local_overlaybd_layers(
            &layers,
            &committed_digests,
            &committed_uuids,
        );

        assert_eq!(artifacts.len(), 2);
        assert!(artifacts
            .iter()
            .any(|artifact| artifact.key == layer_key_from_digest(&committed_descriptor.sha256)));
        assert!(artifacts.iter().any(|artifact| artifact.key
            == "overlaybd-layer/v1/uuid/22222222-3333-4444-5555-666666666666"));
    }
}
