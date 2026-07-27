use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use tempfile::NamedTempFile;
use tracing::warn;

use crate::digest;
use crate::snapshot::repository::backends::common::write_dense_overlaybd_layer_to_file;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::rootfs_snapshot_image_tag;
use crate::snapshot::{
    ExternalLayer, OverlaybdLayerRef, PersistedDiskImagePublication, SnapshotId,
};

use super::client::{AcrClient, AcrClientError};
use super::manifest::{build_oci_image_manifest, minimal_oci_config_blob, OciDescriptor};
use super::source_image::{
    load_source_registry_image, LocalSnapshotDelta, LocalSnapshotDeltaDescriptor,
    SourceRegistryLayer, SourceRegistryRepository,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DiskImageSubject {
    Rootfs,
    AttachedDrive { drive_id: String },
}

impl DiskImageSubject {
    pub(crate) fn log_label(&self) -> &str {
        match self {
            Self::Rootfs => "rootfs",
            Self::AttachedDrive { .. } => "attached_drive",
        }
    }
}

#[derive(Debug)]
pub(crate) struct DiskImageExportOutcome {
    pub(crate) layers: Vec<OverlaybdLayerRef>,
    pub(crate) publication: Option<PersistedDiskImagePublication>,
}

type AcrClientBuilder = dyn Fn(&str) -> Result<AcrClient, AcrClientError> + Send + Sync + 'static;

pub(crate) struct AcrDiskImageExporter {
    // This mutex only protects the in-memory client map. It is never held across
    // an await point; client construction happens outside the lock as well.
    clients: Mutex<HashMap<String, AcrClient>>,
    client_builder: Arc<AcrClientBuilder>,
}

impl AcrDiskImageExporter {
    pub(crate) fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            client_builder: Arc::new(AcrClient::from_docker_config),
        }
    }

    #[cfg(test)]
    fn new_with_client_builder(client_builder: Arc<AcrClientBuilder>) -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            client_builder,
        }
    }

    pub(crate) async fn export(
        &self,
        snapshot_id: &SnapshotId,
        subject: DiskImageSubject,
        image_config_path: &Path,
    ) -> RepositoryResult<DiskImageExportOutcome> {
        let image = load_source_registry_image(image_config_path)?;
        let target = &image.target;

        let tag = snapshot_tag(snapshot_id, &subject);
        validate_snapshot_tag(&tag)?;

        let client = self.client_for_registry(&target.registry).await?;

        let manifest_url = target.manifest_url(&tag);
        client
            .ensure_manifest_absent(&manifest_url, &target.repository, &tag)
            .await
            .map_err(RepositoryError::from)?;

        let mut descriptors = Vec::with_capacity(image.layers.len());
        let mut committed_layers = Vec::with_capacity(image.layers.len());
        let upload_url = target.upload_url();

        for layer in &image.layers {
            let (digest, size) = match layer {
                SourceRegistryLayer::Remote { digest, size } => (digest.clone(), *size),
                SourceRegistryLayer::LocalDelta(local) => {
                    let (digest, size) = self
                        .upload_local_delta(
                            &client,
                            &upload_url,
                            &target.repo_blob_url,
                            &target.repository,
                            local,
                        )
                        .await?;
                    (digest, size)
                }
            };
            descriptors.push(OciDescriptor::overlaybd_layer(digest.clone(), size));
            committed_layers.push(OverlaybdLayerRef::External(ExternalLayer {
                digest,
                repo_blob_url: target.repo_blob_url.clone(),
                size,
            }));
        }

        let (config_bytes, config_digest, config_size) =
            minimal_oci_config_blob(host_architecture_for_oci())?;
        if !client
            .blob_exists(&target.repo_blob_url, &target.repository, &config_digest)
            .await?
        {
            client
                .upload_blob_bytes(
                    &upload_url,
                    &target.repository,
                    config_bytes,
                    &config_digest,
                )
                .await?;
        }
        let manifest = build_oci_image_manifest(
            OciDescriptor::config(config_digest, config_size),
            descriptors,
            &tag,
        )?;
        let manifest_digest = client
            .put_manifest(&manifest_url, &target.repository, manifest)
            .await?;

        Ok(DiskImageExportOutcome {
            layers: committed_layers,
            publication: Some(PersistedDiskImagePublication {
                image_ref: target.image_ref(&tag),
                tag,
                manifest_digest,
                repo_blob_url: target.repo_blob_url.clone(),
            }),
        })
    }

    pub(crate) async fn rollback_publication(
        &self,
        publication: &PersistedDiskImagePublication,
    ) -> RepositoryResult<()> {
        let repository_ref = match SourceRegistryRepository::parse(&publication.repo_blob_url) {
            Ok(repository_ref) => repository_ref,
            Err(error) => {
                warn!(
                    repo_blob_url = %publication.repo_blob_url,
                    error = %error,
                    "cannot roll back ACR publication with invalid repoBlobUrl"
                );
                return Ok(());
            }
        };
        let client = self.client_for_registry(&repository_ref.registry).await?;
        client
            .delete_manifest_by_digest(
                &repository_ref.registry,
                &repository_ref.repository,
                &publication.manifest_digest,
            )
            .await
            .map_err(RepositoryError::from)
    }

    async fn client_for_registry(&self, registry: &str) -> Result<AcrClient, AcrClientError> {
        {
            let clients = self.clients.lock().map_err(|_| AcrClientError::Registry {
                message: "ACR client cache lock poisoned".to_string(),
            })?;
            if let Some(client) = clients.get(registry) {
                return Ok(client.clone());
            }
        }

        let registry = registry.to_string();
        let registry_for_builder = registry.clone();
        let client_builder = Arc::clone(&self.client_builder);
        let client = tokio::task::spawn_blocking(move || (client_builder)(&registry_for_builder))
            .await
            .map_err(|e| AcrClientError::Registry {
                message: format!("join ACR client builder task: {e}"),
            })??;
        let mut clients = self.clients.lock().map_err(|_| AcrClientError::Registry {
            message: "ACR client cache lock poisoned".to_string(),
        })?;
        // Another task may have populated the cache while this task was loading
        // Docker credentials in spawn_blocking. Prefer the cached client and
        // discard the duplicate one.
        if let Some(existing) = clients.get(&registry) {
            return Ok(existing.clone());
        }
        clients.insert(registry, client.clone());
        Ok(client)
    }

    async fn upload_local_delta(
        &self,
        client: &AcrClient,
        upload_url: &str,
        repo_blob_url: &str,
        repository: &str,
        local: &LocalSnapshotDelta,
    ) -> RepositoryResult<(String, u64)> {
        match &local.descriptor {
            LocalSnapshotDeltaDescriptor::Raw { digest, size } => client
                .upload_blob_with_descriptor(
                    upload_url,
                    repo_blob_url,
                    repository,
                    &local.path,
                    digest,
                    *size,
                )
                .await
                .map_err(RepositoryError::from),
            LocalSnapshotDeltaDescriptor::DenseOverlaybd => {
                let dense_temp = NamedTempFile::new().map_err(|e| {
                    RepositoryError::backend(
                        format!("create temp dense ACR delta for '{}'", local.path.display()),
                        e,
                    )
                })?;
                let dense_path = dense_temp.path().to_path_buf();
                let descriptor = write_dense_overlaybd_layer_to_file(&local.path, &dense_path)
                    .await
                    .map_err(|e| {
                        RepositoryError::backend(
                            format!(
                                "dense-export overlaybd ACR delta '{}'",
                                local.path.display()
                            ),
                            e,
                        )
                    })?;
                let uploaded = client
                    .upload_blob_with_descriptor(
                        upload_url,
                        repo_blob_url,
                        repository,
                        &dense_path,
                        &descriptor.digest,
                        descriptor.size,
                    )
                    .await
                    .map_err(RepositoryError::from)?;
                Ok(uploaded)
            }
        }
    }
}

fn snapshot_tag(snapshot_id: &crate::snapshot::SnapshotId, subject: &DiskImageSubject) -> String {
    let rootfs_tag = rootfs_snapshot_image_tag(snapshot_id);
    match subject {
        DiskImageSubject::Rootfs => rootfs_tag,
        DiskImageSubject::AttachedDrive { drive_id } => {
            format!(
                "{rootfs_tag}-drive-{}",
                sanitize_drive_tag_component(drive_id)
            )
        }
    }
}

fn sanitize_drive_tag_component(drive_id: &str) -> String {
    let hash = digest::sha256_hex(drive_id.as_bytes());
    let mut sanitized = drive_id
        .chars()
        .map(|ch| if is_tag_char(ch) { ch } else { '-' })
        .collect::<String>();
    if sanitized.is_empty() {
        sanitized = "drive".to_string();
    }
    sanitized.truncate(32);
    format!("{sanitized}-{}", &hash[..12])
}

pub(crate) fn validate_snapshot_tag(tag: &str) -> RepositoryResult<()> {
    let mut chars = tag.chars();
    let Some(first) = chars.next() else {
        return invalid_tag(tag);
    };
    if tag.len() > 128 || !is_tag_start(first) || !chars.all(is_tag_char) {
        return invalid_tag(tag);
    }
    Ok(())
}

fn invalid_tag<T>(tag: &str) -> RepositoryResult<T> {
    Err(RepositoryError::InvalidRequest {
        reason: format!("snapshot ACR tag '{tag}' is not a valid OCI tag"),
    })
}

fn is_tag_start(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn is_tag_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-')
}

fn host_architecture_for_oci() -> &'static str {
    // This follows the server binary's target architecture. If AgentENV ever
    // supports guest images for a different architecture, pass the runtime
    // architecture into the ACR manifest builder instead.
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "arm" => "arm",
        "riscv64" => "riscv64",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::TempDir;

    use super::super::client::tests::{client as test_client, fake_server, FakeState};
    use super::*;

    #[test]
    fn validates_oci_snapshot_tags() {
        validate_snapshot_tag("agentenv-snapshot-018f0d93-aaaa-bbbb-cccc-0123456789ab").unwrap();
        validate_snapshot_tag("_leading_underscore").unwrap();
        assert!(validate_snapshot_tag("-bad").is_err());
        assert!(validate_snapshot_tag("bad/tag").is_err());
        assert!(validate_snapshot_tag(&"a".repeat(129)).is_err());

        let id = crate::snapshot::SnapshotId::generate();
        let id_str = id.to_string();
        assert_eq!(
            snapshot_tag(&id, &DiskImageSubject::Rootfs),
            format!("agentenv-snapshot-{id_str}")
        );
        let drive_tag = snapshot_tag(
            &id,
            &DiskImageSubject::AttachedDrive {
                drive_id: "data:cache".to_string(),
            },
        );
        assert!(drive_tag.starts_with(&format!("agentenv-snapshot-{id_str}-drive-data-cache-")));
        validate_snapshot_tag(&drive_tag).unwrap();
    }

    #[tokio::test]
    async fn planned_acr_source_with_missing_credentials_fails_without_fallback() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        fs::write(&delta, b"delta").unwrap();
        let image = dir.path().join("image.json");
        fs::write(
            &image,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": "https://registry.example/v2/ns/repo/blobs",
                "lowers": [
                    {"digest": "sha256:base", "size": 123},
                    {"file": delta, "digest": "sha256:delta", "size": 5}
                ],
                "upper": {},
                "resultFile": ""
            }))
            .unwrap(),
        )
        .unwrap();
        let publisher = AcrDiskImageExporter::new_with_client_builder(Arc::new(|registry| {
            Err(AcrClientError::MissingCredentials {
                registry: registry.to_string(),
            })
        }));

        let err = publisher
            .export(
                &crate::snapshot::SnapshotId::generate(),
                DiskImageSubject::Rootfs,
                &image,
            )
            .await
            .expect_err("planned ACR source must not fall back on missing credentials");

        match err {
            RepositoryError::Backend {
                source: Some(source),
                ..
            } => assert!(matches!(
                source.downcast_ref::<AcrClientError>(),
                Some(AcrClientError::MissingCredentials { .. })
            )),
            other => panic!("expected missing credentials backend error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn all_remote_source_registry_image_publishes_manifest_without_layer_upload() {
        let state = Arc::new(Mutex::new(FakeState::with_existing_blobs()));
        let base = fake_server(Arc::clone(&state)).await;
        let dir = TempDir::new().unwrap();
        let image = dir.path().join("image.json");
        fs::write(
            &image,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": format!("{base}/v2/ns/repo/blobs"),
                "lowers": [
                    {"digest": "sha256:base", "size": 123},
                    {"digest": "sha256:delta", "size": 456}
                ],
                "upper": {},
                "resultFile": ""
            }))
            .unwrap(),
        )
        .unwrap();
        let client = test_client();
        let publisher =
            AcrDiskImageExporter::new_with_client_builder(Arc::new(move |_| Ok(client.clone())));
        let snapshot_id = crate::snapshot::SnapshotId::generate();

        let outcome = publisher
            .export(&snapshot_id, DiskImageSubject::Rootfs, &image)
            .await
            .unwrap();

        let expected_tag = format!("agentenv-snapshot-{snapshot_id}");
        let publication = outcome.publication.unwrap();
        assert_eq!(publication.tag, expected_tag);
        assert_eq!(
            publication.image_ref,
            format!(
                "{}/ns/repo:{}",
                base.trim_start_matches("http://"),
                expected_tag
            )
        );
        assert_eq!(
            outcome.layers,
            vec![
                OverlaybdLayerRef::External(ExternalLayer {
                    digest: "sha256:base".to_string(),
                    repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
                    size: 123,
                }),
                OverlaybdLayerRef::External(ExternalLayer {
                    digest: "sha256:delta".to_string(),
                    repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
                    size: 456,
                }),
            ]
        );
        let state = state.lock().unwrap();
        assert_eq!(state.manifest_puts.len(), 1);
        let manifest: serde_json::Value = serde_json::from_slice(&state.manifest_puts[0]).unwrap();
        assert_eq!(manifest["layers"][0]["digest"], "sha256:base");
        assert_eq!(manifest["layers"][1]["digest"], "sha256:delta");
        assert_eq!(
            manifest["annotations"]["io.agentenv.snapshot.tag"],
            expected_tag
        );
    }
}
