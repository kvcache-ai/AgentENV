use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use tempfile::NamedTempFile;
use tracing::warn;

use agentenv_domain::{rootfs_snapshot_image_tag, PersistedDiskImagePublication, SnapshotId};
use overlaybd::config::OverlaybdCompactOutput;
use overlaybd::layer::LayerFile;

use crate::digest;
use crate::layer::{prepare_layer_file_upload, LayerChain, LayerRef, PreparedLayerUpload};
use crate::repository::{RepositoryError, RepositoryResult};

use super::client::{AcrClient, AcrClientError};
use super::manifest::{
    build_oci_image_manifest, host_architecture_for_oci, minimal_oci_config_blob,
    snapshot_oci_config_blob, OciDescriptor, SnapshotOciConfigInput,
};
use super::source_image::{source_registry_image, SourceRegistryLayer, SourceRegistryRepository};

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
    pub(crate) layers: Vec<LayerRef>,
    pub(crate) publication: Option<PersistedDiskImagePublication>,
    pub(crate) local_layers: Vec<PreparedLayerUpload>,
}

type AcrClientBuilder = dyn Fn(&str) -> Result<AcrClient, AcrClientError> + Send + Sync + 'static;

pub(crate) struct AcrDiskImageExporter {
    // This mutex only protects the in-memory client map. It is never held across
    // an await point; client construction happens outside the lock as well.
    clients: Mutex<HashMap<String, AcrClient>>,
    client_builder: Arc<AcrClientBuilder>,
    publish_compression: OverlaybdCompactOutput,
}

impl AcrDiskImageExporter {
    pub(crate) fn new(publish_compression: OverlaybdCompactOutput) -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            client_builder: Arc::new(AcrClient::from_docker_config),
            publish_compression,
        }
    }

    #[cfg(test)]
    fn new_with_client_builder(
        client_builder: Arc<AcrClientBuilder>,
        publish_compression: OverlaybdCompactOutput,
    ) -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            client_builder,
            publish_compression,
        }
    }

    pub(crate) async fn export(
        &self,
        snapshot_id: &SnapshotId,
        subject: DiskImageSubject,
        layers: &LayerChain,
        config: Option<SnapshotOciConfigInput<'_>>,
    ) -> RepositoryResult<DiskImageExportOutcome> {
        let image = source_registry_image(layers)?;
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
        let mut local_layers = Vec::new();
        let upload_url = target.upload_url();

        for layer in &image.layers {
            let (digest, size) = match layer {
                SourceRegistryLayer::Remote { digest, size } => (digest.clone(), *size),
                SourceRegistryLayer::LocalDelta(local) => {
                    let upload = self
                        .upload_local_delta(
                            &client,
                            &upload_url,
                            &target.repo_blob_url,
                            &target.repository,
                            local,
                        )
                        .await?;
                    let descriptor = (upload.digest().to_string(), upload.size());
                    local_layers.push(upload);
                    descriptor
                }
            };
            descriptors.push(OciDescriptor::overlaybd_layer(digest.clone(), size));
            committed_layers.push(LayerRef::External(crate::layer::ExternalLayer {
                digest,
                repo_blob_url: target.repo_blob_url.clone(),
                size,
            }));
        }

        let (config_bytes, config_digest, config_size) = match config {
            Some(config) => snapshot_oci_config_blob(host_architecture_for_oci(), config)?,
            None => minimal_oci_config_blob(host_architecture_for_oci())?,
        };
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
            local_layers,
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
        local: &LayerFile,
    ) -> RepositoryResult<PreparedLayerUpload> {
        let local_path = local.path.clone();
        let (upload, _) =
            prepare_layer_file_upload(local.clone(), self.publish_compression, move || {
                NamedTempFile::new().map_err(|error| {
                    anyhow::Error::new(error).context(format!(
                        "create temp ACR delta for '{}'",
                        local_path.display()
                    ))
                })
            })
            .await
            .map_err(|error| {
                RepositoryError::backend(
                    format!("prepare ACR delta '{}'", local.path.display()),
                    error,
                )
            })?;
        client
            .upload_blob_with_descriptor(
                upload_url,
                repo_blob_url,
                repository,
                upload.path(),
                upload.digest(),
                upload.size(),
            )
            .await
            .map_err(RepositoryError::from)?;
        Ok(upload)
    }
}

/// Best-effort rollback of source-registry disk image publications.
pub(crate) async fn rollback_disk_publications(
    id: &SnapshotId,
    publications: &[PersistedDiskImagePublication],
) {
    if !publications.is_empty() {
        let exporter = AcrDiskImageExporter::new(OverlaybdCompactOutput::Raw);
        for publication in publications.iter().rev() {
            if let Err(error) = exporter.rollback_publication(publication).await {
                warn!(
                    snapshot_id = %id,
                    image_ref = %publication.image_ref,
                    manifest_digest = %publication.manifest_digest,
                    error = %error,
                    "failed to delete source-registry publication; continuing snapshot removal"
                );
            }
        }
    }
}

fn snapshot_tag(snapshot_id: &SnapshotId, subject: &DiskImageSubject) -> String {
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
    if !agentenv_domain::is_valid_oci_tag(tag) {
        return Err(RepositoryError::InvalidRequest {
            reason: format!("snapshot ACR tag '{tag}' is not a valid OCI tag"),
        });
    }
    Ok(())
}

fn is_tag_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-')
}

#[cfg(test)]
mod tests {
    use agentenv_domain::{CommandContext, ExternalLayer, LayerRef};
    use overlaybd::backend::local::LocalFile;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    use overlaybd::backend::switch::new_switch_file;
    use overlaybd::backend::tar::new_tar_file_adaptor;
    use overlaybd::index_file::{
        create_file_rw, CommitArgs, LSMTFile, LSMTReadOnlyFile, LayerInfo, RwLayout,
    };
    use overlaybd::layer::read_sealed_layer_uuids;
    use overlaybd::virtual_file::VirtualFile;
    use overlaybd::zfile::{is_zfile, CompressArgs, CompressOptions, ZFileCompactWriter};
    use serde_json::json;
    use uuid::Uuid;

    use super::super::client::tests::{client as test_client, fake_server, FakeState};
    use super::*;
    use crate::digest::FileDigest;

    #[test]
    fn validates_oci_snapshot_tags() {
        validate_snapshot_tag("agentenv-snapshot-018f0d93-aaaa-bbbb-cccc-0123456789ab").unwrap();
        validate_snapshot_tag("_leading_underscore").unwrap();
        assert!(validate_snapshot_tag("-bad").is_err());
        assert!(validate_snapshot_tag("bad/tag").is_err());
        assert!(validate_snapshot_tag(&"a".repeat(129)).is_err());

        let id = SnapshotId::generate();
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
        let image = LayerChain::from(vec![crate::layer::LayerSource::Referenced {
            reference: LayerRef::External(crate::layer::ExternalLayer {
                digest: "sha256:base".into(),
                size: 123,
                repo_blob_url: "https://registry.example/v2/ns/repo/blobs".into(),
            }),
            local_file: None,
        }]);
        let publisher = AcrDiskImageExporter::new_with_client_builder(
            Arc::new(|registry| {
                Err(AcrClientError::MissingCredentials {
                    registry: registry.to_string(),
                })
            }),
            OverlaybdCompactOutput::Raw,
        );

        let err = publisher
            .export(
                &SnapshotId::generate(),
                DiskImageSubject::Rootfs,
                &image,
                None,
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
        let image = LayerChain::from(vec![
            crate::layer::LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:base".into(),
                    size: 123,
                    repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
                }),
                local_file: None,
            },
            crate::layer::LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:delta".into(),
                    size: 456,
                    repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
                }),
                local_file: None,
            },
        ]);
        let client = test_client();
        let publisher = AcrDiskImageExporter::new_with_client_builder(
            Arc::new(move |_| Ok(client.clone())),
            OverlaybdCompactOutput::Raw,
        );
        let snapshot_id = SnapshotId::generate();
        let context = CommandContext::new(
            HashMap::from([("APP_ENV".to_string(), "snapshot".to_string())]),
            "/workspace",
        );
        let raw = json!({"StopSignal": "SIGTERM"});

        let outcome = publisher
            .export(
                &snapshot_id,
                DiskImageSubject::Rootfs,
                &image,
                Some(SnapshotOciConfigInput::new(&context, Some(&raw))),
            )
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
                LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:base".to_string(),
                    repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
                    size: 123,
                }),
                LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:delta".to_string(),
                    repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
                    size: 456,
                }),
            ]
        );
        let (_, expected_config_digest, _) = snapshot_oci_config_blob(
            host_architecture_for_oci(),
            SnapshotOciConfigInput::new(&context, Some(&raw)),
        )
        .unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.manifest_puts.len(), 1);
        let manifest: serde_json::Value = serde_json::from_slice(&state.manifest_puts[0]).unwrap();
        assert_eq!(manifest["config"]["digest"], expected_config_digest);
        assert_eq!(manifest["layers"][0]["digest"], "sha256:base");
        assert_eq!(manifest["layers"][1]["digest"], "sha256:delta");
        assert_eq!(
            manifest["annotations"]["io.agentenv.snapshot.tag"],
            expected_tag
        );
    }

    const DELTA_VSIZE: u64 = 3 * 4096;
    const SPARSE_VSIZE: u64 = 64 * 1024;

    fn zfile_mode() -> OverlaybdCompactOutput {
        OverlaybdCompactOutput::ZFile {
            algorithm: crate::cfg::OverlaybdCompressionAlgorithm::Lz4,
            workers: 1,
        }
    }

    /// Write a sealed commit-style LSMT layer at `path`, raw or zfile, with explicit
    /// UUID / parent UUID and one distinct byte pattern per 4KiB page.
    async fn write_sealed_delta(path: &Path, zfile: bool, uuid: Uuid, parent_uuid: Option<Uuid>) {
        let data = Arc::new(LocalFile::new(path.with_extension("data")).unwrap());
        let index = Arc::new(LocalFile::new(path.with_extension("index")).unwrap());
        let layer = LSMTFile::create(data, Some(index), DELTA_VSIZE, false)
            .await
            .unwrap();
        for (page, byte) in [(0u64, 0x11u8), (4096, 0x22), (8192, 0x33)] {
            layer.write_at(page, &[byte; 4096]).await.unwrap();
        }
        let output: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path).unwrap());
        let mut args = if zfile {
            let compress_args = CompressArgs::new(CompressOptions::new(
                CompressOptions::LZ4,
                CompressOptions::DEFAULT_BLOCK_SIZE,
                0,
            ));
            let writer = Arc::new(
                ZFileCompactWriter::new(output, &compress_args)
                    .await
                    .unwrap(),
            );
            CommitArgs::from_writer(writer)
        } else {
            CommitArgs::new(output)
        };
        args.uuid = Some(uuid);
        args.parent_uuid = parent_uuid;
        layer.commit_with_args(args).await.unwrap();
    }

    /// Write a sparse read-write LSMT layer sealed in place with explicit UUID / parent UUID,
    /// the shape that routes through dense export.
    async fn write_sparse_delta(path: &Path, uuid: Uuid, parent_uuid: Option<Uuid>) {
        let data: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path).unwrap());
        let mut info = LayerInfo::new(data, None, SPARSE_VSIZE);
        info.uuid = uuid;
        info.parent_uuid = parent_uuid;
        info.rw_layout = RwLayout::Sparse;
        let lsmt = create_file_rw(info).await.unwrap();
        lsmt.write_at(0, &[0xAB; 4096]).await.unwrap();
        lsmt.close_seal().await.unwrap();
    }

    fn expected_delta_contents() -> Vec<u8> {
        let mut expected = vec![0u8; DELTA_VSIZE as usize];
        expected[..4096].fill(0x11);
        expected[4096..8192].fill(0x22);
        expected[8192..].fill(0x33);
        expected
    }

    fn expected_sparse_contents() -> Vec<u8> {
        let mut expected = vec![0u8; SPARSE_VSIZE as usize];
        expected[..4096].fill(0xAB);
        expected
    }

    /// Read a sealed layer's logical contents through the same tar + switch
    /// chain the runtime uses, transparently handling raw and zfile.
    async fn read_layer_contents(path: &Path, len: usize) -> Vec<u8> {
        let local: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(path).unwrap());
        let display = path.display().to_string();
        let tar_adapted = new_tar_file_adaptor(local).await.unwrap();
        let switched = new_switch_file(tar_adapted, true, Some(&display))
            .await
            .unwrap();
        let layer = LSMTReadOnlyFile::open(switched).await.unwrap();
        layer.read_at(0, len).await.unwrap().to_vec()
    }

    /// Export an image whose only local lower is `dir/snapshot.commit`
    /// against the fake registry and return the outcome plus recorded state.
    async fn export_with_local_delta(
        dir: &TempDir,
        mode: OverlaybdCompactOutput,
    ) -> (DiskImageExportOutcome, Arc<Mutex<FakeState>>, String) {
        let state = Arc::new(Mutex::new(FakeState::default()));
        let base = fake_server(Arc::clone(&state)).await;
        let image = dir.path().join("image.json");
        fs::write(
            &image,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": format!("{base}/v2/ns/repo/blobs"),
                "lowers": [
                    {"digest": "sha256:base", "size": 123},
                    {"file": dir.path().join("snapshot.commit")}
                ],
                "upper": {},
                "resultFile": ""
            }))
            .unwrap(),
        )
        .unwrap();
        let client = test_client();
        let publisher = AcrDiskImageExporter::new_with_client_builder(
            Arc::new(move |_| Ok(client.clone())),
            mode,
        );
        let outcome = publisher
            .export(
                &SnapshotId::generate(),
                DiskImageSubject::Rootfs,
                &crate::layer::load_layer_chain_from_config(&image, None)
                    .await
                    .unwrap(),
                None,
            )
            .await
            .unwrap();
        (outcome, state, base)
    }

    /// Reassemble the uploaded layer blob from the PATCH chunks recorded by
    /// the fake registry (the config blob goes through a one-shot PUT, so
    /// only layer bytes land in `uploads`).
    fn uploaded_blob(state: &FakeState, dir: &TempDir) -> PathBuf {
        let path = dir.path().join("uploaded.blob");
        fs::write(&path, state.uploads.concat()).unwrap();
        path
    }

    /// The committed layer ref, the blob digest/size annotations in the
    /// manifest, and the actual uploaded bytes must all agree.
    fn assert_uploaded_layer_identity(
        outcome: &DiskImageExportOutcome,
        state: &FakeState,
        base: &str,
        blob: &Path,
    ) {
        let descriptor = FileDigest::describe_blocking(blob).unwrap();
        assert_eq!(outcome.local_layers.len(), 1);
        let retained = &outcome.local_layers[0];
        assert_eq!(retained.digest(), descriptor.sha256);
        assert_eq!(retained.size(), descriptor.size);
        assert_eq!(fs::read(retained.path()).unwrap(), fs::read(blob).unwrap());
        assert_eq!(
            outcome.layers,
            vec![
                LayerRef::External(ExternalLayer {
                    digest: "sha256:base".to_string(),
                    repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
                    size: 123,
                }),
                LayerRef::External(ExternalLayer {
                    digest: descriptor.sha256.clone(),
                    repo_blob_url: format!("{base}/v2/ns/repo/blobs"),
                    size: descriptor.size,
                }),
            ]
        );
        assert_eq!(state.manifest_puts.len(), 1);
        let manifest: serde_json::Value = serde_json::from_slice(&state.manifest_puts[0]).unwrap();
        assert_eq!(manifest["layers"][1]["digest"], descriptor.sha256);
        assert_eq!(manifest["layers"][1]["size"], descriptor.size);
        assert_eq!(
            manifest["layers"][1]["annotations"]["containerd.io/snapshot/overlaybd/blob-digest"],
            descriptor.sha256
        );
        assert_eq!(
            manifest["layers"][1]["annotations"]["containerd.io/snapshot/overlaybd/blob-size"],
            descriptor.size.to_string()
        );
    }

    async fn assert_zfile_with_contents(blob: &Path, expected: &[u8]) {
        let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(blob).unwrap());
        assert_eq!(is_zfile(file).await.unwrap(), 1);
        assert_eq!(read_layer_contents(blob, expected.len()).await, expected);
    }

    #[tokio::test]
    async fn publish_compression_uploads_zfile_delta_preserving_uuids_and_manifest_annotations() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        let uuid = Uuid::new_v4();
        let parent_uuid = Some(Uuid::new_v4());
        write_sealed_delta(&delta, false, uuid, parent_uuid).await;

        let (outcome, state, base) = export_with_local_delta(&dir, zfile_mode()).await;

        let blob = {
            let state = state.lock().unwrap();
            let blob = uploaded_blob(&state, &dir);
            assert_uploaded_layer_identity(&outcome, &state, &base, &blob);
            blob
        };
        // The uploaded blob is the recontainerized zfile, decodes to the raw
        // delta's contents, differs from the raw bytes, and preserves both UUIDs.
        assert_zfile_with_contents(&blob, &expected_delta_contents()).await;
        assert_eq!(
            read_sealed_layer_uuids(&blob).await.unwrap(),
            (uuid, parent_uuid)
        );
        assert_ne!(fs::read(&blob).unwrap(), fs::read(&delta).unwrap());
    }

    #[tokio::test]
    async fn publish_compression_skips_already_zfile_delta() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        let uuid = Uuid::new_v4();
        let parent_uuid = Some(Uuid::new_v4());
        write_sealed_delta(&delta, true, uuid, parent_uuid).await;

        let (outcome, state, base) = export_with_local_delta(&dir, zfile_mode()).await;

        let blob = {
            let state = state.lock().unwrap();
            let blob = uploaded_blob(&state, &dir);
            assert_uploaded_layer_identity(&outcome, &state, &base, &blob);
            blob
        };
        // Idempotent: the pre-compressed input is uploaded byte for byte with UUIDs intact.
        assert_eq!(fs::read(&blob).unwrap(), fs::read(&delta).unwrap());
        assert_eq!(
            read_sealed_layer_uuids(&blob).await.unwrap(),
            (uuid, parent_uuid)
        );
    }

    #[tokio::test]
    async fn publish_compression_disabled_uploads_raw_delta() {
        let dir = TempDir::new().unwrap();
        let delta = dir.path().join("snapshot.commit");
        let uuid = Uuid::new_v4();
        let parent_uuid = Some(Uuid::new_v4());
        write_sealed_delta(&delta, false, uuid, parent_uuid).await;

        let (outcome, state, base) =
            export_with_local_delta(&dir, OverlaybdCompactOutput::Raw).await;

        let blob = {
            let state = state.lock().unwrap();
            let blob = uploaded_blob(&state, &dir);
            assert_uploaded_layer_identity(&outcome, &state, &base, &blob);
            blob
        };
        // Disabled: the raw layer is uploaded byte for byte with UUIDs intact.
        assert_eq!(fs::read(&blob).unwrap(), fs::read(&delta).unwrap());
        let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(&blob).unwrap());
        assert_eq!(is_zfile(file).await.unwrap(), 0);
        assert_eq!(
            read_sealed_layer_uuids(&blob).await.unwrap(),
            (uuid, parent_uuid)
        );
    }

    #[tokio::test]
    async fn publish_compression_uploads_dense_sparse_delta_preserving_uuids() {
        for (mode, expect_zfile) in [(zfile_mode(), true), (OverlaybdCompactOutput::Raw, false)] {
            let dir = TempDir::new().unwrap();
            let uuid = Uuid::new_v4();
            let parent_uuid = Some(Uuid::new_v4());
            write_sparse_delta(&dir.path().join("snapshot.commit"), uuid, parent_uuid).await;

            let (outcome, state, base) = export_with_local_delta(&dir, mode).await;

            let blob = {
                let state = state.lock().unwrap();
                let blob = uploaded_blob(&state, &dir);
                assert_uploaded_layer_identity(&outcome, &state, &base, &blob);
                blob
            };
            if expect_zfile {
                assert_zfile_with_contents(&blob, &expected_sparse_contents()).await;
            } else {
                let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(&blob).unwrap());
                assert_eq!(is_zfile(file).await.unwrap(), 0);
                assert_eq!(
                    read_layer_contents(&blob, SPARSE_VSIZE as usize).await,
                    expected_sparse_contents()
                );
            }
            assert_eq!(
                read_sealed_layer_uuids(&blob).await.unwrap(),
                (uuid, parent_uuid)
            );
        }
    }
}
