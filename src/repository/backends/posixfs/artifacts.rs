use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

use crate::digest::{self, FileDigest};
use crate::repository::{BlobBody, BlobKey, RepositoryError, RepositoryResult};
use crate::snapshot::SnapshotId;

/// Artifact store backed by files in a POSIX-compatible shared filesystem.
pub struct PosixFsArtifactStore {
    root: PathBuf,
}

impl PosixFsArtifactStore {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn snapshot_dir(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.root.join(BlobKey::posix_snapshot_dir(snapshot_id))
    }

    pub(crate) fn put_blob(&self, key: &BlobKey, body: BlobBody<'_>) -> RepositoryResult<()> {
        match (key, body) {
            (BlobKey::Layer { digest, .. }, BlobBody::File(source)) => {
                self.put_layer(digest, self.blob_path(key), source)
            }
            (BlobKey::Layer { digest, .. }, BlobBody::Bytes(bytes)) => {
                let mut staged = self.staging_file()?;
                std::io::Write::write_all(&mut staged, &bytes).map_err(|error| {
                    RepositoryError::backend(format!("write '{}'", staged.path().display()), error)
                })?;
                self.put_layer(digest, self.blob_path(key), staged.path())
            }
            (BlobKey::Snapshot(..), BlobBody::File(source)) => {
                self.copy_local_artifact(self.blob_path(key), source)
            }
            (BlobKey::Snapshot(id, file), BlobBody::Bytes(bytes)) => {
                self.put_snapshot_bytes(id, file.name(), &bytes)
            }
        }
    }

    /// A temporary file on the repository's filesystem, so storing it as a
    /// layer links it in without copying.
    pub(crate) fn staging_file(&self) -> RepositoryResult<NamedTempFile> {
        let staging_dir = self.root.join(BlobKey::POSIX_STAGING_DIR);
        fs::create_dir_all(&staging_dir).map_err(|error| {
            RepositoryError::backend(
                format!("create staging dir '{}'", staging_dir.display()),
                error,
            )
        })?;
        NamedTempFile::new_in(&staging_dir).map_err(|error| {
            RepositoryError::backend(
                format!("create staging file in '{}'", staging_dir.display()),
                error,
            )
        })
    }

    /// Where `key` is stored.
    pub(crate) fn blob_path(&self, key: &BlobKey) -> PathBuf {
        self.root.join(key.posix_path())
    }

    /// Stores one snapshot file, creating the snapshot's directory.
    fn put_snapshot_bytes(
        &self,
        snapshot_id: &SnapshotId,
        name: &str,
        bytes: &[u8],
    ) -> RepositoryResult<()> {
        let snapshot_dir = self.snapshot_dir(snapshot_id);
        fs::create_dir_all(&snapshot_dir).map_err(|error| {
            RepositoryError::backend(
                format!("create snapshot dir '{}'", snapshot_dir.display()),
                error,
            )
        })?;
        self.write_snapshot_file(snapshot_id, name, bytes)
    }

    /// Removes one snapshot's directory. Shared managed layers stay.
    pub(crate) fn delete_snapshot_files(&self, snapshot_id: &SnapshotId) -> RepositoryResult<()> {
        let snapshot_dir = self.snapshot_dir(snapshot_id);
        match fs::remove_dir_all(&snapshot_dir) {
            Ok(()) => snapshot_dir.parent().map_or(Ok(()), sync_dir),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", snapshot_dir.display()),
                error,
            )),
        }
    }

    /// Atomically replaces one file in an existing snapshot directory.
    fn write_snapshot_file(
        &self,
        snapshot_id: &SnapshotId,
        name: &str,
        bytes: &[u8],
    ) -> RepositoryResult<()> {
        let snapshot_dir = self.snapshot_dir(snapshot_id);
        let path = snapshot_dir.join(name);
        let mut temp = NamedTempFile::new_in(&snapshot_dir).map_err(|error| {
            RepositoryError::backend(
                format!("create temp file in '{}'", snapshot_dir.display()),
                error,
            )
        })?;
        std::io::Write::write_all(&mut temp, bytes)
            .and_then(|()| temp.as_file().sync_all())
            .map_err(|error| {
                RepositoryError::backend(format!("write '{}'", path.display()), error)
            })?;
        temp.persist(&path).map_err(|error| {
            RepositoryError::backend(format!("persist '{}'", path.display()), error.error)
        })?;
        sync_dir(&snapshot_dir)
    }

    fn copy_local_artifact(&self, destination: PathBuf, source: &Path) -> RepositoryResult<()> {
        let source_metadata = fs::metadata(source).map_err(|error| {
            RepositoryError::backend(
                format!("read source artifact metadata '{}'", source.display()),
                error,
            )
        })?;

        let Some(parent) = destination.parent() else {
            return Err(RepositoryError::Backend {
                message: format!(
                    "resolve parent dir for artifact '{}'",
                    destination.display()
                ),
                source: None,
            });
        };
        fs::create_dir_all(parent).map_err(|error| {
            RepositoryError::backend(format!("create artifact dir '{}'", parent.display()), error)
        })?;
        if !same_file(&source_metadata, &destination)? {
            hard_link_or_copy_file_with_sha256(source, &destination)?;
            sync_dir(parent)?;
        }
        let destination_metadata = fs::metadata(&destination).map_err(|error| {
            RepositoryError::backend(
                format!("read artifact metadata '{}'", destination.display()),
                error,
            )
        })?;
        if destination_metadata.len() != source_metadata.len() {
            return Err(RepositoryError::Backend {
                message: format!(
                    "copied artifact size mismatch for '{}' -> '{}': expected {} bytes, found {} bytes",
                    source.display(),
                    destination.display(),
                    source_metadata.len(),
                    destination_metadata.len()
                ),
                source: None,
            });
        }
        Ok(())
    }

    /// Stores a content-addressed layer unless one is already there,
    /// hard-linking it when possible.
    fn put_layer(&self, digest: &str, destination: PathBuf, source: &Path) -> RepositoryResult<()> {
        if destination.exists() {
            let size = fs::metadata(source)
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("read managed layer metadata '{}'", source.display()),
                        error,
                    )
                })?
                .len();
            let destination_size = fs::metadata(&destination)
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("read managed layer metadata '{}'", destination.display()),
                        error,
                    )
                })?
                .len();
            if destination_size != size {
                return Err(RepositoryError::Backend {
                    message: format!(
                        "managed layer '{}' size mismatch: new file has {}, existing file has {}",
                        digest, size, destination_size
                    ),
                    source: None,
                });
            }
            return Ok(());
        }

        let Some(parent) = destination.parent() else {
            return Err(RepositoryError::Backend {
                message: format!(
                    "resolve parent dir for managed layer '{}'",
                    destination.display()
                ),
                source: None,
            });
        };
        fs::create_dir_all(parent).map_err(|error| {
            RepositoryError::backend(
                format!("create managed layer dir '{}'", parent.display()),
                error,
            )
        })?;
        // Fast path for expected immutable overlaybd lower layers. This is
        // safe for layers produced by AgentENV/image-cache/snapshot restack
        // because they are sealed or content-addressed after publication.
        hard_link_or_copy_managed_layer(source, &destination, parent)?;
        sync_dir(parent)
    }
}

fn same_file(source_metadata: &fs::Metadata, destination: &Path) -> RepositoryResult<bool> {
    match fs::metadata(destination) {
        Ok(destination_metadata) => Ok(source_metadata.dev() == destination_metadata.dev()
            && source_metadata.ino() == destination_metadata.ino()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(RepositoryError::backend(
            format!(
                "read destination artifact metadata '{}'",
                destination.display()
            ),
            error,
        )),
    }
}

fn hard_link_or_copy_file_with_sha256(source: &Path, destination: &Path) -> RepositoryResult<()> {
    match fs::hard_link(source, destination) {
        Ok(()) => {
            finalize_hard_linked_file(destination)?;
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => {}
    }
    copy_file_with_sha256(source, destination)
}

fn hard_link_or_copy_managed_layer(
    source: &Path,
    destination: &Path,
    destination_parent: &Path,
) -> RepositoryResult<()> {
    match fs::hard_link(source, destination) {
        Ok(()) => {
            finalize_hard_linked_file(destination)?;
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Ok(());
        }
        Err(_) => {}
    }

    let temp = NamedTempFile::new_in(destination_parent).map_err(|error| {
        RepositoryError::backend(
            format!(
                "create temp managed layer next to '{}'",
                destination.display()
            ),
            error,
        )
    })?;
    fs::copy(source, temp.path()).map_err(|error| {
        RepositoryError::backend(
            format!(
                "copy managed layer from '{}' to temp '{}'",
                source.display(),
                temp.path().display()
            ),
            error,
        )
    })?;
    temp.as_file().sync_all().map_err(|error| {
        RepositoryError::backend(
            format!("sync temp managed layer '{}'", temp.path().display()),
            error,
        )
    })?;
    match temp.persist_noclobber(destination) {
        Ok(_) => Ok(()),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(RepositoryError::backend(
            format!(
                "persist managed layer temp '{}' -> '{}'",
                error.file.path().display(),
                destination.display()
            ),
            error.error,
        )),
    }
}

fn finalize_hard_linked_file(path: &Path) -> RepositoryResult<()> {
    chmod_read_only(path)?;
    fs::File::open(path)
        .map_err(|error| {
            RepositoryError::backend(format!("open hard-linked file '{}'", path.display()), error)
        })?
        .sync_all()
        .map_err(|error| {
            RepositoryError::backend(format!("sync hard-linked file '{}'", path.display()), error)
        })?;
    Ok(())
}

pub(super) fn sync_dir(path: &Path) -> RepositoryResult<()> {
    fs::File::open(path)
        .map_err(|error| RepositoryError::backend(format!("open '{}'", path.display()), error))?
        .sync_all()
        .map_err(|error| RepositoryError::backend(format!("sync '{}'", path.display()), error))?;
    Ok(())
}

fn chmod_read_only(path: &Path) -> RepositoryResult<()> {
    let mut permissions = fs::metadata(path)
        .map_err(|error| {
            RepositoryError::backend(format!("read metadata of '{}'", path.display()), error)
        })?
        .permissions();
    permissions.set_mode(permissions.mode() & !0o222);
    fs::set_permissions(path, permissions).map_err(|error| {
        RepositoryError::backend(format!("chmod '{}' to read-only", path.display()), error)
    })
}

fn copy_file_with_sha256(source: &Path, destination: &Path) -> RepositoryResult<()> {
    // We compute the source digest while copying, then re-read the
    // destination and compare digests after sync_all(). The second pass is
    // intentional: it verifies the persisted bytes rather than only trusting
    // the write loop's in-memory byte stream.
    let mut source_file = fs::File::open(source).map_err(|error| {
        RepositoryError::backend(
            format!("open source artifact '{}'", source.display()),
            error,
        )
    })?;
    let mut destination_file = fs::File::create(destination).map_err(|error| {
        RepositoryError::backend(
            format!("create destination artifact '{}'", destination.display()),
            error,
        )
    })?;
    let (source_digest_hex, copied) =
        digest::copy_with_sha256_hex(&mut source_file, &mut destination_file).map_err(|error| {
            RepositoryError::backend(
                format!(
                    "copy source artifact '{}' to destination artifact '{}'",
                    source.display(),
                    destination.display()
                ),
                error,
            )
        })?;
    let source_digest = FileDigest {
        size: copied,
        sha256: format!("sha256:{source_digest_hex}"),
    };

    destination_file.sync_all().map_err(|error| {
        RepositoryError::backend(
            format!("sync destination artifact '{}'", destination.display()),
            error,
        )
    })?;

    let destination_digest = FileDigest::describe_blocking(destination).map_err(|error| {
        RepositoryError::backend(
            format!("describe destination artifact '{}'", destination.display()),
            error,
        )
    })?;
    if destination_digest != source_digest {
        return Err(RepositoryError::Backend {
            message: format!(
                "copied artifact digest mismatch for '{}' -> '{}': expected {:?}, found {:?}",
                source.display(),
                destination.display(),
                source_digest,
                destination_digest
            ),
            source: None,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;
    use std::sync::Arc;

    use overlaybd::backend::local::LocalFile;
    use overlaybd::index_file::{CommitArgs, LSMTFile};
    use overlaybd::virtual_file::VirtualFile;
    use overlaybd::zfile::{is_zfile, CompressArgs, CompressOptions, ZFileCompactWriter};
    use tempfile::TempDir;

    use super::super::backend::PosixFsRepository;
    use super::super::catalog::PosixFsCatalogStore;
    use super::PosixFsArtifactStore;
    use crate::digest::FileDigest;
    use crate::repository::{BlobKey, LayerRole};
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::store::store_snapshot;
    use crate::snapshot::{
        CommittedAttachedDrive, SnapshotId, SnapshotPublishMetadata, SNAPSHOT_ARTIFACT_LAYOUT,
    };

    fn test_repository(root: &std::path::Path) -> PosixFsRepository {
        PosixFsRepository::new(
            Arc::new(PosixFsCatalogStore::new(root.to_path_buf())),
            Arc::new(PosixFsArtifactStore::new(root.to_path_buf())),
        )
    }

    fn metadata(id: &SnapshotId) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id: id.clone(),
            ..SnapshotPublishMetadata::mock()
        }
    }

    /// Where a POSIX repository stores a layer, relative to its root.
    fn layer_file(digest: &str) -> PathBuf {
        PathBuf::from(
            BlobKey::Layer {
                digest: digest.to_owned(),
                role: LayerRole::Rootfs,
            }
            .posix_path(),
        )
    }

    fn assert_same_inode(left: &std::path::Path, right: &std::path::Path) {
        let left_metadata = fs::metadata(left).expect("left metadata");
        let right_metadata = fs::metadata(right).expect("right metadata");
        assert_eq!(left_metadata.dev(), right_metadata.dev());
        assert_eq!(left_metadata.ino(), right_metadata.ino());
    }

    #[tokio::test]
    async fn attached_drive_rootfs_layers_are_derived_from_resolved_lower_paths() {
        let tempdir = TempDir::new().expect("tempdir");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let local_dir = tempdir.path().join("local");
        let (_, _, mut manifest) = write_mock_built_artifacts(local_dir.as_path())
            .expect("mock built artifacts should write");

        // Create drive directory structure: local_dir/drives/data/
        let drive_dir = local_dir.join("drives").join("data");
        fs::create_dir_all(&drive_dir).expect("create drive dir");

        // Create layers dir through the layout API used by drive image configs.
        let layers_dir = local_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.drive_layers_dir);
        fs::create_dir_all(&layers_dir).expect("create layers dir");
        let drive_layer = layers_dir.join("drive.commit");
        fs::write(&drive_layer, b"drive-layer").expect("drive lower");
        let drive_descriptor =
            FileDigest::describe_blocking(&drive_layer).expect("describe drive lower");

        // Write drive image.json with relative path to layers
        fs::write(
            drive_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.overlaybd_image_config_file),
            format!(
                r#"{{
  "repoBlobUrl": "",
  "lowers": [{{ "file": "../layers/drive.commit", "digest": "{}", "size": {} }}],
  "upper": {{}},
  "resultFile": ""
}}"#,
                drive_descriptor.sha256, drive_descriptor.size
            ),
        )
        .expect("write drive image config");

        manifest = manifest
            .with_extra_drives(&[crate::sandbox::ExtraDrive::Overlaybd {
                drive_id: "data".to_string(),
                image_config_path: local_dir
                    .join(SNAPSHOT_ARTIFACT_LAYOUT.drives_dir)
                    .join("data")
                    .join(SNAPSHOT_ARTIFACT_LAYOUT.overlaybd_image_config_file),
                read_only: true,
                mount_path: crate::sandbox::ExtraDrive::default_mount_path("data"),
                virtual_size: Some(4096),
                sub_path: None,
                snapshot_output_dir: None,
                volume: false,
            }])
            .expect("attached drive manifest should include virtual size");

        let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
            .await
            .expect("import built artifacts");

        let path = match &built.attached_drives[0] {
            CommittedAttachedDrive::Overlaybd { layers, .. } => {
                assert_eq!(layers.len(), 1);
                match &layers[0] {
                    crate::snapshot::OverlaybdLayerRef::Managed(layer) => {
                        layer_file(&layer.digest).display().to_string()
                    }
                    crate::snapshot::OverlaybdLayerRef::External(_) => {
                        panic!("expected managed rootfs layer")
                    }
                }
            }
        };
        assert!(path.contains("managed-layers"));
        assert!(tempdir.path().join(&path).exists());
    }

    #[tokio::test]
    async fn store_snapshot_extracts_rootfs_layers_from_image_config() {
        let tempdir = TempDir::new().expect("tempdir");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let local_dir = tempdir.path().join("local");
        let (_, _, manifest) = write_mock_built_artifacts(local_dir.as_path())
            .expect("mock built artifacts should write");
        let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
            .await
            .expect("collect built artifacts");

        assert_eq!(built.rootfs_layers.len(), 1);
        match &built.rootfs_layers[0] {
            crate::snapshot::OverlaybdLayerRef::Managed(layer) => {
                let path = layer_file(&layer.digest);
                assert!(path.display().to_string().contains("managed-layers"));
                assert!(tempdir.path().join(path).exists());
            }
            crate::snapshot::OverlaybdLayerRef::External(_) => {
                panic!("expected managed rootfs layer")
            }
        }
    }

    #[tokio::test]
    async fn store_snapshot_hard_links_local_publish_artifacts_when_possible() {
        let tempdir = TempDir::new().expect("tempdir");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let local_dir = tempdir.path().join("local");
        let (rootfs_lower, memory_lower, manifest) =
            write_mock_built_artifacts(local_dir.as_path())
                .expect("mock built artifacts should write");

        let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
            .await
            .expect("collect built artifacts");

        let snapshot_dir = tempdir
            .path()
            .join(BlobKey::posix_snapshot_dir(&snapshot_id));
        assert_same_inode(
            &manifest.vm_state.path,
            &snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
        );

        match &built.rootfs_layers[0] {
            crate::snapshot::OverlaybdLayerRef::Managed(layer) => {
                assert_same_inode(
                    &rootfs_lower,
                    &tempdir.path().join(layer_file(&layer.digest)),
                );
            }
            crate::snapshot::OverlaybdLayerRef::External(_) => {
                panic!("expected managed rootfs layer")
            }
        }
        assert_same_inode(
            &memory_lower,
            &tempdir
                .path()
                .join(layer_file(&built.memory_layers[0].digest)),
        );
    }

    #[tokio::test]
    async fn store_snapshot_uses_declared_local_layer_descriptor() {
        let tempdir = TempDir::new().expect("tempdir");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let local_dir = tempdir.path().join("local");
        let (rootfs_lower, _, manifest) = write_mock_built_artifacts(local_dir.as_path())
            .expect("mock built artifacts should write");
        // Deliberately not the content digest: this verifies that descriptor
        // backed import trusts the descriptor instead of re-hashing the file.
        let declared_digest = "sha256:declared-rootfs";
        fs::write(
            &manifest.rootfs.image_config_path,
            format!(
                r#"{{
  "repoBlobUrl": "",
  "lowers": [{{ "file": "{}", "digest": "{}", "size": 10 }}],
  "upper": {{}},
  "resultFile": ""
}}"#,
                rootfs_lower.display(),
                declared_digest
            ),
        )
        .expect("write descriptor rootfs image config");

        let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
            .await
            .expect("collect built artifacts");

        match &built.rootfs_layers[0] {
            crate::snapshot::OverlaybdLayerRef::Managed(layer) => {
                assert_eq!(layer.digest, declared_digest);
                assert_eq!(layer.size, 10);
                let path = layer_file(&layer.digest);
                assert!(tempdir.path().join(path).exists());
            }
            crate::snapshot::OverlaybdLayerRef::External(_) => {
                panic!("expected managed rootfs layer")
            }
        }
    }

    #[tokio::test]
    async fn store_snapshot_keeps_mem_image_config_local_only() {
        let tempdir = TempDir::new().expect("tempdir");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let local_dir = tempdir.path().join("local");
        let (_, _, manifest) = write_mock_built_artifacts(local_dir.as_path())
            .expect("mock built artifacts should write");
        let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
            .await
            .expect("collect built artifacts");

        assert_eq!(built.memory_layers.len(), 1);
        let snapshot_dir = tempdir
            .path()
            .join(BlobKey::posix_snapshot_dir(&snapshot_id));
        let persisted_manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest))
                .expect("read committed firecracker manifest"),
        )
        .expect("parse committed firecracker manifest");
        assert!(persisted_manifest["vmState"].get("path").is_none());
        assert!(persisted_manifest["memory"]
            .get("imageConfigPath")
            .is_none());
        assert!(persisted_manifest["rootfs"]
            .get("imageConfigPath")
            .is_none());
        assert!(
            !snapshot_dir
                .join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config)
                .exists(),
            "committed snapshot dir should not persist mem_image.json"
        );
    }

    #[tokio::test]
    async fn store_snapshot_links_dense_exports_in_from_staging() {
        let tempdir = TempDir::new().expect("tempdir");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let local_dir = tempdir.path().join("local");
        let (_, _, manifest) = write_mock_built_artifacts(local_dir.as_path())
            .expect("mock built artifacts should write");
        // A captured sparse rootfs delta, which is dense-exported to be stored.
        let delta_path = local_dir.join("snapshot.commit");
        let data: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&delta_path).expect("create delta"));
        let delta = LSMTFile::create(data, None, 2 * 4096, true)
            .await
            .expect("create sparse delta");
        delta.write_at(0, &[0xBC; 4096]).await.expect("write delta");
        delta.close_seal().await.expect("seal delta");
        drop(delta);
        fs::write(
            &manifest.rootfs.image_config_path,
            format!(r#"{{"lowers":[{{"file":"{}"}}]}}"#, delta_path.display()),
        )
        .expect("write rootfs image config");

        let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
            .await
            .expect("store snapshot");

        let crate::snapshot::OverlaybdLayerRef::Managed(layer) = &built.rootfs_layers[0] else {
            panic!("expected managed rootfs layer");
        };
        let staged = built
            .local_layers
            .iter()
            .find(|upload| upload.digest() == layer.digest)
            .expect("dense export kept for publication");
        let staging_dir = tempdir.path().join("staging");
        assert!(staged.path().starts_with(&staging_dir));
        assert_same_inode(
            staged.path(),
            &tempdir.path().join(layer_file(&layer.digest)),
        );
        drop(built);
        assert_eq!(fs::read_dir(&staging_dir).expect("staging dir").count(), 0);
    }

    /// Write a real ZFile-compressed sealed LSMT layer, mirroring a compressed
    /// memory lower as found in repositories published while capture-time
    /// compression existed (or produced by publish-time compression).
    async fn write_zfile_lsmt_layer(path: &std::path::Path) {
        let data =
            Arc::new(LocalFile::new(path.with_extension("data")).expect("create layer data file"));
        let index = Arc::new(
            LocalFile::new(path.with_extension("index")).expect("create layer index file"),
        );
        let layer = LSMTFile::create(data, Some(index), 2 * 4096, false)
            .await
            .expect("create layer");
        layer
            .write_at(0, &[0x5A; 4096])
            .await
            .expect("write layer page 0");
        layer
            .write_at(4096, &[0xA5; 4096])
            .await
            .expect("write layer page 1");
        let output = Arc::new(LocalFile::new(path).expect("create zfile layer output"));
        let compress_args = CompressArgs::new(CompressOptions::new(
            CompressOptions::LZ4,
            CompressOptions::DEFAULT_BLOCK_SIZE,
            0,
        ));
        let writer = Arc::new(
            ZFileCompactWriter::new(output, &compress_args)
                .await
                .expect("create zfile compact writer"),
        );
        layer
            .commit_with_args(CommitArgs::from_writer(writer))
            .await
            .expect("commit zfile layer");
    }

    #[tokio::test]
    async fn store_snapshot_preserves_zfile_memory_lower_bytes() {
        let tempdir = TempDir::new().expect("tempdir");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let local_dir = tempdir.path().join("local");
        let (_, _, manifest) = write_mock_built_artifacts(local_dir.as_path())
            .expect("mock built artifacts should write");

        // Replace the mock memory lower with a real ZFile layer. Production
        // mem image configs carry no digest/size for the freshly written
        // lower, so the import hashes the physical file.
        let zfile_lower = local_dir.join("mem.zfile.commit");
        write_zfile_lsmt_layer(&zfile_lower).await;
        let zfile_descriptor =
            FileDigest::describe_blocking(&zfile_lower).expect("describe zfile lower");
        fs::write(
            &manifest.memory.image_config_path,
            format!(r#"{{"lowers":[{{"file":"{}"}}]}}"#, zfile_lower.display()),
        )
        .expect("write zfile memory image config");

        let built = store_snapshot(&repository, &metadata(&snapshot_id), &manifest)
            .await
            .expect("import built artifacts");

        assert_eq!(built.memory_layers.len(), 1);
        let managed = &built.memory_layers[0];
        assert_eq!(managed.digest, zfile_descriptor.sha256);
        assert_eq!(managed.size, zfile_descriptor.size);
        // ZFile wraps the raw LSMT header, so no layer UUID is exposed.
        assert!(managed.uuid.is_none());

        let managed_path = tempdir.path().join(layer_file(&managed.digest));
        assert_eq!(
            fs::read(&managed_path).expect("read managed layer"),
            fs::read(&zfile_lower).expect("read source zfile lower"),
            "managed layer must preserve the physical ZFile bytes"
        );
        assert_eq!(
            fs::metadata(&managed_path)
                .expect("managed layer metadata")
                .len(),
            fs::metadata(&zfile_lower)
                .expect("source zfile metadata")
                .len()
        );
        // The stored bytes still probe as a ZFile with the source algorithm.
        let managed_file = Arc::new(LocalFile::open_ro(&managed_path).expect("open managed layer"));
        assert_eq!(
            is_zfile(managed_file).await.expect("probe managed layer"),
            1
        );
    }
}
