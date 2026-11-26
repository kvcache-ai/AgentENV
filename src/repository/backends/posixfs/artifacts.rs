use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

use crate::digest::{self, FileDigest};
use crate::repository::{BlobBody, BlobKey, RepositoryError, RepositoryResult};
use crate::types::SnapshotId;

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
