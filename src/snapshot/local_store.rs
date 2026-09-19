use anyhow::Context;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use overlaybd::config::load_image_config as load_overlaybd_image_config;
use overlaybd::layer_metadata::resolve_local_layer_path;

use crate::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::types::RuntimeArtifactLease;
use crate::snapshot::SandboxSnapshotManifest;
use crate::snapshot::{SnapshotId, SNAPSHOT_ARTIFACT_LAYOUT};

/// Owns the manifest and immutable local files for Local snapshots.
#[derive(Clone)]
pub(crate) struct LocalSnapshotStore {
    root: PathBuf,
    snapshots: Arc<DashMap<SnapshotId, Arc<LocalSnapshotFiles>>>,
}

struct LocalSnapshotFiles {
    path: PathBuf,
    manifest: SandboxSnapshotManifest,
}

impl Drop for LocalSnapshotFiles {
    fn drop(&mut self) {
        // Local availability ends with this owner process. During normal
        // operation, reclaim only after the store and all readers release it.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let path = self.path.clone();
            runtime.spawn_blocking(move || {
                if let Err(error) = fs::remove_dir_all(&path) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(path = %path.display(), %error, "failed to reclaim Local snapshot");
                    }
                }
            });
        }
    }
}

impl LocalSnapshotStore {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            snapshots: Arc::new(DashMap::new()),
        }
    }

    pub(crate) async fn commit_snapshot(
        &self,
        id: SnapshotId,
        manifest: SandboxSnapshotManifest,
    ) -> RepositoryResult<()> {
        let store = self.clone();
        run_repository_blocking("commit Local snapshot", move || {
            store.commit_snapshot_sync(&id, manifest)
        })
        .await
    }

    fn commit_snapshot_sync(
        &self,
        id: &SnapshotId,
        mut manifest: SandboxSnapshotManifest,
    ) -> RepositoryResult<()> {
        super::publish::validate_attached_drives(&manifest)?;
        let parent = self.root.join("snapshots");
        fs::create_dir_all(&parent).map_err(|error| {
            RepositoryError::backend(
                format!("create Local snapshot parent '{}'", parent.display()),
                error,
            )
        })?;
        let snapshot_dir = tempfile::Builder::new()
            .prefix(&format!("{id}-"))
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(parent)
            .map_err(|error| RepositoryError::backend("create Local snapshot directory", error))?;
        let root = snapshot_dir.path();

        let vm_state_path = root.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        stage_file(&manifest.vm_state.path, &vm_state_path)?;
        manifest.vm_state.path = vm_state_path;

        let memory_config_path = root.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config);
        stage_local_image_config(
            &manifest.memory.image_config_path,
            &memory_config_path,
            "memory",
        )?;
        manifest.memory.image_config_path = memory_config_path;

        let rootfs_config_path = root.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_image_config);
        stage_local_image_config(
            &manifest.rootfs.image_config_path,
            &rootfs_config_path,
            "rootfs",
        )?;
        manifest.rootfs.image_config_path = rootfs_config_path;

        for drive in &mut manifest.attached_drives {
            let image_config_path = root
                .join(SNAPSHOT_ARTIFACT_LAYOUT.drives_dir)
                .join(&drive.drive_id)
                .join(SNAPSHOT_ARTIFACT_LAYOUT.overlaybd_image_config_file);
            stage_local_image_config(
                &drive.image_config_path,
                &image_config_path,
                &format!("drive-{}", drive.drive_id),
            )?;
            drive.image_config_path = image_config_path;
        }
        self.snapshots.insert(
            id.clone(),
            Arc::new(LocalSnapshotFiles {
                path: snapshot_dir.keep(),
                manifest,
            }),
        );
        Ok(())
    }

    pub(crate) fn delete_snapshot(&self, id: &SnapshotId) {
        self.snapshots.remove(id);
    }

    pub(crate) fn contains_snapshot(&self, id: &SnapshotId) -> bool {
        self.snapshots.contains_key(id)
    }
}

fn stage_file(source: &Path, destination: &Path) -> RepositoryResult<()> {
    (|| -> anyhow::Result<()> {
        fs::create_dir_all(destination.parent().context("Local artifact parent")?)?;
        fs::hard_link(source, destination).or_else(|_| copy_snapshot_file(source, destination))?;
        Ok(())
    })()
    .map_err(|error| {
        RepositoryError::backend(
            format!(
                "stage Local artifact '{}' -> '{}'",
                source.display(),
                destination.display()
            ),
            error,
        )
    })
}

fn stage_local_image_config(
    source: &Path,
    destination: &Path,
    description: &str,
) -> RepositoryResult<()> {
    let mut image_config = load_overlaybd_image_config(source).map_err(|error| {
        RepositoryError::backend(
            format!("load {description} image config '{}'", source.display()),
            error,
        )
    })?;
    let parent = destination.parent().ok_or_else(|| {
        RepositoryError::backend(
            "resolve Local image config parent",
            anyhow::anyhow!("missing parent"),
        )
    })?;
    let layer_dir = parent.join("layers");
    for (index, layer) in image_config.lowers.iter_mut().enumerate() {
        let source_layer = resolve_local_layer_path(layer);
        let remote_repo_blob = layer
            .effective_repo_blob_url(&image_config.repo_blob_url)
            .to_string();
        if source_layer.is_none() && (remote_repo_blob.is_empty() || layer.digest.is_empty()) {
            return Err(RepositoryError::Unsupported {
                feature: format!(
                    "Local {description} layer {index} has neither a local artifact nor a remote digest source"
                ),
            });
        }
        // A cache dir can fall back to its remote digest. An explicit file is
        // local even if another lower supplied the image-level repository URL.
        let is_remote_reference =
            layer.file.is_empty() && !remote_repo_blob.is_empty() && !layer.digest.is_empty();
        if source_layer.is_none() || is_remote_reference {
            // Immutable remote lowers remain references in a Local manifest.
            // Clear any source-side cache path so the committed config cannot
            // retain an absolute path owned by the capturing runtime.
            layer.file.clear();
            layer.dir.clear();
            layer.target_file.clear();
            layer.gzip_index.clear();
            continue;
        }
        let source_layer = source_layer.expect("local layer path is present");
        let file_name = source_layer.file_name().ok_or_else(|| {
            RepositoryError::backend(
                "resolve Local layer filename",
                anyhow::anyhow!("missing filename"),
            )
        })?;
        let staged_layer_file = format!("{index}/{}", file_name.to_string_lossy());
        let destination_layer = layer_dir.join(&staged_layer_file);
        stage_file(&source_layer, &destination_layer)?;
        layer.file = format!("layers/{staged_layer_file}");
        layer.dir.clear();
    }

    image_config.result_file.clear();
    let bytes = serde_json::to_vec_pretty(&image_config).map_err(|error| {
        RepositoryError::backend(format!("serialize {description} image config"), error)
    })?;
    fs::create_dir_all(parent).map_err(|error| {
        RepositoryError::backend(
            format!("create Local image config directory '{}'", parent.display()),
            error,
        )
    })?;
    // The store exposes this directory only after every artifact is staged.
    fs::write(destination, bytes)
        .map_err(|error| RepositoryError::backend(format!("write Local {description}"), error))?;
    Ok(())
}

impl LocalSnapshotStore {
    pub(crate) fn acquire_manifest(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<(SandboxSnapshotManifest, Arc<RuntimeArtifactLease>)> {
        let files = self
            .snapshots
            .get(id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| RepositoryError::ArtifactNotFound {
                artifact: format!("Local snapshot '{id}' is unavailable on this owner"),
            })?;
        Ok((files.manifest.clone(), files))
    }
}

async fn run_repository_blocking<T, F>(operation: &'static str, work: F) -> RepositoryResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> RepositoryResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| RepositoryError::Backend {
            message: format!("repository blocking task panicked while trying to {operation}"),
            source: Some(anyhow::Error::from(error)),
        })?
}

pub(crate) fn copy_snapshot_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    let metadata = fs::metadata(source)?;
    if metadata.blocks() * 512 >= metadata.len() {
        return fs::copy(source, destination).map(|_| ());
    }
    match storage_util::copy_sparse_file_blocking(source, destination) {
        // Sealed layers carry their index; a dense copy preserves their content
        // on filesystems that cannot report sparse extents.
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
            ) =>
        {
            fs::copy(source, destination).map(|_| ())
        }
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{FileExt, MetadataExt};
    use tempfile::TempDir;

    #[tokio::test]
    async fn failed_staging_leaves_no_local_snapshot() -> anyhow::Result<()> {
        let root = TempDir::new()?;
        let source = root.path().join("capture");
        let (_, _, manifest) = crate::snapshot::mock::write_mock_built_artifacts(&source)?;
        fs::remove_file(&manifest.memory.image_config_path)?;
        let store = LocalSnapshotStore::new(root.path().join("local"));
        let id = SnapshotId::generate();

        assert!(store.commit_snapshot(id.clone(), manifest).await.is_err());
        assert!(store.acquire_manifest(&id).is_err());
        assert!(fs::read_dir(root.path().join("local/snapshots"))?
            .next()
            .is_none());
        Ok(())
    }

    #[tokio::test]
    async fn local_files_survive_source_removal_while_remote_cache_remains_optional(
    ) -> anyhow::Result<()> {
        let source = TempDir::new()?;
        // A different filesystem also exercises the hard-link fallback.
        let destination = tempfile::tempdir_in("/dev/shm")?;
        let (_, _, manifest) =
            crate::snapshot::mock::write_mock_built_artifacts(&source.path().join("capture"))?;
        let local_file = source.path().join("local.commit");
        let file = fs::File::create(&local_file)?;
        file.set_len(64 * 1024 * 1024)?;
        file.write_all_at(b"local data", 32 * 1024 * 1024)?;
        let cache = source.path().join("cache");
        fs::create_dir(&cache)?;
        fs::write(cache.join("overlaybd.commit"), b"remote cache")?;
        fs::write(
            &manifest.memory.image_config_path,
            serde_json::to_vec(&serde_json::json!({
                "repoBlobUrl": "s3://bucket/managed-layers",
                "lowers": [
                    {"dir": cache, "digest": "sha256:remote"},
                    {"file": local_file, "digest": "sha256:local"}
                ]
            }))?,
        )?;
        let store = LocalSnapshotStore::new(destination.path().to_path_buf());
        let id = SnapshotId::generate();
        store.commit_snapshot(id.clone(), manifest).await?;
        drop(file);
        source.close()?;

        let (manifest, _lease) = store.acquire_manifest(&id)?;
        let image = load_overlaybd_image_config(&manifest.memory.image_config_path)?;
        assert!(image.lowers[0].file.is_empty() && image.lowers[0].dir.is_empty());
        assert_eq!(image.lowers[0].digest, "sha256:remote");
        assert_eq!(image.repo_blob_url, "s3://bucket/managed-layers");
        let retained = fs::File::open(&image.lowers[1].file)?;
        let mut data = [0; 10];
        retained.read_exact_at(&mut data, 32 * 1024 * 1024)?;
        assert_eq!(&data, b"local data");
        assert!(retained.metadata()?.blocks() * 512 < 1024 * 1024);
        let permissions = fs::metadata(manifest.vm_state.path.parent().unwrap())?.permissions();
        assert_eq!(permissions.mode() & 0o077, 0);
        Ok(())
    }
}
