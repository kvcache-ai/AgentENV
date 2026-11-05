use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use overlaybd::config::{resolve_image_config_local_paths, validate_image_config, ImageConfig};
use tempfile::TempDir;

use super::SnapshotId;
use crate::cfg::ConfigManager;
use crate::layer::{LayerChain, LayerRef, LayerSource};
use crate::snapshot::{SandboxSnapshotManifest, SnapshotAttachedDriveArtifacts};

// This is a node-wide soft limit; avoid repeatedly walking all retained artifacts.
const USAGE_RECHECK_INTERVAL: Duration = Duration::from_secs(60);
static SNAPSHOT_USAGE: Mutex<Option<(Instant, std::io::Result<u64>)>> = Mutex::new(None);

fn snapshot_usage(
    root: &Path,
    last_check: &Mutex<Option<(Instant, std::io::Result<u64>)>>,
) -> Result<u64> {
    let mut last_check = last_check.lock().expect("snapshot usage mutex poisoned");
    if last_check
        .as_ref()
        .is_none_or(|(checked_at, _)| checked_at.elapsed() >= USAGE_RECHECK_INTERVAL)
    {
        let result = snapshot_disk_usage(root);
        *last_check = Some((Instant::now(), result));
    }
    match &last_check.as_ref().expect("usage was checked").1 {
        Ok(usage) => Ok(*usage),
        Err(error) => anyhow::bail!("scan snapshot disk usage under {}: {error}", root.display()),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("snapshot quota exceeded under {root}: {usage} allocated bytes reached limit {limit}")]
pub struct SnapshotQuotaExceeded {
    root: PathBuf,
    usage: u64,
    limit: u64,
}

pub(crate) async fn create_snapshot_dir(id: &SnapshotId) -> Result<TempDir> {
    let config = &ConfigManager::global_config().snapshot;
    let root = config.local_root.clone();
    let limit = config.max_local_artifact_bytes;
    let id = id.clone();
    tokio::task::spawn_blocking(move || {
        if let Some(limit) = limit {
            check_snapshot_quota(&root, snapshot_usage(&root, &SNAPSHOT_USAGE)?, limit)?;
        }
        allocate_snapshot(&root, &id)
    })
    .await
    .context("allocate snapshot directory")?
}

fn check_snapshot_quota(root: &Path, usage: u64, limit: u64) -> Result<()> {
    if usage >= limit {
        return Err(SnapshotQuotaExceeded {
            root: root.to_path_buf(),
            usage,
            limit,
        }
        .into());
    }
    Ok(())
}

fn allocate_snapshot(root: &Path, id: &SnapshotId) -> Result<TempDir> {
    fs::create_dir_all(root)?;
    tempfile::Builder::new()
        .prefix(&id.to_string())
        .rand_bytes(0)
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(root)
        .with_context(|| format!("create snapshot {id} under {}", root.display()))
}

fn snapshot_disk_usage(root: &Path) -> std::io::Result<u64> {
    let mut pending = Vec::new();
    match fs::read_dir(root) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                if SnapshotId::parse(&entry.file_name().to_string_lossy()).is_err() {
                    continue;
                }
                let kind = match entry.file_type() {
                    Ok(kind) => kind,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                if kind.is_dir() {
                    pending.push(entry.path());
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    }
    let mut seen = HashSet::new();
    let mut total = 0u64;
    while let Some(path) = pending.pop() {
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let metadata = match fs::symlink_metadata(entry.path()) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() && seen.insert((metadata.dev(), metadata.ino())) {
                total = total.saturating_add(metadata.blocks() * 512);
            }
        }
    }
    Ok(total)
}

/// Capture-only state. The storage config is consumed before publishing the manifest.
pub(crate) struct CapturedImage {
    pub image_config_path: PathBuf,
    pub layers: LayerChain,
    pub config: ImageConfig,
    pub virtual_size: u64,
}

pub(crate) struct CapturedDrive {
    pub drive_id: String,
    pub read_only: bool,
    pub mount_path: PathBuf,
    pub sub_path: Option<PathBuf>,
    pub image: CapturedImage,
}

pub(crate) struct SnapshotCapture {
    pub backend: &'static str,
    pub volume_drive_slots: usize,
    pub physical_extra_drive_count: usize,
    pub vm_state_path: PathBuf,
    pub memory: CapturedImage,
    pub rootfs: CapturedImage,
    pub attached_drives: Vec<CapturedDrive>,
}

impl SnapshotCapture {
    pub(crate) fn finish(mut self, root: &Path) -> Result<SandboxSnapshotManifest> {
        ensure!(self.vm_state_path.is_file(), "snapshot VM state is missing");
        let root = fs::canonicalize(root).context("resolve snapshot directory")?;
        finish_image_config(&mut self.memory, &root)?;
        finish_image_config(&mut self.rootfs, &root)?;
        let mut drives = Vec::with_capacity(self.attached_drives.len());
        for mut drive in self.attached_drives {
            finish_image_config(&mut drive.image, &root)?;
            drives.push(SnapshotAttachedDriveArtifacts {
                drive_id: drive.drive_id,
                read_only: drive.read_only,
                mount_path: drive.mount_path,
                sub_path: drive.sub_path,
                image_config_path: drive.image.image_config_path,
                layers: drive.image.layers,
                virtual_size: drive.image.virtual_size,
            });
        }
        let mut manifest = SandboxSnapshotManifest::new(
            self.backend,
            self.vm_state_path,
            self.memory.image_config_path.clone(),
            self.memory.virtual_size,
            self.rootfs.image_config_path.clone(),
            self.rootfs.virtual_size,
            &[],
        )?;
        manifest.memory.layers = self.memory.layers;
        manifest.rootfs.layers = self.rootfs.layers;
        manifest.attached_drives = drives;
        manifest.volume_drive_slots = self.volume_drive_slots;
        manifest.physical_extra_drive_count = self.physical_extra_drive_count;
        manifest.validate_attached_drives()?;
        Ok(manifest)
    }
}

pub(crate) fn finish_image_config(capture: &mut CapturedImage, root: &Path) -> Result<()> {
    let path = &capture.image_config_path;
    let image = &mut capture.config;
    let layers = &mut capture.layers;
    let config_path = std::path::absolute(path)?;
    resolve_image_config_local_paths(&config_path, image);
    validate_image_config(image)?;
    ensure!(
        image.lowers.len() == layers.layers.len(),
        "snapshot lower/config count mismatch"
    );
    let parent = path.parent().context("snapshot image config parent")?;
    for (index, (layer, lower)) in layers.layers.iter_mut().zip(&mut image.lowers).enumerate() {
        let retained_path = match layer {
            LayerSource::File(file) => &mut file.path,
            LayerSource::Referenced {
                reference,
                local_file,
            } => {
                let Some(file) = local_file else {
                    let remote = match reference {
                        LayerRef::Managed(value) => {
                            !value.digest.is_empty()
                                && layers
                                    .managed_repo_blob_url
                                    .as_ref()
                                    .is_some_and(|url| !url.is_empty())
                        }
                        LayerRef::External(crate::layer::ExternalLayer {
                            digest,
                            repo_blob_url,
                            ..
                        }) => !digest.is_empty() && !repo_blob_url.is_empty(),
                    };
                    ensure!(
                        remote,
                        "snapshot layer {index} has no retained file or remote source"
                    );
                    continue;
                };
                file
            }
        };
        let source = fs::canonicalize(&retained_path)
            .with_context(|| format!("resolve snapshot layer {}", retained_path.display()))?;
        let retained = if source.starts_with(root) {
            source.clone()
        } else {
            let destination = parent
                .join("layers")
                .join(index.to_string())
                .join(source.file_name().context("snapshot layer filename")?);
            fs::create_dir_all(destination.parent().unwrap())?;
            fs::hard_link(&source, &destination)
                .or_else(|_| copy_snapshot_file(&source, &destination))
                .with_context(|| format!("retain snapshot layer {}", source.display()))?;
            destination
        };
        lower.file = retained.display().to_string();
        *retained_path = retained;
    }
    image.upper = Default::default();
    image.result_file.clear();
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, image)?;
    temporary
        .persist(path)
        .map(|_| ())
        .map_err(|error| error.error)
        .with_context(|| format!("write completed snapshot image config {}", path.display()))
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
    use crate::snapshot::Snapshot;
    use overlaybd::config::load_image_config;
    use overlaybd::layer::LayerFile;
    use std::os::unix::fs::FileExt;
    use std::sync::Arc;

    #[test]
    fn admission_counts_shared_snapshot_inodes_once_and_excludes_cache() -> Result<()> {
        let root = tempfile::tempdir()?;
        let id = SnapshotId::generate();
        let first = allocate_snapshot(root.path(), &id)?;
        assert_eq!(first.path(), root.path().join(id.to_string()));
        fs::write(first.path().join("layer"), [0u8; 4096])?;
        let second = allocate_snapshot(root.path(), &SnapshotId::generate())?;
        fs::hard_link(first.path().join("layer"), second.path().join("layer"))?;
        fs::create_dir(root.path().join("cache"))?;
        fs::write(root.path().join("cache/ignored"), [0u8; 8192])?;
        std::os::unix::fs::symlink(root.path().join("cache"), second.path().join("alias"))?;
        let usage = fs::metadata(first.path().join("layer"))?.blocks() * 512;
        assert_eq!(snapshot_disk_usage(root.path())?, usage);
        let rejected = check_snapshot_quota(root.path(), usage, usage).unwrap_err();
        assert!(rejected.downcast_ref::<SnapshotQuotaExceeded>().is_some());
        assert!(allocate_snapshot(root.path(), &id).is_err());
        assert!(first.path().join("layer").is_file());
        drop(first);
        assert_eq!(snapshot_disk_usage(root.path())?, usage);
        drop(second);
        assert_eq!(snapshot_disk_usage(root.path())?, 0);
        check_snapshot_quota(root.path(), snapshot_disk_usage(root.path())?, usage)?;
        Ok(())
    }

    #[test]
    fn admission_rechecks_usage_after_expiry_and_recovers_after_reclamation() -> Result<()> {
        let root = tempfile::tempdir()?;
        let last_check = Mutex::new(None);
        assert_eq!(snapshot_usage(root.path(), &last_check)?, 0);
        let snapshot = allocate_snapshot(root.path(), &SnapshotId::generate())?;
        fs::write(snapshot.path().join("layer"), [0u8; 4096])?;
        check_snapshot_quota(root.path(), snapshot_usage(root.path(), &last_check)?, 1)?;
        last_check.lock().unwrap().as_mut().unwrap().0 -= USAGE_RECHECK_INTERVAL;
        let usage = snapshot_usage(root.path(), &last_check)?;
        assert!(check_snapshot_quota(root.path(), usage, 1).is_err());
        drop(snapshot);
        assert_eq!(snapshot_usage(root.path(), &last_check)?, usage);
        last_check.lock().unwrap().as_mut().unwrap().0 -= USAGE_RECHECK_INTERVAL;
        check_snapshot_quota(root.path(), snapshot_usage(root.path(), &last_check)?, 1)?;
        Ok(())
    }

    #[test]
    fn failed_usage_scan_is_reused_without_admitting_from_an_expired_result() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("snapshots");
        let last_check = Mutex::new(None);
        assert_eq!(snapshot_usage(&path, &last_check)?, 0);
        last_check.lock().unwrap().as_mut().unwrap().0 -= USAGE_RECHECK_INTERVAL;
        fs::write(&path, b"not a directory")?;
        assert!(snapshot_usage(&path, &last_check).is_err());
        fs::remove_file(&path)?;
        assert!(snapshot_usage(&path, &last_check).is_err());
        last_check.lock().unwrap().as_mut().unwrap().0 -= USAGE_RECHECK_INTERVAL;
        assert_eq!(snapshot_usage(&path, &last_check)?, 0);
        Ok(())
    }

    #[test]
    fn completed_capture_owns_private_layers_without_pin_of_remote_cache() -> Result<()> {
        let source = tempfile::tempdir()?;
        let roots = tempfile::tempdir()?;
        let id = SnapshotId::generate();
        let directory = allocate_snapshot(roots.path(), &id)?;
        let mut capture = crate::snapshot::mock::write_mock_capture(directory.path())?.2;
        let private = source.path().join("private.commit");
        let file = fs::File::create(&private)?;
        file.set_len(64 * 1024 * 1024)?;
        file.write_all_at(b"private data", 32 * 1024 * 1024)?;
        capture.memory.layers = vec![
            LayerSource::Referenced {
                reference: LayerRef::External(crate::layer::ExternalLayer {
                    digest: "sha256:remote".into(),
                    size: 0,
                    repo_blob_url: "s3://bucket/layers".into(),
                }),
                local_file: None,
            },
            LayerSource::File(LayerFile::dense(private.clone(), uuid::Uuid::nil(), None)),
        ]
        .into();
        let inode = fs::metadata(&private)?.ino();
        capture.memory.config = capture.memory.layers.to_runtime_config();
        let image = &mut capture.memory.config;
        let cache = source.path().join("remote-cache");
        fs::create_dir(&cache)?;
        image.lowers[0].dir = cache.display().to_string();
        image.result_file = source
            .path()
            .join("runtime-result.json")
            .display()
            .to_string();
        image.download_override = Some(overlaybd::config::DownloadConfig {
            enable: true,
            concurrency: 3,
            ..Default::default()
        });
        fs::write(&capture.rootfs.image_config_path, b"invalid JSON")?;
        assert!(!capture.memory.image_config_path.exists());
        let manifest = capture.finish(directory.path())?;
        let image = load_image_config(&manifest.memory.image_config_path)?;
        assert_eq!(Path::new(&image.lowers[0].dir), cache);
        assert!(image.lowers[0].file.is_empty());
        assert!(image.result_file.is_empty());
        assert_eq!(image.download_override.as_ref().unwrap().concurrency, 3);
        let retained = Path::new(&image.lowers[1].file);
        assert!(retained.starts_with(directory.path()));
        assert_eq!(fs::metadata(retained)?.ino(), inode);
        let path = directory.path().to_path_buf();
        let snapshot = Arc::new(Snapshot::new(id, manifest, directory));
        let reader = Arc::clone(&snapshot);
        drop(file);
        source.close()?;
        assert!(!cache.exists());
        drop(snapshot);
        let mut data = [0; 12];
        fs::File::open(retained)?.read_exact_at(&mut data, 32 * 1024 * 1024)?;
        assert_eq!(&data, b"private data");
        drop(reader);
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn finish_preserves_each_attached_drives_config_and_order() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut capture = crate::snapshot::mock::write_mock_capture(root.path())?.2;
        for (id, delay) in [("scratch", 17), ("data", 23)] {
            let mut image = crate::snapshot::mock::write_mock_capture(&root.path().join(id))?
                .2
                .rootfs;
            image.config.download_override = Some(overlaybd::config::DownloadConfig {
                delay,
                ..Default::default()
            });
            capture.attached_drives.push(CapturedDrive {
                drive_id: id.into(),
                read_only: false,
                mount_path: PathBuf::from("/mnt").join(id),
                sub_path: None,
                image,
            });
        }
        let manifest = capture.finish(root.path())?;
        let drives: Vec<_> = manifest
            .attached_drives
            .iter()
            .map(|drive| {
                let config = load_image_config(&drive.image_config_path)?;
                Ok((
                    drive.drive_id.as_str(),
                    config.download_override.unwrap().delay,
                ))
            })
            .collect::<Result<_>>()?;
        assert_eq!(drives, [("scratch", 17), ("data", 23)]);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn completed_capture_preserves_sparse_layers_across_filesystems() -> Result<()> {
        let source = tempfile::tempdir_in("/tmp")?;
        let roots = tempfile::tempdir_in("/dev/shm")?;
        let directory = allocate_snapshot(roots.path(), &SnapshotId::generate())?;
        let mut capture = crate::snapshot::mock::write_mock_capture(directory.path())?.2;
        let layer = source.path().join("private.commit");
        let file = fs::File::create(&layer)?;
        file.set_len(64 * 1024 * 1024)?;
        file.write_all_at(b"private data", 32 * 1024 * 1024)?;
        capture.memory.layers = vec![LayerSource::File(LayerFile::dense(
            layer,
            uuid::Uuid::nil(),
            None,
        ))]
        .into();
        capture.memory.config = capture.memory.layers.to_runtime_config();
        let manifest = capture.finish(directory.path())?;
        drop(file);
        source.close()?;
        let image = load_image_config(&manifest.memory.image_config_path)?;
        let retained = fs::File::open(&image.lowers[0].file)?;
        let mut data = [0; 12];
        retained.read_exact_at(&mut data, 32 * 1024 * 1024)?;
        assert_eq!(&data, b"private data");
        assert!(retained.metadata()?.blocks() * 512 < 1024 * 1024);
        assert_eq!(
            fs::metadata(directory.path())?.permissions().mode() & 0o777,
            0o700
        );
        Ok(())
    }
}
