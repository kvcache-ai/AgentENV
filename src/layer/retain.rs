//! Shared hard-link and sparse-preserving copy helpers for retaining local
//! OverlayBD layer files inside a self-contained snapshot or volume directory.

use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use overlaybd::config::{validate_image_config, ImageConfig};
use tracing::warn;

use super::{write_image_config, ExternalLayer, LayerChain, LayerRef, LayerSource};

/// Standard subdirectory for newly retained local layers beneath a snapshot or
/// volume config directory (`layers/<index>/<filename>`).
pub(crate) const RETAINED_LAYERS_DIR: &str = "layers";

/// Compute the retained layer location `<base_dir>/layers/<index>/<filename>`.
pub(crate) fn retained_layer_path(base_dir: &Path, index: usize, source: &Path) -> PathBuf {
    let file_name = source
        .file_name()
        .unwrap_or_else(|| OsStr::new("layer.commit"));
    base_dir
        .join(RETAINED_LAYERS_DIR)
        .join(index.to_string())
        .join(file_name)
}

/// Copy a quiesced layer file while preserving sparse holes when present,
/// falling back to a dense copy on filesystems without `SEEK_DATA`/`SEEK_HOLE`.
pub(crate) fn copy_layer_file_blocking(source: &Path, destination: &Path) -> io::Result<()> {
    let metadata = fs::metadata(source)?;
    if metadata.blocks() * 512 >= metadata.len() {
        if let Err(error) = fs::copy(source, destination) {
            let _ = fs::remove_file(destination);
            return Err(error);
        }
        return Ok(());
    }
    match storage_util::copy_sparse_file_blocking(source, destination) {
        Ok(()) => Ok(()),
        // Sealed layers carry their index; a dense copy preserves their content
        // on filesystems that cannot report sparse extents.
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
            ) =>
        {
            if let Err(copy_err) = fs::copy(source, destination) {
                let _ = fs::remove_file(destination);
                return Err(copy_err);
            }
            Ok(())
        }
        Err(error) => {
            let _ = fs::remove_file(destination);
            Err(error)
        }
    }
}

/// Retain `source` at `destination` via hard link when possible, reusing an
/// already-linked identical inode (including read-only files) and falling back
/// to a sparse-preserving copy across filesystems or when hard-linking fails.
pub(crate) fn retain_layer_file_blocking(source: &Path, destination: &Path) -> io::Result<()> {
    retain_layer_file_with(source, destination, |src, dst| fs::hard_link(src, dst))
}

fn retain_layer_file_with<F>(source: &Path, destination: &Path, mut link: F) -> io::Result<()>
where
    F: FnMut(&Path, &Path) -> io::Result<()>,
{
    if source == destination {
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    if same_inode(source, destination)? {
        return Ok(());
    }
    match link(source, destination) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if same_inode(source, destination)? {
                return Ok(());
            }
            fs::remove_file(destination)?;
            if let Err(retry_error) = link(source, destination) {
                warn!(
                    source = %source.display(),
                    destination = %destination.display(),
                    error = %retry_error,
                    "hard-link layer artifact failed; falling back to copy"
                );
                copy_layer_file_blocking(source, destination)
            } else {
                Ok(())
            }
        }
        Err(error) => {
            warn!(
                source = %source.display(),
                destination = %destination.display(),
                error = %error,
                "hard-link layer artifact failed; falling back to copy"
            );
            match fs::remove_file(destination) {
                Ok(()) => {}
                Err(remove_err) if remove_err.kind() == io::ErrorKind::NotFound => {}
                Err(remove_err) => return Err(remove_err),
            }
            copy_layer_file_blocking(source, destination)
        }
    }
}

fn same_inode(source: &Path, destination: &Path) -> io::Result<bool> {
    let retained = match fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let original = fs::metadata(source)?;
    Ok(original.dev() == retained.dev() && original.ino() == retained.ino())
}

impl LayerChain {
    /// Retain any local layer files outside `root` beneath `<parent>/layers/<index>/`,
    /// validate remote-only references, allow the caller to attach node-local cache
    /// hints, and atomically write the completed OverlayBD image config at `path`.
    pub(crate) fn finalize_retained_config(
        &mut self,
        path: &Path,
        root: &Path,
        annotate_image: impl FnOnce(&mut ImageConfig),
    ) -> Result<()> {
        let root = fs::canonicalize(root)
            .with_context(|| format!("resolve retained layer root {}", root.display()))?;
        let parent = path.parent().context("retained image config parent")?;
        for (index, layer) in self.layers.iter_mut().enumerate() {
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
                                    && self
                                        .managed_repo_blob_url
                                        .as_ref()
                                        .is_some_and(|url| !url.is_empty())
                            }
                            LayerRef::External(ExternalLayer {
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
                .with_context(|| format!("resolve retained layer {}", retained_path.display()))?;
            let retained = if source.starts_with(&root) {
                source
            } else {
                let destination = retained_layer_path(parent, index, &source);
                retain_layer_file_blocking(&source, &destination)
                    .with_context(|| format!("retain layer {}", source.display()))?;
                destination
            };
            *retained_path = retained;
        }
        let mut image = self.to_runtime_config();
        annotate_image(&mut image);
        validate_image_config(&image)?;
        write_image_config(path, &image, "completed image config")
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};

    use overlaybd::config::load_image_config;
    use overlaybd::layer::LayerFile;

    use super::*;

    #[test]
    fn retained_layer_path_uses_layers_index_layout() {
        let base = Path::new("/snapshots/snap-1/rootfs");
        assert_eq!(
            retained_layer_path(base, 0, Path::new("/runtime/upper.commit")),
            PathBuf::from("/snapshots/snap-1/rootfs/layers/0/upper.commit")
        );
        assert_eq!(
            retained_layer_path(base, 12, Path::new("/runtime/delta.commit")),
            PathBuf::from("/snapshots/snap-1/rootfs/layers/12/delta.commit")
        );
    }

    #[test]
    fn cross_device_fallback_copies_sparse_layer_and_survives_source_removal() -> io::Result<()> {
        let source_dir = tempfile::tempdir()?;
        let dest_dir = tempfile::tempdir()?;
        let source = source_dir.path().join("delta.commit");
        let file = fs::File::create(&source)?;
        file.set_len(64 * 1024 * 1024)?;
        file.write_all_at(b"sparse payload", 32 * 1024 * 1024)?;
        drop(file);
        let source_inode = (fs::metadata(&source)?.dev(), fs::metadata(&source)?.ino());

        let destination = retained_layer_path(dest_dir.path(), 2, &source);
        retain_layer_file_with(&source, &destination, |_, _| {
            Err(io::Error::from_raw_os_error(libc::EXDEV))
        })?;

        let dest_meta = fs::metadata(&destination)?;
        assert_ne!((dest_meta.dev(), dest_meta.ino()), source_inode);
        assert!(dest_meta.blocks() * 512 < 1024 * 1024);

        // Removing the previous generation directory must leave the retained layer readable.
        source_dir.close()?;
        let mut buf = [0u8; 14];
        fs::File::open(&destination)?.read_exact_at(&mut buf, 32 * 1024 * 1024)?;
        assert_eq!(&buf, b"sparse payload");
        Ok(())
    }

    #[test]
    fn reuses_existing_read_only_hard_link_and_replaces_stale_file() -> io::Result<()> {
        let temp = tempfile::tempdir()?;
        let source = temp.path().join("source.commit");
        fs::write(&source, b"sealed content")?;
        fs::set_permissions(&source, fs::Permissions::from_mode(0o444))?;

        let retained = retained_layer_path(temp.path(), 0, &source);
        fs::create_dir_all(retained.parent().unwrap())?;
        fs::hard_link(&source, &retained)?;

        retain_layer_file_blocking(&source, &retained)?;
        assert_eq!(fs::metadata(&retained)?.ino(), fs::metadata(&source)?.ino());

        let stale = retained_layer_path(temp.path(), 1, &source);
        fs::create_dir_all(stale.parent().unwrap())?;
        fs::write(&stale, b"stale")?;
        retain_layer_file_blocking(&source, &stale)?;
        assert_eq!(fs::read(&stale)?, b"sealed content");
        assert_eq!(fs::metadata(&stale)?.ino(), fs::metadata(&source)?.ino());
        Ok(())
    }

    #[test]
    fn volume_capture_reuses_a_previously_retained_read_only_layer() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let workspace = temporary.path().canonicalize()?;
        let source = workspace.join("legacy.commit");
        fs::write(&source, b"sealed layer")?;
        fs::set_permissions(&source, fs::Permissions::from_mode(0o444))?;
        let volume = workspace.join("volume");
        let retained = volume.join("layers/0/legacy.commit");
        fs::create_dir_all(retained.parent().unwrap())?;
        fs::hard_link(&source, &retained)?;

        let mut layers: LayerChain = vec![LayerSource::File(LayerFile::dense(
            source.clone(),
            uuid::Uuid::nil(),
            None,
        ))]
        .into();
        let image_config = volume.join("config.json");
        layers.finalize_retained_config(&image_config, &volume, |_| {})?;

        assert_eq!(
            load_image_config(&image_config)?.lowers[0].file,
            retained.to_str().unwrap()
        );
        assert_eq!(fs::read(&source)?, b"sealed layer");
        assert_eq!(fs::metadata(&retained)?.ino(), fs::metadata(&source)?.ino());
        Ok(())
    }
}
