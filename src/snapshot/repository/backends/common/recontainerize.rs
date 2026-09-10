//! Publish-time recontainerization of local overlaybd layers.
//!
//! Local layers always stay raw so local resume pays no decompression cost.
//! When `[snapshot.publish_compression]` is enabled, the OSS/ACR upload paths
//! recontainerize each raw local layer as zfile exactly once before upload:
//! the compressed bytes are what gets uploaded, so every content reference
//! derived here (managed-layer digest/size, OSS object key, ACR blob digest
//! and overlaybd blob annotations) describes the compressed bytes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use overlaybd::backend::local::LocalFile;
use overlaybd::config::LayerConfig;
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::is_zfile;
use tempfile::NamedTempFile;
use tracing::debug;

use crate::digest::FileDigest;
use crate::sandbox::{compact_layers, OverlaybdCompactOutput};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};

/// A local layer file prepared for upload, plus the content descriptor of
/// exactly the bytes that will be uploaded.
///
/// Owns the temporary recontainerized copy (when one was produced) so it is
/// removed once the upload completes.
#[derive(Debug)]
pub(crate) struct PreparedLayerUpload {
    path: PathBuf,
    digest: String,
    size: u64,
    _temp: Option<NamedTempFile>,
}

impl PreparedLayerUpload {
    /// Local file holding the exact bytes to upload. Differs from the source
    /// path exactly when the layer was recontainerized as zfile.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// `sha256:<hex>` digest of the bytes at [`Self::path`].
    pub(crate) fn digest(&self) -> &str {
        &self.digest
    }

    /// Byte size of the file at [`Self::path`].
    pub(crate) fn size(&self) -> u64 {
        self.size
    }
}

/// Prepare the local layer `source` for upload under the publish-compression
/// `mode`.
///
/// With compression enabled, a raw local layer is compacted into a temporary
/// zfile and the returned descriptor is computed over the compressed bytes.
/// Inputs that are already zfile are uploaded unchanged (idempotent), as are
/// all inputs when compression is disabled.
///
/// `trusted` carries a capture-side descriptor of `source`'s current bytes
/// when the image config provides one. It is only honored when the file is
/// uploaded unchanged; a recontainerized layer no longer matches it, so the
/// zfile is re-hashed instead.
pub(crate) async fn prepare_layer_upload(
    source: &Path,
    mode: OverlaybdCompactOutput,
    trusted: Option<(&str, u64)>,
) -> RepositoryResult<PreparedLayerUpload> {
    if matches!(mode, OverlaybdCompactOutput::Raw) {
        return passthrough(source, trusted).await;
    }

    let probe: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(source).map_err(|e| {
        RepositoryError::backend(
            format!("open layer '{}' for zfile probe", source.display()),
            e,
        )
    })?);
    let zfile = is_zfile(probe).await.map_err(|e| {
        RepositoryError::backend(
            format!("probe zfile header of layer '{}'", source.display()),
            e,
        )
    })?;
    if zfile == 1 {
        return passthrough(source, trusted).await;
    }

    let temp = NamedTempFile::new().map_err(|e| {
        RepositoryError::backend(
            format!(
                "create temp zfile layer for recontainerizing '{}'",
                source.display()
            ),
            e,
        )
    })?;
    let layer = LayerConfig {
        file: source.display().to_string(),
        ..LayerConfig::default()
    };
    let Some(recontainerized_path) =
        compact_layers(&[layer], temp.path(), mode)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("recontainerize layer '{}' as zfile", source.display()),
                    e,
                )
            })?
    else {
        // A single input layer always produces output; fall back to the
        // original bytes if the compactor ever reports no data.
        return passthrough(source, trusted).await;
    };
    debug!(
        source = %source.display(),
        output = %recontainerized_path.display(),
        "recontainerized local layer as zfile for upload; layer uuid will be unset"
    );

    let descriptor = FileDigest::describe(&recontainerized_path)
        .await
        .map_err(|e| {
            RepositoryError::backend(
                format!(
                    "describe recontainerized zfile layer '{}'",
                    recontainerized_path.display()
                ),
                e,
            )
        })?;
    Ok(PreparedLayerUpload {
        path: recontainerized_path,
        digest: descriptor.sha256,
        size: descriptor.size,
        _temp: Some(temp),
    })
}

/// Upload the source bytes unchanged: honor the trusted descriptor when one
/// is available, otherwise hash the file.
async fn passthrough(
    source: &Path,
    trusted: Option<(&str, u64)>,
) -> RepositoryResult<PreparedLayerUpload> {
    let (digest, size) = match trusted {
        Some((digest, size)) => (digest.to_string(), size),
        None => {
            let descriptor = FileDigest::describe(source).await.map_err(|e| {
                RepositoryError::backend(
                    format!("describe managed layer '{}'", source.display()),
                    e,
                )
            })?;
            (descriptor.sha256, descriptor.size)
        }
    };
    Ok(PreparedLayerUpload {
        path: source.to_path_buf(),
        digest,
        size,
        _temp: None,
    })
}

#[cfg(test)]
mod tests {
    use overlaybd::backend::switch::new_switch_file;
    use overlaybd::backend::tar::new_tar_file_adaptor;
    use overlaybd::index_file::{CommitArgs, LSMTFile, LSMTReadOnlyFile};
    use overlaybd::zfile::{CompressArgs, CompressOptions, ZFileCompactWriter};
    use tempfile::TempDir;

    use super::*;
    use crate::cfg::OverlaybdCompressionAlgorithm;

    const VSIZE: u64 = 3 * 4096;

    /// Write a sealed LSMT layer at `dir/name.commit`, raw or zfile, with one
    /// distinct byte pattern per 4KiB page.
    async fn create_sealed_layer(dir: &Path, name: &str, zfile: bool) -> PathBuf {
        let data = Arc::new(
            LocalFile::new(dir.join(format!("{name}.data"))).expect("create layer data file"),
        );
        let index = Arc::new(
            LocalFile::new(dir.join(format!("{name}.index"))).expect("create layer index file"),
        );
        let layer = LSMTFile::create(data, Some(index), VSIZE, false)
            .await
            .expect("create layer");
        for (page, byte) in [(0u64, 0x11u8), (4096, 0x22), (8192, 0x33)] {
            layer
                .write_at(page, &[byte; 4096])
                .await
                .expect("write layer page");
        }
        let commit_path = dir.join(format!("{name}.commit"));
        let output: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&commit_path).expect("create layer commit output"));
        if zfile {
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
        } else {
            layer
                .commit_with_args(CommitArgs::new(output))
                .await
                .expect("commit raw layer");
        }
        commit_path
    }

    /// Read a sealed layer's full logical contents through the same tar +
    /// switch chain the runtime uses, transparently handling raw and zfile.
    async fn read_layer_contents(path: &Path) -> Vec<u8> {
        let local: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::open_ro(path).expect("open sealed layer"));
        let display = path.display().to_string();
        let tar_adapted = new_tar_file_adaptor(local).await.expect("tar adaptor");
        let switched = new_switch_file(tar_adapted, true, Some(&display))
            .await
            .expect("switch file");
        let layer = LSMTReadOnlyFile::open(switched)
            .await
            .expect("open sealed layer as LSMT");
        layer
            .read_at(0, VSIZE as usize)
            .await
            .expect("read sealed layer")
            .to_vec()
    }

    async fn probe_zfile(path: &Path) -> i32 {
        let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(path).expect("open layer"));
        is_zfile(file).await.expect("probe zfile header")
    }

    fn zfile_mode() -> OverlaybdCompactOutput {
        OverlaybdCompactOutput::ZFile {
            algorithm: OverlaybdCompressionAlgorithm::Lz4,
            workers: 1,
        }
    }

    #[tokio::test]
    async fn disabled_passes_raw_layer_through() {
        let temp = TempDir::new().expect("tempdir");
        let source = create_sealed_layer(temp.path(), "raw", false).await;

        let prepared = prepare_layer_upload(&source, OverlaybdCompactOutput::Raw, None)
            .await
            .expect("prepare upload");

        assert_eq!(prepared.path(), source);
        let descriptor = FileDigest::describe(&source).await.expect("describe raw");
        assert_eq!(prepared.digest(), descriptor.sha256);
        assert_eq!(prepared.size(), descriptor.size);
        assert_eq!(probe_zfile(prepared.path()).await, 0);
    }

    #[tokio::test]
    async fn enabled_recontainerizes_raw_layer_as_zfile() {
        let temp = TempDir::new().expect("tempdir");
        let source = create_sealed_layer(temp.path(), "raw", false).await;

        let prepared = prepare_layer_upload(&source, zfile_mode(), None)
            .await
            .expect("prepare upload");

        assert_ne!(prepared.path(), source);
        assert_eq!(probe_zfile(prepared.path()).await, 1);
        // The descriptor must describe the uploaded (compressed) bytes.
        let descriptor = FileDigest::describe(prepared.path())
            .await
            .expect("describe zfile");
        assert_eq!(prepared.digest(), descriptor.sha256);
        assert_eq!(prepared.size(), descriptor.size);
        let raw_descriptor = FileDigest::describe(&source).await.expect("describe raw");
        assert_ne!(prepared.digest(), raw_descriptor.sha256);
        // The compressed layer must decode to the same logical contents.
        let expected = read_layer_contents(&source).await;
        assert_eq!(read_layer_contents(prepared.path()).await, expected);
    }

    #[tokio::test]
    async fn enabled_skips_zfile_input() {
        let temp = TempDir::new().expect("tempdir");
        let source = create_sealed_layer(temp.path(), "zfile", true).await;

        let prepared = prepare_layer_upload(&source, zfile_mode(), None)
            .await
            .expect("prepare upload");

        assert_eq!(prepared.path(), source);
        let descriptor = FileDigest::describe(&source).await.expect("describe zfile");
        assert_eq!(prepared.digest(), descriptor.sha256);
        assert_eq!(prepared.size(), descriptor.size);
    }

    #[tokio::test]
    async fn trusted_descriptor_honored_only_without_recontainerize() {
        let temp = TempDir::new().expect("tempdir");
        let raw = create_sealed_layer(temp.path(), "raw", false).await;
        let raw_size = std::fs::metadata(&raw).expect("raw metadata").len();
        let trusted = ("sha256:declared", raw_size);

        // Disabled: the trusted descriptor is kept as-is.
        let prepared = prepare_layer_upload(&raw, OverlaybdCompactOutput::Raw, Some(trusted))
            .await
            .expect("prepare disabled");
        assert_eq!(prepared.path(), raw);
        assert_eq!(prepared.digest(), "sha256:declared");
        assert_eq!(prepared.size(), raw_size);

        // Enabled on a raw layer: the descriptor was computed over raw bytes,
        // so the recontainerized zfile must be re-hashed.
        let prepared = prepare_layer_upload(&raw, zfile_mode(), Some(trusted))
            .await
            .expect("prepare enabled");
        assert_ne!(prepared.path(), raw);
        let descriptor = FileDigest::describe(prepared.path())
            .await
            .expect("describe zfile");
        assert_eq!(prepared.digest(), descriptor.sha256);
        assert_ne!(prepared.digest(), "sha256:declared");

        // Enabled on an already-zfile layer: the input is uploaded unchanged,
        // so the trusted descriptor still describes the physical bytes.
        let zfile = create_sealed_layer(temp.path(), "zfile", true).await;
        let zfile_size = std::fs::metadata(&zfile).expect("zfile metadata").len();
        let prepared =
            prepare_layer_upload(&zfile, zfile_mode(), Some(("sha256:declared", zfile_size)))
                .await
                .expect("prepare zfile input");
        assert_eq!(prepared.path(), zfile);
        assert_eq!(prepared.digest(), "sha256:declared");
        assert_eq!(prepared.size(), zfile_size);
    }

    #[tokio::test]
    async fn temp_zfile_removed_on_drop() {
        let temp = TempDir::new().expect("tempdir");
        let source = create_sealed_layer(temp.path(), "raw", false).await;

        let prepared = prepare_layer_upload(&source, zfile_mode(), None)
            .await
            .expect("prepare upload");
        let path = prepared.path().to_path_buf();
        assert!(path.exists());
        drop(prepared);
        assert!(!path.exists(), "temporary zfile must be removed on drop");
    }

    #[tokio::test]
    async fn corrupt_input_fails_probe_instead_of_uploading() {
        let temp = TempDir::new().expect("tempdir");
        let bogus = temp.path().join("snapshot.commit");
        std::fs::write(&bogus, b"not-an-overlaybd-layer").expect("write bogus layer");

        prepare_layer_upload(&bogus, zfile_mode(), None)
            .await
            .expect_err("corrupt layer must fail the publish");
    }
}
