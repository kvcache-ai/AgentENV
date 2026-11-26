//! Publish-time preparation of local OverlayBD layers.
//!
//! Local layers always stay raw so local resume pays no decompression cost.
//! When `[snapshot.publish_compression]` is enabled, the OSS/ACR upload paths
//! recontainerize each raw local layer as zfile via single-layer
//! metadata-preserving export before upload: the compressed bytes are what gets
//! uploaded, so every content reference derived here (managed-layer digest/size,
//! OSS object key, ACR blob digest and overlaybd blob annotations) describes
//! the uploaded bytes while preserving the layer's logical UUID and parent UUID.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::OverlaybdCompactOutput;
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::is_zfile;
use tempfile::NamedTempFile;

use crate::cfg::SnapshotPublishCompressionConfig;

impl From<&SnapshotPublishCompressionConfig> for OverlaybdCompactOutput {
    fn from(config: &SnapshotPublishCompressionConfig) -> Self {
        if config.enabled {
            Self::ZFile {
                algorithm: config.algorithm,
                workers: config.workers.clamp(1, Self::MAX_COMPRESSION_WORKERS),
            }
        } else {
            Self::Raw
        }
    }
}

/// A local layer file prepared for upload, plus the content descriptor of
/// exactly the bytes that will be uploaded.
///
/// Owns the temporary output (when one was produced) so it is removed once
/// upload and post-commit P2P publication complete.
#[derive(Debug)]
pub(crate) struct PreparedLayerUpload {
    path: PathBuf,
    digest: String,
    size: u64,
    _temp: Option<NamedTempFile>,
}

impl PreparedLayerUpload {
    /// Local file holding the exact bytes to upload.
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

/// Prepare a captured physical layer for upload under `mode`, preserving its
/// logical UUID and parent UUID when dense-exporting sparse uppers or
/// recontainerizing raw layers as zfile.
///
/// `make_staging` is called only when the layer actually needs to be rewritten,
/// so dense raw and already-ZFile passthrough uploads allocate no temporary file.
pub(crate) async fn prepare_layer_file_upload<F>(
    source: overlaybd::layer::LayerFile,
    mode: OverlaybdCompactOutput,
    make_staging: F,
) -> Result<(PreparedLayerUpload, Option<String>)>
where
    F: FnOnce() -> Result<NamedTempFile> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("create layer preparation runtime")?;
        let mut staged_file: Option<NamedTempFile> = None;
        let mut make_staging = Some(make_staging);
        let mut alloc_staging = || -> Result<PathBuf> {
            let file = (make_staging
                .take()
                .expect("staging file allocated at most once"))()?;
            let path = file.path().to_path_buf();
            staged_file = Some(file);
            Ok(path)
        };
        let file = runtime
            .block_on(async {
                if matches!(mode, OverlaybdCompactOutput::ZFile { .. }) {
                    let probe: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(&source.path)?);
                    if is_zfile(probe).await? != 1 {
                        let staging_path = alloc_staging()?;
                        let output = Arc::new(LocalFile::open_rw(&staging_path, false)?);
                        let (uuid, descriptor) =
                            overlaybd::layer::export_sealed_layer(&source.path, output, mode)
                                .await?;
                        anyhow::ensure!(
                            source.uuid.is_nil() || source.uuid == uuid,
                            "layer identity changed: {}",
                            source.path.display()
                        );
                        return Ok(overlaybd::layer::LayerFile::dense(
                            staging_path,
                            uuid,
                            Some(descriptor),
                        ));
                    }
                }
                overlaybd::layer::prepare_layer_file(&source, alloc_staging, mode).await
            })
            .with_context(|| format!("prepare layer '{}'", source.path.display()))?;
        let descriptor = file.descriptor.expect("prepared layer has a descriptor");
        let uuid = (!file.uuid.is_nil()).then(|| file.uuid.to_string());
        Ok((
            PreparedLayerUpload {
                path: file.path,
                digest: descriptor.digest,
                size: descriptor.size,
                _temp: staged_file,
            },
            uuid,
        ))
    })
    .await
    .context("prepare layer task")?
}

#[cfg(test)]
mod tests {
    use overlaybd::backend::switch::new_switch_file;
    use overlaybd::backend::tar::new_tar_file_adaptor;
    use overlaybd::index_file::{
        create_file_rw, CommitArgs, LSMTFile, LSMTReadOnlyFile, LayerInfo, RwLayout,
    };
    use overlaybd::layer::{read_sealed_layer_uuids, LayerFile};
    use overlaybd::zfile::{CompressArgs, CompressOptions, ZFileCompactWriter};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::cfg::OverlaybdCompressionAlgorithm;
    use crate::digest::FileDigest;

    const VSIZE: u64 = 3 * 4096;

    #[test]
    fn publish_compression_config_maps_to_compact_output() {
        let disabled = SnapshotPublishCompressionConfig {
            enabled: false,
            algorithm: OverlaybdCompressionAlgorithm::Zstd,
            workers: 4,
        };
        assert_eq!(
            OverlaybdCompactOutput::from(&disabled),
            OverlaybdCompactOutput::Raw
        );

        let zstd = SnapshotPublishCompressionConfig {
            enabled: true,
            algorithm: OverlaybdCompressionAlgorithm::Zstd,
            workers: 4,
        };
        assert_eq!(
            OverlaybdCompactOutput::from(&zstd),
            OverlaybdCompactOutput::ZFile {
                algorithm: OverlaybdCompressionAlgorithm::Zstd,
                workers: 4,
            }
        );

        let clamped = SnapshotPublishCompressionConfig {
            enabled: true,
            algorithm: OverlaybdCompressionAlgorithm::Lz4,
            workers: usize::MAX,
        };
        assert_eq!(
            OverlaybdCompactOutput::from(&clamped),
            OverlaybdCompactOutput::ZFile {
                algorithm: OverlaybdCompressionAlgorithm::Lz4,
                workers: OverlaybdCompactOutput::MAX_COMPRESSION_WORKERS,
            }
        );
    }

    /// Write a sealed LSMT layer at `dir/name.commit`, raw or zfile, with explicit
    /// UUID / parent UUID and one distinct byte pattern per 4KiB page.
    async fn create_sealed_layer(
        dir: &Path,
        name: &str,
        zfile: bool,
        uuid: Uuid,
        parent_uuid: Option<Uuid>,
    ) -> PathBuf {
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
        let mut args = if zfile {
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
            CommitArgs::from_writer(writer)
        } else {
            CommitArgs::new(output)
        };
        args.uuid = Some(uuid);
        args.parent_uuid = parent_uuid;
        layer.commit_with_args(args).await.expect("commit layer");
        commit_path
    }

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
        zfile_mode_with_workers(1)
    }

    fn zfile_mode_with_workers(workers: usize) -> OverlaybdCompactOutput {
        OverlaybdCompactOutput::ZFile {
            algorithm: OverlaybdCompressionAlgorithm::Lz4,
            workers,
        }
    }

    #[tokio::test]
    async fn disabled_passes_raw_layer_through() {
        let temp = TempDir::new().expect("tempdir");
        let uuid = Uuid::new_v4();
        let parent_uuid = Some(Uuid::new_v4());
        let source_path = create_sealed_layer(temp.path(), "raw", false, uuid, parent_uuid).await;
        let source = LayerFile::from_path(source_path.clone(), None)
            .await
            .expect("inspect raw layer");

        let (prepared, returned_uuid) =
            prepare_layer_file_upload(source, OverlaybdCompactOutput::Raw, || {
                panic!("dense raw passthrough must not allocate a staging file")
            })
            .await
            .expect("prepare upload");

        assert_eq!(prepared.path(), source_path);
        assert_eq!(returned_uuid.as_deref(), Some(uuid.to_string().as_str()));
        let descriptor = FileDigest::describe(&source_path)
            .await
            .expect("describe raw");
        assert_eq!(prepared.digest(), descriptor.sha256);
        assert_eq!(prepared.size(), descriptor.size);
        assert_eq!(probe_zfile(prepared.path()).await, 0);
        assert_eq!(
            read_sealed_layer_uuids(prepared.path()).await.unwrap(),
            (uuid, parent_uuid)
        );
    }

    #[tokio::test]
    async fn enabled_recontainerizes_raw_layer_as_zfile_preserving_uuids() {
        let temp = TempDir::new().expect("tempdir");
        let uuid = Uuid::new_v4();
        let parent_uuid = Some(Uuid::new_v4());
        let source_path = create_sealed_layer(temp.path(), "raw", false, uuid, parent_uuid).await;
        let source = LayerFile::from_path(source_path.clone(), None)
            .await
            .expect("inspect raw layer");

        for workers in [1, 2] {
            let staging_dir = temp.path().to_path_buf();
            let (prepared, returned_uuid) = prepare_layer_file_upload(
                source.clone(),
                zfile_mode_with_workers(workers),
                move || Ok(NamedTempFile::new_in(&staging_dir)?),
            )
            .await
            .expect("prepare upload");

            assert_ne!(prepared.path(), source_path);
            assert_eq!(returned_uuid.as_deref(), Some(uuid.to_string().as_str()));
            assert_eq!(probe_zfile(prepared.path()).await, 1);
            let descriptor = FileDigest::describe(prepared.path())
                .await
                .expect("describe zfile");
            assert_eq!(prepared.digest(), descriptor.sha256);
            assert_eq!(prepared.size(), descriptor.size);
            let raw_descriptor = FileDigest::describe(&source_path)
                .await
                .expect("describe raw");
            assert_ne!(prepared.digest(), raw_descriptor.sha256);
            assert_eq!(
                read_sealed_layer_uuids(prepared.path()).await.unwrap(),
                (uuid, parent_uuid)
            );
            let expected = read_layer_contents(&source_path).await;
            assert_eq!(read_layer_contents(prepared.path()).await, expected);
        }
    }

    #[tokio::test]
    async fn enabled_skips_zfile_input() {
        let temp = TempDir::new().expect("tempdir");
        let uuid = Uuid::new_v4();
        let parent_uuid = Some(Uuid::new_v4());
        let source_path = create_sealed_layer(temp.path(), "zfile", true, uuid, parent_uuid).await;
        let source = LayerFile::from_path(source_path.clone(), None)
            .await
            .expect("inspect zfile layer");

        let (prepared, returned_uuid) = prepare_layer_file_upload(source, zfile_mode(), || {
            panic!("already-zfile passthrough must not allocate a staging file")
        })
        .await
        .expect("prepare upload");

        assert_eq!(prepared.path(), source_path);
        assert_eq!(returned_uuid.as_deref(), Some(uuid.to_string().as_str()));
        let descriptor = FileDigest::describe(&source_path)
            .await
            .expect("describe zfile");
        assert_eq!(prepared.digest(), descriptor.sha256);
        assert_eq!(prepared.size(), descriptor.size);
        assert_eq!(
            read_sealed_layer_uuids(prepared.path()).await.unwrap(),
            (uuid, parent_uuid)
        );
    }

    #[tokio::test]
    async fn trusted_descriptor_honored_only_without_recontainerize() {
        let temp = TempDir::new().expect("tempdir");
        let uuid = Uuid::new_v4();
        let raw = create_sealed_layer(temp.path(), "raw", false, uuid, None).await;
        let raw_size = std::fs::metadata(&raw).expect("raw metadata").len();
        let trusted = overlaybd::LayerDescriptor {
            digest: "sha256:declared".to_string(),
            size: raw_size,
        };
        let raw_file = LayerFile::dense(raw.clone(), uuid, Some(trusted));

        // Disabled: the trusted descriptor is kept as-is.
        let (prepared, _) =
            prepare_layer_file_upload(raw_file.clone(), OverlaybdCompactOutput::Raw, || {
                panic!("passthrough must not allocate staging")
            })
            .await
            .expect("prepare disabled");
        assert_eq!(prepared.path(), raw);
        assert_eq!(prepared.digest(), "sha256:declared");
        assert_eq!(prepared.size(), raw_size);

        // Enabled on a raw layer: the descriptor was computed over raw bytes,
        // so a fresh descriptor must be computed while writing the zfile.
        let staging_dir = temp.path().to_path_buf();
        let (prepared, _) = prepare_layer_file_upload(raw_file, zfile_mode(), move || {
            Ok(NamedTempFile::new_in(&staging_dir)?)
        })
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
        let zfile = create_sealed_layer(temp.path(), "zfile", true, uuid, None).await;
        let zfile_size = std::fs::metadata(&zfile).expect("zfile metadata").len();
        let zfile_layer = LayerFile::dense(
            zfile.clone(),
            uuid,
            Some(overlaybd::LayerDescriptor {
                digest: "sha256:declared".to_string(),
                size: zfile_size,
            }),
        );
        let (prepared, _) = prepare_layer_file_upload(zfile_layer, zfile_mode(), || {
            panic!("already-zfile passthrough must not allocate staging")
        })
        .await
        .expect("prepare zfile input");
        assert_eq!(prepared.path(), zfile);
        assert_eq!(prepared.digest(), "sha256:declared");
        assert_eq!(prepared.size(), zfile_size);
    }

    #[tokio::test]
    async fn temp_zfile_removed_on_drop() {
        let temp = TempDir::new().expect("tempdir");
        let source_path =
            create_sealed_layer(temp.path(), "raw", false, Uuid::new_v4(), None).await;
        let source = LayerFile::from_path(source_path, None)
            .await
            .expect("inspect raw layer");

        let staging_dir = temp.path().to_path_buf();
        let (prepared, _) = prepare_layer_file_upload(source, zfile_mode(), move || {
            Ok(NamedTempFile::new_in(&staging_dir)?)
        })
        .await
        .expect("prepare upload");
        let path = prepared.path().to_path_buf();
        assert!(path.exists());
        drop(prepared);
        assert!(!path.exists(), "temporary zfile must be removed on drop");
    }

    #[tokio::test]
    async fn dense_export_preserves_uuids_and_matches_final_bytes_in_each_compression_mode() {
        let dir = TempDir::new().expect("tempdir");
        let sparse_path = dir.path().join("snapshot.commit");
        let uuid = Uuid::new_v4();
        let parent_uuid = Some(Uuid::new_v4());
        let file = Arc::new(LocalFile::new(&sparse_path).expect("create sparse layer"));
        let mut info = LayerInfo::new(file, None, VSIZE);
        info.uuid = uuid;
        info.parent_uuid = parent_uuid;
        info.rw_layout = RwLayout::Sparse;
        let layer = create_file_rw(info).await.expect("create sparse layer");
        layer
            .write_at(0, &[0xAB; 4096])
            .await
            .expect("write first page");
        layer
            .write_at(2 * 4096, &[0xCD; 4096])
            .await
            .expect("write last page");
        layer.close_seal().await.expect("seal sparse layer");
        drop(layer);
        let source = LayerFile::from_path(sparse_path, None)
            .await
            .expect("inspect sparse layer");
        assert!(source.is_sparse_rw());

        let mut expected = vec![0u8; VSIZE as usize];
        expected[..4096].fill(0xAB);
        expected[2 * 4096..].fill(0xCD);

        for mode in [
            OverlaybdCompactOutput::Raw,
            zfile_mode(),
            OverlaybdCompactOutput::ZFile {
                algorithm: OverlaybdCompressionAlgorithm::Zstd,
                workers: 1,
            },
        ] {
            let staging_dir = dir.path().to_path_buf();
            let (prepared, returned_uuid) =
                prepare_layer_file_upload(source.clone(), mode, move || {
                    Ok(NamedTempFile::new_in(&staging_dir)?)
                })
                .await
                .expect("export sparse layer in final publish format");

            let bytes = std::fs::read(prepared.path()).expect("read upload bytes");
            assert_eq!(returned_uuid.as_deref(), Some(uuid.to_string().as_str()));
            assert_eq!(prepared.digest(), crate::digest::sha256_digest(&bytes));
            assert_eq!(prepared.size(), bytes.len() as u64);
            assert_eq!(
                read_sealed_layer_uuids(prepared.path()).await.unwrap(),
                (uuid, parent_uuid)
            );
            assert_eq!(
                read_layer_contents(prepared.path()).await,
                expected,
                "export must preserve data and holes in {mode:?}"
            );
            assert_eq!(
                probe_zfile(prepared.path()).await,
                i32::from(!matches!(mode, OverlaybdCompactOutput::Raw))
            );
        }
    }

    #[tokio::test]
    async fn corrupt_input_fails_instead_of_uploading() {
        let temp = TempDir::new().expect("tempdir");
        let bogus = temp.path().join("snapshot.commit");
        std::fs::write(&bogus, b"not-an-overlaybd-layer").expect("write bogus layer");
        let source = LayerFile::dense(bogus, Uuid::nil(), None);

        let staging_dir = temp.path().to_path_buf();
        prepare_layer_file_upload(source, zfile_mode(), move || {
            Ok(NamedTempFile::new_in(&staging_dir)?)
        })
        .await
        .expect_err("corrupt layer must fail the publish");
    }
}
