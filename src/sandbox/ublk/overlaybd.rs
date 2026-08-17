use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{LayerConfig, UpperMode};
use overlaybd::index_file::{merge_files_ro, CommitArgs, DigestTrackingFile, OrderedWriter};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::{CompressArgs, CompressOptions, ZFileCompactWriter};
use overlaybd::LayerDescriptor;
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

use super::device::UblkDevice;
use crate::cfg::{MemorySnapshotCompressionAlgorithm, MemorySnapshotConfig};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OverlaybdCompactOutput {
    Raw,
    ZFile {
        algorithm: MemorySnapshotCompressionAlgorithm,
        workers: usize,
    },
}

impl OverlaybdCompactOutput {
    /// Maximum persistent compression threads. Prevents absurd config values
    /// from spawning thousands of OS threads during a production pause.
    const MAX_COMPRESSION_WORKERS: usize = 64;

    pub(crate) fn from_memory_snapshot_config(config: &MemorySnapshotConfig) -> Self {
        if config.compression_enabled {
            Self::ZFile {
                algorithm: config.compression_algorithm,
                workers: config
                    .compression_workers
                    .clamp(1, Self::MAX_COMPRESSION_WORKERS),
            }
        } else {
            Self::Raw
        }
    }
}

pub(crate) async fn create_commit_args(
    output: Arc<dyn VirtualFile>,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
) -> Result<CommitArgs> {
    let mut args = match mode {
        OverlaybdCompactOutput::Raw => CommitArgs::new(output),
        OverlaybdCompactOutput::ZFile { algorithm, workers } => {
            let algorithm = match algorithm {
                MemorySnapshotCompressionAlgorithm::Lz4 => CompressOptions::LZ4,
                MemorySnapshotCompressionAlgorithm::Zstd => CompressOptions::ZSTD,
            };
            let mut compress_args = CompressArgs::new(CompressOptions::new(
                algorithm,
                CompressOptions::DEFAULT_BLOCK_SIZE,
                0,
            ));
            compress_args.workers = workers.max(1);
            CommitArgs::from_writer(Arc::new(
                ZFileCompactWriter::new(output, &compress_args).await?,
            ))
        }
    };
    args.concurrency = concurrency;
    Ok(args)
}

/// Like [`create_commit_args`], but tracks a content descriptor (streaming
/// sha256 + size) of the sealed output at no extra I/O cost. The descriptor
/// is `None` when writes were not strictly ordered.
pub(crate) async fn create_commit_args_with_digest(
    output: Arc<dyn VirtualFile>,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
) -> Result<(CommitArgs, Arc<DigestTrackingFile>)> {
    let tracker = Arc::new(DigestTrackingFile::new(output));
    let mut args = create_commit_args(tracker.clone(), mode, concurrency).await?;
    args.writer = Arc::new(OrderedWriter::new(args.writer.clone()));
    Ok((args, tracker))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OverlaybdConfig {
    pub image_config_path: PathBuf,
    pub read_only: bool,
    #[serde(default = "default_runtime_upper_mode")]
    pub runtime_upper_mode: UpperMode,
}

fn default_runtime_upper_mode() -> UpperMode {
    UpperMode::LogStructured
}

#[derive(Clone, Debug)]
pub(crate) struct OverlaybdRuntimeHandle {
    pub(crate) device: UblkDevice,
    pub(crate) image_config_path: PathBuf,
    pub(crate) actual_virtual_size: u64,
}

/// Compact multiple overlaybd layers into one sealed commit at `output_path`.
///
/// Each input layer is opened through the tar adaptor + switch file chain so
/// raw and ZFile-compressed layers can be mixed freely. The merged commit is
/// written directly in the requested `mode` to a sibling temp file and
/// atomically renamed into place; on failure the temp file is removed and any
/// pre-existing output is left untouched.
pub(crate) async fn compact_layers(
    layers: &[LayerConfig],
    output_path: &Path,
    mode: OverlaybdCompactOutput,
) -> Result<Option<(PathBuf, Option<LayerDescriptor>)>> {
    if layers.is_empty() {
        return Ok(None);
    }

    let io_ring = overlaybd::transient_io_ring::shared_transient_io_ring();

    let mut src_files: Vec<Arc<dyn VirtualFile>> = Vec::with_capacity(layers.len());
    for layer in layers {
        let path = Path::new(&layer.file);
        let local: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(path, io_ring.clone())
                .await
                .with_context(|| format!("open layer for compaction: {}", path.display()))?,
        );
        let tar_adapted = overlaybd::backend::tar::new_tar_file_adaptor(local)
            .await
            .with_context(|| format!("adapt layer as tar file: {}", path.display()))?;
        let display = path.display().to_string();
        let switched =
            overlaybd::backend::switch::new_switch_file(tar_adapted, true, Some(&display))
                .await
                .with_context(|| format!("open layer via switch file: {}", path.display()))?;
        src_files.push(switched);
    }

    let lower_tmp = output_path.with_extension(format!("commit.{}.tmp", Uuid::now_v7()));
    let build_result: Result<Option<LayerDescriptor>> = async {
        let output_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp, io_ring)
                .await
                .context("create compacted layer output file")?,
        );
        let (commit_args, digest_tracker) =
            create_commit_args_with_digest(output_file, mode, 32).await?;
        merge_files_ro(&src_files, commit_args)
            .await
            .context("merge layers")?;
        tokio::fs::rename(&lower_tmp, output_path)
            .await
            .with_context(|| {
                format!(
                    "publish compacted layer to {} failed",
                    output_path.display()
                )
            })?;
        Ok(digest_tracker.descriptor())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    let descriptor = build_result?;

    debug!(
        output = %output_path.display(),
        input_layers = layers.len(),
        "compacted overlaybd layers"
    );
    Ok(Some((output_path.to_path_buf(), descriptor)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlaybd::backend::switch::new_switch_file;
    use overlaybd::backend::tar::new_tar_file_adaptor;
    use overlaybd::index_file::{LSMTFile, LSMTReadOnlyFile};
    use overlaybd::transient_io_ring::shared_transient_io_ring;
    use overlaybd::zfile::is_zfile;

    async fn create_sealed_layer(
        dir: &Path,
        name: &str,
        vsize: u64,
        writes: &[(u64, u8)],
        zfile_algo: Option<u8>,
    ) -> PathBuf {
        let io_ring = shared_transient_io_ring();
        let data = Arc::new(
            LocalFile::new(dir.join(format!("{name}.data")), io_ring.clone())
                .await
                .expect("create layer data file"),
        );
        let index = Arc::new(
            LocalFile::new(dir.join(format!("{name}.index")), io_ring.clone())
                .await
                .expect("create layer index file"),
        );
        let layer = LSMTFile::create(data, Some(index), vsize, false)
            .await
            .expect("create layer");
        for (offset, byte) in writes {
            layer
                .write_at(*offset, &[*byte; 4096])
                .await
                .expect("write layer page");
        }
        let commit_path = dir.join(format!("{name}.commit"));
        let output: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&commit_path, io_ring)
                .await
                .expect("create layer commit output"),
        );
        match zfile_algo {
            Some(algo) => {
                let compress_args = CompressArgs::new(CompressOptions::new(
                    algo,
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
            None => {
                layer
                    .commit_with_args(CommitArgs::new(output))
                    .await
                    .expect("commit raw layer");
            }
        }
        commit_path
    }

    async fn read_sealed_layer(path: &Path, len: usize) -> (i32, Vec<u8>) {
        let local: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(path, shared_transient_io_ring())
                .await
                .expect("open sealed layer"),
        );
        let zfile_flag = is_zfile(local.clone()).await.expect("probe zfile header");
        let display = path.display().to_string();
        let tar_adapted = new_tar_file_adaptor(local).await.expect("tar adaptor");
        let switched = new_switch_file(tar_adapted, true, Some(&display))
            .await
            .expect("switch file");
        let layer = LSMTReadOnlyFile::open(switched)
            .await
            .expect("open sealed layer as LSMT");
        let data = layer.read_at(0, len).await.expect("read sealed layer");
        (zfile_flag, data.to_vec())
    }

    fn layer_config(path: &Path) -> LayerConfig {
        LayerConfig {
            file: path.display().to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn compact_layers_mixed_input_and_configured_output() {
        let temp = tempfile::tempdir().expect("tempdir");
        let vsize = 3 * 4096u64;
        let bottom = create_sealed_layer(
            temp.path(),
            "bottom",
            vsize,
            &[(0, 0x11), (4096, 0x11), (8192, 0x11)],
            None,
        )
        .await;
        let middle = create_sealed_layer(
            temp.path(),
            "middle",
            vsize,
            &[(4096, 0x22)],
            Some(CompressOptions::LZ4),
        )
        .await;
        let top = create_sealed_layer(
            temp.path(),
            "top",
            vsize,
            &[(8192, 0x33)],
            Some(CompressOptions::ZSTD),
        )
        .await;
        let layers = [&bottom, &middle, &top]
            .into_iter()
            .map(|path| layer_config(path))
            .collect::<Vec<_>>();

        // Later layers take precedence over earlier ones on overlapping pages.
        let mut expected = vec![0u8; vsize as usize];
        expected[..4096].fill(0x11);
        expected[4096..8192].fill(0x22);
        expected[8192..].fill(0x33);

        for (name, mode, expected_zfile) in [
            ("raw", OverlaybdCompactOutput::Raw, 0),
            (
                "lz4",
                OverlaybdCompactOutput::ZFile {
                    algorithm: MemorySnapshotCompressionAlgorithm::Lz4,
                    // Exercise the segmented parallel compression path.
                    workers: 2,
                },
                1,
            ),
            (
                "zstd",
                OverlaybdCompactOutput::ZFile {
                    algorithm: MemorySnapshotCompressionAlgorithm::Zstd,
                    workers: 1,
                },
                1,
            ),
        ] {
            let output_path = temp.path().join(format!("compacted-{name}.commit"));
            let (published, descriptor) = compact_layers(&layers, &output_path, mode)
                .await
                .expect("compact mixed raw/lz4/zstd layers")
                .expect("compaction produces an output");
            assert_eq!(published, output_path);
            assert!(!output_path.with_extension("commit.tmp").exists());

            // The streamed digest must match a plain sha256 of the output file.
            let descriptor = descriptor.expect("digest descriptor for compacted layer");
            let file_bytes = std::fs::read(&output_path).expect("read compacted output");
            assert_eq!(descriptor.size, file_bytes.len() as u64, "mode {name}");
            assert_eq!(
                descriptor.digest,
                crate::digest::sha256_digest(&file_bytes),
                "mode {name}"
            );

            let (zfile_flag, data) = read_sealed_layer(&output_path, vsize as usize).await;
            assert_eq!(zfile_flag, expected_zfile, "mode {name}");
            assert_eq!(data, expected, "mode {name}");
        }

        // A failed compaction must neither clobber a previously published
        // output nor leave the sibling temp file behind.
        let output_path = temp.path().join("compacted-raw.commit");
        let before = std::fs::read(&output_path).expect("read published output");
        let bogus = temp.path().join("bogus.commit");
        std::fs::write(&bogus, vec![0xAB; 4096]).expect("write bogus layer");
        let mut broken = layers.clone();
        broken.push(layer_config(&bogus));
        compact_layers(&broken, &output_path, OverlaybdCompactOutput::Raw)
            .await
            .expect_err("invalid input layer must fail");
        assert_eq!(
            std::fs::read(&output_path).expect("read published output"),
            before
        );
        assert!(!output_path.with_extension("commit.tmp").exists());
    }
}
