//! Backend-neutral memory layer construction and paused memory state.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::layer::{LayerChain, LayerSource};
use crate::sandbox::overlaybd_snapshot::{
    record_sealed_upper_size, rewrite_lowers_with_runtime_roots, snapshot_compaction_range,
    write_image_config,
};
use crate::sandbox::{SandboxCaptureError, SandboxCaptureResult};
use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{ImageConfig, OverlaybdCompactOutput, UpperMode};
use overlaybd::index::SegmentMapping;
use overlaybd::index_file::{compact_to, open_file_rw};
use overlaybd::layer::LayerFile;
use overlaybd::layer_output::create_commit_args;
use overlaybd::virtual_file::VirtualFile;
use tracing::warn;
use uuid::Uuid;

pub(crate) const OVERLAYBD_ALIGNMENT: u64 = 512;
const DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY: usize = 32;

/// Commit a resumed memory upper while retaining the current base until preparation succeeds.
pub(super) async fn commit_memory_upper(
    image: &mut ImageConfig,
    layers: &mut LayerChain,
    config_path: &Path,
    global_config: &Path,
    roots: &[PathBuf],
    compute_digest: bool,
) -> SandboxCaptureResult<()> {
    let path = PathBuf::from(&image.upper.data);
    let file: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::open_rw(&path, false).context("open paused memory upper")?);
    let index: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::open_rw(path.with_extension("index"), false)?);
    let upper = open_file_rw(file, Some(index)).await?;
    // The resumed upper may contain the previous pause even when this capture has no dirty pages.
    if !layers.layers.is_empty() && upper.index().read().await.upper_len() == 0 {
        return Ok(());
    }
    let size = upper.size().await?;
    let directory = tempfile::Builder::new()
        .prefix("memory-base-")
        .tempdir_in(path.parent().context("memory upper has no parent")?)
        .context("prepare memory base directory")?;
    let lower = directory.path().join("snapshot.commit");
    let layer = LayerSource::File(LayerFile::sealed_sparse(lower.clone(), upper.get_uuid(0)?));
    let includes_delta = snapshot_compaction_range(layers, roots, Some(&layer), false)
        .is_some_and(|range| range.end > layers.layers.len());
    // History merging and capacity rejection can finish before the irreversible seal.
    let prepared = if includes_delta {
        None
    } else {
        Some(
            rewrite_lowers_with_runtime_roots(
                layers,
                global_config,
                directory.path(),
                Some(layer.clone()),
                "mem_compacted.commit",
                roots,
                OverlaybdCompactOutput::Raw,
                compute_digest,
            )
            .await?,
        )
    };
    let mut previous: Vec<_> = layers
        .layers
        .iter()
        .filter_map(|layer| layer.path().map(Path::to_path_buf))
        .collect();
    previous.push(lower.clone());
    // After sealing, the live reader may own files here even if config commit fails.
    let directory = directory.keep();
    async {
        upper.close_seal().await?;
        drop(upper);
        tokio::fs::rename(&path, &lower).await?;
        prepare_sparse_memory_upper(&path, size)?;
        let (lowers, compacted) = if let Some(prepared) = prepared {
            prepared
        } else {
            rewrite_lowers_with_runtime_roots(
                layers,
                global_config,
                &directory,
                Some(layer),
                "mem_compacted.commit",
                roots,
                OverlaybdCompactOutput::Raw,
                compute_digest,
            )
            .await?
        };
        *layers = lowers;
        let projected = layers.to_runtime_config();
        image.lowers = projected.lowers;
        image.repo_blob_url = projected.repo_blob_url;
        write_image_config(config_path, image, "sealed memory base")?;
        record_sealed_upper_size("memory", &lower, compacted).await;
        Ok::<(), anyhow::Error>(())
    }
    .await
    .map_err(SandboxCaptureError::terminal)?;
    remove_replaced_memory_layers(config_path, &previous, layers).await;
    Ok(())
}

pub(super) fn prepare_sparse_memory_upper(path: &Path, virtual_size: u64) -> Result<()> {
    overlaybd::helper::prepare_runtime_upper(
        path,
        Some(&path.with_extension("index")),
        virtual_size,
        UpperMode::Sparse,
    )
    .with_context(|| format!("prepare sparse memory upper {}", path.display()))
}

pub(super) async fn remove_replaced_memory_layers(
    config_path: &Path,
    previous: &[PathBuf],
    lowers: &LayerChain,
) {
    let Some(runtime_root) = config_path
        .parent()
        .and_then(|parent| std::fs::canonicalize(parent).ok())
    else {
        return;
    };
    let retained: Vec<_> = lowers
        .layers
        .iter()
        .filter_map(|layer| layer.path().map(Path::to_path_buf))
        .map(|path| std::fs::canonicalize(&path).unwrap_or(path))
        .collect();
    for path in previous {
        let Ok(path) = std::fs::canonicalize(path) else {
            continue;
        };
        if !path.starts_with(&runtime_root) || retained.contains(&path) {
            continue;
        }
        // The running device keeps its open inode; captured snapshots own their
        // links. Only this runtime's replaced name is no longer needed.
        if let Err(error) = tokio::fs::remove_file(&path).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(%error, path = %path.display(), "failed to reclaim memory layer");
            }
        }
        let mut parent = path.parent();
        while let Some(directory) = parent.filter(|directory| *directory != runtime_root) {
            if tokio::fs::remove_dir(directory).await.is_err() {
                break;
            }
            parent = directory.parent();
        }
    }
}

async fn write_memory_layer(
    src_layers: &[Arc<dyn VirtualFile>],
    mappings: &[SegmentMapping],
    virtual_size: u64,
    output_path: &Path,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
    compute_digest: bool,
) -> Result<LayerSource> {
    let lower_tmp = output_path.with_extension("commit.tmp");
    let build_result = async {
        let output_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp)
                .with_context(|| format!("create temp lower failed: {}", lower_tmp.display()))?,
        );
        let mut commit_args = create_commit_args(output_file, mode, compute_digest).await?;
        commit_args.concurrency = concurrency;
        let uuid = Uuid::new_v4();
        commit_args.uuid = Some(uuid);
        let descriptor = compact_to(src_layers, mappings, virtual_size, commit_args)
            .await
            .context("compact memory layer")?;
        tokio::fs::rename(&lower_tmp, output_path)
            .await
            .with_context(|| {
                format!(
                    "move sealed memory lower into place failed: {}",
                    output_path.display()
                )
            })?;
        Ok(LayerSource::File(LayerFile::dense(
            output_path.to_path_buf(),
            uuid,
            descriptor,
        )))
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    build_result
}

/// Write `mappings` of an arbitrary memory source into a sealed OverlayBD layer.
///
/// The source is any [`VirtualFile`] whose `moffset` sectors the mappings
/// reference, so backends that produce a sparse memory image file (rather
/// than a live process to read) reuse the same layer construction. Returns
/// `Ok(None)` when no segment is present, in which case no layer file is
/// created.
pub(crate) async fn write_virtual_file_memory_layer(
    src_layers: &[Arc<dyn VirtualFile>],
    mappings: &[SegmentMapping],
    virtual_size: u64,
    output_dir: &Path,
    mode: OverlaybdCompactOutput,
    compute_digest: bool,
) -> Result<Option<LayerSource>> {
    if mappings.is_empty() {
        return Ok(None);
    }

    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create mem overlaybd dir: {}", output_dir.display()))?;

    let data_path = output_dir.join("overlaybd.commit");
    let layer = write_memory_layer(
        src_layers,
        mappings,
        virtual_size,
        &data_path,
        mode,
        DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY,
        compute_digest,
    )
    .await?;

    Ok(Some(layer))
}
