//! Backend-neutral memory layer construction and paused memory state.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::layer::LayerChain;
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

fn layer_is_stable_in_place(source: &Path, canonical_parent: &Path, roots: &[PathBuf]) -> bool {
    let resolved = std::fs::canonicalize(source).unwrap_or_else(|_| source.to_path_buf());
    let is_runtime = roots
        .iter()
        .any(|root| source.starts_with(root) || resolved.starts_with(root));
    if !is_runtime {
        return true;
    }
    source == resolved && resolved.starts_with(canonical_parent)
}

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
    let parent = path.parent().context("memory upper has no parent")?;
    let canonical_parent = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
    let upper_uuid = upper.get_uuid(0)?;
    let inplace_lower = canonical_parent.join(format!("lower-{}.commit", Uuid::now_v7()));
    let probe_layer = LayerFile::sealed_sparse(inplace_lower.clone(), upper_uuid);
    let compaction_range = snapshot_compaction_range(layers, roots, Some(&probe_layer), false);
    // Keep non-compacted lowers directly under `canonical_parent` only when
    // every existing runtime-owned lower already has a stable canonical path
    // inside `canonical_parent`. Otherwise stage in a fresh `memory-base-*`
    // TempDir so external or symlinked aliases are relocated to stable paths
    // and any pre-seal adoption failure is cleaned up automatically on drop.
    let all_in_place = compaction_range.is_none()
        && layers.layers.iter().all(|layer| {
            layer
                .path()
                .is_none_or(|source| layer_is_stable_in_place(source, &canonical_parent, roots))
        });
    let (directory, output_dir, lower) = if all_in_place {
        (None, canonical_parent, inplace_lower)
    } else {
        let directory = tempfile::Builder::new()
            .prefix("memory-base-")
            .tempdir_in(&canonical_parent)
            .context("prepare memory base directory")?;
        let output_dir = directory.path().to_path_buf();
        let lower = output_dir.join("snapshot.commit");
        (Some(directory), output_dir, lower)
    };
    let layer = LayerFile::sealed_sparse(lower.clone(), upper_uuid);
    let includes_delta = compaction_range.is_some_and(|range| range.end > layers.layers.len());
    // History merging and capacity rejection can finish before the irreversible seal.
    let prepared = if includes_delta {
        None
    } else {
        Some(
            rewrite_lowers_with_runtime_roots(
                layers,
                global_config,
                &output_dir,
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
    let output_dir = directory.map(tempfile::TempDir::keep).unwrap_or(output_dir);
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
                &output_dir,
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
) -> Result<LayerFile> {
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
        Ok(LayerFile::dense(
            output_path.to_path_buf(),
            uuid,
            descriptor,
        ))
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
) -> Result<Option<LayerFile>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::LayerSource;
    use overlaybd::config::UpperConfig;
    use overlaybd::index::Segment;
    use overlaybd::index_file::{checkpoint_sparse_index, open_files_ro, write_sparse_upper};
    use std::os::unix::fs::MetadataExt;

    async fn write_sparse_page(upper_path: &Path, page_index: u64, byte: u8) -> Result<()> {
        let offset = page_index * 4096;
        let file = LocalFile::open_rw(upper_path, false)?;
        write_sparse_upper(&file, offset, &[byte; 4096]).await?;
        checkpoint_sparse_index(
            &upper_path.with_extension("index"),
            vec![Segment::new(offset / OVERLAYBD_ALIGNMENT, 8)],
        )
        .await
    }

    #[tokio::test]
    async fn commit_memory_upper_keeps_non_compacted_lowers_in_place() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let runtime_dir = temp.path().join("runtime");
        let mem_dir = runtime_dir.join("mem_overlaybd");
        std::fs::create_dir_all(&mem_dir)?;
        let upper_path = mem_dir.join("upper.data");
        let config_path = runtime_dir.join("mem_image.json");
        let global_config = temp.path().join("unused-global.json");

        prepare_sparse_memory_upper(&upper_path, 8192)?;
        let mut image = ImageConfig {
            upper: UpperConfig {
                data: upper_path.display().to_string(),
                index: upper_path.with_extension("index").display().to_string(),
                mode: Some(UpperMode::Sparse),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut layers = LayerChain::default();
        let roots = vec![std::fs::canonicalize(&runtime_dir)?];

        // First commit seals page 0 = 0x11 directly under mem_overlaybd/.
        write_sparse_page(&upper_path, 0, 0x11).await?;
        commit_memory_upper(
            &mut image,
            &mut layers,
            &config_path,
            &global_config,
            &roots,
            false,
        )
        .await?;
        assert_eq!(layers.layers.len(), 1);
        let canonical_mem_dir = std::fs::canonicalize(&mem_dir)?;
        let first_path = layers.layers[0].path().unwrap().to_path_buf();
        assert_eq!(first_path.parent(), Some(canonical_mem_dir.as_path()));
        let first_meta = std::fs::metadata(&first_path)?;

        // Second commit seals page 1 = 0x22 without creating memory-base-* or relinking layer 0.
        write_sparse_page(&upper_path, 1, 0x22).await?;
        commit_memory_upper(
            &mut image,
            &mut layers,
            &config_path,
            &global_config,
            &roots,
            false,
        )
        .await?;
        assert_eq!(layers.layers.len(), 2);
        assert_eq!(layers.layers[0].path(), Some(first_path.as_path()));
        let first_meta_after = std::fs::metadata(&first_path)?;
        assert_eq!(
            (first_meta.dev(), first_meta.ino()),
            (first_meta_after.dev(), first_meta_after.ino())
        );
        let second_path = layers.layers[1].path().unwrap().to_path_buf();
        assert_eq!(second_path.parent(), Some(canonical_mem_dir.as_path()));
        assert_ne!(first_path, second_path);
        assert!(
            std::fs::read_dir(&mem_dir)?
                .filter_map(|e| e.ok())
                .all(|e| !e.file_name().to_string_lossy().starts_with("memory-base-")),
            "non-compacting commits must not create memory-base-* staging directories"
        );

        let files: Vec<Arc<dyn VirtualFile>> = layers
            .layers
            .iter()
            .map(|l| Ok(Arc::new(LocalFile::open_ro(l.path().unwrap())?) as Arc<dyn VirtualFile>))
            .collect::<Result<_>>()?;
        let ro = open_files_ro(&files).await?;
        assert_eq!(ro.read_at(0, 4096).await?.as_ref(), &[0x11; 4096]);
        assert_eq!(ro.read_at(4096, 4096).await?.as_ref(), &[0x22; 4096]);
        Ok(())
    }

    #[tokio::test]
    async fn commit_memory_upper_adopts_external_runtime_root_layers() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let external_root = temp.path().join("external-runtime");
        let runtime_dir = temp.path().join("current-runtime");
        let mem_dir = runtime_dir.join("mem_overlaybd");
        std::fs::create_dir_all(&external_root)?;
        std::fs::create_dir_all(&mem_dir)?;

        // Prepare a sealed lower inside an external runtime root.
        let external_layer_path = external_root.join("ext.commit");
        prepare_sparse_memory_upper(&external_layer_path, 8192)?;
        write_sparse_page(&external_layer_path, 0, 0xAA).await?;
        let ext_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::open_rw(&external_layer_path, false)?);
        let ext_idx: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_rw(
            external_layer_path.with_extension("index"),
            false,
        )?);
        let ext_upper = open_file_rw(ext_file, Some(ext_idx)).await?;
        let ext_uuid = ext_upper.get_uuid(0)?;
        ext_upper.close_seal().await?;
        drop(ext_upper);

        let upper_path = mem_dir.join("upper.data");
        let config_path = runtime_dir.join("mem_image.json");
        let global_config = temp.path().join("unused-global.json");
        prepare_sparse_memory_upper(&upper_path, 8192)?;
        write_sparse_page(&upper_path, 1, 0xBB).await?;

        let mut image = ImageConfig {
            upper: UpperConfig {
                data: upper_path.display().to_string(),
                index: upper_path.with_extension("index").display().to_string(),
                mode: Some(UpperMode::Sparse),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut layers = LayerChain {
            layers: vec![LayerSource::File(LayerFile::sealed_sparse(
                external_layer_path,
                ext_uuid,
            ))],
            managed_repo_blob_url: None,
        };
        let roots = vec![
            std::fs::canonicalize(&external_root)?,
            std::fs::canonicalize(&runtime_dir)?,
        ];

        commit_memory_upper(
            &mut image,
            &mut layers,
            &config_path,
            &global_config,
            &roots,
            false,
        )
        .await?;

        // Deleting the external runtime root must leave the adopted layer readable.
        std::fs::remove_dir_all(&external_root)?;
        assert_eq!(layers.layers.len(), 2);
        let files: Vec<Arc<dyn VirtualFile>> = layers
            .layers
            .iter()
            .map(|l| Ok(Arc::new(LocalFile::open_ro(l.path().unwrap())?) as Arc<dyn VirtualFile>))
            .collect::<Result<_>>()?;
        let ro = open_files_ro(&files).await?;
        assert_eq!(ro.read_at(0, 4096).await?.as_ref(), &[0xAA; 4096]);
        assert_eq!(ro.read_at(4096, 4096).await?.as_ref(), &[0xBB; 4096]);

        // Subsequent non-compacting commits after adoption stay in place with no new memory-base-*.
        let base_dirs_before = std::fs::read_dir(&mem_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("memory-base-"))
            .count();
        assert_eq!(base_dirs_before, 1);
        write_sparse_page(&upper_path, 0, 0xCC).await?;
        commit_memory_upper(
            &mut image,
            &mut layers,
            &config_path,
            &global_config,
            &roots,
            false,
        )
        .await?;
        let base_dirs_after = std::fs::read_dir(&mem_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("memory-base-"))
            .count();
        assert_eq!(base_dirs_after, base_dirs_before);
        assert_eq!(layers.layers.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn commit_memory_upper_relocates_symlink_alias_targeting_runtime_dir() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let alias_dir = temp.path().join("ephemeral-alias");
        let runtime_dir = temp.path().join("current-runtime");
        let mem_dir = runtime_dir.join("mem_overlaybd");
        std::fs::create_dir_all(&alias_dir)?;
        std::fs::create_dir_all(&mem_dir)?;

        // Place a real sealed lower inside mem_overlaybd/, referenced via an
        // external symlink alias that will later be removed.
        let target_layer_path = mem_dir.join("existing.commit");
        prepare_sparse_memory_upper(&target_layer_path, 8192)?;
        write_sparse_page(&target_layer_path, 0, 0x33).await?;
        let target_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::open_rw(&target_layer_path, false)?);
        let target_idx: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_rw(
            target_layer_path.with_extension("index"),
            false,
        )?);
        let target_upper = open_file_rw(target_file, Some(target_idx)).await?;
        let target_uuid = target_upper.get_uuid(0)?;
        target_upper.close_seal().await?;
        drop(target_upper);

        let alias_path = alias_dir.join("alias.commit");
        std::os::unix::fs::symlink(&target_layer_path, &alias_path)?;

        let upper_path = mem_dir.join("upper.data");
        let config_path = runtime_dir.join("mem_image.json");
        let global_config = temp.path().join("unused-global.json");
        prepare_sparse_memory_upper(&upper_path, 8192)?;
        write_sparse_page(&upper_path, 1, 0x44).await?;

        let mut image = ImageConfig {
            upper: UpperConfig {
                data: upper_path.display().to_string(),
                index: upper_path.with_extension("index").display().to_string(),
                mode: Some(UpperMode::Sparse),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut layers = LayerChain {
            layers: vec![LayerSource::File(LayerFile::sealed_sparse(
                alias_path,
                target_uuid,
            ))],
            managed_repo_blob_url: None,
        };
        let roots = vec![std::fs::canonicalize(&runtime_dir)?];

        commit_memory_upper(
            &mut image,
            &mut layers,
            &config_path,
            &global_config,
            &roots,
            false,
        )
        .await?;

        // Removing the ephemeral symlink directory must not break reopening via mem_image.json.
        std::fs::remove_dir_all(&alias_dir)?;
        let saved = overlaybd::config::load_image_config(&config_path)?;
        assert_eq!(saved.lowers.len(), 2);
        let files: Vec<Arc<dyn VirtualFile>> = saved
            .lowers
            .iter()
            .map(|l| Ok(Arc::new(LocalFile::open_ro(&l.file)?) as Arc<dyn VirtualFile>))
            .collect::<Result<_>>()?;
        let ro = open_files_ro(&files).await?;
        assert_eq!(ro.read_at(0, 4096).await?.as_ref(), &[0x33; 4096]);
        assert_eq!(ro.read_at(4096, 4096).await?.as_ref(), &[0x44; 4096]);
        Ok(())
    }

    #[tokio::test]
    async fn commit_memory_upper_cleans_up_partial_external_adoption_on_pre_seal_failure(
    ) -> Result<()> {
        let temp = tempfile::tempdir()?;
        let external_root = temp.path().join("external-runtime");
        let runtime_dir = temp.path().join("current-runtime");
        let mem_dir = runtime_dir.join("mem_overlaybd");
        std::fs::create_dir_all(&external_root)?;
        std::fs::create_dir_all(&mem_dir)?;

        // Layer 0 exists in external_root; layer 1 is missing so adoption fails
        // halfway through before the upper is sealed.
        let valid_ext = external_root.join("layer0.commit");
        std::fs::write(&valid_ext, b"layer0")?;
        let missing_ext = external_root.join("missing.commit");

        let upper_path = mem_dir.join("upper.data");
        let config_path = runtime_dir.join("mem_image.json");
        let global_config = temp.path().join("unused-global.json");
        prepare_sparse_memory_upper(&upper_path, 8192)?;
        write_sparse_page(&upper_path, 0, 0x55).await?;

        let mut image = ImageConfig {
            upper: UpperConfig {
                data: upper_path.display().to_string(),
                index: upper_path.with_extension("index").display().to_string(),
                mode: Some(UpperMode::Sparse),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut layers = LayerChain {
            layers: vec![
                LayerSource::File(LayerFile::sealed_sparse(valid_ext, Uuid::new_v4())),
                LayerSource::File(LayerFile::sealed_sparse(missing_ext, Uuid::new_v4())),
            ],
            managed_repo_blob_url: None,
        };
        let roots = vec![
            std::fs::canonicalize(&external_root)?,
            std::fs::canonicalize(&runtime_dir)?,
        ];

        let err = commit_memory_upper(
            &mut image,
            &mut layers,
            &config_path,
            &global_config,
            &roots,
            false,
        )
        .await
        .unwrap_err();
        assert!(
            !err.is_terminal(),
            "pre-seal adoption failure must be recoverable"
        );

        // No orphaned inherited-layers or memory-base-* staging dir may remain.
        let entries: Vec<String> = std::fs::read_dir(&mem_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            entries
                .iter()
                .all(|name| name == "upper.data" || name == "upper.index"),
            "unexpected leftover files after pre-seal failure: {entries:?}"
        );
        Ok(())
    }
}
