//! Save paused memory and capture independent OverlayBD memory snapshots.
use super::overlaybd_snapshot::{
    local_layer_config, local_layer_config_with_descriptor, record_sealed_upper_size,
    rewrite_lowers_with_runtime_roots, runtime_owned_roots_for_live_config, write_image_config,
};
use super::process_vm_reader::ProcessVmReader;
use crate::overlaybd::{create_commit_args, OverlaybdCompactOutput};
use crate::sandbox::{SandboxCaptureError, SandboxCaptureResult};
use crate::snapshot::SnapshotPlacement;
use anyhow::{ensure, Context, Result};
use firecracker_client::models::DirtyMemoryRanges;
use nix::unistd::Pid;
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{load_image_config, ImageConfig, UpperConfig, UpperMode};
use overlaybd::index::{Segment, SegmentMapping};
use overlaybd::index_file::{compact_to, open_file_rw};
use overlaybd::layer_metadata::resolve_local_layer_path;
use overlaybd::virtual_file::VirtualFile;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tracing::warn;
use uuid::Uuid;

const FIRECRACKER_DIRTY_PAGE_SIZE: u64 = 4096;
const OVERLAYBD_ALIGNMENT: u64 = 512;
const SPARSE_MEMORY_WRITE_CHUNK_SIZE: usize = 4 * 1024 * 1024;
const DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY: usize = 32;

pub(super) async fn save_paused_memory(
    pid: Pid,
    dirty: &DirtyMemoryRanges,
    base_image_config_path: Option<&Path>,
    runtime_dir: &Path,
    owned_roots: &[PathBuf],
) -> SandboxCaptureResult<crate::sandbox::OverlaybdConfig> {
    let config_path = runtime_dir.join("mem_image.json");
    let upper = runtime_dir.join("mem_overlaybd/upper.data");
    if config_path.exists() {
        write_dirty_memory_to_sparse_upper(pid, dirty, &upper).await?;
    } else {
        tokio::fs::create_dir_all(upper.parent().unwrap())
            .await
            .context("prepare memory upper")?;
        prepare_sparse_memory_upper(&upper, dirty.memory_size as u64)?;
        // This new file is not backing the running VM.
        write_dirty_memory_to_sparse_upper(pid, dirty, &upper)
            .await
            .map_err(|error| SandboxCaptureError::recoverable(error.into()))?;
        let mut image = base_image_config_path
            .map(load_image_config)
            .transpose()?
            .unwrap_or_default();
        let roots = base_image_config_path
            .map(|path| runtime_owned_roots_for_live_config(owned_roots, path))
            .unwrap_or_default();
        image.lowers = rewrite_lowers_with_runtime_roots(
            image.lowers,
            runtime_dir,
            None,
            "mem_compacted.commit",
            &roots,
            OverlaybdCompactOutput::Raw,
        )
        .await?
        .0;
        image.upper = UpperConfig {
            data: upper.display().to_string(),
            mode: Some(UpperMode::Sparse),
            ..Default::default()
        };
        write_image_config(&config_path, &image, "paused memory")?;
    }
    Ok(crate::sandbox::OverlaybdConfig {
        image_config_path: config_path,
        read_only: true,
        runtime_upper_mode: UpperMode::Sparse,
    })
}

/// Seal the resumed pause's memory upper once. Later captures write independent
/// deltas and leave the replacement upper for the next pause.
#[allow(clippy::too_many_arguments)]
pub(super) async fn capture_memory_snapshot(
    pid: Pid,
    dirty: &DirtyMemoryRanges,
    base_image_config_path: Option<&Path>,
    snapshot_dir: &Path,
    memory_encoding: OverlaybdCompactOutput,
    upper_needs_sealing: &mut bool,
    placement: SnapshotPlacement,
    owned_roots: &[PathBuf],
) -> Result<crate::sandbox::OverlaybdConfig> {
    let mut image = base_image_config_path
        .map(load_image_config)
        .transpose()?
        .unwrap_or_default();
    let roots = base_image_config_path
        .map(|path| runtime_owned_roots_for_live_config(owned_roots, path))
        .unwrap_or_default();
    let seal_upper = *upper_needs_sealing;
    if seal_upper {
        let upper = PathBuf::from(&image.upper.data);
        write_dirty_memory_to_sparse_upper(pid, dirty, &upper).await?;
        let lower = seal_memory_upper(&upper).await?;
        let mut previous: Vec<_> = image
            .lowers
            .iter()
            .filter_map(resolve_local_layer_path)
            .collect();
        previous.push(lower.clone());
        async {
            let (lowers, compacted) = rewrite_lowers_with_runtime_roots(
                image.lowers.clone(),
                upper.parent().unwrap(),
                Some(local_layer_config(&lower)),
                "mem_compacted.commit",
                &roots,
                OverlaybdCompactOutput::Raw,
            )
            .await?;
            image.lowers = lowers;
            write_image_config(
                base_image_config_path.unwrap(),
                &image,
                "sealed memory base",
            )?;
            record_sealed_upper_size("memory", &lower, compacted).await;
            Ok::<(), anyhow::Error>(())
        }
        .await
        .map_err(SandboxCaptureError::terminal)?;
        *upper_needs_sealing = false;
        remove_replaced_memory_layers(base_image_config_path.unwrap(), &previous, &image.lowers)
            .await;
    }
    if placement == SnapshotPlacement::Distributed && !image.upper.data.is_empty() {
        // These immutable layers are now being exported for publication. Keep
        // the canonical result so later publications do not export them again.
        let previous: Vec<_> = image
            .lowers
            .iter()
            .filter_map(resolve_local_layer_path)
            .collect();
        let mut changed = false;
        for layer in &mut image.lowers {
            let Some(path) = resolve_local_layer_path(layer) else {
                continue;
            };
            if !overlaybd::dense_export::should_dense_export_layer(&path) {
                continue;
            }
            let destination = path.with_extension("published.commit");
            let temporary = NamedTempFile::new_in(path.parent().unwrap())?;
            let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_rw(temporary.path(), false)?);
            let descriptor = match memory_encoding {
                OverlaybdCompactOutput::Raw => {
                    let descriptor =
                        overlaybd::dense_export::write_dense_layer_to_file(&path, file).await?;
                    overlaybd::LayerDescriptor {
                        digest: descriptor.digest,
                        size: descriptor.size,
                    }
                }
                OverlaybdCompactOutput::ZFile { .. } => {
                    let args = create_commit_args(file, memory_encoding, 1).await?;
                    overlaybd::dense_export::write_dense_layer_to(&path, args.writer).await?;
                    let descriptor = crate::digest::FileDigest::describe(temporary.path()).await?;
                    overlaybd::LayerDescriptor {
                        digest: descriptor.sha256,
                        size: descriptor.size,
                    }
                }
            };
            temporary
                .persist(&destination)
                .map_err(|error| error.error)?;
            *layer = local_layer_config_with_descriptor(&destination, Some(&descriptor));
            changed = true;
        }
        if changed {
            write_image_config(
                base_image_config_path.unwrap(),
                &image,
                "published memory base",
            )?;
            remove_replaced_memory_layers(
                base_image_config_path.unwrap(),
                &previous,
                &image.lowers,
            )
            .await;
        }
    }
    let delta = if seal_upper {
        None
    } else if placement == SnapshotPlacement::Local && !dirty.ranges.is_empty() {
        let directory = snapshot_dir.join("mem_overlaybd");
        tokio::fs::create_dir_all(&directory).await?;
        let path = directory.join("overlaybd.commit");
        prepare_sparse_memory_upper(&path, dirty.memory_size as u64)?;
        write_dirty_memory_to_sparse_upper(pid, dirty, &path)
            .await
            .map_err(|error| SandboxCaptureError::recoverable(error.into()))?;
        let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_rw(&path, false)?);
        open_file_rw(file, None).await?.close_seal().await?;
        Some(path)
    } else {
        write_dirty_memory_layer(
            pid,
            dirty,
            &snapshot_dir.join("mem_overlaybd"),
            memory_encoding,
        )
        .await?
    };
    let (lowers, compacted) = rewrite_lowers_with_runtime_roots(
        image.lowers,
        snapshot_dir,
        delta.as_deref().map(local_layer_config),
        "mem_compacted.commit",
        &roots,
        memory_encoding,
    )
    .await?;
    if let Some(delta) = &delta {
        record_sealed_upper_size("memory", delta, compacted).await;
    }
    let image = ImageConfig {
        repo_blob_url: image.repo_blob_url,
        lowers,
        ..Default::default()
    };
    let path = snapshot_dir.join("mem_image.json");
    write_image_config(&path, &image, "memory snapshot")?;
    Ok(crate::sandbox::OverlaybdConfig {
        image_config_path: path,
        read_only: true,
        runtime_upper_mode: UpperMode::LogStructured,
    })
}

async fn remove_replaced_memory_layers(
    config_path: &Path,
    previous: &[PathBuf],
    lowers: &[overlaybd::config::LayerConfig],
) {
    let Some(runtime_root) = config_path
        .parent()
        .and_then(|parent| std::fs::canonicalize(parent).ok())
    else {
        return;
    };
    let retained: Vec<_> = lowers
        .iter()
        .filter_map(resolve_local_layer_path)
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
        if let Some(parent) = path.parent().filter(|parent| *parent != runtime_root) {
            let _ = tokio::fs::remove_dir(parent).await;
        }
    }
}

async fn seal_memory_upper(path: &Path) -> SandboxCaptureResult<PathBuf> {
    let file: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::open_rw(path, false).context("open paused memory upper")?);
    let upper = open_file_rw(file, None).await?;
    let size = upper.size().await?;
    let lower = path.with_file_name(format!("memory-base-{}.commit", Uuid::now_v7()));
    async {
        upper.close_seal().await?;
        drop(upper);
        tokio::fs::rename(path, &lower).await?;
        prepare_sparse_memory_upper(path, size)?;
        Ok(lower)
    }
    .await
    .map_err(SandboxCaptureError::terminal)
}

fn prepare_sparse_memory_upper(path: &Path, virtual_size: u64) -> Result<()> {
    overlaybd::helper::prepare_runtime_upper(path, None, virtual_size, UpperMode::Sparse)
        .with_context(|| format!("prepare sparse memory upper {}", path.display()))
}

async fn write_memory_layer(
    src_layers: &[Arc<dyn VirtualFile>],
    mappings: &[SegmentMapping],
    virtual_size: u64,
    output_path: &Path,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
) -> Result<()> {
    let lower_tmp = output_path.with_extension("commit.tmp");
    let build_result = async {
        let output_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp)
                .with_context(|| format!("create temp lower failed: {}", lower_tmp.display()))?,
        );
        let commit_args = create_commit_args(output_file, mode, concurrency).await?;
        compact_to(src_layers, mappings, virtual_size, commit_args)
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
        Ok(())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    build_result
}

fn checked_i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} must be non-negative, got {value}"))
}

fn dirty_ranges_to_segment_mappings(
    dirty_ranges: &DirtyMemoryRanges,
) -> Result<(Vec<SegmentMapping>, u64)> {
    ensure!(
        dirty_ranges.page_size == FIRECRACKER_DIRTY_PAGE_SIZE as i32,
        "dirty memory page_size must be {FIRECRACKER_DIRTY_PAGE_SIZE}, got {}",
        dirty_ranges.page_size
    );
    let memory_size = checked_i64_to_u64(dirty_ranges.memory_size, "memory_size")?;
    ensure!(
        memory_size % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
        "dirty memory_size {memory_size} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
    );

    let mut mappings = Vec::new();
    for range in &dirty_ranges.ranges {
        let mut base_host_virt_addr =
            checked_i64_to_u64(range.base_host_virt_addr, "base_host_virt_addr")?;
        let mut image_offset = checked_i64_to_u64(range.image_offset, "image_offset")?;
        let length = checked_i64_to_u64(range.length, "length")?;
        ensure!(length > 0, "dirty memory range length must be positive");
        ensure!(
            base_host_virt_addr % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range base_host_virt_addr {base_host_virt_addr:#x} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            image_offset % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range image_offset {image_offset} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            length % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range length {length} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            image_offset
                .checked_add(length)
                .is_some_and(|end| end <= memory_size),
            "dirty range [{image_offset}, {}) exceeds memory_size {memory_size}",
            image_offset.saturating_add(length)
        );

        let mut remaining_sectors = length / OVERLAYBD_ALIGNMENT;
        while remaining_sectors > 0 {
            let sector_count = remaining_sectors.min(Segment::MAX_LENGTH as u64);
            mappings.push(SegmentMapping::new(
                image_offset / OVERLAYBD_ALIGNMENT,
                sector_count as u32,
                base_host_virt_addr / OVERLAYBD_ALIGNMENT,
                false,
                0,
            ));
            let bytes = sector_count * OVERLAYBD_ALIGNMENT;
            base_host_virt_addr += bytes;
            image_offset += bytes;
            remaining_sectors -= sector_count;
        }
    }

    mappings.sort_unstable();
    // `offset` and `length` describe destination memory-image sectors, while
    // `moffset` carries the source Firecracker HVA in sector units. This check
    // intentionally rejects overlapping destination ranges, not overlapping
    // source virtual addresses.
    for pair in mappings.windows(2) {
        let previous = &pair[0];
        let next = &pair[1];
        let previous_end = previous
            .offset()
            .checked_add(u64::from(previous.length()))
            .context("dirty memory segment end overflow")?;
        ensure!(
            previous_end <= next.offset(),
            "overlapping dirty memory destination ranges detected at image sectors {} and {}",
            previous.offset(),
            next.offset()
        );
    }
    Ok((mappings, memory_size))
}

/// Write dirty Firecracker memory ranges into a sealed OverlayBD layer.
async fn write_dirty_memory_layer(
    firecracker_pid: Pid,
    dirty_ranges: &DirtyMemoryRanges,
    output_dir: &Path,
    mode: OverlaybdCompactOutput,
) -> Result<Option<PathBuf>> {
    let (mappings, memory_size) = dirty_ranges_to_segment_mappings(dirty_ranges)?;
    if mappings.is_empty() {
        return Ok(None);
    }

    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create mem overlaybd dir: {}", output_dir.display()))?;

    let data_path = output_dir.join("overlaybd.commit");
    let source_file: Arc<dyn VirtualFile> = Arc::new(ProcessVmReader::new(firecracker_pid));
    let src_layers = vec![source_file];
    write_memory_layer(
        &src_layers,
        &mappings,
        memory_size,
        &data_path,
        mode,
        DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY,
    )
    .await
    .context("compact dirty memory ranges as overlaybd layer")?;

    Ok(Some(data_path))
}

struct MemoryCopyRange {
    host_address: u64,
    file_offset: u64,
    len: usize,
}

async fn write_memory_batch(
    file: &LocalFile,
    batch: &[MemoryCopyRange],
    buffer: &[u8],
) -> Result<()> {
    let mut offset = 0;
    let mut ranges = batch.iter().peekable();
    while let Some(range) = ranges.next() {
        let mut len = range.len;
        while ranges
            .peek()
            .is_some_and(|next| next.file_offset == range.file_offset + len as u64)
        {
            len += ranges.next().unwrap().len;
        }
        overlaybd::index_file::write_sparse_upper(
            file,
            range.file_offset,
            &buffer[offset..offset + len],
        )
        .await
        .with_context(|| format!("write dirty memory at image offset {}", range.file_offset))?;
        offset += len;
    }
    Ok(())
}

/// Copy dirty pages through two owned buffers: read the next batch on a
/// blocking worker while the current batch is written. Both finish before an
/// error returns, so the caller can safely clean up a failed pause.
async fn write_dirty_memory_to_sparse_upper(
    firecracker_pid: Pid,
    dirty_ranges: &DirtyMemoryRanges,
    upper_path: &Path,
) -> SandboxCaptureResult<()> {
    let buffer_size = dirty_ranges
        .ranges
        .iter()
        .map(|range| range.length as u64)
        .sum::<u64>()
        .min(SPARSE_MEMORY_WRITE_CHUNK_SIZE as u64) as usize;
    let mut chunks = dirty_ranges
        .ranges
        .iter()
        .flat_map(|range| {
            let len = range.length as usize;
            (0..len)
                .step_by(SPARSE_MEMORY_WRITE_CHUNK_SIZE)
                .map(move |offset| MemoryCopyRange {
                    host_address: range.base_host_virt_addr as u64 + offset as u64,
                    file_offset: range.image_offset as u64 + offset as u64,
                    len: (len - offset).min(SPARSE_MEMORY_WRITE_CHUNK_SIZE),
                })
        })
        .collect::<Vec<_>>();
    chunks.sort_unstable_by_key(|range| range.file_offset);
    let mut chunks = chunks.into_iter().peekable();
    let mut next_batch = || {
        let mut batch = Vec::new();
        let mut size = 0;
        while let Some(chunk) = chunks.peek() {
            if size + chunk.len > buffer_size {
                break;
            }
            let chunk = chunks.next().unwrap();
            size += chunk.len;
            batch.push(chunk);
        }
        (!batch.is_empty()).then_some(batch)
    };
    let Some(first) = next_batch() else {
        return Ok(());
    };
    let file = LocalFile::open_rw(upper_path, false)
        .with_context(|| format!("open sparse memory upper {}", upper_path.display()))
        .map_err(SandboxCaptureError::recoverable)?;
    if buffer_size == FIRECRACKER_DIRTY_PAGE_SIZE as usize {
        let mut buffer = vec![0; buffer_size];
        ProcessVmReader::new(firecracker_pid)
            .read_exact_remote(first[0].host_address, &mut buffer)
            .map_err(SandboxCaptureError::recoverable)?;
        write_memory_batch(&file, &first, &buffer)
            .await
            .map_err(SandboxCaptureError::terminal)?;
        return Ok(());
    }
    let read_batch = |batch: Vec<MemoryCopyRange>, mut buffer: Vec<u8>| {
        tokio::task::spawn_blocking(move || {
            if buffer.is_empty() {
                buffer = vec![0; buffer_size];
            }
            let reader = ProcessVmReader::new(firecracker_pid);
            let len = batch.iter().map(|range| range.len).sum::<usize>();
            let contiguous_source = batch
                .windows(2)
                .all(|pair| pair[0].host_address + pair[0].len as u64 == pair[1].host_address);
            if contiguous_source {
                reader.read_exact_remote(batch[0].host_address, &mut buffer[..len])?;
            } else {
                let mut remote_iovs = batch
                    .iter()
                    .map(|range| nix::sys::uio::RemoteIoVec {
                        base: range.host_address as usize,
                        len: range.len,
                    })
                    .collect::<Vec<_>>();
                reader.read_exact_remote_vectored(&mut remote_iovs, &mut buffer[..len])?;
            }
            Ok::<_, anyhow::Error>((batch, buffer))
        })
    };
    let (mut batch, mut buffer) = read_batch(first, Vec::new())
        .await
        .context("join first dirty-memory read")
        .and_then(|result| result)
        .map_err(SandboxCaptureError::recoverable)?;
    let mut spare = Vec::new();
    while let Some(next) = next_batch() {
        let pending = read_batch(next, spare);
        let (written, read) = tokio::join!(write_memory_batch(&file, &batch, &buffer), pending);
        written.map_err(SandboxCaptureError::terminal)?;
        let next = read
            .context("join dirty-memory read")
            .and_then(|result| result)
            .map_err(SandboxCaptureError::terminal)?;
        spare = buffer;
        (batch, buffer) = next;
    }
    write_memory_batch(&file, &batch, &buffer)
        .await
        .map_err(SandboxCaptureError::terminal)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use firecracker_client::models::DirtyMemoryRange;
    use overlaybd::index_file::open_file_ro;
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    #[tokio::test]
    async fn capture_resolves_relative_layers_from_the_opened_backing_path() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let original = temp.path().join("original");
        let opened = temp.path().join("opened");
        for (directory, byte) in [(&original, 0x11), (&opened, 0x55)] {
            fs::create_dir(directory)?;
            let layer = directory.join("base.commit");
            prepare_sparse_memory_upper(&layer, 4096)?;
            let file = LocalFile::open_rw(&layer, false)?;
            overlaybd::index_file::write_sparse_upper(&file, 0, &[byte; 4096]).await?;
            open_file_rw(Arc::new(file), None)
                .await?
                .close_seal()
                .await?;
        }
        let config = original.join("image.json");
        fs::write(
            &config,
            br#"{"lowers":[{"file":"./base.commit"}],"upper":{}}"#,
        )?;
        let actual = opened.join("image.json");
        std::os::unix::fs::symlink(&config, &actual)?;
        let mut upper_needs_sealing = false;
        let snapshot = capture_memory_snapshot(
            Pid::this(),
            &DirtyMemoryRanges {
                page_size: 4096,
                memory_size: 4096,
                ranges: Vec::new(),
            },
            Some(&actual),
            &temp.path().join("snapshot"),
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
        )
        .await?;
        fs::remove_dir_all(original)?;
        fs::remove_dir_all(opened)?;
        let image = load_image_config(&snapshot.image_config_path)?;
        let layer = open_file_ro(Arc::new(LocalFile::open_ro(&image.lowers[0].file)?)).await?;
        assert_eq!(layer.read_at(0, 4096).await?, vec![0x55; 4096]);
        Ok(())
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_splits_large_ranges() {
        let first_chunk = Segment::MAX_LENGTH as i64 * OVERLAYBD_ALIGNMENT as i64;
        let dirty_ranges = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: first_chunk + OVERLAYBD_ALIGNMENT as i64,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: 0x1000_0000,
                image_offset: 0,
                length: first_chunk + OVERLAYBD_ALIGNMENT as i64,
            }],
        };

        let (mappings, memory_size) =
            dirty_ranges_to_segment_mappings(&dirty_ranges).expect("convert mappings");

        assert_eq!(memory_size, dirty_ranges.memory_size as u64);
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].offset(), 0);
        assert_eq!(mappings[0].length(), Segment::MAX_LENGTH);
        assert_eq!(mappings[0].moffset, 0x1000_0000 / OVERLAYBD_ALIGNMENT);
        assert_eq!(mappings[1].offset(), Segment::MAX_LENGTH as u64);
        assert_eq!(mappings[1].length(), 1);
        assert_eq!(
            mappings[1].moffset,
            0x1000_0000 / OVERLAYBD_ALIGNMENT + Segment::MAX_LENGTH as u64
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_non_4k_page_size() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: 2048,
            memory_size: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
            ranges: Vec::new(),
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("dirty memory page_size must be 4096"),
            "{err:#}"
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_non_page_aligned_range() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: FIRECRACKER_DIRTY_PAGE_SIZE as i32,
            memory_size: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: 0x1000_0000,
                image_offset: 0,
                length: OVERLAYBD_ALIGNMENT as i64,
            }],
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("dirty range length 512 is not 4096-byte aligned"),
            "{err:#}"
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_overlaps() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: FIRECRACKER_DIRTY_PAGE_SIZE as i32,
            memory_size: (FIRECRACKER_DIRTY_PAGE_SIZE * 3) as i64,
            ranges: vec![
                DirtyMemoryRange {
                    base_host_virt_addr: 0x1000_0000,
                    image_offset: 0,
                    length: (FIRECRACKER_DIRTY_PAGE_SIZE * 2) as i64,
                },
                DirtyMemoryRange {
                    base_host_virt_addr: 0x2000_0000,
                    image_offset: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
                    length: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
                },
            ],
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("overlapping dirty memory destination ranges detected"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn sparse_memory_pipeline_classifies_read_failures_after_completed_writes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("upper.data");
        let size = SPARSE_MEMORY_WRITE_CHUNK_SIZE;
        prepare_sparse_memory_upper(&path, (size + 4096) as u64).unwrap();
        let mut backing = vec![0x67u8; size + 4096];
        let alignment = backing.as_ptr().align_offset(4096);
        let source = &mut backing[alignment..alignment + size];
        let mut ranges = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: (size + 4096) as i64,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: 0,
                image_offset: size as i64,
                length: 4096,
            }],
        };
        let error = write_dirty_memory_to_sparse_upper(Pid::this(), &ranges, &path)
            .await
            .unwrap_err();
        assert!(!error.is_terminal());
        ranges.ranges.insert(
            0,
            DirtyMemoryRange {
                base_host_virt_addr: source.as_ptr() as i64,
                image_offset: 0,
                length: size as i64,
            },
        );
        let error = write_dirty_memory_to_sparse_upper(Pid::this(), &ranges, &path)
            .await
            .unwrap_err();
        assert!(error.is_terminal());
        assert_eq!(&std::fs::read(path).unwrap()[4096..4096 + size], source);
    }

    #[tokio::test]
    async fn sparse_memory_pipeline_preserves_multiple_batches_and_holes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("upper.data");
        let mib = 1024 * 1024;
        let data_len = 10 * mib + 4096;
        prepare_sparse_memory_upper(&path, 32 * mib as u64).unwrap();
        let mut backing = vec![0u8; data_len + 4096];
        let alignment = backing.as_ptr().align_offset(4096);
        let source = &mut backing[alignment..alignment + data_len];
        source[..2 * mib].fill(0x51);
        source[2 * mib..].fill(0xA7);
        let ranges = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: 32 * mib as i64,
            ranges: vec![
                DirtyMemoryRange {
                    base_host_virt_addr: source.as_ptr() as i64,
                    image_offset: 0,
                    length: (2 * mib) as i64,
                },
                DirtyMemoryRange {
                    base_host_virt_addr: source[2 * mib..].as_ptr() as i64,
                    image_offset: (8 * mib) as i64,
                    length: (8 * mib + 4096) as i64,
                },
            ],
        };
        write_dirty_memory_to_sparse_upper(Pid::this(), &ranges, &path)
            .await
            .unwrap();
        let output = std::fs::read(&path).unwrap();
        assert_eq!(&output[4096..4096 + 2 * mib], &source[..2 * mib]);
        assert!(output[4096 + 2 * mib..4096 + 8 * mib]
            .iter()
            .all(|byte| *byte == 0));
        assert_eq!(
            &output[4096 + 8 * mib..4096 + 16 * mib + 4096],
            &source[2 * mib..]
        );
        let allocated = std::fs::metadata(&path).unwrap().blocks();
        assert!(allocated * 512 < 16 * mib as u64);
        source[..4096].fill(0x33);
        let overwrite = DirtyMemoryRanges {
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: source.as_ptr() as i64,
                image_offset: 0,
                length: 4096,
            }],
            ..ranges
        };
        write_dirty_memory_to_sparse_upper(Pid::this(), &overwrite, &path)
            .await
            .unwrap();
        let updated = std::fs::read(&path).unwrap();
        assert_eq!(&updated[4096..8192], &[0x33; 4096]);
        assert_eq!(&updated[8192..], &output[8192..]);
        assert_eq!(std::fs::metadata(&path).unwrap().blocks(), allocated);
    }

    #[tokio::test]
    async fn repeated_snapshots_preserve_the_fixed_base_and_previous_snapshots() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let runtime = temp.path().join("runtime");
        fs::create_dir(&runtime)?;
        let mut upper_needs_sealing = true;
        let mut allocation = vec![0u8; 8192];
        let offset = (4096 - allocation.as_ptr() as usize % 4096) % 4096;
        let page = &mut allocation[offset..offset + 4096];
        page.fill(0x5a);
        let mut dirty = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: 8192,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: page.as_ptr() as i64,
                image_offset: 0,
                length: 4096,
            }],
        };
        let paused = save_paused_memory(Pid::this(), &dirty, None, &runtime, &[]).await?;
        let config = paused.image_config_path;
        let upper = runtime.join("mem_overlaybd/upper.data");
        let first = capture_memory_snapshot(
            Pid::this(),
            &dirty,
            Some(&config),
            &temp.path().join("first"),
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
        )
        .await?;
        page.fill(0xa5);
        dirty.ranges[0].image_offset = 4096;
        let second = capture_memory_snapshot(
            Pid::this(),
            &dirty,
            Some(&config),
            &temp.path().join("second"),
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
        )
        .await?;
        let first_image = load_image_config(&first.image_config_path)?;
        let second_image = load_image_config(&second.image_config_path)?;
        let first_layer: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(
            &first_image.lowers.last().unwrap().file,
        )?);
        let second_layer: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(
            &second_image.lowers.last().unwrap().file,
        )?);
        let first_layer = open_file_ro(first_layer).await?;
        let second_layer = open_file_ro(second_layer).await?;
        assert_eq!(first_layer.read_at(0, 4096).await?, vec![0x5a; 4096]);
        assert_eq!(second_layer.read_at(4096, 4096).await?, vec![0xa5; 4096]);
        let runtime_image = load_image_config(&config)?;
        assert_eq!(Path::new(&runtime_image.upper.data), upper);
        assert_eq!(runtime_image.lowers.len(), 1);

        for cycle in 0..40 {
            save_paused_memory(Pid::this(), &dirty, Some(&config), &runtime, &[]).await?;
            upper_needs_sealing = true;
            page.fill(cycle as u8);
            let output = temp.path().join(format!("capture-{cycle}"));
            capture_memory_snapshot(
                Pid::this(),
                &dirty,
                Some(&config),
                &output,
                OverlaybdCompactOutput::Raw,
                &mut upper_needs_sealing,
                if cycle % 2 == 0 {
                    SnapshotPlacement::Local
                } else {
                    SnapshotPlacement::Distributed
                },
                &[],
            )
            .await?;
            fs::remove_dir_all(output)?;

            let image = load_image_config(&config)?;
            assert!(image.lowers.len() <= 32);
            let retained: Vec<_> = image
                .lowers
                .iter()
                .filter_map(resolve_local_layer_path)
                .collect();
            let mut directories = vec![runtime.clone()];
            while let Some(directory) = directories.pop() {
                for entry in fs::read_dir(directory)? {
                    let path = entry?.path();
                    if path.is_dir() {
                        directories.push(path);
                    } else if path != config && path != upper {
                        assert!(
                            retained.contains(&path),
                            "unowned memory file: {}",
                            path.display()
                        );
                    }
                }
            }
        }
        fs::remove_dir_all(runtime)?;
        let first_layer = open_file_ro(Arc::new(LocalFile::open_ro(
            &first_image.lowers.last().unwrap().file,
        )?))
        .await?;
        let second_layer = open_file_ro(Arc::new(LocalFile::open_ro(
            &second_image.lowers.last().unwrap().file,
        )?))
        .await?;
        assert_eq!(first_layer.read_at(0, 4096).await?, vec![0x5a; 4096]);
        assert_eq!(second_layer.read_at(4096, 4096).await?, vec![0xa5; 4096]);
        Ok(())
    }

    struct FailingSource;

    #[async_trait]
    impl VirtualFile for FailingSource {
        async fn read_at(&self, _offset: u64, _len: usize) -> Result<Bytes> {
            anyhow::bail!("injected memory source read failure")
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            anyhow::bail!("injected memory source write failure")
        }

        async fn size(&self) -> Result<u64> {
            Ok(OVERLAYBD_ALIGNMENT)
        }
    }

    fn one_page_mapping() -> Vec<SegmentMapping> {
        vec![SegmentMapping::new(0, 1, 0, false, 0)]
    }

    #[tokio::test]
    async fn memory_layer_write_failure_removes_temp_and_preserves_final() {
        let temp = tempfile::tempdir().expect("tempdir");
        let output = temp.path().join("memory.commit");
        let lower_tmp = output.with_extension("commit.tmp");
        let existing = b"existing final";
        tokio::fs::write(&output, existing)
            .await
            .expect("write existing final");
        let source: Arc<dyn VirtualFile> = Arc::new(FailingSource);
        write_memory_layer(
            &[source],
            &one_page_mapping(),
            OVERLAYBD_ALIGNMENT,
            &output,
            OverlaybdCompactOutput::Raw,
            1,
        )
        .await
        .expect_err("copy failure should be returned");

        assert!(!lower_tmp.exists());
        assert_eq!(
            tokio::fs::read(&output).await.expect("read final"),
            existing
        );
    }
}
