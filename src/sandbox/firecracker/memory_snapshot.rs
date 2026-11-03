//! Save paused memory and capture independent OverlayBD memory snapshots.
use super::process_vm_reader::ProcessVmReader;
use crate::layer::{LayerChain, LayerSource};
use crate::sandbox::memory_snapshot::{
    commit_memory_upper, prepare_sparse_memory_upper, remove_replaced_memory_layers,
    write_virtual_file_memory_layer, OVERLAYBD_ALIGNMENT,
};
use crate::sandbox::overlaybd_snapshot::{
    record_sealed_upper_size, rewrite_lowers_with_runtime_roots,
    runtime_owned_roots_for_live_config, write_image_config,
};
use crate::sandbox::{SandboxCaptureError, SandboxCaptureResult};
use crate::snapshot::SnapshotMemoryArtifacts;
use crate::snapshot::SnapshotPlacement;
use anyhow::{ensure, Context, Result};
use firecracker_client::models::DirtyMemoryRanges;
use nix::unistd::Pid;
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{load_image_config, OverlaybdCompactOutput, UpperConfig, UpperMode};
use overlaybd::index::{Segment, SegmentMapping};
use overlaybd::index_file::{checkpoint_sparse_index, open_file_rw};
use overlaybd::layer::{export_sparse_layer, LayerFile};
use overlaybd::virtual_file::VirtualFile;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;

const FIRECRACKER_DIRTY_PAGE_SIZE: u64 = 4096;
const SPARSE_MEMORY_WRITE_CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub(super) async fn save_paused_memory(
    pid: Pid,
    dirty: &DirtyMemoryRanges,
    base_image_config_path: Option<&Path>,
    runtime_dir: &Path,
    overlaybd_global_config_path: &Path,
    owned_roots: &[PathBuf],
    base_layers: &mut LayerChain,
) -> SandboxCaptureResult<crate::sandbox::OverlaybdConfig> {
    let config_path = runtime_dir.join("mem_image.json");
    let upper = runtime_dir.join("mem_overlaybd/upper.data");
    let upper_config = if config_path.exists() {
        write_dirty_memory_to_sparse_upper(pid, dirty, &upper).await?;
        load_image_config(&config_path)?.upper
    } else {
        tokio::fs::create_dir_all(upper.parent().unwrap())
            .await
            .context("prepare memory upper")?;
        prepare_sparse_memory_upper(&upper, dirty.memory_size as u64)?;
        // This new file is not backing the running VM.
        write_dirty_memory_to_sparse_upper(pid, dirty, &upper)
            .await
            .map_err(|error| SandboxCaptureError::recoverable(error.into()))?;
        let roots = base_image_config_path
            .map(|path| runtime_owned_roots_for_live_config(owned_roots, path))
            .unwrap_or_default();
        let layers = rewrite_lowers_with_runtime_roots(
            base_layers,
            overlaybd_global_config_path,
            runtime_dir,
            None,
            "mem_compacted.commit",
            &roots,
            OverlaybdCompactOutput::Raw,
            false,
        )
        .await?
        .0;
        let mut image = layers.to_runtime_config();
        image.upper = UpperConfig {
            data: upper.display().to_string(),
            index: upper.with_extension("index").display().to_string(),
            mode: Some(UpperMode::Sparse),
            ..Default::default()
        };
        write_image_config(&config_path, &image, "paused memory")?;
        *base_layers = layers;
        image.upper
    };
    Ok(crate::sandbox::OverlaybdConfig {
        image_config_path: config_path,
        layers: Some(base_layers.clone()),
        upper: Some(upper_config),
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
    overlaybd_global_config_path: &Path,
    memory_encoding: OverlaybdCompactOutput,
    upper_needs_sealing: &mut bool,
    placement: SnapshotPlacement,
    owned_roots: &[PathBuf],
    base_layers: &mut LayerChain,
) -> Result<SnapshotMemoryArtifacts> {
    let compute_digest = placement == SnapshotPlacement::Distributed;
    let mut image = base_image_config_path
        .filter(|_| *upper_needs_sealing || compute_digest)
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
        commit_memory_upper(
            &mut image,
            base_layers,
            base_image_config_path.unwrap(),
            overlaybd_global_config_path,
            &roots,
            compute_digest,
        )
        .await?;
        *upper_needs_sealing = false;
    }
    if placement == SnapshotPlacement::Distributed && !image.upper.data.is_empty() {
        let previous: Vec<_> = base_layers
            .layers
            .iter()
            .filter_map(|layer| layer.path().map(Path::to_path_buf))
            .collect();
        let mut converted = base_layers.clone();
        let mut changed = false;
        for layer in &mut converted.layers {
            let LayerSource::File(source) = layer else {
                continue;
            };
            let destination = source.path.with_extension("published.commit");
            let mut temporary = None;
            let Some(mut prepared) = export_sparse_layer(
                source,
                || {
                    let file = NamedTempFile::new_in(source.path.parent().unwrap())?;
                    let path = file.path().to_path_buf();
                    temporary = Some(file);
                    Ok(path)
                },
                memory_encoding,
            )
            .await?
            else {
                continue;
            };
            temporary
                .expect("export created a staging file")
                .persist(&destination)
                .map_err(|error| error.error)?;
            prepared.path = destination;
            *source = prepared;
            changed = true;
        }
        if changed {
            let projected = converted.to_runtime_config();
            image.lowers = projected.lowers;
            image.repo_blob_url = projected.repo_blob_url;
            write_image_config(
                base_image_config_path.unwrap(),
                &image,
                "published memory base",
            )?;
            *base_layers = converted;
            remove_replaced_memory_layers(base_image_config_path.unwrap(), &previous, base_layers)
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
        let index: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::open_rw(path.with_extension("index"), false)?);
        let upper = open_file_rw(file, Some(index)).await?;
        let uuid = upper.get_uuid(0)?;
        upper.close_seal().await?;
        // Sealed lowers contain their index; only writable uppers need the sidecar.
        tokio::fs::remove_file(path.with_extension("index")).await?;
        Some(LayerSource::File(LayerFile::sealed_sparse(path, uuid)))
    } else {
        write_dirty_memory_layer(
            pid,
            dirty,
            &snapshot_dir.join("mem_overlaybd"),
            memory_encoding,
            compute_digest,
        )
        .await?
    };
    let delta_path = delta
        .as_ref()
        .and_then(|layer| layer.path().map(Path::to_path_buf));
    let compaction_output = match placement {
        SnapshotPlacement::Local => OverlaybdCompactOutput::Raw,
        SnapshotPlacement::Distributed => memory_encoding,
    };
    let (layers, compacted) = rewrite_lowers_with_runtime_roots(
        base_layers,
        overlaybd_global_config_path,
        snapshot_dir,
        delta,
        "mem_compacted.commit",
        &roots,
        compaction_output,
        compute_digest,
    )
    .await?;
    if let Some(delta) = delta_path {
        record_sealed_upper_size("memory", &delta, compacted).await;
    }
    Ok(SnapshotMemoryArtifacts {
        image_config_path: snapshot_dir.join("mem_image.json"),
        layers,
        virtual_size: checked_i64_to_u64(dirty.memory_size, "memory_size")?,
    })
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
    compute_digest: bool,
) -> Result<Option<LayerSource>> {
    let (mappings, memory_size) = dirty_ranges_to_segment_mappings(dirty_ranges)?;
    if mappings.is_empty() {
        return Ok(None);
    }

    let source_file: Arc<dyn VirtualFile> = Arc::new(ProcessVmReader::new(firecracker_pid));
    write_virtual_file_memory_layer(
        &[source_file],
        &mappings,
        memory_size,
        output_dir,
        mode,
        compute_digest,
    )
    .await
    .context("compact dirty memory ranges as overlaybd layer")
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
    let sparse_ranges: Vec<Segment> = dirty_ranges
        .ranges
        .iter()
        .flat_map(|range| {
            let mut offset = range.image_offset as u64 / OVERLAYBD_ALIGNMENT;
            let mut remaining = range.length as u64 / OVERLAYBD_ALIGNMENT;
            std::iter::from_fn(move || {
                if remaining == 0 {
                    return None;
                }
                let length = remaining.min(u64::from(Segment::MAX_LENGTH));
                let segment = Segment::new(offset, length as u32);
                offset += length;
                remaining -= length;
                Some(segment)
            })
        })
        .collect();
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
        checkpoint_sparse_index(&upper_path.with_extension("index"), sparse_ranges)
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
    checkpoint_sparse_index(&upper_path.with_extension("index"), sparse_ranges)
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
    use overlaybd::layer_metadata::resolve_local_layer_path;
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    #[tokio::test]
    async fn capture_resolves_relative_layers_from_the_opened_backing_path() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let global_config_path = &crate::cfg::ConfigManager::global_config()
            .memory_snapshot
            .overlaybd_global_config_path;
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
        let mut layers = crate::layer::load_layer_chain_from_config(&actual, None).await?;
        let snapshot = capture_memory_snapshot(
            Pid::this(),
            &DirtyMemoryRanges {
                page_size: 4096,
                memory_size: 4096,
                ranges: Vec::new(),
            },
            Some(&actual),
            &temp.path().join("snapshot"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        fs::remove_dir_all(original)?;
        fs::remove_dir_all(opened)?;
        let image = snapshot.layers.to_runtime_config();
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
        let data: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_rw(&path, false).unwrap());
        let index: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::open_rw(path.with_extension("index"), false).unwrap());
        let reopened = open_file_rw(data, Some(index)).await.unwrap();
        assert_eq!(reopened.read_at(0, 4096).await.unwrap(), vec![0x33; 4096]);
        assert!(reopened
            .read_at(4 * mib as u64, 4096)
            .await
            .unwrap()
            .iter()
            .all(|byte| *byte == 0));
    }

    #[tokio::test]
    async fn repeated_snapshots_preserve_the_fixed_base_and_previous_snapshots() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let global_config_path = &crate::cfg::ConfigManager::global_config()
            .memory_snapshot
            .overlaybd_global_config_path;
        let runtime = temp.path().join("runtime");
        fs::create_dir(&runtime)?;
        let mut upper_needs_sealing = true;
        let mut layers = LayerChain::default();
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
        let paused = save_paused_memory(
            Pid::this(),
            &dirty,
            None,
            &runtime,
            global_config_path,
            &[],
            &mut layers,
        )
        .await?;
        let clean = DirtyMemoryRanges {
            ranges: Vec::new(),
            ..dirty.clone()
        };
        let config = paused.image_config_path;
        let upper = runtime.join("mem_overlaybd/upper.data");
        let first = capture_memory_snapshot(
            Pid::this(),
            &clean,
            Some(&config),
            &temp.path().join("first"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        page.fill(0xa5);
        dirty.ranges[0].image_offset = 4096;
        let second = capture_memory_snapshot(
            Pid::this(),
            &dirty,
            Some(&config),
            &temp.path().join("second"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        let first_image = first.layers.to_runtime_config();
        let second_image = second.layers.to_runtime_config();
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

        for cycle in 0..3 {
            let changes = if cycle == 0 { &clean } else { &dirty };
            save_paused_memory(
                Pid::this(),
                changes,
                Some(&config),
                &runtime,
                global_config_path,
                &[],
                &mut layers,
            )
            .await?;
            upper_needs_sealing = true;
            page.fill(cycle as u8);
            let output = temp.path().join(format!("capture-{cycle}"));
            capture_memory_snapshot(
                Pid::this(),
                changes,
                Some(&config),
                &output,
                global_config_path,
                OverlaybdCompactOutput::Raw,
                &mut upper_needs_sealing,
                if cycle % 2 == 0 {
                    SnapshotPlacement::Local
                } else {
                    SnapshotPlacement::Distributed
                },
                &[],
                &mut layers,
            )
            .await?;
            fs::remove_dir_all(output)?;

            let image = load_image_config(&config)?;
            assert_eq!(image.lowers.len(), cycle + 1);
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
                        assert!(
                            fs::read_dir(&path)?.next().is_some(),
                            "empty directory: {}",
                            path.display()
                        );
                        directories.push(path);
                    } else if path != config
                        && path != upper
                        && path != Path::new(&image.upper.index)
                    {
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
        let output = temp.path().join("overlaybd.commit");
        let lower_tmp = output.with_extension("commit.tmp");
        let existing = b"existing final";
        tokio::fs::write(&output, existing)
            .await
            .expect("write existing final");
        let source: Arc<dyn VirtualFile> = Arc::new(FailingSource);
        write_virtual_file_memory_layer(
            &[source],
            &one_page_mapping(),
            OVERLAYBD_ALIGNMENT,
            temp.path(),
            OverlaybdCompactOutput::Raw,
            false,
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
