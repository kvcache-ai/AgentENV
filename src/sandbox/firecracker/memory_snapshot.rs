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
        image.result_file.clear();
        write_image_config(&config_path, &image, "paused memory")?;
        *base_layers = layers;
    }
    Ok(crate::sandbox::OverlaybdConfig {
        image_config_path: config_path,
        layers: Some(base_layers.clone()),
        read_only: true,
        runtime_upper_mode: UpperMode::Sparse,
    })
}

fn same_layer_identity(left: &LayerSource, right: &LayerSource) -> bool {
    match (left, right) {
        (LayerSource::File(a), LayerSource::File(b)) => a.uuid == b.uuid,
        (
            LayerSource::Referenced { reference: a, .. },
            LayerSource::Referenced { reference: b, .. },
        ) => a == b,
        _ => false,
    }
}

/// Atomically retain `source` at `dest` via hard link, falling back to a
/// sparse-preserving copy into a temporary file followed by atomic rename so a
/// partial cross-filesystem copy can never be observed at `dest`.
async fn retain_runtime_layer(source: &Path, dest: &Path) -> Result<()> {
    let source = source.to_path_buf();
    let dest = dest.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        if std::fs::hard_link(&source, &dest).is_ok() {
            return Ok(());
        }
        let parent = dest.parent().context("runtime layer has no parent")?;
        let temp_path = parent.join(format!(".tmp-{}.commit", uuid::Uuid::new_v4()));
        if let Err(err) = crate::layer::retain_layer_file_blocking(&source, &temp_path) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(err.into());
        }
        if let Err(err) = std::fs::rename(&temp_path, &dest) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(err.into());
        }
        Ok(())
    })
    .await
    .context("join runtime layer retain task")?
}

/// Promote newly created snapshot layers into `runtime_dir/mem_overlaybd` and
/// advance `base_layers` without mutating any writable upper file.
///
/// Unchanged runtime layers (both `File` and `Referenced` with `local_file`)
/// keep their existing runtime paths (O(1) per new layer). Any failure or
/// cancellation before `*base_layers = next` cleans up newly created files and
/// leaves the prior baseline intact and recoverable.
async fn advance_runtime_memory_baseline(
    runtime_dir: &Path,
    snapshot_dir: &Path,
    snapshot_layers: &LayerChain,
    base_layers: &mut LayerChain,
) -> Result<()> {
    let mem_dir = runtime_dir.join("mem_overlaybd");
    tokio::fs::create_dir_all(&mem_dir).await?;
    let snapshot_root =
        std::fs::canonicalize(snapshot_dir).unwrap_or_else(|_| snapshot_dir.to_path_buf());

    let mut next = snapshot_layers.clone();
    let mut created = Vec::new();
    let prepare = async {
        for (index, layer) in next.layers.iter_mut().enumerate() {
            let Some(path) = layer.path() else {
                continue;
            };
            let resolved = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
            if !resolved.starts_with(&snapshot_root) {
                continue;
            }
            if let Some(prev_path) = base_layers
                .layers
                .get(index)
                .filter(|prev| same_layer_identity(prev, layer))
                .and_then(LayerSource::path)
                .filter(|prev_path| {
                    let prev_resolved = std::fs::canonicalize(prev_path)
                        .unwrap_or_else(|_| prev_path.to_path_buf());
                    !prev_resolved.starts_with(&snapshot_root)
                })
            {
                layer.relocate_file(prev_path.to_path_buf());
                continue;
            }
            let file_name = match layer {
                LayerSource::File(file) => format!("layer-{}.commit", file.uuid),
                LayerSource::Referenced { .. } => {
                    format!("ref-{index}-{}.commit", uuid::Uuid::new_v4())
                }
            };
            let dest = mem_dir.join(file_name);
            retain_runtime_layer(&resolved, &dest).await?;
            created.push(dest.clone());
            layer.relocate_file(dest);
        }
        let config_path = runtime_dir.join("mem_image.json");
        if config_path.exists() {
            let mut image = load_image_config(&config_path)?;
            let projected = next.to_runtime_config();
            image.lowers = projected.lowers;
            image.repo_blob_url = projected.repo_blob_url;
            write_image_config(&config_path, &image, "advanced memory base")?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(err) = prepare {
        for path in created {
            let _ = tokio::fs::remove_file(path).await;
        }
        return Err(err);
    }

    let previous: Vec<_> = base_layers
        .layers
        .iter()
        .filter_map(|layer| layer.path().map(Path::to_path_buf))
        .collect();
    *base_layers = next;
    let cleanup_anchor = runtime_dir.join("mem_image.json");
    remove_replaced_memory_layers(&cleanup_anchor, &previous, base_layers).await;
    Ok(())
}

/// Capture an independent memory snapshot.
///
/// An already-resumed paused upper is sealed once. When `advance_baseline` is
/// set on a Local snapshot, the newly sealed delta (or compacted layer) is
/// hardlinked into `runtime_memory_dir/mem_overlaybd` and `base_layers` is
/// advanced only after export succeeds so the caller can safely CLEAR
/// Firecracker's dirty bitmap afterwards.
#[allow(clippy::too_many_arguments)]
pub(super) async fn capture_memory_snapshot(
    pid: Pid,
    dirty: &DirtyMemoryRanges,
    base_image_config_path: Option<&Path>,
    runtime_memory_dir: &Path,
    advance_baseline: bool,
    snapshot_dir: &Path,
    overlaybd_global_config_path: &Path,
    memory_encoding: OverlaybdCompactOutput,
    upper_needs_sealing: &mut bool,
    placement: SnapshotPlacement,
    owned_roots: &[PathBuf],
    base_layers: &mut LayerChain,
) -> Result<SnapshotMemoryArtifacts> {
    let compute_digest = placement == SnapshotPlacement::Distributed;
    let sealed_paused_upper = *upper_needs_sealing;
    let mut image = if *upper_needs_sealing {
        let config_path = base_image_config_path.context("paused memory config path missing")?;
        let image = load_image_config(config_path)?;
        let upper = PathBuf::from(&image.upper.data);
        write_dirty_memory_to_sparse_upper(pid, dirty, &upper).await?;
        image
    } else if compute_digest {
        base_image_config_path
            .map(load_image_config)
            .transpose()?
            .unwrap_or_default()
    } else {
        Default::default()
    };

    let mut roots = base_image_config_path
        .map(|path| runtime_owned_roots_for_live_config(owned_roots, path))
        .unwrap_or_else(|| owned_roots.to_vec());
    let runtime_root = std::fs::canonicalize(runtime_memory_dir)
        .unwrap_or_else(|_| runtime_memory_dir.to_path_buf());
    if !roots.contains(&runtime_root) {
        roots.push(runtime_root.clone());
    }

    if *upper_needs_sealing {
        let config_path = base_image_config_path.context("paused memory config path missing")?;
        commit_memory_upper(
            &mut image,
            base_layers,
            config_path,
            overlaybd_global_config_path,
            &roots,
            compute_digest,
        )
        .await?;
        *upper_needs_sealing = false;
    }
    if placement == SnapshotPlacement::Distributed {
        let previous: Vec<_> = base_layers
            .layers
            .iter()
            .filter_map(|layer| layer.path().map(Path::to_path_buf))
            .collect();
        let mut converted = base_layers.clone();
        let mut changed = false;
        let has_paused_upper = !image.upper.data.is_empty();
        for layer in &mut converted.layers {
            let LayerSource::File(source) = layer else {
                continue;
            };
            let resolved =
                std::fs::canonicalize(&source.path).unwrap_or_else(|_| source.path.clone());
            if !has_paused_upper && !resolved.starts_with(&runtime_root) {
                continue;
            }
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
            if let Some(config_path) = base_image_config_path.filter(|_| has_paused_upper) {
                let projected = converted.to_runtime_config();
                image.lowers = projected.lowers;
                image.repo_blob_url = projected.repo_blob_url;
                write_image_config(config_path, &image, "published memory base")?;
            }
            *base_layers = converted;
            let runtime_anchor = runtime_memory_dir.join("mem_image.json");
            remove_replaced_memory_layers(&runtime_anchor, &previous, base_layers).await;
            if has_paused_upper {
                if let Some(config_path) = base_image_config_path {
                    remove_replaced_memory_layers(config_path, &previous, base_layers).await;
                }
            }
        }
    }
    let delta = if sealed_paused_upper {
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
        Some(LayerFile::sealed_sparse(path, uuid))
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
    let delta_path = delta.as_ref().map(|layer| layer.path.clone());
    let has_new_delta = delta.is_some();
    let compaction_output = match placement {
        SnapshotPlacement::Local => OverlaybdCompactOutput::Raw,
        SnapshotPlacement::Distributed => memory_encoding,
    };
    tokio::fs::create_dir_all(snapshot_dir).await?;
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
    if advance_baseline && has_new_delta {
        advance_runtime_memory_baseline(runtime_memory_dir, snapshot_dir, &layers, base_layers)
            .await?;
    }
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
) -> Result<Option<LayerFile>> {
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
            br#"{"lowers":[{"file":"./base.commit"},{"dir":"./trace-layer"}],"accelerationLayer":true,"upper":{},"resultFile":"./reader-result.json","download":{"enable":true,"concurrency":3}}"#,
        )?;
        let actual = opened.join("image.json");
        std::os::unix::fs::symlink(&config, &actual)?;
        let mut upper_needs_sealing = false;
        let mut layers = crate::layer::load_layer_chain_from_config(&actual, None).await?;
        let (runtime_source, _) = crate::sandbox::ublk::load_runtime_source(&actual)?;
        assert_eq!(layers.layers.len(), 1);
        assert_eq!(runtime_source.lowers.len(), 1);
        assert!(!runtime_source.acceleration_layer);
        let snapshot = capture_memory_snapshot(
            Pid::this(),
            &DirtyMemoryRanges {
                page_size: 4096,
                memory_size: 4096,
                ranges: Vec::new(),
            },
            Some(&actual),
            temp.path(),
            false,
            &temp.path().join("snapshot"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        assert!(!snapshot.image_config_path.exists());
        let paused = save_paused_memory(
            Pid::this(),
            &DirtyMemoryRanges {
                page_size: 4096,
                memory_size: 4096,
                ranges: Vec::new(),
            },
            Some(&actual),
            &temp.path().join("paused"),
            global_config_path,
            &[],
            &mut layers,
        )
        .await?;
        let paused_image = load_image_config(&paused.image_config_path)?;
        assert!(paused_image.download_override.is_none());
        assert!(paused_image.result_file.is_empty());
        fs::remove_dir_all(original)?;
        fs::remove_dir_all(opened)?;
        let image = snapshot.layers.to_runtime_config();
        assert!(image.download_override.is_none());
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
        let first_image = capture_memory_snapshot(
            Pid::this(),
            &clean,
            Some(&config),
            &runtime,
            false,
            &temp.path().join("first"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?
        .layers
        .to_runtime_config();
        page.fill(0xa5);
        dirty.ranges[0].image_offset = 4096;
        let second_image = capture_memory_snapshot(
            Pid::this(),
            &dirty,
            Some(&config),
            &runtime,
            false,
            &temp.path().join("second"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?
        .layers
        .to_runtime_config();
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
                &runtime,
                false,
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

    #[tokio::test]
    async fn advancing_local_snapshots_chain_incremental_deltas_and_survive_runtime_removal(
    ) -> Result<()> {
        let temp = tempfile::tempdir()?;
        let global_config_path = &crate::cfg::ConfigManager::global_config()
            .memory_snapshot
            .overlaybd_global_config_path;
        let runtime = temp.path().join("runtime");
        fs::create_dir(&runtime)?;
        let mut upper_needs_sealing = false;
        let mut layers = LayerChain::default();
        let mut allocation = vec![0u8; 4096 * 4];
        let offset = (4096 - allocation.as_ptr() as usize % 4096) % 4096;
        let page = &mut allocation[offset..offset + 4096];

        // Snapshot 1: cold start, page 0 = 0x11.
        page.fill(0x11);
        let dirty1 = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: 4096 * 3,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: page.as_ptr() as i64,
                image_offset: 0,
                length: 4096,
            }],
        };
        let snap1 = capture_memory_snapshot(
            Pid::this(),
            &dirty1,
            None,
            &runtime,
            true,
            &temp.path().join("snap1"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        assert_eq!(snap1.layers.layers.len(), 1);
        assert_eq!(layers.layers.len(), 1);

        // Snapshot 2: post-CLEAR incremental delta, only page 1 = 0x22.
        page.fill(0x22);
        let dirty2 = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: 4096 * 3,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: page.as_ptr() as i64,
                image_offset: 4096,
                length: 4096,
            }],
        };
        let snap2 = capture_memory_snapshot(
            Pid::this(),
            &dirty2,
            None,
            &runtime,
            true,
            &temp.path().join("snap2"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        assert_eq!(snap2.layers.layers.len(), 2);

        // Snapshot 3: cumulative sibling fallback (advance_baseline = false), page 2 = 0x33.
        page.fill(0x33);
        let dirty3 = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: 4096 * 3,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: page.as_ptr() as i64,
                image_offset: 8192,
                length: 4096,
            }],
        };
        let snap3 = capture_memory_snapshot(
            Pid::this(),
            &dirty3,
            None,
            &runtime,
            false,
            &temp.path().join("snap3"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        assert_eq!(snap3.layers.layers.len(), 3);
        assert_eq!(
            layers.layers.len(),
            2,
            "cumulative fallback must not advance runtime baseline"
        );

        // Delete runtime directory; all 3 snapshots must remain independently readable.
        fs::remove_dir_all(&runtime)?;
        let open_chain = |chain: &LayerChain| {
            let files: Vec<Arc<dyn VirtualFile>> = chain
                .layers
                .iter()
                .map(|layer| {
                    Arc::new(LocalFile::open_ro(layer.path().unwrap()).unwrap())
                        as Arc<dyn VirtualFile>
                })
                .collect();
            async move { overlaybd::index_file::open_files_ro(&files).await }
        };
        let ro1 = open_chain(&snap1.layers).await?;
        assert_eq!(ro1.read_at(0, 4096).await?.as_ref(), &[0x11; 4096]);
        assert_eq!(ro1.read_at(4096, 4096).await?.as_ref(), &[0x00; 4096]);
        let ro2 = open_chain(&snap2.layers).await?;
        assert_eq!(ro2.read_at(0, 4096).await?.as_ref(), &[0x11; 4096]);
        assert_eq!(ro2.read_at(4096, 4096).await?.as_ref(), &[0x22; 4096]);
        let ro3 = open_chain(&snap3.layers).await?;
        assert_eq!(ro3.read_at(0, 4096).await?.as_ref(), &[0x11; 4096]);
        assert_eq!(ro3.read_at(4096, 4096).await?.as_ref(), &[0x22; 4096]);
        assert_eq!(ro3.read_at(8192, 4096).await?.as_ref(), &[0x33; 4096]);
        Ok(())
    }

    #[tokio::test]
    async fn second_advancing_local_snapshot_failure_is_recoverable_and_preserves_baseline(
    ) -> Result<()> {
        let temp = tempfile::tempdir()?;
        let global_config_path = &crate::cfg::ConfigManager::global_config()
            .memory_snapshot
            .overlaybd_global_config_path;
        let runtime = temp.path().join("runtime");
        fs::create_dir(&runtime)?;
        let mut upper_needs_sealing = false;
        let mut layers = LayerChain::default();
        let batch_size = SPARSE_MEMORY_WRITE_CHUNK_SIZE;
        let mem_size = (batch_size + 4096 * 2) as i64;
        let mut backing = vec![0u8; batch_size + 4096 * 3];
        let alignment = backing.as_ptr().align_offset(4096);
        let region = &mut backing[alignment..alignment + batch_size + 4096];
        region[..4096].fill(0x11);

        // Snapshot 1: establish sealed baseline at page 0 = 0x11.
        let dirty1 = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: mem_size,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: region.as_ptr() as i64,
                image_offset: 0,
                length: 4096,
            }],
        };
        let snap1 = capture_memory_snapshot(
            Pid::this(),
            &dirty1,
            None,
            &runtime,
            true,
            &temp.path().join("snap1"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        assert_eq!(layers.layers.len(), 1);

        // Snapshot 2 (failing): first 4 MiB batch succeeds, second batch fails
        // to read remote HVA 0 — must remain Recoverable and leave prior
        // runtime baseline untouched.
        region[4096..4096 + batch_size].fill(0x22);
        let failing_dirty2 = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: mem_size,
            ranges: vec![
                DirtyMemoryRange {
                    base_host_virt_addr: region[4096..].as_ptr() as i64,
                    image_offset: 4096,
                    length: batch_size as i64,
                },
                DirtyMemoryRange {
                    base_host_virt_addr: 0,
                    image_offset: (4096 + batch_size) as i64,
                    length: 4096,
                },
            ],
        };
        let err = capture_memory_snapshot(
            Pid::this(),
            &failing_dirty2,
            None,
            &runtime,
            true,
            &temp.path().join("snap2-failed"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await
        .expect_err("second snapshot with invalid second batch must fail");
        assert!(
            !SandboxCaptureError::from(err).is_terminal(),
            "failure on second advancing Local snapshot must be recoverable"
        );
        assert_eq!(
            layers.layers.len(),
            1,
            "failed second snapshot must preserve prior baseline chain"
        );

        // Retry snapshot 2 on the same runtime after resuming without CLEAR.
        let retry_dirty2 = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: mem_size,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: region[4096..].as_ptr() as i64,
                image_offset: 4096,
                length: 4096,
            }],
        };
        let snap2 = capture_memory_snapshot(
            Pid::this(),
            &retry_dirty2,
            None,
            &runtime,
            true,
            &temp.path().join("snap2-retry"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        assert_eq!(snap2.layers.layers.len(), 2);
        assert_eq!(layers.layers.len(), 2);

        fs::remove_dir_all(&runtime)?;
        let open_chain = |chain: &LayerChain| {
            let files: Vec<Arc<dyn VirtualFile>> = chain
                .layers
                .iter()
                .map(|layer| {
                    Arc::new(LocalFile::open_ro(layer.path().unwrap()).unwrap())
                        as Arc<dyn VirtualFile>
                })
                .collect();
            async move { overlaybd::index_file::open_files_ro(&files).await }
        };
        let ro1 = open_chain(&snap1.layers).await?;
        assert_eq!(ro1.read_at(0, 4096).await?.as_ref(), &[0x11; 4096]);
        assert_eq!(ro1.read_at(4096, 4096).await?.as_ref(), &[0x00; 4096]);
        let ro2 = open_chain(&snap2.layers).await?;
        assert_eq!(ro2.read_at(0, 4096).await?.as_ref(), &[0x11; 4096]);
        assert_eq!(ro2.read_at(4096, 4096).await?.as_ref(), &[0x22; 4096]);
        Ok(())
    }

    #[tokio::test]
    async fn advancing_baseline_survives_exported_snapshot_deletion_and_reclaims_sparse_on_publish(
    ) -> Result<()> {
        let temp = tempfile::tempdir()?;
        let global_config_path = &crate::cfg::ConfigManager::global_config()
            .memory_snapshot
            .overlaybd_global_config_path;
        let runtime = temp.path().join("runtime");
        let runtime_mem = runtime.join("mem_overlaybd");
        fs::create_dir_all(&runtime_mem)?;

        // Seed a runtime-owned Referenced layer with a local_file in runtime/mem_overlaybd.
        let ref_layer_path = runtime_mem.join("referenced-base.commit");
        prepare_sparse_memory_upper(&ref_layer_path, 4096 * 3)?;
        let ref_file = LocalFile::open_rw(&ref_layer_path, false)?;
        overlaybd::index_file::write_sparse_upper(&ref_file, 0, &[0x77; 4096]).await?;
        checkpoint_sparse_index(
            &ref_layer_path.with_extension("index"),
            vec![Segment::new(0, (4096 / OVERLAYBD_ALIGNMENT) as u32)],
        )
        .await?;
        open_file_rw(
            Arc::new(ref_file),
            Some(Arc::new(LocalFile::open_rw(
                ref_layer_path.with_extension("index"),
                false,
            )?)),
        )
        .await?
        .close_seal()
        .await?;
        fs::remove_file(ref_layer_path.with_extension("index"))?;

        let mut layers = LayerChain::from(vec![LayerSource::Referenced {
            reference: crate::layer::LayerRef::Managed(crate::layer::ManagedLayerRef {
                digest: "sha256:0000000000000000000000000000000000000000000000000000000000000001"
                    .to_string(),
                size: fs::metadata(&ref_layer_path)?.len(),
                uuid: None,
            }),
            local_file: Some(ref_layer_path.clone()),
        }]);
        let mut upper_needs_sealing = false;
        let mut allocation = vec![0u8; 4096 * 4];
        let offset = (4096 - allocation.as_ptr() as usize % 4096) % 4096;
        let page = &mut allocation[offset..offset + 4096];

        // Snapshot 1 (Local, advancing): exports Referenced layer + delta1.
        page.fill(0x11);
        let dirty1 = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: 4096 * 3,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: page.as_ptr() as i64,
                image_offset: 4096,
                length: 4096,
            }],
        };
        let snap1_dir = temp.path().join("snap1");
        capture_memory_snapshot(
            Pid::this(),
            &dirty1,
            None,
            &runtime,
            true,
            &snap1_dir,
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;

        // Delete exported snap1 directory while the VM keeps running; base_layers
        // must still point exclusively into runtime (including Referenced local_file).
        fs::remove_dir_all(&snap1_dir)?;
        assert_eq!(layers.layers[0].path(), Some(ref_layer_path.as_path()));
        let sparse_delta1_path = layers.layers[1]
            .path()
            .expect("delta1 layer path")
            .to_path_buf();
        assert!(sparse_delta1_path.starts_with(&runtime_mem));
        assert!(sparse_delta1_path.exists());

        // Snapshot 2 (Local, advancing) succeeds after snap1 deletion.
        page.fill(0x22);
        let dirty2 = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: 4096 * 3,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: page.as_ptr() as i64,
                image_offset: 8192,
                length: 4096,
            }],
        };
        let snap2 = capture_memory_snapshot(
            Pid::this(),
            &dirty2,
            None,
            &runtime,
            true,
            &temp.path().join("snap2"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Local,
            &[],
            &mut layers,
        )
        .await?;
        assert_eq!(snap2.layers.layers.len(), 3);

        // Snapshot 3 (Distributed) with an external source snapshot config must
        // reclaim the old sparse runtime layer files in `runtime/mem_overlaybd`.
        let source_snap_dir = temp.path().join("source-snap");
        fs::create_dir(&source_snap_dir)?;
        let external_config = source_snap_dir.join("mem_image.json");
        fs::write(
            &external_config,
            r#"{"lowers":[{"file":"./base.commit"}],"upper":{}}"#,
        )?;
        let clean = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: 4096 * 3,
            ranges: Vec::new(),
        };
        capture_memory_snapshot(
            Pid::this(),
            &clean,
            Some(&external_config),
            &runtime,
            false,
            &temp.path().join("snap3-dist"),
            global_config_path,
            OverlaybdCompactOutput::Raw,
            &mut upper_needs_sealing,
            SnapshotPlacement::Distributed,
            &[],
            &mut layers,
        )
        .await?;
        assert!(
            !sparse_delta1_path.exists(),
            "old sparse runtime layer should be reclaimed after Distributed publish"
        );

        // Deleting runtime leaves snap2 readable across all 3 layers.
        fs::remove_dir_all(&runtime)?;
        let files: Vec<Arc<dyn VirtualFile>> = snap2
            .layers
            .layers
            .iter()
            .map(|layer| {
                Arc::new(LocalFile::open_ro(layer.path().unwrap()).unwrap()) as Arc<dyn VirtualFile>
            })
            .collect();
        let ro2 = overlaybd::index_file::open_files_ro(&files).await?;
        assert_eq!(ro2.read_at(0, 4096).await?.as_ref(), &[0x77; 4096]);
        assert_eq!(ro2.read_at(4096, 4096).await?.as_ref(), &[0x11; 4096]);
        assert_eq!(ro2.read_at(8192, 4096).await?.as_ref(), &[0x22; 4096]);
        Ok(())
    }

    #[tokio::test]
    async fn retain_runtime_layer_replaces_stale_dest_atomically() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let src = temp.path().join("src.commit");
        let dest = temp.path().join("dest.commit");
        fs::write(&src, b"complete-layer-bytes")?;
        fs::write(&dest, b"stale-partial")?;

        retain_runtime_layer(&src, &dest).await?;
        assert_eq!(fs::read(&dest)?, b"complete-layer-bytes");
        for entry in fs::read_dir(temp.path())? {
            let name = entry?.file_name();
            assert!(!name.to_string_lossy().starts_with(".tmp-"));
        }

        // Failed retain leaves existing dest untouched and cleans temp files.
        assert!(
            retain_runtime_layer(&temp.path().join("missing.commit"), &dest)
                .await
                .is_err()
        );
        assert_eq!(fs::read(&dest)?, b"complete-layer-bytes");
        Ok(())
    }
}
