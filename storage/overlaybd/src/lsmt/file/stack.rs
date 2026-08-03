use super::helper::*;
use super::readonly::LSMTReadOnlyFile;
use super::readwrite::LSMTFile;
use super::types::*;
use anyhow::{anyhow, ensure, Context, Result};
use futures_util::stream::{self, StreamExt};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use uuid::Uuid;

use crate::io::virtual_file::VirtualFile;
use crate::lsmt::index::{ReadOnlyIndex, Segment};

pub async fn create_file_rw(args: LayerInfo) -> Result<LSMTFile> {
    LSMTFile::create_with_metadata(
        args.fdata.clone(),
        args.findex.clone(),
        args.virtual_size,
        args.rw_layout,
        args.uuid,
        args.parent_uuid,
        args.user_tag.as_deref(),
    )
    .await
}

pub async fn open_file_rw(
    fdata: Arc<dyn VirtualFile>,
    findex: Option<Arc<dyn VirtualFile>>,
) -> Result<LSMTFile> {
    LSMTFile::open(fdata, findex, None, vec![]).await
}

pub async fn open_file_ro(file: Arc<dyn VirtualFile>) -> Result<LSMTReadOnlyFile> {
    LSMTReadOnlyFile::open(file).await
}

/// Open multiple sealed lower layers provided in bottom-to-top order.
///
/// The input slice must be ordered from the oldest/base layer to the newest
/// lower (the direct parent of the writable upper). The returned
/// [`LSMTReadOnlyFile`] normalizes this into its internal top-to-bottom layer
/// order before merging indexes.
fn validate_open_files_ro_inputs(files: &[Arc<dyn VirtualFile>]) -> Result<()> {
    ensure!(!files.is_empty(), "empty file list");
    ensure!(
        files.len() <= MAX_STACK_LAYERS,
        "too many layers: {} > {}",
        files.len(),
        MAX_STACK_LAYERS
    );
    Ok(())
}

async fn merge_readonly_indexes(
    files: &[Arc<dyn VirtualFile>],
    metadata: &[ReadOnlyLayerMetadata],
) -> Result<Arc<ReadOnlyIndex>> {
    ensure!(
        files.len() == metadata.len(),
        "readonly files/metadata length mismatch"
    );
    let total_index_memory = metadata.iter().try_fold(0usize, |total, layer| {
        total
            .checked_add(stack_index_memory_bytes(layer.index_size)?)
            .context("stack index memory size overflow")
    })?;
    ensure!(
        total_index_memory <= MAX_STACK_INDEX_MEMORY_BYTES,
        "stack index memory {total_index_memory} exceeds limit {MAX_STACK_INDEX_MEMORY_BYTES}"
    );

    // `buffer_unordered` returns a stream whose `Future` implementation the
    // compiler cannot prove is `Send`, even though every input is `Send`.
    // This is a known rustc limitation with async closures that capture
    // non-`Send` intermediate state inside the generated state machine.
    // See: https://users.rust-lang.org/t/implementation-of-trait-is-not-general-enough-when-used-inside-tokio-spawn/122490/4
    //
    // `AlwaysSend` is safe to use here: `s` was constructed entirely from
    // `Send` types, so the wrapper does not introduce any actual unsafety —
    // it only silences a spurious compiler error.
    let s = stream::iter(
        files
            .iter()
            .cloned()
            .zip(metadata.iter().cloned())
            .enumerate(),
    )
    .map(|(layer_index, (file, metadata))| async move {
        let result = load_index_and_reset_tags(&file, metadata.index_offset, metadata.index_size)
            .await
            .map(ReadOnlyIndex::new);
        (layer_index, result)
    })
    .buffer_unordered(files.len().min(PARALLEL_LOAD_INDEX))
    .collect::<Vec<_>>();
    let results = storage_util::AlwaysSend::new(s).await;

    let mut ordered: Vec<Option<ReadOnlyIndex>> =
        std::iter::repeat_with(|| None).take(files.len()).collect();
    for (layer_index, result) in results {
        ordered[layer_index] =
            Some(result.context(format!("failed to load readonly layer {layer_index} index"))?);
    }

    let mut ro_indexes = Vec::with_capacity(files.len());
    for (layer_index, index) in ordered.into_iter().enumerate() {
        ro_indexes.push(index.context(format!(
            "parallel readonly index load returned missing layer slot {layer_index}"
        ))?);
    }
    ro_indexes.reverse();

    let refs: Vec<&ReadOnlyIndex> = ro_indexes.iter().collect();
    Ok(Arc::new(ReadOnlyIndex::merge(&refs)))
}

fn build_readonly_stack_from_merged(
    files: &[Arc<dyn VirtualFile>],
    metadata: &[ReadOnlyLayerMetadata],
    merged: Arc<ReadOnlyIndex>,
) -> Result<LSMTReadOnlyFile> {
    ensure!(
        files.len() == metadata.len(),
        "readonly files/metadata length mismatch"
    );

    let mut virtual_size = 0u64;
    for layer in metadata {
        if layer.virtual_size > 0 {
            virtual_size = layer.virtual_size;
        }
    }

    let mut ro_layers: Vec<Arc<dyn VirtualFile>> = files.to_vec();
    let mut uuids: Vec<Uuid> = metadata.iter().map(|layer| layer.uuid).collect();
    // ReadOnlyIndex::merge tags layers top-to-bottom (tag 0 is newest), while
    // open_files_ro receives layers bottom-to-top. Keep the cached artifact hit
    // path on the same tag-to-layer invariant as the merge path.
    ro_layers.reverse();
    uuids.reverse();

    Ok(LSMTReadOnlyFile::from_merged_layers(
        ro_layers,
        merged,
        virtual_size,
        uuids,
        LSMTFileType::ReadOnly,
    ))
}

pub async fn open_files_ro(files: &[Arc<dyn VirtualFile>]) -> Result<LSMTReadOnlyFile> {
    validate_open_files_ro_inputs(files)?;
    let metadata = load_readonly_layers_metadata(files).await?;
    let merged = merge_readonly_indexes(files, &metadata).await?;
    build_readonly_stack_from_merged(files, &metadata, merged)
}

pub async fn open_files_ro_with_premerged_cache(
    files: &[Arc<dyn VirtualFile>],
    cache_dir: impl AsRef<Path>,
    policy: PremergedIndexCachePolicy,
) -> Result<LSMTReadOnlyFile> {
    validate_open_files_ro_inputs(files)?;
    if !policy.read && !policy.write {
        return open_files_ro(files).await;
    }

    let cache_dir = cache_dir.as_ref().to_path_buf();
    if cache_dir.as_os_str().is_empty() {
        return open_files_ro(files).await;
    }

    let metadata = load_readonly_layers_metadata(files).await?;
    let Some(key) = PremergedIndexCacheKey::from_metadata(&metadata) else {
        let merged = merge_readonly_indexes(files, &metadata).await?;
        return build_readonly_stack_from_merged(files, &metadata, merged);
    };

    if policy.read {
        if let Some(merged) = try_read_premerged_index_artifact(&cache_dir, &key).await {
            return build_readonly_stack_from_merged(files, &metadata, merged);
        }
    }

    let lock = acquire_premerged_index_lock(&key.digest_hex).await;
    let guard = lock.clone().lock_owned().await;

    if policy.read {
        if let Some(merged) = try_read_premerged_index_artifact(&cache_dir, &key).await {
            let result = build_readonly_stack_from_merged(files, &metadata, merged);
            drop(guard);
            release_premerged_index_lock(&key.digest_hex, &lock).await;
            return result;
        }
    }

    let merged = match merge_readonly_indexes(files, &metadata).await {
        Ok(merged) => merged,
        Err(err) => {
            drop(guard);
            release_premerged_index_lock(&key.digest_hex, &lock).await;
            return Err(err);
        }
    };

    if policy.write {
        spawn_premerged_index_artifact_write(
            cache_dir.clone(),
            key.clone(),
            merged.clone(),
            policy.max_dir_bytes,
            lock.clone(),
            guard,
        );
        return build_readonly_stack_from_merged(files, &metadata, merged);
    }

    let result = build_readonly_stack_from_merged(files, &metadata, merged);
    drop(guard);
    release_premerged_index_lock(&key.digest_hex, &lock).await;
    result
}

pub async fn merge_files_ro(src_files: &[Arc<dyn VirtualFile>], args: CommitArgs) -> Result<()> {
    ensure!(!src_files.is_empty(), "empty src files");
    let lower = open_files_ro(src_files).await?;
    let query = Segment::new(0, lower.virtual_size.div_ceil(ALIGNMENT) as u32);
    let mut mappings = Vec::new();
    lower.index.lookup(query, &mut mappings);
    compact_to(&lower.layers, &mappings, lower.virtual_size, args).await
}

async fn verify_layer_order(
    layers: &[Arc<dyn VirtualFile>],
    uuids: &[Uuid],
    start_layer: usize,
) -> Result<()> {
    ensure!(
        layers.len() == uuids.len(),
        "layers and uuids size mismatch"
    );
    if start_layer >= layers.len() {
        return Ok(());
    }

    let mut parent_uuid: Option<Uuid> = None;
    for i in start_layer..layers.len() {
        if let Some(expected) = parent_uuid {
            ensure!(
                uuids[i] == expected,
                "parent uuid mismatch at layer {i}: got {}, expected {}",
                uuids[i],
                expected
            );
        }

        if i + 1 < layers.len() {
            let size = layers[i].size().await?;
            let ht = verify_ht(&layers[i], false, size).await?;
            parent_uuid = parse_uuid_field(&ht.parent_uuid);
        }
    }
    Ok(())
}

// NOTE: the lower_layers.layers order is as following:
// [top layer, top - 1 layer, ... , bottom layer].
// Top means the newest (the direct parent of upper) and the bottom means the oldest (base layer).
pub async fn stack_files(
    upper_layer: &LSMTFile,
    lower_layers: &LSMTReadOnlyFile,
    check_order: bool,
) -> Result<LSMTFile> {
    if check_order {
        verify_layer_order(&lower_layers.layers, &lower_layers.uuids, 1).await?;
    }
    let lower_index = lower_layers
        .index_view
        .clone()
        .ok_or_else(|| anyhow!("missing lower readonly index"))?;
    let mut stacked = LSMTFile::open(
        upper_layer.rw_data_file.clone(),
        upper_layer.rw_index_file.clone(),
        Some(lower_index),
        lower_layers.layers.clone(),
    )
    .await?;

    if stacked.virtual_size.load(Ordering::Acquire) == 0 {
        stacked.update_vsize(lower_layers.virtual_size).await?;
    }
    stacked.max_io_size.store(
        upper_layer.max_io_size.load(Ordering::Acquire),
        Ordering::Release,
    );
    stacked.group_commit_size.store(
        upper_layer.group_commit_size.load(Ordering::Acquire),
        Ordering::Release,
    );
    let mut uuids = lower_layers.uuids.clone();
    uuids.push(upper_layer.uuids.first().copied().unwrap_or_else(Uuid::nil));
    stacked.uuids = uuids;
    Ok(stacked)
}

pub async fn open_file_index(file: Arc<dyn VirtualFile>) -> Result<ReadOnlyIndex> {
    let file_size = file.size().await?;
    let trailer = verify_ht(&file, true, file_size).await?;
    validate_index_bounds(
        trailer.index_offset.get(),
        trailer.index_size.get(),
        file_size,
        HEADER_SIZE,
    )?;
    let mappings =
        load_index_and_reset_tags(&file, trailer.index_offset.get(), trailer.index_size.get())
            .await?;
    Ok(ReadOnlyIndex::new(mappings))
}

pub async fn is_lsmt(file: Arc<dyn VirtualFile>) -> i32 {
    let file_size = match file.size().await {
        Ok(s) => s,
        Err(_) => return 0,
    };
    if verify_ht(&file, false, file_size).await.is_err() {
        return 0;
    }
    if verify_ht(&file, true, file_size).await.is_ok() {
        return 1;
    }
    0
}
