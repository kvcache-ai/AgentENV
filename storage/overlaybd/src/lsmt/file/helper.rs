use super::types::*;
use anyhow::{anyhow, bail, ensure, Context, Result};
use futures_util::stream::{self, StreamExt};
use sha2::{Digest, Sha256};
use std::cmp::min;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::mem::size_of;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, OwnedMutexGuard};
use uuid::Uuid;
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, IntoBytes};

use crate::io::virtual_file::VirtualFile;
use crate::lsmt::format::{DiskSegmentMapping, HeaderTrailer};
use crate::lsmt::index::{compress_raw_index, ReadOnlyIndex, Segment, SegmentMapping};
use storage_util::{AlignedBuffer, CompactWriter};

/// Validate the headers and physical layout of an unsealed RW layer pair.
///
/// This is intended for external tools that mutate an OverlayBD upper in place
/// and need a cheap structural check before reopening the image.
pub fn validate_rw_header_pair_paths(
    data_path: &Path,
    index_path: Option<&Path>,
    expected_virtual_size: u64,
    expected_layout: RwLayout,
) -> Result<()> {
    fn read_header(path: &Path, expected_virtual_size: u64) -> Result<(HeaderTrailer, u64)> {
        let file = File::open(path).with_context(|| format!("open RW layer {}", path.display()))?;
        let size = file
            .metadata()
            .with_context(|| format!("stat RW layer {}", path.display()))?
            .len();
        ensure!(
            size >= HEADER_SIZE,
            "RW layer is shorter than its header block: {}",
            path.display()
        );
        let mut bytes = vec![0u8; size_of::<HeaderTrailer>()];
        file.read_exact_at(&mut bytes, 0)
            .with_context(|| format!("read RW header {}", path.display()))?;
        let header = HeaderTrailer::read_from_bytes(&bytes)
            .map_err(|_| anyhow!("invalid RW header bytes: {}", path.display()))?;
        ensure!(
            header.verify_magic(),
            "invalid RW header magic: {}",
            path.display()
        );
        ensure!(
            header.is_header() && !header.is_sealed(),
            "expected unsealed RW header: {}",
            path.display()
        );
        ensure!(
            header.virtual_size.get() == expected_virtual_size,
            "RW virtual size mismatch in {}: expected {}, got {}",
            path.display(),
            expected_virtual_size,
            header.virtual_size.get()
        );
        Ok((header, size))
    }

    let (data_header, data_size) = read_header(data_path, expected_virtual_size)?;
    ensure!(
        data_header.is_data_file(),
        "RW data header has index-file type: {}",
        data_path.display()
    );
    let actual_layout = match (data_header.is_sparse_rw(), data_header.is_hybrid_rw()) {
        (false, false) => RwLayout::LogStructured,
        (true, false) => RwLayout::Sparse,
        (false, true) => RwLayout::HybridLogStructured,
        (true, true) => bail!("RW data header has both sparse and hybrid flags"),
    };
    ensure!(
        actual_layout == expected_layout,
        "RW layout mismatch: expected {expected_layout:?}, got {actual_layout:?}"
    );

    match expected_layout {
        RwLayout::Sparse => {
            ensure!(
                index_path.is_none(),
                "sparse RW layer must not have an index file"
            );
            ensure!(
                data_size == HEADER_SIZE + expected_virtual_size,
                "sparse RW data size mismatch: expected {}, got {}",
                HEADER_SIZE + expected_virtual_size,
                data_size
            );
        }
        RwLayout::LogStructured | RwLayout::HybridLogStructured => {
            let index_path =
                index_path.context("log-structured RW layer requires an index file")?;
            let (index_header, index_size) = read_header(index_path, expected_virtual_size)?;
            ensure!(
                !index_header.is_data_file(),
                "RW index header has data-file type: {}",
                index_path.display()
            );
            let data_uuid = data_header.uuid;
            let index_uuid = index_header.uuid;
            ensure!(index_uuid == data_uuid, "RW data/index UUID mismatch");
            ensure!(
                index_header.index_offset.get() == HEADER_SIZE,
                "RW index header has invalid unsealed index offset: {}",
                index_header.index_offset.get()
            );
            ensure!(
                index_header.is_hybrid_rw() == data_header.is_hybrid_rw(),
                "RW data/index hybrid flag mismatch"
            );
            ensure!(
                data_size.is_multiple_of(ALIGNMENT),
                "RW data file size is not {ALIGNMENT}-byte aligned: {data_size}"
            );
            ensure!(
                index_size >= HEADER_SIZE
                    && (index_size - HEADER_SIZE)
                        .is_multiple_of(size_of::<DiskSegmentMapping>() as u64),
                "RW index mapping area is misaligned: {index_size}"
            );
        }
    }
    Ok(())
}

pub(super) fn parse_uuid_field(field: &[u8]) -> Option<Uuid> {
    let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
    if end == 0 {
        return None;
    }
    let s = std::str::from_utf8(&field[..end]).ok()?;
    Uuid::parse_str(s).ok()
}

fn fill_uuid_field(dst: &mut [u8], uuid: Option<Uuid>) {
    dst.fill(0);
    if let Some(u) = uuid {
        let s = u.hyphenated().to_string();
        let bytes = s.as_bytes();
        let n = bytes.len().min(dst.len().saturating_sub(1));
        dst[..n].copy_from_slice(&bytes[..n]);
    }
}

fn fill_user_tag(dst: &mut [u8], user_tag: Option<&[u8]>) {
    dst.fill(0);
    if let Some(tag) = user_tag {
        let n = tag.len().min(dst.len());
        dst[..n].copy_from_slice(&tag[..n]);
    }
}

pub(super) fn parse_user_tag_field(field: &[u8]) -> Option<Vec<u8>> {
    let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
    if end == 0 {
        return None;
    }
    Some(field[..end].to_vec())
}

pub(super) fn rw_header(
    virtual_size: u64,
    rw_layout: RwLayout,
    uuid: Uuid,
    parent_uuid: Option<Uuid>,
    user_tag: Option<&[u8]>,
) -> HeaderTrailer {
    let mut header = HeaderTrailer::new();
    header.size = U32::new(size_of::<HeaderTrailer>() as u32);
    header.virtual_size = U64::new(virtual_size);

    header.set_header();
    header.set_data_file();
    match rw_layout {
        RwLayout::LogStructured => {}
        RwLayout::Sparse => header.set_sparse_rw(),
        RwLayout::HybridLogStructured => header.set_hybrid_rw(),
    }

    header.set_uuid(&uuid);
    fill_uuid_field(&mut header.parent_uuid, parent_uuid);
    fill_user_tag(&mut header.user_tag, user_tag);
    header
}

fn copy_header_block(dst: &mut [u8], header: &HeaderTrailer) {
    dst.fill(0);
    dst[..size_of::<HeaderTrailer>()].copy_from_slice(header.as_bytes());
}

fn append_u32(dst: &mut Vec<u8>, value: u32) {
    dst.extend_from_slice(&value.to_le_bytes());
}

fn append_u64(dst: &mut Vec<u8>, value: u64) {
    dst.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn read_u32(src: &[u8], offset: usize) -> Result<u32> {
    let bytes: [u8; 4] = src
        .get(offset..offset + 4)
        .context("premerged artifact header is truncated")?
        .try_into()
        .map_err(|_| anyhow!("invalid u32 in premerged artifact header"))?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(src: &[u8], offset: usize) -> Result<u64> {
    let bytes: [u8; 8] = src
        .get(offset..offset + 8)
        .context("premerged artifact header is truncated")?
        .try_into()
        .map_err(|_| anyhow!("invalid u64 in premerged artifact header"))?;
    Ok(u64::from_le_bytes(bytes))
}

fn copy_array<const N: usize>(src: &[u8], offset: usize) -> Result<[u8; N]> {
    let bytes: [u8; N] = src
        .get(offset..offset + N)
        .context("premerged artifact header is truncated")?
        .try_into()
        .map_err(|_| anyhow!("invalid array in premerged artifact header"))?;
    Ok(bytes)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

impl PremergedIndexCacheKey {
    pub(super) fn from_metadata(metadata: &[ReadOnlyLayerMetadata]) -> Option<Self> {
        if metadata.is_empty() || metadata.iter().any(|m| m.uuid.is_nil()) {
            return None;
        }

        let mut key = Vec::new();
        key.extend_from_slice(b"overlaybd-premerged-index-key-v1");
        append_u32(&mut key, PREMERGED_INDEX_FORMAT_VERSION);
        append_u32(&mut key, PREMERGED_INDEX_MERGE_VERSION);
        append_u32(&mut key, metadata.len() as u32);

        // The merged index stores layer tags in top-to-bottom order, so the key
        // uses the same order even though open_files_ro receives bottom-to-top.
        for layer in metadata.iter().rev() {
            key.extend_from_slice(layer.uuid.as_bytes());
            append_u64(&mut key, layer.file_size);
            append_u64(&mut key, layer.virtual_size);
            append_u64(&mut key, layer.index_offset);
            append_u64(&mut key, layer.index_size);
            key.push(layer.header_version);
            key.push(layer.header_sub_version);
            key.push(layer.trailer_version);
            key.push(layer.trailer_sub_version);
        }

        let digest = Sha256::digest(&key);
        let mut digest_bytes = [0u8; 32];
        digest_bytes.copy_from_slice(&digest);
        Some(Self {
            digest: digest_bytes,
            digest_hex: hex_lower(&digest_bytes),
            layer_count: metadata.len(),
        })
    }

    pub(super) fn artifact_path(&self, cache_dir: &Path) -> PathBuf {
        cache_dir
            .join(PREMERGED_INDEX_DIR)
            .join(format!("{}.{}", self.digest_hex, PREMERGED_INDEX_EXT))
    }
}

pub(super) fn premerged_index_locks() -> &'static Mutex<HashMap<String, Weak<Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Weak<Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) async fn acquire_premerged_index_lock(key: &str) -> Arc<Mutex<()>> {
    let mut locks = premerged_index_locks().lock().await;
    if locks.len() > 1024 {
        locks.retain(|_, lock| lock.upgrade().is_some());
    }
    if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(Mutex::new(()));
    locks.insert(key.to_string(), Arc::downgrade(&lock));
    lock
}

pub(super) async fn release_premerged_index_lock(key: &str, lock: &Arc<Mutex<()>>) {
    let mut locks = premerged_index_locks().lock().await;
    let lock_weak = Arc::downgrade(lock);
    let should_remove = match locks.get(key) {
        Some(current) if current.ptr_eq(&lock_weak) => Arc::strong_count(lock) == 1,
        Some(current) => current.upgrade().is_none(),
        None => false,
    };
    if should_remove {
        locks.remove(key);
    }
}

pub(super) fn build_premerged_artifact_header(
    key: &PremergedIndexCacheKey,
    mapping_count: u64,
    body_len: u64,
    body_checksum: [u8; 32],
) -> Vec<u8> {
    let mut header = Vec::with_capacity(PREMERGED_INDEX_HEADER_LEN);
    header.extend_from_slice(&PREMERGED_INDEX_MAGIC);
    append_u32(&mut header, PREMERGED_INDEX_FORMAT_VERSION);
    append_u32(&mut header, PREMERGED_INDEX_MERGE_VERSION);
    append_u32(&mut header, PREMERGED_INDEX_HEADER_LEN as u32);
    append_u32(&mut header, key.layer_count as u32);
    append_u64(&mut header, mapping_count);
    append_u64(&mut header, body_len);
    header.extend_from_slice(&key.digest);
    header.extend_from_slice(&body_checksum);
    assert_eq!(
        header.len(),
        PREMERGED_INDEX_HEADER_LEN,
        "premerged artifact header serialization length changed"
    );
    header
}

fn parse_premerged_artifact_header(bytes: &[u8]) -> Result<PremergedIndexArtifactHeader> {
    ensure!(
        bytes.len() >= PREMERGED_INDEX_HEADER_LEN,
        "premerged artifact header is truncated"
    );
    ensure!(
        bytes[..8] == PREMERGED_INDEX_MAGIC,
        "premerged artifact magic mismatch"
    );

    let format_version = read_u32(bytes, 8)?;
    let merge_version = read_u32(bytes, 12)?;
    let header_len = read_u32(bytes, 16)?;
    ensure!(
        header_len as usize == PREMERGED_INDEX_HEADER_LEN,
        "premerged artifact header length mismatch"
    );

    Ok(PremergedIndexArtifactHeader {
        format_version,
        merge_version,
        layer_count: read_u32(bytes, 20)?,
        mapping_count: read_u64(bytes, 24)?,
        body_len: read_u64(bytes, 32)?,
        key_digest: copy_array(bytes, 40)?,
        body_checksum: copy_array(bytes, 72)?,
    })
}

pub(super) fn serialize_premerged_mappings(mappings: &[SegmentMapping]) -> Vec<u8> {
    let mut body = Vec::with_capacity(mappings.len() * size_of::<DiskSegmentMapping>());
    for mapping in mappings {
        let disk = DiskSegmentMapping::from_memory(mapping);
        body.extend_from_slice(disk.as_bytes());
    }
    body
}

pub(super) fn deserialize_premerged_mappings(
    body: &[u8],
    mapping_count: usize,
    layer_count: usize,
) -> Result<Vec<SegmentMapping>> {
    let stride = size_of::<DiskSegmentMapping>();
    let expected_body_len = mapping_count
        .checked_mul(stride)
        .context("premerged artifact body length overflow")?;
    ensure!(
        body.len() == expected_body_len,
        "premerged artifact body length mismatch"
    );

    let mut mappings = Vec::with_capacity(mapping_count);
    let mut previous_end = None;
    for chunk in body.chunks_exact(stride) {
        let disk = DiskSegmentMapping::read_from_bytes(chunk)
            .map_err(|_| anyhow!("invalid DiskSegmentMapping in premerged artifact"))?;
        let mapping = disk.to_memory();
        ensure!(
            mapping.length() > 0,
            "premerged artifact contains empty mapping"
        );
        ensure!(
            usize::from(mapping.tag) < layer_count,
            "premerged artifact layer tag out of range"
        );
        if let Some(end) = previous_end {
            ensure!(
                mapping.offset() >= end,
                "premerged artifact mappings are not sorted or overlap"
            );
        }
        previous_end = Some(mapping.end());
        mappings.push(mapping);
    }
    Ok(mappings)
}

pub(super) fn decode_premerged_index_artifact(
    bytes: &[u8],
    key: &PremergedIndexCacheKey,
) -> Result<Arc<ReadOnlyIndex>> {
    let header = parse_premerged_artifact_header(bytes)?;

    ensure!(
        header.format_version == PREMERGED_INDEX_FORMAT_VERSION,
        "premerged artifact format version mismatch"
    );
    ensure!(
        header.merge_version == PREMERGED_INDEX_MERGE_VERSION,
        "premerged artifact merge version mismatch"
    );
    ensure!(
        header.layer_count as usize == key.layer_count,
        "premerged artifact layer count mismatch"
    );
    ensure!(
        header.key_digest == key.digest,
        "premerged artifact key digest mismatch"
    );

    let body_len = usize::try_from(header.body_len)
        .context("premerged artifact body length does not fit usize")?;
    let mapping_count = usize::try_from(header.mapping_count)
        .context("premerged artifact mapping count does not fit usize")?;
    let stride = size_of::<DiskSegmentMapping>();
    let expected_body_len = mapping_count
        .checked_mul(stride)
        .context("premerged artifact body length overflow")?;
    ensure!(
        body_len == expected_body_len,
        "premerged artifact mapping count/body length mismatch"
    );
    ensure!(
        bytes.len() == PREMERGED_INDEX_HEADER_LEN + body_len,
        "premerged artifact file length mismatch"
    );

    let body = &bytes[PREMERGED_INDEX_HEADER_LEN..];
    let checksum = Sha256::digest(body);
    ensure!(
        checksum.as_slice() == header.body_checksum,
        "premerged artifact body checksum mismatch"
    );

    Ok(Arc::new(ReadOnlyIndex::new(
        deserialize_premerged_mappings(body, mapping_count, key.layer_count)?,
    )))
}

async fn prune_premerged_index_dir(dir: &Path, max_dir_bytes: u64) -> Result<()> {
    let mut entries = Vec::new();
    let mut total = 0u64;
    let mut reader = tokio::fs::read_dir(dir).await?;

    while let Some(entry) = reader.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|v| v.to_str()) != Some(PREMERGED_INDEX_EXT) {
            continue;
        }
        let metadata = entry.metadata().await?;
        if !metadata.is_file() {
            continue;
        }
        let len = metadata.len();
        let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
        total = total.saturating_add(len);
        entries.push((path, len, modified));
    }

    if total <= max_dir_bytes {
        return Ok(());
    }

    entries.sort_by_key(|(_, _, modified): &(PathBuf, u64, SystemTime)| *modified);
    for (path, len, _) in entries {
        if total <= max_dir_bytes {
            break;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => total = total.saturating_sub(len),
            Err(err) => {
                tracing::warn!(?err, path = %path.display(), "remove premerged index artifact failed")
            }
        }
    }
    Ok(())
}

async fn write_premerged_index_artifact(
    cache_dir: &Path,
    key: &PremergedIndexCacheKey,
    index: &ReadOnlyIndex,
) -> Result<()> {
    let dir = cache_dir.join(PREMERGED_INDEX_DIR);
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("create premerged index cache dir {}", dir.display()))?;

    let body = serialize_premerged_mappings(index.mappings());
    let checksum = Sha256::digest(&body);
    let mut checksum_bytes = [0u8; 32];
    checksum_bytes.copy_from_slice(&checksum);
    let mut artifact = build_premerged_artifact_header(
        key,
        index.mappings().len() as u64,
        body.len() as u64,
        checksum_bytes,
    );
    artifact.extend_from_slice(&body);

    let path = key.artifact_path(cache_dir);
    let tmp = dir.join(format!(".{}.{}.tmp", key.digest_hex, Uuid::new_v4()));
    if let Err(err) = tokio::fs::write(&tmp, &artifact).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(err).with_context(|| format!("write {}", tmp.display()));
    }
    if let Err(err) = tokio::fs::rename(&tmp, &path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(err).with_context(|| {
            format!(
                "rename premerged index artifact {} to {}",
                tmp.display(),
                path.display()
            )
        });
    }

    Ok(())
}

pub(super) async fn try_read_premerged_index_artifact(
    cache_dir: &Path,
    key: &PremergedIndexCacheKey,
) -> Option<Arc<ReadOnlyIndex>> {
    let artifact_path = key.artifact_path(cache_dir);
    match tokio::fs::read(&artifact_path).await {
        Ok(bytes) => match decode_premerged_index_artifact(&bytes, key) {
            Ok(merged) => Some(merged),
            Err(err) => {
                tracing::warn!(
                    ?err,
                    path = %artifact_path.display(),
                    "ignore invalid premerged index artifact"
                );
                None
            }
        },
        Err(err) if err.kind() == ErrorKind::NotFound => None,
        Err(err) => {
            tracing::warn!(
                ?err,
                path = %artifact_path.display(),
                "read premerged index artifact failed"
            );
            None
        }
    }
}

pub(super) fn spawn_premerged_index_artifact_write(
    cache_dir: PathBuf,
    key: PremergedIndexCacheKey,
    merged: Arc<ReadOnlyIndex>,
    max_dir_bytes: u64,
    lock: Arc<Mutex<()>>,
    guard: OwnedMutexGuard<()>,
) {
    tokio::spawn(async move {
        let write_result = write_premerged_index_artifact(&cache_dir, &key, merged.as_ref()).await;
        if let Err(err) = &write_result {
            tracing::warn!(
                ?err,
                digest = %key.digest_hex,
                "write premerged index artifact failed"
            );
        }
        let key_digest = key.digest_hex.clone();
        drop(guard);
        release_premerged_index_lock(&key_digest, &lock).await;
        if write_result.is_ok() {
            let dir = cache_dir.join(PREMERGED_INDEX_DIR);
            if let Err(err) = prune_premerged_index_dir(&dir, max_dir_bytes).await {
                tracing::warn!(
                    ?err,
                    path = %dir.display(),
                    "prune premerged index cache dir failed"
                );
            }
        }
    });
}

pub(super) async fn update_header_vsize(file: &Arc<dyn VirtualFile>, vsize: u64) -> Result<()> {
    let mut raw = vec![0u8; HEADER_SIZE as usize];
    let read = file.read_at_into(0, &mut raw).await?;
    ensure!(
        read >= size_of::<HeaderTrailer>(),
        "failed to read header when updating virtual size"
    );
    let mut ht = HeaderTrailer::read_from_bytes(&raw[..size_of::<HeaderTrailer>()])
        .map_err(|_| anyhow!("invalid header bytes"))?;
    ht.virtual_size = U64::new(vsize);
    let bytes = ht.as_bytes();
    raw[..bytes.len()].copy_from_slice(bytes);
    file.write_at(0, &raw).await?;
    Ok(())
}

pub(super) async fn write_header_block(
    file: &Arc<dyn VirtualFile>,
    header: &HeaderTrailer,
) -> Result<()> {
    let mut header_buf = AlignedBuffer::new(HEADER_SIZE as usize, ALIGNMENT_USIZE)?;
    copy_header_block(header_buf.as_mut(), header);
    file.write_at(0, header_buf.as_ref()).await?;
    Ok(())
}

fn open_truncated_rw_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(path)
        .with_context(|| format!("create overlaybd upper file {}", path.display()))
}

fn write_all_at(file: &File, mut offset: u64, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match file.write_at(buf, offset) {
            Ok(0) => return Err(ErrorKind::WriteZero.into()),
            Ok(n) => {
                offset += n as u64;
                buf = &buf[n..];
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn write_header_block_sync(file: &File, header: &HeaderTrailer) -> Result<()> {
    let mut header_buf = vec![0u8; HEADER_SIZE as usize];
    copy_header_block(&mut header_buf, header);
    write_all_at(file, 0, &header_buf).context("write overlaybd upper header")
}

pub(crate) fn initialize_file_rw_paths(
    data_path: &Path,
    index_path: Option<&Path>,
    virtual_size: u64,
    rw_layout: RwLayout,
) -> Result<()> {
    let uuid = Uuid::new_v4();
    let header = rw_header(virtual_size, rw_layout, uuid, None, None);
    let data_file = open_truncated_rw_file(data_path)?;
    write_header_block_sync(&data_file, &header)
        .with_context(|| format!("initialize overlaybd upper data {}", data_path.display()))?;

    match rw_layout {
        RwLayout::Sparse => {
            let sparse_size = virtual_size
                .checked_add(HEADER_SIZE)
                .context("sparse overlaybd upper size overflow")?;
            data_file.set_len(sparse_size).with_context(|| {
                format!("truncate sparse overlaybd upper {}", data_path.display())
            })?;
            Ok(())
        }
        RwLayout::LogStructured | RwLayout::HybridLogStructured => {
            let index_path = index_path.context(
                "log-structured overlaybd upper requires an index path during initialization",
            )?;
            let index_file = open_truncated_rw_file(index_path)?;
            let mut index_header = header;
            index_header.set_index_file();
            index_header.set_unsealed_index_offset();
            write_header_block_sync(&index_file, &index_header).with_context(|| {
                format!("initialize overlaybd upper index {}", index_path.display())
            })?;
            Ok(())
        }
    }
}

/// Scan the `file` via seek_data and seek_hole, and creating the [SegmentMapping]s
/// based on its filled ranges.
///
/// - `start_offset`: The begin offset in the file, to start to scan. It will also effect
///   the logic offset in the SegmentMapping. For example, if start_offset is 4K, then the
///   logic offset in the returned SegmentMapping will minus 4K from physical offset from the
///   file.
pub async fn create_mappings_from_sparse(
    file: &Arc<dyn VirtualFile>,
    start_offset: u64,
) -> Result<Vec<SegmentMapping>> {
    ensure!(
        start_offset.is_multiple_of(ALIGNMENT),
        "start_offset {start_offset} of create_mappings_from_sparse must be aligned to {ALIGNMENT} bytes"
    );
    let file_size = file.size().await?;
    if file_size <= start_offset {
        return Ok(Vec::new());
    }

    let mut mappings = Vec::new();
    let mut cursor = start_offset;
    loop {
        let begin = match file.seek_data(cursor).await? {
            Some(v) => v,
            None => break,
        };
        if begin >= file_size {
            break;
        }
        let end = match file.seek_hole(begin).await? {
            Some(v) => v.min(file_size),
            None => file_size,
        };
        if end <= begin {
            break;
        }

        if begin < start_offset {
            cursor = end.max(start_offset);
            continue;
        }

        let mut logical = (begin - start_offset) / ALIGNMENT;
        let mut physical = begin / ALIGNMENT;
        let mut blocks = (end - begin) / ALIGNMENT;
        while blocks > 0 {
            let step = blocks.min(Segment::MAX_LENGTH as u64);
            mappings.push(SegmentMapping::new(
                logical,
                step as u32,
                physical,
                false,
                0,
            ));
            logical += step;
            physical += step;
            blocks -= step;
        }
        cursor = end;
    }
    Ok(mappings)
}

pub(super) fn check_alignment(offset: u64, len: usize) -> Result<()> {
    ensure!(
        offset.is_multiple_of(ALIGNMENT) && len.is_multiple_of(ALIGNMENT_USIZE),
        "IO must be aligned to {} bytes (offset: {}, len: {})",
        ALIGNMENT,
        offset,
        len
    );
    Ok(())
}

pub(super) async fn verify_ht(
    file: &Arc<dyn VirtualFile>,
    is_trailer: bool,
    file_size: u64,
) -> Result<HeaderTrailer> {
    if !is_trailer {
        let mut bytes = vec![0u8; HEADER_SIZE as usize];
        let read = file.read_at_into(0, &mut bytes).await?;
        ensure!(
            read >= size_of::<HeaderTrailer>(),
            "failed to read file header."
        );
        let ht = HeaderTrailer::read_from_bytes(&bytes[..size_of::<HeaderTrailer>()])
            .map_err(|_| anyhow!("Invalid header"))?;
        ensure!(
            ht.verify_magic() && ht.is_header(),
            "header magic/type don't match"
        );
        Ok(ht)
    } else {
        ensure!(file_size >= HEADER_SIZE, "File too small for trailer");
        let trailer_offset = file_size - HEADER_SIZE;
        let mut bytes = vec![0u8; HEADER_SIZE as usize];
        let read = file.read_at_into(trailer_offset, &mut bytes).await?;
        ensure!(
            read >= size_of::<HeaderTrailer>(),
            "failed to read file trailer."
        );
        let ht = HeaderTrailer::read_from_bytes(&bytes[..size_of::<HeaderTrailer>()])
            .map_err(|_| anyhow!("Invalid trailer"))?;
        ensure!(
            ht.verify_magic() && ht.is_trailer() && ht.is_data_file() && ht.is_sealed(),
            "trailer magic, trailer type, file type or sealedness doesn't match"
        );
        Ok(ht)
    }
}

async fn load_readonly_layer_metadata(file: Arc<dyn VirtualFile>) -> Result<ReadOnlyLayerMetadata> {
    let file_size = file.size().await?;
    let header = verify_ht(&file, false, file_size).await?;
    let trailer = verify_ht(&file, true, file_size).await?;
    Ok(ReadOnlyLayerMetadata {
        uuid: parse_uuid_field(&trailer.uuid).unwrap_or_else(Uuid::nil),
        file_size,
        virtual_size: trailer.virtual_size.get(),
        index_offset: trailer.index_offset.get(),
        index_size: trailer.index_size.get(),
        header_version: header.version,
        header_sub_version: header.sub_version,
        trailer_version: trailer.version,
        trailer_sub_version: trailer.sub_version,
    })
}

pub(super) async fn load_readonly_layers_metadata(
    files: &[Arc<dyn VirtualFile>],
) -> Result<Vec<ReadOnlyLayerMetadata>> {
    let s = stream::iter(files.iter().cloned().enumerate())
        .map(|(layer_index, file)| async move {
            (layer_index, load_readonly_layer_metadata(file).await)
        })
        .buffer_unordered(files.len().min(PARALLEL_LOAD_INDEX))
        .collect::<Vec<_>>();
    let results = storage_util::AlwaysSend::new(s).await;

    let mut ordered: Vec<Option<ReadOnlyLayerMetadata>> =
        std::iter::repeat_with(|| None).take(files.len()).collect();
    for (layer_index, result) in results {
        ordered[layer_index] = Some(result.context(format!(
            "failed to read readonly layer {layer_index} metadata"
        ))?);
    }

    ordered
        .into_iter()
        .enumerate()
        .map(|(layer_index, entry)| {
            entry.ok_or_else(|| {
                anyhow!("parallel readonly metadata read returned missing layer slot {layer_index}")
            })
        })
        .collect()
}

async fn load_index(
    file: &Arc<dyn VirtualFile>,
    offset: u64,
    count: usize,
    reset_tag: bool,
) -> Result<Vec<SegmentMapping>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let size_bytes = count * size_of::<DiskSegmentMapping>();
    let mut raw_bytes = vec![0u8; size_bytes];
    let read = file.read_at_into(offset, &mut raw_bytes).await?;
    ensure!(read >= size_bytes, "Index file too short");

    let mut mappings = Vec::with_capacity(count);
    let stride = size_of::<DiskSegmentMapping>();

    for i in 0..count {
        let start = i * stride;
        let end = start + stride;
        let dm = DiskSegmentMapping::read_from_bytes(&raw_bytes[start..end])
            .map_err(|_| anyhow!("Invalid DiskSegmentMapping"))?;

        let mut m = dm.to_memory();
        if m.length() == 0 || m.offset() == u64::MAX {
            continue;
        }
        if reset_tag {
            m.tag = 0;
        }
        mappings.push(m);
    }

    Ok(mappings)
}

pub(super) async fn load_index_and_reset_tags(
    file: &Arc<dyn VirtualFile>,
    offset: u64,
    count: usize,
) -> Result<Vec<SegmentMapping>> {
    load_index(file, offset, count, true).await
}

fn invalid_disk_mapping() -> DiskSegmentMapping {
    DiskSegmentMapping::from_memory(&SegmentMapping::new(INVALID_SEGMENT_OFFSET, 0, 0, false, 0))
}

pub(super) fn append_invalid_index_padding(index_bytes: &mut Vec<u8>, padding_count: usize) {
    if padding_count == 0 {
        return;
    }

    let invalid = invalid_disk_mapping();
    for _ in 0..padding_count {
        index_bytes.extend_from_slice(invalid.as_bytes());
    }
}

fn compact_is_zero_block(_buf: &[u8]) -> bool {
    // Keep parity with upstream OverlayBD: file.cpp currently short-circuits
    // `is_zero_block()` to treat compacted data blocks as non-zero.
    COMPACT_ZERO_DETECTION_ENABLED
}

fn push_compact_segment(
    chunk: &[u8],
    data: &mut Vec<u8>,
    prev_end_blocks: &mut usize,
    zero_detected: bool,
    segment: &mut SegmentMapping,
    index: &mut Vec<SegmentMapping>,
) {
    if zero_detected {
        segment.zeroed = true;
    } else {
        let begin = *prev_end_blocks * ALIGNMENT_USIZE;
        let len = segment.length() as usize * ALIGNMENT_USIZE;
        data.extend_from_slice(&chunk[begin..begin + len]);
    }

    *prev_end_blocks += segment.length() as usize;
    index.push(*segment);

    let next_moffset = segment.mend();
    let next_offset = segment.end();
    segment.zeroed = false;
    segment.segment.offset = next_offset;
    segment.segment.length = 0;
    segment.moffset = next_moffset;
}

// ---------------------------------------------------------------------------
// Chunk-based compact: pack multiple SegmentMappings into buffer-sized chunks
// ---------------------------------------------------------------------------

/// One read operation within a [`CompactChunk`].
#[derive(Clone)]
struct CompactChunkEntry {
    /// Source layer index (from `SegmentMapping::tag`).
    tag: u8,
    /// Physical byte offset in the source layer file.
    src_offset: u64,
    /// Number of bytes to read from this source range.
    len: usize,
    /// Logical block offset in the virtual file (for output index entries).
    logical_offset: u64,
}

/// A group of source reads whose data collectively fills one
/// [`CompactWriter`] buffer. Produced by the chunking phase in
/// [`compact_to`] and consumed by [`compact_read_chunk`].
#[derive(Clone)]
struct CompactChunk {
    /// Ordered read operations. Each fills a portion of the buffer.
    entries: Vec<CompactChunkEntry>,
    /// Destination physical offset **in blocks** where this chunk starts.
    dest_moffset: u64,
    /// Total data bytes in this chunk (sum of entry lengths).
    /// Always `<= writer.buffer_size()`.
    total_len: usize,
    /// Insertion order so parallel results can be merged correctly.
    order: usize,
}

struct PreparedCompactChunk {
    order: usize,
    destination_offset: u64,
    len: usize,
    buffer: Box<dyn storage_util::CompactBuffer>,
    index: Vec<SegmentMapping>,
}

/// Read all entries in `chunk` into a single writer-provided buffer.
async fn compact_read_chunk(
    src_layers: &[Arc<dyn VirtualFile>],
    writer: &dyn CompactWriter,
    chunk: CompactChunk,
) -> Result<PreparedCompactChunk> {
    let mut buffer = writer.alloc_buffer().await?;
    let buffer_slice = buffer.as_mut().as_mut();
    let mut buffer_offset = 0usize;
    let mut index = Vec::with_capacity(chunk.entries.len());

    for entry in &chunk.entries {
        let layer_idx = entry.tag as usize;
        ensure!(layer_idx < src_layers.len(), "Invalid layer tag");

        let got = src_layers[layer_idx]
            .read_at_into(
                entry.src_offset,
                &mut buffer_slice[buffer_offset..buffer_offset + entry.len],
            )
            .await?;
        ensure!(
            got == entry.len,
            "failed to read {} bytes from source layer {} at offset {}, got {}",
            entry.len,
            layer_idx,
            entry.src_offset,
            got
        );

        let block_len = (entry.len / ALIGNMENT_USIZE) as u32;
        let dest_phys = chunk.dest_moffset + (buffer_offset / ALIGNMENT_USIZE) as u64;
        index.push(SegmentMapping::new(
            entry.logical_offset,
            block_len,
            dest_phys,
            false,
            entry.tag,
        ));

        buffer_offset += entry.len;
    }

    Ok(PreparedCompactChunk {
        order: chunk.order,
        destination_offset: chunk.dest_moffset * ALIGNMENT,
        len: chunk.total_len,
        buffer,
        index,
    })
}

async fn compact_write_chunk(
    writer: &dyn CompactWriter,
    prepared: PreparedCompactChunk,
) -> Result<(usize, Vec<SegmentMapping>)> {
    writer
        .write(prepared.buffer, prepared.destination_offset, prepared.len)
        .await?;
    Ok((prepared.order, prepared.index))
}

async fn compact_copy_chunk(
    src_layers: &[Arc<dyn VirtualFile>],
    writer: &dyn CompactWriter,
    chunk: CompactChunk,
) -> Result<(usize, Vec<SegmentMapping>)> {
    let prepared = compact_read_chunk(src_layers, writer, chunk).await?;
    compact_write_chunk(writer, prepared).await
}

// ---------------------------------------------------------------------------
// Legacy per-mapping copy — used only when COMPACT_ZERO_DETECTION_ENABLED
// ---------------------------------------------------------------------------

/// Copy data blocks for a single mapping, with optional zero-block detection.
///
/// Kept as a fallback for the zero-detection path which may change the output
/// size per mapping and therefore cannot be pre-chunked.
async fn compact_copy_mapping_with_zero_detection(
    src_layers: &[Arc<dyn VirtualFile>],
    writer: &dyn CompactWriter,
    mapping: SegmentMapping,
    moffset: u64,
) -> Result<(u64, Vec<SegmentMapping>)> {
    let layer_idx = mapping.tag as usize;
    ensure!(layer_idx < src_layers.len(), "Invalid layer tag");

    let mut src_offset = mapping.moffset * ALIGNMENT;
    let mut remaining = mapping.length() as u64 * ALIGNMENT;
    let mut dest_offset = moffset * ALIGNMENT;
    let mut written_bytes = 0u64;
    let buf_size = writer.buffer_size();
    ensure!(
        buf_size.is_multiple_of(ALIGNMENT_USIZE),
        "CompactWriter buffer size {buf_size} not aligned"
    );

    let mut index = Vec::new();
    let mut segment = SegmentMapping::new(mapping.offset(), 0, moffset, false, mapping.tag);
    let mut data = Vec::with_capacity(buf_size);

    while remaining > 0 {
        let step = min(remaining as usize, buf_size);
        let chunk = src_layers[layer_idx].read_at(src_offset, step).await?;
        ensure!(
            chunk.len() == step,
            "failed to read {} bytes from source layer {} at offset {}, got {}",
            step,
            layer_idx,
            src_offset,
            chunk.len()
        );

        data.clear();
        let mut prev_end_blocks = 0usize;
        let mut zero_detected = false;
        let mut have_detection = false;

        for block_begin in (0..step).step_by(ALIGNMENT_USIZE) {
            let block_end = block_begin + ALIGNMENT_USIZE;
            let is_zero = compact_is_zero_block(&chunk[block_begin..block_end]);
            if is_zero {
                if have_detection && !zero_detected && segment.length() > 0 {
                    push_compact_segment(
                        &chunk,
                        &mut data,
                        &mut prev_end_blocks,
                        false,
                        &mut segment,
                        &mut index,
                    );
                }
                segment.segment.length += 1;
                zero_detected = true;
                have_detection = true;
                continue;
            }

            if have_detection && zero_detected {
                push_compact_segment(
                    &chunk,
                    &mut data,
                    &mut prev_end_blocks,
                    true,
                    &mut segment,
                    &mut index,
                );
            }
            segment.segment.length += 1;
            zero_detected = false;
            have_detection = true;
        }

        if segment.length() > 0 {
            push_compact_segment(
                &chunk,
                &mut data,
                &mut prev_end_blocks,
                have_detection && zero_detected,
                &mut segment,
                &mut index,
            );
        }

        if !data.is_empty() {
            writer.write_all_at(&data, dest_offset).await?;
            dest_offset += data.len() as u64;
            written_bytes += data.len() as u64;
        }

        src_offset += step as u64;
        remaining -= step as u64;
    }

    Ok((written_bytes / ALIGNMENT, index))
}

// ---------------------------------------------------------------------------
// compact_to — main entry point
// ---------------------------------------------------------------------------

/// Compact a set of LSMT source layers into a single sealed overlaybd data file.
///
/// Reads every [`SegmentMapping`] in `mappings` from `src_layers` (resolving
/// the LSMT stack top-down) and writes the compacted data plus a fresh
/// [`HeaderTrailer`] to the destination specified in `commit_args`.
///
/// # Parameters
///
/// - `src_layers`: Ordered slice of source layer files, the order should be:
///   [top, top - 1, ..., bottom, upper]
/// - `mappings`: Pre-computed logical-to-physical segment mappings that cover
///   the region to be compacted. Typically produced by an [`Index`] lookup.
/// - `virtual_size`: The virtual disk size (in bytes) to record in the output
///   header.
/// - `commit_args`: Owns the destination writer ([`CommitArgs::writer`]) as well
///   as optional metadata (UUID, parent UUID, user tag) and the I/O concurrency
///   level. Ownership is taken so the writer can be driven to completion without
///   requiring an extra `Arc` clone at the call site. If you need to inspect or
///   reuse the writer after the call, clone the inner `Arc<dyn CompactWriter>`
///   before wrapping it in [`CommitArgs`].
///
/// # Returns
///
/// `Ok(())` once the complete logical format, including the trailer, has been
/// written and the destination writer has been finalized. Finalization does not
/// imply that the output has been synced to stable storage. Returns an error on
/// any read, write, or finalization failure.
pub async fn compact_to(
    src_layers: &[Arc<dyn VirtualFile>],
    mappings: &[SegmentMapping],
    virtual_size: u64,
    commit_args: CommitArgs,
) -> Result<()> {
    let mut header = HeaderTrailer::new();
    header.size = U32::new(size_of::<HeaderTrailer>() as u32);
    header.virtual_size = U64::new(virtual_size);
    header.set_header();
    header.set_data_file();
    header.set_sealed();
    // Keep parity with OverlayBD C++: when commit UUID is not provided, use nil UUID.
    let commit_uuid = commit_args.uuid.unwrap_or_else(Uuid::nil);
    header.set_uuid(&commit_uuid);
    fill_uuid_field(&mut header.parent_uuid, commit_args.parent_uuid);
    fill_user_tag(&mut header.user_tag, commit_args.user_tag.as_deref());

    let mut header_buf = vec![0u8; HEADER_SIZE as usize];
    let header_bytes = header.as_bytes();
    header_buf[..header_bytes.len()].copy_from_slice(header_bytes);

    let writer = commit_args.writer;
    let concurrency = commit_args.concurrency.max(1);

    writer.write_all_at(&header_buf, 0).await?;

    let mut compact_index: Vec<SegmentMapping> = Vec::with_capacity(mappings.len());
    let mut dest_moffset = HEADER_SIZE / ALIGNMENT;

    if COMPACT_ZERO_DETECTION_ENABLED {
        // Legacy per-mapping path. Zero detection may change the output size
        // per mapping, so we cannot pre-chunk or pre-compute offsets.
        for m in mappings {
            if m.zeroed {
                let mut zero = *m;
                zero.moffset = dest_moffset;
                compact_index.push(zero);
                continue;
            }
            let (written_blocks, entries) = compact_copy_mapping_with_zero_detection(
                src_layers,
                writer.as_ref(),
                *m,
                dest_moffset,
            )
            .await?;
            compact_index.extend(entries);
            dest_moffset += written_blocks;
        }
    } else {
        // Phase 1: Pack non-zeroed mappings into buffer-sized chunks.
        //
        // Each chunk aggregates data from one or more mappings (possibly from
        // different source layers) up to `writer.buffer_size()` bytes. Large
        // mappings that exceed the buffer size are split across chunks.
        //
        // Zeroed mappings don't consume destination space — they are recorded
        // directly in `compact_index` and skipped during chunking.
        let buf_size = writer.buffer_size();
        ensure!(
            buf_size >= ALIGNMENT_USIZE && buf_size.is_multiple_of(ALIGNMENT_USIZE),
            "CompactWriter buffer size {buf_size} not aligned"
        );
        let mut chunks: Vec<CompactChunk> = Vec::new();
        let mut current_chunk: Option<CompactChunk> = None;
        let mut chunk_order = 0usize;

        for m in mappings {
            if m.zeroed {
                let mut zero = *m;
                zero.moffset = dest_moffset;
                compact_index.push(zero);
                // NOTE: no need to advance dest_moffset, as this is a zero segement,
                // does not occupy space in dset file.
                continue;
            }

            let mut remaining = m.length() as usize * ALIGNMENT_USIZE;
            let mut src_off = m.moffset * ALIGNMENT;
            let mut logical_off = m.offset();

            while remaining > 0 {
                let chunk = current_chunk.get_or_insert_with(|| {
                    let c = CompactChunk {
                        entries: Vec::new(),
                        dest_moffset,
                        total_len: 0,
                        order: chunk_order,
                    };
                    chunk_order += 1;
                    c
                });

                let space = buf_size - chunk.total_len;
                let take = remaining
                    .min(space)
                    .min(Segment::MAX_LENGTH as usize * ALIGNMENT_USIZE);

                chunk.entries.push(CompactChunkEntry {
                    tag: m.tag,
                    src_offset: src_off,
                    len: take,
                    logical_offset: logical_off,
                });
                chunk.total_len += take;

                let blocks_taken = (take / ALIGNMENT_USIZE) as u64;
                logical_off += blocks_taken;
                src_off += take as u64;
                remaining -= take;
                dest_moffset += blocks_taken;

                if chunk.total_len >= buf_size {
                    chunks.push(current_chunk.take().unwrap());
                }
            }
        }
        // Flush remaining partial chunk.
        if let Some(chunk) = current_chunk.take() {
            chunks.push(chunk);
        }

        // Phase 2: Copy data blocks, potentially in parallel.
        if concurrency == 1 {
            for chunk in chunks {
                let (_, entries) = compact_copy_chunk(src_layers, writer.as_ref(), chunk).await?;
                compact_index.extend(entries);
            }
        } else if writer.requires_ordered_writes() {
            let src_layers: Arc<[Arc<dyn VirtualFile>]> = Arc::from(src_layers.to_vec());
            let mut prepared = stream::iter(chunks)
                .map(|chunk| {
                    let layers = src_layers.clone();
                    let writer = writer.clone();
                    async move { compact_read_chunk(&layers, writer.as_ref(), chunk).await }
                })
                .buffered(concurrency);

            while let Some(chunk) = prepared.next().await {
                let (_, entries) = compact_write_chunk(writer.as_ref(), chunk?).await?;
                compact_index.extend(entries);
            }
        } else {
            // `buffer_unordered` already caps in-flight chunk copies at
            // `concurrency`, so no extra semaphore is needed here.
            let src_layers: Arc<[Arc<dyn VirtualFile>]> = Arc::from(src_layers.to_vec());
            let mut results: Vec<(usize, Vec<SegmentMapping>)> = stream::iter(chunks)
                .map(|chunk| {
                    let writer = writer.clone();
                    let layers = src_layers.clone();
                    async move { compact_copy_chunk(&layers, writer.as_ref(), chunk).await }
                })
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            results.sort_by_key(|(order, _)| *order);
            for (_, entries) in results {
                compact_index.extend(entries);
            }
        }
    }

    // Phase 3: Write the index and trailer.
    // Zeroed mappings were pushed in phase 1 while data entries arrive in
    // phase 2; restore logical-offset order before compressing so that
    // partition_point lookups in ReadOnlyIndex see a sorted index.
    compact_index.sort_unstable();
    compress_raw_index(&mut compact_index);

    let index_offset = dest_moffset * ALIGNMENT;
    let mut index_bytes = Vec::with_capacity(compact_index.len() * size_of::<DiskSegmentMapping>());
    for m in &compact_index {
        let dm = DiskSegmentMapping::from_memory(m);
        index_bytes.extend_from_slice(dm.as_bytes());
    }

    let n_per_block = 4096 / size_of::<DiskSegmentMapping>();
    let remainder = compact_index.len() % n_per_block;
    let padding_count = if remainder > 0 {
        n_per_block - remainder
    } else {
        0
    };

    append_invalid_index_padding(&mut index_bytes, padding_count);

    writer.write_all_at(&index_bytes, index_offset).await?;

    let index_size = compact_index.len() as u64 + padding_count as u64;
    let mut trailer_offset = index_offset + index_bytes.len() as u64;

    if !trailer_offset.is_multiple_of(4096) {
        let pad_len = 4096 - (trailer_offset % 4096);
        let pad = vec![0u8; pad_len as usize];
        writer.write_all_at(&pad, trailer_offset).await?;
        trailer_offset += pad_len;
    }

    let mut trailer = header;
    trailer.set_trailer();
    trailer.index_offset = U64::new(index_offset);
    trailer.index_size = U64::new(index_size);

    let mut trailer_buf = vec![0u8; 4096];
    let trailer_bytes_enc = trailer.as_bytes();
    trailer_buf[..trailer_bytes_enc.len()].copy_from_slice(trailer_bytes_enc);
    writer.write_all_at(&trailer_buf, trailer_offset).await?;
    writer.finalize().await?;

    Ok(())
}
