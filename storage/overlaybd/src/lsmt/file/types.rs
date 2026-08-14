use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::mem::size_of;
use std::sync::Arc;
use uuid::Uuid;

use crate::io::virtual_file::{VirtualFile, VirtualFileWriter};
use storage_util::CompactWriter;

pub(super) const ALIGNMENT: u64 = 512;
pub(super) const ALIGNMENT_USIZE: usize = 512;
pub(super) const HEADER_SIZE: u64 = 4096;
pub(super) const MAX_IO_SIZE: usize = 4 * 1024 * 1024; // 4MB
pub(super) const ALIGNMENT_4K: usize = 4096;
pub(super) const INVALID_SEGMENT_OFFSET: u64 = (1 << 50) - 1;
pub(super) const COMPACT_ZERO_DETECTION_ENABLED: bool = false;
pub(crate) const PARALLEL_LOAD_INDEX: usize = 32;

pub const MAX_STACK_LAYERS: usize = 255;

pub(crate) const PREMERGED_INDEX_DIR: &str = "premerged-index";
pub(super) const PREMERGED_INDEX_EXT: &str = "pmidx";
pub(super) const PREMERGED_INDEX_MAGIC: [u8; 8] = *b"PMIDX001";
pub(super) const PREMERGED_INDEX_FORMAT_VERSION: u32 = 1;
pub(super) const PREMERGED_INDEX_MERGE_VERSION: u32 = 1;
pub(super) const PREMERGED_INDEX_HEADER_LEN: usize = PREMERGED_INDEX_MAGIC.len()
    + size_of::<u32>()
    + size_of::<u32>()
    + size_of::<u32>()
    + size_of::<u32>()
    + size_of::<u64>()
    + size_of::<u64>()
    + 32
    + 32;
pub(super) const MIN_PREMERGED_INDEX_CACHE_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const MAX_PREMERGED_INDEX_CACHE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct PremergedIndexCachePolicy {
    pub read: bool,
    pub write: bool,
    pub max_dir_bytes: u64,
}

impl PremergedIndexCachePolicy {
    pub fn hybrid(cache_size_gb: u32) -> Self {
        Self {
            read: true,
            write: true,
            max_dir_bytes: premerged_index_cache_limit_bytes(cache_size_gb),
        }
    }

    pub fn disabled() -> Self {
        Self {
            read: false,
            write: false,
            max_dir_bytes: MIN_PREMERGED_INDEX_CACHE_BYTES,
        }
    }
}

fn premerged_index_cache_limit_bytes(cache_size_gb: u32) -> u64 {
    if cache_size_gb == 0 {
        return MIN_PREMERGED_INDEX_CACHE_BYTES;
    }

    let gib = 1024_u64 * 1024 * 1024;
    (u64::from(cache_size_gb) * gib / 16).clamp(
        MIN_PREMERGED_INDEX_CACHE_BYTES,
        MAX_PREMERGED_INDEX_CACHE_BYTES,
    )
}

#[derive(Clone, Debug)]
pub(super) struct ReadOnlyLayerMetadata {
    pub(super) uuid: Uuid,
    pub(super) file_size: u64,
    pub(super) virtual_size: u64,
    pub(super) index_offset: u64,
    pub(super) index_size: u64,
    pub(super) header_version: u8,
    pub(super) header_sub_version: u8,
    pub(super) trailer_version: u8,
    pub(super) trailer_sub_version: u8,
}

#[derive(Clone, Debug)]
pub(super) struct PremergedIndexCacheKey {
    pub(super) digest: [u8; 32],
    pub(super) digest_hex: String,
    pub(super) layer_count: usize,
}

#[derive(Debug)]
pub(super) struct PremergedIndexArtifactHeader {
    pub(super) format_version: u32,
    pub(super) merge_version: u32,
    pub(super) layer_count: u32,
    pub(super) mapping_count: u64,
    pub(super) body_len: u64,
    pub(super) key_digest: [u8; 32],
    pub(super) body_checksum: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LSMTFileType {
    ReadOnly = 0,
    ReadWrite = 1,
    SparseReadWrite = 2,
    HybridReadWrite = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RwLayout {
    LogStructured,
    Sparse,
    HybridLogStructured,
}

impl RwLayout {
    pub(super) fn file_type(self) -> LSMTFileType {
        match self {
            RwLayout::LogStructured => LSMTFileType::ReadWrite,
            RwLayout::Sparse => LSMTFileType::SparseReadWrite,
            RwLayout::HybridLogStructured => LSMTFileType::HybridReadWrite,
        }
    }
}

impl From<crate::config::UpperMode> for RwLayout {
    fn from(mode: crate::config::UpperMode) -> Self {
        match mode {
            crate::config::UpperMode::LogStructured => RwLayout::LogStructured,
            crate::config::UpperMode::Sparse => RwLayout::Sparse,
            crate::config::UpperMode::HybridLogStructured => RwLayout::HybridLogStructured,
        }
    }
}

#[derive(Debug)]
pub(super) enum WriteFragment {
    // Hybrid in-place writes intentionally share Sparse durability semantics:
    // they overwrite the only physical upper copy and can expose a torn block
    // after host crash. Snapshot correctness relies on upper-layer quiesce/sync.
    InPlace {
        data_offset: usize,
        len: usize,
        phys_offset: u64,
    },
    Append {
        logical_offset: u64,
        data_offset: usize,
        len: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayerDescriptor {
    pub digest: String,
    pub size: u64,
}

#[derive(Debug)]
pub(super) struct AppendDigestTracker {
    hasher: Sha256,
    pub(super) offset: u64,
}

impl AppendDigestTracker {
    pub(super) async fn from_header(file: &Arc<dyn VirtualFile>) -> Result<Self> {
        let mut raw = vec![0u8; HEADER_SIZE as usize];
        let read = file.read_at_into(0, &mut raw).await?;
        ensure!(
            read == HEADER_SIZE as usize,
            "short read while initializing append digest tracker: expected {HEADER_SIZE}, got {read}"
        );
        let mut hasher = Sha256::new();
        hasher.update(&raw);
        Ok(Self {
            hasher,
            offset: HEADER_SIZE,
        })
    }

    pub(super) fn absorb(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        ensure!(
            offset == self.offset,
            "append digest tracker saw out-of-order write at {offset}, expected {}",
            self.offset
        );
        self.hasher.update(data);
        self.offset = self
            .offset
            .checked_add(data.len() as u64)
            .context("append digest size overflow")?;
        Ok(())
    }

    pub(super) fn descriptor(&self) -> LayerDescriptor {
        LayerDescriptor {
            digest: format!("sha256:{:x}", self.hasher.clone().finalize()),
            size: self.offset,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DataStat {
    pub total_data_size: u64,
    pub valid_data_size: u64,
}

pub struct CommitArgs {
    pub writer: Arc<dyn CompactWriter>,
    pub user_tag: Option<Vec<u8>>,
    pub uuid: Option<Uuid>,
    pub parent_uuid: Option<Uuid>,
    /// Maximum number of in-flight buffer alloc + copy + write operations.
    /// `1` means sequential (the historical default). Values > 1 enable
    /// parallel data-block copying within [`compact_to`].
    pub concurrency: usize,
}

impl CommitArgs {
    /// Backward-compatible constructor: wraps a [`VirtualFile`] in a
    /// [`VirtualFileWriter`] with sequential (concurrency = 1) writes.
    pub fn new(as_file: Arc<dyn VirtualFile>) -> Self {
        Self {
            writer: Arc::new(VirtualFileWriter::new(as_file)),
            user_tag: None,
            uuid: None,
            parent_uuid: None,
            concurrency: 1,
        }
    }

    /// Construct from an arbitrary [`CompactWriter`] implementation.
    pub fn from_writer(writer: Arc<dyn CompactWriter>) -> Self {
        Self {
            writer,
            user_tag: None,
            uuid: None,
            parent_uuid: None,
            concurrency: 1,
        }
    }
}

#[derive(Clone)]
pub struct LayerInfo {
    pub fdata: Arc<dyn VirtualFile>,
    pub findex: Option<Arc<dyn VirtualFile>>,
    pub virtual_size: u64,
    pub parent_uuid: Option<Uuid>,
    pub uuid: Uuid,
    pub user_tag: Option<Vec<u8>>,
    pub rw_layout: RwLayout,
}

impl LayerInfo {
    pub fn new(
        fdata: Arc<dyn VirtualFile>,
        findex: Option<Arc<dyn VirtualFile>>,
        virtual_size: u64,
    ) -> Self {
        Self {
            fdata,
            findex,
            virtual_size,
            parent_uuid: None,
            uuid: Uuid::new_v4(),
            user_tag: None,
            rw_layout: RwLayout::LogStructured,
        }
    }
}
