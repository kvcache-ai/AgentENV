//! Physical block planner for the startup manifest prefetch: maps an ordered
//! first-touch logical page list onto 256 KiB-aligned blocks of the final
//! (post-publish) memory layer objects.
//!
//! The planner is read-only metadata work: LSMT merged-index lookups, zfile
//! jump-table extent translation, and optional tar base-offset adjustment. It
//! never decompresses or re-reads page payloads and never touches the
//! network — the caller supplies the final local layer bytes.

use crate::backend::local::LocalFile;
use crate::backend::tar::{is_tar_file, new_tar_file};
use crate::compression::zfile::{is_zfile, zfile_open_ro, ZFileRO};
use crate::lsmt::file::{load_readonly_layers_metadata, open_files_ro, ReadOnlyLayerMetadata};
use crate::lsmt::index::{LogIndex, Segment};
use crate::virtual_file::VirtualFile;
use anyhow::{ensure, Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Cache-block granularity of the final object (and of prefetch refills).
pub const OBJECT_BLOCK_BYTES: u64 = 256 << 10;
/// Guest page granularity of the recorded first-touch list.
pub const PLAN_PAGE_BYTES: u64 = 4096;
const SECTOR_BYTES: u64 = 512;
/// Budget model: fixed header size of the planned artifact.
const HEADER_BYTES: u64 = 64;
/// Per-block entry size in the block table (object_index u16 + block_id
/// u32 + reserved u16).
const ENTRY_BYTES: u64 = 8;
/// Object table entry: digest_len u16 + digest bytes + size u64.
const OBJECT_ENTRY_FIXED_BYTES: u64 = 10;
/// Budget model: verify-frame granularity.
const FRAME_BYTES: u64 = 1 << 20;

/// Bytes backing a final layer object for planning.
#[derive(Clone)]
pub enum FinalLayerBytes {
    /// A plain local file path (opened read-only by the planner).
    LocalPath(PathBuf),
    /// An already-open virtual file (for example a cache-only handle on a
    /// remote object): reads go through whatever the file is backed by.
    VFile(Arc<dyn VirtualFile>),
}

impl std::fmt::Debug for FinalLayerBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalPath(path) => f.debug_tuple("LocalPath").field(path).finish(),
            Self::VFile(_) => f.write_str("VFile(..)"),
        }
    }
}

/// A final memory layer object the planner maps into: the exact bytes that
/// were (or will be) uploaded, with their content identity.
#[derive(Clone, Debug)]
pub struct FinalLayerSource {
    pub digest: String,
    pub size: u64,
    pub source: FinalLayerBytes,
}

/// Per-page resolution callback: invoked once per processed page with that
/// page's resolved blocks and their positions in the plan's block list
/// (empty for holes and fall-through). A block that was already selected by
/// an earlier page keeps the position of its first selection, so positions
/// always describe first-need order.
pub type PageObserver<'a> = dyn FnMut(u32, &[(u16, u32, usize)]) + Send + 'a;

/// One planned object block: `block_id` counts in 256 KiB units of the
/// object named by `object`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PlannedBlock {
    pub object: u16,
    pub block_id: u32,
    /// True for layer metadata blocks (header + index region), false for
    /// data blocks.
    pub metadata: bool,
}

/// Object-table entry of the resulting plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedObject {
    pub digest: String,
    pub size: u64,
    /// Index into the input layer list; the layer's local path holds the
    /// exact bytes behind this object.
    pub layer: usize,
}

/// Coverage/amplification accounting of a [`PackPlan`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanStats {
    /// Pages in the first-touch list.
    pub pages_total: usize,
    /// Pages mapped to at least one backed physical range.
    pub pages_backed: usize,
    /// Pages with no backed physical range (holes / unbacked zeros).
    pub pages_zero_hole: usize,
    /// Unique data blocks selected from page mappings.
    pub data_blocks: usize,
    /// Unique metadata blocks (headers + index regions) selected.
    pub metadata_blocks: usize,
    /// data_blocks + metadata_blocks.
    pub unique_blocks: usize,
    /// unique_blocks × 256 KiB (slot payload).
    pub payload_bytes: u64,
    /// Pages whose dependency blocks did not fit the remaining budget; the
    /// plan stops at the first such page.
    pub pages_truncated_by_budget: usize,
    /// Missing-block runs in the complement of the selection (what the
    /// background still has to fetch), summed over contributing objects.
    pub gap_runs: usize,
    /// Total missing blocks in those runs.
    pub gap_blocks: usize,
}

/// The ordered, deduplicated physical block plan.
pub struct PackPlan {
    pub objects: Vec<PlannedObject>,
    /// First-need order, unique per (object, block).
    pub blocks: Vec<PlannedBlock>,
    pub stats: PlanStats,
}

enum LayerLayout {
    /// Raw LSMT bytes (possibly tar-wrapped): logical == physical + base.
    Raw { tar_base: u64 },
    /// ZFile-compressed final object (possibly tar-wrapped): translate each
    /// logical block through the jump table.
    ZFile { zfile: Arc<ZFileRO>, tar_base: u64 },
}

struct OpenedLayer {
    lsmt_file: Arc<dyn VirtualFile>,
    layout: LayerLayout,
    metadata: ReadOnlyLayerMetadata,
}

async fn open_layer(source: &FinalLayerSource) -> Result<OpenedLayer> {
    let local: Arc<dyn VirtualFile> = match &source.source {
        FinalLayerBytes::LocalPath(path) => Arc::new(
            LocalFile::open_ro(path).with_context(|| format!("open layer {}", path.display()))?,
        ),
        FinalLayerBytes::VFile(file) => file.clone(),
    };
    let (unwrapped, tar_base) = if is_tar_file(local.as_ref()).await? {
        let tar = new_tar_file(local).await?;
        let base = tar.base_offset();
        (tar as Arc<dyn VirtualFile>, base)
    } else {
        (local, 0)
    };
    let (lsmt_file, layout) = if is_zfile(unwrapped.clone()).await? == 1 {
        let zfile = Arc::new(zfile_open_ro(unwrapped.clone(), false).await?);
        (
            zfile.clone() as Arc<dyn VirtualFile>,
            LayerLayout::ZFile { zfile, tar_base },
        )
    } else {
        (unwrapped.clone(), LayerLayout::Raw { tar_base })
    };
    let metadata = load_readonly_layers_metadata(std::slice::from_ref(&lsmt_file))
        .await?
        .into_iter()
        .next()
        .context("layer metadata missing")?;
    Ok(OpenedLayer {
        lsmt_file,
        layout,
        metadata,
    })
}

/// Map one physical extent of the *logical* layer file to 256 KiB-aligned
/// block ids of the *final* object, appending to `out`.
fn map_extent_blocks(
    layout: &LayerLayout,
    logical_offset: u64,
    logical_len: u64,
    object_size: u64,
    out: &mut Vec<u32>,
) -> Result<()> {
    if logical_len == 0 {
        return Ok(());
    }
    match layout {
        LayerLayout::Raw { tar_base } => {
            push_aligned_blocks(logical_offset + tar_base, logical_len, object_size, out)
        }
        LayerLayout::ZFile { zfile, tar_base } => {
            let zblock = u64::from(zfile.options().block_size);
            ensure!(zblock != 0, "zfile block size is zero");
            let first = logical_offset / zblock;
            let last = (logical_offset + logical_len - 1) / zblock;
            for idx in first..=last {
                let (c_off, c_len) = zfile.compressed_block_extent(idx * zblock)?;
                push_aligned_blocks(c_off + tar_base, c_len, object_size, out)?;
            }
            Ok(())
        }
    }
}

fn push_aligned_blocks(offset: u64, len: u64, object_size: u64, out: &mut Vec<u32>) -> Result<()> {
    ensure!(len > 0, "empty extent to align");
    let object_blocks = object_size.div_ceil(OBJECT_BLOCK_BYTES);
    let first = offset / OBJECT_BLOCK_BYTES;
    let last = (offset + len - 1) / OBJECT_BLOCK_BYTES;
    for block in first..=last.min(object_blocks.saturating_sub(1)) {
        out.push(u32::try_from(block).context("block id overflow")?);
    }
    Ok(())
}

/// Plan the 256 KiB-aligned, deduplicated, first-need-ordered physical block
/// list for `pages` (logical 4 KiB page numbers into the memory image's
/// virtual size) against the final layer objects in `layers` (bottom-to-top
/// order, matching the memory image's lowers).
///
/// `budget_bytes` bounds the *total v2 file size* (header + object table +
/// entry table + frame checksums + slot payload). The plan stops at the
/// first page whose newly required blocks would exceed it.
pub async fn plan_blocks(
    pages: &[u32],
    mem_virtual_size: u64,
    layers: &[FinalLayerSource],
    budget_bytes: u64,
) -> Result<PackPlan> {
    plan_blocks_with_observer(
        pages,
        mem_virtual_size,
        layers,
        budget_bytes,
        &mut |_, _| {},
    )
    .await
}

/// Like [`plan_blocks`], additionally invoking `on_page` for every processed
/// page with its resolved blocks (see [`PageObserver`]).
pub async fn plan_blocks_with_observer(
    pages: &[u32],
    mem_virtual_size: u64,
    layers: &[FinalLayerSource],
    budget_bytes: u64,
    on_page: &mut PageObserver<'_>,
) -> Result<PackPlan> {
    ensure!(!layers.is_empty(), "no final layers to plan against");
    ensure!(layers.len() <= u16::MAX as usize + 1, "too many layers");
    let planner = BlockPlanner::new(layers, budget_bytes).await?;
    planner.plan(pages, mem_virtual_size, on_page)
}

/// Like [`plan_blocks`], but translates `(voffset, vlen)` spans with one
/// merged-index lookup per span instead of one lookup per 4 KiB page.
/// Spans must be page-aligned and in memory-image bounds; blocks come out
/// in first-need order following the input span order, deduplicated and
/// budget-checked per span exactly like the per-page path.
pub async fn plan_ranges(
    ranges: &[(u64, u64)],
    mem_virtual_size: u64,
    layers: &[FinalLayerSource],
    budget_bytes: u64,
) -> Result<PackPlan> {
    ensure!(!layers.is_empty(), "no final layers to plan against");
    ensure!(layers.len() <= u16::MAX as usize + 1, "too many layers");
    let planner = BlockPlanner::new(layers, budget_bytes).await?;
    planner.plan_ranges(ranges, mem_virtual_size)
}

struct BlockPlanner<'a> {
    layers: &'a [FinalLayerSource],
    opened: Vec<OpenedLayer>,
    index: Arc<dyn LogIndex>,
    budget_bytes: u64,
    spent_bytes: u64,
    stats: PlanStats,
    objects: Vec<PlannedObject>,
    object_of_layer: Vec<Option<u16>>,
    blocks: Vec<PlannedBlock>,
    seen: HashMap<(u16, u32), usize>,
    metadata_emitted: Vec<bool>,
}

impl<'a> BlockPlanner<'a> {
    async fn new(layers: &'a [FinalLayerSource], budget_bytes: u64) -> Result<Self> {
        let mut opened = Vec::with_capacity(layers.len());
        for layer in layers {
            ensure!(
                layer.size > 0,
                "final layer '{}' has zero size",
                layer.digest
            );
            opened.push(open_layer(layer).await?);
        }
        let lsmt_files: Vec<Arc<dyn VirtualFile>> =
            opened.iter().map(|layer| layer.lsmt_file.clone()).collect();
        let merged = open_files_ro(&lsmt_files).await?;
        Ok(Self {
            layers,
            opened,
            index: merged.index(),
            budget_bytes,
            spent_bytes: HEADER_BYTES,
            stats: PlanStats::default(),
            objects: Vec::new(),
            object_of_layer: vec![None; layers.len()],
            blocks: Vec::new(),
            seen: HashMap::new(),
            metadata_emitted: vec![false; layers.len()],
        })
    }

    /// Register the object for `layer_idx` (lazily) and stage one block if it
    /// is new (against both the committed selection and the pending stage).
    /// Returns its encoded (object, block) pair when newly staged.
    fn stage_block(
        &mut self,
        layer_idx: usize,
        block_id: u32,
        staged: &[(u16, u32, bool)],
    ) -> Option<(u16, u32, bool)> {
        let object_idx = match self.object_of_layer[layer_idx] {
            Some(idx) => idx,
            None => {
                let idx = u16::try_from(self.objects.len()).expect("object index overflow");
                self.objects.push(PlannedObject {
                    digest: self.layers[layer_idx].digest.clone(),
                    size: self.layers[layer_idx].size,
                    layer: layer_idx,
                });
                self.object_of_layer[layer_idx] = Some(idx);
                idx
            }
        };
        if self.seen.contains_key(&(object_idx, block_id))
            || staged
                .iter()
                .any(|(o, b, _)| (*o, *b) == (object_idx, block_id))
        {
            return None;
        }
        Some((object_idx, block_id, false))
    }

    /// Resolve one lookup's mappings into `(layer, block)` dependencies of
    /// the span. Returns true when at least one mapping has a physical range.
    fn resolve_span_blocks(
        &self,
        mappings: &[crate::lsmt::index::SegmentMapping],
        out: &mut Vec<(usize, u32)>,
    ) -> Result<bool> {
        let mut backed = false;
        for mapping in mappings {
            if !mapping.has_physical_range() {
                continue;
            }
            backed = true;
            let layer_idx = self.layers.len() - 1 - mapping.tag as usize;
            let layer = &self.opened[layer_idx];
            let mut raw_blocks = Vec::new();
            map_extent_blocks(
                &layer.layout,
                mapping.moffset * SECTOR_BYTES,
                u64::from(mapping.length()) * SECTOR_BYTES,
                self.layers[layer_idx].size,
                &mut raw_blocks,
            )?;
            for block_id in raw_blocks {
                out.push((layer_idx, block_id));
            }
        }
        Ok(backed)
    }

    /// Stage a resolved span's blocks (metadata for freshly-touched layers
    /// first) as one budget unit. On success returns the span's full
    /// resolution (newly staged blocks plus blocks first selected earlier)
    /// with positions; on budget overflow rolls the staging back and returns
    /// `None` (the caller stops at this span).
    fn stage_span(
        &mut self,
        span_blocks: &[(usize, u32)],
    ) -> Result<Option<Vec<(u16, u32, usize)>>> {
        let stage_start_objects = self.objects.len();
        let stage_data_blocks = self.stats.data_blocks;
        let stage_meta_blocks = self.stats.metadata_blocks;
        let mut staged: Vec<(u16, u32, bool)> = Vec::new();

        let mut staged_layers: Vec<usize> = Vec::new();
        for (layer_idx, _) in span_blocks.iter().copied() {
            if staged_layers.contains(&layer_idx) {
                continue;
            }
            staged_layers.push(layer_idx);
            if !self.metadata_emitted[layer_idx] {
                self.stage_metadata(layer_idx, &mut staged)?;
            }
        }
        for (layer_idx, block_id) in span_blocks.iter().copied() {
            if let Some(pair) = self.stage_block(layer_idx, block_id, &staged) {
                staged.push(pair);
                self.stats.data_blocks += 1;
            }
        }

        let mut new_bytes = 0u64;
        for object in &self.objects[stage_start_objects..] {
            new_bytes += OBJECT_ENTRY_FIXED_BYTES + object.digest.len() as u64;
        }
        new_bytes += staged.len() as u64 * (OBJECT_BLOCK_BYTES + ENTRY_BYTES);
        new_bytes += 4 * (staged.len() as u64 * OBJECT_BLOCK_BYTES).div_ceil(FRAME_BYTES);

        if self.spent_bytes + new_bytes > self.budget_bytes {
            self.objects.truncate(stage_start_objects);
            for entry in &mut self.object_of_layer {
                if entry.is_some_and(|idx| idx as usize >= stage_start_objects) {
                    *entry = None;
                }
            }
            self.stats.data_blocks = stage_data_blocks;
            self.stats.metadata_blocks = stage_meta_blocks;
            return Ok(None);
        }

        self.spent_bytes += new_bytes;
        for (object, block_id, metadata) in staged {
            let position = self.blocks.len();
            self.seen.insert((object, block_id), position);
            self.blocks.push(PlannedBlock {
                object,
                block_id,
                metadata,
            });
        }

        let mut resolved: Vec<(u16, u32, usize)> = Vec::new();
        for (layer_idx, block_id) in span_blocks.iter().copied() {
            let Some(object_idx) = self.object_of_layer[layer_idx] else {
                continue;
            };
            if let Some(position) = self.seen.get(&(object_idx, block_id)) {
                resolved.push((object_idx, block_id, *position));
            }
        }
        Ok(Some(resolved))
    }

    fn finish(mut self) -> PackPlan {
        self.stats.unique_blocks = self.seen.len();
        self.stats.payload_bytes = self.stats.unique_blocks as u64 * OBJECT_BLOCK_BYTES;
        self.gap_stats();
        PackPlan {
            objects: self.objects,
            blocks: self.blocks,
            stats: self.stats,
        }
    }

    fn plan(
        mut self,
        pages: &[u32],
        mem_virtual_size: u64,
        on_page: &mut PageObserver<'_>,
    ) -> Result<PackPlan> {
        self.stats.pages_total = pages.len();
        let mut truncated = false;
        let mut mappings = Vec::new();
        let mut span_blocks: Vec<(usize, u32)> = Vec::new();

        for &page in pages {
            if truncated {
                self.stats.pages_truncated_by_budget += 1;
                continue;
            }
            let voffset = u64::from(page) * PLAN_PAGE_BYTES;
            ensure!(
                voffset + PLAN_PAGE_BYTES <= mem_virtual_size,
                "page {page:#x} out of memory image range {mem_virtual_size:#x}"
            );
            mappings.clear();
            self.index.lookup(
                Segment::new(
                    voffset / SECTOR_BYTES,
                    (PLAN_PAGE_BYTES / SECTOR_BYTES) as u32,
                ),
                &mut mappings,
            );
            span_blocks.clear();
            if !self.resolve_span_blocks(&mappings, &mut span_blocks)? {
                self.stats.pages_zero_hole += 1;
                on_page(page, &[]);
                continue;
            }
            match self.stage_span(&span_blocks)? {
                Some(resolved) => {
                    self.stats.pages_backed += 1;
                    on_page(page, &resolved);
                }
                None => {
                    truncated = true;
                    self.stats.pages_truncated_by_budget += 1;
                }
            }
        }
        Ok(self.finish())
    }

    fn plan_ranges(mut self, ranges: &[(u64, u64)], mem_virtual_size: u64) -> Result<PackPlan> {
        let mut truncated = false;
        let mut mappings = Vec::new();
        let mut span_blocks: Vec<(usize, u32)> = Vec::new();

        for &(voffset, vlen) in ranges {
            ensure!(
                vlen > 0
                    && voffset % PLAN_PAGE_BYTES == 0
                    && vlen % PLAN_PAGE_BYTES == 0
                    && voffset + vlen <= mem_virtual_size,
                "range {voffset:#x}+{vlen:#x} out of memory image range {mem_virtual_size:#x}"
            );
            let span_pages = (vlen / PLAN_PAGE_BYTES) as usize;
            self.stats.pages_total += span_pages;
            if truncated {
                self.stats.pages_truncated_by_budget += span_pages;
                continue;
            }
            mappings.clear();
            self.index.lookup(
                Segment::new(voffset / SECTOR_BYTES, (vlen / SECTOR_BYTES) as u32),
                &mut mappings,
            );
            span_blocks.clear();
            if !self.resolve_span_blocks(&mappings, &mut span_blocks)? {
                self.stats.pages_zero_hole += span_pages;
                continue;
            }
            if self.stage_span(&span_blocks)?.is_some() {
                self.stats.pages_backed += span_pages;
            } else {
                truncated = true;
                self.stats.pages_truncated_by_budget += span_pages;
            }
        }
        Ok(self.finish())
    }

    /// Stage a layer's metadata blocks (header + index/trailer region) the
    /// first time one of its data blocks is needed.
    fn stage_metadata(
        &mut self,
        layer_idx: usize,
        staged: &mut Vec<(u16, u32, bool)>,
    ) -> Result<()> {
        if self.metadata_emitted[layer_idx] {
            return Ok(());
        }
        self.metadata_emitted[layer_idx] = true;
        let layer = &self.opened[layer_idx];
        let object_size = self.layers[layer_idx].size;
        let mut meta_raw = Vec::new();
        map_extent_blocks(
            &layer.layout,
            0,
            PLAN_PAGE_BYTES,
            object_size,
            &mut meta_raw,
        )?;
        let index_offset = layer.metadata.index_offset;
        let tail_len = layer.metadata.file_size.saturating_sub(index_offset);
        if tail_len > 0 {
            map_extent_blocks(
                &layer.layout,
                index_offset,
                tail_len,
                object_size,
                &mut meta_raw,
            )?;
        }
        for block_id in meta_raw {
            if let Some((object, block_id, _)) = self.stage_block(layer_idx, block_id, staged) {
                staged.push((object, block_id, true));
                self.stats.metadata_blocks += 1;
            }
        }
        Ok(())
    }

    /// Missing-block run statistics over contributing objects (what the
    /// background still has to fetch after this plan is imported).
    fn gap_stats(&mut self) {
        for (layer_idx, source) in self.layers.iter().enumerate() {
            let Some(object_idx) = self.object_of_layer[layer_idx] else {
                continue;
            };
            let total = source.size.div_ceil(OBJECT_BLOCK_BYTES);
            let mut selected: Vec<u32> = self
                .seen
                .keys()
                .filter(|(o, _)| *o == object_idx)
                .map(|(_, b)| *b)
                .collect();
            selected.sort_unstable();
            let mut cursor = 0u64;
            for block in selected {
                let b = u64::from(block);
                if b > cursor {
                    self.stats.gap_runs += 1;
                    self.stats.gap_blocks += (b - cursor) as usize;
                }
                cursor = b + 1;
            }
            if cursor < total {
                self.stats.gap_runs += 1;
                self.stats.gap_blocks += (total - cursor) as usize;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsmt::file::{CommitArgs, LSMTFile};
    use tempfile::TempDir;

    const VSIZE: u64 = 4 * 1024 * 1024;
    const BIG_BUDGET: u64 = 1 << 30;

    /// Write `pages` (page_idx → fill byte) into a fresh RW LSMT and seal it
    /// as a final layer file. `filler` densely writes pages [0, filler) first
    /// so measured pages land in distinct 256 KiB physical blocks.
    async fn make_raw_layer_at(
        dir: &TempDir,
        name: &str,
        pages: &[(u32, u8)],
        filler: u32,
    ) -> FinalLayerSource {
        let data = Arc::new(LocalFile::new(dir.path().join(format!("{name}.data"))).unwrap());
        let index = Arc::new(LocalFile::new(dir.path().join(format!("{name}.index"))).unwrap());
        let lsmt = LSMTFile::create(data, Some(index), VSIZE, false)
            .await
            .unwrap();
        for page in 0..filler {
            // pseudo-random page content: keeps zfile blocks incompressible
            // so fixtures spread across many 256 KiB physical blocks
            let buf: Vec<u8> = (0..PLAN_PAGE_BYTES as usize)
                .map(|i| ((i as u64).wrapping_mul(2_654_435_761) >> 24) as u8)
                .collect();
            lsmt.write_at(u64::from(page) * PLAN_PAGE_BYTES, &buf)
                .await
                .unwrap();
        }
        for (page, fill) in pages {
            let buf = vec![*fill; PLAN_PAGE_BYTES as usize];
            lsmt.write_at(u64::from(*page) * PLAN_PAGE_BYTES, &buf)
                .await
                .unwrap();
        }
        let path = dir.path().join(name);
        let dest: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&path).unwrap());
        lsmt.commit_with_args(CommitArgs::new(dest.clone()))
            .await
            .unwrap();
        let size = dest.size().await.unwrap();
        FinalLayerSource {
            digest: format!("sha256:{name}"),
            size,
            source: FinalLayerBytes::LocalPath(path),
        }
    }

    async fn make_raw_layer(dir: &TempDir, name: &str, pages: &[(u32, u8)]) -> FinalLayerSource {
        make_raw_layer_at(dir, name, pages, 0).await
    }

    async fn make_zfile_layer(dir: &TempDir, name: &str, pages: &[(u32, u8)]) -> FinalLayerSource {
        let raw = make_raw_layer_at(dir, &format!("{name}-raw"), pages, 1024).await;
        let raw_path = match &raw.source {
            FinalLayerBytes::LocalPath(path) => path.clone(),
            FinalLayerBytes::VFile(_) => unreachable!(),
        };
        let raw_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(&raw_path).unwrap());
        let raw_bytes = raw_file.read_at(0, raw.size as usize).await.unwrap();
        let path = dir.path().join(name);
        let dest: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&path).unwrap());
        let args = crate::compression::zfile::CompressArgs::new(
            crate::compression::zfile::CompressOptions::new(
                crate::compression::zfile::CompressOptions::LZ4,
                4096,
                0,
            ),
        );
        let mut builder = crate::compression::zfile::ZFileBuilder::new(dest.clone(), &args)
            .await
            .unwrap();
        builder.write(raw_bytes.as_ref()).await.unwrap();
        builder.finish().await.unwrap();
        let sealed = builder.close().await.unwrap();
        let size = sealed.size().await.unwrap();
        FinalLayerSource {
            digest: format!("sha256:{name}"),
            size,
            source: FinalLayerBytes::LocalPath(path),
        }
    }

    async fn read_page_via_stack(layers: &[FinalLayerSource], page: u32) -> Vec<u8> {
        let mut files: Vec<Arc<dyn VirtualFile>> = Vec::with_capacity(layers.len());
        for layer in layers {
            let layer_path = match &layer.source {
                FinalLayerBytes::LocalPath(path) => path.clone(),
                FinalLayerBytes::VFile(_) => unreachable!(),
            };
            let local: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(&layer_path).unwrap());
            let unwrapped = if is_tar_file(local.as_ref()).await.unwrap() {
                new_tar_file(local).await.unwrap() as Arc<dyn VirtualFile>
            } else {
                local
            };
            let file = if is_zfile(unwrapped.clone()).await.unwrap() == 1 {
                crate::compression::zfile::zfile_open_ro_vfile(unwrapped, false)
                    .await
                    .unwrap()
            } else {
                unwrapped
            };
            files.push(file);
        }
        let merged = open_files_ro(&files).await.unwrap();
        merged
            .read_at(u64::from(page) * PLAN_PAGE_BYTES, PLAN_PAGE_BYTES as usize)
            .await
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn raw_layer_maps_pages_to_aligned_blocks_in_first_need_order() {
        let dir = TempDir::new().unwrap();
        // filler pages 0..299 push later writes into distinct physical blocks
        let layer =
            make_raw_layer_at(&dir, "l0", &[(1, 0xAA), (300, 0xCC), (900, 0xEE)], 1024).await;

        // request order [300, 900]: 300's physical block must precede 900's
        let plan = plan_blocks(
            &[300, 900, 1, 300],
            VSIZE,
            std::slice::from_ref(&layer),
            BIG_BUDGET,
        )
        .await
        .unwrap();
        assert_eq!(plan.stats.pages_backed, 4);
        assert_eq!(plan.stats.pages_zero_hole, 0);
        assert!(plan.stats.metadata_blocks >= 1);
        // no duplicate block ids
        let ids: Vec<u32> = plan.blocks.iter().map(|block| block.block_id).collect();
        let unique: std::collections::HashSet<u32> = ids.iter().copied().collect();
        assert_eq!(unique.len(), ids.len(), "no duplicate blocks");
        // metadata block 0 is selected and first
        assert_eq!(ids.first().copied(), Some(0));
        // first-need order: the two distinct data blocks (300's, 900's) keep
        // request order; metadata is block 0 + the object tail block.
        let tail_block = ((layer.size - 1) / OBJECT_BLOCK_BYTES) as u32;
        let data_ids: Vec<u32> = ids
            .iter()
            .copied()
            .filter(|id| *id != 0 && *id != tail_block)
            .collect();
        assert!(data_ids.len() >= 2, "300 and 900 in distinct blocks");
        assert!(
            data_ids[0] < data_ids[1],
            "first-need order: 300's block before 900's"
        );
        // read-back consistency through the normal LSMT path
        assert_eq!(
            read_page_via_stack(std::slice::from_ref(&layer), 1).await,
            vec![0xAA; 4096]
        );
        assert_eq!(
            read_page_via_stack(std::slice::from_ref(&layer), 300).await,
            vec![0xCC; 4096]
        );
        assert_eq!(read_page_via_stack(&[layer], 900).await, vec![0xEE; 4096]);
    }

    #[tokio::test]
    async fn shadowed_parent_pages_resolve_to_top_layer() {
        let dir = TempDir::new().unwrap();
        let lower = make_raw_layer(&dir, "lower", &[(5, 0x11), (6, 0x12)]).await;
        let upper = make_raw_layer(&dir, "upper", &[(5, 0x22)]).await;
        let layers = [lower, upper];
        let plan = plan_blocks(&[5, 6], VSIZE, &layers, BIG_BUDGET)
            .await
            .unwrap();

        // page 5 must come from the upper object (located by digest)
        let upper_object = plan
            .objects
            .iter()
            .position(|object| object.digest == "sha256:upper")
            .expect("upper object in plan") as u16;
        assert!(plan.blocks.iter().any(|block| block.object == upper_object));
        assert_eq!(plan.objects.len(), 2);
        // and the read-back through the merged stack returns the shadowing data
        assert_eq!(read_page_via_stack(&layers, 5).await, vec![0x22; 4096]);
        assert_eq!(read_page_via_stack(&layers, 6).await, vec![0x12; 4096]);
    }

    #[tokio::test]
    async fn holes_and_unbacked_pages_produce_no_blocks() {
        let dir = TempDir::new().unwrap();
        let layer = make_raw_layer(&dir, "l0", &[(7, 0x77)]).await;
        let plan = plan_blocks(&[7, 8, 9, 500], VSIZE, &[layer], BIG_BUDGET)
            .await
            .unwrap();
        assert_eq!(plan.stats.pages_backed, 1);
        assert_eq!(plan.stats.pages_zero_hole, 3);
        assert!(plan.stats.unique_blocks >= 1);
    }

    #[tokio::test]
    async fn zfile_layer_maps_through_compressed_extents_and_reads_back() {
        let dir = TempDir::new().unwrap();
        let layer = make_zfile_layer(&dir, "zl0", &[(3, 0x33), (300, 0x44)]).await;
        let plan = plan_blocks(&[3, 300], VSIZE, std::slice::from_ref(&layer), BIG_BUDGET)
            .await
            .unwrap();

        assert_eq!(plan.stats.pages_backed, 2);
        assert!(plan.stats.unique_blocks >= 2);
        assert!(plan
            .blocks
            .iter()
            .all(|block| (block.block_id as u64) < layer.size.div_ceil(OBJECT_BLOCK_BYTES)));
        // every selected block must be within the final compressed object
        let object_blocks = layer.size.div_ceil(OBJECT_BLOCK_BYTES);
        assert!(plan
            .blocks
            .iter()
            .all(|block| (block.block_id as u64) < object_blocks));
        // the compressed object reads back the same content through the zfile path
        assert_eq!(
            read_page_via_stack(std::slice::from_ref(&layer), 3).await,
            vec![0x33; 4096]
        );
        assert_eq!(read_page_via_stack(&[layer], 300).await, vec![0x44; 4096]);
    }

    #[tokio::test]
    async fn budget_truncates_at_first_unfittable_page_without_half_sets() {
        let dir = TempDir::new().unwrap();
        // filler 0..298 so 100/200/900 compact into distinct physical blocks
        let layer =
            make_raw_layer_at(&dir, "l0", &[(100, 0xAA), (200, 0xBB), (900, 0xCC)], 1024).await;
        // 5 unique blocks expected (meta + 3 data); budget must admit exactly 4
        let budget = 1_100_000u64;
        let plan = plan_blocks(&[100, 200, 900], VSIZE, &[layer], budget)
            .await
            .unwrap();
        assert_eq!(
            plan.stats.pages_truncated_by_budget, 1,
            "the third page's dependency set must not fit"
        );
        assert_eq!(plan.stats.pages_backed, 2);
        assert!(plan.stats.payload_bytes <= budget);
        // no half dependency set: everything committed must be within budget
        let total = HEADER_BYTES
            + plan
                .objects
                .iter()
                .map(|o| OBJECT_ENTRY_FIXED_BYTES + o.digest.len() as u64)
                .sum::<u64>()
            + plan.blocks.len() as u64 * (OBJECT_BLOCK_BYTES + ENTRY_BYTES)
            + 4 * (plan.blocks.len() as u64 * OBJECT_BLOCK_BYTES).div_ceil(FRAME_BYTES);
        assert!(total <= budget);
    }

    #[tokio::test]
    async fn metadata_blocks_precede_first_data_block_of_layer() {
        let dir = TempDir::new().unwrap();
        let layer = make_raw_layer(&dir, "l0", &[(700, 0xEE)]).await;
        let plan = plan_blocks(&[700], VSIZE, std::slice::from_ref(&layer), BIG_BUDGET)
            .await
            .unwrap();
        assert!(plan.stats.metadata_blocks >= 1);
        // block 0 (header) must be in the selection and before any data block
        assert_eq!(plan.blocks.first().map(|b| b.block_id), Some(0));
        // index region tail blocks also selected
        let tail_block = (layer.size - 1) / OBJECT_BLOCK_BYTES;
        assert!(plan.blocks.iter().any(|b| b.block_id == tail_block as u32));
    }
    #[tokio::test]
    async fn vfile_input_plans_like_local_path() {
        let dir = TempDir::new().unwrap();
        let layer = make_raw_layer_at(&dir, "l0", &[(1, 0xAA), (300, 0xCC)], 1024).await;
        let layer_path = match &layer.source {
            FinalLayerBytes::LocalPath(path) => path.clone(),
            FinalLayerBytes::VFile(_) => unreachable!(),
        };
        let vfile_layer = FinalLayerSource {
            digest: layer.digest.clone(),
            size: layer.size,
            source: FinalLayerBytes::VFile(Arc::new(LocalFile::open_ro(&layer_path).unwrap())),
        };

        let via_path = plan_blocks(&[300, 1], VSIZE, std::slice::from_ref(&layer), BIG_BUDGET)
            .await
            .unwrap();
        let via_vfile = plan_blocks(
            &[300, 1],
            VSIZE,
            std::slice::from_ref(&vfile_layer),
            BIG_BUDGET,
        )
        .await
        .unwrap();
        assert_eq!(via_path.blocks, via_vfile.blocks);
        assert_eq!(via_path.stats.unique_blocks, via_vfile.stats.unique_blocks);
    }

    #[tokio::test]
    async fn observer_reports_positions_in_first_need_order() {
        let dir = TempDir::new().unwrap();
        // pages 300 and 900 land in distinct blocks; page 301 shares 300's block
        let layer =
            make_raw_layer_at(&dir, "l0", &[(300, 0xCC), (301, 0xDD), (900, 0xEE)], 1024).await;
        type Observed = (u32, Vec<(u16, u32, usize)>);
        let mut seen: Vec<Observed> = Vec::new();
        let plan = plan_blocks_with_observer(
            &[300, 900, 301],
            VSIZE,
            std::slice::from_ref(&layer),
            BIG_BUDGET,
            &mut |page, blocks| seen.push((page, blocks.to_vec())),
        )
        .await
        .unwrap();

        assert_eq!(seen.len(), 3);
        // page 300 resolves to (object 0, its block) at some position p
        let (_, first) = &seen[0];
        assert_eq!(first.len(), 1);
        let (obj_300, block_300, pos_300) = first[0];
        assert_eq!(obj_300, 0);
        // metadata block precedes it, so the data position is > 0
        assert!(pos_300 > 0);
        // page 900: a different block at a later position
        let (_, second) = &seen[1];
        assert_eq!(second.len(), 1);
        assert_ne!(second[0].1, block_300);
        assert!(second[0].2 > pos_300);
        // page 301 shares 300's block: same position as the first need
        let (_, third) = &seen[2];
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].1, block_300);
        assert_eq!(third[0].2, pos_300, "dedup keeps the first-need position");
        // and the plan holds each block once
        assert_eq!(
            plan.blocks
                .iter()
                .filter(|b| b.object == 0 && b.block_id == block_300)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn ranges_plan_matches_per_page_plan() {
        let dir = TempDir::new().unwrap();
        let layer =
            make_raw_layer_at(&dir, "l0", &[(1, 0xAA), (300, 0xCC), (900, 0xEE)], 1024).await;
        let pages = [1u32, 300, 301, 900];
        let per_page = plan_blocks(&pages, VSIZE, std::slice::from_ref(&layer), BIG_BUDGET)
            .await
            .unwrap();
        let spans: Vec<(u64, u64)> = vec![
            (PLAN_PAGE_BYTES, PLAN_PAGE_BYTES),
            (300 * PLAN_PAGE_BYTES, 2 * PLAN_PAGE_BYTES),
            (900 * PLAN_PAGE_BYTES, PLAN_PAGE_BYTES),
        ];
        let per_span = plan_ranges(&spans, VSIZE, std::slice::from_ref(&layer), BIG_BUDGET)
            .await
            .unwrap();
        assert_eq!(per_page.objects, per_span.objects);
        assert_eq!(per_page.blocks, per_span.blocks);
        assert_eq!(per_page.stats.pages_total, per_span.stats.pages_total);
        assert_eq!(per_page.stats.pages_backed, per_span.stats.pages_backed);
        assert_eq!(
            per_page.stats.pages_zero_hole,
            per_span.stats.pages_zero_hole
        );
        assert_eq!(per_page.stats.unique_blocks, per_span.stats.unique_blocks);
    }
}
