use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use nix::unistd::Pid;
use std::sync::Arc;
use uuid::Uuid;

use overlaybd::image_file::ImageFile;
use overlaybd::index::SegmentMapping;
use overlaybd::index_file::{compact_to, CommitArgs};
use overlaybd::virtual_file::VirtualFile;

use storage_util::CompactWriter;

use crate::backend::MemoryImageBackend;
use crate::handler::{GuestRegionUffdMapping, PageState};
use crate::process_vm_reader::ProcessVmReader;

// ---------------------------------------------------------------------------
// OverlaybdMemoryImage — MemoryImageBackend backed by overlaybd ImageFile
// ---------------------------------------------------------------------------

/// A memory snapshot image stored in overlaybd (LSMT) format.
///
/// Uses the same [`ImageFile`] type as disk overlaybd images, unifying the
/// layer opening and I/O path for both memory and disk.
pub struct OverlaybdMemoryImage {
    file: ImageFile,
    virtual_size: u64,
    pagesize: usize,
}

impl OverlaybdMemoryImage {
    pub fn new(file: ImageFile, virtual_size: u64, pagesize: usize) -> Self {
        Self {
            file,
            virtual_size,
            pagesize,
        }
    }

    pub fn virtual_size(&self) -> u64 {
        self.virtual_size
    }

    pub fn file(&self) -> &ImageFile {
        &self.file
    }
}

#[async_trait]
impl MemoryImageBackend for OverlaybdMemoryImage {
    fn pagesize(&self) -> usize {
        self.pagesize
    }

    async fn read_page_at_offset(&self, offset: u64) -> Result<Option<Bytes>> {
        self.file
            .read_at(offset, self.pagesize)
            .await
            .with_context(|| format!("read_at offset = {offset:#x} of overlaybd"))
            .map(Some)
    }

    async fn snapshot(
        &self,
        peer_pid: i32,
        mem_regions: &[GuestRegionUffdMapping],
        dirty_pages: &DashMap<u64, PageState>,
        _layer_dir: &str,
        writer: Arc<dyn CompactWriter>,
    ) -> Result<Uuid> {
        const ALIGNMENT: u64 = 512;

        let reader = ProcessVmReader::new(Pid::from_raw(peer_pid));

        let mut mappings = Vec::new();
        for entry in dirty_pages.iter() {
            let host_virtual_pgoff = *entry.key();
            let host_virtual_start = host_virtual_pgoff * self.pagesize as u64;
            let image_offset = hva_to_image_offset(mem_regions, host_virtual_start)?;
            match *entry.value() {
                PageState::Accessed => {
                    mappings.push(SegmentMapping::new(
                        image_offset / ALIGNMENT,
                        self.pagesize as u32 / ALIGNMENT as u32,
                        host_virtual_start / ALIGNMENT,
                        false,
                        0,
                    ));
                }
                PageState::Removed => {
                    mappings.push(SegmentMapping::new(
                        image_offset / ALIGNMENT,
                        self.pagesize as u32 / ALIGNMENT as u32,
                        0,
                        true,
                        0,
                    ));
                }
            }
        }

        mappings.sort_unstable();

        let uuid = Uuid::new_v4();
        let parent_uuid = self.file.get_uuid(0).context("get uuid of parent layer")?;

        let mut commit_args = CommitArgs::from_writer(writer);
        commit_args.concurrency = 64;
        commit_args.uuid = Some(uuid);
        commit_args.parent_uuid = Some(parent_uuid);

        let src_layers: Vec<Arc<dyn VirtualFile>> = vec![Arc::new(reader)];
        let _ = compact_to(&src_layers, &mappings, self.virtual_size, commit_args)
            .await
            .context("compact_to during overlaybd snapshot")?;

        Ok(uuid)
    }
}

/// Convert host virtual address (HVA) to image offset.
fn hva_to_image_offset(mem_regions: &[GuestRegionUffdMapping], hva: u64) -> Result<u64> {
    let region = mem_regions
        .iter()
        .find(|r| hva >= r.base_host_virt_addr && hva < r.base_host_virt_addr + r.size as u64)
        .with_context(|| format!("find region for hva {hva:#x}"))?;
    let region_offset = hva - region.base_host_virt_addr;
    Ok(region.offset + region_offset)
}
