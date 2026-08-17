//! Write-path decorators used to attach a content descriptor to compacted or
//! committed output.
//!
//! [`DigestTrackingFile`] is a [`VirtualFile`] decorator that streams a
//! sha256 over every `write_at` payload in file order, letting a sealed layer
//! carry its content descriptor (digest + size) without a post-write re-read.
//! Writes landing out of order poison the tracker so it yields no descriptor;
//! callers then fall back to hashing the output file explicitly.
//!
//! [`OrderedWriter`] is a [`CompactWriter`] decorator that forces the ordered
//! compaction path (sequential, in-order writes) so the digest tracked at the
//! output-file level is well-defined. The raw `VirtualFileWriter` would
//! otherwise let `compact_to` issue out-of-order concurrent chunk writes.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use storage_util::compact_writer::{CompactBuffer, CompactWriter};

use super::types::LayerDescriptor;
use crate::io::virtual_file::VirtualFile;

pub struct DigestTrackingFile {
    inner: Arc<dyn VirtualFile>,
    state: Mutex<DigestTrackingState>,
}

struct DigestTrackingState {
    hasher: Sha256,
    expected_offset: u64,
    poisoned: bool,
}

impl DigestTrackingFile {
    pub fn new(inner: Arc<dyn VirtualFile>) -> Self {
        Self {
            inner,
            state: Mutex::new(DigestTrackingState {
                hasher: Sha256::new(),
                expected_offset: 0,
                poisoned: false,
            }),
        }
    }

    /// Content descriptor of everything written so far, or `None` when writes
    /// were not strictly ordered (the digest is then meaningless).
    pub fn descriptor(&self) -> Option<LayerDescriptor> {
        let state = self.state.lock().expect("digest tracker lock");
        if state.poisoned {
            return None;
        }
        Some(LayerDescriptor {
            digest: format!("sha256:{:x}", state.hasher.clone().finalize()),
            size: state.expected_offset,
        })
    }

    fn absorb(&self, offset: u64, data: &[u8]) {
        let mut state = self.state.lock().expect("digest tracker lock");
        if offset != state.expected_offset {
            state.poisoned = true;
            return;
        }
        state.hasher.update(data);
        state.expected_offset += data.len() as u64;
    }
}

#[async_trait::async_trait]
impl VirtualFile for DigestTrackingFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        self.inner.read_at(offset, len).await
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let written = self.inner.write_at(offset, data).await?;
        self.absorb(offset, &data[..written]);
        Ok(written)
    }

    // `as_any` is intentionally not overridden: fast paths that downcast to
    // the inner concrete file must not bypass digest tracking.

    async fn size(&self) -> Result<u64> {
        self.inner.size().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }

    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        self.inner.seek_data(offset).await
    }

    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        self.inner.seek_hole(offset).await
    }

    async fn discard(&self, offset: u64, len: u64) -> Result<()> {
        self.inner.discard(offset, len).await
    }

    async fn evict_range(&self, offset: u64, len: u64) -> Result<()> {
        self.inner.evict_range(offset, len).await
    }

    async fn evict_all(&self) -> Result<()> {
        self.inner.evict_all().await
    }

    async fn fgetxattr(&self, name: &str) -> Result<Vec<u8>> {
        self.inner.fgetxattr(name).await
    }

    async fn flistxattr(&self) -> Result<Vec<String>> {
        self.inner.flistxattr().await
    }

    async fn fsetxattr(&self, name: &str, value: &[u8], flags: i32) -> Result<()> {
        self.inner.fsetxattr(name, value, flags).await
    }

    async fn fremovexattr(&self, name: &str) -> Result<()> {
        self.inner.fremovexattr(name).await
    }
}

pub struct OrderedWriter {
    inner: Arc<dyn CompactWriter>,
}

impl OrderedWriter {
    pub fn new(inner: Arc<dyn CompactWriter>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl CompactWriter for OrderedWriter {
    async fn alloc_buffer(&self) -> Result<Box<dyn CompactBuffer>> {
        self.inner.alloc_buffer().await
    }

    fn buffer_size(&self) -> usize {
        self.inner.buffer_size()
    }

    fn requires_ordered_writes(&self) -> bool {
        true
    }

    async fn write(&self, buf: Box<dyn CompactBuffer>, offset: u64, len: usize) -> Result<()> {
        self.inner.write(buf, offset, len).await
    }

    async fn write_all_at(&self, data: &[u8], offset: u64) -> Result<()> {
        self.inner.write_all_at(data, offset).await
    }

    async fn finalize(&self) -> Result<()> {
        self.inner.finalize().await
    }
}
