use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use storage_util::{CompactBuffer, CompactWriter};
use tokio::sync::Mutex;

use crate::backend::local::LocalFile;
use crate::io::virtual_file::VirtualFile;
use crate::layer::layer_metadata::read_overlaybd_layer_is_sparse_rw;
use crate::lsmt::file::{CommitArgs, LSMTReadOnlyFile};

const DENSE_EXPORT_BUFFER_SIZE: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DenseLayerDescriptor {
    pub digest: String,
    pub size: u64,
}

/// Tracks the digest and size of a dense overlaybd byte stream.
///
/// Callers must feed bytes in strictly increasing offset order. This mirrors
/// how content digests are defined and lets streaming upload paths verify the
/// exact bytes they emitted without buffering the whole dense layer.
pub struct DenseLayerDigest {
    hasher: Sha256,
    offset: u64,
}

impl Default for DenseLayerDigest {
    fn default() -> Self {
        Self {
            hasher: Sha256::new(),
            offset: 0,
        }
    }
}

impl DenseLayerDigest {
    pub fn absorb(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        if offset != self.offset {
            anyhow::bail!(
                "dense overlaybd writer received out-of-order write: got offset {}, expected {}",
                offset,
                self.offset
            );
        }
        self.hasher.update(data);
        self.offset = self
            .offset
            .checked_add(data.len() as u64)
            .context("dense overlaybd output size overflow")?;
        Ok(())
    }

    pub fn descriptor(&self) -> DenseLayerDescriptor {
        DenseLayerDescriptor {
            digest: format!("sha256:{:x}", self.hasher.clone().finalize()),
            size: self.offset,
        }
    }
}

struct DigestCompactWriter {
    buf_size: usize,
    state: Mutex<DenseLayerDigest>,
}

impl DigestCompactWriter {
    fn new(buf_size: usize) -> Arc<Self> {
        Arc::new(Self {
            buf_size,
            state: Mutex::new(DenseLayerDigest::default()),
        })
    }

    async fn finish(&self) -> DenseLayerDescriptor {
        self.state.lock().await.descriptor()
    }

    async fn absorb(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.state.lock().await.absorb(offset, data)
    }
}

#[async_trait]
impl CompactWriter for DigestCompactWriter {
    async fn alloc_buffer(&self) -> Result<Box<dyn CompactBuffer>> {
        Ok(Box::new(vec![0u8; self.buf_size]))
    }

    fn buffer_size(&self) -> usize {
        self.buf_size
    }

    fn requires_ordered_writes(&self) -> bool {
        true
    }

    async fn write(&self, buf: Box<dyn CompactBuffer>, offset: u64, len: usize) -> Result<()> {
        let data = AsRef::<[u8]>::as_ref(buf.as_ref());
        if len > data.len() {
            anyhow::bail!("dense overlaybd digest write len exceeds buffer range");
        }
        self.absorb(offset, &data[..len]).await
    }

    async fn write_all_at(&self, data: &[u8], offset: u64) -> Result<()> {
        self.absorb(offset, data).await
    }
}

pub fn should_dense_export_layer(path: &Path) -> bool {
    match read_overlaybd_layer_is_sparse_rw(path) {
        Ok(is_sparse) => is_sparse,
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                error = %format_args!("{error:#}"),
                "failed to inspect overlaybd sparse header; falling back to raw layer upload"
            );
            false
        }
    }
}

pub async fn describe_dense_layer(path: &Path) -> Result<DenseLayerDescriptor> {
    let writer = DigestCompactWriter::new(DENSE_EXPORT_BUFFER_SIZE);
    write_dense_layer_to(path, writer.clone()).await?;
    Ok(writer.finish().await)
}

pub async fn write_dense_layer_to(path: &Path, writer: Arc<dyn CompactWriter>) -> Result<()> {
    let source: Arc<dyn VirtualFile> = Arc::new(
        LocalFile::open_ro(path)
            .with_context(|| format!("open sparse overlaybd layer '{}'", path.display()))?,
    );
    let layer = LSMTReadOnlyFile::open(source)
        .await
        .with_context(|| format!("open sealed sparse overlaybd layer '{}'", path.display()))?;
    // `LSMTReadOnlyFile::open` above constructs a single-file sealed layer from
    // this path. `commit_preserving_metadata` intentionally rejects stacked
    // read-only views because their trailer metadata is not a single source of
    // truth for a newly exported dense object.
    let mut args = CommitArgs::from_writer(writer);
    // Dense export writers hash and upload a single byte stream, so compact
    // output must stay sequential even if the default commit concurrency changes.
    args.concurrency = 1;
    layer
        .commit_preserving_metadata(args)
        .await
        .with_context(|| format!("dense-export sparse overlaybd layer '{}'", path.display()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::local::LocalFile;
    use crate::io::virtual_file::VirtualFile;
    use crate::lsmt::file::{CommitArgs, LSMTFile, LSMTReadOnlyFile};

    use super::*;

    #[tokio::test]
    async fn dense_export_rewrites_sparse_offsets_without_changing_logical_data() {
        let temp = tempfile::tempdir().unwrap();
        let sparse_path = temp.path().join("snapshot.commit");
        let dense_path = temp.path().join("dense.commit");
        let virtual_size = 64 * 1024 * 1024;

        let sparse_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&sparse_path).unwrap());
        let sparse = LSMTFile::create(sparse_file, None, virtual_size, true)
            .await
            .unwrap();
        let first = vec![0xAB; 4096];
        let second = vec![0xCD; 4096];
        sparse.write_at(0, &first).await.unwrap();
        sparse.write_at(32 * 1024 * 1024, &second).await.unwrap();
        sparse.close_seal().await.unwrap();
        drop(sparse);

        assert!(should_dense_export_layer(&sparse_path));

        let dense_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&dense_path).unwrap());
        write_dense_layer_to(&sparse_path, CommitArgs::new(dense_file.clone()).writer)
            .await
            .unwrap();

        let descriptor = describe_dense_layer(&sparse_path).await.unwrap();
        let dense_size = dense_file.size().await.unwrap();
        let dense_bytes = std::fs::read(&dense_path).unwrap();
        let dense_digest = format!("sha256:{:x}", Sha256::digest(&dense_bytes));
        assert_eq!(descriptor.size, dense_size);
        assert_eq!(descriptor.digest, dense_digest);
        assert!(
            dense_size < 1024 * 1024,
            "dense output should skip the large sparse hole, got {dense_size}"
        );

        let dense = LSMTReadOnlyFile::open(dense_file).await.unwrap();
        assert_eq!(dense.read_at(0, 4096).await.unwrap().as_ref(), &first[..]);
        assert_eq!(
            dense
                .read_at(32 * 1024 * 1024, 4096)
                .await
                .unwrap()
                .as_ref(),
            &second[..]
        );
        assert!(dense
            .read_at(4096, 4096)
            .await
            .unwrap()
            .iter()
            .all(|b| *b == 0));
    }
}
