use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::index_file::{CommitArgs, LSMTReadOnlyFile};
use overlaybd::virtual_file::{
    DigestCompactWriter, VirtualFile, DIGEST_COMPACT_WRITER_BUFFER_SIZE,
};

/// Export and sync the layer, returning the digest of its final representation.
pub(crate) async fn write_dense_overlaybd_layer_to_file(
    source: &Path,
    destination: &Path,
) -> Result<overlaybd::LayerDescriptor> {
    let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(destination).with_context(|| {
        format!(
            "create dense overlaybd layer destination '{}'",
            destination.display()
        )
    })?);
    let source = LSMTReadOnlyFile::open(Arc::new(LocalFile::open_ro(source)?)).await?;
    let mut args = CommitArgs::from_writer(DigestCompactWriter::new(
        DIGEST_COMPACT_WRITER_BUFFER_SIZE,
        file.clone(),
    ));
    args.concurrency = 1;
    let descriptor = source
        .commit_preserving_metadata(args)
        .await?
        .context("dense overlaybd writer did not return a digest")?;
    file.sync()
        .await
        .with_context(|| format!("sync dense overlaybd layer '{}'", destination.display()))?;

    Ok(descriptor)
}
