pub(crate) mod acr;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::dense_export;
use overlaybd::index_file::CommitArgs;
use overlaybd::virtual_file::VirtualFile;

pub(crate) async fn write_dense_overlaybd_layer_to_file(
    source: &Path,
    destination: &Path,
) -> Result<dense_export::DenseLayerDescriptor> {
    let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(destination).with_context(|| {
        format!(
            "create dense overlaybd layer destination '{}'",
            destination.display()
        )
    })?);
    dense_export::write_dense_layer_to(source, CommitArgs::new(file.clone()).writer).await?;
    file.sync()
        .await
        .with_context(|| format!("sync dense overlaybd layer '{}'", destination.display()))?;

    let descriptor = crate::digest::FileDigest::describe(destination)
        .await
        .with_context(|| {
            format!(
                "describe dense overlaybd layer destination '{}'",
                destination.display()
            )
        })?;
    Ok(dense_export::DenseLayerDescriptor {
        digest: descriptor.sha256,
        size: descriptor.size,
    })
}

pub(crate) fn write_dense_overlaybd_layer_to_file_blocking(
    source: &Path,
    destination: &Path,
) -> Result<dense_export::DenseLayerDescriptor> {
    // POSIX snapshot publish calls this inside `run_repository_blocking`.
    // Do not call it from an async task; use the async variant above instead.
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create runtime for dense overlaybd export")?;
    runtime.block_on(write_dense_overlaybd_layer_to_file(&source, &destination))
}
