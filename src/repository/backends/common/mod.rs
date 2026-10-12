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
