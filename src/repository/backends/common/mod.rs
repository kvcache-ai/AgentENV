use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::OverlaybdCompactOutput;
use overlaybd::index_file::LSMTReadOnlyFile;
use overlaybd::layer_output::create_commit_args;
use overlaybd::virtual_file::VirtualFile;

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
    let args = create_commit_args(file.clone(), OverlaybdCompactOutput::Raw, true).await?;
    let descriptor = source
        .commit_preserving_metadata(args)
        .await?
        .context("layer writer did not return a digest")?;
    file.sync()
        .await
        .with_context(|| format!("sync dense overlaybd layer '{}'", destination.display()))?;

    Ok(descriptor)
}
