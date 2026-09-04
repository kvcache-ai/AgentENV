pub(crate) mod acr;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::dense_export;
use overlaybd::index_file::CommitArgs;
use overlaybd::virtual_file::VirtualFile;

use crate::snapshot::{ManagedLayer, OverlaybdLayerRef, RepositoryError, RepositoryResult};
use uuid::Uuid;

pub(crate) async fn materialize_volume_image_config(
    layers: &[OverlaybdLayerRef],
    destination: &Path,
    managed_layer: impl Fn(&ManagedLayer) -> overlaybd::config::LayerConfig,
) -> RepositoryResult<PathBuf> {
    use overlaybd::config::{ImageConfig, LayerConfig};

    let image_config = ImageConfig {
        lowers: layers
            .iter()
            .map(|layer| match layer {
                OverlaybdLayerRef::Managed(layer) => managed_layer(layer),
                OverlaybdLayerRef::External(layer) => LayerConfig {
                    repo_blob_url: layer.repo_blob_url.clone(),
                    digest: layer.digest.clone(),
                    size: layer.size,
                    ..LayerConfig::default()
                },
            })
            .collect(),
        ..ImageConfig::default()
    };
    let parent = destination
        .parent()
        .ok_or_else(|| RepositoryError::Backend {
            message: format!(
                "volume image config '{}' has no parent",
                destination.display()
            ),
            source: None,
        })?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        RepositoryError::backend(
            format!("create volume image config dir '{}'", parent.display()),
            error,
        )
    })?;
    let bytes = serde_json::to_vec_pretty(&image_config)
        .map_err(|error| RepositoryError::backend("serialize volume image config", error))?;
    let temporary = destination.with_extension(format!("tmp-{}", Uuid::now_v7().simple()));
    if let Err(error) = tokio::fs::write(&temporary, bytes).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(RepositoryError::backend(
            format!(
                "write temporary volume image config '{}'",
                temporary.display()
            ),
            error,
        ));
    }
    if let Err(error) = tokio::fs::rename(&temporary, destination).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(RepositoryError::backend(
            format!("install volume image config '{}'", destination.display()),
            error,
        ));
    }
    Ok(destination.to_path_buf())
}

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
