use std::path::Path;

use anyhow::{ensure, Context, Result};
use overlaybd::config::{resolve_image_config_local_paths, ImageConfig};
use overlaybd::layer::LayerFile;
use overlaybd::layer_metadata::resolve_local_layer_path;

use super::{LayerChain, LayerRef, LayerSource, ManagedLayerRef};
use crate::digest::FileDigest;

pub(crate) async fn load_layer_chain_from_config(
    path: &Path,
    managed_repo_blob_url: Option<&str>,
) -> Result<LayerChain> {
    let mut image: ImageConfig = serde_json::from_slice(&tokio::fs::read(path).await?)?;
    resolve_image_config_local_paths(path, &mut image);
    if image.acceleration_layer {
        // The last entry contains a prefetch trace, outside the content layer chain.
        image.lowers.pop();
    }
    let mut layers = Vec::with_capacity(image.lowers.len());
    for lower in &image.lowers {
        let url = lower.effective_repo_blob_url(&image.repo_blob_url);
        if (lower.file.is_empty() || !lower.repo_blob_url.is_empty())
            && !url.is_empty()
            && !lower.digest.is_empty()
        {
            let reference = if managed_repo_blob_url
                .is_some_and(|managed| managed.trim_end_matches('/') == url.trim_end_matches('/'))
            {
                LayerRef::Managed(ManagedLayerRef {
                    digest: lower.digest.clone(),
                    size: lower.size,
                    uuid: (!lower.uuid.is_empty()).then(|| lower.uuid.clone()),
                })
            } else {
                LayerRef::External(crate::layer::ExternalLayer {
                    digest: lower.digest.clone(),
                    size: lower.size,
                    repo_blob_url: url.to_owned(),
                })
            };
            layers.push(LayerSource::Referenced {
                reference,
                local_file: None,
            });
            continue;
        }
        let source = resolve_local_layer_path(lower)
            .context("layer has no local file or remote reference")?;
        let source = tokio::fs::canonicalize(source).await?;
        let descriptor = if lower.digest.is_empty() {
            None
        } else {
            let actual = FileDigest::describe(&source).await?;
            ensure!(
                actual.sha256 == lower.digest && (lower.size == 0 || lower.size == actual.size),
                "layer descriptor does not match {}",
                source.display()
            );
            Some(overlaybd::LayerDescriptor {
                digest: actual.sha256,
                size: actual.size,
            })
        };
        let file = LayerFile::from_path(source, descriptor).await?;
        if !lower.uuid.is_empty() {
            ensure!(
                lower.uuid.parse::<uuid::Uuid>()? == file.uuid,
                "layer UUID does not match {}",
                file.path.display()
            );
        }
        layers.push(LayerSource::File(file));
    }
    Ok(LayerChain {
        layers,
        managed_repo_blob_url: managed_repo_blob_url.map(str::to_owned),
    })
}
