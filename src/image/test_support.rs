use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::json;

use crate::digest::FileDigest;
use crate::repository::{
    BlobBody, BlobKey, LayerRole, Repository, RepositoryError, RepositoryResult,
};
use crate::types::{ManagedLayer, OverlaybdLayerRef};

pub(crate) fn write_built_image(root: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(root)?;
    let layer = root.join("base.overlaybd.commit");
    std::fs::write(&layer, b"base-layer")?;
    let descriptor = FileDigest::describe_blocking(&layer)?;
    let config = root.join("image.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&json!({
            "repoBlobUrl": "",
            "lowers": [{"file": layer, "digest": descriptor.sha256, "size": descriptor.size}],
            "upper": {},
            "resultFile": "",
        }))?,
    )?;
    Ok(config)
}

pub(crate) async fn store_layers(
    repository: &dyn Repository,
    image_config_path: &Path,
) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
    let config = overlaybd::config::load_image_config(image_config_path)
        .map_err(|error| RepositoryError::backend("load image config", error))?;
    let mut layers = Vec::with_capacity(config.lowers.len());
    for lower in config.lowers {
        let key = BlobKey::Layer {
            digest: lower.digest.clone(),
            role: LayerRole::Rootfs,
        };
        repository
            .put_blob(&key, BlobBody::File(Path::new(&lower.file)))
            .await?;
        layers.push(OverlaybdLayerRef::Managed(ManagedLayer {
            digest: lower.digest,
            size: lower.size,
            uuid: None,
        }));
    }
    Ok(layers)
}
