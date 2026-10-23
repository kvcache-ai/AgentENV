//! Image mounts are sandbox-scoped exclusive volumes over an image's layers,
//! so they share volume attachment, capture, fork, and restore.

use std::collections::HashMap;
use std::path::PathBuf;

use agentenv_http_server::models;
use overlaybd::config::ImageConfig;
use uuid::Uuid;

use super::volumes::error_response as volume_error_response;
use super::ApiImpl;
use crate::image::ImageError;
use crate::sandbox::{normalize_mount_path, UblkDeviceManager};
use crate::types::{ImageMount, OverlaybdLayerRef};
use crate::volume::{VolumeManager, VolumeMode};

/// Volumes created for image mounts, keyed by normalized guest path.
#[derive(Debug, Default)]
pub(super) struct CreatedImageMounts {
    pub volumes: HashMap<String, String>,
    pub mounts: HashMap<String, ImageMount>,
}

impl CreatedImageMounts {
    pub fn volume_ids(&self) -> Vec<String> {
        self.volumes.values().cloned().collect()
    }

    pub(super) fn insert(&mut self, path: String, volume_id: String, mount: ImageMount) {
        self.volumes.insert(path.clone(), volume_id);
        self.mounts.insert(path, mount);
    }
}

/// Creates one sandbox-scoped volume per requested image mount. When a mount
/// fails, the volumes already created for the request are deleted.
pub(super) async fn create_image_mount_volumes(
    api: &ApiImpl,
    requested: &[models::ImageMount],
) -> Result<CreatedImageMounts, models::Error> {
    let mut created = CreatedImageMounts::default();
    let result = create_image_mount_volumes_inner(api, requested, &mut created).await;
    if result.is_err() {
        delete_volumes(&api.volume_manager, &created.volume_ids()).await;
    }
    result.map(|()| created)
}

async fn create_image_mount_volumes_inner(
    api: &ApiImpl,
    requested: &[models::ImageMount],
    created: &mut CreatedImageMounts,
) -> Result<(), models::Error> {
    for mount in requested {
        let path = normalize_mount_path(PathBuf::from(mount.mount_path.trim()))
            .map_err(|error| ApiImpl::error(400, error.to_string()))?
            .to_string_lossy()
            .into_owned();
        if created.mounts.contains_key(&path) {
            return Err(ApiImpl::error(
                400,
                format!("duplicate image mount path: {path}"),
            ));
        }
        let image = mount.image.trim();
        let layers = image_mount_layers(api, image).await?;
        let size_mb = image_virtual_size_mb(api, image, &layers).await?;
        let volume = api
            .volume_manager
            .create_from_snapshot(
                format!("image-mount-{}", Uuid::now_v7().simple()),
                VolumeMode::Exclusive,
                size_mb,
                layers,
                true,
            )
            .await
            .map_err(|error| volume_error_response(error).1)?;
        created.insert(
            path,
            volume.id,
            ImageMount {
                image: image.to_owned(),
                read_only: mount.read_only.unwrap_or(false),
            },
        );
    }
    Ok(())
}

/// Layers an image mount references without copying them: a published image's
/// layers, or the remote layers of an OverlayBD-native image.
async fn image_mount_layers(
    api: &ApiImpl,
    image: &str,
) -> Result<Vec<OverlaybdLayerRef>, models::Error> {
    let resolve_error = |error: ImageError| {
        ApiImpl::error(
            if error.is_user_error() { 400 } else { 500 },
            format!("resolve image mount '{image}': {error:#}"),
        )
    };
    let resolver = api.image_resolver();
    if let Some(published) = resolver
        .published_image(image)
        .await
        .map_err(resolve_error)?
    {
        return Ok(published.layers);
    }
    let resolved = resolver.resolve(image).await.map_err(resolve_error)?;
    let config: ImageConfig = tokio::fs::read(&resolved.overlaybd_config_path)
        .await
        .map_err(anyhow::Error::from)
        .and_then(|bytes| serde_json::from_slice(&bytes).map_err(anyhow::Error::from))
        .map_err(|error| {
            ApiImpl::error(
                500,
                format!("read image mount '{image}' configuration: {error:#}"),
            )
        })?;
    // Converted layers live only in this node's cache; referencing them would
    // republish the whole image on every capture.
    if config.lowers.iter().any(|layer| !layer.file.is_empty()) {
        return Err(ApiImpl::error(
            400,
            format!(
                "image mount '{image}' needs node-local conversion; publish it with \
                 `aenv build --image` or create a volume from it"
            ),
        ));
    }
    crate::snapshot::store::store_volume_layers(
        api.snapshot_manager.repository().as_ref(),
        &resolved.overlaybd_config_path,
    )
    .await
    .map_err(|error| {
        ApiImpl::error(
            500,
            format!("reference image mount '{image}' layers: {error}"),
        )
    })
}

/// Use the same backing config and size resolver as the eventual block device.
async fn image_virtual_size_mb(
    api: &ApiImpl,
    image: &str,
    layers: &[OverlaybdLayerRef],
) -> Result<u64, models::Error> {
    let result = async {
        let directory = tempfile::tempdir()?;
        let image_config = crate::repository::materialize_volume_backing(
            api.snapshot_manager.repository().as_ref(),
            layers,
            &directory.path().join("image.json"),
        )
        .await?;
        let virtual_size = UblkDeviceManager::global()
            .resolve_overlaybd_virtual_size(&image_config)
            .await?;
        bytes_to_mib(virtual_size)
    }
    .await;
    result.map_err(|error| {
        ApiImpl::error(
            500,
            format!("resolve image mount '{image}' size: {error:#}"),
        )
    })
}

fn bytes_to_mib(virtual_size: u64) -> anyhow::Result<u64> {
    const MIB: u64 = 1024 * 1024;
    anyhow::ensure!(
        virtual_size.is_multiple_of(MIB),
        "image virtual size {virtual_size} is not MiB-aligned"
    );
    Ok(virtual_size / MIB)
}

/// Best-effort deletion of unreserved image mount volumes after a failed request.
async fn delete_volumes(manager: &VolumeManager, volume_ids: &[String]) {
    for volume_id in volume_ids {
        let _ = manager.delete(volume_id).await;
    }
}

pub(super) fn image_mounts_model(
    mounts: &HashMap<String, ImageMount>,
) -> Option<Vec<models::ImageMount>> {
    (!mounts.is_empty()).then(|| {
        mounts
            .iter()
            .map(|(path, mount)| models::ImageMount {
                image: mount.image.clone(),
                mount_path: path.clone(),
                read_only: Some(mount.read_only),
            })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_size_converts_to_mib() {
        for size_mb in [1, 8 * 1024, 64 * 1024, 128 * 1024] {
            assert_eq!(bytes_to_mib(size_mb * 1024 * 1024).unwrap(), size_mb);
        }
    }

    #[test]
    fn virtual_size_rejects_partial_mib() {
        for size in [1, 1024 * 1024 - 1, 128 * 1024 * 1024 * 1024 + 1] {
            assert!(bytes_to_mib(size)
                .unwrap_err()
                .to_string()
                .contains("not MiB-aligned"));
        }
    }

    #[test]
    fn model_lists_mounts_by_guest_path() {
        assert_eq!(image_mounts_model(&HashMap::new()), None);
        let mounts = HashMap::from([(
            "/mnt/models".to_owned(),
            ImageMount {
                image: "aenv/models:v1".to_owned(),
                read_only: false,
            },
        )]);
        let model = image_mounts_model(&mounts).unwrap();
        assert_eq!(model.len(), 1);
        assert_eq!(model[0].mount_path, "/mnt/models");
        assert_eq!(model[0].image, "aenv/models:v1");
        assert_eq!(model[0].read_only, Some(false));
    }
}
