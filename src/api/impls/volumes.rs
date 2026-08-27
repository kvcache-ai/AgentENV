use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use agentenv_http_server::apis::volumes::*;
use agentenv_http_server::models;

use crate::image::ImageResolver;
use crate::sandbox::{normalize_mount_path, ExtraDrive};
use crate::volume::{
    VolumeError, VolumeManager, VolumeMode, VolumeRecord, VolumeStatus, DEFAULT_VOLUME_PAGE_SIZE,
    DEFAULT_VOLUME_SIZE_MB,
};

use super::ApiImpl;

pub(super) async fn resolve_volume_mounts(
    manager: &VolumeManager,
    image_resolver: &ImageResolver,
    mounts: &HashMap<String, String>,
    owner: &str,
) -> Result<(Vec<ExtraDrive>, HashMap<String, String>), models::Error> {
    let result = resolve_volume_mounts_inner(manager, image_resolver, mounts, owner).await;
    if result.is_err() {
        if let Err(error) = manager.release_owner(owner).await {
            tracing::warn!(%owner, %error, "failed to clean up volume reservations after mount resolution failed");
        }
    }
    result
}

async fn resolve_volume_mounts_inner(
    manager: &VolumeManager,
    image_resolver: &ImageResolver,
    mounts: &HashMap<String, String>,
    owner: &str,
) -> Result<(Vec<ExtraDrive>, HashMap<String, String>), models::Error> {
    let mut drives = Vec::with_capacity(mounts.len());
    let limits = manager.limits();
    if mounts.len() > limits.max_mounts {
        return Err(error_response(VolumeError::TooManyMountedVolumes {
            max_count: limits.max_mounts,
        })
        .1);
    }
    let mut volume_ids = HashSet::with_capacity(mounts.len());
    let mut normalized_mounts = HashMap::with_capacity(mounts.len());

    let mut mounts = mounts.iter().collect::<Vec<_>>();
    mounts.sort_unstable_by_key(|(mount_path, _)| *mount_path);
    for (mount_path, reference) in mounts {
        let mount_path = normalize_mount_path(PathBuf::from(mount_path))
            .map_err(|error| ApiImpl::error(400, error.to_string()))?;
        if normalized_mounts.keys().any(|existing| {
            let existing = PathBuf::from(existing);
            existing.starts_with(&mount_path) || mount_path.starts_with(existing)
        }) {
            return Err(ApiImpl::error(
                400,
                format!(
                    "volume mount path overlaps another mount: {}",
                    mount_path.display()
                ),
            ));
        }
        let volume = manager
            .get(reference)
            .await
            .map_err(|error| error_response(error).1)?;
        if volume.size_mb > limits.max_size_mb {
            return Err(error_response(VolumeError::SizeLimitExceeded {
                max_size_mb: limits.max_size_mb,
            })
            .1);
        }
        if !volume_ids.insert(volume.id.clone()) {
            return Err(ApiImpl::error(
                400,
                format!("volume {} is mounted more than once", volume.id),
            ));
        }
        manager
            .reserve(&volume.id, owner)
            .await
            .map_err(|error| error_response(error).1)?;

        let mut source_record = manager
            .materialize_backing(&volume.id)
            .await
            .map_err(|error| error_response(error).1)?;
        while source_record.source == "volume" && source_record.backing_image_config.is_none() {
            let Some(parent_id) = source_record.parent_volume_id.as_deref() else {
                break;
            };
            let parent = manager
                .materialize_backing(parent_id)
                .await
                .map_err(|error| error_response(error).1)?;
            source_record = parent;
        }
        let image_config_path = match source_record.backing_image_config.clone() {
            Some(path) => path,
            None if source_record.source == "empty" || source_record.source == "volume" => {
                return Err(ApiImpl::error(
                    500,
                    format!("volume {} has no backing image", volume.id),
                ));
            }
            None => {
                let resolved = image_resolver
                    .resolve(&source_record.source)
                    .await
                    .map_err(|error| {
                        ApiImpl::error(
                            if error.is_user_error() { 400 } else { 500 },
                            format!("resolve volume {} source: {error:#}", volume.id),
                        )
                    })?;
                manager
                    .ensure_backing_config(&volume.id, &resolved.overlaybd_config_path)
                    .await
                    .map_err(|error| {
                        ApiImpl::error(500, format!("persist volume {} source: {error}", volume.id))
                    })?
            }
        };

        let normalized_path = mount_path.to_string_lossy().into_owned();
        let drive = ExtraDrive::try_new_overlaybd_with_mount_path(
            volume.id.clone(),
            image_config_path,
            volume.mode == VolumeMode::ReadOnly,
            mount_path,
            None::<PathBuf>,
        )
        .and_then(|drive| {
            drive.try_with_virtual_size(
                volume
                    .size_mb
                    .checked_mul(1024 * 1024)
                    .ok_or_else(|| anyhow::anyhow!("volume size is too large"))?,
            )
        })
        .map_err(|error| ApiImpl::error(400, error.to_string()))?;
        let snapshot_output_dir = (volume.mode == VolumeMode::Exclusive)
            .then(|| manager.data_dir(&volume.id))
            .flatten();
        drives.push(drive.with_snapshot_output_dir(snapshot_output_dir));
        normalized_mounts.insert(normalized_path, volume.id);
    }

    Ok((drives, normalized_mounts))
}

fn to_model(record: VolumeRecord) -> models::Volume {
    let status = match record.status {
        VolumeStatus::Ready => "ready",
        VolumeStatus::Uploading => "uploading",
        VolumeStatus::Failed => "failed",
    };
    let mut model = models::Volume::new(record.id, record.name, record.size_mb, status.to_owned());
    model.mode = Some(match record.mode {
        VolumeMode::ReadOnly => "ro".to_owned(),
        VolumeMode::Exclusive => "exclusive".to_owned(),
    });
    model
}

pub(super) fn error_response(error: VolumeError) -> (i32, models::Error) {
    let code = match error {
        VolumeError::InvalidName
        | VolumeError::MultipleSources
        | VolumeError::SourceNotFound(_)
        | VolumeError::InvalidSize
        | VolumeError::InvalidNextToken
        | VolumeError::InvalidPageLimit
        | VolumeError::SizeLimitExceeded { .. }
        | VolumeError::SizeMismatch => 400,
        VolumeError::TooManyMountedVolumes { .. } => 400,
        VolumeError::NotFound(_) => 404,
        VolumeError::NameConflict(_)
        | VolumeError::Reserved(_)
        | VolumeError::Uploading(_)
        | VolumeError::Failed(_) => 409,
        VolumeError::Storage(_) => 500,
    };
    (code, ApiImpl::error(code, error.to_string()))
}

#[async_trait]
impl Volumes<()> for ApiImpl {
    type Claims = super::Claims;

    async fn volumes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::VolumesGetQueryParams,
    ) -> Result<VolumesGetResponse, ()> {
        match self
            .volume_manager
            .list_page(
                query_params.next_token.as_deref(),
                query_params
                    .limit
                    .map(|limit| limit as usize)
                    .unwrap_or(DEFAULT_VOLUME_PAGE_SIZE),
            )
            .await
        {
            Ok(page) => Ok(VolumesGetResponse::Status200_VolumesReturnedSuccessfully {
                body: page.records.into_iter().map(to_model).collect(),
                x_next_token: page.next_token,
            }),
            Err(error @ (VolumeError::InvalidNextToken | VolumeError::InvalidPageLimit)) => Ok(
                VolumesGetResponse::Status400_BadRequest(error_response(error).1),
            ),
            Err(error) => Ok(VolumesGetResponse::Status500_ServerError(ApiImpl::error(
                500,
                error.to_string(),
            ))),
        }
    }

    async fn volumes_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::NewVolume,
    ) -> Result<VolumesPostResponse, ()> {
        let mode = match body.mode.as_deref() {
            None | Some("exclusive") => VolumeMode::Exclusive,
            Some("ro") => VolumeMode::ReadOnly,
            Some(mode) => {
                return Ok(VolumesPostResponse::Status400_BadRequest(ApiImpl::error(
                    400,
                    format!("unsupported volume mode: {mode}"),
                )))
            }
        };
        let resolved_image = match (body.from_volume.is_none(), body.image.as_deref()) {
            (true, Some(image)) => match self.image_resolver.resolve(image).await {
                Ok(resolved) => Some(resolved.overlaybd_config_path),
                Err(error) => {
                    let code = if error.is_user_error() { 400 } else { 500 };
                    return Ok(match code {
                        400 => VolumesPostResponse::Status400_BadRequest(ApiImpl::error(
                            code,
                            format!("resolve volume image: {error:#}"),
                        )),
                        _ => VolumesPostResponse::Status500_ServerError(ApiImpl::error(
                            code,
                            format!("resolve volume image: {error:#}"),
                        )),
                    });
                }
            },
            _ => None,
        };

        match self
            .volume_manager
            .create(
                body.name.clone(),
                mode,
                body.from_volume.clone(),
                body.image.clone(),
                body.size_mb.unwrap_or(DEFAULT_VOLUME_SIZE_MB),
            )
            .await
        {
            Ok(record) => {
                if let Some(config) = resolved_image {
                    if let Err(error) = self
                        .volume_manager
                        .ensure_backing_config(&record.id, &config)
                        .await
                    {
                        let _ = self.volume_manager.delete(&record.id).await;
                        let (code, error) = error_response(error);
                        return Ok(match code {
                            400 => VolumesPostResponse::Status400_BadRequest(error),
                            409 => VolumesPostResponse::Status409_Conflict(error),
                            _ => VolumesPostResponse::Status500_ServerError(error),
                        });
                    }
                }
                Ok(VolumesPostResponse::Status201_VolumeCreatedSuccessfully(
                    to_model(record),
                ))
            }
            Err(error) => {
                let (code, error) = error_response(error);
                match code {
                    400 => Ok(VolumesPostResponse::Status400_BadRequest(error)),
                    409 => Ok(VolumesPostResponse::Status409_Conflict(error)),
                    _ => Ok(VolumesPostResponse::Status500_ServerError(error)),
                }
            }
        }
    }

    async fn volumes_volume_id_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::VolumesVolumeIdDeletePathParams,
    ) -> Result<VolumesVolumeIdDeleteResponse, ()> {
        match self.volume_manager.delete(&path_params.volume_id).await {
            Ok(()) => Ok(VolumesVolumeIdDeleteResponse::Status204_VolumeDeletedSuccessfully),
            Err(error) => {
                let (code, error) = error_response(error);
                match code {
                    404 => Ok(VolumesVolumeIdDeleteResponse::Status404_NotFound(error)),
                    409 => Ok(VolumesVolumeIdDeleteResponse::Status409_Conflict(error)),
                    _ => Ok(VolumesVolumeIdDeleteResponse::Status500_ServerError(error)),
                }
            }
        }
    }

    async fn volumes_volume_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::VolumesVolumeIdGetPathParams,
    ) -> Result<VolumesVolumeIdGetResponse, ()> {
        match self.volume_manager.get(&path_params.volume_id).await {
            Ok(record) => Ok(
                VolumesVolumeIdGetResponse::Status200_VolumeReturnedSuccessfully(to_model(record)),
            ),
            Err(error) => {
                let (code, error) = error_response(error);
                match code {
                    404 => Ok(VolumesVolumeIdGetResponse::Status404_NotFound(error)),
                    _ => Ok(VolumesVolumeIdGetResponse::Status500_ServerError(error)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use crate::cfg::AppConfig;

    #[tokio::test]
    async fn resolves_empty_volume_mount_to_extra_drive_and_reserves_it() {
        let directory = tempfile::tempdir().unwrap();
        let manager = VolumeManager::open(directory.path().join("catalog"))
            .await
            .unwrap();
        let record = manager
            .create("my-data".to_owned(), VolumeMode::Exclusive, None, None, 16)
            .await
            .unwrap();
        let resolver = ImageResolver::new(&AppConfig {
            deps_path: directory.path().join("deps"),
            ..AppConfig::default()
        });
        let mounts = HashMap::from([(String::from("/mnt/data"), record.id.clone())]);

        let (drives, normalized) = resolve_volume_mounts(&manager, &resolver, &mounts, "pending")
            .await
            .expect("volume mount should resolve");
        assert_eq!(drives.len(), 1);
        assert_eq!(drives[0].drive_id(), record.id);
        assert_eq!(drives[0].mount_path(), Path::new("/mnt/data"));
        assert!(!drives[0].read_only());
        assert_eq!(normalized.get("/mnt/data"), Some(&record.id));
        assert_eq!(
            manager
                .get(&record.id)
                .await
                .unwrap()
                .reserved_by_sandbox_id,
            Some("pending".to_owned())
        );
    }

    #[tokio::test]
    async fn resolves_volume_drives_in_mount_path_order() {
        let directory = tempfile::tempdir().unwrap();
        let manager = VolumeManager::open(directory.path().join("catalog"))
            .await
            .unwrap();
        let first = manager
            .create("first".to_owned(), VolumeMode::Exclusive, None, None, 16)
            .await
            .unwrap();
        let second = manager
            .create("second".to_owned(), VolumeMode::Exclusive, None, None, 16)
            .await
            .unwrap();
        let resolver = ImageResolver::new(&AppConfig {
            deps_path: directory.path().join("deps"),
            ..AppConfig::default()
        });
        let mounts = HashMap::from([
            (String::from("/mnt/z-last"), first.id),
            (String::from("/mnt/a-first"), second.id),
        ]);

        let (drives, _) = resolve_volume_mounts(&manager, &resolver, &mounts, "pending")
            .await
            .expect("volume mounts should resolve");

        assert_eq!(drives[0].mount_path(), Path::new("/mnt/a-first"));
        assert_eq!(drives[1].mount_path(), Path::new("/mnt/z-last"));
    }
}
