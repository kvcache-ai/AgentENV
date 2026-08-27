use async_trait::async_trait;
use axum::extract::*;
use axum_extra::extract::CookieJar;
use bytes::Bytes;
use headers::Host;
use http::Method;
use serde::{Deserialize, Serialize};

use crate::{models, types::*};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum VolumesGetResponse {
    /// Volumes returned successfully
    Status200_VolumesReturnedSuccessfully {
        body: Vec<models::Volume>,
        x_next_token: Option<String>,
    },
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum VolumesPostResponse {
    /// Volume created successfully
    Status201_VolumeCreatedSuccessfully(models::Volume),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum VolumesVolumeIdDeleteResponse {
    /// Volume deleted successfully
    Status204_VolumeDeletedSuccessfully,
    /// Not found
    Status404_NotFound(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum VolumesVolumeIdGetResponse {
    /// Volume returned successfully
    Status200_VolumeReturnedSuccessfully(models::Volume),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

/// Volumes
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait Volumes<E: std::fmt::Debug + Send + Sync + 'static = ()>: super::ErrorHandler<E> {
    type Claims;

    /// List volumes.
    ///
    /// VolumesGet - GET /volumes
    async fn volumes_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::VolumesGetQueryParams,
    ) -> Result<VolumesGetResponse, E>;

    /// Create a volume.
    ///
    /// VolumesPost - POST /volumes
    async fn volumes_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        body: &models::NewVolume,
    ) -> Result<VolumesPostResponse, E>;

    /// Delete a volume.
    ///
    /// VolumesVolumeIdDelete - DELETE /volumes/{volumeID}
    async fn volumes_volume_id_delete(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::VolumesVolumeIdDeletePathParams,
    ) -> Result<VolumesVolumeIdDeleteResponse, E>;

    /// Get a volume.
    ///
    /// VolumesVolumeIdGet - GET /volumes/{volumeID}
    async fn volumes_volume_id_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::VolumesVolumeIdGetPathParams,
    ) -> Result<VolumesVolumeIdGetResponse, E>;
}
