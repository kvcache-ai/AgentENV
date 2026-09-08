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
    /// Successfully listed team volumes
    Status200_SuccessfullyListedTeamVolumes {
        body: Vec<models::Volume>,
        x_next_token: Option<String>,
    },
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum VolumesPostResponse {
    /// Successfully created a new team volume
    Status201_SuccessfullyCreatedANewTeamVolume(models::Volume),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Conflict
    Status409_Conflict(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum VolumesVolumeIdDeleteResponse {
    /// Successfully deleted a team volume
    Status204_SuccessfullyDeletedATeamVolume,
    /// Authentication error
    Status401_AuthenticationError(models::Error),
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
    /// Successfully retrieved a team volume
    Status200_SuccessfullyRetrievedATeamVolume(models::Volume),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
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

    /// List team volumes.
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

    /// Create team volume.
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

    /// Delete team volume.
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

    /// Team volume.
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
