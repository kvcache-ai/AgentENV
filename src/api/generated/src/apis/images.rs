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
pub enum ImagesBuildsBuildIdDeleteResponse {
    /// Builder released
    Status204_BuilderReleased,
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
pub enum ImagesBuildsBuildIdGetResponse {
    /// Image build status
    Status200_ImageBuildStatus(models::ImageBuildInfo),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum ImagesBuildsBuildIdLogsGetResponse {
    /// Image build logs
    Status200_ImageBuildLogs(Vec<models::BuildLogEntry>),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum ImagesBuildsPostResponse {
    /// Builder preparation accepted
    Status202_BuilderPreparationAccepted {
        body: models::ImageBuilder,
        x_agentenv_build_id: Option<String>,
    },
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Concurrent build limit reached
    Status429_ConcurrentBuildLimitReached(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum ImagesGetResponse {
    /// Published image page
    Status200_PublishedImagePage(models::ImagePage),
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
pub enum ImagesImageDigestDeleteResponse {
    /// Image deleted
    Status204_ImageDeleted,
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
pub enum ImagesImageDigestGetResponse {
    /// Published image details
    Status200_PublishedImageDetails(models::ImageDetails),
    /// Bad request
    Status400_BadRequest(models::Error),
    /// Authentication error
    Status401_AuthenticationError(models::Error),
    /// Not found
    Status404_NotFound(models::Error),
    /// Server error
    Status500_ServerError(models::Error),
}

/// Images
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait Images<E: std::fmt::Debug + Send + Sync + 'static = ()>: super::ErrorHandler<E> {
    type Claims;

    /// Release an image build.
    ///
    /// ImagesBuildsBuildIdDelete - DELETE /images/builds/{buildID}
    async fn images_builds_build_id_delete(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::ImagesBuildsBuildIdDeletePathParams,
    ) -> Result<ImagesBuildsBuildIdDeleteResponse, E>;

    /// Get image build status.
    ///
    /// ImagesBuildsBuildIdGet - GET /images/builds/{buildID}
    async fn images_builds_build_id_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::ImagesBuildsBuildIdGetPathParams,
    ) -> Result<ImagesBuildsBuildIdGetResponse, E>;

    /// Get image build logs.
    ///
    /// ImagesBuildsBuildIdLogsGet - GET /images/builds/{buildID}/logs
    async fn images_builds_build_id_logs_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::ImagesBuildsBuildIdLogsGetPathParams,
        query_params: &models::ImagesBuildsBuildIdLogsGetQueryParams,
    ) -> Result<ImagesBuildsBuildIdLogsGetResponse, E>;

    /// Prepare an image build.
    ///
    /// ImagesBuildsPost - POST /images/builds
    async fn images_builds_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        body: &models::ImageBuildRequest,
    ) -> Result<ImagesBuildsPostResponse, E>;

    /// List published images.
    ///
    /// ImagesGet - GET /images
    async fn images_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::ImagesGetQueryParams,
    ) -> Result<ImagesGetResponse, E>;

    /// Delete a published image.
    ///
    /// ImagesImageDigestDelete - DELETE /images/{imageDigest}
    async fn images_image_digest_delete(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::ImagesImageDigestDeletePathParams,
    ) -> Result<ImagesImageDigestDeleteResponse, E>;

    /// Inspect a published image.
    ///
    /// ImagesImageDigestGet - GET /images/{imageDigest}
    async fn images_image_digest_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::ImagesImageDigestGetPathParams,
    ) -> Result<ImagesImageDigestGetResponse, E>;
}
