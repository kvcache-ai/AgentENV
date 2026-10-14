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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum ImagesTagsGetResponse {
    /// Image tag page
    Status200_ImageTagPage(models::ImageTagPage),
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
pub enum ImagesTagsTagDeleteResponse {
    /// Image tag deleted
    Status204_ImageTagDeleted,
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
pub enum ImagesTagsTagGetResponse {
    /// Image tag
    Status200_ImageTag(models::ImageTag),
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
pub enum ImagesTagsTagPutResponse {
    /// Image tagged
    Status200_ImageTagged(models::ImageTag),
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

    /// List image tags.
    ///
    /// ImagesTagsGet - GET /images/tags
    async fn images_tags_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        query_params: &models::ImagesTagsGetQueryParams,
    ) -> Result<ImagesTagsGetResponse, E>;

    /// Delete an image tag.
    ///
    /// ImagesTagsTagDelete - DELETE /images/tags/{tag}
    async fn images_tags_tag_delete(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::ImagesTagsTagDeletePathParams,
    ) -> Result<ImagesTagsTagDeleteResponse, E>;

    /// Get an image tag.
    ///
    /// ImagesTagsTagGet - GET /images/tags/{tag}
    async fn images_tags_tag_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::ImagesTagsTagGetPathParams,
    ) -> Result<ImagesTagsTagGetResponse, E>;

    /// Tag a published image.
    ///
    /// ImagesTagsTagPut - PUT /images/tags/{tag}
    async fn images_tags_tag_put(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::ImagesTagsTagPutPathParams,
        body: &models::ImageTagRequest,
    ) -> Result<ImagesTagsTagPutResponse, E>;
}
