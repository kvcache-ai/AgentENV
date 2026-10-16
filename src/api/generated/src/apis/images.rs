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
pub enum ImagesNameTagsGetResponse {
    /// Image tags
    Status200_ImageTags(Vec<models::ImageTag>),
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
pub enum ImagesTagsDeleteResponse {
    /// Tags deleted
    Status204_TagsDeleted,
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
pub enum ImagesTagsPostResponse {
    /// Tags assigned
    Status201_TagsAssigned(models::AssignedImageTags),
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

    /// List image tags.
    ///
    /// ImagesNameTagsGet - GET /images/{name}/tags
    async fn images_name_tags_get(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        path_params: &models::ImagesNameTagsGetPathParams,
    ) -> Result<ImagesNameTagsGetResponse, E>;

    /// Delete image tags.
    ///
    /// ImagesTagsDelete - DELETE /images/tags
    async fn images_tags_delete(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        body: &models::DeleteImageTagsRequest,
    ) -> Result<ImagesTagsDeleteResponse, E>;

    /// Assign image tags.
    ///
    /// ImagesTagsPost - POST /images/tags
    async fn images_tags_post(
        &self,

        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        claims: &Self::Claims,
        body: &models::AssignImageTagsRequest,
    ) -> Result<ImagesTagsPostResponse, E>;
}
