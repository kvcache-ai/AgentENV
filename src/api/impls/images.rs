use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;
use tracing::warn;

use agentenv_http_server::apis::images::*;
use agentenv_http_server::models;

use crate::image::{ImageError, PulledPublication, IMAGE_TAG_PREFIX};

use super::ApiImpl;

#[async_trait]
impl Images<()> for ApiImpl {
    type Claims = super::Claims;

    async fn images_pulls_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::ImagePullRequest,
    ) -> Result<ImagesPullsPostResponse, ()> {
        let (tag, publication) = match self.image_resolver.pull_and_publish(&body.image).await {
            Ok(published) => published,
            Err(error @ ImageError::NotFound { .. }) => {
                return Ok(ImagesPullsPostResponse::Status404_NotFound(Self::error(
                    404,
                    error.to_string(),
                )));
            }
            Err(error) if error.is_user_error() => {
                return Ok(ImagesPullsPostResponse::Status400_BadRequest(Self::error(
                    400,
                    error.to_string(),
                )));
            }
            Err(error) => {
                warn!(error = %format_args!("{error:#}"), image = %body.image, "failed to pull and publish image");
                return Ok(ImagesPullsPostResponse::Status500_ServerError(Self::error(
                    500,
                    format!("pull and publish image '{}': {error:#}", body.image),
                )));
            }
        };
        let image = format!("{IMAGE_TAG_PREFIX}{tag}");
        Ok(match publication {
            PulledPublication::Published(digest) | PulledPublication::Unchanged(digest) => {
                ImagesPullsPostResponse::Status200_SuccessfullyPublishedTheImage(
                    models::PulledImage::new(image, digest),
                )
            }
            PulledPublication::OtherArchitecture(architecture) => {
                ImagesPullsPostResponse::Status409_Conflict(Self::error(
                    409,
                    format!("{image} points to an image for {architecture}; it was left unchanged"),
                ))
            }
        })
    }
}
