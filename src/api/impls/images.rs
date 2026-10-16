//! Image tags. A tag belongs to an image name and points to an image built
//! under that name: imageOnly builds create tags, and assignment copies an
//! existing tag of the same name.

use anyhow::{ensure, Result};
use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;
use tracing::info;

use agentenv_http_server::apis::images::*;
use agentenv_http_server::models;

use super::template::datetime_from_unix_ms;
use super::ApiImpl;
use crate::image::ImageCatalog;
use crate::repository::RepositoryResult;
use crate::types::{ImageName, ImageTag};

/// The tags `tags` of `name`, which must not be empty.
fn parse_tags(name: &ImageName, tags: &[String]) -> Result<Vec<ImageTag>> {
    ensure!(!tags.is_empty(), "tags must not be empty");
    tags.iter().map(|tag| ImageTag::new(name, tag)).collect()
}

#[async_trait]
impl Images<()> for ApiImpl {
    type Claims = super::Claims;

    async fn images_name_tags_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::ImagesNameTagsGetPathParams,
    ) -> Result<ImagesNameTagsGetResponse, ()> {
        use ImagesNameTagsGetResponse::*;
        let name = match ImageName::parse(&path_params.name) {
            Ok(name) => name,
            Err(error) => return Ok(Status400_BadRequest(Self::error(400, error.to_string()))),
        };
        Ok(
            match self
                .snapshot_manager
                .repository()
                .list_image_tags(&name)
                .await
            {
                Ok(tags) => Status200_ImageTags(
                    tags.into_iter()
                        .map(|(tag, record)| {
                            models::ImageTag::new(
                                tag,
                                record.image_digest,
                                datetime_from_unix_ms(record.created_at_unix_ms),
                            )
                        })
                        .collect(),
                ),
                Err(error) => Status500_ServerError(Self::internal_error(&error)),
            },
        )
    }

    async fn images_tags_post(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::AssignImageTagsRequest,
    ) -> Result<ImagesTagsPostResponse, ()> {
        use ImagesTagsPostResponse::*;
        let parsed = ImageTag::parse(&body.target)
            .and_then(|target| Ok((parse_tags(target.name(), &body.tags)?, target)));
        let (tags, target) = match parsed {
            Ok(parsed) => parsed,
            Err(error) => return Ok(Status400_BadRequest(Self::error(400, error.to_string()))),
        };
        let repository = self.snapshot_manager.repository();
        let assigned: RepositoryResult<Option<String>> = async {
            let Some(digest) = repository.get_image_tag(&target).await? else {
                return Ok(None);
            };
            if repository.get_image(&digest).await?.is_none() {
                return Ok(None);
            }
            for tag in &tags {
                repository.put_image_tag(tag, &digest).await?;
            }
            Ok(Some(digest))
        }
        .await;
        Ok(match assigned {
            Ok(Some(digest)) => {
                let tags: Vec<String> = tags.iter().map(|tag| tag.tag().to_owned()).collect();
                info!(%target, ?tags, image_digest = %digest, "assigned image tags");
                Status201_TagsAssigned(models::AssignedImageTags::new(tags, digest))
            }
            Ok(None) => Status404_NotFound(Self::error(
                404,
                format!("image tag '{target}' does not point to a published image"),
            )),
            Err(error) => Status500_ServerError(Self::internal_error(&error)),
        })
    }

    async fn images_tags_delete(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        body: &models::DeleteImageTagsRequest,
    ) -> Result<ImagesTagsDeleteResponse, ()> {
        use ImagesTagsDeleteResponse::*;
        let tags = match ImageName::parse(&body.name).and_then(|name| parse_tags(&name, &body.tags))
        {
            Ok(tags) => tags,
            Err(error) => return Ok(Status400_BadRequest(Self::error(400, error.to_string()))),
        };
        let repository = self.snapshot_manager.repository();
        for tag in &tags {
            if let Err(error) = repository.delete_image_tag(tag).await {
                return Ok(Status500_ServerError(Self::internal_error(&error)));
            }
        }
        info!(image = %body.name, tags = ?body.tags, "deleted image tags");
        Ok(Status204_TagsDeleted)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use axum::body::Body;
    use http::{Request, StatusCode};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::api::impls::image_build::tests::{publish_test_image, test_api};
    use crate::image::ImageCatalog;
    use crate::types::ImageTag;
    use crate::volume::VolumeLimits;

    #[tokio::test]
    async fn tags_are_assigned_listed_and_deleted_within_an_image_name() -> Result<()> {
        let (_root, api, _) = test_api(VolumeLimits::default()).await?;
        let repository = api.snapshot_manager.repository();
        let digest = publish_test_image(&api).await?;
        repository
            .put_image_tag(&ImageTag::parse("web:v1")?, &digest)
            .await?;
        let other = publish_test_image(&api).await?;
        let app = crate::api::server::new(Arc::new(api.clone()));
        let send = |method: http::Method, path: &str, body: Option<Value>| {
            let request = Request::builder()
                .method(method)
                .uri(path)
                .header("host", "localhost")
                .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                .header("content-type", "application/json")
                .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
                .unwrap();
            let app = app.clone();
            async move {
                let response = app.oneshot(request).await?;
                let status = response.status();
                let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
                let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                anyhow::Ok((status, body))
            }
        };

        let (status, assigned) = send(
            http::Method::POST,
            "/images/tags",
            Some(json!({"target": "web:v1", "tags": ["prod", "latest"]})),
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            assigned,
            json!({"tags": ["prod", "latest"], "imageDigest": digest})
        );
        let (status, listed) = send(http::Method::GET, "/images/web/tags", None).await?;
        assert_eq!(status, StatusCode::OK);
        let listed = listed.as_array().unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|tag| (
                    tag["tag"].as_str().unwrap(),
                    tag["imageDigest"].as_str().unwrap()
                ))
                .collect::<Vec<_>>(),
            [
                ("latest", digest.as_str()),
                ("prod", digest.as_str()),
                ("v1", digest.as_str())
            ]
        );
        assert!(listed.iter().all(|tag| tag["createdAt"].is_string()));
        // Tags of another name never move this name's tags.
        repository
            .put_image_tag(&ImageTag::parse("api:v1")?, &other)
            .await?;
        let (status, _) = send(http::Method::GET, "/images/api/tags", None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            repository
                .get_image_tag(&ImageTag::parse("web:prod")?)
                .await?,
            Some(digest.clone())
        );

        for (body, expected) in [
            (
                json!({"target": "web:missing", "tags": ["x"]}),
                StatusCode::NOT_FOUND,
            ),
            (
                json!({"target": "Web:v1", "tags": ["x"]}),
                StatusCode::BAD_REQUEST,
            ),
            (
                json!({"target": "web:v1", "tags": []}),
                StatusCode::BAD_REQUEST,
            ),
            (
                json!({"target": "web:v1", "tags": [".x"]}),
                StatusCode::BAD_REQUEST,
            ),
            (
                json!({"target": "web:v1", "tags": ["api:v1"]}),
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let (status, _) = send(http::Method::POST, "/images/tags", Some(body.clone())).await?;
            assert_eq!(status, expected, "{body}");
        }
        let (status, _) = send(http::Method::GET, "/images/Web/tags", None).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _) = send(
            http::Method::DELETE,
            "/images/tags",
            Some(json!({"name": "web", "tags": ["prod", "never-existed"]})),
        )
        .await?;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(repository
            .get_image_tag(&ImageTag::parse("web:prod")?)
            .await?
            .is_none());
        // Deleting a tag keeps the image it pointed to.
        assert!(repository.get_image(&digest).await?.is_some());
        let (status, _) = send(
            http::Method::DELETE,
            "/images/tags",
            Some(json!({"name": "web", "tags": []})),
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        Ok(())
    }
}
