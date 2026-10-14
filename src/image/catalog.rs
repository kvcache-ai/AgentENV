//! Published image descriptions and image tags, stored as repository
//! documents so every backend follows the same rules.

use async_trait::async_trait;

use super::PublishedImage;
use crate::repository::{DocumentKey, Repository, RepositoryError, RepositoryResult};
use crate::types::ImageTag;

/// Typed access to published image descriptions and image tags.
#[async_trait]
pub trait ImageCatalog {
    /// Publish only after all referenced layers are durable. The digest binds
    /// the complete description; repeated publication is idempotent.
    async fn put_image(&self, image: &PublishedImage) -> RepositoryResult<String>;

    async fn get_image(&self, digest: &str) -> RepositoryResult<Option<PublishedImage>>;

    /// Points `tag` at an image digest, moving an existing tag. Callers check
    /// that the image is published.
    async fn put_image_tag(&self, tag: &ImageTag, digest: &str) -> RepositoryResult<()>;

    /// The image digest `tag` points to; `None` when the tag does not exist.
    async fn get_image_tag(&self, tag: &ImageTag) -> RepositoryResult<Option<String>>;
}

#[async_trait]
impl<R: Repository + ?Sized> ImageCatalog for R {
    async fn put_image(&self, image: &PublishedImage) -> RepositoryResult<String> {
        let (digest, bytes) = image
            .encode()
            .map_err(|error| RepositoryError::backend("encode image", error))?;
        self.put_document(DocumentKey::image(&digest)?, bytes)
            .await?;
        Ok(digest)
    }

    async fn get_image(&self, digest: &str) -> RepositoryResult<Option<PublishedImage>> {
        let Some(bytes) = self.get_document(DocumentKey::image(digest)?).await? else {
            return Ok(None);
        };
        PublishedImage::decode(digest, &bytes)
            .map(Some)
            .map_err(|error| RepositoryError::backend("verify image description", error))
    }

    async fn put_image_tag(&self, tag: &ImageTag, digest: &str) -> RepositoryResult<()> {
        DocumentKey::image(digest)?;
        let bytes = serde_json::to_vec(digest)
            .map_err(|error| RepositoryError::backend("encode image tag", error))?;
        self.put_document(DocumentKey::ImageTag(tag.clone()), bytes)
            .await
    }

    async fn get_image_tag(&self, tag: &ImageTag) -> RepositoryResult<Option<String>> {
        let Some(bytes) = self
            .get_document(DocumentKey::ImageTag(tag.clone()))
            .await?
        else {
            return Ok(None);
        };
        let digest = serde_json::from_slice::<String>(&bytes)
            .ok()
            .filter(|digest| crate::digest::is_sha256_digest(digest))
            .ok_or_else(|| {
                RepositoryError::backend(
                    "verify image tag",
                    anyhow::anyhow!("image tag '{tag}' does not hold an image digest"),
                )
            })?;
        Ok(Some(digest))
    }
}
