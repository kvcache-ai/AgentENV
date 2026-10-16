//! Published image descriptions and image tags, stored as repository
//! documents so every backend follows the same rules.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::PublishedImage;
use crate::repository::{
    DocumentCollection, DocumentKey, Repository, RepositoryError, RepositoryResult,
};
use crate::types::{now_unix_ms, ImageName, ImageTag};

/// Where an image tag points, and since when.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageTagRecord {
    pub image_digest: String,
    pub created_at_unix_ms: i64,
}

/// Typed access to published image descriptions and image tags.
#[async_trait]
pub trait ImageCatalog {
    /// Publish only after all referenced layers are durable. The digest binds
    /// the complete description; repeated publication is idempotent.
    async fn put_image(&self, image: &PublishedImage) -> RepositoryResult<String>;

    async fn get_image(&self, digest: &str) -> RepositoryResult<Option<PublishedImage>>;

    /// Points `tag` at an image digest, creating or moving it. Callers check
    /// that the image is published under the tag's name.
    async fn put_image_tag(&self, tag: &ImageTag, digest: &str) -> RepositoryResult<()>;

    /// The image digest `tag` points to; `None` when the tag does not exist.
    async fn get_image_tag(&self, tag: &ImageTag) -> RepositoryResult<Option<String>>;

    /// Every tag of `name` and where it points, in tag order.
    async fn list_image_tags(
        &self,
        name: &ImageName,
    ) -> RepositoryResult<Vec<(String, ImageTagRecord)>>;

    /// Removes the tag only; the image it points to stays published.
    async fn delete_image_tag(&self, tag: &ImageTag) -> RepositoryResult<()>;
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
        let record = ImageTagRecord {
            image_digest: digest.to_owned(),
            created_at_unix_ms: now_unix_ms(),
        };
        let bytes = serde_json::to_vec(&record)
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
        Ok(Some(decode_tag(tag, &bytes)?.image_digest))
    }

    async fn list_image_tags(
        &self,
        name: &ImageName,
    ) -> RepositoryResult<Vec<(String, ImageTagRecord)>> {
        self.read_documents(DocumentCollection::ImageTags(name.clone()))
            .await?
            .into_iter()
            .map(|(id, bytes)| {
                let tag = ImageTag::new(name, &id)
                    .map_err(|error| RepositoryError::backend("list image tags", error))?;
                Ok((id, decode_tag(&tag, &bytes)?))
            })
            .collect()
    }

    async fn delete_image_tag(&self, tag: &ImageTag) -> RepositoryResult<()> {
        self.delete_document(DocumentKey::ImageTag(tag.clone()))
            .await
    }
}

fn decode_tag(tag: &ImageTag, bytes: &[u8]) -> RepositoryResult<ImageTagRecord> {
    serde_json::from_slice::<ImageTagRecord>(bytes)
        .ok()
        .filter(|record| crate::digest::is_sha256_digest(&record.image_digest))
        .ok_or_else(|| {
            RepositoryError::backend(
                "verify image tag",
                anyhow::anyhow!("image tag '{tag}' does not hold an image digest"),
            )
        })
}
