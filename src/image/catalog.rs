//! Published image descriptions and image tags, stored as repository
//! documents so every backend follows the same rules.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::PublishedImage;
use crate::repository::{DocumentKey, Repository, RepositoryError, RepositoryResult};
use crate::types::{now_unix_ms, ImageTag};

/// Where an image tag points, and since when.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageTagRecord {
    pub image_digest: String,
    pub created_at_unix_ms: i64,
    /// For a pulled image's copy, the registry manifest the image was
    /// converted from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_manifest_digest: Option<String>,
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

    /// Points a pulled image's tag at the copy converted from
    /// `source_manifest_digest`.
    async fn put_pulled_image_tag(
        &self,
        tag: &ImageTag,
        digest: &str,
        source_manifest_digest: &str,
    ) -> RepositoryResult<()>;

    /// The image digest `tag` points to; `None` when the tag does not exist.
    async fn get_image_tag(&self, tag: &ImageTag) -> RepositoryResult<Option<String>>;

    /// The record of `tag`; `None` when the tag does not exist.
    async fn get_image_tag_record(
        &self,
        tag: &ImageTag,
    ) -> RepositoryResult<Option<ImageTagRecord>>;
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
        put_tag(self, tag, digest, None).await
    }

    async fn put_pulled_image_tag(
        &self,
        tag: &ImageTag,
        digest: &str,
        source_manifest_digest: &str,
    ) -> RepositoryResult<()> {
        put_tag(self, tag, digest, Some(source_manifest_digest.to_owned())).await
    }

    async fn get_image_tag(&self, tag: &ImageTag) -> RepositoryResult<Option<String>> {
        Ok(self
            .get_image_tag_record(tag)
            .await?
            .map(|record| record.image_digest))
    }

    async fn get_image_tag_record(
        &self,
        tag: &ImageTag,
    ) -> RepositoryResult<Option<ImageTagRecord>> {
        let Some(bytes) = self
            .get_document(DocumentKey::ImageTag(tag.clone()))
            .await?
        else {
            return Ok(None);
        };
        decode_tag(tag, &bytes).map(Some)
    }
}

async fn put_tag<R: Repository + ?Sized>(
    repository: &R,
    tag: &ImageTag,
    digest: &str,
    source_manifest_digest: Option<String>,
) -> RepositoryResult<()> {
    DocumentKey::image(digest)?;
    let record = ImageTagRecord {
        image_digest: digest.to_owned(),
        created_at_unix_ms: now_unix_ms(),
        source_manifest_digest,
    };
    let bytes = serde_json::to_vec(&record)
        .map_err(|error| RepositoryError::backend("encode image tag", error))?;
    repository
        .put_document(DocumentKey::ImageTag(tag.clone()), bytes)
        .await
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

#[cfg(test)]
mod tests {
    use super::{ImageCatalog, ImageTagRecord};
    use crate::repository::backends::PosixFsRepository;
    use crate::repository::{DocumentKey, Repository};
    use crate::types::ImageTag;

    #[tokio::test]
    async fn pulled_tags_record_their_source_manifest() -> anyhow::Result<()> {
        let temp = tempfile::TempDir::new()?;
        let repository = PosixFsRepository::new(temp.path().to_path_buf());
        let digest = crate::digest::sha256_digest(b"image");
        let source = crate::digest::sha256_digest(b"manifest");
        let pulled = ImageTag::parse("docker.io/library/python:3.12")?;
        repository
            .put_pulled_image_tag(&pulled, &digest, &source)
            .await?;
        let record = repository.get_image_tag_record(&pulled).await?.unwrap();
        assert_eq!(record.image_digest, digest);
        assert_eq!(record.source_manifest_digest, Some(source));
        assert_eq!(
            repository.get_image_tag(&pulled).await?,
            Some(digest.clone())
        );

        // Records written before the field existed, and built tags, have no source.
        let built = ImageTag::parse("web:v1")?;
        repository
            .put_document(
                DocumentKey::ImageTag(built.clone()),
                format!(r#"{{"imageDigest":"{digest}","createdAtUnixMs":1}}"#).into_bytes(),
            )
            .await?;
        assert_eq!(
            repository.get_image_tag_record(&built).await?,
            Some(ImageTagRecord {
                image_digest: digest.clone(),
                created_at_unix_ms: 1,
                source_manifest_digest: None,
            })
        );
        repository.put_image_tag(&built, &digest).await?;
        let bytes = repository
            .get_document(DocumentKey::ImageTag(built))
            .await?
            .unwrap();
        assert!(!String::from_utf8(bytes)?.contains("sourceManifestDigest"));
        Ok(())
    }
}
