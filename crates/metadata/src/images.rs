//! Published OverlayBD image descriptions and mutable `name:tag` pointers.

use agentenv_domain::{is_sha256_digest, ImageTag, ImageTagRecord, PublishedImage, ResourceKind};
use sqlx::AssertSqlSafe;

use crate::database::{begin_tx, commit_tx, dispatch_db, impl_sql_backend, Db};
use crate::layers::{collect_disk_relations, now_unix_ms, LayerRelations};
use crate::{MetaError, MetaResult, MetaServer};

fn validate_digest(digest: &str) -> MetaResult<()> {
    if !is_sha256_digest(digest) {
        return Err(MetaError::InvalidRequest {
            reason: format!("invalid image digest '{digest}'"),
        });
    }
    Ok(())
}

fn decode_image(digest: &str, raw: String) -> MetaResult<PublishedImage> {
    PublishedImage::decode(digest, raw.as_bytes())
        .map_err(|error| MetaError::backend("verify published image", error))
}

impl MetaServer {
    pub async fn put_image(
        &self,
        image: &PublishedImage,
        restored_layers: &[String],
    ) -> MetaResult<String> {
        let (digest, bytes) = image.encode().map_err(|error| MetaError::InvalidRequest {
            reason: error.to_string(),
        })?;
        let payload_json = String::from_utf8(bytes)
            .map_err(|error| MetaError::backend("encode published image utf8", error))?;
        let relations = collect_disk_relations(&image.layers)?;
        dispatch_db!(&self.database, |pool| {
            Db(pool)
                .put_image(&digest, image, &payload_json, &relations, restored_layers)
                .await
        })
    }

    pub async fn get_image(&self, digest: &str) -> MetaResult<Option<PublishedImage>> {
        validate_digest(digest)?;
        let payload: Option<String> = dispatch_db!(&self.database, |pool| {
            sqlx::query_scalar("SELECT payload_json FROM images WHERE digest = $1 AND NOT retired")
                .bind(digest)
                .fetch_optional(pool)
                .await
        })
        .map_err(|error| MetaError::backend("get published image", error))?;
        payload.map(|raw| decode_image(digest, raw)).transpose()
    }

    pub async fn get_image_tag(
        &self,
        tag: &ImageTag,
    ) -> MetaResult<Option<(ImageTagRecord, Option<PublishedImage>)>> {
        let row: Option<(String, i64, Option<String>, Option<String>)> =
            dispatch_db!(&self.database, |pool| {
                sqlx::query_as(
                    "SELECT t.image_digest, t.created_at, t.source_manifest_digest, i.payload_json
                     FROM image_tags t
                     LEFT JOIN images i ON i.digest = t.image_digest AND NOT i.retired
                     WHERE t.name = $1 AND t.tag = $2",
                )
                .bind(tag.name().as_ref())
                .bind(tag.tag())
                .fetch_optional(pool)
                .await
            })
            .map_err(|error| MetaError::backend("get image tag", error))?;
        match row {
            Some((digest, created_at_unix_ms, source_manifest_digest, payload)) => {
                let image = payload.map(|raw| decode_image(&digest, raw)).transpose()?;
                Ok(Some((
                    ImageTagRecord {
                        image_digest: digest,
                        created_at_unix_ms,
                        source_manifest_digest,
                    },
                    image,
                )))
            }
            None => Ok(None),
        }
    }

    pub async fn put_image_tag(
        &self,
        tag: &ImageTag,
        digest: &str,
        source_manifest_digest: Option<&str>,
    ) -> MetaResult<()> {
        validate_digest(digest)?;
        if source_manifest_digest.is_some_and(str::is_empty) {
            return Err(MetaError::InvalidRequest {
                reason: "source manifest digest cannot be empty".into(),
            });
        }
        dispatch_db!(&self.database, |pool| {
            Db(pool)
                .put_image_tag(tag, digest, source_manifest_digest)
                .await
        })
    }

    pub async fn retire_image(&self, digest: &str) -> MetaResult<()> {
        validate_digest(digest)?;
        dispatch_db!(&self.database, |pool| Db(pool).retire_image(digest).await)
    }
}

impl_sql_backend! {
impl<D> Db<'_, D> {
    async fn put_image(
        &self,
        digest: &str,
        image: &PublishedImage,
        payload_json: &str,
        relations: &LayerRelations,
        restored_layers: &[String],
    ) -> MetaResult<String> {
        let schema_version =
            i32::try_from(image.schema_version).map_err(|_| MetaError::InvalidRequest {
                reason: "unsupported image schema version".into(),
            })?;
        let now = now_unix_ms();
        loop {
            let mut tx = begin_tx(self.0, "begin image publication").await?;
            sqlx::query::<D>(
                "INSERT INTO images (digest, schema_version, os, architecture, payload_json, retired, created_at)
                 VALUES ($1, $2, $3, $4, $5, FALSE, $6)
                 ON CONFLICT (digest) DO UPDATE SET retired = FALSE",
            )
            .bind(digest)
            .bind(schema_version)
            .bind(image.os.as_str())
            .bind(image.architecture.as_str())
            .bind(payload_json)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("register published image", error))?;
            if !Self::attach_layers(
                &mut tx,
                ResourceKind::Image,
                digest,
                relations,
                restored_layers,
            )
            .await?
            {
                tx.rollback()
                    .await
                    .map_err(|error| MetaError::backend("restart image layer checks", error))?;
                continue;
            }
            commit_tx(tx, "commit image publication").await?;
            return Ok(digest.to_owned());
        }
    }

    async fn put_image_tag(
        &self,
        tag: &ImageTag,
        digest: &str,
        source_manifest_digest: Option<&str>,
    ) -> MetaResult<()> {
        let mut tx = begin_tx(self.0, "begin image tag update").await?;
        let retired: Option<bool> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT retired FROM images WHERE digest = $1{}",
            D::FOR_SHARE,
        )))
        .bind(digest)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("verify published image for tag", error))?;
        if retired != Some(false) {
            return Err(MetaError::Unavailable {
                reason: format!("published image '{digest}' does not exist"),
            });
        }
        sqlx::query::<D>(
            "INSERT INTO image_tags (name, tag, image_digest, source_manifest_digest, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $5)
             ON CONFLICT (name, tag) DO UPDATE
             SET image_digest = EXCLUDED.image_digest,
                 source_manifest_digest = EXCLUDED.source_manifest_digest,
                 created_at = EXCLUDED.created_at,
                 updated_at = EXCLUDED.updated_at",
        )
        .bind(tag.name().as_ref())
        .bind(tag.tag())
        .bind(digest)
        .bind(source_manifest_digest)
        .bind(now_unix_ms())
        .execute(&mut *tx)
        .await
        .map_err(|error| MetaError::backend("upsert image tag", error))?;
        commit_tx(tx, "commit image tag update").await
    }

    async fn retire_image(&self, digest: &str) -> MetaResult<()> {
        let mut tx = begin_tx(self.0, "begin image retirement").await?;
        sqlx::query::<D>("UPDATE images SET retired = TRUE WHERE digest = $1")
            .bind(digest)
            .execute(&mut *tx)
            .await
            .map_err(|error| MetaError::backend("retire published image", error))?;
        Self::release_owner_heads(&mut tx, ResourceKind::Image, digest).await?;
        commit_tx(tx, "commit image retirement").await
    }
}
}
