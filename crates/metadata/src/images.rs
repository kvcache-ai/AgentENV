//! Published OverlayBD image descriptions and mutable `name:tag` pointers.

use agentenv_domain::{is_sha256_digest, ImageTag, ImageTagRecord, PublishedImage, ResourceKind};

use crate::db::{is_foreign_key_violation, MetaSqlExt, Sql};
use crate::layers::{attach_layers, collect_disk_relations, now_unix_ms};
use crate::{MetaError, MetaResult, MetaServer};

fn require_digest(digest: &str) -> MetaResult<()> {
    if !is_sha256_digest(digest) {
        return Err(MetaError::invalid(format!(
            "invalid image digest '{digest}'"
        )));
    }
    Ok(())
}

impl MetaServer {
    pub async fn put_image(
        &self,
        image: &PublishedImage,
        restored_layers: &[String],
    ) -> MetaResult<String> {
        let (digest, bytes) = image
            .encode()
            .map_err(|e| MetaError::invalid(e.to_string()))?;
        let payload = String::from_utf8(bytes)
            .map_err(|e| MetaError::backend("encode published image utf8", e))?;
        let relations = collect_disk_relations(&image.layers)?;
        let now = now_unix_ms();
        loop {
            let mut tx = self.database.begin("begin image publication").await?;
            tx.execute(
                Sql::new(
                    "INSERT INTO images (digest, payload_json, created_at) VALUES ($1, $2, $3)
                     ON CONFLICT (digest) DO NOTHING",
                )
                .bind(&digest)
                .bind(&payload)
                .bind(now),
            )
            .await
            .meta_err("register published image")?;
            if !attach_layers(
                &mut tx,
                ResourceKind::Image,
                &digest,
                &relations,
                restored_layers,
            )
            .await?
            {
                tx.rollback("restart image layer checks").await?;
                continue;
            }
            tx.commit("commit image publication").await?;
            return Ok(digest);
        }
    }

    pub async fn get_image(&self, digest: &str) -> MetaResult<Option<PublishedImage>> {
        require_digest(digest)?;
        let Some(row) = self
            .database
            .fetch_optional(
                Sql::new("SELECT payload_json FROM images WHERE digest = $1").bind(digest),
            )
            .await
            .meta_err("get published image")?
        else {
            return Ok(None);
        };
        PublishedImage::decode(digest, row.get::<String>(0)?.as_bytes())
            .map(Some)
            .map_err(|e| MetaError::backend("verify published image", e))
    }

    pub async fn get_image_tag(
        &self,
        tag: &ImageTag,
    ) -> MetaResult<Option<(ImageTagRecord, PublishedImage)>> {
        let Some(row) = self
            .database
            .fetch_optional(
                Sql::new(
                    "SELECT t.image_digest, t.created_at, t.source_manifest_digest, i.payload_json
                     FROM image_tags t
                     INNER JOIN images i ON i.digest = t.image_digest
                     WHERE t.name = $1 AND t.tag = $2",
                )
                .bind(tag.name().as_ref())
                .bind(tag.tag()),
            )
            .await
            .meta_err("get image tag")?
        else {
            return Ok(None);
        };
        let digest: String = row.get(0)?;
        let image = PublishedImage::decode(&digest, row.get::<String>(3)?.as_bytes())
            .map_err(|e| MetaError::backend("verify published image", e))?;
        Ok(Some((
            ImageTagRecord {
                image_digest: digest,
                created_at_unix_ms: row.get(1)?,
                source_manifest_digest: row.get(2)?,
            },
            image,
        )))
    }

    pub async fn put_image_tag(
        &self,
        tag: &ImageTag,
        digest: &str,
        source_manifest_digest: Option<&str>,
    ) -> MetaResult<()> {
        require_digest(digest)?;
        if source_manifest_digest.is_some_and(str::is_empty) {
            return Err(MetaError::invalid("source manifest digest cannot be empty"));
        }
        self.database
            .execute(
                Sql::new(
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
                .bind(now_unix_ms()),
            )
            .await
            .map_err(|e| {
                if is_foreign_key_violation(&e) {
                    MetaError::unavailable(format!("published image '{digest}' does not exist"))
                } else {
                    MetaError::backend("upsert image tag", e)
                }
            })?;
        Ok(())
    }
}
