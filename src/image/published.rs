//! Repository- and SQL-backed storage, resolution, and background publication
//! for published OverlayBD images (`sha256:...` digests and `aenv/...` tags).

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
#[cfg(test)]
use futures::future::BoxFuture;
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use super::cache::{CachedImageConfig, OverlaybdLayerStore, SourceImageStore};
use super::oci_image;
use super::resolver::{detect_arch, resolved_from_cached_config, ResolvedBlockImage};
use super::{ImageError, ImageResult, PublishedImage};
use crate::layer::{LayerRef, PreparedLayerUpload};
use crate::metadata::{commit_with_layer_restore, map_meta_error, MetaClient, MetaError};
#[cfg(test)]
use crate::repository::RepositoryResult;
use crate::repository::{LayerRole, Repository};
use crate::types::ImageTag;

#[cfg(test)]
pub(super) type TestStoreImageLayers =
    fn(Arc<dyn Repository>, PathBuf) -> BoxFuture<'static, RepositoryResult<Vec<LayerRef>>>;

/// The outcome of publishing a pulled image.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PulledPublication {
    /// The image was published with this digest.
    Published(String),
    /// The tag already points to this digest, published from the same source.
    Unchanged(String),
    /// The tag points to an image of this architecture and was left alone.
    OtherArchitecture(String),
}

/// Manages published OverlayBD images and tags over `MetaClient` + `Repository`,
/// including deduplicated and ordered background publication of pulled images.
#[derive(Clone)]
pub(super) struct PublishedImageStore {
    repository: Arc<dyn Repository>,
    meta: MetaClient,
    store: Arc<dyn SourceImageStore>,
    overlaybd_layers: Arc<dyn OverlaybdLayerStore>,
    /// The source manifest of each pulled-image tag this node is publishing
    /// or has published, so repeated resolutions skip the repository.
    pulled_publications: Arc<Mutex<HashMap<ImageTag, String>>>,
    /// Orders this node's publications of each pulled-image tag, so an older
    /// source cannot overwrite a newer one.
    pulled_publication_locks: Arc<Mutex<HashMap<ImageTag, Arc<tokio::sync::Mutex<()>>>>>,
    #[cfg(test)]
    test_store_layers: Option<TestStoreImageLayers>,
}

impl PublishedImageStore {
    pub(super) fn new(
        repository: Arc<dyn Repository>,
        meta: MetaClient,
        store: Arc<dyn SourceImageStore>,
        overlaybd_layers: Arc<dyn OverlaybdLayerStore>,
    ) -> Self {
        Self {
            repository,
            meta,
            store,
            overlaybd_layers,
            pulled_publications: Arc::new(Mutex::new(HashMap::new())),
            pulled_publication_locks: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(test)]
            test_store_layers: None,
        }
    }

    #[cfg(test)]
    pub(super) fn set_test_store_layers(&mut self, store_layers: Option<TestStoreImageLayers>) {
        self.test_store_layers = store_layers;
    }

    pub(super) async fn publish_image(&self, image: &ResolvedBlockImage) -> Result<String> {
        self.publish_image_with_uploads(image, |repository, path| {
            crate::repository::store_disk_layers_with_uploads(repository, path)
        })
        .await
    }

    #[cfg(test)]
    pub(super) async fn publish_image_with<'a, F, Fut>(
        &'a self,
        image: &'a ResolvedBlockImage,
        store_layers: F,
    ) -> Result<String>
    where
        F: FnOnce(&'a dyn Repository, &'a Path) -> Fut,
        Fut: Future<Output = RepositoryResult<Vec<LayerRef>>>,
    {
        self.publish_image_with_uploads(image, |repository, path| async move {
            let layers = store_layers(repository, path).await?;
            Ok((layers, Vec::new()))
        })
        .await
    }

    async fn publish_image_with_uploads<'a, F, Fut>(
        &'a self,
        image: &'a ResolvedBlockImage,
        store_layers: F,
    ) -> Result<String>
    where
        F: FnOnce(&'a dyn Repository, &'a Path) -> Fut,
        Fut: Future<
            Output = crate::repository::RepositoryResult<(Vec<LayerRef>, Vec<PreparedLayerUpload>)>,
        >,
    {
        // Capacity eviction takes this same source-image lock. Keep the local
        // converted layers rooted until the repository has committed them.
        // OCI refs carry `"host/repo@sha256:..."`, while `SourceImageStore`
        // keys entries by the bare manifest digest.
        let lock_key = image
            .image_ref
            .rsplit_once('@')
            .map(|(_, digest)| digest)
            .unwrap_or(&image.image_ref);
        let source = self.store.open(lock_key, None).await?;
        let _source_lock = source.begin_conversion().await?;
        let config = image
            .raw_config
            .clone()
            .context("built image configuration is missing")?;
        let (layers, uploads) = store_layers(
            self.repository.as_ref(),
            image.overlaybd_config_path.as_path(),
        )
        .await?;
        let description = PublishedImage::new(
            oci_image::host_arch_to_oci(&detect_arch()?)?.into(),
            layers,
            config,
        );
        put_image_with_restore(&self.meta, self.repository.as_ref(), &description, &uploads).await
    }

    pub(super) async fn resolve_digest(&self, digest: &str) -> ImageResult<ResolvedBlockImage> {
        super::buildkit::validate_digest(digest).map_err(|error| ImageError::InvalidReference {
            reason: error.to_string(),
        })?;
        let image = self
            .meta
            .get_image(digest.to_owned())
            .await
            .map_err(|error| match error {
                MetaError::InvalidRequest { reason } => ImageError::InvalidReference { reason },
                other => ImageError::Other(anyhow::Error::from(map_meta_error(other))),
            })?
            .ok_or_else(|| ImageError::NotFound {
                reason: format!("published OverlayBD image '{digest}' was not found"),
            })?;
        self.materialize_published_image(digest, image).await
    }

    /// Resolves a tag to the digest it points to now. The result carries only
    /// the digest, so moving the tag later never changes this resolution.
    pub(super) async fn resolve_tag(&self, reference: &str) -> ImageResult<ResolvedBlockImage> {
        let tag = ImageTag::parse(reference).map_err(|error| ImageError::InvalidReference {
            reason: error.to_string(),
        })?;
        let (tag_entry, image) = self
            .meta
            .get_image_tag(tag.clone())
            .await
            .map_err(|error| ImageError::Other(anyhow::Error::from(map_meta_error(error))))?
            .ok_or_else(|| ImageError::NotFound {
                reason: format!("image tag '{tag}' was not found"),
            })?;
        let digest = tag_entry.image_digest.as_str();
        debug!(%tag, digest, "resolved image tag");
        let image = image.ok_or_else(|| ImageError::NotFound {
            reason: format!(
                "image tag '{tag}': published OverlayBD image '{digest}' was not found"
            ),
        })?;
        self.materialize_published_image(digest, image)
            .await
            .map_err(|error| error.context(format_args!("image tag '{tag}'")))
    }

    async fn materialize_published_image(
        &self,
        digest: &str,
        image: PublishedImage,
    ) -> ImageResult<ResolvedBlockImage> {
        if image.architecture != oci_image::host_arch_to_oci(&detect_arch()?)? {
            return Err(ImageError::UnsupportedImage {
                reason: format!("image requires {}", image.architecture),
            });
        }
        let mut metadata = oci_image::parse_oci_image_config(
            &serde_json::to_string(&json!({"config": image.config}))
                .map_err(anyhow::Error::from)?,
        )?;
        let layers = crate::repository::layer_chain_for_refs(
            self.repository.as_ref(),
            &image.layers,
            LayerRole::Rootfs,
        )
        .map_err(anyhow::Error::from)?;
        let mut runtime_config = layers.to_runtime_config();
        self.overlaybd_layers
            .attach_to_image_config(&mut runtime_config);
        let config = serde_json::to_value(runtime_config).map_err(anyhow::Error::from)?;
        metadata.layers = Some(layers);
        let source = self
            .store
            .open(digest, Some("published-overlaybd-v1"))
            .await?;
        if let CachedImageConfig::Found {
            image_config_path, ..
        } = source.cached_config().await?
        {
            return Ok(resolved_from_cached_config(
                digest,
                image_config_path,
                metadata,
            ));
        }
        let conversion = source.begin_conversion().await?;
        let path = source
            .publish_config(&config, metadata.clone(), conversion)
            .await?;
        Ok(resolved_from_cached_config(digest, path, metadata))
    }

    /// Publishes the copy of a pulled image in the background unless this
    /// node already did for `source_manifest_digest`, after this node's
    /// earlier publications of `tag`, and not once a newer source of `tag`
    /// supersedes it. Returns the task, or `None` when there is nothing to
    /// publish.
    pub(super) fn spawn_pulled_publication(
        &self,
        tag: ImageTag,
        source_manifest_digest: String,
        image: &ResolvedBlockImage,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let publication = self.pulled_publication(&tag, &source_manifest_digest, image)?;
        {
            let mut publications = self
                .pulled_publications
                .lock()
                .expect("pulled image publications lock poisoned");
            if publications.get(&tag) == Some(&source_manifest_digest) {
                return None;
            }
            publications.insert(tag.clone(), source_manifest_digest.clone());
        }
        let lock = self.pulled_publication_lock(&tag);
        let publications = Arc::clone(&self.pulled_publications);
        Some(tokio::spawn(async move {
            let _lock = lock.lock_owned().await;
            let superseded = publications
                .lock()
                .expect("pulled image publications lock poisoned")
                .get(&tag)
                != Some(&source_manifest_digest);
            if superseded {
                return;
            }
            if publication.await.is_err() {
                // Let a later resolution retry.
                let mut publications = publications
                    .lock()
                    .expect("pulled image publications lock poisoned");
                if publications.get(&tag) == Some(&source_manifest_digest) {
                    publications.remove(&tag);
                }
            }
        }))
    }

    /// Publishes `image` as `tag` and waits for it, in a task that finishes
    /// even if the caller goes away.
    pub(super) async fn publish_pulled_now(
        &self,
        tag: &ImageTag,
        source_manifest_digest: &str,
        image: &ResolvedBlockImage,
    ) -> ImageResult<PulledPublication> {
        let publication = self
            .pulled_publication(tag, source_manifest_digest, image)
            .context("pulled image publication is not configured")?;
        let lock = self.pulled_publication_lock(tag);
        let publications = Arc::clone(&self.pulled_publications);
        let tag_owned = tag.clone();
        let digest_owned = source_manifest_digest.to_owned();
        let outcome = tokio::spawn(async move {
            let _lock = lock.lock_owned().await;
            let result = publication.await;
            if matches!(
                result,
                Ok(PulledPublication::Published(_) | PulledPublication::Unchanged(_))
            ) {
                publications
                    .lock()
                    .expect("pulled image publications lock poisoned")
                    .insert(tag_owned, digest_owned);
            }
            result
        })
        .await
        .context("pulled image publication task failed")??;
        Ok(outcome)
    }

    /// The lock that orders this node's publications of `tag`.
    fn pulled_publication_lock(&self, tag: &ImageTag) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .pulled_publication_locks
            .lock()
            .expect("pulled image publication locks poisoned");
        Arc::clone(locks.entry(tag.clone()).or_default())
    }

    /// The publication of `image` as `tag`, independent of `self` so it can
    /// run in its own task. `None` without an image config.
    fn pulled_publication(
        &self,
        tag: &ImageTag,
        source_manifest_digest: &str,
        image: &ResolvedBlockImage,
    ) -> Option<impl Future<Output = Result<PulledPublication>> + Send + 'static> {
        let config = image.raw_config.clone()?;
        let published = self.clone();
        let tag = tag.clone();
        let source_manifest_digest = source_manifest_digest.to_owned();
        let image_config_path = image.overlaybd_config_path.clone();
        Some(async move {
            let result = published
                .publish_pulled_image(&tag, &source_manifest_digest, image_config_path, config)
                .await;
            let outcome = match &result {
                Ok(PulledPublication::Published(digest)) => {
                    info!(%tag, %digest, "published pulled image to the repository");
                    "published"
                }
                Ok(PulledPublication::Unchanged(_)) => {
                    debug!(%tag, "pulled image is already published");
                    "unchanged"
                }
                Ok(PulledPublication::OtherArchitecture(architecture)) => {
                    warn!(
                        %tag,
                        %architecture,
                        "pulled image tag points to an image of another architecture; leaving it"
                    );
                    "skipped"
                }
                Err(error) => {
                    warn!(
                        %tag,
                        error = %format_args!("{error:#}"),
                        "failed to publish pulled image to the repository"
                    );
                    "failed"
                }
            };
            metrics::counter!("agentenv_image_pulled_publication_total", "result" => outcome)
                .increment(1);
            result
        })
    }

    /// Publishes the image at `image_config_path`, converted from
    /// `source_manifest_digest`, and points `tag` at it. Skips an unchanged
    /// source, and leaves a tag that points to another architecture's image.
    async fn publish_pulled_image(
        &self,
        tag: &ImageTag,
        source_manifest_digest: &str,
        image_config_path: PathBuf,
        config: Value,
    ) -> Result<PulledPublication> {
        let architecture = oci_image::host_arch_to_oci(&detect_arch()?)?;
        if let Some((tag_entry, current)) = self
            .meta
            .get_image_tag(tag.clone())
            .await
            .map_err(map_meta_error)?
        {
            if tag_entry.source_manifest_digest.as_deref() == Some(source_manifest_digest) {
                return Ok(PulledPublication::Unchanged(tag_entry.image_digest));
            }
            if let Some(current) = current {
                if current.architecture != architecture {
                    return Ok(PulledPublication::OtherArchitecture(current.architecture));
                }
            }
        }
        // Capacity eviction takes this same source-image lock. Keep the local
        // converted layers rooted until the repository has committed them.
        let source = self.store.open(source_manifest_digest, None).await?;
        let _source_lock = source.begin_conversion().await?;
        #[cfg(test)]
        let (layers, uploads) = match self.test_store_layers {
            Some(store_layers) => (
                store_layers(Arc::clone(&self.repository), image_config_path).await?,
                Vec::new(),
            ),
            None => {
                crate::repository::store_disk_layers_with_uploads(
                    self.repository.as_ref(),
                    &image_config_path,
                )
                .await?
            }
        };
        #[cfg(not(test))]
        let (layers, uploads) = crate::repository::store_disk_layers_with_uploads(
            self.repository.as_ref(),
            &image_config_path,
        )
        .await?;
        let image = PublishedImage::new(architecture.into(), layers, config);
        let digest =
            put_image_with_restore(&self.meta, self.repository.as_ref(), &image, &uploads).await?;
        self.meta
            .put_image_tag(
                tag.clone(),
                digest.clone(),
                Some(source_manifest_digest.to_owned()),
            )
            .await
            .map_err(map_meta_error)?;
        Ok(PulledPublication::Published(digest))
    }
}

async fn put_image_with_restore(
    meta: &MetaClient,
    repository: &dyn Repository,
    description: &PublishedImage,
    uploads: &[PreparedLayerUpload],
) -> Result<String> {
    let local_layers: Vec<(&str, &Path)> = uploads
        .iter()
        .map(|upload| (upload.digest(), upload.path()))
        .collect();
    let managed: Vec<_> = description
        .layers
        .iter()
        .filter_map(|layer| match layer {
            LayerRef::Managed(layer) => Some(layer),
            LayerRef::External(_) => None,
        })
        .collect();
    commit_with_layer_restore(
        repository,
        &managed,
        &local_layers,
        |error| anyhow::Error::from(map_meta_error(error)),
        anyhow::Error::from,
        |restored| {
            let description = description.clone();
            async move { meta.put_image(description, restored).await }
        },
    )
    .await
}
