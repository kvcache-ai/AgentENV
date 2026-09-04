// This facade keeps image-cache ownership in one place while callers describe
// only the cache objects they currently hold.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use overlaybd::layer_metadata::read_overlaybd_layer_uuid;
use serde_json::Value;
use tokio::sync::OnceCell;
use tracing::{debug, info, warn};

use super::gc::{
    ImageCacheGcBlocked, ImageCacheGcBlockedReason, ImageCacheGcReport, ImageCacheGcSummary,
    ImageCacheLiveRuntimeRefs,
};
use super::graph::{
    non_empty, path_is_inside, stable_path_identity, unix_now_secs, CapacityEvictionCandidate,
    HardCommitId, HardCommitObjectRecord, ImageCacheConfigId, ImageCacheHoldOwner,
    ImageCacheMetadataStore,
};
use super::source_config::{
    read_source_image_metadata, source_image_config_filename, source_image_config_is_usable,
    source_image_metadata_path_for_config_path, write_json_atomic, ImageCacheSourceImageKey,
    SourceImageCache, SourceImageLock,
};
use super::store::{CachedImageConfig, OverlaybdLayerLocation};
use crate::cfg::{AppConfig, ResolvedImageCacheConfig};
use crate::digest::FileDigest;
use crate::image::commit_index::{self, CommitIndex};
use crate::image::local_layer::LocalLayer;
use crate::image::oci_image::{ConvertedLayerP2pMetadata, ImageConversion, LayerConversionKey};
use crate::image::ImageResolutionMetadata;
use crate::local_store::LocalStoreDurability;
use crate::p2p::{P2pArtifactKey, P2pPublishMode, P2pPublishRequest, P2pTransport};

const IMAGE_CACHE_CONFIG_DIR: &str = "configs";
const IMAGE_CACHE_INDEX_DIR: &str = "indexes";
const IMAGE_CACHE_METADATA_DIR: &str = "metadata";
const IMAGE_CACHE_STAGING_DIR: &str = "staging";
const RUNTIME_HOLD_NAMESPACE: &str = "runtime";
const PAUSED_HOLD_NAMESPACE: &str = "paused";

/// Namespace of a local-image protection lease. Neutral to sandbox lifecycle —
/// callers map their own lifecycle states (e.g. starting, paused) onto these.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HoldNamespace {
    /// Transient lease for an owner that is starting or running.
    Runtime,
    /// Durable lease for an owner that is paused.
    Paused,
}

impl HoldNamespace {
    fn as_str(self) -> &'static str {
        match self {
            HoldNamespace::Runtime => RUNTIME_HOLD_NAMESPACE,
            HoldNamespace::Paused => PAUSED_HOLD_NAMESPACE,
        }
    }

    fn release_reason(self) -> &'static str {
        match self {
            HoldNamespace::Runtime => "runtime_release",
            HoldNamespace::Paused => "paused_release",
        }
    }
}

pub(crate) struct ImageCacheService {
    commit_store: PathBuf,
    index_dir: PathBuf,
    config_dir: PathBuf,
    metadata_store_path: PathBuf,
    staging_dir: PathBuf,
    metadata_store: OnceCell<ImageCacheMetadataStore>,
    source_images: Arc<SourceImageCache>,
    p2p_transport: OnceLock<Arc<dyn P2pTransport>>,
}

impl fmt::Debug for ImageCacheService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageCacheService")
            .field("commit_store", &self.commit_store)
            .field("index_dir", &self.index_dir)
            .field("config_dir", &self.config_dir)
            .finish_non_exhaustive()
    }
}

impl ImageCacheService {
    pub(crate) fn shared_from_app_config(config: &AppConfig) -> Arc<Self> {
        Self::shared_from_resolved_config(config.image_cache_layout())
    }

    fn shared_from_resolved_config(config: ResolvedImageCacheConfig) -> Arc<Self> {
        static SHARED: OnceLock<StdMutex<BTreeMap<PathBuf, Arc<ImageCacheService>>>> =
            OnceLock::new();
        let key = stable_path_identity(&config.root_dir);
        let services = SHARED.get_or_init(|| StdMutex::new(BTreeMap::new()));
        let mut services = services
            .lock()
            .expect("image cache service registry lock poisoned");
        if let Some(service) = services.get(&key) {
            return Arc::clone(service);
        }
        let service = Arc::new(Self::from_resolved_config(config));
        services.insert(key, Arc::clone(&service));
        service
    }

    pub(crate) fn from_resolved_config(config: ResolvedImageCacheConfig) -> Self {
        let root_dir = config.root_dir;
        let commit_store = config.commit_store;

        Self {
            index_dir: root_dir.join(IMAGE_CACHE_INDEX_DIR),
            config_dir: root_dir.join(IMAGE_CACHE_CONFIG_DIR),
            metadata_store_path: root_dir.join(IMAGE_CACHE_METADATA_DIR),
            staging_dir: root_dir.join(IMAGE_CACHE_STAGING_DIR),
            commit_store,
            metadata_store: OnceCell::new(),
            source_images: Arc::new(SourceImageCache::default()),
            p2p_transport: OnceLock::new(),
        }
    }

    pub(crate) fn initialize_p2p_transport(&self, transport: Arc<dyn P2pTransport>) {
        assert!(
            self.p2p_transport.set(transport).is_ok(),
            "image cache P2P transport already initialized"
        );
    }

    /// Compute the `(config, metadata)` sidecar paths for a source image
    /// identity. The `ImageCacheSourceImageKey` is built and consumed here, so
    /// callers and the resource ports speak only the raw
    /// `(manifest_digest, repository_scope)`.
    fn source_image_paths(
        &self,
        manifest_digest: &str,
        repository_scope: Option<&str>,
    ) -> Result<(PathBuf, PathBuf)> {
        let key = ImageCacheSourceImageKey::new(
            manifest_digest,
            repository_scope.map(|s| s.to_string()),
        )?;
        let image_config_path = self.config_dir.join(source_image_config_filename(&key));
        let metadata_path = source_image_metadata_path_for_config_path(&image_config_path);
        Ok((image_config_path, metadata_path))
    }

    async fn open_source_image(
        self: &Arc<Self>,
        manifest_digest: &str,
        repository_scope: Option<&str>,
    ) -> Result<SourceImageSession> {
        let (image_config_path, metadata_path) =
            self.source_image_paths(manifest_digest, repository_scope)?;
        let lock = self.source_images.acquire_lock(&image_config_path).await;
        Ok(SourceImageSession {
            image_cache: Arc::clone(self),
            _lock: lock,
            image_config_path,
            metadata_path,
        })
    }

    pub(crate) async fn ensure_layout(&self) -> Result<()> {
        for (label, path) in [
            ("overlaybd commit cache", self.commit_store.as_path()),
            ("image cache index", self.index_dir.as_path()),
            ("image cache config", self.config_dir.as_path()),
            ("image cache metadata", self.metadata_store_path.as_path()),
            ("image cache staging", self.staging_dir.as_path()),
        ] {
            tokio::fs::create_dir_all(path)
                .await
                .with_context(|| format!("create {label} dir {}", path.display()))?;
        }
        Ok(())
    }

    async fn commit_store_hard_commits_from_config_paths(
        &self,
        config_paths: impl IntoIterator<Item = PathBuf>,
        context: &'static str,
    ) -> Result<BTreeSet<HardCommitId>> {
        let commit_store = self.commit_store.clone();
        let config_paths = config_paths.into_iter().collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || {
            let mut objects = BTreeSet::new();
            for config_path in config_paths {
                objects.extend(
                    ImageCacheMetadataStore::commit_store_hard_commit_digests_from_config_path(
                        &config_path,
                        &commit_store,
                    )
                    .with_context(|| format!("{context} {}", config_path.display()))?,
                );
            }
            Ok(objects)
        })
        .await
        .context("join image-cache config parse")?
    }

    async fn record_source_config_refs_from_config_path(
        &self,
        image_config_path: &Path,
    ) -> Result<()> {
        self.metadata_store()
            .await?
            .record_config_refs_from_config_path(image_config_path)
            .await?;
        Ok(())
    }

    async fn record_source_config_refs_from_config_path_best_effort(
        &self,
        image_config_path: &Path,
        reason: &str,
    ) {
        if let Err(error) = self
            .record_source_config_refs_from_config_path(image_config_path)
            .await
        {
            warn!(
                reason,
                config = %image_config_path.display(),
                error = %error,
                "failed to record image cache source config refs"
            );
        }
    }

    async fn rebuild_metadata_from_configs(&self) -> Result<()> {
        self.metadata_store()
            .await?
            .rebuild_from_configs(&self.config_dir)
            .await
    }

    async fn record_hard_commit_object(
        &self,
        digest: impl Into<String>,
        file: Option<PathBuf>,
        size: Option<u64>,
        p2p_keys: &[P2pArtifactKey],
    ) -> Result<()> {
        self.metadata_store()
            .await?
            .record_hard_commit_object(
                HardCommitId::new(digest)?,
                file,
                size,
                p2p_keys.iter().cloned().collect(),
            )
            .await
    }

    pub(crate) async fn add_hard_commit_p2p_key(
        &self,
        digest: &str,
        key: &P2pArtifactKey,
    ) -> Result<()> {
        self.metadata_store()
            .await?
            .add_hard_commit_p2p_key(&HardCommitId::new(digest)?, key)
            .await
    }

    /// Persist ownership for a converted OCI layer before publishing it to the
    /// P2P catalog. A missing or disabled transport is a normal no-op; callers
    /// can keep the local conversion path usable when P2P is disabled.
    async fn publish_converted_layer(
        &self,
        key: &LayerConversionKey,
        layer: &LocalLayer,
    ) -> Result<bool> {
        let Some(transport) = self.p2p_transport.get().cloned() else {
            return Ok(false);
        };
        if transport.local_endpoint().is_none() {
            return Ok(false);
        }

        // Recording ownership first closes the crash window where a catalog entry could
        // outlive the cache metadata that tells GC how to remove it.
        let p2p_key = key.p2p_key();
        let p2p_metadata = ConvertedLayerP2pMetadata::from_local_layer(layer);
        let serialized_metadata = serde_json::to_value(&p2p_metadata)
            .context("serialize converted image layer P2P metadata")?;
        self.add_hard_commit_p2p_key(&layer.digest, &p2p_key)
            .await
            .with_context(|| format!("record converted image layer P2P ownership '{p2p_key}'"))?;

        let request = P2pPublishRequest::file(p2p_key.clone(), layer.path.clone())
            .with_publish_mode(P2pPublishMode::Reference)
            .with_metadata(serialized_metadata);
        transport
            .publish(&request)
            .await
            .with_context(|| format!("publish converted image layer '{p2p_key}'"))?;

        Ok(true)
    }

    #[cfg(test)]
    pub(crate) async fn import_hard_commit_trusted_descriptor(
        self: &Arc<Self>,
        source: &Path,
        digest: &str,
        size: u64,
    ) -> Result<PathBuf> {
        let hold = Self::acquire_operation_hold_for_hard_commits(
            Arc::clone(self),
            "import_hard_commit_trusted_descriptor",
            [digest.to_string()],
        )
        .await?;

        let result = self
            .seed_and_record_hard_commit(source, digest, size, true, &[])
            .await;
        hold.release_best_effort("hard_commit_import_release").await;
        result
    }

    /// Seed a commit file into the commit store and record its hard-commit
    /// object fact. Hold lifecycle is the caller's responsibility; this helper
    /// only does the seed + record shared by import and overlaybd persist.
    async fn seed_and_record_hard_commit(
        &self,
        source: &Path,
        digest: &str,
        size: u64,
        trusted_descriptor: bool,
        p2p_keys: &[P2pArtifactKey],
    ) -> Result<PathBuf> {
        let commit_store = self.commit_store.clone();
        let source = source.to_path_buf();
        let digest_owned = digest.to_string();
        let cached_path = tokio::task::spawn_blocking(move || {
            if trusted_descriptor {
                commit_index::seed_commit_file_trusted_descriptor(
                    &commit_store,
                    &source,
                    &digest_owned,
                    size,
                )
            } else {
                commit_index::seed_commit_file(&commit_store, &source, &digest_owned, size)
            }
        })
        .await
        .context("join hard commit seed task")??;
        self.record_hard_commit_object(digest, Some(cached_path.clone()), Some(size), p2p_keys)
            .await?;
        Ok(cached_path)
    }

    async fn create_or_replace_hold(
        &self,
        owner: ImageCacheHoldOwner,
        refs: BTreeSet<HardCommitId>,
    ) -> Result<()> {
        self.metadata_store()
            .await?
            .create_or_replace_hold(&owner, &refs)
            .await
    }

    async fn release_hold(&self, owner: &ImageCacheHoldOwner) -> Result<()> {
        self.metadata_store().await?.release_hold(owner).await
    }

    async fn release_hold_best_effort(&self, owner: &ImageCacheHoldOwner, reason: &str) {
        if let Err(error) = self.release_hold(owner).await {
            warn!(
                reason,
                owner = %owner,
                error = %error,
                "failed to release image cache hold"
            );
        }
    }

    /// Owner keys of all holds in `namespace` (e.g. the sandbox ids of
    /// `paused/*` holds). Used by startup reconcile to find orphaned holds.
    async fn list_hold_keys_in_namespace(&self, namespace: &str) -> Result<Vec<String>> {
        Ok(self
            .metadata_store()
            .await?
            .list_hold_owners_in_namespaces(&[namespace])
            .await?
            .into_iter()
            .map(|owner| owner.key().to_string())
            .collect())
    }

    /// Startup cleanup for transient holds only; durable `paused/*` holds and
    /// source config roots are preserved.
    async fn cleanup_stale_runtime_holds(&self) -> Result<usize> {
        let released = self
            .metadata_store()
            .await?
            .release_holds_in_namespaces(&["runtime", "operation"])
            .await?;
        if !released.is_empty() {
            info!(
                count = released.len(),
                "released stale runtime image-cache holds at startup"
            );
        }
        Ok(released.len())
    }

    fn next_operation_hold_owner(operation: &str) -> Result<ImageCacheHoldOwner> {
        static NEXT_OPERATION_HOLD_ID: AtomicU64 = AtomicU64::new(1);
        let operation = non_empty("image cache operation", operation.to_string())?;
        let hold_id = NEXT_OPERATION_HOLD_ID.fetch_add(1, Ordering::Relaxed);
        ImageCacheHoldOwner::new("operation", format!("{operation}/{hold_id}"))
    }

    async fn acquire_operation_hold_for_hard_commits(
        image_cache: Arc<Self>,
        operation: impl Into<String>,
        digests: impl IntoIterator<Item = String>,
    ) -> Result<ImageCacheOperationHold> {
        let owner = Self::next_operation_hold_owner(&operation.into())?;
        let digests: BTreeSet<HardCommitId> = digests
            .into_iter()
            .map(HardCommitId::new)
            .collect::<Result<BTreeSet<_>>>()?;
        image_cache
            .create_or_replace_hold(owner.clone(), digests.clone())
            .await?;
        Ok(ImageCacheOperationHold {
            image_cache,
            owner,
            digests,
            attempted_p2p_lookups: BTreeSet::new(),
            materialized: true,
            released: false,
        })
    }

    /// Lazy resolve-scoped operation hold; materialized on first protected commit.
    fn begin_hard_commit_operation(
        self: &Arc<Self>,
        operation: &str,
    ) -> Result<ImageCacheOperationHold> {
        Ok(ImageCacheOperationHold {
            image_cache: Arc::clone(self),
            owner: Self::next_operation_hold_owner(operation)?,
            digests: BTreeSet::new(),
            attempted_p2p_lookups: BTreeSet::new(),
            materialized: false,
            released: false,
        })
    }

    /// Hold cache-owned commit-store `file=` commits from these configs; skip
    /// recoverable `dir=` layers and runtime-owned local files. Callers fail
    /// closed on commit-store descriptor errors.
    async fn acquire_hold_for_config_paths(
        &self,
        owner: ImageCacheHoldOwner,
        config_paths: impl IntoIterator<Item = PathBuf>,
    ) -> Result<()> {
        let refs = self
            .commit_store_hard_commits_from_config_paths(
                config_paths,
                "parse image-cache hold config",
            )
            .await?;
        if refs.is_empty() {
            return Ok(());
        }
        self.create_or_replace_hold(owner, refs).await
    }

    // --- Liveness-facing facade: hold owner/key encoding stays internal here, so
    // callers describe only a namespace, an opaque owner id, and the neutral
    // image config paths whose cache-owned commit-store refs should be protected.

    /// Protect the cache-owned commit-store refs an owner opens, in `namespace`.
    pub(crate) async fn protect(
        &self,
        namespace: HoldNamespace,
        owner_id: &str,
        image_config_paths: Vec<PathBuf>,
    ) -> Result<()> {
        let owner = ImageCacheHoldOwner::new(namespace.as_str(), owner_id.to_string())?;
        self.acquire_hold_for_config_paths(owner, image_config_paths)
            .await
    }

    pub(crate) async fn release_protection_best_effort(
        &self,
        namespace: HoldNamespace,
        owner_id: &str,
    ) {
        if let Ok(owner) = ImageCacheHoldOwner::new(namespace.as_str(), owner_id.to_string()) {
            self.release_hold_best_effort(&owner, namespace.release_reason())
                .await;
        }
    }

    /// Startup reconcile: drop transient runtime/operation holds, then release
    /// holds in `namespace` whose owner is no longer live.
    pub(crate) async fn reconcile_namespace(
        &self,
        namespace: HoldNamespace,
        live_owner_ids: &[String],
    ) -> Result<()> {
        self.cleanup_stale_runtime_holds().await?;
        let live: BTreeSet<&str> = live_owner_ids.iter().map(String::as_str).collect();
        for key in self.list_hold_keys_in_namespace(namespace.as_str()).await? {
            if !live.contains(key.as_str()) {
                if let Ok(owner) = ImageCacheHoldOwner::new(namespace.as_str(), key) {
                    self.release_hold_best_effort(&owner, "startup_reconcile_orphan")
                        .await;
                }
            }
        }
        Ok(())
    }

    /// Run a maintenance pass (optional capacity eviction + fail-closed GC),
    /// deriving live refs from the running-set artifacts the adapter passes in.
    /// Callers never see holds, digests, or the metadata graph.
    pub(crate) async fn run_maintenance(
        self: &Arc<Self>,
        running: Vec<(String, Vec<PathBuf>)>,
        watermark: Option<(u64, u64)>,
        min_age: Duration,
    ) -> Result<ImageCacheGcSummary> {
        let reconciled = if let Some((high, low)) = watermark {
            self.evict_source_configs_over_capacity(high, low, min_age)
                .await?;
            true
        } else {
            false
        };
        let live_refs = self.live_refs_from_running(running).await?;
        let report = self.run_gc(live_refs, !reconciled).await?;
        Ok(ImageCacheGcSummary::from_report(&report))
    }

    async fn live_refs_from_running(
        &self,
        running: Vec<(String, Vec<PathBuf>)>,
    ) -> Result<ImageCacheLiveRuntimeRefs> {
        let mut refs = ImageCacheLiveRuntimeRefs::new();
        for (sandbox_id, image_config_paths) in running {
            let owner = ImageCacheHoldOwner::new(RUNTIME_HOLD_NAMESPACE, sandbox_id)?;
            for object in self
                .commit_store_hard_commits_from_config_paths(
                    image_config_paths,
                    "parse live runtime image config",
                )
                .await?
            {
                refs.entry(object).or_default().push(owner.clone());
            }
        }
        for owners in refs.values_mut() {
            owners.sort();
            owners.dedup();
        }
        Ok(refs)
    }

    async fn check_collectable_hard_commit(
        &self,
        record: &HardCommitObjectRecord,
        config_referrers: Vec<ImageCacheConfigId>,
        live_refs: &ImageCacheLiveRuntimeRefs,
        ignore_owner: Option<&ImageCacheHoldOwner>,
    ) -> Result<HardCommitGcDecision> {
        let digest = record.digest.clone();

        if !config_referrers.is_empty() {
            return Ok(HardCommitGcDecision::Blocked(ImageCacheGcBlocked {
                digest,
                reason: ImageCacheGcBlockedReason::RootedByConfig {
                    configs: config_referrers
                        .into_iter()
                        .map(|config_id| config_id.as_str().to_string())
                        .collect(),
                },
            }));
        }

        let owners = self
            .metadata_store()
            .await?
            .hard_commit_hold_referrers(&digest)
            .await?
            .into_iter()
            .filter(|owner| ignore_owner != Some(owner))
            .collect::<Vec<_>>();
        if !owners.is_empty() {
            return Ok(HardCommitGcDecision::Blocked(ImageCacheGcBlocked {
                digest,
                reason: ImageCacheGcBlockedReason::Held { owners },
            }));
        }

        let Some(file) = record.file.clone() else {
            return Ok(HardCommitGcDecision::Blocked(ImageCacheGcBlocked {
                digest,
                reason: ImageCacheGcBlockedReason::Unverifiable(
                    "hard commit object has no recorded file".to_string(),
                ),
            }));
        };
        if let Some(reason) = self.commit_file_block_reason(&file, record.size) {
            return Ok(HardCommitGcDecision::Blocked(ImageCacheGcBlocked {
                digest,
                reason,
            }));
        }

        if let Some(owners) = live_refs.get(&digest) {
            if !owners.is_empty() {
                return Ok(HardCommitGcDecision::Blocked(ImageCacheGcBlocked {
                    digest,
                    reason: ImageCacheGcBlockedReason::LiveRuntime {
                        owners: owners.clone(),
                    },
                }));
            }
        }

        Ok(HardCommitGcDecision::Collectable {
            digest,
            file,
            size: record.size,
            p2p_keys: record.p2p_keys.clone(),
        })
    }

    async fn metadata_store(&self) -> Result<&ImageCacheMetadataStore> {
        self.metadata_store
            .get_or_try_init(|| {
                ImageCacheMetadataStore::open(
                    self.metadata_store_path.clone(),
                    LocalStoreDurability::Sync,
                )
            })
            .await
    }

    fn commit_file_block_reason(
        &self,
        file: &Path,
        size: Option<u64>,
    ) -> Option<ImageCacheGcBlockedReason> {
        if !path_is_inside(file, &self.commit_store) {
            return Some(ImageCacheGcBlockedReason::Unverifiable(
                "commit file is outside the commit store".to_string(),
            ));
        }
        let metadata = match std::fs::symlink_metadata(file) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Some(ImageCacheGcBlockedReason::Unverifiable(
                    "commit file is missing".to_string(),
                ));
            }
            Err(error) => {
                return Some(ImageCacheGcBlockedReason::Unverifiable(format!(
                    "stat commit file failed: {error}"
                )));
            }
        };
        if !metadata.is_file() {
            return Some(ImageCacheGcBlockedReason::Unverifiable(
                "commit path is not a regular file".to_string(),
            ));
        }
        if let Some(expected) = size {
            let actual = metadata.len();
            if actual != expected {
                return Some(ImageCacheGcBlockedReason::Unverifiable(format!(
                    "commit file size mismatch: expected {expected}, got {actual}"
                )));
            }
        }
        None
    }

    async fn delete_collectable_hard_commit(
        &self,
        digest: &HardCommitId,
        file: &Path,
        p2p_keys: &BTreeSet<P2pArtifactKey>,
    ) -> Result<()> {
        if !p2p_keys.is_empty() {
            let transport = self
                .p2p_transport
                .get()
                .cloned()
                .context("image cache P2P transport is not initialized")?;
            for key in p2p_keys {
                transport
                    .unpublish(key)
                    .await
                    .with_context(|| format!("unpublish image cache P2P key '{key}'"))?;
            }
        }

        tokio::fs::remove_file(file)
            .await
            .with_context(|| format!("remove image cache commit file {}", file.display()))?;

        if let Some(parent) = file.parent() {
            if parent != self.commit_store && path_is_inside(parent, &self.commit_store) {
                let _ = tokio::fs::remove_dir(parent).await;
            }
        }

        self.metadata_store()
            .await?
            .remove_hard_commit_object(digest)
            .await
    }

    /// Deleting GC is fail-closed: reconcile roots, collect live runtime refs,
    /// then re-check each candidate under an operation hold before deleting.
    /// Fail-closed deleting GC over the candidate hard commits, given the live
    /// runtime refs the caller already computed. Optionally rebuilds the
    /// config-derived metadata first (skip it when capacity eviction already
    /// reconciled it this pass).
    async fn run_gc(
        self: &Arc<Self>,
        live_refs: ImageCacheLiveRuntimeRefs,
        rebuild_metadata: bool,
    ) -> Result<ImageCacheGcReport> {
        if rebuild_metadata {
            self.rebuild_metadata_from_configs().await?;
        }

        let metadata = self.metadata_store().await?;
        let config_referrers = metadata.hard_commit_config_referrer_map().await?;
        let hard_commit_objects = metadata.list_hard_commit_objects().await?;
        let mut report = ImageCacheGcReport::default();
        for record in hard_commit_objects {
            let referrers = config_referrers
                .get(&record.digest)
                .cloned()
                .unwrap_or_default();
            match self
                .check_collectable_hard_commit(&record, referrers, &live_refs, None)
                .await?
            {
                HardCommitGcDecision::Blocked(blocked) => {
                    report.blocked.push(blocked);
                    continue;
                }
                HardCommitGcDecision::Collectable { .. } => {}
            }

            let digest = record.digest.clone();
            let hold = Self::acquire_operation_hold_for_hard_commits(
                Arc::clone(self),
                "gc",
                [digest.as_str().to_string()],
            )
            .await?;

            let result = async {
                let fresh = self
                    .metadata_store()
                    .await?
                    .get_hard_commit_object(&digest)
                    .await?;

                match fresh {
                    None => report.blocked.push(ImageCacheGcBlocked {
                        digest: digest.clone(),
                        reason: ImageCacheGcBlockedReason::Unverifiable(
                            "hard commit object record disappeared before delete".to_string(),
                        ),
                    }),
                    Some(record) => {
                        // Source-config publish is not serialized by the GC
                        // operation hold, so refresh roots after taking the
                        // hold and before deleting the commit.
                        let fresh_referrers = self
                            .metadata_store()
                            .await?
                            .hard_commit_config_referrers(&digest)
                            .await?;
                        match self
                            .check_collectable_hard_commit(
                                &record,
                                fresh_referrers,
                                &live_refs,
                                Some(hold.owner()),
                            )
                            .await?
                        {
                            HardCommitGcDecision::Collectable {
                                digest,
                                file,
                                size,
                                p2p_keys,
                            } => {
                                match self
                                    .delete_collectable_hard_commit(&digest, &file, &p2p_keys)
                                    .await
                                {
                                    Ok(()) => {
                                        report.collected += 1;
                                        report.freed_bytes += size.unwrap_or(0);
                                    }
                                    Err(error) => {
                                        warn!(
                                            digest = %digest,
                                            error = %error,
                                            "failed to delete collectable image cache commit"
                                        );
                                        report.blocked.push(ImageCacheGcBlocked {
                                            digest,
                                            reason: ImageCacheGcBlockedReason::DeleteFailed(
                                                error.to_string(),
                                            ),
                                        });
                                    }
                                }
                            }
                            HardCommitGcDecision::Blocked(blocked) => report.blocked.push(blocked),
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;

            hold.release_best_effort("gc").await;
            result?;
        }

        match self.reclaim_stale_indexes().await {
            Ok(stale_indexes) => {
                if stale_indexes > 0 {
                    info!(
                        stale_indexes,
                        "image cache reclaimed stale conversion indexes"
                    );
                }
            }
            Err(error) => warn!(error = %error, "image cache stale-index reclaim failed"),
        }

        Ok(report)
    }

    async fn reclaim_stale_indexes(&self) -> Result<usize> {
        let mut removed = 0usize;
        let mut digest_dirs = match tokio::fs::read_dir(&self.index_dir).await {
            Ok(dirs) => dirs,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error).context("read image cache index dir"),
        };
        while let Some(digest_dir) = digest_dirs.next_entry().await? {
            if !digest_dir.file_type().await?.is_dir() {
                continue;
            }
            let digest_dir_path = digest_dir.path();
            let mut index_files = tokio::fs::read_dir(&digest_dir_path).await?;
            let mut remaining = 0usize;
            while let Some(index_file) = index_files.next_entry().await? {
                let index_path = index_file.path();
                let stale = match commit_index::CommitIndex::read(&index_path).await {
                    Ok(Some(index)) => {
                        !commit_index::commit_file(&self.commit_store, &index.commit_digest)
                            .exists()
                    }
                    Ok(None) => true,
                    Err(error) => {
                        warn!(
                            index = %index_path.display(),
                            error = %error,
                            "failed to read image cache index; keeping it"
                        );
                        false
                    }
                };
                if stale && tokio::fs::remove_file(&index_path).await.is_ok() {
                    removed += 1;
                } else if !stale {
                    remaining += 1;
                }
            }
            if remaining == 0 {
                let _ = tokio::fs::remove_dir(&digest_dir_path).await;
            }
        }
        Ok(removed)
    }

    async fn evict_source_configs_over_capacity(
        &self,
        high_watermark_bytes: u64,
        low_watermark_bytes: u64,
        min_age: Duration,
    ) -> Result<()> {
        self.rebuild_metadata_from_configs().await?;
        let now = unix_now_secs();
        let evictable_before = now.saturating_sub(min_age.as_secs());
        let plan = self
            .metadata_store()
            .await?
            .plan_capacity_eviction(high_watermark_bytes, low_watermark_bytes, evictable_before)
            .await?;

        for candidate in &plan.candidates {
            self.delete_source_image_config_best_effort(candidate, evictable_before)
                .await;
        }

        Ok(())
    }

    async fn delete_source_image_config_best_effort(
        &self,
        candidate: &CapacityEvictionCandidate,
        evictable_before: u64,
    ) {
        let config_id = &candidate.config_id;
        let config_path = self.config_dir.join(config_id.as_str());
        let _lock = self.source_images.acquire_lock(&config_path).await;
        let metadata = match self.metadata_store().await {
            Ok(metadata) => metadata,
            Err(error) => {
                warn!(
                    config = %config_path.display(),
                    error = %error,
                    "failed to open image cache metadata while evicting source config"
                );
                return;
            }
        };
        match metadata.config_last_used(config_id).await {
            Ok(Some(last_used))
                if last_used == candidate.last_used && last_used <= evictable_before => {}
            Ok(_) => return,
            Err(error) => {
                warn!(
                    config = %config_path.display(),
                    error = %error,
                    "failed to recheck source image config eviction candidate"
                );
                return;
            }
        }
        if let Err(error) = tokio::fs::remove_file(&config_path).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(
                    config = %config_path.display(),
                    error = %error,
                    "failed to evict source image config"
                );
                return;
            }
        }
        if let Err(error) = metadata.remove_config_refs(config_id).await {
            warn!(
                config = %config_path.display(),
                error = %error,
                "failed to remove evicted source image config refs"
            );
        }
        let _ =
            tokio::fs::remove_file(source_image_metadata_path_for_config_path(&config_path)).await;
    }
}

pub(crate) struct SourceImageSession {
    image_cache: Arc<ImageCacheService>,
    _lock: SourceImageLock,
    image_config_path: PathBuf,
    metadata_path: PathBuf,
}

impl SourceImageSession {
    pub(crate) async fn lookup_cached_config(&self) -> Result<CachedImageConfig> {
        if !source_image_config_is_usable(&self.image_config_path) {
            return Ok(CachedImageConfig::Missing);
        }
        match read_source_image_metadata(&self.metadata_path).await? {
            Some(metadata) => {
                self.touch_root_best_effort("resolve_cache_hit").await;
                Ok(CachedImageConfig::Found {
                    image_config_path: self.image_config_path.clone(),
                    metadata: Some(Box::new(metadata)),
                })
            }
            None => {
                self.touch_root_best_effort("resolve_rebuild_metadata")
                    .await;
                Ok(CachedImageConfig::Found {
                    image_config_path: self.image_config_path.clone(),
                    metadata: None,
                })
            }
        }
    }

    /// Begin a resolve-scoped conversion: ensure the cache layout exists and take
    /// the operation hold that protects every commit the conversion persists
    /// until `publish_config` roots them.
    pub(crate) async fn begin_conversion(&self) -> Result<ImageCacheOperationHold> {
        self.image_cache.ensure_layout().await?;
        self.image_cache
            .begin_hard_commit_operation("oci-conversion")
    }

    pub(crate) async fn complete_cached_metadata(
        &self,
        metadata: ImageResolutionMetadata,
    ) -> Result<PathBuf> {
        self.image_cache.ensure_layout().await?;
        write_json_atomic(&self.metadata_path, &metadata, "image metadata").await?;
        Ok(self.image_config_path.clone())
    }

    async fn touch_root_best_effort(&self, reason: &str) {
        self.image_cache
            .record_source_config_refs_from_config_path_best_effort(&self.image_config_path, reason)
            .await
    }
}

/// A resolve-scoped conversion session: holds the per-image lock and the
/// operation hold that protects converted/reused commits, forwarding conversion
/// calls to the hold. `publish_image_config` drops it AFTER recording the config
/// root (and any earlier failure drops it too), so the commits are already
/// protected by the durable root before lock and operation hold are released.
struct ConversionSession {
    _session: SourceImageSession,
    hold: ImageCacheOperationHold,
}

#[async_trait]
impl ImageConversion for ConversionSession {
    fn remote_layer_dir_hint(&self, layer_digest: &str) -> PathBuf {
        self.hold.remote_layer_dir_hint(layer_digest)
    }

    fn create_temp_staging_dir(&self) -> Result<tempfile::TempDir> {
        self.hold.create_temp_staging_dir()
    }

    async fn lookup_converted_layer(
        &mut self,
        key: &LayerConversionKey,
    ) -> Result<Option<LocalLayer>> {
        self.hold.lookup_converted_layer(key).await
    }

    async fn store_converted_layer(
        &mut self,
        key: &LayerConversionKey,
        temp_commit: &Path,
    ) -> Result<LocalLayer> {
        self.hold.store_converted_layer(key, temp_commit).await
    }
}

impl ImageCacheService {
    /// Look up a cached resolved config by source identity (short-lived lock).
    pub(crate) async fn lookup_image_config(
        self: &Arc<Self>,
        manifest_digest: &str,
        repository_scope: Option<&str>,
    ) -> Result<CachedImageConfig> {
        self.open_source_image(manifest_digest, repository_scope)
            .await?
            .lookup_cached_config()
            .await
    }

    /// Begin a resolve-scoped conversion, taking the per-image lock + operation
    /// hold and returning a handle that carries both until publish.
    pub(crate) async fn begin_image_conversion(
        self: &Arc<Self>,
        manifest_digest: &str,
        repository_scope: Option<&str>,
    ) -> Result<Box<dyn ImageConversion>> {
        let session = self
            .open_source_image(manifest_digest, repository_scope)
            .await?;
        let hold = session.begin_conversion().await?;
        Ok(Box::new(ConversionSession {
            _session: session,
            hold,
        }))
    }

    /// Publish the resolved config (the durable root for its commits) and its
    /// metadata, then finish the conversion session. The config root is recorded
    /// BEFORE the conversion handle is dropped, so commits stay protected across
    /// the whole publish; the drop then releases the per-image lock + hold.
    pub(crate) async fn publish_image_config(
        self: &Arc<Self>,
        manifest_digest: &str,
        repository_scope: Option<&str>,
        overlaybd_config: &Value,
        metadata: ImageResolutionMetadata,
        conversion: Box<dyn ImageConversion>,
    ) -> Result<PathBuf> {
        self.ensure_layout().await?;
        let (image_config_path, metadata_path) =
            self.source_image_paths(manifest_digest, repository_scope)?;
        write_json_atomic(&image_config_path, overlaybd_config, "image config").await?;
        self.record_source_config_refs_from_config_path_best_effort(
            &image_config_path,
            "publish_source_image_config",
        )
        .await;
        write_json_atomic(&metadata_path, &metadata, "image metadata").await?;
        drop(conversion);
        Ok(image_config_path)
    }

    /// Write only the resolution metadata sidecar for an already-cached config.
    pub(crate) async fn write_image_metadata(
        self: &Arc<Self>,
        manifest_digest: &str,
        repository_scope: Option<&str>,
        metadata: ImageResolutionMetadata,
    ) -> Result<PathBuf> {
        self.open_source_image(manifest_digest, repository_scope)
            .await?
            .complete_cached_metadata(metadata)
            .await
    }

    /// Decide where a runnable snapshot lower should point for `digest`: bind to
    /// the cached `file=` only for cache-local layers with no registry fallback,
    /// else the commit `dir=` (overlaybd falls back to the registry).
    pub(crate) fn overlaybd_layer_location(
        &self,
        digest: &str,
        size: u64,
        has_remote: bool,
    ) -> OverlaybdLayerLocation {
        if !has_remote {
            if let Some(file) =
                commit_index::cached_commit_file_if_usable(&self.commit_store, digest, size)
            {
                return OverlaybdLayerLocation::LocalFile(file);
            }
        }
        OverlaybdLayerLocation::CacheDir(commit_index::commit_dir(&self.commit_store, digest))
    }

    /// Local roots P2P may publish overlaybd layer bytes from.
    pub(crate) fn allowed_overlaybd_publish_roots(&self) -> Vec<PathBuf> {
        vec![self.commit_store.clone()]
    }
}

pub(crate) struct ImageCacheOperationHold {
    image_cache: Arc<ImageCacheService>,
    owner: ImageCacheHoldOwner,
    digests: BTreeSet<HardCommitId>,
    attempted_p2p_lookups: BTreeSet<P2pArtifactKey>,
    materialized: bool,
    released: bool,
}

impl ImageCacheOperationHold {
    fn owner(&self) -> &ImageCacheHoldOwner {
        &self.owner
    }

    pub(crate) async fn release_best_effort(mut self, reason: &str) {
        self.released = true;
        if !self.materialized {
            return;
        }
        self.image_cache
            .release_hold_best_effort(&self.owner, reason)
            .await;
    }

    fn commit_dir(&self, commit_digest: &str) -> PathBuf {
        commit_index::commit_dir(&self.image_cache.commit_store, commit_digest)
    }

    fn commit_file(&self, commit_digest: &str) -> PathBuf {
        commit_index::commit_file(&self.image_cache.commit_store, commit_digest)
    }

    fn conversion_index_path(
        &self,
        source_digest: &str,
        converter: &str,
        virtual_size_gib: u64,
        mkfs: bool,
        parent_commit_digest: Option<&str>,
    ) -> PathBuf {
        commit_index::index_path(
            &self.image_cache.index_dir,
            source_digest,
            converter,
            virtual_size_gib,
            mkfs,
            parent_commit_digest,
        )
    }

    async fn protect_hard_commit(&mut self, digest: &str) -> Result<()> {
        let digest = HardCommitId::new(digest)?;
        if self.digests.insert(digest) {
            self.image_cache
                .create_or_replace_hold(self.owner.clone(), self.digests.clone())
                .await?;
            self.materialized = true;
        }
        Ok(())
    }

    async fn persist_overlaybd_commit(
        &mut self,
        temp_commit: &Path,
        digest: &str,
        size: u64,
        p2p_keys: &[P2pArtifactKey],
    ) -> Result<PathBuf> {
        self.protect_hard_commit(digest).await?;
        self.image_cache
            .seed_and_record_hard_commit(temp_commit, digest, size, true, p2p_keys)
            .await
    }

    async fn write_conversion_index(
        &self,
        key: &LayerConversionKey,
        layer: &LocalLayer,
    ) -> Result<()> {
        let index = CommitIndex::oci_layer(
            key.source_layer_digest.clone(),
            layer.digest.clone(),
            layer.size,
            key.converter_id.clone(),
            key.virtual_size_gib,
            key.mkfs,
            key.parent_commit_digest.clone(),
        );
        index
            .write(&self.conversion_index_path(
                &key.source_layer_digest,
                &key.converter_id,
                key.virtual_size_gib,
                key.mkfs,
                key.parent_commit_digest.as_deref(),
            ))
            .await
    }

    async fn lookup_remote_converted_layer(
        &mut self,
        key: &LayerConversionKey,
    ) -> Result<Option<LocalLayer>> {
        let Some(transport) = self.image_cache.p2p_transport.get().cloned() else {
            return Ok(None);
        };
        let p2p_key = key.p2p_key();
        if !self.attempted_p2p_lookups.insert(p2p_key.clone()) {
            debug!(key = %p2p_key, "skipping repeated P2P converted image layer lookup");
            return Ok(None);
        }
        let descriptor = match transport.lookup(&p2p_key).await {
            Ok(Some(descriptor)) => descriptor,
            Ok(None) => return Ok(None),
            Err(error) => {
                debug!(key = %p2p_key, error = %error, "P2P converted image layer lookup failed; falling back to registry conversion");
                return Ok(None);
            }
        };
        let metadata = match ConvertedLayerP2pMetadata::parse(&descriptor.metadata) {
            Ok(metadata) => metadata,
            Err(error) => {
                debug!(key = %p2p_key, error = %error, "P2P converted image layer metadata mismatch; falling back to registry conversion");
                return Ok(None);
            }
        };

        let staging = self.create_temp_staging_dir()?;
        let destination = staging.path().join("overlaybd.commit");
        if let Err(error) = transport.fetch(&descriptor, &destination).await {
            debug!(key = %p2p_key, error = %error, "P2P converted image layer fetch failed; falling back to registry conversion");
            return Ok(None);
        }
        let described = match FileDigest::describe(&destination).await {
            Ok(described) => described,
            Err(error) => {
                debug!(key = %p2p_key, error = %error, "P2P converted image layer digest failed; falling back to registry conversion");
                // A successful fetch automatically republishes the P2P artifact, so if the file is invalid, we must unpublish it.
                let _ = transport.unpublish(&p2p_key).await;
                return Ok(None);
            }
        };
        if described.sha256 != metadata.commit_digest || described.size != metadata.size {
            debug!(
                key = %p2p_key,
                expected_digest = %metadata.commit_digest,
                actual_digest = %described.sha256,
                expected_size = metadata.size,
                actual_size = described.size,
                "P2P converted image layer failed digest or size validation"
            );
            let _ = transport.unpublish(&p2p_key).await;
            return Ok(None);
        }
        match read_overlaybd_layer_uuid(&destination) {
            Ok(uuid) if uuid == key.expected_layer_uuid => {}
            Ok(uuid) => {
                debug!(key = %p2p_key, found_uuid = %uuid, expected_uuid = %key.expected_layer_uuid, "P2P converted image layer UUID mismatch");
                let _ = transport.unpublish(&p2p_key).await;
                return Ok(None);
            }
            Err(error) => {
                debug!(key = %p2p_key, error = %error, "P2P converted image layer is not a sealed OverlayBD layer");
                let _ = transport.unpublish(&p2p_key).await;
                return Ok(None);
            }
        }

        let stored = self
            .persist_overlaybd_commit(
                &destination,
                &metadata.commit_digest,
                metadata.size,
                std::slice::from_ref(&p2p_key),
            )
            .await?;
        let layer = LocalLayer {
            path: stored,
            digest: metadata.commit_digest,
            size: metadata.size,
        };
        self.write_conversion_index(key, &layer).await?;
        info!(key = %p2p_key, digest = %layer.digest, "reused converted user image layer from P2P");
        Ok(Some(layer))
    }
}

#[async_trait]
impl ImageConversion for ImageCacheOperationHold {
    fn remote_layer_dir_hint(&self, layer_digest: &str) -> PathBuf {
        self.commit_dir(layer_digest)
    }

    fn create_temp_staging_dir(&self) -> Result<tempfile::TempDir> {
        // Stay on the same filesystem as the commit cache so produced
        // `layer.commit` files can be hard-linked into it (avoids EXDEV).
        let parent = &self.image_cache.staging_dir;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create staging dir {}", parent.display()))?;
        tempfile::tempdir_in(parent)
            .with_context(|| format!("create tempdir in {}", parent.display()))
    }

    async fn lookup_converted_layer(
        &mut self,
        key: &LayerConversionKey,
    ) -> Result<Option<LocalLayer>> {
        let index_path = self.conversion_index_path(
            &key.source_layer_digest,
            &key.converter_id,
            key.virtual_size_gib,
            key.mkfs,
            key.parent_commit_digest.as_deref(),
        );
        let Some(index) = CommitIndex::read(&index_path).await? else {
            debug!(
                digest = %key.source_layer_digest,
                path = %index_path.display(),
                "converted user image layer index is missing from cache"
            );
            return self.lookup_remote_converted_layer(key).await;
        };
        if !index.matches_oci_context(
            &key.source_layer_digest,
            &key.converter_id,
            key.virtual_size_gib,
            key.mkfs,
            key.parent_commit_digest.as_deref(),
        ) {
            debug!(
                digest = %key.source_layer_digest,
                path = %index_path.display(),
                "converted user image layer index has stale conversion context"
            );
            return self.lookup_remote_converted_layer(key).await;
        }

        // Protect the indexed commit BEFORE stat'ing its file. A concurrent GC
        // pass re-checks hold referrers at delete time, so this hold blocks
        // deletion of a commit we are about to reuse into the (not-yet-published)
        // source config. If GC already won the race, the stat below finds the
        // file gone and the caller falls through to a fresh conversion — both
        // outcomes are safe.
        self.protect_hard_commit(&index.commit_digest).await?;

        let cached = self.commit_file(&index.commit_digest);
        let cached_metadata = tokio::fs::metadata(&cached).await.ok();
        if !cached_metadata.is_some_and(|metadata| {
            metadata.is_file() && metadata.len() > 0 && metadata.len() == index.size
        }) {
            info!(
                digest = %key.source_layer_digest,
                commit_digest = %index.commit_digest,
                path = %cached.display(),
                "converted user image layer commit is missing from cache"
            );
            return self.lookup_remote_converted_layer(key).await;
        }

        match read_overlaybd_layer_uuid(&cached) {
            Ok(uuid) if uuid == key.expected_layer_uuid => {}
            Ok(uuid) => {
                info!(
                    digest = %key.source_layer_digest,
                    commit_digest = %index.commit_digest,
                    path = %cached.display(),
                    found_uuid = %uuid,
                    expected_uuid = %key.expected_layer_uuid,
                    "converted user image layer cache has stale UUID"
                );
                return self.lookup_remote_converted_layer(key).await;
            }
            Err(err) => {
                info!(
                    digest = %key.source_layer_digest,
                    commit_digest = %index.commit_digest,
                    path = %cached.display(),
                    error = %err,
                    "converted user image layer cache is not a valid sealed overlaybd layer"
                );
                return self.lookup_remote_converted_layer(key).await;
            }
        }

        info!(
            digest = %key.source_layer_digest,
            commit_digest = %index.commit_digest,
            path = %cached.display(),
            "user image layer already converted, reusing content-addressed cache"
        );
        Ok(Some(LocalLayer {
            path: cached,
            digest: index.commit_digest,
            size: index.size,
        }))
    }

    async fn store_converted_layer(
        &mut self,
        key: &LayerConversionKey,
        temp_commit: &Path,
    ) -> Result<LocalLayer> {
        let described = FileDigest::describe(temp_commit).await.with_context(|| {
            format!(
                "describe converted user image layer {}",
                temp_commit.display()
            )
        })?;
        let stored = self
            .persist_overlaybd_commit(temp_commit, &described.sha256, described.size, &[])
            .await?;
        let layer = LocalLayer {
            path: stored,
            digest: described.sha256,
            size: described.size,
        };
        if let Err(error) = self.image_cache.publish_converted_layer(key, &layer).await {
            debug!(
                key = %key.p2p_key(),
                error = %error,
                "failed to publish converted image layer to P2P"
            );
        }
        self.write_conversion_index(key, &layer).await?;
        Ok(layer)
    }
}

impl Drop for ImageCacheOperationHold {
    fn drop(&mut self) {
        if self.released || !self.materialized {
            return;
        }
        release_operation_hold_best_effort(
            Arc::clone(&self.image_cache),
            self.owner.clone(),
            "operation_hold_drop",
        );
    }
}

fn release_operation_hold_best_effort(
    image_cache: Arc<ImageCacheService>,
    owner: ImageCacheHoldOwner,
    reason: &'static str,
) {
    // Operation holds are created and dropped inside async resolver/import/GC
    // paths, so a drop with no current runtime means the hold outlived its
    // normal lifecycle. Best-effort release favors keeping over mis-deleting,
    // and the transient `operation` namespace is reclaimed by the startup
    // `cleanup_stale_runtime_holds` pass, so skipping release here is safe.
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        warn!(
            owner = %owner,
            reason,
            "skipping image cache operation hold release: no current Tokio runtime"
        );
        return;
    };
    let _release_task = handle.spawn(async move {
        image_cache.release_hold_best_effort(&owner, reason).await;
    });
}

enum HardCommitGcDecision {
    Collectable {
        digest: HardCommitId,
        file: PathBuf,
        size: Option<u64>,
        p2p_keys: BTreeSet<P2pArtifactKey>,
    },
    Blocked(ImageCacheGcBlocked),
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;
    use std::sync::Arc;

    use overlaybd::backend::local::LocalFile;
    use overlaybd::index_file::{create_file_rw, LayerInfo};
    use overlaybd::virtual_file::VirtualFile;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::cfg::ResolvedImageCacheConfig;
    use crate::image::commit_index;
    use crate::image::ImageBaseContext;
    use crate::p2p::mock::MockTransport;
    use crate::p2p::{DisabledP2pTransport, P2pArtifactDescriptor, P2pArtifactProvider};

    fn test_service(temp: &TempDir) -> ImageCacheService {
        let root_dir = temp.path().join("image-cache");
        ImageCacheService::from_resolved_config(ResolvedImageCacheConfig {
            commit_store: root_dir.join("commits"),
            remote_blocks_dir: root_dir.join("remote-blocks"),
            root_dir,
            remote_blocks_size_gb: 10,
            capacity_bytes: None,
        })
    }

    fn image_config_json(repo_blob_url: &str, lowers: serde_json::Value) -> serde_json::Value {
        json!({
            "repoBlobUrl": repo_blob_url,
            "lowers": lowers,
            "upper": {},
            "resultFile": ""
        })
    }

    fn write_image_config(path: &Path, repo_blob_url: &str, lowers: serde_json::Value) {
        std::fs::create_dir_all(path.parent().expect("config parent")).expect("create config dir");
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&image_config_json(repo_blob_url, lowers))
                .expect("serialize config"),
        )
        .expect("write config");
    }

    fn write_config(path: &Path) {
        write_image_config(
            path,
            "https://registry.example/v2/repo/blobs",
            json!([{
                "file": "../commits/sha256-hard/overlaybd.commit",
                "digest": "sha256:hard",
                "size": 11
            }]),
        );
    }

    fn write_commit_file(service: &ImageCacheService, digest: &str, payload: &[u8]) -> PathBuf {
        let path = commit_index::commit_file(&service.commit_store, digest);
        std::fs::create_dir_all(path.parent().expect("commit parent")).expect("create commit dir");
        std::fs::write(&path, payload).expect("write commit file");
        path
    }

    async fn assert_imported_commit_is_collectable(
        service: &Arc<ImageCacheService>,
        cached: PathBuf,
        digest: &str,
        payload: &[u8],
    ) {
        assert_eq!(
            cached,
            commit_index::commit_file(&service.commit_store, digest)
        );
        assert_eq!(std::fs::read(&cached).expect("read cached commit"), payload);

        let report = service
            .run_gc(ImageCacheLiveRuntimeRefs::new(), true)
            .await
            .expect("run gc");
        assert_eq!(report.collected, 1);
        assert_eq!(report.freed_bytes, payload.len() as u64);
        assert!(!cached.exists());
    }

    async fn write_test_index(
        service: &ImageCacheService,
        source_digest: &str,
        commit_digest: &str,
    ) {
        let index = commit_index::CommitIndex::oci_layer(
            source_digest.to_string(),
            commit_digest.to_string(),
            4,
            "test-converter".to_string(),
            1,
            false,
            None,
        );
        index
            .write(&commit_index::index_path(
                &service.index_dir,
                source_digest,
                "test-converter",
                1,
                false,
                None,
            ))
            .await
            .expect("write index");
    }

    fn hard_refs(digests: impl IntoIterator<Item = &'static str>) -> BTreeSet<HardCommitId> {
        digests
            .into_iter()
            .map(|digest| HardCommitId::new(digest).expect("hard commit id"))
            .collect()
    }

    async fn write_sealed_layer(path: &Path, uuid: uuid::Uuid) {
        let index_path = path.with_extension("index");
        let data_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(path).expect("data file"));
        let index_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(index_path).expect("index file"));
        let mut info = LayerInfo::new(data_file, Some(index_file), 8192);
        info.uuid = uuid;
        let file = create_file_rw(info).await.expect("create rw layer");
        file.write_at(0, &[0x5a; 4096]).await.expect("write layer");
        file.close_seal().await.expect("seal layer");
    }

    /// Conversion key matching `write_test_index`'s context (converter
    /// `test-converter`, virtual size 1, no parent, mkfs false).
    fn test_conversion_key(source_digest: &str, expected_uuid: uuid::Uuid) -> LayerConversionKey {
        LayerConversionKey {
            source_layer_digest: source_digest.to_string(),
            converter_id: "test-converter".to_string(),
            virtual_size_gib: 1,
            mkfs: false,
            parent_commit_digest: None,
            expected_layer_uuid: expected_uuid,
        }
    }

    #[tokio::test]
    async fn lookup_converted_layer_misses_for_missing_or_invalid_commit() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        service.ensure_layout().await.expect("ensure layout");
        write_test_index(&service, "sha256:src", "sha256:commit-missing").await;
        write_test_index(&service, "sha256:bad-src", "sha256:commit-bad").await;
        // 4 bytes matches the indexed size, so the lookup passes the size gate and
        // is rejected purely because the bytes are not a sealed overlaybd layer.
        write_commit_file(&service, "sha256:commit-bad", b"junk");

        let mut hold = service.begin_hard_commit_operation("test").expect("hold");
        let key = test_conversion_key("sha256:src", uuid::Uuid::nil());
        assert!(hold
            .lookup_converted_layer(&key)
            .await
            .expect("lookup")
            .is_none());
        let key = test_conversion_key("sha256:bad-src", uuid::Uuid::nil());
        assert!(hold
            .lookup_converted_layer(&key)
            .await
            .expect("lookup")
            .is_none());
    }

    #[tokio::test]
    async fn publish_converted_layer_records_p2p_ownership() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        service.ensure_layout().await.expect("ensure layout");
        let transport = Arc::new(MockTransport::default());
        service.initialize_p2p_transport(transport.clone());

        let digest = "sha256:converted";
        let file = write_commit_file(&service, digest, b"converted");
        service
            .record_hard_commit_object(digest, Some(file.clone()), Some(9), &[])
            .await
            .expect("record converted commit");
        let key = test_conversion_key("sha256:source", uuid::Uuid::new_v4());
        let layer = LocalLayer {
            path: file,
            digest: digest.to_string(),
            size: 9,
        };

        assert!(service
            .publish_converted_layer(&key, &layer)
            .await
            .expect("publish converted layer"));
        let p2p_key = key.p2p_key();
        let descriptor = transport
            .descriptors
            .read()
            .await
            .get(&p2p_key)
            .cloned()
            .expect("published descriptor");
        assert_eq!(
            ConvertedLayerP2pMetadata::parse(&descriptor.metadata)
                .expect("metadata")
                .commit_digest,
            digest
        );
        let record = service
            .metadata_store()
            .await
            .expect("metadata store")
            .get_hard_commit_object(&HardCommitId::new(digest).expect("commit id"))
            .await
            .expect("read commit record")
            .expect("commit record");
        assert!(record.p2p_keys.contains(&p2p_key));
    }

    #[tokio::test]
    async fn disabled_p2p_publish_does_not_record_ownership() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        service.ensure_layout().await.expect("ensure layout");
        service.initialize_p2p_transport(Arc::new(DisabledP2pTransport));

        let digest = "sha256:disabled-p2p";
        let file = write_commit_file(&service, digest, b"converted");
        service
            .record_hard_commit_object(digest, Some(file.clone()), Some(9), &[])
            .await
            .expect("record converted commit");
        let key = test_conversion_key("sha256:disabled-source", uuid::Uuid::new_v4());
        let layer = LocalLayer {
            path: file,
            digest: digest.to_string(),
            size: 9,
        };

        assert!(!service
            .publish_converted_layer(&key, &layer)
            .await
            .expect("skip disabled P2P publish"));
        let record = service
            .metadata_store()
            .await
            .expect("metadata store")
            .get_hard_commit_object(&HardCommitId::new(digest).expect("commit id"))
            .await
            .expect("read commit record")
            .expect("commit record");
        assert!(record.p2p_keys.is_empty());
    }

    #[tokio::test]
    async fn failed_converted_layer_publish_keeps_gc_ownership_record() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        service.ensure_layout().await.expect("ensure layout");
        let transport = Arc::new(MockTransport::default());
        transport
            .fail_publish
            .store(true, std::sync::atomic::Ordering::Relaxed);
        service.initialize_p2p_transport(transport);

        let digest = "sha256:publish-failure";
        let file = write_commit_file(&service, digest, b"payload");
        service
            .record_hard_commit_object(digest, Some(file.clone()), Some(7), &[])
            .await
            .expect("record converted commit");
        let key = test_conversion_key("sha256:source-failure", uuid::Uuid::new_v4());
        let layer = LocalLayer {
            path: file,
            digest: digest.to_string(),
            size: 7,
        };

        assert!(service.publish_converted_layer(&key, &layer).await.is_err());
        let record = service
            .metadata_store()
            .await
            .expect("metadata store")
            .get_hard_commit_object(&HardCommitId::new(digest).expect("commit id"))
            .await
            .expect("read commit record")
            .expect("commit record");
        assert!(record.p2p_keys.contains(&key.p2p_key()));
    }

    #[tokio::test]
    async fn lookup_converted_layer_rehydrates_from_p2p() {
        let provider_temp = TempDir::new().expect("provider tempdir");
        let consumer_temp = TempDir::new().expect("consumer tempdir");
        let provider = Arc::new(test_service(&provider_temp));
        let consumer = Arc::new(test_service(&consumer_temp));
        provider.ensure_layout().await.expect("provider layout");
        consumer.ensure_layout().await.expect("consumer layout");
        let transport = Arc::new(MockTransport::default());
        provider.initialize_p2p_transport(transport.clone());
        consumer.initialize_p2p_transport(transport.clone());

        let key = test_conversion_key("sha256:source-p2p", uuid::Uuid::new_v4());
        let provider_path = provider_temp.path().join("provider.commit");
        write_sealed_layer(&provider_path, key.expected_layer_uuid).await;
        let described = FileDigest::describe(&provider_path)
            .await
            .expect("describe provider layer");
        let provider_digest = HardCommitId::new(&described.sha256).expect("provider digest");
        provider
            .record_hard_commit_object(
                &described.sha256,
                Some(provider_path.clone()),
                Some(described.size),
                &[],
            )
            .await
            .expect("record provider layer");
        provider
            .publish_converted_layer(
                &key,
                &LocalLayer {
                    path: provider_path,
                    digest: described.sha256.clone(),
                    size: described.size,
                },
            )
            .await
            .expect("publish provider layer");

        let mut hold = consumer
            .begin_hard_commit_operation("p2p-rehydrate")
            .expect("hold");
        let layer = hold
            .lookup_converted_layer(&key)
            .await
            .expect("lookup remote layer")
            .expect("remote layer");
        assert_eq!(layer.digest, described.sha256);
        assert_eq!(layer.size, described.size);
        assert!(layer.path.exists());
        let record = consumer
            .metadata_store()
            .await
            .expect("consumer metadata store")
            .get_hard_commit_object(&provider_digest)
            .await
            .expect("read consumer record")
            .expect("consumer commit record");
        assert!(record.p2p_keys.contains(&key.p2p_key()));
    }

    #[tokio::test]
    async fn failed_p2p_fetch_is_attempted_once_per_operation() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        service.ensure_layout().await.expect("ensure layout");
        let transport = Arc::new(MockTransport::default());
        service.initialize_p2p_transport(transport.clone());

        let key = test_conversion_key("sha256:stale-p2p", uuid::Uuid::new_v4());
        let p2p_key = key.p2p_key();
        let advertised_layer = LocalLayer {
            path: temp.path().join("missing.commit"),
            digest: "sha256:missing-p2p-blob".to_string(),
            size: 123,
        };
        transport.descriptors.write().await.insert(
            p2p_key.clone(),
            P2pArtifactDescriptor {
                key: p2p_key,
                providers: vec![P2pArtifactProvider::Local],
                backend_locator: Some("missing-mock-blob".to_string()),
                metadata: serde_json::to_value(ConvertedLayerP2pMetadata::from_local_layer(
                    &advertised_layer,
                ))
                .expect("serialize converted layer metadata"),
            },
        );

        let mut first_hold = service
            .begin_hard_commit_operation("first-p2p-fetch")
            .expect("first hold");
        for _ in 0..2 {
            assert!(first_hold
                .lookup_converted_layer(&key)
                .await
                .expect("lookup converted layer")
                .is_none());
        }
        assert_eq!(
            transport
                .lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            transport
                .fetch_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        drop(first_hold);

        let mut second_hold = service
            .begin_hard_commit_operation("second-p2p-fetch")
            .expect("second hold");
        assert!(second_hold
            .lookup_converted_layer(&key)
            .await
            .expect("retry lookup in new operation")
            .is_none());
        assert_eq!(
            transport
                .lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        assert_eq!(
            transport
                .fetch_count
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn import_hard_commit_publishes_facts_and_releases_operation_hold() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        let payload = b"trusted descriptor commit";
        let source = temp.path().join("trusted.commit");
        std::fs::write(&source, payload).expect("write source");
        let digest = "sha256:trusted-descriptor";

        let cached = service
            .import_hard_commit_trusted_descriptor(&source, digest, payload.len() as u64)
            .await
            .expect("import trusted hard commit");

        assert_imported_commit_is_collectable(&service, cached, digest, payload).await;
    }

    #[tokio::test]
    async fn publish_source_image_config_writes_files_and_records_refs() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        let manifest_digest = "sha256:manifest";
        let repository_scope = Some("repo/app");
        let commit_file = write_commit_file(&service, "sha256:layer", b"payload");
        let config = image_config_json(
            "",
            json!([{
                "file": commit_file.display().to_string(),
                "digest": "sha256:layer",
                "size": 7
            }]),
        );
        let metadata = ImageResolutionMetadata {
            base_context: ImageBaseContext {
                workdir: Some("/work".to_string()),
                ..Default::default()
            },
            raw_config: Some(json!({ "architecture": "x86_64" })),
        };

        let conversion = service
            .begin_image_conversion(manifest_digest, repository_scope)
            .await
            .expect("begin conversion");
        let published_path = service
            .publish_image_config(
                manifest_digest,
                repository_scope,
                &config,
                metadata.clone(),
                conversion,
            )
            .await
            .expect("publish source image config");

        assert!(published_path.exists());
        match service
            .lookup_image_config(manifest_digest, repository_scope)
            .await
            .expect("lookup cached config")
        {
            CachedImageConfig::Found {
                metadata: Some(cached_metadata),
                ..
            } => assert_eq!(*cached_metadata, metadata),
            CachedImageConfig::Found { metadata: None, .. } => {
                panic!("metadata should be cached")
            }
            CachedImageConfig::Missing => panic!("source config should be cached"),
        }
        let mut config_referrers = service
            .metadata_store()
            .await
            .expect("metadata store")
            .hard_commit_config_referrer_map()
            .await
            .expect("config referrers");
        assert_eq!(
            config_referrers
                .remove(&HardCommitId::new("sha256:layer").expect("hard commit id"))
                .unwrap_or_default(),
            vec![ImageCacheConfigId::from_config_path(&published_path).expect("config id")]
        );
    }

    #[test]
    fn overlaybd_layer_location_binds_file_for_cache_local_and_dir_for_remote() {
        let temp = TempDir::new().expect("tempdir");
        let service = test_service(&temp);
        let payload = b"layerbytes";
        let commit_file = write_commit_file(&service, "sha256:layer", payload);
        let size = payload.len() as u64;

        // Cache-local layer with a usable cached commit binds to `file=`.
        match service.overlaybd_layer_location("sha256:layer", size, false) {
            OverlaybdLayerLocation::LocalFile(path) => assert_eq!(path, commit_file),
            other => panic!("expected LocalFile, got {other:?}"),
        }

        // A remote-recoverable layer must use `dir=` even when the commit is
        // cached, so the local commit stays GC-reclaimable (overlaybd falls back
        // to the registry) — the GC-safety invariant.
        match service.overlaybd_layer_location("sha256:layer", size, true) {
            OverlaybdLayerLocation::CacheDir(path) => assert_eq!(
                path,
                commit_index::commit_dir(&service.commit_store, "sha256:layer")
            ),
            other => panic!("expected CacheDir for remote layer, got {other:?}"),
        }

        // Local-only layer without a usable cached commit falls back to `dir=`.
        match service.overlaybd_layer_location("sha256:absent", size, false) {
            OverlaybdLayerLocation::CacheDir(_) => {}
            other => panic!("expected CacheDir for missing commit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn run_gc_collects_free_candidate_and_skips_roots_holds_and_live_refs() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        let p2p = Arc::new(MockTransport::default());
        service.initialize_p2p_transport(p2p.clone());
        let free_file = write_commit_file(&service, "sha256:free", b"free");
        let rooted_file = write_commit_file(&service, "sha256:rooted", b"rooted");
        let held_file = write_commit_file(&service, "sha256:held", b"held");
        let live_file = write_commit_file(&service, "sha256:live", b"live");
        service
            .record_hard_commit_object(
                "sha256:free",
                Some(free_file.clone()),
                Some(4),
                &["overlaybd-layer/v1/explicit-free".to_string()],
            )
            .await
            .expect("record free object");
        service
            .record_hard_commit_object("sha256:rooted", Some(rooted_file.clone()), Some(6), &[])
            .await
            .expect("record rooted object");
        service
            .record_hard_commit_object("sha256:held", Some(held_file.clone()), Some(4), &[])
            .await
            .expect("record held object");
        service
            .record_hard_commit_object("sha256:live", Some(live_file.clone()), Some(4), &[])
            .await
            .expect("record live object");
        let source_config = service.config_dir.join("base-image.json");
        write_image_config(
            &source_config,
            "",
            json!([{
                "file": rooted_file.display().to_string(),
                "digest": "sha256:rooted",
                "size": 6
            }]),
        );
        service
            .record_source_config_refs_from_config_path(&source_config)
            .await
            .expect("record source config root");
        let paused_owner =
            ImageCacheHoldOwner::new("paused", "sandbox-paused").expect("paused owner");
        service
            .create_or_replace_hold(paused_owner.clone(), hard_refs(["sha256:held"]))
            .await
            .expect("record paused hold");

        let live_owner = ImageCacheHoldOwner::new("runtime", "sandbox-live").expect("live owner");
        let report = service
            .run_gc(
                BTreeMap::from([(
                    HardCommitId::new("sha256:live").expect("live object"),
                    vec![live_owner.clone()],
                )]),
                true,
            )
            .await
            .expect("run gc");

        assert_eq!(report.collected, 1);
        assert_eq!(report.freed_bytes, 4);
        assert!(!free_file.exists());
        assert_eq!(
            *p2p.unpublished_keys.read().await,
            vec!["overlaybd-layer/v1/explicit-free".to_string()]
        );
        assert!(held_file.exists());
        assert!(live_file.exists());
        assert!(rooted_file.exists());

        let reasons = report
            .blocked
            .iter()
            .map(|blocked| (blocked.digest.as_str().to_string(), blocked.reason.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            reasons.get("sha256:held"),
            Some(&ImageCacheGcBlockedReason::Held {
                owners: vec![paused_owner]
            })
        );
        assert_eq!(
            reasons.get("sha256:live"),
            Some(&ImageCacheGcBlockedReason::LiveRuntime {
                owners: vec![live_owner]
            })
        );
        assert_eq!(
            reasons.get("sha256:rooted"),
            Some(&ImageCacheGcBlockedReason::RootedByConfig {
                configs: vec!["base-image.json".to_string()]
            })
        );
    }

    #[tokio::test]
    async fn run_gc_refuses_p2p_object_without_initialized_transport() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        let file = write_commit_file(&service, "sha256:p2p", b"p2p");
        service
            .record_hard_commit_object(
                "sha256:p2p",
                Some(file.clone()),
                Some(3),
                &["overlaybd-layer/v1/p2p".to_string()],
            )
            .await
            .expect("record P2P object");

        let report = service.run_gc(BTreeMap::new(), true).await.expect("run gc");

        assert_eq!(report.collected, 0);
        assert!(file.exists());
        assert!(report.blocked.iter().any(|blocked| {
            blocked.digest.as_str() == "sha256:p2p"
                && matches!(
                    &blocked.reason,
                    ImageCacheGcBlockedReason::DeleteFailed(reason)
                        if reason.contains("P2P transport is not initialized")
                )
        }));
    }

    #[tokio::test]
    async fn runtime_config_hold_tracks_only_commit_store_commits() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        let leased = write_commit_file(&service, "sha256:leased", b"leased");
        let outside_file = temp.path().join("elsewhere/foreign.commit");
        let outside_file_only = temp.path().join("elsewhere/runtime.commit");
        service
            .record_hard_commit_object("sha256:leased", Some(leased.clone()), Some(6), &[])
            .await
            .expect("record leased object");

        let runtime_config = temp.path().join("runtime/image.json");
        write_image_config(
            &runtime_config,
            "https://registry.example/v2/repo/blobs",
            json!([
                {
                    "file": leased.display().to_string(),
                    "digest": "sha256:leased",
                    "size": 6
                },
                {
                    "file": outside_file.display().to_string(),
                    "digest": "sha256:outside",
                    "size": 7
                },
                {
                    "file": outside_file_only.display().to_string()
                },
                {
                    "dir": "../commits/sha256-remote",
                    "digest": "sha256:remote",
                    "size": 9
                }
            ]),
        );

        let owner = ImageCacheHoldOwner::new("runtime", "sandbox-running").expect("runtime owner");
        service
            .acquire_hold_for_config_paths(owner.clone(), [runtime_config])
            .await
            .expect("acquire runtime lease");

        let metadata = service.metadata_store().await.expect("metadata store");
        assert_eq!(
            metadata
                .hard_commit_hold_referrers(&HardCommitId::new("sha256:leased").unwrap())
                .await
                .expect("leased referrers"),
            vec![owner.clone()]
        );
        for digest in ["sha256:outside", "sha256:remote"] {
            assert!(metadata
                .hard_commit_hold_referrers(&HardCommitId::new(digest).unwrap())
                .await
                .expect("excluded referrers")
                .is_empty());
        }

        let report = service
            .run_gc(ImageCacheLiveRuntimeRefs::new(), true)
            .await
            .expect("run gc");

        assert_eq!(report.collected, 0);
        assert!(leased.exists());
        let reasons = report
            .blocked
            .iter()
            .map(|blocked| (blocked.digest.as_str().to_string(), blocked.reason.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            reasons.get("sha256:leased"),
            Some(&ImageCacheGcBlockedReason::Held {
                owners: vec![owner]
            })
        );
    }

    #[tokio::test]
    async fn runtime_config_hold_fails_closed_for_descriptorless_commit_store_file() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        let commit_file = write_commit_file(&service, "sha256:missing-descriptor", b"payload");
        let runtime_config = temp.path().join("runtime/image.json");
        write_image_config(
            &runtime_config,
            "",
            json!([{
                "file": commit_file.display().to_string()
            }]),
        );

        let owner = ImageCacheHoldOwner::new("runtime", "sandbox-running").expect("runtime owner");
        let err = service
            .acquire_hold_for_config_paths(owner, [runtime_config])
            .await
            .expect_err("commit-store local file without digest must fail closed");

        assert!(
            format!("{err:#}").contains("commit-store file but no digest"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn runtime_config_hold_fails_closed_for_sizeless_commit_store_file() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        let commit_file = write_commit_file(&service, "sha256:missing-size", b"payload");
        let runtime_config = temp.path().join("runtime/image.json");
        write_image_config(
            &runtime_config,
            "",
            json!([{
                "file": commit_file.display().to_string(),
                "digest": "sha256:missing-size"
            }]),
        );

        let owner = ImageCacheHoldOwner::new("runtime", "sandbox-running").expect("runtime owner");
        let err = service
            .acquire_hold_for_config_paths(owner, [runtime_config])
            .await
            .expect_err("commit-store local file without size must fail closed");

        assert!(
            format!("{err:#}").contains("commit-store file but no size"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn cleanup_stale_runtime_holds_clears_only_runtime_namespaces() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        let file = write_commit_file(&service, "sha256:held", b"held");
        service
            .record_hard_commit_object("sha256:held", Some(file.clone()), Some(4), &[])
            .await
            .expect("record fact");

        let runtime_owner =
            ImageCacheHoldOwner::new("runtime", "sandbox-1").expect("runtime owner");
        let paused_owner =
            ImageCacheHoldOwner::new("paused", "sandbox-paused").expect("paused owner");
        let operation_owner =
            ImageCacheHoldOwner::new("operation", "oci-conversion/1").expect("operation owner");
        let config = service.config_dir.join("base-image.json");
        write_image_config(
            &config,
            "",
            json!([{
                "file": file.display().to_string(),
                "digest": "sha256:held",
                "size": 4
            }]),
        );
        service
            .record_source_config_refs_from_config_path(&config)
            .await
            .expect("source config root");
        for owner in [&runtime_owner, &paused_owner, &operation_owner] {
            service
                .create_or_replace_hold(owner.clone(), hard_refs(["sha256:held"]))
                .await
                .expect("runtime hold");
        }

        let released = service
            .cleanup_stale_runtime_holds()
            .await
            .expect("cleanup stale holds");
        assert_eq!(released, 2);

        // Runtime/operation leases gone; the source config root
        // and the paused hold (durable across restart) survive.
        let digest = HardCommitId::new("sha256:held").expect("hard commit");
        let metadata = service.metadata_store().await.expect("metadata store");
        let mut config_referrers = metadata
            .hard_commit_config_referrer_map()
            .await
            .expect("config refs");
        assert_eq!(
            config_referrers
                .remove(&HardCommitId::new("sha256:held").expect("hard commit id"))
                .unwrap_or_default(),
            vec![ImageCacheConfigId::from_config_path(&config).expect("config id")]
        );
        assert_eq!(
            metadata
                .hard_commit_hold_referrers(&digest)
                .await
                .expect("hold referrers"),
            vec![paused_owner]
        );
        assert_eq!(
            service
                .list_hold_keys_in_namespace("paused")
                .await
                .expect("list paused holds"),
            vec!["sandbox-paused".to_string()]
        );

        let report = service
            .run_gc(ImageCacheLiveRuntimeRefs::new(), true)
            .await
            .expect("run gc");
        assert_eq!(report.collected, 0);
        assert!(file.exists());
    }

    #[tokio::test]
    async fn maintenance_sweeps_stale_indexes() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        write_commit_file(&service, "sha256:live", b"live");
        write_test_index(&service, "sha256:src-live", "sha256:live").await;
        write_test_index(&service, "sha256:src-dead", "sha256:dead").await;

        let removed = service
            .reclaim_stale_indexes()
            .await
            .expect("reclaim stale indexes");

        assert_eq!(removed, 1);
        assert!(service
            .index_dir
            .join(commit_index::digest_slug("sha256:src-live"))
            .exists());
        assert!(!service
            .index_dir
            .join(commit_index::digest_slug("sha256:src-dead"))
            .exists());
    }

    #[tokio::test]
    async fn evict_source_configs_over_capacity_removes_unrooted_config() {
        let temp = TempDir::new().expect("tempdir");
        let service = Arc::new(test_service(&temp));
        let config_path = service.config_dir.join("base-image.json");
        write_config(&config_path);

        // High/low watermark 0 selects every config; eviction un-roots it by
        // removing the source config file so the hard-commit GC can reclaim it.
        service
            .evict_source_configs_over_capacity(0, 0, Duration::from_secs(0))
            .await
            .expect("eviction");
        assert!(!config_path.exists());
    }
}
