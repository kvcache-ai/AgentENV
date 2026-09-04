use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use overlaybd::config::{
    lexically_normalize_path, load_image_config as load_overlaybd_image_config,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::warn;

use crate::local_store::{LocalKvBatchOp, LocalKvStore, LocalStoreDurability};
use crate::p2p::P2pArtifactKey;

const SCHEMA_VERSION: u32 = 1;
const SCHEMA_VERSION_KEY: &[u8] = b"schema/version";
const HARD_COMMIT_OBJECT_PREFIX: &[u8] = b"object/hard-commit/";
const HOLD_RECORD_PREFIX: &[u8] = b"hold/";
const CONFIG_TO_HARD_PREFIX: &[u8] = b"ref/config-to-hard/";
const HOLD_TO_HARD_PREFIX: &[u8] = b"ref/hold-to-hard/";
const HARD_TO_HOLD_PREFIX: &[u8] = b"ref/hard-to-hold/";
const CONFIG_LAST_USED_PREFIX: &[u8] = b"config-last-used/";
const IMAGE_CONFIG_SUFFIX: &str = "-image.json";

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct ImageCacheHoldOwner {
    namespace: String,
    key: String,
}

impl ImageCacheHoldOwner {
    pub(crate) fn new(namespace: impl Into<String>, key: impl Into<String>) -> Result<Self> {
        Ok(Self {
            namespace: non_empty("image cache hold namespace", namespace.into())?,
            key: non_empty("image cache hold key", key.into())?,
        })
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }
}

impl fmt::Display for ImageCacheHoldOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.key)
    }
}

pub(super) fn non_empty(field: &str, value: String) -> Result<String> {
    if value.trim().is_empty() {
        bail!("{field} is empty");
    }
    Ok(value)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CacheOwnedLocalFileLower {
    pub(super) digest: String,
    pub(super) file: PathBuf,
    pub(super) size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CacheOwnedImageConfigFacts {
    pub(super) local_file_lowers: Vec<CacheOwnedLocalFileLower>,
    pub(super) upper_file: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub(crate) struct ImageCacheMetadataStore {
    store: LocalKvStore,
    object_update_lock: Arc<Mutex<()>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ImageCacheConfigId(String);

impl ImageCacheConfigId {
    pub(crate) fn from_config_path(path: &Path) -> Result<Self> {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .with_context(|| {
                format!(
                    "image cache config path has no file name: {}",
                    path.display()
                )
            })?;
        Self::from_filename(filename)
    }

    fn from_filename(filename: impl Into<String>) -> Result<Self> {
        let filename = filename.into();
        if !is_regular_config_filename(&filename) {
            bail!(
                "image cache config '{}' is not a source image config",
                filename
            );
        }
        Ok(Self(filename))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ImageCacheConfigId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct HardCommitId(String);

impl HardCommitId {
    pub(crate) fn new(digest: impl Into<String>) -> Result<Self> {
        let digest = digest.into();
        if digest.trim().is_empty() {
            bail!("hard commit digest is empty");
        }
        Ok(Self(digest))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HardCommitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct HardCommitObjectRecord {
    pub(crate) digest: HardCommitId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) size: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(crate) p2p_keys: BTreeSet<P2pArtifactKey>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ConfigReferenceRecord {
    config_id: ImageCacheConfigId,
    digest: HardCommitId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ConfigLastUsedRecord {
    config_id: ImageCacheConfigId,
    last_used: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CapacityEvictionPlan {
    pub(crate) candidates: Vec<CapacityEvictionCandidate>,
    pub(crate) total_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapacityEvictionCandidate {
    pub(crate) config_id: ImageCacheConfigId,
    pub(crate) last_used: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct HoldRecord {
    namespace: String,
    key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct HoldHardCommitReferenceRecord {
    namespace: String,
    key: String,
    digest: HardCommitId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedHardCommitRef {
    digest: HardCommitId,
    file: PathBuf,
    size: u64,
}

impl ImageCacheMetadataStore {
    pub(crate) async fn open(
        path: impl Into<PathBuf>,
        durability: LocalStoreDurability,
    ) -> Result<Self> {
        let store = LocalKvStore::open(path, durability).await?;
        let metadata = Self {
            store,
            object_update_lock: Arc::new(Mutex::new(())),
        };
        metadata.ensure_schema_version().await?;
        Ok(metadata)
    }

    pub(crate) async fn record_hard_commit_object(
        &self,
        digest: HardCommitId,
        file: Option<PathBuf>,
        size: Option<u64>,
        p2p_keys: BTreeSet<P2pArtifactKey>,
    ) -> Result<()> {
        let _guard = self.object_update_lock.lock().await;
        let mut p2p_keys = p2p_keys;
        if let Some(existing) = self.get_hard_commit_object(&digest).await? {
            p2p_keys.extend(existing.p2p_keys);
        }
        self.store
            .put(
                hard_commit_object_key(&digest),
                serde_json::to_vec(&HardCommitObjectRecord {
                    digest: digest.clone(),
                    file,
                    size,
                    p2p_keys,
                })
                .context("serialize hard commit object record")?,
            )
            .await
            .with_context(|| format!("record hard commit object {digest}"))
    }

    pub(crate) async fn record_config_refs_from_config_path(
        &self,
        config_path: &Path,
    ) -> Result<()> {
        let config_id = ImageCacheConfigId::from_config_path(config_path)?;
        let hard_refs = load_cache_owned_hard_commit_refs(config_path)?;
        let new_refs = hard_refs
            .iter()
            .map(|reference| reference.digest.clone())
            .collect::<BTreeSet<_>>();
        let _guard = self.object_update_lock.lock().await;
        let existing = self.config_refs_set(&config_id).await?;
        let mut ops = config_ref_replacement_ops(&config_id, &existing, &new_refs)?;
        for reference in &hard_refs {
            let p2p_keys = self
                .get_hard_commit_object(&reference.digest)
                .await?
                .map(|record| record.p2p_keys)
                .unwrap_or_default();
            ops.push(hard_commit_object_put_op(
                &reference.digest,
                Some(reference.file.clone()),
                Some(reference.size),
                p2p_keys,
            )?);
        }
        // This path runs on every resolve (cache hit via record_source_image
        // config root and miss via publish), so it doubles as the LRU "touch".
        // `rebuild_from_configs` does not call it, so reconciles never reset it.
        if new_refs.is_empty() {
            ops.push(LocalKvBatchOp::delete(config_last_used_key(&config_id)));
        } else {
            ops.push(config_last_used_put_op(&config_id, unix_now_secs())?);
        }

        self.store
            .write_batch(ops)
            .await
            .with_context(|| format!("record image cache refs for config {config_id}"))
    }

    /// Cache-owned `file=` lowers inside the commit store; remote-recoverable
    /// `dir=` layers and runtime-owned local files are intentionally excluded.
    pub(crate) fn commit_store_hard_commit_digests_from_config_path(
        config_path: &Path,
        commit_store: &Path,
    ) -> Result<Vec<HardCommitId>> {
        Ok(
            load_commit_store_owned_hard_commit_refs(config_path, commit_store)?
                .into_iter()
                .map(|reference| reference.digest)
                .collect(),
        )
    }

    pub(crate) async fn remove_hard_commit_object(&self, digest: &HardCommitId) -> Result<()> {
        self.store
            .delete(hard_commit_object_key(digest))
            .await
            .with_context(|| format!("remove hard commit object {digest}"))
    }

    pub(crate) async fn add_hard_commit_p2p_key(
        &self,
        digest: &HardCommitId,
        key: &P2pArtifactKey,
    ) -> Result<()> {
        let _guard = self.object_update_lock.lock().await;
        let Some(mut record) = self.get_hard_commit_object(digest).await? else {
            bail!("hard commit object {digest} does not exist while recording P2P key '{key}'");
        };
        if !record.p2p_keys.insert(key.clone()) {
            return Ok(());
        }
        self.store
            .put(
                hard_commit_object_key(digest),
                serde_json::to_vec(&record).context("serialize hard commit object record")?,
            )
            .await
            .with_context(|| format!("record P2P key for hard commit {digest}"))?;
        Ok(())
    }

    pub(crate) async fn get_hard_commit_object(
        &self,
        digest: &HardCommitId,
    ) -> Result<Option<HardCommitObjectRecord>> {
        let key = hard_commit_object_key(digest);
        let Some(value) = self.store.get(key.clone()).await? else {
            return Ok(None);
        };
        Ok(Some(decode_json(
            &key,
            &value,
            "hard commit object record",
        )?))
    }

    async fn scan_decode<T>(&self, prefix: &[u8], record_type: &'static str) -> Result<Vec<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut records = Vec::new();
        for (key, value) in self.store.scan_prefix(prefix.to_vec()).await? {
            records.push(decode_json(&key, &value, record_type)?);
        }
        Ok(records)
    }

    pub(crate) async fn list_hard_commit_objects(&self) -> Result<Vec<HardCommitObjectRecord>> {
        let mut records = self
            .scan_decode::<HardCommitObjectRecord>(
                HARD_COMMIT_OBJECT_PREFIX,
                "hard commit object record",
            )
            .await?;
        records.sort_by(|left, right| left.digest.cmp(&right.digest));
        Ok(records)
    }

    pub(crate) async fn create_or_replace_hold(
        &self,
        owner: &ImageCacheHoldOwner,
        refs: &BTreeSet<HardCommitId>,
    ) -> Result<()> {
        let existing = self.hold_refs_set(owner).await?;
        let mut ops = hold_ref_replacement_ops(owner, &existing, refs)?;
        ops.push(hold_record_put_op(owner)?);

        self.store
            .write_batch(ops)
            .await
            .with_context(|| format!("replace image cache hold {owner}"))
    }

    pub(crate) async fn release_hold(&self, owner: &ImageCacheHoldOwner) -> Result<()> {
        let existing = self.hold_refs_set(owner).await?;
        let ops = hold_ref_removal_ops(owner, &existing);
        self.store
            .write_batch(ops)
            .await
            .with_context(|| format!("release image cache hold {owner}"))
    }

    pub(crate) async fn list_hold_owners_in_namespaces(
        &self,
        namespaces: &[&str],
    ) -> Result<Vec<ImageCacheHoldOwner>> {
        let mut owners = Vec::new();
        for record in self
            .scan_decode::<HoldRecord>(HOLD_RECORD_PREFIX, "hold record")
            .await?
        {
            if namespaces.contains(&record.namespace.as_str()) {
                owners.push(ImageCacheHoldOwner::new(record.namespace, record.key)?);
            }
        }
        Ok(owners)
    }

    /// Startup cleanup for transient namespaces. Do not pass durable namespaces.
    pub(crate) async fn release_holds_in_namespaces(
        &self,
        namespaces: &[&str],
    ) -> Result<Vec<ImageCacheHoldOwner>> {
        let owners = self.list_hold_owners_in_namespaces(namespaces).await?;
        let mut ops = Vec::new();
        for owner in &owners {
            ops.extend(hold_ref_removal_ops(
                owner,
                &self.hold_refs_set(owner).await?,
            ));
        }
        if !ops.is_empty() {
            self.store
                .write_batch(ops)
                .await
                .context("release stale image cache holds at startup")?;
        }
        Ok(owners)
    }

    pub(crate) async fn hard_commit_hold_referrers(
        &self,
        digest: &HardCommitId,
    ) -> Result<Vec<ImageCacheHoldOwner>> {
        let prefix = hard_to_hold_prefix_for_digest(digest);
        let mut owners = BTreeSet::new();
        for record in self
            .scan_decode::<HoldHardCommitReferenceRecord>(&prefix, "hard-to-hold reference")
            .await?
        {
            owners.insert(ImageCacheHoldOwner::new(record.namespace, record.key)?);
        }
        Ok(owners.into_iter().collect())
    }

    pub(crate) async fn hard_commit_config_referrer_map(
        &self,
    ) -> Result<BTreeMap<HardCommitId, Vec<ImageCacheConfigId>>> {
        let mut referrers = BTreeMap::<HardCommitId, Vec<ImageCacheConfigId>>::new();
        for (config_id, refs) in self.config_ref_map().await? {
            for digest in refs {
                referrers.entry(digest).or_default().push(config_id.clone());
            }
        }
        for configs in referrers.values_mut() {
            configs.sort();
            configs.dedup();
        }
        Ok(referrers)
    }

    pub(crate) async fn hard_commit_config_referrers(
        &self,
        digest: &HardCommitId,
    ) -> Result<Vec<ImageCacheConfigId>> {
        let mut referrers = BTreeSet::new();
        for (config_id, refs) in self.config_ref_map().await? {
            if refs.contains(digest) {
                referrers.insert(config_id);
            }
        }
        Ok(referrers.into_iter().collect())
    }

    pub(crate) async fn remove_config_refs(&self, config_id: &ImageCacheConfigId) -> Result<()> {
        let existing = self.config_refs_set(config_id).await?;
        self.store
            .write_batch(config_ref_removal_ops(config_id, &existing))
            .await
            .with_context(|| format!("remove image cache refs for config {config_id}"))
    }

    pub(crate) async fn rebuild_from_configs(&self, configs_dir: &Path) -> Result<()> {
        let parsed = parse_configs_dir(configs_dir).await.with_context(|| {
            format!(
                "rebuild image cache metadata from {}",
                configs_dir.display()
            )
        })?;
        let _guard = self.object_update_lock.lock().await;
        let existing = self.config_ref_map().await?;
        let existing_last_used = self.config_last_used_map().await?;
        let now = unix_now_secs();
        let mut ops = Vec::new();

        for stale_config in existing.keys().filter(|id| !parsed.contains_key(*id)) {
            ops.extend(config_ref_removal_ops(
                stale_config,
                existing.get(stale_config).expect("iterated existing key"),
            ));
        }

        for (config_id, hard_refs) in &parsed {
            let new_refs = hard_refs
                .iter()
                .map(|reference| reference.digest.clone())
                .collect::<BTreeSet<_>>();
            let old_refs = existing.get(config_id).cloned().unwrap_or_default();
            ops.extend(config_ref_replacement_ops(config_id, &old_refs, &new_refs)?);
            for reference in hard_refs {
                let p2p_keys = self
                    .get_hard_commit_object(&reference.digest)
                    .await?
                    .map(|record| record.p2p_keys)
                    .unwrap_or_default();
                ops.push(hard_commit_object_put_op(
                    &reference.digest,
                    Some(reference.file.clone()),
                    Some(reference.size),
                    p2p_keys,
                )?);
            }
            // Rebuilds preserve recency; new configs get a baseline touch.
            if new_refs.is_empty() {
                ops.push(LocalKvBatchOp::delete(config_last_used_key(config_id)));
            } else if !existing_last_used.contains_key(config_id) {
                ops.push(config_last_used_put_op(config_id, now)?);
            }
        }

        self.store.write_batch(ops).await.with_context(|| {
            format!(
                "write image cache metadata rebuild from {}",
                configs_dir.display()
            )
        })
    }

    async fn ensure_schema_version(&self) -> Result<()> {
        match self.store.get(SCHEMA_VERSION_KEY).await? {
            Some(bytes) => {
                match serde_json::from_slice::<u32>(&bytes) {
                    Ok(version) if version == SCHEMA_VERSION => Ok(()),
                    Ok(version) => {
                        self.reset_schema_version(&format!(
                            "unsupported image cache metadata schema version {version}; expected {SCHEMA_VERSION}"
                        ))
                        .await
                    }
                    Err(error) => {
                        self.reset_schema_version(&format!(
                            "invalid image cache metadata schema version: {error}"
                        ))
                        .await
                    }
                }
            }
            None => self.write_schema_version().await,
        }
    }

    async fn write_schema_version(&self) -> Result<()> {
        self.store
            .put(
                SCHEMA_VERSION_KEY,
                serde_json::to_vec(&SCHEMA_VERSION)
                    .context("serialize image cache metadata schema version")?,
            )
            .await
            .context("write image cache metadata schema version")
    }

    async fn reset_schema_version(&self, reason: &str) -> Result<()> {
        let mut ops = self
            .store
            .entries()
            .await
            .with_context(|| format!("scan image cache metadata before reset: {reason}"))?
            .into_iter()
            .map(|(key, _)| LocalKvBatchOp::delete(key))
            .collect::<Vec<_>>();
        warn!(
            reason,
            entries = ops.len(),
            "wiping image-cache metadata store due to schema version mismatch"
        );
        ops.push(schema_version_put_op()?);
        self.store
            .write_batch(ops)
            .await
            .with_context(|| format!("reset image cache metadata store: {reason}"))
    }

    async fn config_refs_set(
        &self,
        config_id: &ImageCacheConfigId,
    ) -> Result<BTreeSet<HardCommitId>> {
        let prefix = config_to_hard_prefix_for_config(config_id);
        let mut refs = BTreeSet::new();
        for record in self
            .scan_decode::<ConfigReferenceRecord>(&prefix, "config-to-hard reference")
            .await?
        {
            refs.insert(record.digest);
        }
        Ok(refs)
    }

    async fn config_ref_map(&self) -> Result<BTreeMap<ImageCacheConfigId, BTreeSet<HardCommitId>>> {
        let mut refs = BTreeMap::<ImageCacheConfigId, BTreeSet<HardCommitId>>::new();
        for record in self
            .scan_decode::<ConfigReferenceRecord>(CONFIG_TO_HARD_PREFIX, "config-to-hard reference")
            .await?
        {
            refs.entry(record.config_id)
                .or_default()
                .insert(record.digest);
        }
        Ok(refs)
    }

    async fn config_last_used_map(&self) -> Result<BTreeMap<ImageCacheConfigId, u64>> {
        let mut last_used = BTreeMap::new();
        for record in self
            .scan_decode::<ConfigLastUsedRecord>(CONFIG_LAST_USED_PREFIX, "config last-used record")
            .await?
        {
            last_used.insert(record.config_id, record.last_used);
        }
        Ok(last_used)
    }

    pub(crate) async fn config_last_used(
        &self,
        config_id: &ImageCacheConfigId,
    ) -> Result<Option<u64>> {
        let key = config_last_used_key(config_id);
        let Some(value) = self.store.get(key.clone()).await? else {
            return Ok(None);
        };
        let record: ConfigLastUsedRecord = decode_json(&key, &value, "config last-used record")?;
        if &record.config_id != config_id {
            bail!(
                "config last-used key mismatch: requested {config_id}, record has {}",
                record.config_id
            );
        }
        Ok(Some(record.last_used))
    }

    /// LRU source-config eviction plan. The freed estimate ignores holds, so any
    /// shortfall is retried next pass by hard-commit GC.
    pub(crate) async fn plan_capacity_eviction(
        &self,
        high_watermark_bytes: u64,
        low_watermark_bytes: u64,
        evictable_before: u64,
    ) -> Result<CapacityEvictionPlan> {
        let config_refs = self.config_ref_map().await?;
        let sizes: BTreeMap<HardCommitId, u64> = self
            .list_hard_commit_objects()
            .await?
            .into_iter()
            .map(|record| (record.digest, record.size.unwrap_or(0)))
            .collect();
        let total_bytes: u64 = sizes.values().copied().sum();
        if total_bytes <= high_watermark_bytes {
            return Ok(CapacityEvictionPlan {
                total_bytes,
                ..Default::default()
            });
        }

        let mut referrers: BTreeMap<HardCommitId, BTreeSet<ImageCacheConfigId>> = BTreeMap::new();
        for (config_id, commits) in &config_refs {
            for commit in commits {
                referrers
                    .entry(commit.clone())
                    .or_default()
                    .insert(config_id.clone());
            }
        }

        let last_used = self.config_last_used_map().await?;
        let mut evictable: Vec<(u64, ImageCacheConfigId)> = config_refs
            .keys()
            .filter_map(|config_id| {
                last_used
                    .get(config_id)
                    .filter(|&&used| used <= evictable_before)
                    .map(|&used| (used, config_id.clone()))
            })
            .collect();
        evictable.sort();

        let mut evicting = BTreeSet::new();
        let mut covered: BTreeSet<HardCommitId> = BTreeSet::new();
        let mut estimated_freed_bytes = 0u64;
        let mut candidates = Vec::new();
        for (last_used, config_id) in evictable {
            if total_bytes.saturating_sub(estimated_freed_bytes) <= low_watermark_bytes {
                break;
            }
            evicting.insert(config_id.clone());
            if let Some(commits) = config_refs.get(&config_id) {
                for commit in commits {
                    if covered.contains(commit) {
                        continue;
                    }
                    if referrers
                        .get(commit)
                        .is_some_and(|refs| refs.is_subset(&evicting))
                    {
                        covered.insert(commit.clone());
                        estimated_freed_bytes += sizes.get(commit).copied().unwrap_or(0);
                    }
                }
            }
            candidates.push(CapacityEvictionCandidate {
                config_id,
                last_used,
            });
        }

        Ok(CapacityEvictionPlan {
            candidates,
            total_bytes,
        })
    }

    async fn hold_refs_set(&self, owner: &ImageCacheHoldOwner) -> Result<BTreeSet<HardCommitId>> {
        let prefix = hold_to_hard_prefix_for_owner(owner);
        let mut refs = BTreeSet::new();
        for record in self
            .scan_decode::<HoldHardCommitReferenceRecord>(&prefix, "hold-to-hard reference")
            .await?
        {
            refs.insert(record.digest);
        }
        Ok(refs)
    }
}

fn push_mirrored_delete(ops: &mut Vec<LocalKvBatchOp>, forward: Vec<u8>, reverse: Vec<u8>) {
    ops.push(LocalKvBatchOp::delete(forward));
    ops.push(LocalKvBatchOp::delete(reverse));
}

fn put_json_op<T: Serialize>(
    key: Vec<u8>,
    record: &T,
    context: &'static str,
) -> Result<LocalKvBatchOp> {
    Ok(LocalKvBatchOp::put(
        key,
        serde_json::to_vec(record).context(context)?,
    ))
}

fn push_mirrored_put_json<T: Serialize>(
    ops: &mut Vec<LocalKvBatchOp>,
    forward: Vec<u8>,
    reverse: Vec<u8>,
    record: &T,
    context: &'static str,
) -> Result<()> {
    let value = serde_json::to_vec(record).context(context)?;
    ops.push(LocalKvBatchOp::put(forward, value.clone()));
    ops.push(LocalKvBatchOp::put(reverse, value));
    Ok(())
}

fn config_ref_replacement_ops(
    config_id: &ImageCacheConfigId,
    existing: &BTreeSet<HardCommitId>,
    refs: &BTreeSet<HardCommitId>,
) -> Result<Vec<LocalKvBatchOp>> {
    let mut ops = Vec::new();
    for digest in existing.difference(refs) {
        ops.push(LocalKvBatchOp::delete(config_to_hard_key(
            config_id, digest,
        )));
    }
    for digest in refs.difference(existing) {
        ops.push(put_json_op(
            config_to_hard_key(config_id, digest),
            &ConfigReferenceRecord {
                config_id: config_id.clone(),
                digest: digest.clone(),
            },
            "serialize image cache reference",
        )?);
    }
    Ok(ops)
}

fn config_ref_removal_ops(
    config_id: &ImageCacheConfigId,
    existing: &BTreeSet<HardCommitId>,
) -> Vec<LocalKvBatchOp> {
    let mut ops = Vec::new();
    for digest in existing {
        ops.push(LocalKvBatchOp::delete(config_to_hard_key(
            config_id, digest,
        )));
    }
    ops.push(LocalKvBatchOp::delete(config_last_used_key(config_id)));
    ops
}

fn config_last_used_put_op(
    config_id: &ImageCacheConfigId,
    last_used: u64,
) -> Result<LocalKvBatchOp> {
    put_json_op(
        config_last_used_key(config_id),
        &ConfigLastUsedRecord {
            config_id: config_id.clone(),
            last_used,
        },
        "serialize config last-used record",
    )
}

fn schema_version_put_op() -> Result<LocalKvBatchOp> {
    put_json_op(
        SCHEMA_VERSION_KEY.to_vec(),
        &SCHEMA_VERSION,
        "serialize image cache metadata schema version",
    )
}

pub(super) fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

pub(super) fn load_cache_owned_image_config_facts(
    image_config_path: &Path,
) -> Result<CacheOwnedImageConfigFacts> {
    let image_config = load_overlaybd_image_config(image_config_path)
        .with_context(|| format!("load image config {}", image_config_path.display()))?;

    let mut local_file_lowers = Vec::new();
    for (idx, lower) in image_config.lowers.into_iter().enumerate() {
        if lower.file.is_empty() {
            continue;
        }
        if lower.digest.is_empty() {
            bail!(
                "image config {} lower {idx} has local file but no digest",
                image_config_path.display()
            );
        }
        if lower.size == 0 {
            bail!(
                "image config {} lower {idx} has local file but no size",
                image_config_path.display()
            );
        }
        local_file_lowers.push(CacheOwnedLocalFileLower {
            digest: lower.digest,
            file: PathBuf::from(lower.file),
            size: lower.size,
        });
    }

    let upper_file =
        (!image_config.upper.data.is_empty()).then(|| PathBuf::from(image_config.upper.data));

    Ok(CacheOwnedImageConfigFacts {
        local_file_lowers,
        upper_file,
    })
}

pub(super) fn stable_path_identity(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let absolute = lexically_normalize_path(&absolute);
    canonicalize_nearest_existing_parent(&absolute)
}

/// True when `path` is `parent` or lives under it, compared on stable identities
/// (lexical normalization + nearest-existing-parent canonicalization) so it is
/// robust to `..`, symlinks, and not-yet-existing leaves.
pub(super) fn path_is_inside(path: &Path, parent: &Path) -> bool {
    stable_path_identity(path).starts_with(stable_path_identity(parent))
}

fn canonicalize_nearest_existing_parent(path: &Path) -> PathBuf {
    let mut suffix = Vec::<OsString>::new();
    let mut cursor = path;
    loop {
        if let Ok(mut base) = std::fs::canonicalize(cursor) {
            for component in suffix.iter().rev() {
                base.push(component);
            }
            return base;
        }
        if let Some(name) = cursor.file_name() {
            suffix.push(name.to_os_string());
        }
        let Some(parent) = cursor.parent().filter(|parent| *parent != cursor) else {
            return path.to_path_buf();
        };
        cursor = parent;
    }
}

fn hard_commit_object_put_op(
    digest: &HardCommitId,
    file: Option<PathBuf>,
    size: Option<u64>,
    p2p_keys: BTreeSet<P2pArtifactKey>,
) -> Result<LocalKvBatchOp> {
    put_json_op(
        hard_commit_object_key(digest),
        &HardCommitObjectRecord {
            digest: digest.clone(),
            file,
            size,
            p2p_keys,
        },
        "serialize hard commit object record",
    )
}

fn hold_record_put_op(owner: &ImageCacheHoldOwner) -> Result<LocalKvBatchOp> {
    put_json_op(
        hold_record_key(owner),
        &HoldRecord {
            namespace: owner.namespace().to_string(),
            key: owner.key().to_string(),
        },
        "serialize image cache hold record",
    )
}

fn hold_ref_replacement_ops(
    owner: &ImageCacheHoldOwner,
    existing: &BTreeSet<HardCommitId>,
    refs: &BTreeSet<HardCommitId>,
) -> Result<Vec<LocalKvBatchOp>> {
    let mut ops = Vec::new();
    for digest in existing.difference(refs) {
        push_mirrored_delete(
            &mut ops,
            hold_to_hard_key(owner, digest),
            hard_to_hold_key(digest, owner),
        );
    }
    for digest in refs.difference(existing) {
        push_mirrored_put_json(
            &mut ops,
            hold_to_hard_key(owner, digest),
            hard_to_hold_key(digest, owner),
            &hold_hard_reference_record(owner, digest),
            "serialize image cache hold reference",
        )?;
    }
    Ok(ops)
}

fn hold_ref_removal_ops(
    owner: &ImageCacheHoldOwner,
    existing: &BTreeSet<HardCommitId>,
) -> Vec<LocalKvBatchOp> {
    let mut ops = Vec::new();
    for digest in existing {
        push_mirrored_delete(
            &mut ops,
            hold_to_hard_key(owner, digest),
            hard_to_hold_key(digest, owner),
        );
    }
    ops.push(LocalKvBatchOp::delete(hold_record_key(owner)));
    ops
}

fn hold_hard_reference_record(
    owner: &ImageCacheHoldOwner,
    digest: &HardCommitId,
) -> HoldHardCommitReferenceRecord {
    HoldHardCommitReferenceRecord {
        namespace: owner.namespace().to_string(),
        key: owner.key().to_string(),
        digest: digest.clone(),
    }
}

async fn parse_configs_dir(
    configs_dir: &Path,
) -> Result<BTreeMap<ImageCacheConfigId, Vec<ParsedHardCommitRef>>> {
    let mut paths = Vec::new();
    let mut entries = match tokio::fs::read_dir(configs_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => {
            return Err(err).with_context(|| format!("read configs dir {}", configs_dir.display()))
        }
    };

    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("read configs dir {}", configs_dir.display()))?
    {
        let path = entry.path();
        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_regular_config_filename(filename) {
            continue;
        }
        let metadata = entry
            .metadata()
            .await
            .with_context(|| format!("stat image config {}", path.display()))?;
        if metadata.is_file() {
            paths.push(path);
        }
    }
    paths.sort();

    let mut parsed = BTreeMap::new();
    for path in paths {
        let config_id = ImageCacheConfigId::from_config_path(&path)?;
        let refs = load_cache_owned_hard_commit_refs(&path)?;
        parsed.insert(config_id, refs);
    }
    Ok(parsed)
}

fn load_cache_owned_hard_commit_refs(image_config_path: &Path) -> Result<Vec<ParsedHardCommitRef>> {
    let facts = load_cache_owned_image_config_facts(image_config_path)?;
    let mut refs = Vec::new();
    let mut seen = BTreeSet::new();

    // In cache-owned image configs, `file=` lowers are hard commits.
    // `dir=`+repoBlobUrl lowers are remote-recoverable and are never strongly
    // pinned, so they are not tracked as hard commits here.
    for lower in facts.local_file_lowers {
        let digest = HardCommitId::new(lower.digest)?;
        if seen.insert(digest.clone()) {
            refs.push(ParsedHardCommitRef {
                digest,
                file: lower.file,
                size: lower.size,
            });
        }
    }
    Ok(refs)
}

fn load_commit_store_owned_hard_commit_refs(
    image_config_path: &Path,
    commit_store: &Path,
) -> Result<Vec<ParsedHardCommitRef>> {
    let image_config = load_overlaybd_image_config(image_config_path)
        .with_context(|| format!("load image config {}", image_config_path.display()))?;
    let mut refs = Vec::new();
    let mut seen = BTreeSet::new();

    for (idx, lower) in image_config.lowers.into_iter().enumerate() {
        if lower.file.is_empty() {
            continue;
        }
        let file = PathBuf::from(lower.file);
        if !path_is_inside(&file, commit_store) {
            continue;
        }
        if lower.digest.is_empty() {
            bail!(
                "image config {} lower {idx} has image-cache commit-store file but no digest",
                image_config_path.display()
            );
        }
        if lower.size == 0 {
            bail!(
                "image config {} lower {idx} has image-cache commit-store file but no size",
                image_config_path.display()
            );
        }

        let digest = HardCommitId::new(lower.digest)?;
        if seen.insert(digest.clone()) {
            refs.push(ParsedHardCommitRef {
                digest,
                file,
                size: lower.size,
            });
        }
    }

    Ok(refs)
}

fn is_regular_config_filename(filename: &str) -> bool {
    filename.ends_with(IMAGE_CONFIG_SUFFIX)
}

fn config_last_used_key(config_id: &ImageCacheConfigId) -> Vec<u8> {
    key_with_components(CONFIG_LAST_USED_PREFIX, [config_id.as_str()])
}

fn hold_record_key(owner: &ImageCacheHoldOwner) -> Vec<u8> {
    key_with_components(HOLD_RECORD_PREFIX, [owner.namespace(), owner.key()])
}

fn hard_commit_object_key(digest: &HardCommitId) -> Vec<u8> {
    key_with_components(HARD_COMMIT_OBJECT_PREFIX, [digest.as_str()])
}

fn config_to_hard_key(config_id: &ImageCacheConfigId, digest: &HardCommitId) -> Vec<u8> {
    key_with_components(CONFIG_TO_HARD_PREFIX, [config_id.as_str(), digest.as_str()])
}

fn config_to_hard_prefix_for_config(config_id: &ImageCacheConfigId) -> Vec<u8> {
    let mut prefix = key_with_components(CONFIG_TO_HARD_PREFIX, [config_id.as_str()]);
    prefix.push(b'/');
    prefix
}

fn hold_to_hard_key(owner: &ImageCacheHoldOwner, digest: &HardCommitId) -> Vec<u8> {
    key_with_components(
        HOLD_TO_HARD_PREFIX,
        [
            owner.namespace().to_string(),
            owner.key().to_string(),
            digest.as_str().to_string(),
        ],
    )
}

fn hard_to_hold_key(digest: &HardCommitId, owner: &ImageCacheHoldOwner) -> Vec<u8> {
    key_with_components(
        HARD_TO_HOLD_PREFIX,
        [
            digest.as_str().to_string(),
            owner.namespace().to_string(),
            owner.key().to_string(),
        ],
    )
}

fn hold_to_hard_prefix_for_owner(owner: &ImageCacheHoldOwner) -> Vec<u8> {
    let mut prefix = key_with_components(HOLD_TO_HARD_PREFIX, [owner.namespace(), owner.key()]);
    prefix.push(b'/');
    prefix
}

fn hard_to_hold_prefix_for_digest(digest: &HardCommitId) -> Vec<u8> {
    let mut prefix = key_with_components(HARD_TO_HOLD_PREFIX, [digest.as_str()]);
    prefix.push(b'/');
    prefix
}

fn key_with_components<S>(prefix: &'static [u8], components: impl IntoIterator<Item = S>) -> Vec<u8>
where
    S: AsRef<str>,
{
    let mut key = prefix.to_vec();
    let mut first = true;
    for component in components {
        if !first {
            key.push(b'/');
        }
        first = false;
        key.extend_from_slice(hex_encode(component.as_ref().as_bytes()).as_bytes());
    }
    key
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_json<T>(key: &[u8], value: &[u8], record_type: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(value).with_context(|| {
        format!(
            "parse image cache metadata {record_type} at key {}",
            String::from_utf8_lossy(key)
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    async fn test_store(temp: &TempDir) -> ImageCacheMetadataStore {
        ImageCacheMetadataStore::open(
            temp.path().join("metadata.db"),
            LocalStoreDurability::Memory,
        )
        .await
        .expect("open metadata store")
    }

    fn config_id(name: &str) -> ImageCacheConfigId {
        ImageCacheConfigId::from_filename(name).expect("config id")
    }

    fn hard(digest: &str) -> HardCommitId {
        HardCommitId::new(digest).expect("hard commit id")
    }

    fn hold_owner(namespace: &str, key: &str) -> ImageCacheHoldOwner {
        ImageCacheHoldOwner::new(namespace, key).expect("hold owner")
    }

    fn refs_of(digests: impl IntoIterator<Item = HardCommitId>) -> BTreeSet<HardCommitId> {
        digests.into_iter().collect()
    }

    fn write_config(path: &Path, lowers: serde_json::Value, repo_blob_url: &str) {
        std::fs::create_dir_all(path.parent().expect("config parent")).expect("create configs");
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": repo_blob_url,
                "lowers": lowers,
                "upper": {},
                "resultFile": ""
            }))
            .expect("serialize config"),
        )
        .expect("write config");
    }

    #[tokio::test]
    async fn hard_commit_p2p_keys_are_persisted_and_preserved_by_config_reconcile() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store(&temp).await;
        let digest = hard("sha256:layer");
        let first_key = "overlaybd-layer/v1/sha256:layer".to_string();
        let second_key = "overlaybd-layer/v1/uuid/test".to_string();

        store
            .record_hard_commit_object(
                digest.clone(),
                Some(temp.path().join("commits/layer")),
                Some(10),
                BTreeSet::from([first_key.clone()]),
            )
            .await
            .expect("record hard commit object");
        assert_eq!(
            store
                .get_hard_commit_object(&digest)
                .await
                .expect("load hard commit object")
                .expect("hard commit object")
                .p2p_keys,
            BTreeSet::from([first_key.clone()])
        );

        store
            .add_hard_commit_p2p_key(&digest, &second_key)
            .await
            .expect("add p2p key");

        let config = temp.path().join("configs/image-image.json");
        record_config(
            &store,
            &config,
            json!([{
                "file": "../commits/layer",
                "digest": "sha256:layer",
                "size": 10
            }]),
        )
        .await;

        assert_eq!(
            store
                .get_hard_commit_object(&digest)
                .await
                .expect("load reconciled hard commit object")
                .expect("reconciled hard commit object")
                .p2p_keys,
            BTreeSet::from([first_key, second_key])
        );
    }

    #[tokio::test]
    async fn adding_hard_commit_p2p_key_rejects_missing_object() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store(&temp).await;
        let digest = hard("sha256:missing");
        let key = "oci-layer/v1/missing".to_string();

        let error = store
            .add_hard_commit_p2p_key(&digest, &key)
            .await
            .expect_err("missing hard commit object must fail");
        assert!(error.to_string().contains("does not exist"));
    }

    async fn record_config(
        store: &ImageCacheMetadataStore,
        path: &Path,
        lowers: serde_json::Value,
    ) {
        write_config(path, lowers, "");
        store
            .record_config_refs_from_config_path(path)
            .await
            .expect("record config refs");
    }

    #[tokio::test]
    async fn hold_ref_lifecycle_replaces_stale_refs_then_clears_on_release() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store(&temp).await;
        let owner = hold_owner("test", "owner-1");
        let digest1 = hard("sha256:one");
        let digest2 = hard("sha256:two");
        let digest3 = hard("sha256:three");

        // Replacing {digest1, digest2} with {digest2, digest3} drops digest1's
        // reverse ref and keeps digest2/digest3 pointing at the owner.
        store
            .create_or_replace_hold(&owner, &refs_of([digest1.clone(), digest2.clone()]))
            .await
            .expect("record initial hold");
        store
            .create_or_replace_hold(&owner, &refs_of([digest2.clone(), digest3.clone()]))
            .await
            .expect("replace hold");

        assert!(store
            .hard_commit_hold_referrers(&digest1)
            .await
            .expect("digest1 referrers")
            .is_empty());
        assert_eq!(
            store
                .hard_commit_hold_referrers(&digest2)
                .await
                .expect("digest2 referrers"),
            vec![owner.clone()]
        );
        assert_eq!(
            store
                .hard_commit_hold_referrers(&digest3)
                .await
                .expect("digest3 referrers"),
            vec![owner.clone()]
        );

        // Releasing clears the hold record and both forward/reverse ref indexes.
        store.release_hold(&owner).await.expect("release hold");

        assert!(store
            .list_hold_owners_in_namespaces(&["test"])
            .await
            .expect("hold owners")
            .is_empty());
        for digest in [&digest2, &digest3] {
            assert!(store
                .hard_commit_hold_referrers(digest)
                .await
                .expect("digest referrers")
                .is_empty());
        }
    }

    #[tokio::test]
    async fn rebuild_from_configs_records_only_hard_commit_refs() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store(&temp).await;
        let configs = temp.path().join("configs");
        let config = configs.join("mixed-image.json");

        write_config(
            &config,
            json!([
                {
                    "file": "../commits/sha256-hard/overlaybd.commit",
                    "digest": "sha256:hard",
                    "size": 11
                },
                {
                    "dir": "../commits/sha256-soft",
                    "digest": "sha256:soft",
                    "size": 22
                },
                {
                    "digest": "sha256:remote-block",
                    "size": 33
                },
                {
                    "dir": "../indexes/base",
                    "gzipIndex": "../indexes/base.index",
                    "digest": "sha256:index",
                    "size": 44
                }
            ]),
            "https://registry.example/v2/repo/blobs",
        );

        store
            .rebuild_from_configs(&configs)
            .await
            .expect("rebuild metadata");

        let config = config_id("mixed-image.json");
        assert_eq!(
            store
                .list_hard_commit_objects()
                .await
                .expect("hard objects")
                .into_iter()
                .map(|record| record.digest)
                .collect::<Vec<_>>(),
            vec![hard("sha256:hard")]
        );
        let mut referrers = store
            .hard_commit_config_referrer_map()
            .await
            .expect("hard config referrers");
        assert_eq!(
            referrers.remove(&hard("sha256:hard")).unwrap_or_default(),
            vec![config]
        );
    }

    #[tokio::test]
    async fn rebuild_from_configs_fails_closed_on_malformed_config() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store(&temp).await;
        let configs = temp.path().join("configs");
        let existing_config = config_id("existing-image.json");
        let existing_digest = hard("sha256:existing");

        record_config(
            &store,
            &temp.path().join("seed/existing-image.json"),
            json!([{
                "file": "../commits/sha256-existing/overlaybd.commit",
                "digest": "sha256:existing",
                "size": 10
            }]),
        )
        .await;
        write_config(
            &configs.join("bad-image.json"),
            json!([{
                "file": "../commits/missing-digest/overlaybd.commit",
                "size": 10
            }]),
            "",
        );

        let err = store
            .rebuild_from_configs(&configs)
            .await
            .expect_err("malformed config should fail rebuild");

        assert!(
            // The "no digest" cause is a source in the error chain; the outer
            // `to_string()` only carries the "rebuild ..." context, so match the
            // full alternate-formatted chain.
            format!("{err:#}").contains("local file but no digest"),
            "unexpected error: {err:#}"
        );
        let mut referrers = store
            .hard_commit_config_referrer_map()
            .await
            .expect("existing referrers");
        assert_eq!(
            referrers.remove(&existing_digest).unwrap_or_default(),
            vec![existing_config]
        );
    }

    #[tokio::test]
    async fn plan_capacity_eviction_frees_shared_commit_only_when_all_referrers_evicted() {
        let temp = TempDir::new().expect("tempdir");
        let store = test_store(&temp).await;
        let a = config_id("a-image.json");
        let b = config_id("b-image.json");
        // Both reference the shared base; a also has an exclusive layer.
        record_config(
            &store,
            &temp.path().join(a.as_str()),
            json!([
                {
                    "file": "../commits/sha256-shared/overlaybd.commit",
                    "digest": "sha256:shared",
                    "size": 500
                },
                {
                    "file": "../commits/sha256-a-only/overlaybd.commit",
                    "digest": "sha256:a-only",
                    "size": 50
                }
            ]),
        )
        .await;
        record_config(
            &store,
            &temp.path().join(b.as_str()),
            json!([{
                "file": "../commits/sha256-shared/overlaybd.commit",
                "digest": "sha256:shared",
                "size": 500
            }]),
        )
        .await;

        // High watermark 0 forces eviction; low watermark 0 cleans everything
        // eligible. Evicting `a` first frees only its exclusive 50 (shared still
        // held by b); evicting `b` then frees shared.
        let plan = store
            .plan_capacity_eviction(0, 0, u64::MAX)
            .await
            .expect("plan");
        assert_eq!(plan.total_bytes, 550);
        assert_eq!(
            plan.candidates
                .iter()
                .map(|candidate| candidate.config_id.clone())
                .collect::<Vec<_>>(),
            vec![a, b]
        );
    }
}
