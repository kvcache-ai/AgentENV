//! Offline migration of pre-SQL POSIX and OSS repository catalogs into SQL
//! metadata (`aenv-snapshot import`).

mod legacy;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use agentenv_domain::{
    is_sha256_digest, is_valid_volume_component, BuildErrorReason, BuildJobTransition, BuildStatus,
    BuildTarget, ImageName, ImageTag, ImageTagRecord, PublishedImage, VolumeMode, VolumeRecord,
};
use agentenv_metadata::{MetaClient, SnapshotEntry};
use anyhow::{bail, ensure, Context, Result};
use overlaybd::layer::LayerFile;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use self::legacy::{
    decode_legacy_snapshot_record, open_legacy_sources_from_config, LegacyBuildStatus,
    LegacyCatalog, LegacyDocumentCollection, LegacyDocumentKey, LegacyImageBuildRecord,
};
use crate::cfg::AppConfig;
use crate::digest::FileDigest;
use crate::repository::{
    BlobBody, BlobKey, BlobLocation, LayerRole, Repository, RepositoryError, SnapshotFile,
};
use crate::snapshot::{
    CommittedAttachedDrive, LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotId, SnapshotManifest,
    SnapshotPlacement, SnapshotRuntimeManifest,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ImportOptions {
    pub all: bool,
    pub dry_run: bool,
    pub skip_local: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct ResourceImportReport {
    pub imported: Vec<String>,
    pub reused: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_deleting: Vec<String>,
    pub failed: BTreeMap<String, String>,
}

impl ResourceImportReport {
    pub fn is_empty(&self) -> bool {
        self.imported.is_empty()
            && self.reused.is_empty()
            && self.skipped_deleting.is_empty()
            && self.failed.is_empty()
    }
}

/// Summary printed as JSON by `aenv-snapshot import`.
#[derive(Debug, Default, Serialize)]
pub struct RepositoryImportReport {
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
    pub imported: Vec<SnapshotId>,
    pub reused: Vec<SnapshotId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_local: Vec<SnapshotId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_uncommitted: Vec<SnapshotId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_deleting: Vec<SnapshotId>,
    pub failed: BTreeMap<SnapshotId, String>,
    #[serde(skip_serializing_if = "ResourceImportReport::is_empty")]
    pub volumes: ResourceImportReport,
    #[serde(skip_serializing_if = "ResourceImportReport::is_empty")]
    pub images: ResourceImportReport,
    #[serde(skip_serializing_if = "ResourceImportReport::is_empty")]
    pub image_tags: ResourceImportReport,
    #[serde(skip_serializing_if = "ResourceImportReport::is_empty")]
    pub image_builds: ResourceImportReport,
}

impl RepositoryImportReport {
    pub fn has_failures(&self) -> bool {
        !self.failed.is_empty()
            || !self.volumes.failed.is_empty()
            || !self.images.failed.is_empty()
            || !self.image_tags.failed.is_empty()
            || !self.image_builds.failed.is_empty()
    }
}

/// Parses deduplicated snapshot IDs (bare UUID or `aenv-snapshot-<uuid>`).
pub fn parse_snapshot_ids<'a>(
    tokens: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<SnapshotId>> {
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for raw in tokens {
        let trimmed = raw.trim();
        let id = SnapshotId::parse(trimmed.strip_prefix("aenv-snapshot-").unwrap_or(trimmed))?;
        if seen.insert(id.clone()) {
            ids.push(id);
        }
    }
    ensure!(!ids.is_empty(), "at least one snapshot ID is required");
    Ok(ids)
}

/// Opens the configured repository and its pre-SQL legacy catalog and imports resources.
pub async fn import_from_config(
    config: &AppConfig,
    meta: &MetaClient,
    ids: &[SnapshotId],
    options: ImportOptions,
) -> Result<RepositoryImportReport> {
    let sources = open_legacy_sources_from_config(config)?;
    import_repository(
        sources.repo.as_ref(),
        sources.catalog.as_ref(),
        meta,
        ids,
        options,
    )
    .await
}

async fn import_repository(
    repository: &dyn Repository,
    catalog: &dyn LegacyCatalog,
    meta: &MetaClient,
    ids: &[SnapshotId],
    options: ImportOptions,
) -> Result<RepositoryImportReport> {
    if options.all {
        ensure!(
            ids.is_empty(),
            "--all cannot be combined with explicit snapshot IDs"
        );
    } else {
        ensure!(!options.skip_local, "--skip-local requires --all");
    }

    let mut importer = Importer::new(repository, catalog, meta, options);
    let mut report = RepositoryImportReport {
        dry_run: options.dry_run,
        ..RepositoryImportReport::default()
    };

    let snapshot_ids = if options.all {
        discover_ids(catalog, LegacyDocumentCollection::SnapshotRecords)
            .await?
            .into_iter()
            .map(|raw| {
                SnapshotId::parse(&raw)
                    .with_context(|| format!("parse discovered legacy snapshot ID '{raw}'"))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        ids.to_vec()
    };
    for id in &snapshot_ids {
        if let Err(error) = importer.import_snapshot(id, &mut report).await {
            report.failed.insert(id.clone(), format!("{error:#}"));
        }
    }
    if !options.all {
        return Ok(report);
    }

    let cache = match catalog
        .get_document(LegacyDocumentKey::BuildCacheState)
        .await
        .context("read legacy build cache state")?
    {
        Some(bytes) => serde_json::from_slice::<CacheHead>(&bytes)
            .context("parse legacy template-build/cache-head.json")?,
        None => CacheHead::default(),
    };
    for id in &discover_ids(catalog, LegacyDocumentCollection::VolumeRecords).await? {
        if let Err(error) = importer
            .import_volume(id, &cache, &mut report.volumes)
            .await
        {
            report
                .volumes
                .failed
                .insert(id.clone(), format!("{error:#}"));
        }
    }
    for digest in &discover_ids(catalog, LegacyDocumentCollection::Images).await? {
        if let Err(error) = importer.import_image(digest, &mut report.images).await {
            report
                .images
                .failed
                .insert(digest.clone(), format!("{error:#}"));
        }
    }
    for tag in &discover_ids(catalog, LegacyDocumentCollection::ImageTags).await? {
        if let Err(error) = importer.import_image_tag(tag, &mut report.image_tags).await {
            report
                .image_tags
                .failed
                .insert(tag.clone(), format!("{error:#}"));
        }
    }
    for raw_id in &discover_ids(catalog, LegacyDocumentCollection::ImageBuilds).await? {
        if let Err(error) = importer
            .import_image_build(raw_id, &mut report.image_builds)
            .await
        {
            report
                .image_builds
                .failed
                .insert(raw_id.clone(), format!("{error:#}"));
        }
    }
    Ok(report)
}

async fn discover_ids(
    catalog: &dyn LegacyCatalog,
    collection: LegacyDocumentCollection,
) -> Result<Vec<String>> {
    catalog
        .list_documents(collection)
        .await
        .with_context(|| format!("list legacy documents under '{}'", collection.prefix()))
}

#[derive(Default, Deserialize)]
struct CacheHead {
    #[serde(default)]
    current: Option<String>,
    #[serde(default)]
    retired: HashSet<String>,
}

struct Importer<'a> {
    repo: &'a dyn Repository,
    catalog: &'a dyn LegacyCatalog,
    meta: &'a MetaClient,
    options: ImportOptions,
    audited_layers: HashMap<String, (u64, Uuid)>,
    layer_identities: HashMap<Uuid, (Option<Uuid>, String)>,
    claimed_snapshot_ids: HashSet<SnapshotId>,
    claimed_snapshot_aliases: HashMap<SnapshotAlias, SnapshotId>,
    claimed_volume_names: HashMap<String, String>,
    available_images: HashSet<String>,
}

impl<'a> Importer<'a> {
    fn new(
        repo: &'a dyn Repository,
        catalog: &'a dyn LegacyCatalog,
        meta: &'a MetaClient,
        options: ImportOptions,
    ) -> Self {
        Self {
            repo,
            catalog,
            meta,
            options,
            audited_layers: HashMap::new(),
            layer_identities: HashMap::new(),
            claimed_snapshot_ids: HashSet::new(),
            claimed_snapshot_aliases: HashMap::new(),
            claimed_volume_names: HashMap::new(),
            available_images: HashSet::new(),
        }
    }

    async fn audit_managed_layer(
        &mut self,
        layer: &ManagedLayerRef,
        role: LayerRole,
    ) -> Result<(u64, Uuid)> {
        ensure!(
            is_sha256_digest(&layer.digest),
            "invalid managed layer digest '{}'",
            layer.digest
        );
        let (size, uuid) = match self.audited_layers.get(&layer.digest).copied() {
            Some(cached) => cached,
            None => {
                let key = BlobKey::Layer {
                    digest: layer.digest.clone(),
                    role,
                };
                let stat_size = self
                    .repo
                    .stat_blob(&key)
                    .await?
                    .with_context(|| format!("missing managed layer '{}'", layer.digest))?;
                ensure!(stat_size > 0, "managed layer '{}' is empty", layer.digest);

                let (path, _staging) = match self.repo.locate_blob(&key)? {
                    BlobLocation::Local(path) => (path, None),
                    BlobLocation::Remote(_) => {
                        let file = self.repo.staging_file()?;
                        let downloaded = self.repo.fetch_blob(&key, file.path()).await?;
                        ensure!(
                            downloaded == stat_size,
                            "managed layer '{}' size changed during download",
                            layer.digest
                        );
                        (file.path().to_path_buf(), Some(file))
                    }
                };
                let actual = FileDigest::describe(&path)
                    .await
                    .with_context(|| format!("hash managed layer '{}'", layer.digest))?;
                ensure!(
                    actual.size == stat_size && actual.sha256 == layer.digest,
                    "managed layer '{}' digest mismatch: actual {} ({} bytes)",
                    layer.digest,
                    actual.sha256,
                    actual.size
                );
                let uuid = LayerFile::from_path(path, None)
                    .await
                    .with_context(|| format!("read UUID of managed layer '{}'", layer.digest))?
                    .uuid;
                self.audited_layers
                    .insert(layer.digest.clone(), (stat_size, uuid));
                (stat_size, uuid)
            }
        };
        ensure!(
            layer.size == 0 || layer.size == size,
            "layer '{}' size mismatch: declared {}, actual {size}",
            layer.digest,
            layer.size
        );
        if let Some(declared) = &layer.uuid {
            ensure!(
                Uuid::parse_str(declared).ok() == Some(uuid),
                "layer '{}' declares UUID {declared}, but stored layer has {uuid}",
                layer.digest
            );
        }
        Ok((size, uuid))
    }

    async fn audit_disk_chain(
        &mut self,
        managed_url: Option<&str>,
        layers: &mut [LayerRef],
        role: LayerRole,
        delta: &mut BTreeMap<Uuid, (Option<Uuid>, String)>,
    ) -> Result<()> {
        let mut managed = Vec::new();
        for layer in layers {
            if let LayerRef::External(ext) = layer {
                if managed_url == Some(ext.repo_blob_url.trim_end_matches('/')) {
                    *layer = LayerRef::Managed(ManagedLayerRef {
                        digest: std::mem::take(&mut ext.digest),
                        size: ext.size,
                        uuid: None,
                    });
                }
            }
            if let LayerRef::Managed(m) = layer {
                let (size, uuid) = self.audit_managed_layer(m, role).await?;
                m.size = size;
                m.uuid = (!uuid.is_nil()).then(|| uuid.to_string());
                managed.push(&*m);
            }
        }
        stage_chain_identities(managed, &self.layer_identities, delta)
    }

    async fn import_snapshot(
        &mut self,
        id: &SnapshotId,
        report: &mut RepositoryImportReport,
    ) -> Result<()> {
        ensure!(
            !self.claimed_snapshot_ids.contains(id),
            "snapshot '{id}' appears multiple times in this batch"
        );
        let bytes = self
            .catalog
            .get_document(LegacyDocumentKey::SnapshotRecord(id.clone()))
            .await?
            .with_context(|| format!("legacy snapshot '{id}' not found"))?;
        let raw: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse legacy snapshot '{id}'"))?;
        if raw.get("deleting").and_then(Value::as_bool) == Some(true) {
            ensure!(self.options.all, "legacy snapshot '{id}' is deleting");
            report.skipped_deleting.push(id.clone());
            return Ok(());
        }
        let mut record = decode_legacy_snapshot_record(raw)
            .with_context(|| format!("parse legacy snapshot '{id}'"))?;
        ensure!(&record.id == id, "legacy snapshot '{id}' has mismatched ID");

        if record.snapshot_type == SnapshotPlacement::Local {
            ensure!(
                self.options.all && self.options.skip_local,
                "snapshot '{id}' is Local; promote it on its owner before importing"
            );
            report.skipped_local.push(id.clone());
            return Ok(());
        }
        if record.manifest.is_none() {
            ensure!(
                self.options.all,
                "snapshot '{id}' is missing committed manifest"
            );
            report.skipped_uncommitted.push(id.clone());
            return Ok(());
        }
        record.validate().map_err(anyhow::Error::msg)?;

        let existing = match self
            .meta
            .get_snapshot(id.to_string())
            .await
            .context("query SQL snapshot directory")?
            .filter(|entry| &entry.id == id)
        {
            Some(entry) => {
                ensure!(entry.is_visible(), "snapshot '{id}' is retired");
                Some(entry)
            }
            None => None,
        };

        if let Some(alias) = record.alias.as_ref() {
            let bytes = self
                .catalog
                .get_document(LegacyDocumentKey::SnapshotAlias(alias.clone()))
                .await?
                .with_context(|| {
                    format!("legacy alias '{alias}' is missing for snapshot '{id}'")
                })?;
            let bound: SnapshotId = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse legacy alias '{alias}'"))?;
            ensure!(
                bound == *id,
                "legacy alias '{alias}' already points to '{bound}'"
            );
            if let Some(owner) = self
                .claimed_snapshot_aliases
                .get(alias)
                .filter(|owner| *owner != id)
            {
                bail!("legacy alias '{alias}' conflicts with snapshot '{owner}' in this batch");
            }
            if let Some(other) = self
                .meta
                .list_snapshots(agentenv_domain::SnapshotListFilter {
                    alias: Some(alias.to_string()),
                    limit: Some(1),
                    ..Default::default()
                })
                .await
                .context("check SQL snapshot alias")?
                .into_iter()
                .find(|entry| &entry.id != id)
            {
                bail!("SQL alias '{alias}' already points to '{}'", other.id);
            }
        }

        let vm_key = BlobKey::Snapshot(id.clone(), SnapshotFile::VmState);
        let vm_state_size = self
            .repo
            .stat_blob(&vm_key)
            .await?
            .with_context(|| format!("snapshot '{id}' is missing vm_state.bin"))?;
        ensure!(vm_state_size > 0, "snapshot '{id}' has empty vm_state.bin");
        let manifest_key = BlobKey::Snapshot(id.clone(), SnapshotFile::FirecrackerManifest);
        let backend_bytes =
            self.repo
                .read_blob(&manifest_key)
                .await
                .map_err(|error| match error {
                    RepositoryError::ArtifactNotFound { .. } => {
                        anyhow::anyhow!("snapshot '{id}' is missing firecracker-manifest.json")
                    }
                    other => other.into(),
                })?;
        validate_backend_manifest(
            &backend_bytes,
            record.manifest.as_ref().expect("validated manifest"),
        )?;

        let managed_url = self
            .repo
            .managed_layers_repo_blob_url()
            .map(|url| url.trim_end_matches('/').to_owned());
        let mut delta = BTreeMap::new();
        for volume in &mut record.config.volume_snapshots {
            self.audit_disk_chain(
                managed_url.as_deref(),
                &mut volume.layers,
                LayerRole::AttachedDrive,
                &mut delta,
            )
            .await?;
        }
        {
            let manifest = record.manifest.as_mut().expect("validated manifest");
            self.audit_disk_chain(
                managed_url.as_deref(),
                &mut manifest.rootfs_layers,
                LayerRole::Rootfs,
                &mut delta,
            )
            .await?;
            for CommittedAttachedDrive::Overlaybd { layers, .. } in &mut manifest.attached_drives {
                self.audit_disk_chain(
                    managed_url.as_deref(),
                    layers,
                    LayerRole::AttachedDrive,
                    &mut delta,
                )
                .await?;
            }
            for layer in &mut manifest.memory_layers {
                let (size, uuid) = self.audit_managed_layer(layer, LayerRole::Memory).await?;
                layer.size = size;
                layer.uuid = (!uuid.is_nil()).then(|| uuid.to_string());
            }
            stage_chain_identities(&manifest.memory_layers, &self.layer_identities, &mut delta)?;
        }

        let needs_promote = existing.as_ref().is_some_and(|entry| {
            matches!(entry.snapshot_placement(), Ok(SnapshotPlacement::Local))
        });
        if let Some(entry) = existing.as_ref() {
            let expected = SnapshotEntry::from_record(&record);
            if needs_promote {
                ensure!(
                    entry.source_kind == expected.source_kind
                        && entry.source_sandbox_id == expected.source_sandbox_id
                        && entry.created_at_unix_ms == expected.created_at_unix_ms,
                    "snapshot '{id}' already exists in SQL as Local with different metadata"
                );
            } else {
                ensure!(
                    entry == &expected,
                    "snapshot '{id}' already exists in SQL with different directory metadata"
                );
            }
        }

        let expected_bytes = crate::snapshot::content::encode_snapshot_content(&record)?;
        let expected: Value = serde_json::from_slice(&expected_bytes)?;
        let content_key = BlobKey::Snapshot(id.clone(), SnapshotFile::Record);
        match self.repo.read_blob(&content_key).await {
            Ok(bytes) => {
                let raw_actual: Value = serde_json::from_slice(&bytes)
                    .with_context(|| format!("decode existing snapshot.json for '{id}'"))?;
                let mut actual = raw_actual.clone();
                normalize_zero_layer_sizes(&mut actual, &self.audited_layers);
                ensure!(
                    actual == expected,
                    "existing snapshot.json for '{id}' has different content"
                );
                if raw_actual != expected && !self.options.dry_run {
                    self.repo
                        .put_blob(&content_key, BlobBody::Bytes(expected_bytes))
                        .await?;
                }
            }
            Err(RepositoryError::ArtifactNotFound { .. }) => {
                ensure!(
                    existing.is_none() || needs_promote,
                    "registered snapshot '{id}' is missing snapshot.json"
                );
                if !self.options.dry_run {
                    self.repo
                        .put_blob(&content_key, BlobBody::Bytes(expected_bytes))
                        .await?;
                }
            }
            Err(error) => return Err(error.into()),
        }

        if existing.is_some() && !needs_promote {
            report.reused.push(id.clone());
        } else {
            if !self.options.dry_run {
                if needs_promote {
                    self.meta
                        .promote_snapshot(record.clone(), Vec::new())
                        .await
                        .with_context(|| format!("promote snapshot '{id}'"))?;
                } else {
                    self.meta
                        .register_snapshot(record.clone(), Vec::new())
                        .await
                        .with_context(|| format!("register snapshot '{id}'"))?;
                }
                let still_live = self
                    .catalog
                    .get_document(LegacyDocumentKey::SnapshotRecord(id.clone()))
                    .await
                    .with_context(|| format!("recheck legacy snapshot '{id}' after register"))?
                    .is_some_and(|bytes| {
                        serde_json::from_slice::<Value>(&bytes)
                            .ok()
                            .and_then(|v| v.get("deleting").and_then(Value::as_bool))
                            != Some(true)
                    });
                if !still_live {
                    self.meta
                        .retire_snapshot(id.clone())
                        .await
                        .with_context(|| format!("retire concurrently deleted snapshot '{id}'"))?;
                    bail!("legacy snapshot '{id}' was deleted during import");
                }
            }
            report.imported.push(id.clone());
        }

        self.layer_identities.extend(delta);
        self.claimed_snapshot_ids.insert(id.clone());
        if let Some(alias) = record.alias {
            self.claimed_snapshot_aliases.insert(alias, id.clone());
        }
        Ok(())
    }

    async fn import_volume(
        &mut self,
        volume_id: &str,
        cache: &CacheHead,
        report: &mut ResourceImportReport,
    ) -> Result<()> {
        ensure!(
            is_valid_volume_component(volume_id),
            "invalid legacy volume id '{volume_id}'"
        );
        let bytes = self
            .catalog
            .get_document(LegacyDocumentKey::VolumeRecord(volume_id.to_owned()))
            .await?
            .with_context(|| format!("legacy volume '{volume_id}' not found"))?;
        let raw: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse legacy volume '{volume_id}'"))?;
        if raw.get("deleting").and_then(Value::as_bool) == Some(true) {
            report.skipped_deleting.push(volume_id.to_owned());
            return Ok(());
        }
        let mut record: VolumeRecord = serde_json::from_value(raw)
            .with_context(|| format!("decode legacy volume '{volume_id}'"))?;
        ensure!(
            record.id == volume_id
                && is_valid_volume_component(&record.name)
                && record.size_mb > 0
                && !(record.mode == VolumeMode::ReadOnly
                    && record.reserved_by_sandbox_id.is_some()),
            "invalid legacy volume metadata for '{volume_id}'"
        );
        // Ephemeral BuildKit cache volumes are regenerated on demand; skip them so
        // legacy seeds/work disks do not appear as user volumes after upgrade.
        if cache.current.as_deref() == Some(record.id.as_str())
            || cache.retired.contains(&record.id)
            || record.name.starts_with("aenv-buildkit-work-")
            || record.name.starts_with("aenv-buildkit-seed-")
        {
            return Ok(());
        }
        // Sandboxes are drained before offline upgrade; never carry stale pre-upgrade
        // sandbox leases into SQL metadata where they would permanently wedge the volume.
        record.reserved_by_sandbox_id = None;
        record.read_only_mounts.clear();

        let alias_bytes = self
            .catalog
            .get_document(LegacyDocumentKey::VolumeAlias(record.name.clone()))
            .await?
            .with_context(|| format!("legacy volume alias '{}' is missing", record.name))?;
        let bound: String = serde_json::from_slice(&alias_bytes)
            .with_context(|| format!("parse legacy volume alias '{}'", record.name))?;
        ensure!(
            bound == record.id,
            "legacy volume alias '{}' already points to '{bound}'",
            record.name
        );
        ensure!(
            self.claimed_volume_names
                .get(&record.name)
                .is_none_or(|owner| owner == &record.id),
            "legacy volume name '{}' conflicts within batch",
            record.name
        );

        let managed_url = self
            .repo
            .managed_layers_repo_blob_url()
            .map(|url| url.trim_end_matches('/').to_owned());
        let mut delta = BTreeMap::new();
        self.audit_disk_chain(
            managed_url.as_deref(),
            &mut record.backing_layers,
            LayerRole::AttachedDrive,
            &mut delta,
        )
        .await?;

        let existing = match self
            .meta
            .get_volume(record.id.clone())
            .await
            .context("query SQL volume catalog")?
        {
            Some(existing) if existing.id == record.id => {
                ensure!(
                    existing == record,
                    "volume '{}' already exists in SQL with different metadata",
                    record.id
                );
                true
            }
            Some(other) => bail!(
                "volume ID '{}' collides with volume '{}'",
                record.id,
                other.id
            ),
            None => {
                if let Some(other) = self
                    .meta
                    .get_volume(record.name.clone())
                    .await
                    .context("check SQL volume name")?
                {
                    bail!(
                        "volume name '{}' already belongs to '{}'",
                        record.name,
                        other.id
                    );
                }
                false
            }
        };

        if existing {
            report.reused.push(record.id.clone());
        } else {
            if !self.options.dry_run {
                self.meta
                    .create_volume(record.clone(), None, Vec::new())
                    .await
                    .with_context(|| format!("create volume '{}'", record.id))?;
            }
            report.imported.push(record.id.clone());
        }
        self.layer_identities.extend(delta);
        self.claimed_volume_names
            .insert(record.name, record.id.clone());
        Ok(())
    }

    async fn import_image(
        &mut self,
        digest: &str,
        report: &mut ResourceImportReport,
    ) -> Result<()> {
        let bytes = self
            .catalog
            .get_document(LegacyDocumentKey::image(digest)?)
            .await?
            .with_context(|| format!("legacy published image '{digest}' not found"))?;
        let image = PublishedImage::decode(digest, &bytes)
            .with_context(|| format!("verify legacy published image '{digest}'"))?;

        let mut managed = Vec::with_capacity(image.layers.len());
        for layer in &image.layers {
            let LayerRef::Managed(m) = layer else {
                bail!("published image '{digest}' must only reference managed layers");
            };
            self.audit_managed_layer(m, LayerRole::Rootfs).await?;
            managed.push(m);
        }
        let mut delta = BTreeMap::new();
        stage_chain_identities(managed, &self.layer_identities, &mut delta)?;

        if let Some(existing) = self
            .meta
            .get_image(digest.to_owned())
            .await
            .context("query SQL published image")?
        {
            ensure!(
                existing == image,
                "published image '{digest}' already exists in SQL with different payload"
            );
            report.reused.push(digest.to_owned());
        } else {
            if !self.options.dry_run {
                let stored = self
                    .meta
                    .put_image(image, Vec::new())
                    .await
                    .with_context(|| format!("put published image '{digest}'"))?;
                ensure!(
                    stored == digest,
                    "stored published image digest '{stored}' does not match '{digest}'"
                );
            }
            report.imported.push(digest.to_owned());
        }
        self.layer_identities.extend(delta);
        self.available_images.insert(digest.to_owned());
        Ok(())
    }

    async fn import_image_tag(
        &self,
        raw_tag: &str,
        report: &mut ResourceImportReport,
    ) -> Result<()> {
        let tag =
            ImageTag::parse(raw_tag).with_context(|| format!("parse image tag '{raw_tag}'"))?;
        let bytes = self
            .catalog
            .get_document(LegacyDocumentKey::ImageTag(tag.clone()))
            .await?
            .with_context(|| format!("legacy image tag '{tag}' not found"))?;
        let record: ImageTagRecord = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse legacy image tag '{tag}'"))?;
        ensure!(
            is_sha256_digest(&record.image_digest)
                && record.source_manifest_digest.as_deref() != Some(""),
            "legacy image tag '{tag}' has invalid digest fields"
        );
        let target_exists = self.available_images.contains(&record.image_digest)
            || self
                .meta
                .get_image(record.image_digest.clone())
                .await
                .context("verify image tag target in SQL")?
                .is_some();
        ensure!(
            target_exists,
            "legacy image tag '{tag}' references missing published image '{}'",
            record.image_digest
        );

        if let Some((existing, _)) = self
            .meta
            .get_image_tag(tag.clone())
            .await
            .context("query SQL image tag")?
        {
            ensure!(
                existing.image_digest == record.image_digest
                    && existing.source_manifest_digest == record.source_manifest_digest,
                "image tag '{tag}' already exists in SQL pointing to '{}'",
                existing.image_digest
            );
            report.reused.push(raw_tag.to_owned());
        } else {
            if !self.options.dry_run {
                self.meta
                    .put_image_tag(tag, record.image_digest, record.source_manifest_digest)
                    .await
                    .with_context(|| format!("put image tag '{raw_tag}'"))?;
            }
            report.imported.push(raw_tag.to_owned());
        }
        Ok(())
    }

    async fn import_image_build(
        &self,
        raw_id: &str,
        report: &mut ResourceImportReport,
    ) -> Result<()> {
        let id = SnapshotId::parse(raw_id)
            .with_context(|| format!("parse legacy image build ID '{raw_id}'"))?;
        let bytes = self
            .catalog
            .get_document(LegacyDocumentKey::ImageBuild(id.clone()))
            .await?
            .with_context(|| format!("legacy image build '{id}' not found"))?;
        let raw: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse legacy image build '{id}'"))?;
        if raw.get("deleting").and_then(Value::as_bool) == Some(true) {
            report.skipped_deleting.push(raw_id.to_owned());
            return Ok(());
        }
        let legacy: LegacyImageBuildRecord = serde_json::from_value(raw)
            .with_context(|| format!("decode legacy image build '{id}'"))?;
        let parsed_name = ImageName::parse(&legacy.name)
            .with_context(|| format!("invalid image name in legacy build '{id}'"))?;
        ensure!(
            !legacy.tags.is_empty(),
            "legacy image build '{id}' requires at least one tag"
        );
        for tag in &legacy.tags {
            ImageTag::new(&parsed_name, tag)
                .with_context(|| format!("invalid image tag '{tag}' in legacy build '{id}'"))?;
        }
        let target = BuildTarget::Image {
            name: parsed_name.to_string(),
            tags: legacy.tags,
        };

        let (desired_status, desired_digest, desired_error) = match legacy.build.status {
            LegacyBuildStatus::Waiting => (BuildStatus::Waiting, None, None),
            LegacyBuildStatus::Building => (
                BuildStatus::Error,
                None,
                Some(BuildErrorReason::new(
                    "build interrupted during offline migration",
                )),
            ),
            LegacyBuildStatus::Ready => {
                let digest = legacy.image_digest.with_context(|| {
                    format!("ready legacy image build '{id}' is missing imageDigest")
                })?;
                ensure!(
                    is_sha256_digest(&digest),
                    "ready legacy image build '{id}' has invalid imageDigest '{digest}'"
                );
                (BuildStatus::Ready, Some(digest), None)
            }
            LegacyBuildStatus::Error => (
                BuildStatus::Error,
                None,
                Some(
                    legacy
                        .build
                        .error_reason
                        .map(Into::into)
                        .unwrap_or_else(|| BuildErrorReason::new("legacy build failed")),
                ),
            ),
        };

        let needs_start = legacy.build.started_at_unix_ms.is_some()
            || legacy.build.status == LegacyBuildStatus::Building;
        let existing_status = if let Some(existing) = self
            .meta
            .get_build_job(id.clone())
            .await
            .context("query SQL build job")?
        {
            if existing.target == target
                && existing.status == desired_status
                && existing.result_image_digest == desired_digest
                && existing.error_reason == desired_error
            {
                report.reused.push(raw_id.to_owned());
                return Ok(());
            }
            let resumable_partial = existing.target == target
                && existing.result_image_digest.is_none()
                && existing.error_reason.is_none()
                && (existing.status == BuildStatus::Waiting
                    || (existing.status == BuildStatus::Building
                        && matches!(desired_status, BuildStatus::Ready | BuildStatus::Error)));
            ensure!(
                resumable_partial,
                "build job '{id}' already exists in SQL with different metadata"
            );
            Some(existing.status)
        } else {
            None
        };

        if !self.options.dry_run {
            if existing_status.is_none() {
                self.meta
                    .create_build_job(id.clone(), target)
                    .await
                    .with_context(|| format!("create SQL build job '{id}'"))?;
            }
            if existing_status.unwrap_or(BuildStatus::Waiting) == BuildStatus::Waiting
                && (needs_start
                    || matches!(desired_status, BuildStatus::Ready | BuildStatus::Error))
            {
                self.meta
                    .update_build_job(id.clone(), BuildJobTransition::Start)
                    .await
                    .with_context(|| format!("mark imported build job '{id}' started"))?;
            }
            match desired_status {
                BuildStatus::Waiting | BuildStatus::Building => {}
                BuildStatus::Ready => {
                    let digest = desired_digest.expect("validated ready digest");
                    self.meta
                        .update_build_job(
                            id.clone(),
                            BuildJobTransition::Finish {
                                image_digest: Some(digest),
                            },
                        )
                        .await
                        .with_context(|| format!("mark imported build job '{id}' ready"))?;
                }
                BuildStatus::Error => {
                    let reason = desired_error.expect("validated error reason");
                    self.meta
                        .update_build_job(id.clone(), BuildJobTransition::Fail { reason })
                        .await
                        .with_context(|| format!("mark imported build job '{id}' failed"))?;
                }
            }
        }
        report.imported.push(raw_id.to_owned());
        Ok(())
    }
}

fn validate_backend_manifest(bytes: &[u8], committed: &SnapshotManifest) -> Result<()> {
    let raw: Value = serde_json::from_slice(bytes).context("decode firecracker-manifest.json")?;
    ensure!(
        raw.get("guest")
            .is_none_or(|g| g.get("os").and_then(Value::as_str) == Some("linux")),
        "only Linux Firecracker snapshots can be imported"
    );
    let manifest: SnapshotRuntimeManifest =
        serde_json::from_value(raw).context("decode firecracker-manifest.json")?;
    ensure!(
        manifest.version == 1,
        "unsupported backend manifest version"
    );
    ensure!(
        manifest.backend == "firecracker",
        "unsupported snapshot backend"
    );
    ensure!(
        manifest.memory.virtual_size > 0
            && !committed.memory_layers.is_empty()
            && manifest.rootfs.virtual_size > 0
            && !committed.rootfs_layers.is_empty(),
        "missing memory or rootfs image"
    );
    manifest.validate_attached_drives()?;
    ensure!(
        manifest.attached_drives.len() == committed.attached_drives.len(),
        "attached drive count differs from the legacy record"
    );
    let mut seen = BTreeSet::new();
    for (artifact, drive) in manifest
        .attached_drives
        .iter()
        .zip(&committed.attached_drives)
    {
        let CommittedAttachedDrive::Overlaybd {
            drive_id,
            layers,
            read_only,
            virtual_size,
            ..
        } = drive;
        ensure!(
            seen.insert(drive_id)
                && artifact.drive_id == *drive_id
                && artifact.read_only == *read_only
                && artifact.virtual_size == *virtual_size
                && *virtual_size > 0
                && !layers.is_empty(),
            "invalid attached drive '{drive_id}'"
        );
    }
    Ok(())
}

fn stage_chain_identities<'a>(
    chain: impl IntoIterator<Item = &'a ManagedLayerRef>,
    committed: &HashMap<Uuid, (Option<Uuid>, String)>,
    delta: &mut BTreeMap<Uuid, (Option<Uuid>, String)>,
) -> Result<()> {
    let mut parent: Option<Uuid> = None;
    for layer in chain {
        let uuid = layer.directory_uuid().map_err(anyhow::Error::msg)?;
        let legacy = layer
            .uuid
            .as_deref()
            .is_none_or(|value| Uuid::parse_str(value).is_ok_and(|parsed| parsed.is_nil()));
        let predecessor = if legacy { None } else { parent };
        if let Some((existing_parent, existing_digest)) =
            delta.get(&uuid).or_else(|| committed.get(&uuid))
        {
            ensure!(
                *existing_parent == predecessor && existing_digest == &layer.digest,
                "managed layer '{uuid}' has conflicting lineage or digest across imported resources"
            );
        } else {
            delta.insert(uuid, (predecessor, layer.digest.clone()));
        }
        if !legacy {
            parent = Some(uuid);
        }
    }
    Ok(())
}

fn normalize_zero_layer_sizes(content: &mut Value, audited: &HashMap<String, (u64, Uuid)>) {
    let fill = |layers: Option<&mut Value>, wrapped: bool| {
        let Some(items) = layers.and_then(Value::as_array_mut) else {
            return;
        };
        for item in items {
            let target = if wrapped {
                item.get_mut("Managed")
            } else {
                Some(item)
            };
            let Some(obj) = target.and_then(Value::as_object_mut) else {
                continue;
            };
            if obj.get("size").and_then(Value::as_u64) == Some(0) {
                if let Some(&(actual, _)) = obj
                    .get("digest")
                    .and_then(Value::as_str)
                    .and_then(|digest| audited.get(digest))
                {
                    obj.insert("size".into(), json!(actual));
                }
            }
        }
    };
    if let Some(volumes) = content
        .pointer_mut("/config/volume_snapshots")
        .and_then(Value::as_array_mut)
    {
        for volume in volumes {
            fill(volume.get_mut("layers"), true);
        }
    }
    fill(content.pointer_mut("/manifest/rootfs_layers"), true);
    fill(content.pointer_mut("/manifest/memory_layers"), false);
    if let Some(drives) = content
        .pointer_mut("/manifest/attached_drives")
        .and_then(Value::as_array_mut)
    {
        for drive in drives {
            fill(drive.pointer_mut("/Overlaybd/layers"), true);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use agentenv_domain::{SnapshotVolume, VolumeStatus};
    use agentenv_metadata::{transport, MetaError};
    use overlaybd::backend::local::LocalFile;
    use overlaybd::index_file::{create_file_rw, LayerInfo};
    use overlaybd::virtual_file::VirtualFile;

    use super::legacy::PosixLegacyCatalog;
    use super::*;
    use crate::repository::backends::PosixFsRepository;
    use crate::snapshot::{SnapshotRecord, SnapshotSource, SnapshotSourceKind};

    fn write_legacy_doc(root: &Path, key: LegacyDocumentKey, bytes: &[u8]) -> Result<()> {
        let path = root.join(key.posix_path());
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, bytes)?;
        Ok(())
    }

    async fn store_layer(
        repo: &PosixFsRepository,
        dir: &Path,
        name: &str,
        role: LayerRole,
        uuid_override: Option<Uuid>,
    ) -> Result<ManagedLayerRef> {
        let path = dir.join(name);
        let mut info = LayerInfo::new(
            Arc::new(LocalFile::new(&path)?),
            Some(Arc::new(LocalFile::new(dir.join(format!("{name}.index")))?)),
            4 * 1024 * 1024,
        );
        if let Some(uuid) = uuid_override {
            info.uuid = uuid;
        }
        let uuid = info.uuid;
        let layer = create_file_rw(info).await?;
        let mut block = [0xA5u8; 4096];
        block[..name.len()].copy_from_slice(name.as_bytes());
        layer.write_at(4096, &block).await?;
        layer.close_seal().await?;
        let bytes = std::fs::read(&path)?;
        let digest = crate::digest::sha256_digest(&bytes);
        repo.put_blob(
            &BlobKey::Layer {
                digest: digest.clone(),
                role,
            },
            BlobBody::File(&path),
        )
        .await?;
        Ok(ManagedLayerRef {
            digest,
            size: bytes.len() as u64,
            uuid: (!uuid.is_nil()).then(|| uuid.to_string()),
        })
    }

    async fn seed_legacy(
        repo: &PosixFsRepository,
        root: &Path,
        id: &SnapshotId,
        alias: Option<&str>,
        source: Value,
        rootfs: &ManagedLayerRef,
        memory: &ManagedLayerRef,
    ) -> Result<()> {
        let backend = json!({
            "version": 1, "backend": "firecracker", "vmState": {}, "vsock": true,
            "memory": {"virtualSize": 4194304}, "rootfs": {"virtualSize": 4194304}, "attachedDrives": [],
        });
        let legacy = json!({
            "id": id, "alias": alias, "source": source,
            "created_at_unix_ms": 1, "updated_at_unix_ms": 2,
            "resources": {"cpu_count": 1, "memory_mib": 128, "disk_size_mib": 1024},
            "committed": {
                "context": {"workdir": "/", "env_vars": {}},
                "runtime_versions": {"kernel_version": "k", "firecracker_version": "v", "envd_version": "e"},
                "attached_drives": [],
                "memory_layers": [{"digest": memory.digest, "size": memory.size}],
                "rootfs_layers": [{"Managed": {"digest": rootfs.digest, "size": 0}}],
            },
        });
        repo.put_blob(
            &BlobKey::Snapshot(id.clone(), SnapshotFile::VmState),
            BlobBody::Bytes(b"state".to_vec()),
        )
        .await?;
        repo.put_blob(
            &BlobKey::Snapshot(id.clone(), SnapshotFile::FirecrackerManifest),
            BlobBody::Bytes(serde_json::to_vec(&backend)?),
        )
        .await?;
        write_legacy_doc(
            root,
            LegacyDocumentKey::SnapshotRecord(id.clone()),
            &serde_json::to_vec(&legacy)?,
        )?;
        if let Some(alias) = alias {
            write_legacy_doc(
                root,
                LegacyDocumentKey::SnapshotAlias(SnapshotAlias::parse(alias)?),
                &serde_json::to_vec(id)?,
            )?;
        }
        Ok(())
    }

    fn sandbox_record() -> SnapshotRecord {
        let mut record = SnapshotRecord::mock_ready(SnapshotManifest::default());
        record.config.source = SnapshotSource::Sandbox {
            source_sandbox_id: "original".into(),
            source_sandbox_metadata: None,
        };
        record
    }

    struct DeleteOnRecheck<'a>(&'a PosixLegacyCatalog, SnapshotId, AtomicUsize);

    #[async_trait::async_trait]
    impl LegacyCatalog for DeleteOnRecheck<'_> {
        async fn get_document(
            &self,
            key: LegacyDocumentKey,
        ) -> crate::repository::RepositoryResult<Option<Vec<u8>>> {
            if matches!(&key, LegacyDocumentKey::SnapshotRecord(id) if id == &self.1)
                && self.2.fetch_add(1, Ordering::SeqCst) >= 1
            {
                return Ok(None);
            }
            self.0.get_document(key).await
        }

        async fn list_documents(
            &self,
            collection: LegacyDocumentCollection,
        ) -> crate::repository::RepositoryResult<Vec<String>> {
            self.0.list_documents(collection).await
        }
    }

    #[tokio::test]
    async fn imports_legacy_repository_idempotently_and_enforces_guardrails() -> Result<()> {
        let (a, b) = (SnapshotId::generate(), SnapshotId::generate());
        assert_eq!(
            parse_snapshot_ids([
                format!("aenv-snapshot-{a}").as_str(),
                b.to_string().as_str(),
                a.to_string().as_str(),
            ])?,
            vec![a, b]
        );
        assert!(parse_snapshot_ids(std::iter::empty()).is_err());

        let tmp = tempfile::tempdir()?;
        let repo_root = tmp.path().join("repository");
        let repo = PosixFsRepository::new(repo_root.clone());
        let catalog = PosixLegacyCatalog::new(repo_root.clone());
        let mut import_config = AppConfig::default();
        import_config.backend.posix_fs = Some(crate::cfg::PosixFsBackendConfig {
            snapshot_store: tmp.path().to_path_buf(),
        });

        let rootfs = store_layer(&repo, tmp.path(), "rootfs", LayerRole::Rootfs, None).await?;
        let zero_uuid = store_layer(
            &repo,
            tmp.path(),
            "zero-uuid",
            LayerRole::Rootfs,
            Some(Uuid::nil()),
        )
        .await?;
        assert_eq!(zero_uuid.uuid, None);
        let memory = store_layer(&repo, tmp.path(), "memory", LayerRole::Memory, None).await?;
        let vol_layer = store_layer(
            &repo,
            tmp.path(),
            "vol-layer",
            LayerRole::AttachedDrive,
            None,
        )
        .await?;
        let img_layer =
            store_layer(&repo, tmp.path(), "img-layer", LayerRole::Rootfs, None).await?;

        let (server, _embedded) = crate::metadata::test_meta(tmp.path()).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let rpc = tokio::spawn(transport::serve_tcp(listener, server.clone(), async move {
            let _ = stop_rx.await;
            Ok(())
        }));
        let meta = transport::connect(&addr.to_string()).await?;
        let sbx_src = || json!({"Sandbox": {"source_sandbox_id": "original"}});
        let import_one = |id: &SnapshotId| {
            let id = id.clone();
            let (repo, catalog, meta) = (&repo, &catalog, &meta);
            async move { import_repository(repo, catalog, meta, &[id], ImportOptions::default()).await }
        };
        let import_all = |dry_run: bool| {
            let opts = ImportOptions {
                all: true,
                dry_run,
                skip_local: true,
            };
            import_from_config(&import_config, &meta, &[], opts)
        };
        let remove_legacy = |id: SnapshotId| -> Result<()> {
            let rel = LegacyDocumentKey::SnapshotRecord(id).posix_path();
            Ok(std::fs::remove_file(repo_root.join(rel))?)
        };

        // Existing unrelated pinned SQL entry must stay untouched.
        let unrelated = sandbox_record();
        server.register_snapshot(unrelated.clone(), &[]).await?;
        let pin_id = Uuid::now_v7();
        assert!(
            server
                .pin_snapshot(&unrelated.id, pin_id, Some("node-a".into()))
                .await?
        );

        // 1. Targeted import (zero-UUID rootfs + volume_snapshots size=0) + idempotent rerun.
        let (selected, unselected) = (SnapshotId::generate(), SnapshotId::generate());
        seed_legacy(
            &repo,
            &repo_root,
            &selected,
            Some("kept"),
            sbx_src(),
            &zero_uuid,
            &memory,
        )
        .await?;
        let mut sel_raw: Value = serde_json::from_slice(
            &catalog
                .get_document(LegacyDocumentKey::SnapshotRecord(selected.clone()))
                .await?
                .unwrap(),
        )?;
        sel_raw["committed"]["volume_snapshots"] = serde_json::to_value(vec![SnapshotVolume {
            mount_path: "/data".into(),
            mode: VolumeMode::ReadOnly,
            size_mb: 64,
            layers: vec![LayerRef::Managed(ManagedLayerRef {
                digest: vol_layer.digest.clone(),
                size: 0,
                uuid: None,
            })],
        }])?;
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::SnapshotRecord(selected.clone()),
            &serde_json::to_vec(&sel_raw)?,
        )?;
        seed_legacy(
            &repo,
            &repo_root,
            &unselected,
            None,
            sbx_src(),
            &rootfs,
            &memory,
        )
        .await?;

        let report = import_one(&selected).await?;
        assert!(!report.has_failures() && report.imported == vec![selected.clone()]);
        assert_eq!(server.get_snapshot("kept").await?.unwrap().id, selected);
        assert!(server
            .get_snapshot(&unselected.to_string())
            .await?
            .is_none());

        let content: Value = serde_json::from_slice(
            &repo
                .read_blob(&BlobKey::Snapshot(selected.clone(), SnapshotFile::Record))
                .await?,
        )?;
        assert!(content.get("id").is_none());
        assert_eq!(
            content["manifest"]["rootfs_layers"],
            json!([{"Managed": zero_uuid}])
        );
        assert_eq!(content["manifest"]["memory_layers"], json!([memory]));
        assert_eq!(import_one(&selected).await?.reused, vec![selected.clone()]);

        // 2. Half-promoted Local SQL row + zero-size snapshot.json normalization vs non-zero drift.
        let repaired_id = SnapshotId::generate();
        let mut local_sql = sandbox_record();
        local_sql.id = repaired_id.clone();
        local_sql.snapshot_type = SnapshotPlacement::Local;
        local_sql.owner_node_id = Some("node-1".into());
        local_sql.manifest = None;
        local_sql.created_at_unix_ms = 1;
        local_sql.updated_at_unix_ms = 1;
        server.register_snapshot(local_sql, &[]).await?;
        seed_legacy(
            &repo,
            &repo_root,
            &repaired_id,
            Some("repaired"),
            sbx_src(),
            &rootfs,
            &memory,
        )
        .await?;
        let mut zero_size_doc = content.clone();
        zero_size_doc["config"]["volume_snapshots"] = json!([]);
        zero_size_doc["manifest"]["rootfs_layers"] = json!([{"Managed": {
            "digest": rootfs.digest, "size": 0, "uuid": rootfs.uuid
        }}]);
        let repaired_blob = BlobKey::Snapshot(repaired_id.clone(), SnapshotFile::Record);
        repo.put_blob(
            &repaired_blob,
            BlobBody::Bytes(serde_json::to_vec(&zero_size_doc)?),
        )
        .await?;
        let BlobLocation::Local(repaired_path) = repo.locate_blob(&repaired_blob)? else {
            panic!("expected local snapshot.json path");
        };
        std::fs::set_permissions(
            &repaired_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o644),
        )?;
        assert_eq!(
            import_one(&repaired_id).await?.imported,
            vec![repaired_id.clone()]
        );
        assert_eq!(
            std::os::unix::fs::PermissionsExt::mode(
                &std::fs::metadata(&repaired_path)?.permissions()
            ) & 0o777,
            0o644
        );
        let rewritten_doc: Value = serde_json::from_slice(&repo.read_blob(&repaired_blob).await?)?;
        assert_eq!(
            rewritten_doc["manifest"]["rootfs_layers"][0]["Managed"]["size"],
            json!(rootfs.size)
        );
        assert_eq!(
            server
                .get_snapshot("repaired")
                .await?
                .unwrap()
                .snapshot_placement()?,
            SnapshotPlacement::Distributed
        );
        assert_eq!(
            import_one(&repaired_id).await?.reused,
            vec![repaired_id.clone()]
        );

        zero_size_doc["manifest"]["rootfs_layers"][0]["Managed"]["size"] = json!(1);
        repo.put_blob(
            &repaired_blob,
            BlobBody::Bytes(serde_json::to_vec(&zero_size_doc)?),
        )
        .await?;
        assert!(import_one(&repaired_id).await?.failed[&repaired_id].contains("different content"));
        repo.delete_blob(&repaired_blob).await?;
        assert!(
            import_one(&repaired_id).await?.failed[&repaired_id].contains("missing snapshot.json")
        );
        zero_size_doc["manifest"]["rootfs_layers"][0]["Managed"]["size"] = json!(rootfs.size);
        repo.put_blob(
            &repaired_blob,
            BlobBody::Bytes(serde_json::to_vec(&zero_size_doc)?),
        )
        .await?;

        // 3. Guardrails: retired SQL ID, UUID-shaped alias only, empty attached drive, missing attachedDrives, concurrent delete.
        let retired = sandbox_record();
        let retired_id = retired.id.clone();
        server.register_snapshot(retired, &[]).await?;
        server.retire_snapshot(retired_id.clone()).await?;
        seed_legacy(
            &repo,
            &repo_root,
            &retired_id,
            None,
            sbx_src(),
            &rootfs,
            &memory,
        )
        .await?;
        assert!(import_one(&retired_id).await?.failed[&retired_id].contains("is retired"));
        remove_legacy(retired_id)?;

        let alias_only = SnapshotId::generate();
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::SnapshotAlias(SnapshotAlias::parse(&alias_only.to_string())?),
            &serde_json::to_vec(&selected)?,
        )?;
        assert!(import_one(&alias_only).await?.failed[&alias_only].contains("not found"));

        let bad_drive = SnapshotId::generate();
        seed_legacy(
            &repo,
            &repo_root,
            &bad_drive,
            None,
            sbx_src(),
            &rootfs,
            &memory,
        )
        .await?;
        let mut bad_rec: Value = serde_json::from_slice(
            &catalog
                .get_document(LegacyDocumentKey::SnapshotRecord(bad_drive.clone()))
                .await?
                .unwrap(),
        )?;
        bad_rec["committed"]["attached_drives"] = json!([{"Overlaybd": {
            "drive_id": "data", "layers": [], "read_only": true, "virtual_size": 4096, "mount_path": "/data"
        }}]);
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::SnapshotRecord(bad_drive.clone()),
            &serde_json::to_vec(&bad_rec)?,
        )?;
        repo.put_blob(
            &BlobKey::Snapshot(bad_drive.clone(), SnapshotFile::FirecrackerManifest),
            BlobBody::Bytes(serde_json::to_vec(&json!({
                "version": 1, "backend": "firecracker", "vmState": {},
                "memory": {"virtualSize": 4194304}, "rootfs": {"virtualSize": 4194304},
                "attachedDrives": [{"driveId": "data", "readOnly": true, "virtualSize": 4096}],
            }))?),
        )
        .await?;
        assert!(import_one(&bad_drive).await?.failed[&bad_drive].contains("invalid attached drive"));
        repo.put_blob(
            &BlobKey::Snapshot(bad_drive.clone(), SnapshotFile::FirecrackerManifest),
            BlobBody::Bytes(serde_json::to_vec(&json!({
                "version": 1, "backend": "firecracker", "vmState": {},
                "memory": {"virtualSize": 4194304}, "rootfs": {"virtualSize": 4194304},
            }))?),
        )
        .await?;
        assert!(import_one(&bad_drive).await?.failed[&bad_drive]
            .contains("decode firecracker-manifest.json"));
        remove_legacy(bad_drive)?;

        let racing_id = SnapshotId::generate();
        seed_legacy(
            &repo,
            &repo_root,
            &racing_id,
            Some("racing"),
            sbx_src(),
            &rootfs,
            &memory,
        )
        .await?;
        let racing = DeleteOnRecheck(&catalog, racing_id.clone(), AtomicUsize::new(0));
        let race_report = import_repository(
            &repo,
            &racing,
            &meta,
            std::slice::from_ref(&racing_id),
            ImportOptions::default(),
        )
        .await?;
        assert!(race_report.failed[&racing_id].contains("deleted during import"));
        assert!(!server
            .get_snapshot(&racing_id.to_string())
            .await?
            .unwrap()
            .is_visible());
        assert!(server.get_snapshot("racing").await?.is_none());
        remove_legacy(racing_id)?;

        // 4. Full-repository --all --dry-run -> real --all --skip-local -> idempotent rerun
        //    covering snapshots, templates, volumes, cache head, images, tags, and images/builds/.
        let template_id = SnapshotId::generate();
        seed_legacy(
            &repo,
            &repo_root,
            &template_id,
            Some("python-tpl"),
            json!({"Template": {"build": {"status": "ready"}}}),
            &rootfs,
            &memory,
        )
        .await?;

        let uncommitted_id = SnapshotId::generate();
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::SnapshotRecord(uncommitted_id.clone()),
            &serde_json::to_vec(&json!({
                "id": uncommitted_id, "alias": null,
                "source": {"Template": {"build": {"status": "building"}}},
                "created_at_unix_ms": 1, "updated_at_unix_ms": 2,
                "resources": {"cpu_count": 1, "memory_mib": 128, "disk_size_mib": 1024},
                "committed": null,
            }))?,
        )?;
        let mut local = sandbox_record();
        local.snapshot_type = SnapshotPlacement::Local;
        local.owner_node_id = Some("node-1".into());
        local.manifest = None;
        let local_id = local.id.clone();
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::SnapshotRecord(local_id.clone()),
            &serde_json::to_vec(&local)?,
        )?;
        assert!(import_one(&local_id).await?.failed[&local_id].contains("Local"));
        let deleting_id = SnapshotId::generate();
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::SnapshotRecord(deleting_id.clone()),
            &serde_json::to_vec(&json!({"id": deleting_id, "deleting": true}))?,
        )?;

        let work_vol_name = format!("aenv-buildkit-work-{}", SnapshotId::generate());
        for (id, name, mode, deleting) in [
            ("vol_user", "app-data", VolumeMode::Exclusive, false),
            ("vol_seed", "cache-seed", VolumeMode::ReadOnly, false),
            ("vol_retired", "cache-old", VolumeMode::ReadOnly, false),
            (
                "vol_work",
                work_vol_name.as_str(),
                VolumeMode::Exclusive,
                false,
            ),
            ("vol_gone", "gone-data", VolumeMode::Exclusive, true),
        ] {
            let mut val = serde_json::to_value(VolumeRecord {
                id: id.into(),
                name: name.into(),
                mode,
                size_mb: 128,
                status: VolumeStatus::Ready,
                reserved_by_sandbox_id: None,
                backing_layers: vec![LayerRef::Managed(ManagedLayerRef {
                    digest: vol_layer.digest.clone(),
                    size: 0,
                    uuid: None,
                })],
                read_only_mounts: Vec::new(),
            })?;
            if deleting {
                val["deleting"] = json!(true);
            }
            write_legacy_doc(
                &repo_root,
                LegacyDocumentKey::VolumeRecord(id.into()),
                &serde_json::to_vec(&val)?,
            )?;
            if !deleting {
                write_legacy_doc(
                    &repo_root,
                    LegacyDocumentKey::VolumeAlias(name.into()),
                    &serde_json::to_vec(id)?,
                )?;
            }
        }
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::BuildCacheState,
            &serde_json::to_vec(&json!({"current": "vol_seed", "retired": ["vol_retired"]}))?,
        )?;

        let image = PublishedImage::new(
            "amd64".into(),
            vec![LayerRef::Managed(ManagedLayerRef {
                digest: img_layer.digest.clone(),
                size: img_layer.size,
                uuid: None,
            })],
            json!({"Cmd": ["/bin/sh"]}),
        );
        let (image_digest, image_bytes) = image.encode()?;
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::image(&image_digest)?,
            &image_bytes,
        )?;
        let pulled_tag = ImageTag::parse("docker.io/library/python:3.12")?;
        for (tag, src_manifest) in [
            (ImageTag::parse("web:v1")?, None),
            (pulled_tag.clone(), Some("sha256:manifest123".to_owned())),
        ] {
            write_legacy_doc(
                &repo_root,
                LegacyDocumentKey::ImageTag(tag),
                &serde_json::to_vec(&ImageTagRecord {
                    image_digest: image_digest.clone(),
                    created_at_unix_ms: 10,
                    source_manifest_digest: src_manifest,
                })?,
            )?;
        }
        std::fs::write(repo_root.join("images/tags/web/v1.lock"), b"")?;

        // Legacy v0.2.5 imageOnly build records under images/builds/.
        let ready_build_id = SnapshotId::generate();
        let failed_build_id = SnapshotId::generate();
        let interrupted_build_id = SnapshotId::generate();
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::ImageBuild(ready_build_id.clone()),
            &serde_json::to_vec(&json!({
                "name": "web",
                "tags": ["v1"],
                "build": {
                    "status": "Ready",
                    "started_at_unix_ms": 10,
                    "finished_at_unix_ms": 20,
                    "error_reason": null
                },
                "imageDigest": image_digest,
                "createdAtUnixMs": 5
            }))?,
        )?;
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::ImageBuild(failed_build_id.clone()),
            &serde_json::to_vec(&json!({
                "name": "web",
                "tags": ["broken"],
                "build": {
                    "status": "Error",
                    "started_at_unix_ms": 11,
                    "finished_at_unix_ms": 12,
                    "error_reason": "legacy string error"
                },
                "createdAtUnixMs": 6
            }))?,
        )?;
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::ImageBuild(interrupted_build_id.clone()),
            &serde_json::to_vec(&json!({
                "name": "web",
                "tags": ["wip"],
                "build": {
                    "status": "Building",
                    "started_at_unix_ms": 15,
                    "finished_at_unix_ms": null,
                    "error_reason": null
                },
                "createdAtUnixMs": 7
            }))?,
        )?;
        std::fs::write(
            repo_root.join(format!("images/builds/{ready_build_id}.lock")),
            b"",
        )?;

        let dry = import_all(true).await?;
        assert!(dry.dry_run && !dry.has_failures(), "{dry:?}");
        assert_eq!(dry.imported.len(), 2);
        assert_eq!(dry.reused.len(), 2);
        assert_eq!(dry.skipped_local, vec![local_id]);
        assert_eq!(dry.skipped_uncommitted, vec![uncommitted_id]);
        assert_eq!(dry.skipped_deleting, vec![deleting_id]);
        assert_eq!(dry.volumes.imported, vec!["vol_user"]);
        assert_eq!(dry.volumes.skipped_deleting, vec!["vol_gone"]);
        assert_eq!(dry.images.imported, vec![image_digest.clone()]);
        assert_eq!(dry.image_tags.imported.len(), 2);
        assert_eq!(dry.image_builds.imported.len(), 3);
        assert!(server.get_snapshot("python-tpl").await?.is_none());
        assert!(server.get_volume("vol_user").await?.is_none());
        assert!(server.get_image(&image_digest).await?.is_none());
        assert!(server.get_build_job(&ready_build_id).await?.is_none());

        assert!(!import_all(false).await?.has_failures());
        assert_eq!(
            server
                .get_snapshot("python-tpl")
                .await?
                .unwrap()
                .source_kind,
            SnapshotSourceKind::Template
        );
        assert!(server.get_volume("vol_user").await?.is_some());
        assert!(server.get_volume("vol_seed").await?.is_none());
        assert!(server.get_volume("vol_retired").await?.is_none());
        assert!(server.get_volume("vol_work").await?.is_none());
        assert_eq!(server.get_image(&image_digest).await?.unwrap(), image);
        assert_eq!(
            server
                .get_image_tag(&pulled_tag)
                .await?
                .unwrap()
                .0
                .source_manifest_digest
                .as_deref(),
            Some("sha256:manifest123")
        );
        let ready_job = server.get_build_job(&ready_build_id).await?.unwrap();
        assert_eq!(ready_job.status, BuildStatus::Ready);
        assert_eq!(
            ready_job.result_image_digest.as_deref(),
            Some(image_digest.as_str())
        );
        let failed_job = server.get_build_job(&failed_build_id).await?.unwrap();
        assert_eq!(failed_job.status, BuildStatus::Error);
        assert_eq!(
            failed_job.error_reason,
            Some(BuildErrorReason::new("legacy string error"))
        );
        let interrupted_job = server.get_build_job(&interrupted_build_id).await?.unwrap();
        assert_eq!(interrupted_job.status, BuildStatus::Error);
        assert!(server.delete_build_job(&interrupted_build_id).await?);
        // Re-import interrupted build to verify idempotent rerun below.
        let rerun_after_delete = import_all(false).await?;
        assert!(
            !rerun_after_delete.has_failures()
                && rerun_after_delete.image_builds.imported
                    == vec![interrupted_build_id.to_string()]
                && rerun_after_delete.image_builds.reused.len() == 2
        );

        let rerun = import_all(false).await?;
        assert!(
            !rerun.has_failures()
                && rerun.imported.is_empty()
                && rerun.reused.len() == 4
                && rerun.volumes.reused == vec!["vol_user"]
                && rerun.images.reused == vec![image_digest]
                && rerun.image_tags.reused.len() == 2
                && rerun.image_builds.reused.len() == 3
        );

        // 5. Dangling image tag & corrupted layer bytes fail digest verification; unrelated pin intact.
        write_legacy_doc(
            &repo_root,
            LegacyDocumentKey::ImageTag(ImageTag::parse("orphan:v1")?),
            &serde_json::to_vec(&ImageTagRecord {
                image_digest: crate::digest::sha256_digest(b"missing"),
                created_at_unix_ms: 12,
                source_manifest_digest: None,
            })?,
        )?;
        let BlobLocation::Local(layer_path) = repo.locate_blob(&BlobKey::Layer {
            digest: rootfs.digest.clone(),
            role: LayerRole::Rootfs,
        })?
        else {
            panic!("expected local path");
        };
        let mut raw_layer = std::fs::read(&layer_path)?;
        raw_layer[0] ^= 0xFF;
        std::fs::remove_file(&layer_path)?;
        std::fs::write(&layer_path, raw_layer)?;
        let corrupted = import_all(true).await?;
        assert!(corrupted.image_tags.failed["orphan:v1"].contains("missing published image"));
        assert!(corrupted.failed[&template_id].contains("digest mismatch"));

        server.retire_snapshot(unrelated.id.clone()).await?;
        assert!(
            server
                .pin_snapshot(&unrelated.id, pin_id, Some("node-a".into()))
                .await?
        );
        assert!(matches!(
            server
                .pin_snapshot(&unrelated.id, Uuid::now_v7(), Some("node-a".into()))
                .await,
            Err(MetaError::Unavailable { .. })
        ));

        drop(meta);
        let _ = stop_tx.send(());
        rpc.await??;
        server.close().await;
        Ok(())
    }
}
