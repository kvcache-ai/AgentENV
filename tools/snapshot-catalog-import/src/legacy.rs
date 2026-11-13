use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::ErrorKind;
use std::path::Path;

use agentenv_domain::{
    CommittedAttachedDrive, ExternalLayer, LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotId,
    SnapshotManifest, SnapshotPlacement, SnapshotRecord, SnapshotSource,
};
use anyhow::{ensure, Context, Result};
use object_store_operator::{OpenDalError, OpenDalErrorKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::store::{Source, Stamp, Store};

const RECORDS: &str = "catalog/records/";
const ALIASES: &str = "catalog/aliases/";
const PLAN_VERSION: u32 = 2;

#[derive(Default)]
pub struct Relations {
    pub layers: BTreeMap<Uuid, (Option<Uuid>, String)>,
    pub heads: BTreeSet<(SnapshotId, Uuid)>,
}

#[derive(Default)]
pub struct Catalog {
    pub records: BTreeMap<SnapshotId, SnapshotRecord>,
    pub local_ids: Vec<SnapshotId>,
    pub deferred: usize,
    pub relations: Relations,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Checked<T> {
    pub stamp: Stamp,
    pub value: T,
}

#[derive(Deserialize, Serialize)]
pub struct Plan {
    version: u32,
    source: Source,
    documents: BTreeMap<String, Checked<Value>>,
    runtime: BTreeMap<String, Stamp>,
    layers: BTreeMap<String, Checked<Uuid>>,
    pub manifests: BTreeMap<SnapshotId, Checked<String>>,
}

impl Plan {
    pub fn read(path: &Path, source: &Source) -> Result<Option<Self>> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).with_context(|| format!("open {}", path.display())),
        };
        let plan: Self =
            serde_json::from_reader(file).with_context(|| format!("parse {}", path.display()))?;
        ensure!(
            plan.version == PLAN_VERSION && plan.source == *source,
            "{} belongs to a different source or plan version",
            path.display()
        );
        Ok(Some(plan))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let parent = path.parent().filter(|path| !path.as_os_str().is_empty());
        let mut file = tempfile::NamedTempFile::new_in(parent.unwrap_or_else(|| Path::new(".")))?;
        serde_json::to_writer(file.as_file_mut(), self)?;
        file.persist(path)
            .with_context(|| format!("save {}", path.display()))?;
        Ok(())
    }

    pub async fn refresh(
        store: &mut Store,
        previous: Option<&Self>,
        source_frozen: bool,
    ) -> Result<(Self, Catalog, BTreeMap<String, Stamp>)> {
        let artifact_inventory = store.inventory("artifacts/").await?;
        let (mut catalog, documents) =
            read_catalog(store, previous, source_frozen, &artifact_inventory).await?;
        let layer_inventory = if catalog.records.is_empty() {
            BTreeMap::new()
        } else {
            store.inventory("managed-layers/").await?
        };
        let mut runtime = BTreeMap::new();
        let mut layers = BTreeMap::new();
        for (id, mut record) in std::mem::take(&mut catalog.records) {
            let [backend, state] = runtime_keys(&id);
            if !source_frozen && !artifact_inventory.contains_key(&state) {
                catalog.deferred += 1;
                continue;
            }
            let backend_stamp = required_stamp(&artifact_inventory, &backend)?;
            let state_stamp = required_stamp(&artifact_inventory, &state)?;
            if let Some(old_stamp) = previous.and_then(|plan| plan.runtime.get(&state)) {
                ensure!(
                    old_stamp == state_stamp,
                    "{state} changed since the previous audit"
                );
            }
            let manifest = record
                .manifest
                .as_mut()
                .context("missing committed snapshot")?;

            let volume_layers = record
                .config
                .volume_snapshots
                .iter()
                .flat_map(|volume| volume.layers.iter())
                .filter_map(|layer| match layer {
                    LayerRef::Managed(layer) => Some(layer),
                    LayerRef::External(_) => None,
                });
            for layer in managed_chains(manifest)
                .flatten()
                .map(|layer| &*layer)
                .chain(volume_layers)
            {
                if layers.contains_key(&layer.digest) {
                    continue;
                }
                let key = managed_key(&layer.digest)?;
                let stamp = required_stamp(&layer_inventory, &key)?;
                let cached = previous
                    .and_then(|plan| plan.layers.get(&layer.digest))
                    .filter(|checked| checked.stamp == *stamp);
                let uuid = match cached {
                    Some(checked) => checked.value,
                    None => store
                        .layer_uuid(&key, &layer.digest, stamp)
                        .await
                        .with_context(|| format!("inspect managed layer {key}"))?,
                };
                layers.insert(
                    layer.digest.clone(),
                    Checked {
                        stamp: stamp.clone(),
                        value: uuid,
                    },
                );
            }
            runtime.insert(backend, backend_stamp.clone());
            runtime.insert(state, state_stamp.clone());
            catalog.records.insert(id, record);
        }
        catalog.relations = normalize_layers(&mut catalog.records, &layers)?;
        let manifests = previous
            .into_iter()
            .flat_map(|plan| &plan.manifests)
            .filter(|(id, _)| catalog.records.contains_key(*id))
            .map(|(id, manifest)| (id.clone(), manifest.clone()))
            .collect();
        let plan = Self {
            version: PLAN_VERSION,
            source: store.source.clone(),
            documents,
            runtime,
            layers,
            manifests,
        };
        Ok((plan, catalog, artifact_inventory))
    }

    pub fn require_same_catalog(&self, current: &Self) -> Result<()> {
        ensure!(
            self.documents.len() == current.documents.len()
                && current.documents.iter().all(|(key, document)| {
                    self.documents
                        .get(key)
                        .is_some_and(|checked| checked.stamp == document.stamp)
                })
                && self.runtime == current.runtime
                && self.layers.len() == current.layers.len()
                && current.layers.iter().all(|(digest, layer)| {
                    self.layers
                        .get(digest)
                        .is_some_and(|checked| checked.stamp == layer.stamp)
                }),
            "legacy inventory changed after apply. Reconcile again before switching traffic"
        );
        Ok(())
    }
}

async fn read_catalog(
    store: &mut Store,
    previous: Option<&Plan>,
    source_frozen: bool,
    artifacts: &BTreeMap<String, Stamp>,
) -> Result<(Catalog, BTreeMap<String, Checked<Value>>)> {
    let mut documents = BTreeMap::new();
    let mut catalog = Catalog::default();
    let mut pending = Vec::new();
    let mut aliases: BTreeMap<SnapshotAlias, SnapshotId> = BTreeMap::new();
    let mut excluded_ids = BTreeSet::new();
    let managed_url = store.managed_layer_url();

    for prefix in [RECORDS, ALIASES] {
        for (key, stamp) in store.inventory(prefix).await? {
            let cached = previous.and_then(|plan| plan.documents.get(&key));
            let value = match cached.filter(|cached| cached.stamp == stamp) {
                Some(cached) => cached.value.clone(),
                None => {
                    let Some(bytes) = read_during_audit(store, &key, &stamp, source_frozen).await?
                    else {
                        catalog.deferred += 1;
                        continue;
                    };
                    serde_json::from_slice(&bytes).with_context(|| format!("decode {key}"))?
                }
            };
            let name = document_name(&key, prefix)?;
            if prefix == RECORDS {
                let id = SnapshotId::parse(name)?;
                ensure!(name == id.to_string(), "noncanonical record key {key}");
                let recorded_id: SnapshotId = serde_json::from_value(value["id"].clone())?;
                ensure!(recorded_id == id, "{key} contains a different snapshot ID");
                if value.pointer("/config/source/Template").is_some()
                    || value.pointer("/source/Template").is_some()
                    || value.get("deleting").and_then(Value::as_bool) == Some(true)
                {
                    excluded_ids.insert(id);
                    documents.insert(key, Checked { stamp, value });
                    continue;
                }
                let placement: SnapshotPlacement = serde_json::from_value(
                    value
                        .get("snapshot_type")
                        .cloned()
                        .unwrap_or_else(|| Value::String("distributed".into())),
                )?;
                if placement == SnapshotPlacement::Local {
                    catalog.local_ids.push(id);
                } else {
                    pending.push((id, key.clone()));
                }
            } else {
                let alias = SnapshotAlias::parse(name)?;
                aliases.insert(
                    alias,
                    serde_json::from_value(value.clone())
                        .with_context(|| format!("decode legacy alias {key}"))?,
                );
            }
            documents.insert(key, Checked { stamp, value });
        }
    }
    ensure!(
        !source_frozen || catalog.local_ids.is_empty(),
        "{} Local snapshots remain, including {}. Promote them on their owners before handoff",
        catalog.local_ids.len(),
        catalog.local_ids[0]
    );
    aliases.retain(|_, id| !excluded_ids.contains(id));
    for (id, key) in pending {
        let alias: Option<SnapshotAlias> = serde_json::from_value(
            documents[&key]
                .value
                .get("alias")
                .cloned()
                .unwrap_or(Value::Null),
        )?;
        if let Some(alias) = alias {
            if aliases.get(&alias) != Some(&id) {
                ensure!(
                    !source_frozen,
                    "legacy alias {alias} does not point to {id}"
                );
                catalog.deferred += 1;
                continue;
            }
        }
        let backend_key = runtime_keys(&id)[0].clone();
        let Some(stamp) = artifacts.get(&backend_key) else {
            ensure!(!source_frozen, "{backend_key} is missing");
            catalog.deferred += 1;
            continue;
        };
        if let Some(old_stamp) = previous.and_then(|plan| plan.runtime.get(&backend_key)) {
            ensure!(
                old_stamp == stamp,
                "{backend_key} changed since the previous audit"
            );
        }
        let cached = previous
            .and_then(|plan| plan.documents.get(&backend_key))
            .filter(|checked| checked.stamp == *stamp);
        let backend = match cached {
            Some(checked) => checked.value.clone(),
            None => {
                let Some(bytes) =
                    read_during_audit(store, &backend_key, stamp, source_frozen).await?
                else {
                    catalog.deferred += 1;
                    continue;
                };
                serde_json::from_slice(&bytes).with_context(|| format!("decode {backend_key}"))?
            }
        };
        let mut record = decode_legacy_record(documents[&key].value.clone(), backend.clone())
            .with_context(|| format!("decode legacy snapshot {key}"))?;
        record
            .validate()
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("validate {key}"))?;
        classify_owned_layers(
            record
                .manifest
                .as_mut()
                .context("missing committed snapshot")?,
            &managed_url,
        );
        catalog.records.insert(id, record);
        documents.insert(
            backend_key,
            Checked {
                stamp: stamp.clone(),
                value: backend,
            },
        );
    }
    for record in catalog.records.values() {
        if let Some(alias) = &record.alias {
            aliases.remove(alias);
            if let Ok(id) = SnapshotId::parse(alias.as_ref()) {
                ensure!(
                    id == record.id || !catalog.records.contains_key(&id),
                    "alias {alias} is shadowed by another snapshot ID"
                );
            }
        }
    }
    if source_frozen {
        if let Some((alias, id)) = aliases.first_key_value() {
            anyhow::bail!("legacy alias {alias} points to {id} without a matching record alias");
        }
    }
    catalog.deferred += aliases.len();
    Ok((catalog, documents))
}

async fn read_during_audit(
    store: &mut Store,
    key: &str,
    stamp: &Stamp,
    source_frozen: bool,
) -> Result<Option<Vec<u8>>> {
    match store.read_checked(key, stamp).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error)
            if !source_frozen
                && error.downcast_ref::<OpenDalError>().is_some_and(|error| {
                    matches!(
                        error.kind(),
                        OpenDalErrorKind::NotFound | OpenDalErrorKind::ConditionNotMatch
                    )
                }) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn document_name<'a>(key: &'a str, prefix: &str) -> Result<&'a str> {
    key.strip_prefix(prefix)
        .and_then(|name| name.strip_suffix(".json"))
        .filter(|name| !name.contains('/'))
        .with_context(|| format!("unexpected legacy catalog key {key}"))
}

fn required_stamp<'a>(inventory: &'a BTreeMap<String, Stamp>, key: &str) -> Result<&'a Stamp> {
    let stamp = inventory
        .get(key)
        .with_context(|| format!("{key} is missing"))?;
    ensure!(stamp.size > 0, "{key} is empty");
    Ok(stamp)
}

fn runtime_keys(id: &SnapshotId) -> [String; 2] {
    [
        format!("artifacts/{id}/firecracker-manifest.json"),
        format!("artifacts/{id}/vm_state.bin"),
    ]
}

fn managed_key(digest: &str) -> Result<String> {
    let hash = digest.strip_prefix("sha256:").unwrap_or_default();
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid managed layer digest {digest}"
    );
    Ok(format!("managed-layers/{digest}"))
}

pub fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn disk_chains<'a>(
    rootfs: &'a mut Vec<LayerRef>,
    drives: &'a mut [CommittedAttachedDrive],
) -> impl Iterator<Item = &'a mut Vec<LayerRef>> {
    std::iter::once(rootfs).chain(drives.iter_mut().map(|drive| {
        let CommittedAttachedDrive::Overlaybd { layers, .. } = drive;
        layers
    }))
}

fn decode_legacy_record(value: Value, backend_value: Value) -> Result<SnapshotRecord> {
    let backend: BackendManifest = serde_json::from_value(backend_value)?;
    let guest_os = value.pointer("/committed/guest_os").and_then(Value::as_str);
    ensure!(
        guest_os.is_none_or(|os| os == "linux"),
        "only Linux Firecracker snapshots can be imported"
    );
    let record: SnapshotRecord = serde_json::from_value(value)?;
    ensure!(
        matches!(record.config.source, SnapshotSource::Sandbox { .. }),
        "expected a reusable sandbox snapshot"
    );
    validate_backend(
        &backend,
        record
            .manifest
            .as_ref()
            .context("missing committed snapshot")?,
    )?;
    Ok(record)
}

fn classify_owned_layers(manifest: &mut SnapshotManifest, managed_url: &str) {
    for layer in disk_chains(&mut manifest.rootfs_layers, &mut manifest.attached_drives).flatten() {
        if let LayerRef::External(ExternalLayer {
            digest,
            size,
            repo_blob_url,
        }) = layer
        {
            // Older OSS writers could label a remote lower as External even when
            // it pointed at this repository's managed objects.
            if repo_blob_url.trim_end_matches('/') == managed_url {
                *layer = LayerRef::Managed(ManagedLayerRef {
                    digest: std::mem::take(digest),
                    size: *size,
                    uuid: None,
                });
            }
        }
    }
}

fn managed_chains(
    manifest: &mut SnapshotManifest,
) -> impl Iterator<Item = Vec<&mut ManagedLayerRef>> {
    let memory = manifest.memory_layers.iter_mut().collect();
    let disks = disk_chains(&mut manifest.rootfs_layers, &mut manifest.attached_drives);
    std::iter::once(memory).chain(disks.map(|layers| {
        layers
            .iter_mut()
            .filter_map(|layer| match layer {
                LayerRef::Managed(layer) => Some(layer),
                LayerRef::External(_) => None,
            })
            .collect()
    }))
}

fn normalize_layers(
    records: &mut BTreeMap<SnapshotId, SnapshotRecord>,
    layers: &BTreeMap<String, Checked<Uuid>>,
) -> Result<Relations> {
    // The directory retains only the managed projection of each bottom-up chain.
    let mut relations = Relations::default();
    for (id, record) in records {
        let manifest = record
            .manifest
            .as_mut()
            .context("missing committed snapshot")?;
        for volume in &record.config.volume_snapshots {
            for layer in &volume.layers {
                if let LayerRef::Managed(layer) = layer {
                    checked_layer(id, layer, layers)?;
                }
            }
        }
        for chain in managed_chains(manifest) {
            let mut parent = None;
            for layer in chain {
                let audit = checked_layer(id, layer, layers)?;
                let uuid = audit.value;
                ensure!(!uuid.is_nil(), "{id} references a nil layer UUID");
                let identity = (parent, layer.digest.clone());
                ensure!(
                    relations
                        .layers
                        .entry(uuid)
                        .or_insert_with(|| identity.clone())
                        == &identity,
                    "{id} reuses layer UUID {uuid} with a different managed parent or digest"
                );
                layer.size = audit.stamp.size;
                layer.uuid = Some(uuid.to_string());
                parent = Some(uuid);
            }
            if let Some(head) = parent {
                relations.heads.insert((id.clone(), head));
            }
        }
    }
    Ok(relations)
}

fn checked_layer<'a>(
    id: &SnapshotId,
    layer: &ManagedLayerRef,
    layers: &'a BTreeMap<String, Checked<Uuid>>,
) -> Result<&'a Checked<Uuid>> {
    let audit = layers
        .get(&layer.digest)
        .with_context(|| format!("{id} references unverified layer {}", layer.digest))?;
    ensure!(
        layer.size == 0 || layer.size == audit.stamp.size,
        "{id} has an invalid descriptor for {}",
        layer.digest
    );
    if let Some(declared) = &layer.uuid {
        ensure!(
            Uuid::parse_str(declared).ok() == Some(audit.value),
            "{id} declares UUID {declared} for {}, but the layer has {}",
            layer.digest,
            audit.value
        );
    }
    Ok(audit)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BackendManifest {
    version: u32,
    #[serde(default = "legacy_backend")]
    backend: String,
    #[serde(default)]
    guest: Option<Value>,
    #[serde(rename = "vmState")]
    _vm_state: VmState,
    memory: ImageSize,
    rootfs: ImageSize,
    attached_drives: Vec<Drive>,
}

fn legacy_backend() -> String {
    "firecracker".to_owned()
}

#[derive(Deserialize)]
struct VmState {}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImageSize {
    virtual_size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Drive {
    drive_id: String,
    read_only: bool,
    virtual_size: u64,
}

fn validate_backend(manifest: &BackendManifest, committed: &SnapshotManifest) -> Result<()> {
    ensure!(
        manifest.version == 1,
        "unsupported backend manifest version {}",
        manifest.version
    );
    ensure!(
        manifest.backend == "firecracker",
        "unsupported snapshot backend {:?}",
        manifest.backend
    );
    if let Some(guest) = &manifest.guest {
        ensure!(
            guest.get("os").and_then(Value::as_str) == Some("linux"),
            "only Linux Firecracker snapshots can be imported"
        );
    }
    ensure!(
        manifest.memory.virtual_size > 0
            && !committed.memory_layers.is_empty()
            && manifest.rootfs.virtual_size > 0
            && !committed.rootfs_layers.is_empty(),
        "missing memory or rootfs image"
    );
    ensure!(
        manifest.attached_drives.len() == committed.attached_drives.len(),
        "attached drive count differs from the legacy record"
    );
    let mut drives = BTreeSet::new();
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
            drives.insert(drive_id)
                && artifact.drive_id == *drive_id
                && artifact.read_only == *read_only
                && artifact.virtual_size == *virtual_size
                && *virtual_size > 0
                && !layers.is_empty(),
            "invalid attached drive {drive_id}"
        );
    }
    Ok(())
}

pub fn record_json(record: &SnapshotRecord) -> Result<Value> {
    Ok(record.artifact_json()?)
}
