use std::collections::{BTreeSet, HashMap};

use agentenv_domain::{
    CommittedAttachedDrive, ExternalLayer, LayerRef, ManagedLayerRef, SnapshotId, SnapshotManifest,
    SnapshotPlacement, SnapshotRecord, SnapshotSource,
};
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::store::{Stamp, Store};

pub async fn read_snapshot(
    store: &mut Store,
    id: &SnapshotId,
    audited_layers: &mut HashMap<String, (Stamp, Uuid)>,
) -> Result<SnapshotRecord> {
    let key = format!("catalog/records/{id}.json");
    let stamp = store.stamp(&key).await?;
    let value: Value = serde_json::from_slice(&store.read_checked(&key, &stamp).await?)
        .with_context(|| format!("decode {key}"))?;
    ensure!(
        value.get("snapshot_type").and_then(Value::as_str) != Some("local"),
        "snapshot {id} is Local; promote it on its owner before importing"
    );

    let backend_key = format!("artifacts/{id}/firecracker-manifest.json");
    let backend_stamp = store.stamp(&backend_key).await?;
    let backend = serde_json::from_slice(&store.read_checked(&backend_key, &backend_stamp).await?)
        .with_context(|| format!("decode {backend_key}"))?;
    let state_key = format!("artifacts/{id}/vm_state.bin");
    ensure!(
        store.stamp(&state_key).await?.size > 0,
        "{state_key} is empty"
    );

    let mut record = decode_legacy_record(value, backend)
        .with_context(|| format!("decode legacy snapshot {key}"))?;
    ensure!(record.id == *id, "{key} contains a different snapshot ID");
    ensure!(
        record.snapshot_type == SnapshotPlacement::Distributed,
        "snapshot {id} is Local"
    );
    record
        .validate()
        .map_err(anyhow::Error::msg)
        .with_context(|| format!("validate {key}"))?;
    let manifest = record
        .manifest
        .as_mut()
        .context("missing committed snapshot")?;
    let isolated = manifest.isolated_layers;
    let managed_url = store.layer_url(isolated);
    classify_owned_layers(manifest, &managed_url);
    for layer in managed_chains(manifest).flatten() {
        let (size, uuid) = audit_layer(store, audited_layers, layer, isolated).await?;
        layer.size = size;
        layer.uuid = (!uuid.is_nil()).then(|| uuid.to_string());
    }
    for volume in &record.config.volume_snapshots {
        for layer in &volume.layers {
            if let LayerRef::Managed(layer) = layer {
                audit_layer(store, audited_layers, layer, false).await?;
            }
        }
    }
    Ok(record)
}

async fn audit_layer(
    store: &mut Store,
    audited: &mut HashMap<String, (Stamp, Uuid)>,
    layer: &ManagedLayerRef,
    isolated: bool,
) -> Result<(u64, Uuid)> {
    let key = layer_key(&layer.digest, isolated)?;
    let (stamp, uuid) = match audited.entry(key.clone()) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let stamp = store.stamp(&key).await?;
            ensure!(stamp.size > 0, "{key} is empty");
            let uuid = store
                .layer_uuid(&key, &layer.digest, &stamp)
                .await
                .with_context(|| format!("inspect managed layer {key}"))?;
            entry.insert((stamp, uuid))
        }
    };
    ensure!(
        layer.size == 0 || layer.size == stamp.size,
        "{key} has an invalid recorded size"
    );
    if let Some(declared) = &layer.uuid {
        ensure!(
            Uuid::parse_str(declared).ok() == Some(*uuid),
            "{key} declares UUID {declared}, but the layer has {uuid}"
        );
    }
    Ok((stamp.size, *uuid))
}

fn layer_key(digest: &str, isolated: bool) -> Result<String> {
    let hash = digest.strip_prefix("sha256:").unwrap_or_default();
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid managed layer digest {digest}"
    );
    let prefix = if isolated {
        "snapshot-layers"
    } else {
        "managed-layers"
    };
    Ok(format!("{prefix}/{digest}"))
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
