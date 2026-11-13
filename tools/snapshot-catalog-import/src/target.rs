use std::collections::{BTreeMap, BTreeSet};

use agentenv_domain::{SnapshotListFilter, SnapshotMetadata};
use agentenv_metadata::MetaServer;
use anyhow::{ensure, Context, Result};

use crate::legacy::{record_json, sha256, Catalog, Checked, Plan};
use crate::shadow::Shadow;
use crate::store::{Stamp, Store};

async fn manifests(
    store: &mut Store,
    catalog: &Catalog,
    plan: &mut Plan,
    artifact_inventory: &BTreeMap<String, Stamp>,
    allow_write: bool,
) -> Result<()> {
    let mut missing = Vec::new();
    for (id, record) in &catalog.records {
        let expected = record_json(record)?;
        let digest = sha256(&serde_json::to_vec(&expected)?);
        let key = format!("artifacts/{id}/snapshot.json");
        let Some(stamp) = artifact_inventory.get(&key) else {
            ensure!(allow_write, "{key} is missing");
            missing.push((id, key, expected, digest));
            continue;
        };
        let checked = plan.manifests.get(id);
        if checked.is_some_and(|checked| checked.stamp == *stamp && checked.value == digest) {
            continue;
        }
        let actual: serde_json::Value =
            serde_json::from_slice(&store.read_checked(&key, stamp).await?)
                .with_context(|| format!("decode {key}"))?;
        ensure!(actual == expected, "existing {key} has different content");
        plan.manifests.insert(
            id.clone(),
            Checked {
                stamp: stamp.clone(),
                value: digest,
            },
        );
    }
    for (id, key, expected, digest) in missing {
        // The legacy directory can delete this path, but only the single
        // shadow importer creates committed manifests before handoff.
        if let Some(bytes) = store.read_optional(&key).await? {
            let actual: serde_json::Value =
                serde_json::from_slice(&bytes).with_context(|| format!("decode {key}"))?;
            ensure!(actual == expected, "existing {key} has different content");
        } else {
            store
                .write(&key, serde_json::to_vec_pretty(&expected)?)
                .await?;
        }
        let stamp = store.stamp(&key).await?;
        let actual: serde_json::Value =
            serde_json::from_slice(&store.read_checked(&key, &stamp).await?)?;
        ensure!(actual == expected, "{key} differs after write");
        plan.manifests.insert(
            id.clone(),
            Checked {
                stamp,
                value: digest,
            },
        );
    }
    Ok(())
}

pub async fn sync(
    shadow: &mut Shadow,
    metadata: &MetaServer,
    store: &mut Store,
    catalog: &Catalog,
    plan: &mut Plan,
    artifact_inventory: &BTreeMap<String, Stamp>,
) -> Result<()> {
    shadow.check_state().await?;
    let mut keep = BTreeSet::new();
    let mut remove = BTreeSet::new();
    for entry in metadata
        .list_snapshots(SnapshotListFilter::default())
        .await?
    {
        let id = entry.id().clone();
        shadow.ensure_owned(&id)?;
        let unchanged = match catalog.records.get(&id) {
            Some(record) => entry == SnapshotMetadata::from(record),
            None => false,
        };
        if unchanged {
            keep.insert(id);
        } else {
            remove.insert(id);
        }
    }
    manifests(store, catalog, plan, artifact_inventory, true).await?;
    shadow
        .reserve_delta(&remove, catalog.records.keys().cloned().collect())
        .await?;
    let mut registrations = 0;
    for (id, record) in &catalog.records {
        if !keep.contains(id) {
            metadata
                .register_snapshot(
                    record.into(),
                    record
                        .manifest
                        .clone()
                        .context("missing committed snapshot")?,
                    &[],
                )
                .await
                .with_context(|| format!("register snapshot {id}"))?;
            registrations += 1;
        }
    }
    println!(
        "Registered {registrations}, withdrew {}, reused {}",
        remove.len(),
        keep.len()
    );
    Ok(())
}

pub async fn verify(
    shadow: &mut Shadow,
    metadata: &MetaServer,
    store: &mut Store,
    catalog: &Catalog,
    plan: &mut Plan,
    artifact_inventory: &BTreeMap<String, Stamp>,
) -> Result<()> {
    shadow.check_state().await?;
    let entries = metadata
        .list_snapshots(SnapshotListFilter::default())
        .await?;
    ensure!(
        entries.len() == catalog.records.len(),
        "target has {} of {} snapshots",
        entries.len(),
        catalog.records.len()
    );
    for entry in entries {
        let id = entry.id().clone();
        shadow.ensure_owned(&id)?;
        let record = catalog
            .records
            .get(&id)
            .with_context(|| format!("unexpected snapshot {id}"))?;
        ensure!(
            entry == SnapshotMetadata::from(record),
            "target snapshot metadata differs for {id}"
        );
    }
    shadow.verify_relations(&catalog.relations).await?;
    manifests(store, catalog, plan, artifact_inventory, false).await?;
    shadow.seal().await?;
    Ok(())
}
