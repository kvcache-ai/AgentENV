use agentenv_domain::{SnapshotContent, SnapshotMetadata, SnapshotRecord};
use agentenv_metadata::MetaServer;
use anyhow::{ensure, Context, Result};

use crate::store::Store;

pub async fn import_snapshot(
    metadata: &MetaServer,
    store: &mut Store,
    record: SnapshotRecord,
    existing: Option<SnapshotMetadata>,
) -> Result<bool> {
    let entry = SnapshotMetadata::from(&record);
    let id = &entry.id;
    if let Some(existing) = &existing {
        ensure!(
            *existing == entry,
            "snapshot {id} has different directory metadata"
        );
    }
    let content = SnapshotContent::from(&record);
    let expected = serde_json::to_value(&content)?;
    let key = format!("artifacts/{id}/snapshot.json");
    let bytes = match store.read_optional(&key).await? {
        Some(bytes) => bytes,
        None => {
            ensure!(
                existing.is_none(),
                "registered snapshot {id} is missing {key}"
            );
            store
                .create_content(&key, serde_json::to_vec_pretty(&expected)?)
                .await?;
            store
                .read_optional(&key)
                .await?
                .with_context(|| format!("read back {key}"))?
        }
    };
    let mut actual: serde_json::Value =
        serde_json::from_slice(&bytes).with_context(|| format!("decode {key}"))?;
    // Earlier RCs included directory metadata in the artifact. Keep existing bytes.
    if actual.get("metadata").is_some() || actual.get("id").is_some() {
        let old: SnapshotRecord = serde_json::from_value(actual)?;
        ensure!(&old.id == id, "{key} contains a different snapshot ID");
        actual = serde_json::to_value(SnapshotContent::from(&old))?;
    }
    ensure!(actual == expected, "existing {key} has different content");

    if existing.is_some() {
        return Ok(true);
    }
    let manifest = content.manifest.context("missing committed snapshot")?;
    metadata
        .register_snapshot(entry.clone(), manifest, &[])
        .await
        .with_context(|| format!("register snapshot {}", entry.id))?;
    Ok(false)
}
