//! Targeted import of selected legacy Distributed sandbox snapshots into the SQL
//! snapshot directory (`aenv-snapshot import`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, IsTerminal, Read};

use agentenv_metadata::{context::current as meta_context, MetaClient, SnapshotEntry};
use anyhow::{ensure, Context, Result};
use overlaybd::layer::LayerFile;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::repository::{
    BlobBody, BlobKey, BlobLocation, LayerRole, LegacyDocumentKey, Repository, RepositoryError,
    SnapshotFile,
};
use crate::snapshot::{
    CommittedAttachedDrive, LayerRef, ManagedLayerRef, SnapshotId, SnapshotManifest,
    SnapshotPlacement, SnapshotRecord, SnapshotSource,
};

/// Summary printed as JSON by `aenv-snapshot import`.
#[derive(Debug, Default, Serialize)]
pub struct SnapshotImportReport {
    pub imported: Vec<SnapshotId>,
    pub reused: Vec<SnapshotId>,
    pub failed: BTreeMap<SnapshotId, String>,
}

/// Parses deduplicated snapshot IDs (bare UUID or `aenv-snapshot-<uuid>`) from
/// positional arguments, or from piped stdin when no arguments are given.
pub fn read_snapshot_ids(args: &[String]) -> Result<Vec<SnapshotId>> {
    if !args.is_empty() {
        return parse_ids(args.iter().map(String::as_str));
    }
    ensure!(
        !io::stdin().is_terminal(),
        "provide snapshot IDs as arguments or pipe them via stdin"
    );
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .context("read snapshot IDs from stdin")?;
    parse_ids(
        input
            .lines()
            .map(|line| line.split('#').next().unwrap_or("").trim())
            .filter(|token| !token.is_empty()),
    )
}

fn parse_ids<'a>(tokens: impl IntoIterator<Item = &'a str>) -> Result<Vec<SnapshotId>> {
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

pub async fn import_snapshots<R: Repository + ?Sized>(
    repository: &R,
    meta: &MetaClient,
    ids: &[SnapshotId],
) -> SnapshotImportReport {
    let mut report = SnapshotImportReport::default();
    let mut sizes = HashMap::new();
    let mut uuids = HashMap::new();
    for id in ids {
        match import_one(repository, meta, id, &mut sizes, &mut uuids).await {
            Ok(true) => report.imported.push(id.clone()),
            Ok(false) => report.reused.push(id.clone()),
            Err(error) => {
                report.failed.insert(id.clone(), format!("{error:#}"));
            }
        }
    }
    report
}

fn collect_managed_layers<'a>(
    manifest: &'a mut SnapshotManifest,
    managed_url: Option<&str>,
) -> Vec<(LayerRole, &'a mut ManagedLayerRef)> {
    let disk_layers = manifest
        .rootfs_layers
        .iter_mut()
        .map(|layer| (LayerRole::Rootfs, layer))
        .chain(manifest.attached_drives.iter_mut().flat_map(|drive| {
            let CommittedAttachedDrive::Overlaybd { layers, .. } = drive;
            layers
                .iter_mut()
                .map(|layer| (LayerRole::AttachedDrive, layer))
        }));
    let mut managed = Vec::new();
    for (role, layer) in disk_layers {
        if let LayerRef::External(external) = layer {
            if managed_url.is_some_and(|url| {
                url.trim_end_matches('/') == external.repo_blob_url.trim_end_matches('/')
            }) {
                *layer = LayerRef::Managed(ManagedLayerRef {
                    digest: std::mem::take(&mut external.digest),
                    size: external.size,
                    uuid: None,
                });
            }
        }
        if let LayerRef::Managed(layer) = layer {
            managed.push((role, layer));
        }
    }
    managed.extend(
        manifest
            .memory_layers
            .iter_mut()
            .map(|layer| (LayerRole::Memory, layer)),
    );
    managed
}

async fn import_one<R: Repository + ?Sized>(
    repository: &R,
    meta: &MetaClient,
    id: &SnapshotId,
    sizes: &mut HashMap<String, u64>,
    uuids: &mut HashMap<String, Uuid>,
) -> Result<bool> {
    let existing = match meta
        .get_snapshot(meta_context(), id.to_string())
        .await
        .context("query SQL snapshot directory")??
    {
        Some(entry) if entry.id() == id => {
            ensure!(entry.is_visible(), "snapshot '{id}' is retired");
            Some(entry)
        }
        _ => None,
    };

    let bytes = repository
        .get_legacy_document(LegacyDocumentKey::SnapshotRecord(id.clone()))
        .await?
        .with_context(|| format!("legacy snapshot '{id}' not found"))?;
    let raw: Value =
        serde_json::from_slice(&bytes).with_context(|| format!("parse legacy snapshot '{id}'"))?;
    ensure!(
        raw.get("deleting").and_then(Value::as_bool) != Some(true),
        "legacy snapshot '{id}' is deleting"
    );
    let mut record: SnapshotRecord =
        serde_json::from_value(raw).with_context(|| format!("parse legacy snapshot '{id}'"))?;
    ensure!(&record.id == id, "legacy snapshot '{id}' has mismatched ID");
    ensure!(
        matches!(record.config.source, SnapshotSource::Sandbox { .. }),
        "snapshot '{id}' is not a sandbox snapshot"
    );
    ensure!(
        record.snapshot_type == SnapshotPlacement::Distributed,
        "snapshot '{id}' is Local; promote it on its owner before importing"
    );
    record.validate().map_err(anyhow::Error::msg)?;
    {
        let manifest = record
            .manifest
            .as_ref()
            .with_context(|| format!("snapshot '{id}' is missing committed manifest"))?;
        ensure!(
            !manifest.rootfs_layers.is_empty() && !manifest.memory_layers.is_empty(),
            "snapshot '{id}' is missing rootfs or memory layers"
        );
    }

    if let Some(alias) = record.alias.as_ref() {
        let bytes = repository
            .get_legacy_document(LegacyDocumentKey::SnapshotAlias(alias.clone()))
            .await?
            .with_context(|| format!("legacy alias '{alias}' is missing for snapshot '{id}'"))?;
        let bound: SnapshotId = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse legacy alias '{alias}'"))?;
        ensure!(
            bound == *id,
            "legacy alias '{alias}' already points to '{bound}'"
        );
    }

    let vm_state = repository
        .stat_blob(&BlobKey::Snapshot(id.clone(), SnapshotFile::VmState))
        .await?
        .with_context(|| format!("snapshot '{id}' is missing vm_state.bin"))?;
    ensure!(vm_state.size > 0, "snapshot '{id}' has empty vm_state.bin");

    let backend_manifest = repository
        .stat_blob(&BlobKey::Snapshot(
            id.clone(),
            SnapshotFile::FirecrackerManifest,
        ))
        .await?
        .with_context(|| format!("snapshot '{id}' is missing firecracker-manifest.json"))?;
    ensure!(
        backend_manifest.size > 0,
        "snapshot '{id}' has empty firecracker-manifest.json"
    );

    let managed_url = repository.managed_layers_repo_blob_url();
    let manifest = record
        .manifest
        .as_mut()
        .expect("validated Distributed manifest");
    for (role, layer) in collect_managed_layers(manifest, managed_url.as_deref()) {
        let key = BlobKey::Layer {
            digest: layer.digest.clone(),
            role,
        };
        let size = match sizes.get(&layer.digest) {
            Some(&size) => size,
            None => {
                let meta = repository
                    .stat_blob(&key)
                    .await?
                    .with_context(|| format!("missing managed layer '{}'", layer.digest))?;
                ensure!(meta.size > 0, "managed layer '{}' is empty", layer.digest);
                sizes.insert(layer.digest.clone(), meta.size);
                meta.size
            }
        };
        ensure!(
            layer.size == 0 || layer.size == size,
            "layer '{}' size mismatch: declared {}, actual {size}",
            layer.digest,
            layer.size
        );
        layer.size = size;
        if layer.uuid.is_none() {
            let uuid = match uuids.get(&layer.digest) {
                Some(&uuid) => uuid,
                None => {
                    let (path, _staging) = match repository.locate_blob(&key)? {
                        BlobLocation::Local(path) => (path, None),
                        BlobLocation::Remote(_) => {
                            let file = repository.staging_file()?;
                            repository.fetch_blob(&key, file.path()).await?;
                            (file.path().to_path_buf(), Some(file))
                        }
                    };
                    let uuid = LayerFile::from_path(path, None)
                        .await
                        .with_context(|| format!("read UUID of managed layer '{}'", layer.digest))?
                        .uuid;
                    ensure!(
                        !uuid.is_nil(),
                        "managed layer '{}' has nil UUID",
                        layer.digest
                    );
                    uuids.insert(layer.digest.clone(), uuid);
                    uuid
                }
            };
            layer.uuid = Some(uuid.to_string());
        }
    }

    let needs_promote = existing
        .as_ref()
        .is_some_and(|entry| matches!(entry.snapshot_placement(), Ok(SnapshotPlacement::Local)));
    if let Some(entry) = existing.as_ref() {
        let expected = SnapshotEntry::from_record(&record);
        if needs_promote {
            ensure!(
                entry.source_sandbox_id == expected.source_sandbox_id
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

    let expected_bytes = crate::repository::encode_snapshot_content(&record)?;
    let expected: Value = serde_json::from_slice(&expected_bytes)?;
    let content_key = BlobKey::Snapshot(id.clone(), SnapshotFile::Record);
    match repository.read_blob(&content_key).await {
        Ok(bytes) => {
            let actual: Value = serde_json::from_slice(&bytes)
                .with_context(|| format!("decode existing snapshot.json for '{id}'"))?;
            ensure!(
                actual == expected,
                "existing snapshot.json for '{id}' has different content"
            );
        }
        Err(RepositoryError::ArtifactNotFound { .. }) => {
            ensure!(
                existing.is_none() || needs_promote,
                "registered snapshot '{id}' is missing snapshot.json"
            );
            repository
                .put_blob(&content_key, BlobBody::Bytes(expected_bytes))
                .await?;
        }
        Err(error) => return Err(error.into()),
    }

    if needs_promote {
        meta.promote_snapshot(meta_context(), record, Vec::new())
            .await
            .with_context(|| format!("promote snapshot '{id}'"))??;
        return Ok(true);
    }
    if existing.is_some() {
        return Ok(false);
    }
    meta.register_snapshot(meta_context(), record, Vec::new())
        .await
        .with_context(|| format!("register snapshot '{id}'"))??;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use agentenv_metadata::{
        transport, DatabaseConfig as MetaDatabaseConfig, MetaError, MetaServer,
    };
    use overlaybd::backend::local::LocalFile;
    use overlaybd::index_file::{create_file_rw, LayerInfo};
    use overlaybd::virtual_file::VirtualFile;
    use serde_json::json;

    use super::*;
    use crate::repository::backends::PosixFsRepository;
    use crate::snapshot::SnapshotAlias;

    async fn store_layer(
        repo: &PosixFsRepository,
        dir: &Path,
        name: &str,
        role: LayerRole,
    ) -> Result<ManagedLayerRef> {
        let path = dir.join(name);
        let info = LayerInfo::new(
            Arc::new(LocalFile::new(&path)?),
            Some(Arc::new(LocalFile::new(dir.join(format!("{name}.index")))?)),
            4 * 1024 * 1024,
        );
        let uuid = info.uuid;
        let layer = create_file_rw(info).await?;
        layer.write_at(4096, &[0xA5; 4096]).await?;
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
            uuid: Some(uuid.to_string()),
        })
    }

    async fn seed_legacy(
        repo: &PosixFsRepository,
        id: &SnapshotId,
        alias: Option<&str>,
        rootfs: &ManagedLayerRef,
        memory: &ManagedLayerRef,
    ) -> Result<()> {
        let backend = json!({
            "version": 1, "backend": "firecracker", "vmState": {}, "vsock": true,
            "memory": {"virtualSize": 4194304}, "rootfs": {"virtualSize": 4194304}, "attachedDrives": [],
        });
        let legacy = json!({
            "id": id, "alias": alias,
            "source": {"Sandbox": {"source_sandbox_id": "original"}},
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
        repo.put_legacy_document(
            LegacyDocumentKey::SnapshotRecord(id.clone()),
            serde_json::to_vec(&legacy)?,
        )
        .await?;
        if let Some(alias) = alias {
            repo.put_legacy_document(
                LegacyDocumentKey::SnapshotAlias(SnapshotAlias::parse(alias)?),
                serde_json::to_vec(id)?,
            )
            .await?;
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

    #[tokio::test]
    async fn imports_selected_snapshots_idempotently_and_enforces_guardrails() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let repo = PosixFsRepository::new(tmp.path().join("repo"));
        let rootfs = store_layer(&repo, tmp.path(), "rootfs", LayerRole::Rootfs).await?;
        let memory = store_layer(&repo, tmp.path(), "memory", LayerRole::Memory).await?;

        let db = MetaDatabaseConfig::Sqlite {
            path: tmp.path().join("catalog.sqlite3"),
        };
        db.migrate().await?;
        let server = MetaServer::open(&db).await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let rpc = tokio::spawn(transport::serve_tcp(listener, server.clone(), async move {
            let _ = stop_rx.await;
            Ok(())
        }));
        let meta = transport::connect(&addr.to_string()).await?;

        // Existing unrelated pinned SQL entry must stay untouched.
        let unrelated = sandbox_record();
        server.register_snapshot(unrelated.clone(), &[]).await?;
        let pin_id = Uuid::now_v7();
        assert!(
            server
                .pin_resource(
                    agentenv_domain::ResourceKind::Snapshot,
                    &unrelated.id.to_string(),
                    pin_id,
                    Some("node-a".into()),
                )
                .await?
        );

        // 1. Targeted import + idempotent rerun + untouched unselected snapshot.
        let (selected, unselected) = (SnapshotId::generate(), SnapshotId::generate());
        seed_legacy(&repo, &selected, Some("kept"), &rootfs, &memory).await?;
        seed_legacy(&repo, &unselected, None, &rootfs, &memory).await?;

        let report = import_snapshots(&repo, &meta, std::slice::from_ref(&selected)).await;
        assert_eq!(report.imported, vec![selected.clone()]);
        assert!(report.failed.is_empty());
        assert_eq!(server.get_snapshot("kept").await?.unwrap().id(), &selected);
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
            json!([{"Managed": rootfs}])
        );
        assert_eq!(content["manifest"]["memory_layers"], json!([memory]));

        let rerun = import_snapshots(&repo, &meta, std::slice::from_ref(&selected)).await;
        assert_eq!(rerun.reused, vec![selected.clone()]);
        assert!(rerun.imported.is_empty() && rerun.failed.is_empty());

        // 2. Guardrails: content drift, retired SQL ID, and Local snapshot are rejected.
        repo.put_blob(
            &BlobKey::Snapshot(selected.clone(), SnapshotFile::Record),
            BlobBody::Bytes(b"{}".to_vec()),
        )
        .await?;
        assert!(
            import_snapshots(&repo, &meta, std::slice::from_ref(&selected))
                .await
                .failed[&selected]
                .contains("different content")
        );

        let retired = sandbox_record();
        let retired_id = retired.id.clone();
        server.register_snapshot(retired, &[]).await?;
        server.retire_snapshot(retired_id.clone()).await?;
        assert!(
            import_snapshots(&repo, &meta, std::slice::from_ref(&retired_id))
                .await
                .failed[&retired_id]
                .contains("is retired")
        );

        let mut local = sandbox_record();
        local.snapshot_type = SnapshotPlacement::Local;
        local.owner_node_id = Some("node-1".into());
        local.manifest = None;
        let local_id = local.id.clone();
        repo.put_legacy_document(
            LegacyDocumentKey::SnapshotRecord(local_id.clone()),
            serde_json::to_vec(&local)?,
        )
        .await?;
        assert!(
            import_snapshots(&repo, &meta, std::slice::from_ref(&local_id))
                .await
                .failed[&local_id]
                .contains("Local")
        );

        // 3. Half-promoted snapshot (Distributed in legacy, stuck as Local in SQL) is repaired.
        let repaired_id = SnapshotId::generate();
        let mut local_sql = sandbox_record();
        local_sql.id = repaired_id.clone();
        local_sql.snapshot_type = SnapshotPlacement::Local;
        local_sql.owner_node_id = Some("node-1".into());
        local_sql.manifest = None;
        local_sql.created_at_unix_ms = 1;
        local_sql.updated_at_unix_ms = 1;
        server.register_snapshot(local_sql, &[]).await?;
        seed_legacy(&repo, &repaired_id, Some("repaired"), &rootfs, &memory).await?;

        let report = import_snapshots(&repo, &meta, std::slice::from_ref(&repaired_id)).await;
        assert_eq!(report.imported, vec![repaired_id.clone()]);
        assert_eq!(
            server
                .get_snapshot("repaired")
                .await?
                .unwrap()
                .snapshot_placement()?,
            SnapshotPlacement::Distributed
        );

        // 4. Existing SQL pin on unrelated snapshot remained intact.
        server.retire_snapshot(unrelated.id.clone()).await?;
        assert!(
            server
                .pin_resource(
                    agentenv_domain::ResourceKind::Snapshot,
                    &unrelated.id.to_string(),
                    pin_id,
                    Some("node-a".into()),
                )
                .await?
        );
        assert!(matches!(
            server
                .pin_resource(
                    agentenv_domain::ResourceKind::Snapshot,
                    &unrelated.id.to_string(),
                    Uuid::now_v7(),
                    Some("node-a".into()),
                )
                .await,
            Err(MetaError::Unavailable { .. })
        ));

        drop(meta);
        let _ = stop_tx.send(());
        rpc.await??;
        server.close().await;
        Ok(())
    }

    #[test]
    fn parses_and_deduplicates_snapshot_ids() -> Result<()> {
        let (a, b) = (SnapshotId::generate(), SnapshotId::generate());
        assert_eq!(
            read_snapshot_ids(&[format!("aenv-snapshot-{a}"), b.to_string(), a.to_string()])?,
            vec![a, b]
        );
        assert!(parse_ids(std::iter::empty()).is_err());
        Ok(())
    }
}
