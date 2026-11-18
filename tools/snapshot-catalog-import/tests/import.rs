use std::env;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use agentenv_domain::{
    ExternalLayer, LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotManifest, SnapshotMetadata, SnapshotRecord, SnapshotSource, SnapshotVolume,
};
use agentenv_metadata::{DatabaseConfig, LayerState, MetaServer};
use anyhow::{Context, Result};
use object_store_operator::{
    build_object_store_operator, AddressingStyle, ObjectStoreOperatorConfig, Operator,
    ResolvedCredential,
};
use overlaybd::backend::local::LocalFile;
use overlaybd::index_file::{create_file_rw, LayerInfo};
use overlaybd::virtual_file::VirtualFile;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const BUCKET: &str = "snapshot-catalog-import-test";

#[tokio::test]
#[ignore = "requires a disposable local S3 endpoint and the snapshot-catalog-import-test bucket"]
async fn imports_selected_snapshots_into_an_existing_directory() -> Result<()> {
    let endpoint = env::var("IMPORT_TEST_S3_ENDPOINT")?;
    let prefix = format!("catalog-import-{}", Uuid::new_v4());
    let store = build_object_store_operator(
        &ObjectStoreOperatorConfig {
            service: object_store_operator::ObjectStoreService::S3,
            endpoint: endpoint.clone(),
            bucket: BUCKET.into(),
            region: "us-east-1".into(),
            addressing_style: AddressingStyle::Path,
            timeout: None,
            max_retries: None,
        },
        Some(&ResolvedCredential::new(
            env::var("AWS_ACCESS_KEY_ID")?,
            env::var("AWS_SECRET_ACCESS_KEY")?,
            None,
            None,
        )?),
    )?;
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("catalog.sqlite3");
    let postgres = env::var("IMPORT_TEST_POSTGRES_URL").ok();
    let database = match &postgres {
        Some(url) => DatabaseConfig::Postgres { url: url.clone() },
        None => DatabaseConfig::Sqlite {
            path: db_path.clone(),
        },
    };
    database.migrate().await?;
    let metadata = MetaServer::open(&database).await?;
    let unrelated = sandbox_snapshot(SnapshotManifest::default());
    metadata
        .register_snapshot(
            SnapshotMetadata::from(&unrelated),
            SnapshotManifest::default(),
            &[],
        )
        .await?;
    let pin_id = Uuid::new_v4().to_string();
    metadata
        .pin_snapshot(unrelated.id.clone(), &pin_id)
        .await?
        .context("pin existing snapshot")?;

    let rootfs = make_layer(&store, &prefix, tmp.path(), "rootfs").await?;
    let memory = put_layer(&store, &prefix, tmp.path(), "memory", Uuid::nil()).await?;
    let committed = SnapshotManifest {
        rootfs_layers: vec![LayerRef::External(ExternalLayer {
            digest: rootfs.digest.clone(),
            size: 0,
            repo_blob_url: format!("s3://{BUCKET}/{prefix}/managed-layers/"),
        })],
        memory_layers: vec![memory],
        ..SnapshotManifest::default()
    };
    let first = sandbox_snapshot(committed.clone());
    let unselected = sandbox_snapshot(committed.clone());
    let mut second = sandbox_snapshot(committed.clone());
    second.alias = Some(SnapshotAlias::parse("selected")?);
    let shared_volume = put_layer(&store, &prefix, tmp.path(), "volume", Uuid::nil()).await?;
    second.config.volume_snapshots.push(SnapshotVolume {
        mount_path: "/data".into(),
        mode: Default::default(),
        size_mb: 4,
        layers: vec![LayerRef::Managed(shared_volume)],
    });
    second.config.custom_extension_params = Some(serde_json::from_value(json!({
        "counter": 1e19, "ratio": 1.6930994060828455_f64,
    }))?);
    for record in [&first, &second, &unselected] {
        put_snapshot(&store, &prefix, record).await?;
    }
    let legacy_key = format!("{prefix}/catalog/records/{}.json", second.id);
    let legacy_bytes = store.read(&legacy_key).await?.to_vec();
    let cli = |ids: &[SnapshotId], expected_error: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_snapshot-catalog-import"));
        command.args([
            "--endpoint",
            &endpoint,
            "--bucket",
            BUCKET,
            "--region",
            "us-east-1",
            "--prefix",
            &prefix,
        ]);
        if let Some(url) = &postgres {
            command.env("AENV_DATABASE_POSTGRES_URL", url);
        } else {
            command
                .arg("--sqlite-path")
                .arg(&db_path)
                .env_remove("AENV_DATABASE_POSTGRES_URL");
        }
        for id in ids {
            command.arg("--snapshot-id").arg(id.to_string());
        }
        let output = command.output().expect("run importer");
        let stderr = String::from_utf8_lossy(&output.stderr);
        match expected_error {
            Some(error) => assert!(
                !output.status.success() && stderr.contains(error),
                "expected {error:?}; {stderr}"
            ),
            None => assert!(output.status.success(), "{stderr}"),
        }
        String::from_utf8(output.stdout).expect("UTF-8 importer output")
    };
    let selected = [first.id.clone(), second.id.clone(), second.id.clone()];
    assert!(cli(&selected, None).contains("Imported 2, reused 0"));
    assert!(cli(&selected, None).contains("Imported 0, reused 2"));
    let first_key = format!("{prefix}/artifacts/{}/snapshot.json", first.id);
    let mut old: Value = serde_json::from_slice(&store.read(&first_key).await?.to_vec())?;
    old["metadata"] = serde_json::to_value(SnapshotMetadata::from(&first))?;
    let old = serde_json::to_vec(&old)?;
    store.write(&first_key, old.clone()).await?;
    assert!(cli(std::slice::from_ref(&first.id), None).contains("Imported 0, reused 1"));
    assert_eq!(store.read(&first_key).await?.to_vec(), old);
    assert_eq!(
        metadata
            .list_snapshots(SnapshotListFilter::default())
            .await?
            .len(),
        3
    );
    assert_eq!(
        metadata
            .get_snapshot(&unrelated.id.to_string())
            .await?
            .unwrap(),
        SnapshotMetadata::from(&unrelated)
    );
    assert!(metadata
        .get_snapshot(&unselected.id.to_string())
        .await?
        .is_none());
    assert!(
        !store
            .exists(&format!(
                "{prefix}/artifacts/{}/snapshot.json",
                unselected.id
            ))
            .await?
    );
    assert_eq!(store.read(&legacy_key).await?.to_vec(), legacy_bytes);

    let content_key = format!("{prefix}/artifacts/{}/snapshot.json", second.id);
    let content_bytes = store.read(&content_key).await?.to_vec();
    let content: Value = serde_json::from_slice(&content_bytes)?;
    assert_eq!(content.as_object().unwrap().len(), 2);
    assert!(content.get("metadata").is_none());
    assert_eq!(content["config"], serde_json::to_value(&second.config)?);
    let manifest: SnapshotManifest = serde_json::from_value(content["manifest"].clone())?;
    let legacy_memory = &manifest.memory_layers[0];
    assert!(legacy_memory.uuid.is_none());
    let memory_id = legacy_memory
        .directory_uuid()
        .map_err(anyhow::Error::msg)?
        .to_string();
    assert_eq!(
        metadata.check_layers(&[memory_id]).await?[0].state,
        LayerState::Available
    );
    let LayerRef::Managed(owned) = &manifest.rootfs_layers[0] else {
        panic!("legacy external lower inside the managed prefix must be tracked by SQL");
    };
    assert_eq!(owned.size, rootfs.size);
    let tracked = metadata
        .check_layers(&[owned.uuid.clone().context("missing imported UUID")?])
        .await?;
    assert_eq!(tracked[0].state, LayerState::Available);
    assert_eq!(
        metadata.get_snapshot("selected").await?.unwrap(),
        SnapshotMetadata::from(&second)
    );

    store.write(&content_key, "{}").await?;
    cli(std::slice::from_ref(&second.id), Some("different content"));
    assert_eq!(store.read(&content_key).await?.to_vec(), b"{}");
    store.write(&content_key, content_bytes).await?;
    let mut changed = second.clone();
    changed.updated_at_unix_ms += 1;
    store
        .write(&legacy_key, legacy_record_bytes(&changed)?)
        .await?;
    cli(
        std::slice::from_ref(&second.id),
        Some("different directory metadata"),
    );
    store.write(&legacy_key, legacy_bytes).await?;

    let mut conflict = sandbox_snapshot(committed);
    conflict.alias = second.alias.clone();
    put_snapshot(&store, &prefix, &conflict).await?;
    cli(
        std::slice::from_ref(&conflict.id),
        Some("already points to"),
    );
    assert!(metadata
        .get_snapshot(&conflict.id.to_string())
        .await?
        .is_none());
    assert_eq!(
        metadata.get_snapshot("selected").await?.unwrap(),
        SnapshotMetadata::from(&second)
    );

    metadata.retire_snapshot(unrelated.id.clone()).await?;
    assert!(
        metadata.reap_retired_snapshots(10).await?.is_empty(),
        "import must preserve the existing pin"
    );
    Ok(())
}

#[tokio::test]
async fn a_uuid_shaped_alias_does_not_mask_a_retired_import_id() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("catalog.sqlite3");
    let database = DatabaseConfig::Sqlite {
        path: db_path.clone(),
    };
    database.migrate().await?;
    let metadata = MetaServer::open(&database).await?;
    let retired = SnapshotId::generate();
    metadata.retire_snapshot(retired.clone()).await?;
    let mut other = sandbox_snapshot(SnapshotManifest::default());
    other.alias = Some(SnapshotAlias::parse(&retired.to_string())?);
    metadata
        .register_snapshot((&other).into(), SnapshotManifest::default(), &[])
        .await?;
    assert_eq!(
        metadata
            .get_snapshot(&retired.to_string())
            .await?
            .unwrap()
            .id,
        other.id
    );
    let output = Command::new(env!("CARGO_BIN_EXE_snapshot-catalog-import"))
        .args([
            "--endpoint",
            "http://127.0.0.1:1",
            "--bucket",
            BUCKET,
            "--region",
            "us-east-1",
            "--prefix",
            "fixture",
        ])
        .arg("--snapshot-id")
        .arg(retired.to_string())
        .arg("--sqlite-path")
        .arg(&db_path)
        .env("AWS_ACCESS_KEY_ID", "fixture-access")
        .env("AWS_SECRET_ACCESS_KEY", "fixture-secret")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("SNAPSHOT_IMPORT_CREDENTIAL_PROCESS")
        .env_remove("AENV_DATABASE_POSTGRES_URL")
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && stderr.contains("is retired"),
        "{stderr}"
    );
    assert!(!metadata
        .get_snapshot_by_id(&retired)
        .await?
        .context("retirement lost")?
        .is_visible());
    Ok(())
}

fn sandbox_snapshot(manifest: SnapshotManifest) -> SnapshotRecord {
    let mut record = SnapshotRecord::mock_ready(manifest);
    record.config.source = SnapshotSource::Sandbox {
        source_sandbox_id: "source-sandbox".into(),
        source_sandbox_metadata: None,
    };
    record
}

fn legacy_record_bytes(record: &SnapshotRecord) -> Result<Vec<u8>> {
    let mut committed = serde_json::to_value(&record.manifest)?;
    let committed_obj = committed.as_object_mut().expect("manifest object");
    let config_val = serde_json::to_value(&record.config)?;
    let config_obj = config_val.as_object().expect("config object");
    for key in [
        "context",
        "startup",
        "runtime_versions",
        "image_configs",
        "volume_snapshots",
        "custom_extension_params",
    ] {
        if let Some(value) = config_obj.get(key) {
            committed_obj.insert(key.to_owned(), value.clone());
        }
    }
    let legacy = json!({
        "id": record.id,
        "snapshot_type": record.snapshot_type,
        "owner_node_id": record.owner_node_id,
        "alias": record.alias,
        "source": record.config.source,
        "resources": record.config.resources,
        "created_at_unix_ms": record.created_at_unix_ms,
        "updated_at_unix_ms": record.updated_at_unix_ms,
        "committed": committed,
    });
    Ok(serde_json::to_vec(&legacy)?)
}

async fn put_snapshot(store: &Operator, prefix: &str, record: &SnapshotRecord) -> Result<()> {
    let id = &record.id;
    store
        .write(
            &format!("{prefix}/artifacts/{id}/vm_state.bin"),
            "fixture VM state",
        )
        .await?;
    let backend = json!({
        "version": 1, "vmState": {},
        "backend": "firecracker",
        "memory": {"virtualSize": 4 * 1024 * 1024},
        "rootfs": {"virtualSize": 4 * 1024 * 1024}, "attachedDrives": [],
    });
    store
        .write(
            &format!("{prefix}/artifacts/{id}/firecracker-manifest.json"),
            serde_json::to_vec(&backend)?,
        )
        .await?;
    store
        .write(
            &format!("{prefix}/catalog/records/{id}.json"),
            legacy_record_bytes(record)?,
        )
        .await?;
    if let Some(alias) = &record.alias {
        store
            .write(
                &format!("{prefix}/catalog/aliases/{alias}.json"),
                serde_json::to_vec(id)?,
            )
            .await?;
    }
    Ok(())
}

async fn make_layer(
    store: &Operator,
    prefix: &str,
    dir: &Path,
    name: &str,
) -> Result<ManagedLayerRef> {
    put_layer(store, prefix, dir, name, Uuid::new_v4()).await
}

async fn put_layer(
    store: &Operator,
    prefix: &str,
    dir: &Path,
    name: &str,
    uuid: Uuid,
) -> Result<ManagedLayerRef> {
    let path = dir.join(name);
    let data = Arc::new(LocalFile::new(&path)?);
    let index = Arc::new(LocalFile::new(dir.join(format!("{name}.index")))?);
    let mut info = LayerInfo::new(data, Some(index), 4 * 1024 * 1024);
    info.uuid = uuid;
    let layer = create_file_rw(info).await?;
    layer.write_at(4096, &[0xA5; 4096]).await?;
    layer.close_seal().await?;
    let bytes = std::fs::read(path)?;
    let size = bytes.len() as u64;
    let digest = format!("sha256:{:x}", Sha256::digest(&bytes));
    store
        .write(&format!("{prefix}/managed-layers/{digest}"), bytes)
        .await?;
    Ok(ManagedLayerRef {
        digest,
        size,
        uuid: None,
    })
}
