use std::env;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use agentenv_domain::{
    ExternalLayer, LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotManifest, SnapshotMetadata, SnapshotPlacement, SnapshotRecord, SnapshotSource,
    SnapshotVolume,
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
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const BUCKET: &str = "snapshot-catalog-import-test";

#[tokio::test]
#[ignore = "requires a disposable local S3 endpoint and the snapshot-catalog-import-test bucket"]
async fn stages_and_reconciles_the_final_legacy_directory() -> Result<()> {
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

    let rootfs = make_layer(&store, &prefix, tmp.path(), "rootfs").await?;
    let memory = make_layer(&store, &prefix, tmp.path(), "memory").await?;
    let committed = SnapshotManifest {
        rootfs_layers: vec![LayerRef::External(ExternalLayer {
            digest: rootfs.digest.clone(),
            size: 0,
            repo_blob_url: format!("s3://{BUCKET}/{prefix}/managed-layers/"),
        })],
        memory_layers: vec![memory],
        ..SnapshotManifest::mock()
    };
    let obsolete = make_layer(&store, &prefix, tmp.path(), "obsolete-rootfs").await?;
    let mut old_committed = committed.clone();
    old_committed.rootfs_layers = vec![LayerRef::Managed(obsolete)];
    let mut replaced = sandbox_snapshot(old_committed);
    let mut retained = sandbox_snapshot(committed.clone());
    let volume_layer = put_layer(&store, &prefix, tmp.path(), "legacy-volume", Uuid::nil()).await?;
    retained.config.volume_snapshots.push(SnapshotVolume {
        mount_path: "/data".to_owned(),
        mode: Default::default(),
        size_mb: 4,
        layers: vec![LayerRef::Managed(volume_layer)],
    });
    let mut replacement = sandbox_snapshot(committed.clone());
    replacement.config.custom_extension_params = Some(serde_json::from_value(json!({
        "counter": 1e19,
        "ratio": 1.6930994060828455_f64,
    }))?);
    let mut promoted = sandbox_snapshot(SnapshotManifest::mock());
    promoted.snapshot_type = SnapshotPlacement::Local;
    promoted.owner_node_id = Some("old-node".to_owned());
    let alias = SnapshotAlias::parse("reused")?;
    replaced.alias = Some(alias.clone());
    replacement.alias = Some(alias);

    let cli = |action: &str, expected_error: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_snapshot-catalog-import"));
        command
            .args([
                "--endpoint",
                &endpoint,
                "--bucket",
                BUCKET,
                "--region",
                "us-east-1",
                "--prefix",
                &prefix,
            ])
            .arg("--plan")
            .arg(tmp.path().join("plan.json"))
            .arg(action);
        if matches!(action, "apply" | "verify") {
            command.arg("--source-frozen");
        }
        if action != "prepare" {
            if let Some(url) = &postgres {
                command.env("AENV_DATABASE_POSTGRES_URL", url);
            } else {
                command
                    .arg("--sqlite-path")
                    .arg(&db_path)
                    .env_remove("AENV_DATABASE_POSTGRES_URL");
            }
        }
        let output = command.output().expect("run importer");
        let stderr = String::from_utf8_lossy(&output.stderr);
        match expected_error {
            Some(error) => assert!(
                !output.status.success() && stderr.contains(error),
                "{action}: expected {error:?}; {stderr}"
            ),
            None => assert!(output.status.success(), "{action}: {stderr}"),
        }
        String::from_utf8(output.stdout).expect("UTF-8 importer output")
    };

    put_snapshot(&store, &prefix, &retained).await?;
    put_snapshot(&store, &prefix, &replaced).await?;
    store
        .write(
            &format!("{prefix}/catalog/records/{}.json", promoted.id),
            legacy_record_bytes(&promoted)?,
        )
        .await?;
    let mut in_flight = sandbox_snapshot(committed.clone());
    in_flight.alias = Some(SnapshotAlias::parse("pending")?);
    let pending_key = format!("{prefix}/catalog/records/{}.json", in_flight.id);
    // Legacy publication writes the record and alias separately.
    store
        .write(&pending_key, legacy_record_bytes(&in_flight)?)
        .await?;
    let retained_manifest = format!("{prefix}/artifacts/{}/snapshot.json", retained.id);
    cli("prepare", None);
    assert!(metadata
        .list_snapshots(SnapshotListFilter::default())
        .await?
        .is_empty());
    assert!(!store.exists(&retained_manifest).await?);
    cli("apply", Some("no shadow marker"));
    let staged = cli("stage", None);
    assert!(staged.contains("Registered 2, withdrew 0, reused 0"));
    assert!(staged.contains("1 Local snapshots awaiting owner action"));
    assert!(store.exists(&retained_manifest).await?);
    let imported: SnapshotRecord =
        serde_json::from_slice(&store.read(&retained_manifest).await?.to_vec())?;
    assert_eq!(
        imported.config.volume_snapshots,
        retained.config.volume_snapshots
    );
    assert_eq!(
        metadata
            .get_snapshot("reused")
            .await?
            .context("missing staged alias")?
            .id(),
        &replaced.id
    );
    assert!(metadata
        .get_snapshot(&promoted.id.to_string())
        .await?
        .is_none());
    assert!(staged.contains("1 deferred inventory items"));
    cli("apply", Some("Local snapshots remain"));

    let old_manifest = format!("{prefix}/artifacts/{}/snapshot.json", replaced.id);
    let old: SnapshotRecord = serde_json::from_slice(&store.read(&old_manifest).await?.to_vec())?;
    let LayerRef::Managed(old_rootfs) = &old.manifest.as_ref().unwrap().rootfs_layers[0] else {
        panic!("old rootfs layer was not staged");
    };

    store
        .delete(&format!("{prefix}/catalog/aliases/reused.json"))
        .await?;
    store
        .delete(&format!("{prefix}/catalog/records/{}.json", replaced.id))
        .await?;
    store.delete(&old_manifest).await?;
    put_snapshot(&store, &prefix, &replacement).await?;
    promoted.snapshot_type = SnapshotPlacement::Distributed;
    promoted.owner_node_id = None;
    let late_memory = make_layer(&store, &prefix, tmp.path(), "late-memory").await?;
    promoted.manifest = Some(SnapshotManifest {
        memory_layers: vec![late_memory],
        ..committed.clone()
    });
    put_snapshot(&store, &prefix, &promoted).await?;
    cli("apply", Some("legacy alias pending does not point to"));
    store.delete(&pending_key).await?;

    let manifest = format!("{prefix}/artifacts/{}/snapshot.json", replacement.id);
    let state = format!("{prefix}/artifacts/{}/vm_state.bin", retained.id);
    store.write(&state, "changed VM state").await?;
    cli("apply", Some("changed since the previous audit"));
    assert!(!store.exists(&manifest).await?);
    assert_eq!(
        metadata
            .get_snapshot("reused")
            .await?
            .context("staged alias lost after rejected apply")?
            .id(),
        &replaced.id
    );
    store.write(&state, "fixture VM state").await?;

    if postgres.is_none() {
        let pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::new().filename(&db_path),
        )
        .await?;
        // Make the second registration fail after the withdrawal and first
        // registration have committed. Retry through the normal CLI.
        let second = promoted.id.clone().max(replacement.id.clone());
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TRIGGER stop_registration BEFORE INSERT ON snapshots
             WHEN NEW.snapshot_id = '{second}'
             BEGIN SELECT RAISE(ABORT, 'fixture registration interrupted'); END"
        )))
        .execute(&pool)
        .await?;
        cli("apply", Some("fixture registration interrupted"));
        assert_eq!(
            metadata
                .list_snapshots(SnapshotListFilter::default())
                .await?
                .len(),
            2
        );
        assert!(metadata.get_snapshot(&second.to_string()).await?.is_none());
        sqlx::query("DROP TRIGGER stop_registration")
            .execute(&pool)
            .await?;
        assert!(cli("apply", None).contains("Registered 1, withdrew 0, reused 2"));
    } else {
        assert!(cli("apply", None).contains("Registered 2, withdrew 1, reused 1"));
    }
    assert!(cli("apply", None).contains("Registered 0, withdrew 0, reused 3"));
    assert_eq!(
        metadata
            .get_snapshot("reused")
            .await?
            .context("missing rebound alias")?
            .id(),
        &replacement.id
    );
    assert!(metadata
        .get_snapshot(&replaced.id.to_string())
        .await?
        .is_none());
    assert!(metadata
        .get_snapshot(&promoted.id.to_string())
        .await?
        .is_some());
    let old_uuid = old_rootfs.uuid.clone().context("missing old rootfs UUID")?;
    assert_eq!(
        metadata.check_layers(&[old_uuid]).await?[0].state,
        LayerState::Missing
    );

    let imported: SnapshotRecord = serde_json::from_slice(&store.read(&manifest).await?.to_vec())?;
    assert!(imported.manifest.as_ref().unwrap().memory_layers[0]
        .uuid
        .as_deref()
        .is_some_and(|uuid| Uuid::parse_str(uuid).is_ok()));
    let LayerRef::Managed(owned) = &imported.manifest.as_ref().unwrap().rootfs_layers[0] else {
        panic!("legacy external lower inside the managed prefix must be tracked by SQL");
    };
    assert_eq!(owned.size, rootfs.size);
    let uuid = owned
        .uuid
        .clone()
        .context("missing UUID on imported rootfs layer")?;
    let tracked = metadata.check_layers(&[uuid]).await?;
    assert_eq!(tracked[0].state, LayerState::Available);
    let entry = metadata
        .get_snapshot("reused")
        .await?
        .context("missing imported entry")?;
    assert_eq!(entry, SnapshotMetadata::from(&imported));
    assert_eq!(
        serde_json::to_value(&imported.config)?,
        serde_json::to_value(&replacement.config)?
    );
    assert_eq!(imported.id, replacement.id);

    let extra = sandbox_snapshot(committed);
    put_snapshot(&store, &prefix, &extra).await?;
    cli("verify", Some("legacy inventory changed after apply"));
    store
        .delete(&format!("{prefix}/catalog/records/{}.json", extra.id))
        .await?;

    let good_manifest = store.read(&manifest).await?.to_vec();
    store.write(&manifest, "{}").await?;
    cli("apply", Some("different content"));
    assert_eq!(store.read(&manifest).await?.to_vec(), b"{}");
    cli("verify", Some("different content"));
    store.write(&manifest, good_manifest).await?;
    cli("verify", None);
    cli("verify", None);
    cli("stage", Some("sealed"));
    cli("apply", Some("sealed"));
    Ok(())
}

#[tokio::test]
async fn stage_rejects_a_nonempty_database_before_opening_the_source() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let db_path = tmp.path().join("catalog.sqlite3");
    let database = DatabaseConfig::Sqlite {
        path: db_path.clone(),
    };
    database.migrate().await?;
    let metadata = MetaServer::open(&database).await?;
    // Retired rows are hidden from the public listing and must still block stage.
    let retired = SnapshotId::generate();
    metadata.retire_snapshot(retired.clone()).await?;

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
        .arg("--plan")
        .arg(tmp.path().join("plan.json"))
        .args(["stage", "--sqlite-path"])
        .arg(&db_path)
        .env("AWS_ACCESS_KEY_ID", "fixture-access")
        .env("AWS_SECRET_ACCESS_KEY", "fixture-secret")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("SNAPSHOT_IMPORT_CREDENTIAL_PROCESS")
        .env_remove("AENV_DATABASE_POSTGRES_URL")
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && stderr.contains("stage requires an empty shadow database"),
        "{stderr}"
    );
    assert!(!metadata
        .get_snapshot(&retired.to_string())
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
