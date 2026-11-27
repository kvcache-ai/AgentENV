use anyhow::Result;
use serde_json::{json, Value};
use std::path::Path;
use std::process::{Command, Output};

fn database_command(path: &Path, command: &str) -> Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_aenv-metadata"))
        .env("AENV_DATABASE_BACKEND", "sqlite")
        .env("AENV_DATABASE_SQLITE_PATH", path)
        .args(["db", command])
        .output()?)
}

#[test]
fn database_cli_is_read_only_until_migrate() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("not-created/metadata.sqlite3");
    let status = database_command(&path, "status")?;
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    let initial: Value = serde_json::from_slice(&status.stdout)?;
    assert_eq!(initial["initialized"], false);
    assert_eq!(initial["pending"], json!([1]));
    assert!(!path.parent().unwrap().exists());
    let check = database_command(&path, "check")?;
    assert!(!check.status.success());
    assert!(String::from_utf8_lossy(&check.stderr).contains("pending migrations"));
    assert!(!path.parent().unwrap().exists());
    let migrated = database_command(&path, "migrate")?;
    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    let repeated = database_command(&path, "migrate")?;
    assert!(repeated.status.success());
    let ready = database_command(&path, "check")?;
    assert!(
        ready.status.success(),
        "{}",
        String::from_utf8_lossy(&ready.stderr)
    );
    let ready: Value = serde_json::from_slice(&ready.stdout)?;
    assert_eq!(ready["initialized"], true);
    assert_eq!(ready["pending"], json!([]));
    assert_eq!(ready["applied"], json!([1]));

    // Conflicting backend flags must fail fast instead of falling back silently.
    for (args, expected) in [
        (
            &[
                "--backend",
                "sqlite",
                "--postgres-url",
                "postgresql://agentenv@localhost/agentenv",
                "db",
                "status",
            ][..],
            "cannot be set when --backend=sqlite",
        ),
        (
            &[
                "--backend",
                "postgres",
                "--sqlite-path",
                "/tmp/metadata.sqlite3",
                "--postgres-url",
                "postgresql://agentenv@localhost/agentenv",
                "db",
                "status",
            ][..],
            "cannot be set when --backend=postgres",
        ),
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_aenv-metadata"))
            .env_remove("AENV_DATABASE_BACKEND")
            .env_remove("AENV_DATABASE_SQLITE_PATH")
            .env_remove("AENV_DATABASE_POSTGRES_URL")
            .args(args)
            .output()?;
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains(expected));
    }
    Ok(())
}

async fn verify_cross_node_alias_restore(
    shared_store: &Path,
    node_a_local: &Path,
    node_b_local: &Path,
    meta_a: agentenv_metadata::MetaClient,
    meta_b: agentenv_metadata::MetaClient,
    alias: &str,
) -> Result<()> {
    use agentenv::snapshot::mock::{
        snapshot_config_for_test, test_posix_storage, write_mock_built_artifacts,
    };
    use agentenv::snapshot::{SnapshotAlias, SnapshotId, SnapshotManager, SnapshotSource};

    let (repo_a, res_a) = test_posix_storage(shared_store, node_a_local.to_path_buf())?;
    let (repo_b, res_b) = test_posix_storage(shared_store, node_b_local.to_path_buf())?;
    let node_a = SnapshotManager::from_parts(meta_a, repo_a, res_a, None);
    let node_b = SnapshotManager::from_parts(meta_b, repo_b, res_b, None);

    let staging = tempfile::tempdir()?;
    let (_, _, manifest) = write_mock_built_artifacts(staging.path())?;
    let snapshot_id = SnapshotId::generate();
    let config = snapshot_config_for_test(SnapshotSource::Sandbox {
        source_sandbox_id: "sandbox-on-node-a".to_string(),
        source_sandbox_metadata: None,
    });
    node_a
        .publish(
            snapshot_id.clone(),
            Some(SnapshotAlias::parse(alias)?),
            config,
            manifest,
            None,
        )
        .await?;

    let looked_up = node_b.get(alias).await?.expect("node B resolves alias");
    assert_eq!(looked_up.id, snapshot_id);
    let runnable = node_b.resolve_runnable(looked_up).await?;
    let runtime = runnable.manifest.runtime_manifest();
    assert_eq!(std::fs::read(&runtime.vm_state.path)?, b"vm state");
    assert!(runtime.rootfs.image_config_path.starts_with(node_b_local));
    assert!(runtime.memory.image_config_path.is_file());
    let rootfs_cfg: overlaybd::config::ImageConfig =
        serde_json::from_slice(&std::fs::read(&runtime.rootfs.image_config_path)?)?;
    assert!(!rootfs_cfg.lowers.is_empty());
    for lower in &rootfs_cfg.lowers {
        assert!(Path::new(&lower.file).is_file());
    }
    Ok(())
}

#[tokio::test]
async fn remote_gc_uses_the_service_repository_and_respects_pins() -> Result<()> {
    use agentenv::cfg::ConfigManager;
    use agentenv::snapshot::mock::test_posix_storage;
    use agentenv::snapshot::SnapshotManager;
    use agentenv_domain::{ResourceKind, SnapshotManifest, SnapshotRecord, SnapshotSource};
    use agentenv_metadata::{transport, DatabaseConfig, MetaServer};

    ConfigManager::init_global()?;

    let root = tempfile::tempdir()?;
    let database = root.path().join("directory.sqlite3");
    DatabaseConfig::Sqlite {
        path: database.clone(),
    }
    .migrate()
    .await?;
    let address = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let server_store = root.path().join("server-store");
    let client_store = root.path().join("client-store");
    let server_cfg = root.path().join("server.toml");
    let client_cfg = root.path().join("client.toml");
    std::fs::write(
        &server_cfg,
        format!("[backend.posix_fs]\nsnapshot_store = {:?}\n", server_store),
    )?;
    std::fs::write(
        &client_cfg,
        format!(
            "[database]\nmeta_address = \"{address}\"\n[backend.posix_fs]\nsnapshot_store = {:?}\n",
            client_store
        ),
    )?;

    let spawn_daemon = || -> Result<tokio::process::Child> {
        Ok(
            tokio::process::Command::new(env!("CARGO_BIN_EXE_aenv-metadata"))
                .args(["--backend", "sqlite", "--sqlite-path"])
                .arg(&database)
                .args(["serve", "--listen"])
                .arg(address.to_string())
                .arg("--config")
                .arg(&server_cfg)
                .env("AENV_HOME_PATH", root.path())
                .kill_on_drop(true)
                .spawn()?,
        )
    };
    let addr = address.to_string();
    async fn connect(
        daemon: &mut tokio::process::Child,
        addr: &str,
    ) -> Result<agentenv_metadata::MetaClient> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(status) = daemon.try_wait()? {
                    anyhow::bail!("metadata exited: {status}");
                }
                if let Ok(client) = agentenv_metadata::transport::connect(addr).await {
                    break Ok::<_, anyhow::Error>(client);
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await?
    }

    let mut daemon = spawn_daemon()?;
    let client = connect(&mut daemon, &addr).await?;
    let mut record = SnapshotRecord::mock_ready(SnapshotManifest::default());
    record.config.source = SnapshotSource::Sandbox {
        source_sandbox_id: "source".into(),
        source_sandbox_metadata: None,
    };
    let id = record.id.clone();
    client.register_snapshot(record, Vec::new()).await?;
    let pin = uuid::Uuid::now_v7();
    assert!(
        client
            .pin_resource(
                ResourceKind::Snapshot,
                id.to_string(),
                pin,
                Some("node-a".into()),
            )
            .await?
    );
    client.retire_snapshot(id.clone()).await?;
    let files = |store: &Path| store.join("repository/snapshots").join(id.to_string());
    for store in [&server_store, &client_store] {
        std::fs::create_dir_all(files(store))?;
        std::fs::write(files(store).join("vm_state.bin"), b"retained")?;
    }
    let gc = || -> Result<Value> {
        let output = Command::new(env!("CARGO_BIN_EXE_aenv-snapshot"))
            .arg("--config")
            .arg(&client_cfg)
            .args(["gc", "--limit", "1"])
            .env("AENV_HOME_PATH", root.path())
            .output()?;
        anyhow::ensure!(
            output.status.success(),
            "GC failed: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    };
    assert_eq!(gc()?["reaped_snapshots"], 0);
    assert!(files(&server_store).is_dir());
    client
        .release_pin(ResourceKind::Snapshot, id.to_string(), pin)
        .await?;
    assert_eq!(gc()?["reaped_snapshots"], 1);
    assert!(!files(&server_store).exists());
    assert!(files(&client_store).join("vm_state.bin").is_file());

    // Cross-node Distributed publish -> alias restore over TCP metadata service +
    // transparent reconnect after daemon restart.
    let meta_b = connect(&mut daemon, &addr).await?;
    let node_b_local = root.path().join("node-b-local");
    verify_cross_node_alias_restore(
        &server_store,
        &root.path().join("node-a-local"),
        &node_b_local,
        client,
        meta_b.clone(),
        "cross-node-tcp-alias",
    )
    .await?;

    daemon.kill().await?;
    let _ = daemon.wait().await;
    let mut restarted = spawn_daemon()?;
    let _ = connect(&mut restarted, &addr).await?;
    let (repo_b, res_b) = test_posix_storage(&server_store, node_b_local)?;
    let node_b = SnapshotManager::from_parts(meta_b, repo_b, res_b, None);
    let after_restart = node_b
        .get("cross-node-tcp-alias")
        .await?
        .expect("existing client must reconnect after metadata restart");
    assert!(node_b
        .resolve_runnable(after_restart)
        .await?
        .manifest
        .runtime_manifest()
        .vm_state
        .path
        .is_file());
    restarted.kill().await?;

    // Shared PostgreSQL cross-node alias restore when AENV_TEST_DATABASE_URL is configured.
    if let Ok(url) =
        std::env::var("AENV_TEST_DATABASE_URL").or_else(|_| std::env::var("AENV_TEST_POSTGRES_URL"))
    {
        let db = DatabaseConfig::Postgres { url };
        db.migrate().await?;
        let server_a = MetaServer::open(&db).await?;
        let server_b = MetaServer::open(&db).await?;
        let alias = format!("cross-node-pg-{}", uuid::Uuid::now_v7());
        verify_cross_node_alias_restore(
            &root.path().join("pg-shared-store"),
            &root.path().join("pg-node-a"),
            &root.path().join("pg-node-b"),
            transport::embedded(server_a.clone()),
            transport::embedded(server_b.clone()),
            &alias,
        )
        .await?;
        server_a.close().await;
        server_b.close().await;
    }
    Ok(())
}
