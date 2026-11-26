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
    Ok(())
}

#[tokio::test]
async fn remote_gc_uses_the_service_repository_and_respects_pins() -> Result<()> {
    use agentenv_domain::{ResourceKind, SnapshotManifest, SnapshotRecord, SnapshotSource};
    use agentenv_metadata::{transport, DatabaseConfig};
    use std::time::Duration;

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
    let mut daemon = tokio::process::Command::new(env!("CARGO_BIN_EXE_aenv-metadata"))
        .arg("--backend")
        .arg("sqlite")
        .arg("--sqlite-path")
        .arg(database)
        .args(["serve", "--listen"])
        .arg(address.to_string())
        .arg("--config")
        .arg(server_cfg)
        .env("AENV_HOME_PATH", root.path())
        .kill_on_drop(true)
        .spawn()?;
    let addr = address.to_string();
    let client = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = daemon.try_wait()? {
                anyhow::bail!("metadata exited: {status}");
            }
            if let Ok(client) = transport::connect(&addr).await {
                break Ok::<_, anyhow::Error>(client);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
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
    daemon.kill().await?;
    Ok(())
}
