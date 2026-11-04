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
    assert_eq!(initial["pending"], json!([]));
    assert!(!path.parent().unwrap().exists());
    let check = database_command(&path, "check")?;
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );
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
    assert_eq!(ready["applied"], json!([]));
    Ok(())
}
