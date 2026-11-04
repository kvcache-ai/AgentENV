use std::path::Path;

use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{ConnectOptions, Connection, SqliteConnection, SqlitePool};

use super::migrations::{self, MigrationStatus};

/// Open a persistent SQLite connection pool, creating its directory and file if needed.
/// Schema changes remain the migration runner's responsibility.
pub async fn open_pool(path: &Path) -> Result<SqlitePool> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("create SQLite database directory")?;
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal);
    SqlitePool::connect_with(options)
        .await
        .context("open SQLite database")
}

/// Open one read-only connection to an existing database, without changing its journal mode.
pub async fn open_read_only(path: &Path) -> Result<SqliteConnection> {
    SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .connect()
        .await
        .context("open SQLite database read-only")
}

/// Inspect an existing database read-only; an absent file remains absent.
pub async fn migration_status(path: &Path) -> Result<MigrationStatus> {
    if !path.try_exists().context("inspect SQLite database file")? {
        return Ok(MigrationStatus::uninitialized(
            &migrations::sqlite::MIGRATIONS,
        ));
    }
    let mut connection = open_read_only(path).await?;
    let result = migrations::sqlite::inspect(&mut connection).await;
    let closed = connection.close().await;
    let status = result?;
    closed.context("close SQLite status connection")?;
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Acquire;

    #[tokio::test]
    async fn creates_persistent_database_with_concurrent_reader_and_writer() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("persistent/metadata.sqlite3");
        let pool = open_pool(&path).await?;
        assert!(path.is_file());
        sqlx::raw_sql("CREATE TABLE counter (value INTEGER); INSERT INTO counter VALUES (1);")
            .execute(&pool)
            .await?;
        let mut reader = pool.acquire().await?;
        let mut writer = pool.acquire().await?;
        for connection in [&mut reader, &mut writer] {
            let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
                .fetch_one(&mut **connection)
                .await?;
            assert_eq!(foreign_keys, 1);
        }
        let mut snapshot = reader.begin().await?;
        let before: i64 = sqlx::query_scalar("SELECT value FROM counter")
            .fetch_one(&mut *snapshot)
            .await?;
        assert_eq!(before, 1);
        sqlx::query("UPDATE counter SET value = 2")
            .execute(&mut *writer)
            .await?;
        let retained: i64 = sqlx::query_scalar("SELECT value FROM counter")
            .fetch_one(&mut *snapshot)
            .await?;
        assert_eq!(retained, 1);
        snapshot.commit().await?;
        drop((reader, writer));
        pool.close().await;

        let reopened = open_pool(&path).await?;
        let current: i64 = sqlx::query_scalar("SELECT value FROM counter")
            .fetch_one(&reopened)
            .await?;
        assert_eq!(current, 2);
        reopened.close().await;
        Ok(())
    }
}
