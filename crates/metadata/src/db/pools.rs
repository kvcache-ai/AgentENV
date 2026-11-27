//! PostgreSQL and SQLite connection pool builders.

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use sqlx::postgres::PgConnectOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{ConnectOptions, Connection, PgPool, SqlitePool};

use super::migrations::{self, MigrationStatus};

/// Open a PostgreSQL connection pool without changing the database schema.
pub(super) async fn open_postgres(url: &str) -> Result<PgPool> {
    // The parser logs unknown URL parameters before returning; their values may be secrets.
    let options = tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        url.parse::<PgConnectOptions>()
    })
    .map_err(|_| anyhow!("invalid database.postgres_url"))?;
    PgPool::connect_with(options)
        .await
        .context("connect PostgreSQL database")
}

/// Open a persistent SQLite connection pool, creating its directory and file if needed.
pub(super) async fn open_sqlite(path: &Path) -> Result<SqlitePool> {
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

/// Inspect an existing SQLite database read-only; an absent file remains absent.
pub(super) async fn sqlite_migration_status(path: &Path) -> Result<MigrationStatus> {
    if !path.try_exists().context("inspect SQLite database file")? {
        return Ok(MigrationStatus::uninitialized(
            &migrations::SQLITE_MIGRATIONS,
        ));
    }
    let mut connection = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .connect()
        .await
        .context("open SQLite database read-only")?;
    let result = migrations::inspect_sqlite(&mut connection).await;
    let closed = connection.close().await;
    let status = result?;
    closed.context("close SQLite status connection")?;
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Acquire;

    #[test]
    fn connection_parameters_do_not_expose_credentials() {
        let output = tempfile::NamedTempFile::new().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(std::sync::Mutex::new(output.reopen().unwrap()))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let error = futures::executor::block_on(open_postgres(
                "postgresql://localhost/db?passwrod=private-password&sslmode=private-password",
            ))
            .unwrap_err();
            assert_eq!(error.to_string(), "invalid database.postgres_url");
            tracing::warn!("parser returned");
        });
        let log = std::fs::read_to_string(output.path()).unwrap();
        assert!(log.contains("parser returned"));
        assert!(!log.contains("private-password"));
    }

    #[tokio::test]
    async fn creates_persistent_database_with_concurrent_reader_and_writer() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("persistent/metadata.sqlite3");
        let pool = open_sqlite(&path).await?;
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

        let reopened = open_sqlite(&path).await?;
        let current: i64 = sqlx::query_scalar("SELECT value FROM counter")
            .fetch_one(&reopened)
            .await?;
        assert_eq!(current, 2);
        reopened.close().await;
        Ok(())
    }
}
