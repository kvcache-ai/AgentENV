use anyhow::{Context, Result};
use sqlx::migrate::Migrator;
use sqlx::{SqliteConnection, SqlitePool};

use super::{MigrationStatus, READ_HISTORY_SQL};

pub static MIGRATIONS: Migrator = sqlx::migrate!("./migrations/sqlite");

/// Apply SQLite migrations; run only one migrator per database at a time.
pub async fn apply(pool: &SqlitePool) -> Result<()> {
    let mut connection = pool.acquire().await?;
    connection.close_on_drop();
    let result = MIGRATIONS.run_direct(None, &mut *connection, false).await;
    let closed = connection.close().await;
    result.context("migrate SQLite database")?;
    closed.context("close SQLite migration connection")?;
    Ok(())
}

/// Read and validate migration history without creating it or applying migrations.
pub async fn inspect(connection: &mut SqliteConnection) -> Result<MigrationStatus> {
    let initialized: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations')")
        .fetch_one(&mut *connection).await?;
    if !initialized {
        return Ok(MigrationStatus::uninitialized(&MIGRATIONS));
    }
    let history = sqlx::query_as(READ_HISTORY_SQL)
        .fetch_all(connection)
        .await?;
    super::validate_history(history, &MIGRATIONS)
}
