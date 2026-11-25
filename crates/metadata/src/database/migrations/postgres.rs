use anyhow::{Context, Result};
use sqlx::migrate::Migrator;
use sqlx::{PgConnection, PgPool};

use super::{MigrationStatus, READ_HISTORY_SQL};

pub static MIGRATIONS: Migrator = sqlx::migrate!("./migrations/postgres");

/// Apply versioned migrations using SQLx's history, checksums and advisory lock.
pub async fn apply(pool: &PgPool) -> Result<()> {
    // A failed or cancelled migration must not return a session advisory lock to the pool.
    let mut connection = pool.acquire().await?;
    connection.close_on_drop();
    let result = MIGRATIONS.run_direct(None, &mut *connection, false).await;
    let closed = connection.close().await;
    result.context("migrate PostgreSQL database")?;
    closed.context("close PostgreSQL migration connection")?;
    Ok(())
}

/// Read and validate migration history without creating it or applying migrations.
pub async fn inspect(connection: &mut PgConnection) -> Result<MigrationStatus> {
    let initialized: bool =
        sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
            .fetch_one(&mut *connection)
            .await?;
    if !initialized {
        return Ok(MigrationStatus::uninitialized(&MIGRATIONS));
    }
    let history = sqlx::query_as(READ_HISTORY_SQL)
        .fetch_all(connection)
        .await?;
    super::validate_history(history, &MIGRATIONS)
}
