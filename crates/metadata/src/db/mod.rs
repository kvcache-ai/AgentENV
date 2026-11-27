//! Database connections, dialect-neutral SQL execution, and migrations.

pub mod migrations;
mod pools;
mod sql;

use anyhow::{Context, Result};
use sqlx::{PgPool, SqlitePool};

use self::migrations::MigrationStatus;
pub(crate) use self::sql::{
    is_foreign_key_violation, is_unique_violation, DbConn, DbRow, DbTx, MetaSqlExt, Sql,
};

/// Connection settings shared by embedded startup and metadata administration.
#[derive(Clone)]
pub enum DatabaseConfig {
    Sqlite { path: std::path::PathBuf },
    Postgres { url: String },
}

impl DatabaseConfig {
    pub(crate) async fn open(&self) -> Result<DatabasePool> {
        match self {
            Self::Sqlite { path } => Ok(DatabasePool::Sqlite(pools::open_sqlite(path).await?)),
            Self::Postgres { url } => Ok(DatabasePool::Postgres(pools::open_postgres(url).await?)),
        }
    }

    pub async fn migrate(&self) -> Result<()> {
        let database = self.open().await?;
        let result = database.migrate().await;
        database.close().await;
        result
    }

    pub async fn status(&self) -> Result<MigrationStatus> {
        match self {
            Self::Sqlite { path } => pools::sqlite_migration_status(path).await,
            Self::Postgres { .. } => {
                let database = self.open().await?;
                let result = database.migration_status().await;
                database.close().await;
                result
            }
        }
    }
}

/// Database connections owned by the metadata process.
#[derive(Clone)]
pub(crate) enum DatabasePool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl DatabasePool {
    pub(crate) async fn migrate(&self) -> Result<()> {
        match self {
            Self::Postgres(p) => {
                migrations::apply(p, &migrations::PG_MIGRATIONS, "PostgreSQL").await
            }
            Self::Sqlite(p) => migrations::apply(p, &migrations::SQLITE_MIGRATIONS, "SQLite").await,
        }
    }

    pub(crate) async fn close(&self) {
        match self {
            Self::Postgres(pool) => pool.close().await,
            Self::Sqlite(pool) => pool.close().await,
        }
    }

    pub(crate) async fn migration_status(&self) -> Result<MigrationStatus> {
        match self {
            Self::Postgres(pool) => {
                let mut conn = pool
                    .acquire()
                    .await
                    .context("acquire PostgreSQL connection for migration check")?;
                migrations::inspect_postgres(&mut conn).await
            }
            Self::Sqlite(pool) => {
                let mut conn = pool
                    .acquire()
                    .await
                    .context("acquire SQLite connection for migration check")?;
                migrations::inspect_sqlite(&mut conn).await
            }
        }
        .context("check database migration history")
    }
}
