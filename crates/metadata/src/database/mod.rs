//! Database connections and migrations, independent of snapshot business models.

pub mod migrations;
pub mod postgres;
pub mod sqlite;

use anyhow::{Context, Result};
use sqlx::{PgPool, SqlitePool};

use self::migrations::MigrationStatus;

/// Connection settings shared by embedded startup and metadata administration.
#[derive(Clone)]
pub enum DatabaseConfig {
    Sqlite { path: std::path::PathBuf },
    Postgres { url: String },
}

impl DatabaseConfig {
    pub(crate) async fn open(&self) -> Result<DatabasePool> {
        match self {
            Self::Sqlite { path } => DatabasePool::open_sqlite(path).await,
            Self::Postgres { url } => DatabasePool::open_postgres(url).await,
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
            Self::Sqlite { path } => sqlite::migration_status(path).await,
            Self::Postgres { .. } => {
                let database = self.open().await?;
                let result = database.migration_status().await;
                database.close().await;
                result
            }
        }
    }
}

/// Database connections owned by the metadata process. Consumers clone the native
/// pool handles; the owner closes them after its consumers finish.
#[derive(Clone)]
pub enum DatabasePool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl DatabasePool {
    pub async fn open_postgres(url: &str) -> Result<Self> {
        Ok(Self::Postgres(postgres::open_pool(url).await?))
    }

    pub async fn open_sqlite(path: &std::path::Path) -> Result<Self> {
        Ok(Self::Sqlite(sqlite::open_pool(path).await?))
    }

    pub async fn migrate(&self) -> Result<()> {
        match self {
            Self::Postgres(pool) => migrations::postgres::apply(pool).await,
            Self::Sqlite(pool) => migrations::sqlite::apply(pool).await,
        }
    }

    pub async fn close(&self) {
        match self {
            Self::Postgres(pool) => pool.close().await,
            Self::Sqlite(pool) => pool.close().await,
        }
    }

    pub async fn migration_status(&self) -> Result<MigrationStatus> {
        match self {
            Self::Postgres(pool) => {
                let mut connection = pool
                    .acquire()
                    .await
                    .context("acquire PostgreSQL connection for migration check")?;
                migrations::postgres::inspect(&mut connection).await
            }
            Self::Sqlite(pool) => {
                let mut connection = pool
                    .acquire()
                    .await
                    .context("acquire SQLite connection for migration check")?;
                migrations::sqlite::inspect(&mut connection).await
            }
        }
        .context("check database migration history")
    }
}
