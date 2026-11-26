//! Database connections, SQL dialect abstraction, and migrations.

pub mod migrations;
pub mod postgres;
pub mod sqlite;

use anyhow::{Context, Result};
use sqlx::{AssertSqlSafe, Database, PgPool, Pool, Postgres, Sqlite, SqlitePool, Transaction};

use self::migrations::MigrationStatus;
use crate::{MetaError, MetaResult};

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

/// Uniform `rows_affected` extraction across PostgreSQL and SQLite query results.
pub(crate) trait QueryRows {
    fn rows_affected(&self) -> u64;
}

impl QueryRows for sqlx::postgres::PgQueryResult {
    fn rows_affected(&self) -> u64 {
        Self::rows_affected(self)
    }
}

impl QueryRows for sqlx::sqlite::SqliteQueryResult {
    fn rows_affected(&self) -> u64 {
        Self::rows_affected(self)
    }
}

/// SQL dialect differences between PostgreSQL and SQLite across all metadata tables.
pub(crate) trait SqlDialect: Database {
    const BEGIN: &'static str;
    const UUID_CAST: &'static str;
    const FOR_UPDATE: &'static str;
    const FOR_SHARE: &'static str;
    const IN_UUID_SET: &'static str;
    const IN_TEXT_SET: &'static str;
    const JSON_UUID_SET_PREFIX: &'static str;
    const JSON_UUID_SET_SUFFIX: &'static str;
    const LOCK_ROWS: bool;

    fn uuid(parameter: usize) -> String {
        format!("${parameter}{}", Self::UUID_CAST)
    }

    fn encode_ids(ids: &[String]) -> Result<String> {
        Ok(serde_json::to_string(ids)?)
    }
}

impl SqlDialect for Postgres {
    const BEGIN: &'static str = "BEGIN ISOLATION LEVEL READ COMMITTED";
    const UUID_CAST: &'static str = "::uuid";
    const FOR_UPDATE: &'static str = " FOR UPDATE";
    const FOR_SHARE: &'static str = " FOR SHARE";
    const IN_UUID_SET: &'static str =
        "= ANY(ARRAY(SELECT value::uuid FROM json_array_elements_text($1::json)))";
    const IN_TEXT_SET: &'static str =
        "= ANY(ARRAY(SELECT value FROM json_array_elements_text($1::json)))";
    const JSON_UUID_SET_PREFIX: &'static str =
        " AND snapshots.snapshot_id IN (SELECT value::uuid FROM json_array_elements_text(";
    const JSON_UUID_SET_SUFFIX: &'static str = "::json))";
    const LOCK_ROWS: bool = true;
}

impl SqlDialect for Sqlite {
    const BEGIN: &'static str = "BEGIN IMMEDIATE";
    const UUID_CAST: &'static str = "";
    const FOR_UPDATE: &'static str = "";
    const FOR_SHARE: &'static str = "";
    const IN_UUID_SET: &'static str = "IN (SELECT value FROM json_each($1))";
    const IN_TEXT_SET: &'static str = "IN (SELECT value FROM json_each($1))";
    const JSON_UUID_SET_PREFIX: &'static str =
        " AND snapshots.snapshot_id IN (SELECT value FROM json_each(";
    const JSON_UUID_SET_SUFFIX: &'static str = "))";
    const LOCK_ROWS: bool = false;
}

/// Starts a transaction with the dialect's standard isolation mode.
pub(crate) async fn begin_tx<'a, D: SqlDialect>(
    pool: &'a Pool<D>,
    context: &str,
) -> MetaResult<Transaction<'a, D>> {
    pool.begin_with(AssertSqlSafe(D::BEGIN.to_owned()))
        .await
        .map_err(|error| MetaError::backend(context, error))
}

/// Commits a transaction, mapping errors into [`MetaError::Backend`].
pub(crate) async fn commit_tx<D: SqlDialect>(
    tx: Transaction<'_, D>,
    context: &str,
) -> MetaResult<()> {
    tx.commit()
        .await
        .map_err(|error| MetaError::backend(context, error))
}

/// Dispatches an expression across PostgreSQL and SQLite pools with a single generic body.
macro_rules! dispatch_db {
    ($database:expr, |$pool:ident| $body:expr) => {
        match $database {
            $crate::database::DatabasePool::Postgres($pool) => $body,
            $crate::database::DatabasePool::Sqlite($pool) => $body,
        }
    };
}
pub(crate) use dispatch_db;
