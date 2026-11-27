//! Embedded forward migrations and read-only history inspection for PostgreSQL and SQLite.

use std::collections::BTreeMap;

use anyhow::{ensure, Context, Result};
use serde::Serialize;
use sqlx::migrate::{Migrate, MigrateError, Migrator};
use sqlx::{Database, PgConnection, Pool, SqliteConnection};

pub(super) static PG_MIGRATIONS: Migrator = sqlx::migrate!("./migrations/postgres");
pub(super) static SQLITE_MIGRATIONS: Migrator = sqlx::migrate!("./migrations/sqlite");

const READ_HISTORY_SQL: &str =
    "SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version";

#[derive(Debug, Serialize)]
pub struct MigrationStatus {
    /// Whether SQLx migration history exists; this does not imply business schema readiness.
    pub initialized: bool,
    pub applied: Vec<i64>,
    pub pending: Vec<i64>,
}

impl MigrationStatus {
    /// An empty migration bundle does not require a history table.
    pub fn ensure_ready(&self) -> Result<()> {
        ensure!(
            self.pending.is_empty(),
            "metadata database has pending migrations {:?}; run aenv-metadata db migrate before starting its consumers",
            self.pending
        );
        Ok(())
    }

    pub(super) fn uninitialized(migrations: &Migrator) -> Self {
        Self {
            initialized: false,
            applied: Vec::new(),
            pending: migrations
                .iter()
                .filter(|m| !m.migration_type.is_down_migration())
                .map(|m| m.version)
                .collect(),
        }
    }
}

/// Apply bundled forward migrations on a dedicated connection closed immediately afterwards
/// (so a failed or cancelled PostgreSQL migration cannot return an advisory lock to the pool).
pub(super) async fn apply<D>(pool: &Pool<D>, migrations: &Migrator, label: &str) -> Result<()>
where
    D: Database,
    D::Connection: Migrate,
{
    let mut connection = pool.acquire().await?;
    connection.close_on_drop();
    let result = migrations.run_direct(None, &mut *connection, false).await;
    let closed = connection.close().await;
    result.with_context(|| format!("migrate {label} database"))?;
    closed.with_context(|| format!("close {label} migration connection"))?;
    Ok(())
}

/// Read and validate PostgreSQL migration history without mutating the database.
pub(super) async fn inspect_postgres(connection: &mut PgConnection) -> Result<MigrationStatus> {
    let initialized: bool =
        sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
            .fetch_one(&mut *connection)
            .await?;
    if !initialized {
        return Ok(MigrationStatus::uninitialized(&PG_MIGRATIONS));
    }
    let history = sqlx::query_as(READ_HISTORY_SQL)
        .fetch_all(connection)
        .await?;
    validate_history(history, &PG_MIGRATIONS)
}

/// Read and validate SQLite migration history without mutating the database.
pub(super) async fn inspect_sqlite(connection: &mut SqliteConnection) -> Result<MigrationStatus> {
    let initialized: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations')",
    )
    .fetch_one(&mut *connection)
    .await?;
    if !initialized {
        return Ok(MigrationStatus::uninitialized(&SQLITE_MIGRATIONS));
    }
    let history = sqlx::query_as(READ_HISTORY_SQL)
        .fetch_all(connection)
        .await?;
    validate_history(history, &SQLITE_MIGRATIONS)
}

fn validate_history(
    history: Vec<(i64, bool, Vec<u8>)>,
    migrations: &Migrator,
) -> Result<MigrationStatus> {
    // History is ordered by version. Any failed migration takes precedence over version checks.
    if let Some((version, _, _)) = history.iter().find(|(_, success, _)| !success) {
        return Err(MigrateError::Dirty(*version).into());
    }
    let mut pending: BTreeMap<_, _> = migrations
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .map(|m| (m.version, m))
        .collect();
    let mut applied = Vec::with_capacity(history.len());
    for (version, _, checksum) in history {
        match pending.remove(&version) {
            Some(expected) if checksum.as_slice() != expected.checksum.as_ref() => {
                return Err(MigrateError::VersionMismatch(version).into());
            }
            None => return Err(MigrateError::VersionMissing(version).into()),
            _ => {}
        }
        applied.push(version);
    }
    Ok(MigrationStatus {
        initialized: true,
        applied,
        pending: pending.into_keys().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::migrate::{Migration, MigrationType};
    use sqlx::SqlSafeStr;

    #[test]
    fn history_requires_matching_successful_migrations() -> Result<()> {
        let migration = Migration::new(
            1,
            "first".into(),
            MigrationType::Simple,
            "SELECT 1;".into_sql_str(),
            false,
        );
        let checksum = migration.checksum.to_vec();
        let migrations = Migrator::with_migrations(vec![migration]);
        let missing = validate_history(vec![], &migrations)?;
        assert_eq!(missing.pending, [1]);
        assert!(missing.ensure_ready().is_err());
        let applied = validate_history(vec![(1, true, checksum.clone())], &migrations)?;
        assert_eq!(applied.applied, [1]);
        applied.ensure_ready()?;
        for version in [0, 2] {
            assert!(matches!(
                validate_history(vec![(version, true, checksum.clone())], &migrations)
                    .unwrap_err()
                    .downcast::<MigrateError>()?,
                MigrateError::VersionMissing(actual) if actual == version
            ));
        }
        assert!(matches!(
            validate_history(vec![(1, true, vec![])], &migrations)
                .unwrap_err()
                .downcast::<MigrateError>()?,
            MigrateError::VersionMismatch(1)
        ));
        assert!(matches!(
            validate_history(vec![(1, false, checksum)], &migrations)
                .unwrap_err()
                .downcast::<MigrateError>()?,
            MigrateError::Dirty(1)
        ));
        Ok(())
    }
}
