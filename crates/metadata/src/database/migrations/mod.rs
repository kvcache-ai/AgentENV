//! Embedded migrations and read-only history inspection.

use std::collections::BTreeMap;

use anyhow::{ensure, Result};
use serde::Serialize;
use sqlx::migrate::{MigrateError, Migrator};

pub mod postgres;
pub mod sqlite;

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
                .filter(|migration| !migration.migration_type.is_down_migration())
                .map(|migration| migration.version)
                .collect(),
        }
    }
}

const READ_HISTORY_SQL: &str =
    "SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version";

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
        .filter(|migration| !migration.migration_type.is_down_migration())
        .map(|migration| (migration.version, migration))
        .collect();
    let mut applied = Vec::with_capacity(history.len());
    for (version, _, checksum) in history {
        match pending.remove(&version) {
            Some(expected) if checksum.as_slice() != expected.checksum.as_ref() => {
                return Err(MigrateError::VersionMismatch(version).into());
            }
            None => {
                return Err(MigrateError::VersionMissing(version).into());
            }
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
