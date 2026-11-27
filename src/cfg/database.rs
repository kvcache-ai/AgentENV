use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Result};
use confique::Config;
use serde::Deserialize;

use super::{impl_config_default, parse_required_path, parse_trimmed_string, resolve_path};

const DEFAULT_SQLITE_PATH: &str = "$AENV_HOME/metadata.sqlite3";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseBackend {
    Sqlite,
    Postgres,
}

/// Validates and trims a PostgreSQL connection URL (`postgres://` or `postgresql://`).
pub fn validate_postgres_url(url: Option<&str>, missing_msg: &str) -> Result<String> {
    let url = url.unwrap_or_default().trim();
    ensure!(!url.is_empty(), "{missing_msg}");
    ensure!(
        url::Url::parse(url)
            .is_ok_and(|parsed| matches!(parsed.scheme(), "postgres" | "postgresql")),
        "database.postgres_url must be a PostgreSQL connection URL"
    );
    Ok(url.to_owned())
}

#[derive(Clone, Config)]
pub struct DatabaseConfig {
    /// Optional TCP address of a standalone `aenv-metadata serve` instance.
    /// Mutually exclusive with PostgreSQL and custom SQLite settings.
    #[config(env = "AENV_META_ADDRESS", parse_env = parse_trimmed_string)]
    pub meta_address: Option<String>,
    #[config(default = "sqlite", env = "AENV_DATABASE_BACKEND")]
    pub backend: DatabaseBackend,
    #[config(
        default = "$AENV_HOME/metadata.sqlite3",
        env = "AENV_DATABASE_SQLITE_PATH",
        parse_env = parse_required_path
    )]
    pub sqlite_path: PathBuf,
    #[config(env = "AENV_DATABASE_POSTGRES_URL")]
    pub postgres_url: Option<String>,
}

impl_config_default!(DatabaseConfig);

impl fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("meta_address", &self.meta_address)
            .field("backend", &self.backend)
            .field("sqlite_path", &self.sqlite_path)
            .field(
                "postgres_url",
                &self.postgres_url.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl DatabaseConfig {
    pub(super) fn normalize(&mut self, home: &Path, config_dir: &Path) -> Result<()> {
        if let Some(address) = self.meta_address.as_mut() {
            *address = address.trim().to_string();
        }
        if let Some(url) = self.postgres_url.as_mut() {
            *url = url.trim().to_string();
        }
        if self.meta_address.is_none() && self.backend == DatabaseBackend::Sqlite {
            ensure!(
                !self.sqlite_path.as_os_str().is_empty(),
                "database.sqlite_path must not be empty"
            );
            self.sqlite_path = resolve_path(home, config_dir, &self.sqlite_path);
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<()> {
        let custom_sqlite = self.sqlite_path != Path::new(DEFAULT_SQLITE_PATH);
        if let Some(address) = self.meta_address.as_deref() {
            ensure!(
                !address.trim().is_empty(),
                "database.meta_address must not be empty"
            );
            ensure!(
                self.postgres_url.is_none(),
                "database.meta_address cannot be combined with database.postgres_url"
            );
            ensure!(
                self.backend != DatabaseBackend::Postgres,
                "database.meta_address cannot be combined with database.backend = postgres"
            );
            ensure!(
                !custom_sqlite,
                "database.meta_address cannot be combined with database.sqlite_path"
            );
            return Ok(());
        }
        match self.backend {
            DatabaseBackend::Sqlite => {
                ensure!(
                    self.postgres_url.is_none(),
                    "database.postgres_url cannot be set when database.backend = sqlite"
                );
                ensure!(
                    !self.sqlite_path.as_os_str().is_empty(),
                    "database.sqlite_path must not be empty"
                );
            }
            DatabaseBackend::Postgres => {
                ensure!(
                    !custom_sqlite,
                    "database.sqlite_path cannot be set when database.backend = postgres"
                );
                validate_postgres_url(
                    self.postgres_url.as_deref(),
                    "database.postgres_url is required when database.backend = postgres",
                )?;
            }
        }
        Ok(())
    }

    pub fn embedded_sqlite_path(&self) -> Option<&Path> {
        (self.meta_address.is_none() && self.backend == DatabaseBackend::Sqlite)
            .then_some(self.sqlite_path.as_path())
    }

    pub fn to_metadata_config(&self) -> Result<agentenv_metadata::DatabaseConfig> {
        match self.backend {
            DatabaseBackend::Sqlite => Ok(agentenv_metadata::DatabaseConfig::Sqlite {
                path: self.sqlite_path.clone(),
            }),
            DatabaseBackend::Postgres => Ok(agentenv_metadata::DatabaseConfig::Postgres {
                url: validate_postgres_url(
                    self.postgres_url.as_deref(),
                    "database.postgres_url is required when database.backend = postgres",
                )?,
            }),
        }
    }
}
