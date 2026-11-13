use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use confique::Config;
use serde::Deserialize;

use super::{impl_config_default, parse_required_path, parse_trimmed_string, resolve_path};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseBackend {
    Sqlite,
    Postgres,
}

#[derive(Clone, Config)]
pub struct DatabaseConfig {
    /// Optional TCP address of a standalone `aenv-metadata serve` instance.
    /// When set, the process connects over TCP and ignores local database settings.
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
            return Ok(());
        }
        if self.backend == DatabaseBackend::Sqlite {
            ensure!(
                !self.sqlite_path.as_os_str().is_empty(),
                "database.sqlite_path must not be empty"
            );
            self.sqlite_path = resolve_path(home, config_dir, &self.sqlite_path);
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<()> {
        if let Some(address) = self.meta_address.as_deref() {
            ensure!(
                !address.trim().is_empty(),
                "database.meta_address must not be empty"
            );
            return Ok(());
        }
        if self.backend == DatabaseBackend::Postgres {
            let url = self.postgres_url.as_deref().unwrap_or_default();
            ensure!(
                !url.trim().is_empty(),
                "database.postgres_url is required when database.backend = postgres"
            );
            ensure!(
                url::Url::parse(url)
                    .is_ok_and(|url| matches!(url.scheme(), "postgres" | "postgresql")),
                "database.postgres_url must be a PostgreSQL connection URL"
            );
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
                url: self.postgres_url.clone().context(
                    "database.postgres_url is required when database.backend = postgres",
                )?,
            }),
        }
    }
}
