use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};

use agentenv_domain::SnapshotId;
use agentenv_metadata::DatabaseConfig;
use anyhow::{ensure, Context, Result};
use sqlx::{AnyConnection, Connection};
use url::Url;
use uuid::Uuid;

use crate::legacy::Relations;

pub struct Shadow {
    connection: AnyConnection,
    _sqlite_lock: Option<File>,
    postgres: bool,
    owned: BTreeSet<SnapshotId>,
    sealed: bool,
}

impl Shadow {
    pub async fn open(config: &DatabaseConfig, source: &str, initialize: bool) -> Result<Self> {
        sqlx::any::install_default_drivers();
        let (url, sqlite_lock) = match config {
            DatabaseConfig::Postgres { url } => (url.clone(), None),
            DatabaseConfig::Sqlite { path } => {
                let path = path
                    .canonicalize()
                    .context("resolve SQLite shadow database path")?;
                let mut lock_path = path.as_os_str().to_os_string();
                lock_path.push(".snapshot-catalog-import.lock");
                let lock = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(lock_path)
                    .context("open SQLite shadow lock")?;
                lock.try_lock().context("lock SQLite shadow database")?;
                let url = Url::from_file_path(path).map_err(|_| {
                    anyhow::anyhow!("SQLite shadow database path is not a file URL")
                })?;
                (
                    format!("sqlite{}?mode=rw", &url.as_str()["file".len()..]),
                    Some(lock),
                )
            }
        };
        let postgres = sqlite_lock.is_none();
        let mut connection = AnyConnection::connect(&url)
            .await
            .context("connect shadow database")?;
        let marker_exists = if postgres {
            ensure!(
                sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock(5067197512643076174)")
                    .fetch_one(&mut connection)
                    .await?,
                "another snapshot importer is using this PostgreSQL database"
            );
            "SELECT COUNT(*) FROM information_schema.tables
             WHERE table_schema = current_schema() AND table_name = 'snapshot_catalog_import'"
        } else {
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'snapshot_catalog_import'"
        };
        let mut tx = connection.begin().await?;
        if sqlx::query_scalar::<_, i64>(marker_exists)
            .fetch_one(&mut *tx)
            .await?
            == 0
        {
            ensure!(
                initialize,
                "target has no shadow marker. Run stage on an empty database first"
            );
            ensure!(
                sqlx::query_scalar::<_, i64>(
                    "SELECT (SELECT COUNT(*) FROM snapshots) + (SELECT COUNT(*) FROM layers) +
                            (SELECT COUNT(*) FROM snapshot_heads) + (SELECT COUNT(*) FROM snapshot_pins)"
                ).fetch_one(&mut *tx).await? == 0,
                "stage requires an empty shadow database on the first run"
            );
            sqlx::query(
                "CREATE TABLE snapshot_catalog_import (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                source TEXT NOT NULL,
                owned TEXT NOT NULL,
                sealed BOOLEAN NOT NULL
            )",
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query("INSERT INTO snapshot_catalog_import VALUES (1, $1, '[]', FALSE)")
                .bind(source)
                .execute(&mut *tx)
                .await?;
        }
        let (saved_source, owned, sealed) = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT source, owned, CASE WHEN sealed THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END
             FROM snapshot_catalog_import WHERE singleton = 1",
        ).fetch_one(&mut *tx).await?;
        ensure!(
            saved_source == source,
            "shadow database is bound to another legacy repository"
        );
        tx.commit().await?;
        Ok(Self {
            connection,
            _sqlite_lock: sqlite_lock,
            postgres,
            owned: serde_json::from_str(&owned).context("decode shadow ownership")?,
            sealed: sealed != 0,
        })
    }

    pub fn ensure_writable(&self) -> Result<()> {
        ensure!(!self.sealed, "shadow database is sealed");
        Ok(())
    }

    pub async fn check_state(&mut self) -> Result<()> {
        check_shadow_state(&mut self.connection).await
    }

    pub fn ensure_owned(&self, id: &SnapshotId) -> Result<()> {
        ensure!(
            self.sealed || self.owned.contains(id),
            "target contains snapshot {id} written outside this importer"
        );
        Ok(())
    }

    pub async fn reserve_delta(
        &mut self,
        remove: &BTreeSet<SnapshotId>,
        final_ids: BTreeSet<SnapshotId>,
    ) -> Result<()> {
        if remove.is_empty() && self.owned == final_ids {
            return Ok(());
        }
        let (remove_heads, remove_snapshot) = if self.postgres {
            (
                "DELETE FROM snapshot_heads WHERE snapshot_id = CAST($1 AS UUID)",
                "DELETE FROM snapshots WHERE snapshot_id = CAST($1 AS UUID) AND NOT retired",
            )
        } else {
            (
                "DELETE FROM snapshot_heads WHERE snapshot_id = $1",
                "DELETE FROM snapshots WHERE snapshot_id = $1 AND NOT retired",
            )
        };
        let mut tx = self.connection.begin().await?;
        check_shadow_state(&mut tx).await?;
        for id in remove {
            ensure!(
                self.owned.contains(id),
                "cannot remove unowned snapshot {id}"
            );
            sqlx::query(remove_heads)
                .bind(id.to_string())
                .execute(&mut *tx)
                .await?;
            let result = sqlx::query(remove_snapshot)
                .bind(id.to_string())
                .execute(&mut *tx)
                .await?;
            ensure!(
                result.rows_affected() == 1,
                "staged snapshot {id} changed during reconciliation"
            );
        }
        if !remove.is_empty() {
            // Only the disposable shadow loses these rows. Artifacts remain in OSS.
            sqlx::query("WITH RECURSIVE reachable(uuid) AS (
                    SELECT layer_uuid FROM snapshot_heads
                    UNION
                    SELECT layers.managed_parent_uuid FROM layers
                    JOIN reachable ON layers.uuid = reachable.uuid
                    WHERE layers.managed_parent_uuid IS NOT NULL
                )
                DELETE FROM layers WHERE NOT EXISTS (SELECT 1 FROM reachable WHERE reachable.uuid = layers.uuid)")
                .execute(&mut *tx).await?;
        }
        // Reserve ownership before registering. A retry can fill any missing
        // registrations if this transaction committed and the process stopped.
        sqlx::query("UPDATE snapshot_catalog_import SET owned = $1 WHERE singleton = 1")
            .bind(serde_json::to_string(&final_ids)?)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.owned = final_ids;
        Ok(())
    }

    pub async fn verify_relations(&mut self, expected: &Relations) -> Result<()> {
        let layers = sqlx::query_as::<_, (String, Option<String>, String)>(
            "SELECT CAST(uuid AS TEXT), CAST(managed_parent_uuid AS TEXT), digest FROM layers",
        )
        .fetch_all(&mut self.connection)
        .await?;
        let layers = layers
            .into_iter()
            .map(|(uuid, parent, digest)| {
                Ok((
                    Uuid::parse_str(&uuid)?,
                    (parent.as_deref().map(Uuid::parse_str).transpose()?, digest),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        ensure!(
            layers == expected.layers,
            "shadow managed-layer graph differs from the legacy catalog"
        );

        let heads = sqlx::query_as::<_, (String, String)>(
            "SELECT CAST(snapshot_id AS TEXT), CAST(layer_uuid AS TEXT) FROM snapshot_heads",
        )
        .fetch_all(&mut self.connection)
        .await?;
        let heads = heads
            .into_iter()
            .map(|(id, uuid)| Ok((SnapshotId::parse(&id)?, Uuid::parse_str(&uuid)?)))
            .collect::<Result<BTreeSet<_>>>()?;
        ensure!(
            heads == expected.heads,
            "shadow snapshot heads differ from the legacy catalog"
        );
        Ok(())
    }

    pub async fn seal(&mut self) -> Result<()> {
        if !self.sealed {
            sqlx::query("UPDATE snapshot_catalog_import SET owned = '[]', sealed = TRUE WHERE singleton = 1")
                .execute(&mut self.connection).await?;
            self.sealed = true;
        }
        Ok(())
    }
}

async fn check_shadow_state(connection: &mut AnyConnection) -> Result<()> {
    ensure!(sqlx::query_scalar::<_, i64>(
        "SELECT (SELECT COUNT(*) FROM snapshots WHERE retired OR snapshot_type <> 'distributed') +
                (SELECT COUNT(*) FROM layers WHERE state <> 'available') +
                (SELECT COUNT(*) FROM snapshot_pins)"
    ).fetch_one(connection).await? == 0,
        "target has retirement, pin or GC state. Use an isolated shadow database");
    Ok(())
}
