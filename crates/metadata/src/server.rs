use agentenv_domain::{
    SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotPlacement, SnapshotRecord,
};

use crate::database::{DatabaseConfig, DatabasePool};
use crate::{snapshot_directory, MetaError, MetaResult, SnapshotEntry};

/// Snapshot directory and the database connections it owns.
#[derive(Clone)]
pub struct MetaServer {
    database: DatabasePool,
}

impl MetaServer {
    pub async fn open(config: &DatabaseConfig) -> anyhow::Result<Self> {
        let database = config.open().await?;
        let ready = database
            .migration_status()
            .await
            .and_then(|status| status.ensure_ready());
        if let Err(error) = ready {
            database.close().await;
            return Err(error);
        }
        Ok(Self { database })
    }

    pub async fn close(&self) {
        self.database.close().await;
    }

    /// Register a snapshot and, for Distributed placement, its managed layer relationships.
    pub async fn register_snapshot(&self, record: SnapshotRecord) -> MetaResult<SnapshotEntry> {
        validate_record(&record)?;
        if record.snapshot_type == SnapshotPlacement::Local {
            let committed = record.committed.as_ref().expect("record was validated");
            if !committed.rootfs_layers.is_empty()
                || !committed.memory_layers.is_empty()
                || !committed.attached_drives.is_empty()
            {
                return Err(MetaError::InvalidRequest {
                    reason: "Local registration cannot retain published layer references".into(),
                });
            }
        }
        snapshot_directory::register(&self.database, &record).await
    }

    /// Promote a Local directory entry after the caller has published immutable content.
    pub async fn promote_snapshot(
        &self,
        record: SnapshotRecord,
    ) -> MetaResult<Option<SnapshotEntry>> {
        validate_record(&record)?;
        if record.snapshot_type != SnapshotPlacement::Distributed {
            return Err(MetaError::InvalidRequest {
                reason: "promotion requires Distributed placement".into(),
            });
        }
        snapshot_directory::promote(&self.database, &record).await
    }

    /// Retain the identity and its alias even if retirement precedes registration.
    pub async fn retire_snapshot(&self, id: SnapshotId) -> MetaResult<()> {
        snapshot_directory::retire(&self.database, &id).await
    }

    pub async fn get_snapshot(&self, reference: &str) -> MetaResult<Option<SnapshotEntry>> {
        snapshot_directory::get(&self.database, reference).await
    }

    pub async fn list_snapshots(
        &self,
        filter: SnapshotListFilter,
    ) -> MetaResult<Vec<SnapshotEntry>> {
        let mut entries = snapshot_directory::list(&self.database).await?;
        entries.retain(|entry| entry.matches(&filter));
        Ok(entries)
    }
}

fn validate_record(record: &SnapshotRecord) -> MetaResult<()> {
    record
        .validate_committed_metadata()
        .map_err(|reason| MetaError::InvalidRequest { reason })?;
    if record.deleting {
        return Err(MetaError::InvalidRequest {
            reason: "cannot register a snapshot being deleted".into(),
        });
    }
    if let Some(alias) = &record.alias {
        SnapshotAlias::parse(alias.as_ref()).map_err(|error| MetaError::InvalidRequest {
            reason: error.to_string(),
        })?;
    }
    Ok(())
}
