use std::sync::Arc;

use crate::db::{DatabaseConfig, DatabasePool};
use crate::gc::GcCursor;
use crate::GcStorage;

/// Unified metadata service and the database connections it owns.
///
/// Resource operations are implemented directly on [`MetaServer`] in their
/// respective domain modules (`snapshots`, `volumes`, `images`, `build_jobs`,
/// `layers`, and `gc`).
#[derive(Clone)]
pub struct MetaServer {
    pub(crate) database: DatabasePool,
    pub(crate) storage: Option<Arc<dyn GcStorage>>,
    pub(crate) gc_cursor: Arc<tokio::sync::Mutex<GcCursor>>,
    pub(crate) active_collections: Arc<tokio::sync::RwLock<()>>,
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
        Ok(Self {
            database,
            storage: None,
            gc_cursor: Default::default(),
            active_collections: Default::default(),
        })
    }

    pub fn with_gc_storage(mut self, storage: Arc<dyn GcStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub async fn health(&self) -> crate::MetaResult<()> {
        Ok(())
    }

    pub async fn close(&self) {
        let _drained = self.active_collections.write().await;
        self.database.close().await;
    }
}
