//! Node-local snapshot GC pin ledger (`pins.db`: `pin_id -> snapshot_id`) and coalesced refcount.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use agentenv_domain::SnapshotId;
use agentenv_metadata::{MetaClient, MetaError};
use async_trait::async_trait;
use uuid::Uuid;

use super::map_meta_error;
use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::repository::{RepositoryError, RepositoryResult};

/// Narrow snapshot GC pin interface used by the Orchestrator and `SnapshotHandle`.
#[async_trait]
pub trait SnapshotPinner: Send + Sync {
    async fn pin(&self, id: &SnapshotId) -> RepositoryResult<()>;
    fn unpin(&self, id: &SnapshotId);
}

const PINS_DB_DIR: &str = "pins.db";

struct PinEntry {
    pin_id: Option<Uuid>,
    refs: usize,
}

pub struct NodePins {
    meta: MetaClient,
    db: Option<LocalKvStore>,
    node_id: String,
    runtime: Option<tokio::runtime::Handle>,
    entries: Mutex<HashMap<SnapshotId, PinEntry>>,
}

impl NodePins {
    /// Opens `<root>/pins.db` and preloads persisted pins at refcount 0.
    pub async fn open(meta: MetaClient, root: &Path, node_id: String) -> anyhow::Result<Self> {
        let db = LocalKvStore::open(root.join(PINS_DB_DIR), LocalStoreDurability::Sync).await?;
        let mut entries = HashMap::<SnapshotId, PinEntry>::new();
        for (key, value) in db.entries().await? {
            let Some((pin_id, snapshot_id)) = std::str::from_utf8(&key)
                .ok()
                .and_then(|k| Uuid::parse_str(k).ok())
                .zip(
                    std::str::from_utf8(&value)
                        .ok()
                        .and_then(|v| SnapshotId::parse(v).ok()),
                )
            else {
                let _ = db.delete(key).await;
                continue;
            };
            entries.entry(snapshot_id).or_insert(PinEntry {
                pin_id: Some(pin_id),
                refs: 0,
            });
        }
        Ok(Self {
            meta,
            db: Some(db),
            node_id,
            runtime: tokio::runtime::Handle::try_current().ok(),
            entries: Mutex::new(entries),
        })
    }

    /// Creates an in-memory pin tracker for short-lived CLI processes (no `pins.db`).
    pub fn ephemeral(meta: MetaClient, owner_label: impl Into<String>) -> Self {
        Self {
            meta,
            db: None,
            node_id: owner_label.into(),
            runtime: tokio::runtime::Handle::try_current().ok(),
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub async fn pin_snapshot(&self, id: &SnapshotId) -> RepositoryResult<()> {
        if let Some(entry) = self.entries.lock().unwrap().get_mut(id) {
            if entry.pin_id.is_some() {
                entry.refs += 1;
                return Ok(());
            }
        }

        let pin_id = Uuid::now_v7();
        let pinned = self
            .meta
            .pin_snapshot(id.clone(), pin_id, Some(self.node_id.clone()))
            .await
            .map_err(map_meta_error)?;
        if !pinned {
            self.entries
                .lock()
                .unwrap()
                .entry(id.clone())
                .or_insert(PinEntry {
                    pin_id: None,
                    refs: 0,
                })
                .refs += 1;
            return Ok(());
        }

        if let Some(db) = &self.db {
            db.put(pin_id.to_string().into_bytes(), id.to_string().into_bytes())
                .await
                .map_err(|error| RepositoryError::backend("persist snapshot pin", error))?;
        }

        let duplicate = {
            let mut entries = self.entries.lock().unwrap();
            match entries.get_mut(id) {
                Some(entry) if entry.pin_id.is_some() => {
                    entry.refs += 1;
                    true
                }
                Some(entry) => {
                    entry.pin_id = Some(pin_id);
                    entry.refs += 1;
                    false
                }
                None => {
                    entries.insert(
                        id.clone(),
                        PinEntry {
                            pin_id: Some(pin_id),
                            refs: 1,
                        },
                    );
                    false
                }
            }
        };
        if duplicate {
            self.spawn_release(id.clone(), pin_id);
        }
        Ok(())
    }

    pub fn unpin_snapshot(&self, id: &SnapshotId) {
        if let Some(pin_id) = self.decrement(id) {
            self.spawn_release(id.clone(), pin_id);
        }
    }

    pub async fn unpin_snapshot_and_wait(&self, id: &SnapshotId) {
        if let Some(pin_id) = self.decrement(id) {
            Self::release_now(&self.meta, self.db.as_ref(), id.clone(), pin_id).await;
        }
    }

    fn decrement(&self, id: &SnapshotId) -> Option<Uuid> {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.get_mut(id)?;
        if entry.refs > 1 {
            entry.refs -= 1;
            return None;
        }
        entries.remove(id).and_then(|entry| entry.pin_id)
    }

    pub fn release_orphans(&self) {
        let mut released = Vec::new();
        self.entries.lock().unwrap().retain(|id, entry| {
            if entry.refs == 0 {
                if let Some(pin_id) = entry.pin_id {
                    released.push((id.clone(), pin_id));
                }
                false
            } else {
                true
            }
        });
        for (id, pin_id) in released {
            self.spawn_release(id, pin_id);
        }
    }

    async fn release_now(
        meta: &MetaClient,
        db: Option<&LocalKvStore>,
        id: SnapshotId,
        pin_id: Uuid,
    ) {
        if !matches!(
            meta.release_snapshot_pin(id, pin_id).await,
            Err(MetaError::Backend { .. })
        ) {
            if let Some(db) = db {
                let _ = db.delete(pin_id.to_string().into_bytes()).await;
            }
        }
    }

    fn spawn_release(&self, id: SnapshotId, pin_id: Uuid) {
        let Some(rt) = tokio::runtime::Handle::try_current()
            .ok()
            .or_else(|| self.runtime.clone())
        else {
            return;
        };
        let (meta, db) = (self.meta.clone(), self.db.clone());
        rt.spawn(async move {
            Self::release_now(&meta, db.as_ref(), id, pin_id).await;
        });
    }
}

#[async_trait]
impl SnapshotPinner for NodePins {
    async fn pin(&self, id: &SnapshotId) -> RepositoryResult<()> {
        self.pin_snapshot(id).await
    }

    fn unpin(&self, id: &SnapshotId) {
        self.unpin_snapshot(id);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agentenv_domain::{SnapshotManifest, SnapshotRecord, SnapshotSource};
    use agentenv_metadata::MetaServer;

    use super::*;

    fn sandbox_record() -> SnapshotRecord {
        let mut record = SnapshotRecord::mock_ready(SnapshotManifest::default());
        record.config.source = SnapshotSource::Sandbox {
            source_sandbox_id: "pin-test-sbx".into(),
            source_sandbox_metadata: None,
        };
        record
    }

    async fn wait_pin_released(server: &MetaServer, snap_id: &SnapshotId, pin_id: Uuid) -> bool {
        for _ in 0..200 {
            if matches!(
                server
                    .pin_snapshot(snap_id, pin_id, Some("node-a".into()))
                    .await,
                Err(MetaError::Unavailable { .. })
            ) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    #[tokio::test]
    async fn restart_claims_paused_pin_and_releases_orphan_pins() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let (server, meta) = crate::metadata::test_meta(root.path()).await;

        let s_paused = sandbox_record();
        let s_orphan = sandbox_record();
        server.register_snapshot(s_paused.clone(), &[]).await?;
        server.register_snapshot(s_orphan.clone(), &[]).await?;

        let node1 = NodePins::open(meta.clone(), root.path(), "node-a".into()).await?;
        node1.pin_snapshot(&s_paused.id).await?;
        node1.pin_snapshot(&s_paused.id).await?;
        node1.pin_snapshot(&s_orphan.id).await?;
        node1.unpin_snapshot(&s_paused.id);
        drop(node1);

        let kv =
            LocalKvStore::open(root.path().join(PINS_DB_DIR), LocalStoreDurability::Sync).await?;
        let entries = kv.entries().await?;
        assert_eq!(entries.len(), 2, "two pins for s_paused should coalesce");
        let (mut paused_pin, mut orphan_pin) = (None, None);
        for (k, v) in entries {
            let pin_id = Uuid::parse_str(std::str::from_utf8(&k)?)?;
            let snap_id = SnapshotId::parse(std::str::from_utf8(&v)?)?;
            if snap_id == s_paused.id {
                paused_pin = Some(pin_id);
            } else if snap_id == s_orphan.id {
                orphan_pin = Some(pin_id);
            }
        }
        let (paused_pin, orphan_pin) = (paused_pin.unwrap(), orphan_pin.unwrap());
        drop(kv);

        server.retire_snapshot(s_paused.id.clone()).await?;
        server.retire_snapshot(s_orphan.id.clone()).await?;

        let node2 = NodePins::open(meta.clone(), root.path(), "node-a".into()).await?;
        node2.pin_snapshot(&s_paused.id).await?;
        node2.release_orphans();
        assert!(wait_pin_released(&server, &s_orphan.id, orphan_pin).await);
        assert!(
            server
                .pin_snapshot(&s_paused.id, paused_pin, Some("node-a".into()))
                .await?
        );

        node2.unpin_snapshot(&s_paused.id);
        assert!(wait_pin_released(&server, &s_paused.id, paused_pin).await);
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn unregistered_and_ephemeral_snapshot_pins() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let (server, meta) = crate::metadata::test_meta(root.path()).await;

        // 1. Unregistered pin keeps refcount when SQL row appears later.
        let record = sandbox_record();
        let node = NodePins::open(meta.clone(), root.path(), "node-a".into()).await?;
        node.pin_snapshot(&record.id).await?;
        server.register_snapshot(record.clone(), &[]).await?;
        node.pin_snapshot(&record.id).await?;
        node.unpin_snapshot(&record.id);
        server.retire_snapshot(record.id.clone()).await?;
        drop(node);

        let kv =
            LocalKvStore::open(root.path().join(PINS_DB_DIR), LocalStoreDurability::Sync).await?;
        let entries = kv.entries().await?;
        assert_eq!(entries.len(), 1);
        let pin_id = Uuid::parse_str(std::str::from_utf8(&entries[0].0)?)?;
        drop(kv);

        assert!(
            server
                .pin_snapshot(&record.id, pin_id, Some("node-a".into()))
                .await?
        );
        let node2 = NodePins::open(meta.clone(), root.path(), "node-a".into()).await?;
        node2.pin_snapshot(&record.id).await?;
        node2.unpin_snapshot(&record.id);
        assert!(wait_pin_released(&server, &record.id, pin_id).await);

        // 2. Ephemeral CLI pins do not touch pins.db and release synchronously.
        let exported = sandbox_record();
        server.register_snapshot(exported.clone(), &[]).await?;
        let ephemeral = NodePins::ephemeral(meta, "cli-export");
        ephemeral.pin_snapshot(&exported.id).await?;
        ephemeral.unpin_snapshot_and_wait(&exported.id).await;

        server.close().await;
        Ok(())
    }
}
