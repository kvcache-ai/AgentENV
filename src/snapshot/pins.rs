//! Node-local snapshot pin ledger (`pins.db`: `pin_id -> snapshot_id`) and coalesced refcount.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use agentenv_metadata::{context::current, MetaClient, MetaError};
use uuid::Uuid;

use super::catalog::{map_meta_error, map_rpc_error};
use super::SnapshotId;
use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::repository::{RepositoryError, RepositoryResult};

const PINS_DB_DIR: &str = "pins.db";

struct PinEntry {
    pin_id: Option<Uuid>,
    refs: usize,
}

pub(crate) struct SnapshotPins {
    db: LocalKvStore,
    node_id: String,
    entries: Mutex<HashMap<SnapshotId, PinEntry>>,
}

impl SnapshotPins {
    /// Opens `<snapshot_root>/pins.db` and preloads persisted pins at refcount 0.
    pub(crate) async fn open(snapshot_root: &Path, node_id: String) -> anyhow::Result<Self> {
        let db =
            LocalKvStore::open(snapshot_root.join(PINS_DB_DIR), LocalStoreDurability::Sync).await?;
        let mut entries = HashMap::<SnapshotId, PinEntry>::new();
        for (key, value) in db.entries().await? {
            let Some((pin_id, snap_id)) = std::str::from_utf8(&key)
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
            entries.entry(snap_id).or_insert(PinEntry {
                pin_id: Some(pin_id),
                refs: 0,
            });
        }
        Ok(Self {
            db,
            node_id,
            entries: Mutex::new(entries),
        })
    }

    pub(crate) async fn pin(&self, meta: &MetaClient, id: &SnapshotId) -> RepositoryResult<()> {
        if let Some(entry) = self.entries.lock().unwrap().get_mut(id) {
            if entry.pin_id.is_some() {
                entry.refs += 1;
                return Ok(());
            }
        }

        let pin_id = Uuid::now_v7();
        let entry = meta
            .pin_snapshot(current(), id.clone(), pin_id, Some(self.node_id.clone()))
            .await
            .map_err(|error| map_rpc_error("pin snapshot", error))?
            .map_err(map_meta_error)?;
        if entry.is_none() {
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

        self.db
            .put(pin_id.to_string().into_bytes(), id.to_string().into_bytes())
            .await
            .map_err(|error| RepositoryError::backend("persist snapshot pin", error))?;

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
            self.spawn_release(meta, id.clone(), pin_id);
        }
        Ok(())
    }

    pub(crate) fn unpin(&self, meta: &MetaClient, id: &SnapshotId) {
        let mut entries = self.entries.lock().unwrap();
        let Some(entry) = entries.get_mut(id) else {
            return;
        };
        if entry.refs > 1 {
            entry.refs -= 1;
            return;
        }
        let pin_id = entries.remove(id).and_then(|entry| entry.pin_id);
        drop(entries);
        if let Some(pin_id) = pin_id {
            self.spawn_release(meta, id.clone(), pin_id);
        }
    }

    pub(crate) fn release_orphans(&self, meta: &MetaClient) {
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
            self.spawn_release(meta, id, pin_id);
        }
    }

    fn spawn_release(&self, meta: &MetaClient, id: SnapshotId, pin_id: Uuid) {
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (meta, db) = (meta.clone(), self.db.clone());
        rt.spawn(async move {
            if let Ok(res) = meta.release_pin(current(), id, pin_id).await {
                if !matches!(res, Err(MetaError::Backend { .. })) {
                    let _ = db.delete(pin_id.to_string().into_bytes()).await;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agentenv_metadata::{transport, DatabaseConfig, MetaServer};

    use super::*;
    use crate::snapshot::{SnapshotManifest, SnapshotRecord, SnapshotSource};

    fn sandbox_record() -> SnapshotRecord {
        let mut record = SnapshotRecord::mock_ready(SnapshotManifest::default());
        record.config.source = SnapshotSource::Sandbox {
            source_sandbox_id: "pin-test-sbx".into(),
            source_sandbox_metadata: None,
        };
        record
    }

    #[tokio::test]
    async fn restart_claims_paused_pin_and_releases_orphan_pins() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let db = DatabaseConfig::Sqlite {
            path: root.path().join("meta.sqlite3"),
        };
        db.migrate().await?;
        let server = MetaServer::open(&db).await?;

        let s_paused = sandbox_record();
        let s_orphan = sandbox_record();
        server
            .register_snapshot((&s_paused).into(), SnapshotManifest::default(), &[])
            .await?;
        server
            .register_snapshot((&s_orphan).into(), SnapshotManifest::default(), &[])
            .await?;

        let meta = transport::embedded(server.clone());
        let node1 = SnapshotPins::open(root.path(), "node-a".into()).await?;
        node1.pin(&meta, &s_paused.id).await?;
        node1.pin(&meta, &s_paused.id).await?;
        node1.pin(&meta, &s_orphan.id).await?;
        node1.unpin(&meta, &s_paused.id);
        drop(node1);

        let kv =
            LocalKvStore::open(root.path().join(PINS_DB_DIR), LocalStoreDurability::Sync).await?;
        let entries = kv.entries().await?;
        assert_eq!(
            entries.len(),
            2,
            "two pins for s_paused should coalesce to one"
        );
        let mut paused_pin = None;
        let mut orphan_pin = None;
        for (k, v) in entries {
            let pin_id = Uuid::parse_str(std::str::from_utf8(&k)?)?;
            let snap_id = SnapshotId::parse(std::str::from_utf8(&v)?)?;
            if snap_id == s_paused.id {
                paused_pin = Some(pin_id);
            } else if snap_id == s_orphan.id {
                orphan_pin = Some(pin_id);
            }
        }
        let paused_pin = paused_pin.expect("s_paused pin persisted");
        let orphan_pin = orphan_pin.expect("s_orphan pin persisted");
        drop(kv);

        server.retire_snapshot(s_paused.id.clone()).await?;
        server.retire_snapshot(s_orphan.id.clone()).await?;

        let node2 = SnapshotPins::open(root.path(), "node-a".into()).await?;
        node2.pin(&meta, &s_paused.id).await?;
        node2.release_orphans(&meta);

        let wait_released = |snap_id: SnapshotId, pin_id: Uuid| {
            let server = server.clone();
            async move {
                for _ in 0..200 {
                    if matches!(
                        server
                            .pin_snapshot(snap_id.clone(), pin_id, Some("node-a".into()))
                            .await,
                        Err(MetaError::Unavailable { .. })
                    ) {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                false
            }
        };

        assert!(
            wait_released(s_orphan.id.clone(), orphan_pin).await,
            "unclaimed orphan pin should be released"
        );
        assert!(
            server
                .pin_snapshot(s_paused.id.clone(), paused_pin, Some("node-a".into()))
                .await?
                .is_some(),
            "claimed paused pin should stay active even after snapshot retirement"
        );

        node2.unpin(&meta, &s_paused.id);
        assert!(
            wait_released(s_paused.id.clone(), paused_pin).await,
            "final unpin should release paused pin"
        );
        server.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn unregistered_pin_keeps_refcount_when_sql_row_appears_later() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let db = DatabaseConfig::Sqlite {
            path: root.path().join("meta.sqlite3"),
        };
        db.migrate().await?;
        let server = MetaServer::open(&db).await?;
        let meta = transport::embedded(server.clone());
        let node = SnapshotPins::open(root.path(), "node-a".into()).await?;

        let record = sandbox_record();
        // First pin before SQL registration (Ok(None)) still records local ref=1 with pin_id=None.
        node.pin(&meta, &record.id).await?;
        // SQL registers snapshot, second pin acquires a real SQL pin H and brings refs to 2.
        server
            .register_snapshot((&record).into(), SnapshotManifest::default(), &[])
            .await?;
        node.pin(&meta, &record.id).await?;
        // First unpin (e.g. handle drop on pause) must keep H alive (refs: 2 -> 1).
        node.unpin(&meta, &record.id);
        server.retire_snapshot(record.id.clone()).await?;
        drop(node);

        let kv =
            LocalKvStore::open(root.path().join(PINS_DB_DIR), LocalStoreDurability::Sync).await?;
        let entries = kv.entries().await?;
        assert_eq!(entries.len(), 1, "H must still be persisted in pins.db");
        let pin_id = Uuid::parse_str(std::str::from_utf8(&entries[0].0)?)?;
        drop(kv);

        // H must still be active in SQL while the remaining owner exists.
        assert!(server
            .pin_snapshot(record.id.clone(), pin_id, Some("node-a".into()))
            .await?
            .is_some());

        // Reopen, claim the remaining owner, then final unpin releases H.
        let node2 = SnapshotPins::open(root.path(), "node-a".into()).await?;
        node2.pin(&meta, &record.id).await?;
        node2.unpin(&meta, &record.id);
        let mut released = false;
        for _ in 0..200 {
            if matches!(
                server
                    .pin_snapshot(record.id.clone(), pin_id, Some("node-a".into()))
                    .await,
                Err(MetaError::Unavailable { .. })
            ) {
                released = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(released, "final unpin should release H in SQL");
        server.close().await;
        Ok(())
    }
}
