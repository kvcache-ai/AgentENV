//! Node-local GC pin ledger (`pins.db`: `pin_id -> "<kind>:<id>"`) and coalesced refcount.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use agentenv_domain::{ResourceKind, SnapshotId};
use agentenv_metadata::{context::current, MetaClient, MetaError};
use uuid::Uuid;

use super::{map_meta_error, map_rpc_error};
use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::repository::{RepositoryError, RepositoryResult};

const PINS_DB_DIR: &str = "pins.db";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PinTarget {
    kind: ResourceKind,
    id: String,
}

impl PinTarget {
    fn encode(&self) -> Vec<u8> {
        format!("{}:{}", self.kind.as_str(), self.id).into_bytes()
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let raw = std::str::from_utf8(bytes).ok()?;
        if let Some((kind, id)) = raw.split_once(':') {
            if let Ok(kind) = ResourceKind::parse(kind) {
                if !id.is_empty() {
                    return Some(Self {
                        kind,
                        id: id.to_owned(),
                    });
                }
            }
        }
        // Backward compatibility with legacy bare-UUID snapshot entries.
        let id = SnapshotId::parse(raw).ok()?;
        Some(Self {
            kind: ResourceKind::Snapshot,
            id: id.to_string(),
        })
    }
}

struct PinEntry {
    pin_id: Option<Uuid>,
    refs: usize,
}

pub struct NodePins {
    db: Option<LocalKvStore>,
    node_id: Option<String>,
    entries: Mutex<HashMap<PinTarget, PinEntry>>,
}

impl NodePins {
    /// Opens `<root>/pins.db` and preloads persisted pins at refcount 0.
    pub async fn open(root: &Path, node_id: String) -> anyhow::Result<Self> {
        let db = LocalKvStore::open(root.join(PINS_DB_DIR), LocalStoreDurability::Sync).await?;
        let mut entries = HashMap::<PinTarget, PinEntry>::new();
        for (key, value) in db.entries().await? {
            let Some((pin_id, target)) = std::str::from_utf8(&key)
                .ok()
                .and_then(|k| Uuid::parse_str(k).ok())
                .zip(PinTarget::decode(&value))
            else {
                let _ = db.delete(key).await;
                continue;
            };
            entries.entry(target).or_insert(PinEntry {
                pin_id: Some(pin_id),
                refs: 0,
            });
        }
        Ok(Self {
            db: Some(db),
            node_id: Some(node_id),
            entries: Mutex::new(entries),
        })
    }

    /// Creates an in-memory pin tracker for short-lived CLI processes (no `pins.db`).
    pub fn ephemeral(owner_label: impl Into<String>) -> Self {
        Self {
            db: None,
            node_id: Some(owner_label.into()),
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub async fn pin_snapshot(&self, meta: &MetaClient, id: &SnapshotId) -> RepositoryResult<()> {
        self.pin(meta, ResourceKind::Snapshot, &id.to_string())
            .await
    }

    pub fn unpin_snapshot(&self, meta: &MetaClient, id: &SnapshotId) {
        self.unpin(meta, ResourceKind::Snapshot, &id.to_string());
    }

    pub async fn unpin_snapshot_and_wait(&self, meta: &MetaClient, id: &SnapshotId) {
        self.unpin_and_wait(meta, ResourceKind::Snapshot, &id.to_string())
            .await;
    }

    pub async fn pin(
        &self,
        meta: &MetaClient,
        kind: ResourceKind,
        id: &str,
    ) -> RepositoryResult<()> {
        let target = PinTarget {
            kind,
            id: id.to_owned(),
        };
        if let Some(entry) = self.entries.lock().unwrap().get_mut(&target) {
            if entry.pin_id.is_some() {
                entry.refs += 1;
                return Ok(());
            }
        }

        let pin_id = Uuid::now_v7();
        let pinned = meta
            .pin_resource(
                current(),
                kind,
                target.id.clone(),
                pin_id,
                self.node_id.clone(),
            )
            .await
            .map_err(|error| map_rpc_error("pin resource", error))?
            .map_err(map_meta_error)?;
        if !pinned {
            self.entries
                .lock()
                .unwrap()
                .entry(target)
                .or_insert(PinEntry {
                    pin_id: None,
                    refs: 0,
                })
                .refs += 1;
            return Ok(());
        }

        if let Some(db) = &self.db {
            db.put(pin_id.to_string().into_bytes(), target.encode())
                .await
                .map_err(|error| RepositoryError::backend("persist resource pin", error))?;
        }

        let duplicate = {
            let mut entries = self.entries.lock().unwrap();
            match entries.get_mut(&target) {
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
                        target.clone(),
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
            self.spawn_release(meta, target, pin_id);
        }
        Ok(())
    }

    pub fn unpin(&self, meta: &MetaClient, kind: ResourceKind, id: &str) {
        let target = PinTarget {
            kind,
            id: id.to_owned(),
        };
        if let Some(pin_id) = self.decrement(&target) {
            self.spawn_release(meta, target, pin_id);
        }
    }

    pub async fn unpin_and_wait(&self, meta: &MetaClient, kind: ResourceKind, id: &str) {
        let target = PinTarget {
            kind,
            id: id.to_owned(),
        };
        if let Some(pin_id) = self.decrement(&target) {
            Self::release_now(meta, self.db.as_ref(), target, pin_id).await;
        }
    }

    fn decrement(&self, target: &PinTarget) -> Option<Uuid> {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.get_mut(target)?;
        if entry.refs > 1 {
            entry.refs -= 1;
            return None;
        }
        entries.remove(target).and_then(|entry| entry.pin_id)
    }

    pub fn release_orphans(&self, meta: &MetaClient) {
        let mut released = Vec::new();
        self.entries.lock().unwrap().retain(|target, entry| {
            if entry.refs == 0 {
                if let Some(pin_id) = entry.pin_id {
                    released.push((target.clone(), pin_id));
                }
                false
            } else {
                true
            }
        });
        for (target, pin_id) in released {
            self.spawn_release(meta, target, pin_id);
        }
    }

    async fn release_now(
        meta: &MetaClient,
        db: Option<&LocalKvStore>,
        target: PinTarget,
        pin_id: Uuid,
    ) {
        if let Ok(res) = meta
            .release_pin(current(), target.kind, target.id, pin_id)
            .await
        {
            if !matches!(res, Err(MetaError::Backend { .. })) {
                if let Some(db) = db {
                    let _ = db.delete(pin_id.to_string().into_bytes()).await;
                }
            }
        }
    }

    fn spawn_release(&self, meta: &MetaClient, target: PinTarget, pin_id: Uuid) {
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (meta, db) = (meta.clone(), self.db.clone());
        rt.spawn(async move {
            Self::release_now(&meta, db.as_ref(), target, pin_id).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agentenv_domain::{
        LayerRef, ManagedLayerRef, SnapshotManifest, SnapshotRecord, SnapshotSource, VolumeMode,
        VolumePurpose, VolumeRecord, VolumeStatus,
    };
    use agentenv_metadata::{transport, DatabaseConfig, MetaServer};

    use super::*;

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
            .register_snapshot((&s_paused).into(), &[], SnapshotManifest::default(), &[])
            .await?;
        server
            .register_snapshot((&s_orphan).into(), &[], SnapshotManifest::default(), &[])
            .await?;

        let meta = transport::embedded(server.clone());
        let node1 = NodePins::open(root.path(), "node-a".into()).await?;
        node1.pin_snapshot(&meta, &s_paused.id).await?;
        node1.pin_snapshot(&meta, &s_paused.id).await?;
        node1.pin_snapshot(&meta, &s_orphan.id).await?;
        node1.unpin_snapshot(&meta, &s_paused.id);
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
            let target = PinTarget::decode(&v).expect("valid pin target");
            assert_eq!(target.kind, ResourceKind::Snapshot);
            if target.id == s_paused.id.to_string() {
                paused_pin = Some(pin_id);
            } else if target.id == s_orphan.id.to_string() {
                orphan_pin = Some(pin_id);
            }
        }
        let paused_pin = paused_pin.expect("s_paused pin persisted");
        let orphan_pin = orphan_pin.expect("s_orphan pin persisted");
        drop(kv);

        server.retire_snapshot(s_paused.id.clone()).await?;
        server.retire_snapshot(s_orphan.id.clone()).await?;

        let node2 = NodePins::open(root.path(), "node-a".into()).await?;
        node2.pin_snapshot(&meta, &s_paused.id).await?;
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

        node2.unpin_snapshot(&meta, &s_paused.id);
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
        let node = NodePins::open(root.path(), "node-a".into()).await?;

        let record = sandbox_record();
        // First pin before SQL registration (Ok(false)) still records local ref=1 with pin_id=None.
        node.pin_snapshot(&meta, &record.id).await?;
        // SQL registers snapshot, second pin acquires a real SQL pin H and brings refs to 2.
        server
            .register_snapshot((&record).into(), &[], SnapshotManifest::default(), &[])
            .await?;
        node.pin_snapshot(&meta, &record.id).await?;
        // First unpin (e.g. handle drop on pause) must keep H alive (refs: 2 -> 1).
        node.unpin_snapshot(&meta, &record.id);
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
        let node2 = NodePins::open(root.path(), "node-a".into()).await?;
        node2.pin_snapshot(&meta, &record.id).await?;
        node2.unpin_snapshot(&meta, &record.id);
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

    #[tokio::test]
    async fn decodes_legacy_bare_uuid_and_supports_volume_and_ephemeral_pins() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let db = DatabaseConfig::Sqlite {
            path: root.path().join("meta.sqlite3"),
        };
        db.migrate().await?;
        let server = MetaServer::open(&db).await?;
        let meta = transport::embedded(server.clone());

        let legacy_snap = sandbox_record();
        server
            .register_snapshot((&legacy_snap).into(), &[], SnapshotManifest::default(), &[])
            .await?;
        let legacy_pin = Uuid::now_v7();
        server
            .pin_snapshot(legacy_snap.id.clone(), legacy_pin, Some("node-a".into()))
            .await?;
        let kv =
            LocalKvStore::open(root.path().join(PINS_DB_DIR), LocalStoreDurability::Sync).await?;
        kv.put(
            legacy_pin.to_string().into_bytes(),
            legacy_snap.id.to_string().into_bytes(),
        )
        .await?;
        drop(kv);

        let node = NodePins::open(root.path(), "node-a".into()).await?;
        node.pin_snapshot(&meta, &legacy_snap.id).await?;
        node.unpin_snapshot_and_wait(&meta, &legacy_snap.id).await;
        assert!(matches!(
            server
                .pin_snapshot(legacy_snap.id.clone(), legacy_pin, Some("node-a".into()))
                .await,
            Err(MetaError::Unavailable { .. })
        ));

        let vol = VolumeRecord {
            id: format!("vol_{}", Uuid::now_v7().simple()),
            name: "pinned-vol".into(),
            purpose: VolumePurpose::User,
            mode: VolumeMode::Exclusive,
            size_mb: 64,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_image_config: None,
            backing_layers: vec![LayerRef::Managed(ManagedLayerRef {
                digest: format!("sha256:{}", "b".repeat(64)),
                size: 16,
                uuid: Some(Uuid::now_v7().to_string()),
            })],
            read_only_mounts: Vec::new(),
            deleting: false,
        };
        server.create_volume(vol.clone(), None, &[]).await?;
        let ephemeral = NodePins::ephemeral("cli-export");
        ephemeral.pin(&meta, ResourceKind::Volume, &vol.id).await?;
        ephemeral
            .unpin_and_wait(&meta, ResourceKind::Volume, &vol.id)
            .await;

        server.close().await;
        Ok(())
    }
}
