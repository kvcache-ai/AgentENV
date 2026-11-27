use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agentenv_domain::{
    posix_layer_path, posix_snapshot_dir, LayerRef, ManagedLayerRef, ResourceKind, SnapshotAlias,
    SnapshotId, SnapshotManifest, SnapshotPlacement, SnapshotRecord, SnapshotSource,
    SnapshotVolume, VolumeMode, VolumePurpose, VolumeRecord, VolumeStatus,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::{AssertSqlSafe, Connection, SqliteConnection, SqlitePool};
use tempfile::TempDir;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::sweep::{ADVISORY_LOCK_KEY, LAYER_BATCH_SIZE, MAX_STALLED_BATCHES, SNAPSHOT_BATCH_SIZE};
use super::{GcCursor, GcStorage};
use crate::database::DatabasePool;
use crate::{DatabaseConfig, MetaServer};

static PG_GC_TEST_LOCK: Mutex<()> = Mutex::const_new(());

struct PosixTestGcStorage(PathBuf);

#[async_trait]
impl GcStorage for PosixTestGcStorage {
    async fn delete_snapshot(&self, id: &SnapshotId) -> Result<()> {
        let path = self.0.join(posix_snapshot_dir(id));
        match tokio::fs::remove_dir_all(&path).await {
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            result => result.with_context(|| format!("remove snapshot {}", path.display())),
        }
    }

    async fn delete_layers(&self, digests: &[String]) -> Vec<Result<()>> {
        let mut results = Vec::with_capacity(digests.len());
        for digest in digests {
            let path = self.0.join(posix_layer_path(digest));
            results.push(match tokio::fs::remove_file(&path).await {
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                result => result.with_context(|| format!("remove layer {}", path.display())),
            });
        }
        results
    }
}

fn layer(uuid: Uuid) -> ManagedLayerRef {
    ManagedLayerRef {
        uuid: Some(uuid.to_string()),
        digest: format!("sha256:{:064x}", uuid.as_u128()),
        size: 1,
    }
}

fn snap(layers: Vec<ManagedLayerRef>) -> SnapshotRecord {
    let mut record = SnapshotRecord::mock_ready(SnapshotManifest {
        memory_layers: layers,
        ..Default::default()
    });
    record.config.source = SnapshotSource::Sandbox {
        source_sandbox_id: "gc-test-sbx".into(),
        source_sandbox_metadata: None,
    };
    record
}

async fn register(srv: &MetaServer, record: &SnapshotRecord) -> Result<()> {
    srv.register_snapshot(record.clone(), &[]).await?;
    Ok(())
}

fn touch_file(path: PathBuf) -> Result<PathBuf> {
    fs::create_dir_all(path.parent().unwrap())?;
    fs::write(&path, b"x")?;
    Ok(path)
}

fn touch_layer(root: &Path, l: &ManagedLayerRef) -> Result<PathBuf> {
    touch_file(root.join(posix_layer_path(&l.digest)))
}

fn blocking_layer(root: &Path, uuid: Uuid) -> Result<(ManagedLayerRef, PathBuf)> {
    let l = layer(uuid);
    let path = root.join(posix_layer_path(&l.digest));
    fs::create_dir_all(&path)?;
    Ok((l, path))
}

fn touch_snap(root: &Path, id: &SnapshotId) -> Result<PathBuf> {
    let dir = root.join(posix_snapshot_dir(id));
    touch_file(dir.join("vm_state.bin"))?;
    Ok(dir)
}

async fn open_sqlite() -> Result<(TempDir, DatabaseConfig, MetaServer, PathBuf)> {
    let tmp = tempfile::tempdir()?;
    let db = DatabaseConfig::Sqlite {
        path: tmp.path().join("db.sqlite3"),
    };
    db.migrate().await?;
    let root = tmp.path().join("repo");
    let server = MetaServer::open(&db)
        .await?
        .with_gc_storage(Arc::new(PosixTestGcStorage(root.clone())));
    Ok((tmp, db, server, root))
}

fn sqlite_pool(server: &MetaServer) -> &SqlitePool {
    match &server.database {
        DatabasePool::Sqlite(pool) => pool,
        DatabasePool::Postgres(_) => unreachable!(),
    }
}

async fn insert_layer(
    tx: &mut SqliteConnection,
    l: &ManagedLayerRef,
    parent: Option<&str>,
    state: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO layers (uuid, managed_parent_uuid, digest, state) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(l.uuid.as_deref().unwrap())
    .bind(parent)
    .bind(&l.digest)
    .bind(state)
    .execute(tx)
    .await?;
    Ok(())
}

async fn insert_retired_snap(tx: &mut SqliteConnection, id: &SnapshotId) -> Result<()> {
    sqlx::query(
        "INSERT INTO snapshots \
         (snapshot_id, source_kind, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at) \
         VALUES ($1, 'sandbox', NULL, 'retired', 'distributed', NULL, 'sbx', 1, 1)",
    )
    .bind(id.to_string())
    .execute(tx)
    .await?;
    Ok(())
}

async fn count(server: &MetaServer, clause: &str) -> Result<i64> {
    Ok(
        sqlx::query_scalar(AssertSqlSafe(format!("SELECT count(*) FROM {clause}")))
            .fetch_one(sqlite_pool(server))
            .await?,
    )
}

async fn scalar(db: &DatabasePool, sql: &str, id: &str) -> Result<String> {
    Ok(match db {
        DatabasePool::Postgres(p) => {
            sqlx::query_scalar(AssertSqlSafe(sql.replace("$1", "$1::uuid")))
                .bind(id)
                .fetch_one(p)
                .await?
        }
        DatabasePool::Sqlite(p) => {
            sqlx::query_scalar(AssertSqlSafe(sql.to_owned()))
                .bind(id)
                .fetch_one(p)
                .await?
        }
    })
}

async fn layer_state(s: &MetaServer, l: &ManagedLayerRef) -> Result<String> {
    scalar(
        &s.database,
        "SELECT state FROM layers WHERE uuid = $1",
        l.uuid.as_deref().unwrap(),
    )
    .await
}

async fn snap_state(s: &MetaServer, id: &SnapshotId) -> Result<String> {
    scalar(
        &s.database,
        "SELECT state FROM snapshots WHERE snapshot_id = $1",
        &id.to_string(),
    )
    .await
}

#[tokio::test]
async fn reclaims_retired_snapshots_with_shared_layers_pins_and_id_tombstones() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut dbs = vec![DatabaseConfig::Sqlite {
        path: tmp.path().join("catalog.sqlite3"),
    }];
    let _pg = match std::env::var("AENV_TEST_DATABASE_URL") {
        Ok(url) => {
            dbs.push(DatabaseConfig::Postgres { url });
            Some(PG_GC_TEST_LOCK.lock().await)
        }
        Err(_) => None,
    };

    for db in &dbs {
        db.migrate().await?;
        let root = tempfile::tempdir()?;
        let srv = MetaServer::open(db)
            .await?
            .with_gc_storage(Arc::new(PosixTestGcStorage(root.path().to_owned())));

        // Retired Local snapshot reaps both its shared snapshot.json directory and its
        // managed volume layers, and transitions cleanly to `reaped` (NULL placement/owner).
        let local_vol_layer = layer(Uuid::now_v7());
        let local_vol_f = touch_layer(root.path(), &local_vol_layer)?;
        let mut local = snap(vec![]);
        local.snapshot_type = SnapshotPlacement::Local;
        local.owner_node_id = Some("node-a".into());
        local.manifest = None;
        local.config.volume_snapshots.push(SnapshotVolume {
            mount_path: "/data".into(),
            mode: VolumeMode::Exclusive,
            size_mb: 64,
            layers: vec![LayerRef::Managed(local_vol_layer.clone())],
        });
        register(&srv, &local).await?;
        srv.retire_snapshot(local.id.clone()).await?;
        let local_dir = touch_snap(root.path(), &local.id)?;

        // Two Distributed snapshots share [base, mid] and diverge at leaf_a / leaf_b;
        // snap_b's alias equals snap_a.id to verify GC's `reaped` tombstone never yields to alias.
        let [base, mid, leaf_a, leaf_b] = std::array::from_fn(|_| layer(Uuid::now_v7()));
        let snap_a = snap(vec![base.clone(), mid.clone(), leaf_a.clone()]);
        let mut snap_b = snap(vec![base.clone(), mid.clone(), leaf_b.clone()]);
        snap_b.alias = Some(SnapshotAlias::parse(&snap_a.id.to_string())?);
        register(&srv, &snap_a).await?;
        register(&srv, &snap_b).await?;
        let [base_f, mid_f, leaf_a_f, leaf_b_f] = [
            touch_layer(root.path(), &base)?,
            touch_layer(root.path(), &mid)?,
            touch_layer(root.path(), &leaf_a)?,
            touch_layer(root.path(), &leaf_b)?,
        ];
        let (dir_a, dir_b) = (
            touch_snap(root.path(), &snap_a.id)?,
            touch_snap(root.path(), &snap_b.id)?,
        );

        // Active pin protects snap_a and leaf_a even after retirement.
        let pin = Uuid::now_v7();
        assert!(
            srv.pin_resource(
                ResourceKind::Snapshot,
                &snap_a.id.to_string(),
                pin,
                Some("node-a".into()),
            )
            .await?
        );
        srv.retire_snapshot(snap_a.id.clone()).await?;
        srv.collect_garbage(u32::MAX).await?;
        assert!(
            !local_dir.exists(),
            "GC must clean up shared snapshot.json directory for retired Local snapshots"
        );
        assert!(
            !local_vol_f.exists(),
            "GC must reap managed volume layers owned by retired Local snapshots"
        );
        assert_eq!(snap_state(&srv, &local.id).await?, "reaped");
        assert_eq!(layer_state(&srv, &local_vol_layer).await?, "deleted");
        assert!(dir_a.exists() && leaf_a_f.exists());
        assert_eq!(snap_state(&srv, &snap_a.id).await?, "retired");

        // Releasing pin reclaims snap_a + leaf_a, keeps mid/base and post-GC ID tombstone.
        srv.release_pin(ResourceKind::Snapshot, &snap_a.id.to_string(), pin)
            .await?;
        srv.collect_garbage(u32::MAX).await?;
        assert!(!dir_a.exists() && !leaf_a_f.exists());
        assert!(dir_b.exists() && leaf_b_f.exists() && mid_f.exists() && base_f.exists());
        assert_eq!(snap_state(&srv, &snap_a.id).await?, "reaped");
        let after_gc = srv
            .get_snapshot(&snap_a.id.to_string(), false)
            .await?
            .unwrap();
        assert!(after_gc.id == snap_a.id && !after_gc.is_visible());
        assert_eq!(layer_state(&srv, &leaf_a).await?, "deleted");
        assert_eq!(layer_state(&srv, &mid).await?, "available");

        // Retiring snap_b frees the remaining chain wave by wave in one sweep.
        srv.retire_snapshot(snap_b.id.clone()).await?;
        srv.collect_garbage(u32::MAX).await?;
        assert!(!dir_b.exists() && !leaf_b_f.exists() && !mid_f.exists() && !base_f.exists());
        for l in [&leaf_b, &mid, &base] {
            assert_eq!(layer_state(&srv, l).await?, "deleted");
        }
        srv.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn reclaims_released_volume_heads_and_protects_shared_digests() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let mut dbs = vec![DatabaseConfig::Sqlite {
        path: tmp.path().join("volume-gc.sqlite3"),
    }];
    let _pg = match std::env::var("AENV_TEST_DATABASE_URL") {
        Ok(url) => {
            dbs.push(DatabaseConfig::Postgres { url });
            Some(PG_GC_TEST_LOCK.lock().await)
        }
        Err(_) => None,
    };

    for db in &dbs {
        db.migrate().await?;
        let root = tempfile::tempdir()?;
        let srv = MetaServer::open(db)
            .await?
            .with_gc_storage(Arc::new(PosixTestGcStorage(root.path().to_owned())));

        let [base, old_top, new_top] = std::array::from_fn(|_| layer(Uuid::now_v7()));
        let [base_f, old_top_f, new_top_f] = [
            touch_layer(root.path(), &base)?,
            touch_layer(root.path(), &old_top)?,
            touch_layer(root.path(), &new_top)?,
        ];
        let mut vol = VolumeRecord {
            id: format!("vol_{}", Uuid::now_v7().simple()),
            name: format!("gc_vol_{}", Uuid::now_v7().simple()),
            purpose: VolumePurpose::User,
            mode: VolumeMode::Exclusive,
            size_mb: 128,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_layers: vec![
                LayerRef::Managed(base.clone()),
                LayerRef::Managed(old_top.clone()),
            ],
            read_only_mounts: Vec::new(),
        };
        srv.create_volume(vol.clone(), None, &[]).await?;

        // Pin the volume, replace its top layer, and confirm the released head is held until unpinned.
        let pin = Uuid::now_v7();
        assert!(
            srv.pin_resource(ResourceKind::Volume, &vol.id, pin, Some("node-a".into()))
                .await?
        );
        vol.status = VolumeStatus::Uploading;
        srv.update_volume(vol.clone(), &[]).await?;
        vol.status = VolumeStatus::Ready;
        vol.backing_layers = vec![
            LayerRef::Managed(base.clone()),
            LayerRef::Managed(new_top.clone()),
        ];
        srv.update_volume(vol.clone(), &[]).await?;

        // Second volume sharing `old_top`: releasing its head while `vol` still holds a pin
        // must keep `old_top` available and on disk until `vol`'s pin is released.
        let vol2 = VolumeRecord {
            id: format!("vol_{}", Uuid::now_v7().simple()),
            name: format!("gc_vol2_{}", Uuid::now_v7().simple()),
            backing_layers: vec![
                LayerRef::Managed(base.clone()),
                LayerRef::Managed(old_top.clone()),
            ],
            ..vol.clone()
        };
        srv.create_volume(vol2.clone(), None, &[]).await?;
        srv.delete_volume(&vol2.id).await?;

        let blocked = srv.collect_garbage(u32::MAX).await?;
        assert_eq!(blocked.reaped_heads, 1);
        assert_eq!(blocked.deleted_layers, 0);
        assert!(old_top_f.exists());
        assert_eq!(layer_state(&srv, &old_top).await?, "available");

        srv.release_pin(ResourceKind::Volume, &vol.id, pin).await?;
        let swept = srv.collect_garbage(u32::MAX).await?;
        assert_eq!(swept.reaped_heads, 1);
        assert_eq!(swept.deleted_layers, 1);
        assert!(!old_top_f.exists());
        assert!(base_f.exists() && new_top_f.exists());
        assert_eq!(layer_state(&srv, &old_top).await?, "deleted");

        // Deleting the volume releases its remaining head and cascades down to `base`.
        srv.delete_volume(&vol.id).await?;
        let after_delete = srv.collect_garbage(u32::MAX).await?;
        assert_eq!(after_delete.reaped_heads, 1);
        assert_eq!(after_delete.deleted_layers, 2);
        assert!(!new_top_f.exists() && !base_f.exists());

        // Shared digest across two distinct UUIDs: deleting the first UUID must keep the blob.
        let shared_digest = format!("sha256:{}", "a".repeat(64));
        let u1 = ManagedLayerRef {
            uuid: Some(Uuid::now_v7().to_string()),
            digest: shared_digest.clone(),
            size: 16,
        };
        let u2 = ManagedLayerRef {
            uuid: Some(Uuid::now_v7().to_string()),
            digest: shared_digest,
            size: 16,
        };
        let shared_file = touch_layer(root.path(), &u1)?;
        let s1 = snap(vec![u1.clone()]);
        let s2 = snap(vec![u2.clone()]);
        register(&srv, &s1).await?;
        register(&srv, &s2).await?;
        srv.retire_snapshot(s1.id.clone()).await?;
        let first = srv.collect_garbage(u32::MAX).await?;
        assert_eq!(first.reaped_snapshots, 1);
        assert_eq!(first.deleted_layers, 0);
        assert_eq!(layer_state(&srv, &u1).await?, "deleted");
        assert_eq!(layer_state(&srv, &u2).await?, "available");
        assert!(
            shared_file.exists(),
            "shared digest blob must survive while another live UUID references it"
        );

        srv.retire_snapshot(s2.id.clone()).await?;
        let second = srv.collect_garbage(u32::MAX).await?;
        assert_eq!(second.reaped_snapshots, 1);
        assert_eq!(second.deleted_layers, 1);
        assert_eq!(layer_state(&srv, &u2).await?, "deleted");
        assert!(!shared_file.exists());

        srv.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn recovers_deleting_layers_across_restart_and_backoffs_on_stalls() -> Result<()> {
    let (_tmp, db, srv, root) = open_sqlite().await?;

    // Part A: `LAYER_BATCH_SIZE + 2` deleting layers across 2 pages (0, 64 fail unlink).
    // Healthy layers (including page 2) and exposed parent must recover across restart.
    let parent = layer(Uuid::from_u128(0x0001));
    let parent_f = touch_layer(&root, &parent)?;
    let (total, bad) = (LAYER_BATCH_SIZE + 2, [0usize, 64]);
    let mut items = Vec::with_capacity(total);
    let mut tx = sqlite_pool(&srv).begin().await?;
    insert_layer(&mut tx, &parent, None, "available").await?;
    for i in 0..total {
        let uuid = Uuid::from_u128(0x1000 + i as u128);
        let (l, p) = if bad.contains(&i) {
            blocking_layer(&root, uuid)?
        } else {
            let l = layer(uuid);
            let p = touch_layer(&root, &l)?;
            (l, p)
        };
        let pid = (i + 1 == total).then_some(parent.uuid.as_deref().unwrap());
        insert_layer(&mut tx, &l, pid, "deleting").await?;
        items.push((l, p));
    }
    tx.commit().await?;
    srv.close().await;

    let srv = MetaServer::open(&db)
        .await?
        .with_gc_storage(Arc::new(PosixTestGcStorage(root.clone())));
    assert!(srv.collect_garbage(u32::MAX).await.is_err());
    for (i, (l, p)) in items.iter().enumerate() {
        let state = layer_state(&srv, l).await?;
        if bad.contains(&i) {
            assert!(p.exists() && state == "deleting");
        } else {
            assert!(!p.exists() && state == "deleted", "layer {i} starved");
        }
    }
    assert!(!parent_f.exists() && layer_state(&srv, &parent).await? == "deleted");

    for i in bad {
        fs::remove_dir(&items[i].1)?;
        fs::write(&items[i].1, b"fixed")?;
    }
    srv.collect_garbage(u32::MAX).await?;
    assert_eq!(count(&srv, "layers WHERE state = 'deleting'").await?, 0);

    // Part B: Sustained zero-progress layer failures pause claiming new snapshots after MAX_STALLED_BATCHES.
    let total_snaps = (MAX_STALLED_BATCHES + 1) * SNAPSHOT_BATCH_SIZE;
    let mut dirs = Vec::with_capacity(total_snaps);
    for _ in 0..total_snaps {
        let (bad_l, _) = blocking_layer(&root, Uuid::now_v7())?;
        let s = snap(vec![bad_l]);
        dirs.push(touch_snap(&root, &s.id)?);
        register(&srv, &s).await?;
        srv.retire_snapshot(s.id).await?;
    }
    assert!(srv.collect_garbage(u32::MAX).await.is_err());
    assert_eq!(
        count(&srv, "layers WHERE state = 'deleting'").await?,
        (MAX_STALLED_BATCHES * SNAPSHOT_BATCH_SIZE) as i64
    );
    assert_eq!(
        count(&srv, "snapshots WHERE state = 'retired'").await?,
        SNAPSHOT_BATCH_SIZE as i64
    );

    // Next sweep: recovery stalls (1) and snapshot batch 1 stalls (2), pausing further claims.
    assert!(srv.collect_garbage(u32::MAX).await.is_err());
    assert_eq!(
        count(&srv, "layers WHERE state = 'deleting'").await?,
        total_snaps as i64
    );
    assert!(dirs.iter().all(|d| !d.exists()));
    srv.close().await;
    Ok(())
}

#[tokio::test]
async fn advances_cursor_past_stalled_pages_across_sweeps_and_phases() -> Result<()> {
    let (_tmp, _db, srv, root) = open_sqlite().await?;

    // 1. Two full bad layer pages + 1 healthy tail layer: sweep 1 stalls keeping cursor; sweep 2 reclaims tail.
    let bad_layers = MAX_STALLED_BATCHES * LAYER_BATCH_SIZE;
    let mut bad_dirs = Vec::with_capacity(bad_layers);
    let tail_l = layer(Uuid::from_u128(0x2000 + bad_layers as u128));
    let tail_f = touch_layer(&root, &tail_l)?;
    let mut tx = sqlite_pool(&srv).begin().await?;
    for i in 0..bad_layers {
        let (l, p) = blocking_layer(&root, Uuid::from_u128(0x2000 + i as u128))?;
        bad_dirs.push(p);
        insert_layer(&mut tx, &l, None, "deleting").await?;
    }
    insert_layer(&mut tx, &tail_l, None, "deleting").await?;
    tx.commit().await?;

    assert!(srv.collect_garbage(u32::MAX).await.is_err());
    assert!(tail_f.exists());
    srv.collect_garbage(u32::MAX).await?;
    assert!(!tail_f.exists() && layer_state(&srv, &tail_l).await? == "deleted");

    // 2. Cross-phase short-tail stall (`LAYER_BATCH_SIZE + 1` bad deleting layers + 1 healthy retired snapshot):
    //    sweep 1 stalls on the 1-item tail keeping cursor; sweep 2 confirms end-of-table and reclaims snapshot.
    let short_bad = (MAX_STALLED_BATCHES - 1) * LAYER_BATCH_SIZE + 1;
    for p in &bad_dirs[short_bad..] {
        fs::remove_dir(p)?;
    }
    assert!(srv.collect_garbage(u32::MAX).await.is_err());
    let hl = layer(Uuid::now_v7());
    let hl_f = touch_layer(&root, &hl)?;
    let hs = snap(vec![hl.clone()]);
    let hs_dir = touch_snap(&root, &hs.id)?;
    register(&srv, &hs).await?;
    srv.retire_snapshot(hs.id.clone()).await?;

    assert!(srv.collect_garbage(u32::MAX).await.is_err());
    assert!(hs_dir.exists() && snap_state(&srv, &hs.id).await? == "retired");
    srv.collect_garbage(u32::MAX).await?;
    assert!(!hs_dir.exists() && !hl_f.exists() && snap_state(&srv, &hs.id).await? == "reaped");
    assert_eq!(layer_state(&srv, &hl).await?, "deleted");

    for p in &bad_dirs[..short_bad] {
        fs::remove_dir(p)?;
    }
    srv.collect_garbage(u32::MAX).await?;
    assert_eq!(count(&srv, "layers WHERE state = 'deleting'").await?, 0);

    // 3. Two full bad snapshot pages (512 ENOTDIR regular files) + healthy #513 + wrap-around after fix.
    let bad_snaps = MAX_STALLED_BATCHES * SNAPSHOT_BATCH_SIZE;
    let mut bad_files = Vec::with_capacity(bad_snaps);
    let tail_id = SnapshotId::parse(&Uuid::from_u128(0x4000 + bad_snaps as u128).to_string())?;
    let tail_dir = touch_snap(&root, &tail_id)?;
    let mut tx = sqlite_pool(&srv).begin().await?;
    for i in 0..bad_snaps {
        let id = SnapshotId::parse(&Uuid::from_u128(0x4000 + i as u128).to_string())?;
        bad_files.push(touch_file(root.join(posix_snapshot_dir(&id)))?);
        insert_retired_snap(&mut tx, &id).await?;
    }
    insert_retired_snap(&mut tx, &tail_id).await?;
    tx.commit().await?;

    assert!(srv.collect_garbage(u32::MAX).await.is_err());
    assert!(tail_dir.exists());
    srv.collect_garbage(u32::MAX).await?;
    assert!(!tail_dir.exists() && snap_state(&srv, &tail_id).await? == "reaped");
    for p in &bad_files {
        fs::remove_file(p)?;
    }
    srv.collect_garbage(u32::MAX).await?;
    assert_eq!(count(&srv, "snapshots WHERE state = 'retired'").await?, 0);
    srv.close().await;
    Ok(())
}

#[tokio::test]
async fn postgres_gc_waits_for_concurrent_publisher_and_excludes_second_leader() -> Result<()> {
    let Ok(url) = std::env::var("AENV_TEST_DATABASE_URL") else {
        return Ok(());
    };
    let _guard = PG_GC_TEST_LOCK.lock().await;
    let db = DatabaseConfig::Postgres { url: url.clone() };
    db.migrate().await?;
    let root = tempfile::tempdir()?;
    let srv = MetaServer::open(&db)
        .await?
        .with_gc_storage(Arc::new(PosixTestGcStorage(root.path().to_owned())));

    // Part A: Publisher holds `FOR SHARE` on `parent` while GC deletes `leaf` and blocks on
    // `FOR UPDATE` for `parent`; committing a child under `parent` rescues `parent`.
    let [parent, leaf, child] = std::array::from_fn(|_| layer(Uuid::now_v7()));
    let retired = snap(vec![parent.clone(), leaf.clone()]);
    register(&srv, &retired).await?;
    srv.retire_snapshot(retired.id.clone()).await?;
    let (parent_f, leaf_f) = (
        touch_layer(root.path(), &parent)?,
        touch_layer(root.path(), &leaf)?,
    );
    let retired_dir = touch_snap(root.path(), &retired.id)?;

    let DatabasePool::Postgres(pool) = &srv.database else {
        unreachable!()
    };
    let mut pub_conn = pool.acquire().await?;
    let pub_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *pub_conn)
        .await?;
    let mut pub_tx = pub_conn.begin().await?;
    let parent_id = parent.uuid.as_deref().unwrap();
    let state: String =
        sqlx::query_scalar("SELECT state FROM layers WHERE uuid = $1::uuid FOR SHARE")
            .bind(parent_id)
            .fetch_one(&mut *pub_tx)
            .await?;
    assert_eq!(state, "available");

    let gc_srv = srv.clone();
    let gc_task = tokio::spawn(async move { gc_srv.collect_garbage(u32::MAX).await });

    let mut blocked = false;
    for _ in 0..500 {
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))",
        )
        .bind(pub_pid)
        .fetch_one(pool)
        .await?
        {
            blocked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(blocked, "GC did not block on publisher's FOR SHARE lock");

    let rescuer = snap(vec![parent.clone(), child.clone()]);
    sqlx::query(
        "INSERT INTO snapshots \
         (snapshot_id, source_kind, alias, state, placement, owner_node_id, source_sandbox_id, created_at, updated_at) \
         VALUES ($1::uuid, 'sandbox', NULL, 'active', 'distributed', NULL, 'rescuer-sbx', $2, $3)",
    )
    .bind(rescuer.id.to_string())
    .bind(rescuer.created_at_unix_ms)
    .bind(rescuer.updated_at_unix_ms)
    .execute(&mut *pub_tx)
    .await?;
    sqlx::query(
        "INSERT INTO layers (uuid, managed_parent_uuid, digest, state) \
         VALUES ($1::uuid, $2::uuid, $3, 'available')",
    )
    .bind(child.uuid.as_deref().unwrap())
    .bind(parent_id)
    .bind(&child.digest)
    .execute(&mut *pub_tx)
    .await?;
    sqlx::query(
        "INSERT INTO layer_heads (owner_kind, owner_id, layer_uuid, released) \
         VALUES ('snapshot', $1, $2::uuid, FALSE)",
    )
    .bind(rescuer.id.to_string())
    .bind(child.uuid.as_deref().unwrap())
    .execute(&mut *pub_tx)
    .await?;
    pub_tx.commit().await?;
    drop(pub_conn);

    gc_task.await??;
    assert!(!retired_dir.exists() && !leaf_f.exists() && parent_f.exists());
    assert_eq!(layer_state(&srv, &leaf).await?, "deleted");
    assert_eq!(layer_state(&srv, &parent).await?, "available");
    srv.retire_snapshot(rescuer.id).await?;

    // Part B: Second instance skips GC without moving `cursor` while advisory lock is held.
    let locked_l = layer(Uuid::now_v7());
    let locked_s = snap(vec![locked_l.clone()]);
    register(&srv, &locked_s).await?;
    srv.retire_snapshot(locked_s.id.clone()).await?;
    let (locked_dir, locked_f) = (
        touch_snap(root.path(), &locked_s.id)?,
        touch_layer(root.path(), &locked_l)?,
    );

    let mut leader = pool.acquire().await?;
    leader.close_on_drop();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(ADVISORY_LOCK_KEY)
            .fetch_one(&mut *leader)
            .await?
    );

    let srv2 = MetaServer::open(&DatabaseConfig::Postgres { url })
        .await?
        .with_gc_storage(Arc::new(PosixTestGcStorage(root.path().to_owned())));
    let expected = GcCursor {
        after_layer: Some(Uuid::nil().to_string()),
        after_snapshot: Some(Uuid::nil().to_string()),
        after_head: Some((
            "volume".to_owned(),
            "vol_0".to_owned(),
            Uuid::nil().to_string(),
        )),
    };
    *srv2.gc_cursor.lock().await = expected.clone();
    let skipped = srv2.collect_garbage(u32::MAX).await?;
    assert_eq!(skipped.reaped_snapshots, 0);
    assert_eq!(skipped.deleted_layers, 0);
    assert!(locked_dir.exists() && locked_f.exists());
    assert_eq!(*srv2.gc_cursor.lock().await, expected);

    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(ADVISORY_LOCK_KEY)
        .execute(&mut *leader)
        .await?;
    leader.close().await?;

    srv2.collect_garbage(u32::MAX).await?;
    assert!(
        !locked_dir.exists()
            && !locked_f.exists()
            && layer_state(&srv2, &locked_l).await? == "deleted"
    );
    srv2.close().await;
    srv.close().await;
    Ok(())
}
