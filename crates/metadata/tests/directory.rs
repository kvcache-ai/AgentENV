use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use agentenv_domain::{
    BuildErrorReason, BuildStatus, BuildTarget, ExternalLayer, ImageTag, LayerRef, ManagedLayerRef,
    PublishedImage, ResourceKind, SandboxResources, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotManifest, SnapshotMetadata, SnapshotPlacement, SnapshotSourceKind, SnapshotState,
    SnapshotVisibility, SnapshotVolume, VolumeMode, VolumePurpose, VolumeRecord, VolumeStatus,
};
use agentenv_metadata::{
    context::current, transport, DatabaseConfig, GcStorage, LayerState, MetaClient, MetaError,
    MetaServer,
};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use sqlx::{ConnectOptions, Connection};
use uuid::Uuid;

#[derive(Default)]
struct RecordingGcStorage {
    deleted_snapshots: Mutex<Vec<SnapshotId>>,
    deleted_layers: Mutex<Vec<String>>,
    failing_layers: Mutex<BTreeSet<String>>,
}

impl RecordingGcStorage {
    fn take_snapshots(&self) -> Vec<SnapshotId> {
        std::mem::take(&mut *self.deleted_snapshots.lock().unwrap())
    }

    fn take_layers(&self) -> Vec<String> {
        std::mem::take(&mut *self.deleted_layers.lock().unwrap())
    }

    fn fail_layer(&self, digest: &str) {
        self.failing_layers
            .lock()
            .unwrap()
            .insert(digest.to_owned());
    }

    fn clear_failing_layer(&self, digest: &str) {
        self.failing_layers.lock().unwrap().remove(digest);
    }
}

#[async_trait]
impl GcStorage for RecordingGcStorage {
    async fn delete_snapshot(&self, id: &SnapshotId) -> Result<()> {
        self.deleted_snapshots.lock().unwrap().push(id.clone());
        Ok(())
    }

    async fn delete_layer(&self, digest: &str) -> Result<()> {
        if self.failing_layers.lock().unwrap().contains(digest) {
            anyhow::bail!("injected delete failure for {digest}");
        }
        self.deleted_layers.lock().unwrap().push(digest.to_owned());
        Ok(())
    }
}

async fn sql_rows(database: &DatabaseConfig, statement: &str) -> Result<Vec<String>> {
    let query = sqlx::AssertSqlSafe(statement);
    match database {
        DatabaseConfig::Postgres { url } => {
            let mut connection = sqlx::PgConnection::connect(url).await?;
            Ok(sqlx::query_scalar(query).fetch_all(&mut connection).await?)
        }
        DatabaseConfig::Sqlite { path } => {
            let mut connection = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .connect()
                .await?;
            Ok(sqlx::query_scalar(query).fetch_all(&mut connection).await?)
        }
    }
}

fn layer(digit: char) -> ManagedLayerRef {
    ManagedLayerRef {
        uuid: Some(SnapshotId::generate().to_string()),
        digest: format!("sha256:{}", digit.to_string().repeat(64)),
        size: 4096,
    }
}

#[derive(Clone)]
struct TestSnapshot {
    metadata: SnapshotMetadata,
    volume_snapshots: Vec<SnapshotVolume>,
    manifest: SnapshotManifest,
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn snapshot(rootfs_layers: Vec<LayerRef>) -> TestSnapshot {
    let now = now_unix_ms();
    TestSnapshot {
        metadata: SnapshotMetadata {
            id: SnapshotId::generate(),
            source_kind: SnapshotSourceKind::Sandbox,
            visibility: SnapshotVisibility::Public,
            alias: None,
            state: SnapshotState::Active,
            placement: Some(SnapshotPlacement::Distributed),
            owner_node_id: None,
            source_sandbox_id: Some("sandbox-source".into()),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        },
        volume_snapshots: Vec::new(),
        manifest: SnapshotManifest {
            rootfs_layers,
            ..SnapshotManifest::mock()
        },
    }
}

fn promoted_record(local: &TestSnapshot, committed: SnapshotManifest) -> TestSnapshot {
    let mut record = local.clone();
    record.metadata.placement = Some(SnapshotPlacement::Distributed);
    record.metadata.owner_node_id = None;
    record.metadata.updated_at_unix_ms += 1000;
    record.manifest = committed;
    record
}

#[tokio::test]
async fn published_snapshots_and_unified_resources_survive_reopen() -> Result<()> {
    let root = tempfile::tempdir()?;
    let sqlite = DatabaseConfig::Sqlite {
        path: root.path().join("metadata.sqlite3"),
    };
    Box::pin(exercise_directory(&sqlite)).await?;
    if let Ok(url) = std::env::var("AENV_TEST_DATABASE_URL") {
        Box::pin(exercise_directory(&DatabaseConfig::Postgres { url })).await?;
    }
    Ok(())
}

async fn exercise_directory(database: &DatabaseConfig) -> Result<()> {
    database.migrate().await?;
    let server = MetaServer::open(database).await?;
    let client = transport::embedded(server.clone());
    let register = |record: TestSnapshot| {
        client.register_snapshot(
            current(),
            record.metadata,
            record.volume_snapshots,
            record.manifest,
            Vec::new(),
        )
    };

    let [base, left, right] = [layer('a'), layer('b'), layer('c')];
    let external = LayerRef::External(ExternalLayer {
        digest: format!("sha256:{}", "e".repeat(64)),
        size: 4096,
        repo_blob_url: "https://registry.example/v2/image/blobs".into(),
    });
    let mut first = snapshot(vec![
        LayerRef::Managed(base.clone()),
        external,
        LayerRef::Managed(left.clone()),
    ]);
    let alias = format!("snap_{}", first.metadata.id);
    first.metadata.alias = Some(SnapshotAlias::parse(&alias)?);
    let source_sandbox_id = "sandbox-source-one";
    first.metadata.source_sandbox_id = Some(source_sandbox_id.into());
    let mut second = snapshot(vec![
        LayerRef::Managed(base.clone()),
        LayerRef::Managed(right.clone()),
    ]);
    second.metadata.alias = Some(SnapshotAlias::parse(&format!(
        "snap-{}",
        second.metadata.id
    ))?);

    let (published_left, published_right) =
        tokio::join!(register(first.clone()), register(second.clone()),);
    published_left??;
    published_right??;
    let base_id = base.uuid.as_deref().unwrap();
    for (record, head) in [(&first, &left), (&second, &right)] {
        let chain = sql_rows(
            database,
            &format!(
                "SELECT CAST(tip.uuid AS TEXT) || '/' || COALESCE(CAST(tip.managed_parent_uuid AS TEXT), '')
                 FROM layer_heads h JOIN layers tip ON tip.uuid = h.layer_uuid
                 WHERE h.owner_kind = 'snapshot' AND h.owner_id = '{}' AND NOT h.released",
                record.metadata.id
            ),
        )
        .await?;
        let head_id = head.uuid.as_deref().unwrap();
        assert_eq!(chain, [format!("{head_id}/{base_id}")]);
    }

    let mut alias_conflict = first.clone();
    alias_conflict.metadata.id = SnapshotId::generate();
    assert!(matches!(
        register(alias_conflict.clone()).await?,
        Err(MetaError::AliasConflict { existing, .. }) if existing == first.metadata.id
    ));

    // A non-available parent must roll back the new snapshot as well.
    sql_rows(
        database,
        &format!("UPDATE layers SET state = 'deleting' WHERE uuid = '{base_id}' RETURNING state"),
    )
    .await?;
    let blocked = snapshot(vec![LayerRef::Managed(base), LayerRef::Managed(layer('d'))]);
    assert!(matches!(
        register(blocked.clone()).await?,
        Err(MetaError::Unavailable { .. })
    ));
    assert!(client
        .get_snapshot(current(), blocked.metadata.id.to_string())
        .await??
        .is_none());

    let mut local = snapshot(Vec::new());
    local.metadata.placement = Some(SnapshotPlacement::Local);
    local.metadata.owner_node_id = Some("node-one".into());
    local.manifest = SnapshotManifest::default();
    local.metadata.created_at_unix_ms = first.metadata.created_at_unix_ms + 1;
    local.metadata.updated_at_unix_ms = local.metadata.created_at_unix_ms;
    register(local.clone()).await??;
    let blocked_promotion = promoted_record(&local, first.manifest.clone());
    assert!(matches!(
        client
            .promote_snapshot(
                current(),
                blocked_promotion.metadata.clone(),
                blocked_promotion.volume_snapshots.clone(),
                blocked_promotion.manifest.clone(),
                Vec::new(),
            )
            .await?,
        Err(MetaError::Unavailable { .. })
    ));
    drop(client);
    server.close().await;

    let server = MetaServer::open(database).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let serving = tokio::spawn(transport::serve_tcp(listener, server.clone(), async move {
        let _ = stopped.await;
        Ok(())
    }));
    let client = transport::connect(&address).await?;
    let register = |record: TestSnapshot| {
        client.register_snapshot(
            current(),
            record.metadata,
            record.volume_snapshots,
            record.manifest,
            Vec::new(),
        )
    };
    let entry = client.get_snapshot(current(), alias).await??.unwrap();
    assert_eq!(entry, first.metadata);

    let alias_filter = SnapshotListFilter {
        alias_prefix: Some("snap_".into()),
        ..SnapshotListFilter::default()
    };
    let source_filter = SnapshotListFilter::sandbox_snapshots(Some(source_sandbox_id.into()), None);
    let mut distributed_ids = [&first.metadata.id, &second.metadata.id];
    distributed_ids.sort_unstable();
    for (filter, expected) in [
        (
            SnapshotListFilter::default(),
            vec![&local.metadata.id, distributed_ids[0], distributed_ids[1]],
        ),
        (alias_filter, vec![&first.metadata.id]),
        (source_filter.clone(), vec![&first.metadata.id]),
        (
            SnapshotListFilter {
                snapshot_ids: Some(vec![second.metadata.id.clone()]),
                ..source_filter
            },
            vec![],
        ),
        (SnapshotListFilter::by_ids([]), vec![]),
        (
            SnapshotListFilter {
                alias_prefix: Some("bad prefix".into()),
                ..SnapshotListFilter::default()
            },
            vec![],
        ),
        (
            SnapshotListFilter {
                alias_prefix: Some("\0".into()),
                ..SnapshotListFilter::default()
            },
            vec![],
        ),
    ] {
        let listed = client.list_snapshots(current(), filter).await??;
        assert_eq!(
            listed.iter().map(|entry| entry.id()).collect::<Vec<_>>(),
            expected
        );
    }

    let entry = client
        .get_snapshot(current(), local.metadata.id.to_string())
        .await??
        .unwrap();
    assert_eq!(entry, local.metadata);
    exercise_lifecycle(&client, &server, database, &local).await?;
    client
        .retire_snapshot(current(), first.metadata.id.clone())
        .await??;
    let filter =
        SnapshotListFilter::by_ids([first.metadata.id.clone(), second.metadata.id.clone()]);
    let listed = client.list_snapshots(current(), filter).await??;
    assert_eq!(
        listed.iter().map(|entry| entry.id()).collect::<Vec<_>>(),
        [&second.metadata.id]
    );

    let mut replacement = snapshot(Vec::new());
    replacement.metadata.alias = first.metadata.alias.clone();
    register(replacement.clone()).await??;
    client
        .retire_snapshot(current(), first.metadata.id.clone())
        .await??;
    assert_eq!(
        client
            .get_snapshot(
                current(),
                replacement.metadata.alias.as_ref().unwrap().to_string()
            )
            .await??
            .unwrap()
            .id(),
        &replacement.metadata.id
    );

    Box::pin(exercise_unified_resources(&client, database)).await?;

    let _ = stop.send(());
    serving.await??;
    server.close().await;
    Ok(())
}

async fn exercise_lifecycle(
    client: &MetaClient,
    server: &MetaServer,
    database: &DatabaseConfig,
    local: &TestSnapshot,
) -> Result<()> {
    let register = |record: TestSnapshot| {
        client.register_snapshot(
            current(),
            record.metadata,
            record.volume_snapshots,
            record.manifest,
            Vec::new(),
        )
    };
    let head = layer('f');
    let content = SnapshotManifest {
        memory_layers: vec![head.clone()],
        ..SnapshotManifest::mock()
    };
    let mut target = promoted_record(local, content.clone());
    target.metadata.alias = Some(SnapshotAlias::parse(&format!(
        "promoted-{}",
        local.metadata.id
    ))?);
    let expected = target.clone();
    let promote = |record: TestSnapshot| {
        client.promote_snapshot(
            current(),
            record.metadata,
            record.volume_snapshots,
            record.manifest,
            Vec::new(),
        )
    };
    let retire = |id: &SnapshotId| client.retire_snapshot(current(), id.clone());

    let entry = promote(target.clone()).await??.unwrap();
    let mut retry_target = target.clone();
    retry_target.metadata.updated_at_unix_ms += 500;
    retry_target.metadata.source_sandbox_id = Some("retry-source-ignored".into());
    retry_target.manifest.memory_layers.push(layer('0'));
    let retry = promote(retry_target).await??.unwrap();
    assert_eq!(entry, retry);
    assert_eq!(retry, expected.metadata);
    let mut changed_alias = target.clone();
    changed_alias.metadata.alias = Some(SnapshotAlias::parse(&format!(
        "other-{}",
        local.metadata.id
    ))?);
    assert!(matches!(
        promote(changed_alias).await?,
        Err(MetaError::SnapshotConflict { .. })
    ));

    retire(&local.metadata.id).await??;
    retire(&local.metadata.id).await??;
    assert!(matches!(
        promote(target).await?,
        Err(MetaError::Unavailable { .. })
    ));
    let heads = format!(
        "SELECT CAST(layer_uuid AS TEXT) FROM layer_heads WHERE owner_kind = 'snapshot' AND owner_id = '{}'",
        local.metadata.id
    );
    assert_eq!(sql_rows(database, &heads).await?, [head.uuid.unwrap()]);

    let local_record = || {
        let mut record = local.clone();
        record.metadata.id = SnapshotId::generate();
        record
    };
    let late = local_record();
    assert!(promote(promoted_record(&late, content.clone()))
        .await??
        .is_none());
    retire(&late.metadata.id).await??;
    retire(&late.metadata.id).await??;
    assert!(!client
        .get_snapshot(current(), late.metadata.id.to_string())
        .await??
        .unwrap()
        .is_visible());
    assert!(matches!(
        register(late.clone()).await?,
        Err(MetaError::Unavailable { .. })
    ));
    let mut uuid_aliased = snapshot(vec![LayerRef::Managed(layer('9'))]);
    uuid_aliased.metadata.alias = Some(SnapshotAlias::parse(&late.metadata.id.to_string())?);
    register(uuid_aliased.clone()).await??;
    assert_eq!(
        client
            .get_snapshot(current(), late.metadata.id.to_string())
            .await??
            .unwrap()
            .id(),
        &uuid_aliased.metadata.id
    );

    let racing = local_record();
    register(racing.clone()).await??;
    let (promotion, retirement) = tokio::join!(
        promote(promoted_record(&racing, content.clone())),
        retire(&racing.metadata.id),
    );
    retirement??;
    let expected_placement = match promotion? {
        Ok(Some(_)) => SnapshotPlacement::Distributed,
        Err(MetaError::Unavailable { .. }) => SnapshotPlacement::Local,
        other => panic!("unexpected promotion result: {other:?}"),
    };
    let retired = client
        .get_snapshot(current(), racing.metadata.id.to_string())
        .await??
        .unwrap();
    assert!(!retired.is_visible());
    assert_eq!(retired.snapshot_placement()?, expected_placement);

    let candidate = local_record();
    register(candidate.clone()).await??;
    let shared = layer('e');
    let mut registration = snapshot(vec![LayerRef::Managed(shared.clone())]);
    registration.metadata.alias = Some(SnapshotAlias::parse(&format!(
        "contested-{}",
        candidate.metadata.id
    ))?);
    let mut promotion = promoted_record(
        &candidate,
        SnapshotManifest {
            memory_layers: vec![shared],
            ..SnapshotManifest::mock()
        },
    );
    promotion.metadata.alias = registration.metadata.alias.clone();
    let (registered, promoted) = tokio::join!(register(registration.clone()), promote(promotion),);
    assert!(matches!(
        (registered?, promoted?),
        (Ok(_), Err(MetaError::AliasConflict { .. }))
            | (Err(MetaError::AliasConflict { .. }), Ok(Some(_)))
    ));
    Box::pin(exercise_pins_and_gc(client, server, database)).await
}

async fn exercise_pins_and_gc(
    client: &MetaClient,
    server: &MetaServer,
    database: &DatabaseConfig,
) -> Result<()> {
    let storage = Arc::new(RecordingGcStorage::default());
    let gc_server = server.clone().with_gc_storage(Arc::clone(&storage) as _);
    let register = |record: TestSnapshot| {
        Box::pin(client.register_snapshot(
            current(),
            record.metadata,
            record.volume_snapshots,
            record.manifest,
            Vec::new(),
        ))
    };
    let retire = |id: &SnapshotId| Box::pin(client.retire_snapshot(current(), id.clone()));

    // Drain any retired snapshots left over from earlier lifecycle steps.
    gc_server.collect_garbage(64).await?;
    storage.take_snapshots();
    storage.take_layers();

    // Retired Local snapshot with managed volume layer reaps the layer without calling delete_snapshot.
    let local_vol_layer = layer('1');
    let mut local = snapshot(Vec::new());
    local.metadata.placement = Some(SnapshotPlacement::Local);
    local.metadata.owner_node_id = Some("local-owner".into());
    local.volume_snapshots.push(SnapshotVolume {
        mount_path: "/data".into(),
        mode: VolumeMode::Exclusive,
        size_mb: 32,
        layers: vec![LayerRef::Managed(local_vol_layer.clone())],
    });
    register(local.clone()).await??;
    retire(&local.metadata.id).await??;
    let report = gc_server.collect_garbage(64).await?;
    assert_eq!(report.reaped_snapshots, 1);
    assert_eq!(report.deleted_layers, 1);
    assert!(storage.take_snapshots().is_empty());
    assert_eq!(
        storage.take_layers(),
        std::slice::from_ref(&local_vol_layer.digest)
    );
    let reaped_local = client
        .get_snapshot(current(), local.metadata.id.to_string())
        .await??
        .unwrap();
    assert_eq!(reaped_local.state, SnapshotState::Reaped);
    assert_eq!(reaped_local.placement, None);
    assert_eq!(reaped_local.owner_node_id, None);

    // Pin/release tombstones, wave-by-wave leaf layer GC, failure recovery, and revival.
    let [layer_x, layer_y, layer_z] = [layer('7'), layer('8'), layer('6')];
    let s0 = snapshot(vec![
        LayerRef::Managed(layer_x.clone()),
        LayerRef::Managed(layer_y.clone()),
    ]);
    let s1 = snapshot(vec![
        LayerRef::Managed(layer_x.clone()),
        LayerRef::Managed(layer_y.clone()),
        LayerRef::Managed(layer_z.clone()),
    ]);

    // Release arriving before registration/pin records a tombstone that blocks a late pin.
    let tombstone_pin = Uuid::now_v7();
    client
        .release_pin(
            current(),
            ResourceKind::Snapshot,
            s0.metadata.id.to_string(),
            tombstone_pin,
        )
        .await??;
    register(s0.clone()).await??;
    register(s1.clone()).await??;
    assert!(matches!(
        client
            .pin_snapshot(
                current(),
                s0.metadata.id.clone(),
                tombstone_pin,
                Some("node-one".into()),
            )
            .await?,
        Err(MetaError::Unavailable { .. })
    ));

    let active_pin = Uuid::now_v7();
    let pinned = client
        .pin_snapshot(
            current(),
            s0.metadata.id.clone(),
            active_pin,
            Some("node-one".into()),
        )
        .await??
        .unwrap();
    assert_eq!(pinned.id(), &s0.metadata.id);
    // Repeated pin with the same pin_id is idempotent.
    assert!(client
        .pin_snapshot(
            current(),
            s0.metadata.id.clone(),
            active_pin,
            Some("node-one".into()),
        )
        .await??
        .is_some());
    assert_eq!(
        sql_rows(
            database,
            &format!("SELECT owner_node_id FROM gc_pins WHERE pin_id = '{active_pin}'")
        )
        .await?,
        ["node-one"]
    );

    // Retiring s0 blocks new pins, but active_pin prevents reaping s0's heads.
    retire(&s0.metadata.id).await??;
    assert!(matches!(
        client
            .pin_snapshot(
                current(),
                s0.metadata.id.clone(),
                Uuid::now_v7(),
                Some("node-one".into()),
            )
            .await?,
        Err(MetaError::Unavailable { .. })
    ));
    let report = gc_server.collect_garbage(64).await?;
    assert_eq!(report.reaped_snapshots, 0);
    assert!(storage.take_snapshots().is_empty());

    // Releasing active_pin with a mismatched snapshot_id must be rejected and leave s0 pinned.
    assert!(matches!(
        client
            .release_pin(
                current(),
                ResourceKind::Snapshot,
                s1.metadata.id.to_string(),
                active_pin,
            )
            .await?,
        Err(MetaError::InvalidRequest { .. })
    ));
    assert_eq!(gc_server.collect_garbage(64).await?.reaped_snapshots, 0);

    // Releasing active_pin makes s0 reapable; s1 still retains z -> y -> x.
    client
        .release_pin(
            current(),
            ResourceKind::Snapshot,
            s0.metadata.id.to_string(),
            active_pin,
        )
        .await??;
    let report = gc_server.collect_garbage(64).await?;
    assert_eq!(report.reaped_snapshots, 1);
    assert_eq!(report.deleted_layers, 0);
    assert_eq!(
        storage.take_snapshots().as_slice(),
        std::slice::from_ref(&s0.metadata.id)
    );
    assert!(storage.take_layers().is_empty());

    // Inject a failure on layer_y so wave 1 deletes z, wave 2 stalls leaving y in deleting
    // (which protects x), and the next sweep recovers y and then deletes x.
    storage.fail_layer(&layer_y.digest);
    retire(&s1.metadata.id).await??;
    assert!(gc_server.collect_garbage(64).await.is_err());
    assert_eq!(
        storage.take_snapshots().as_slice(),
        std::slice::from_ref(&s1.metadata.id)
    );
    assert_eq!(
        storage.take_layers().as_slice(),
        std::slice::from_ref(&layer_z.digest)
    );
    let mid_checks = server
        .check_layers(&[
            layer_z.uuid.clone().unwrap(),
            layer_y.uuid.clone().unwrap(),
            layer_x.uuid.clone().unwrap(),
        ])
        .await?;
    assert_eq!(mid_checks[0].state, LayerState::Deleted);
    assert_eq!(mid_checks[1].state, LayerState::Deleting);
    assert_eq!(mid_checks[2].state, LayerState::Available);

    storage.clear_failing_layer(&layer_y.digest);
    let recovery = gc_server.collect_garbage(64).await?;
    assert_eq!(recovery.reaped_snapshots, 0);
    assert_eq!(recovery.deleted_layers, 2);
    assert_eq!(
        storage.take_layers(),
        [layer_y.digest.clone(), layer_x.digest.clone()]
    );

    let checks = server
        .check_layers(&[
            layer_x.uuid.clone().unwrap(),
            "01010101-0101-0101-0101-010101010101".into(),
        ])
        .await?;
    assert_eq!(checks[0].state, LayerState::Deleted);
    assert_eq!(checks[1].state, LayerState::Missing);

    // Deleted layers become available in the same transaction as their new heads.
    let s2 = snapshot(vec![LayerRef::Managed(layer_x.clone())]);
    let Err(MetaError::MissingLayers { uuids }) = register(s2.clone()).await? else {
        anyhow::bail!("publication must require the deleted layer's UUID");
    };
    assert_eq!(uuids, vec![layer_x.uuid.clone().unwrap()]);
    client
        .register_snapshot(
            current(),
            s2.metadata,
            s2.volume_snapshots,
            s2.manifest,
            uuids,
        )
        .await??;

    // Retired Distributed snapshot with only external layers (no managed heads) is still reapable.
    let mut ext_only = snapshot(vec![LayerRef::External(ExternalLayer {
        digest: format!("sha256:{}", "9".repeat(64)),
        size: 4096,
        repo_blob_url: "https://registry.example/v2/image/blobs".into(),
    })]);
    ext_only.metadata.alias = Some(SnapshotAlias::parse(&format!(
        "reaped-{}",
        ext_only.metadata.id
    ))?);
    register(ext_only.clone()).await??;
    retire(&ext_only.metadata.id).await??;
    let ext_gc = gc_server.collect_garbage(64).await?;
    assert_eq!(ext_gc.reaped_snapshots, 1);
    assert_eq!(
        storage.take_snapshots().as_slice(),
        std::slice::from_ref(&ext_only.metadata.id)
    );
    assert_eq!(gc_server.collect_garbage(64).await?.reaped_snapshots, 0);
    let reaped = client
        .get_snapshot(current(), ext_only.metadata.id.to_string())
        .await??
        .unwrap();
    assert!(!reaped.is_visible());
    assert!(reaped.source_sandbox_id().is_some());
    let alias = ext_only.metadata.alias.as_ref().unwrap().to_string();
    assert!(client
        .get_snapshot(current(), alias.clone())
        .await??
        .is_none());
    let mut new_owner = snapshot(Vec::new());
    new_owner.metadata.alias = ext_only.metadata.alias.clone();
    register(new_owner.clone()).await??;
    retire(&ext_only.metadata.id).await??;
    assert_eq!(
        client.get_snapshot(current(), alias).await??.unwrap().id(),
        &new_owner.metadata.id
    );
    let mut uuid_alias = snapshot(Vec::new());
    uuid_alias.metadata.alias = Some(SnapshotAlias::parse(&ext_only.metadata.id.to_string())?);
    register(uuid_alias).await??;
    assert_eq!(
        client
            .get_snapshot(current(), ext_only.metadata.id.to_string())
            .await??
            .unwrap()
            .id(),
        &ext_only.metadata.id
    );
    Ok(())
}

async fn exercise_unified_resources(client: &MetaClient, database: &DatabaseConfig) -> Result<()> {
    // 1. Internal builder snapshot visibility isolation.
    let builder_alias = format!("builder-{}", Uuid::now_v7().simple());
    let builder_meta = SnapshotMetadata {
        id: SnapshotId::generate(),
        source_kind: SnapshotSourceKind::Builder,
        visibility: SnapshotVisibility::Internal,
        alias: Some(SnapshotAlias::parse(&builder_alias)?),
        state: SnapshotState::Active,
        placement: Some(SnapshotPlacement::Distributed),
        owner_node_id: None,
        source_sandbox_id: None,
        created_at_unix_ms: now_unix_ms(),
        updated_at_unix_ms: now_unix_ms(),
    };
    client
        .register_snapshot(
            current(),
            builder_meta.clone(),
            Vec::new(),
            SnapshotManifest::mock(),
            Vec::new(),
        )
        .await??;
    assert!(client
        .get_snapshot(current(), builder_alias.clone())
        .await??
        .is_none());
    assert!(client
        .get_snapshot(current(), builder_meta.id.to_string())
        .await??
        .is_none());
    assert_eq!(
        client
            .get_snapshot_with_visibility(
                current(),
                builder_alias.clone(),
                SnapshotVisibility::Internal,
            )
            .await??
            .unwrap()
            .id(),
        &builder_meta.id
    );
    let builders = client
        .list_snapshots(current(), SnapshotListFilter::builders())
        .await??;
    assert!(builders.iter().any(|entry| entry.id() == &builder_meta.id));
    let public = client
        .list_snapshots(current(), SnapshotListFilter::default())
        .await??;
    assert!(public.iter().all(|entry| entry.id() != &builder_meta.id));

    // 2. Volumes, cloning, reservations, and build-cache seed rotation.
    let vol_layer = layer('2');
    let vol_suffix = Uuid::now_v7().simple().to_string();
    let user_vol = VolumeRecord {
        id: format!("vol_u_{vol_suffix}"),
        name: format!("data-{vol_suffix}"),
        purpose: VolumePurpose::User,
        mode: VolumeMode::Exclusive,
        size_mb: 256,
        status: VolumeStatus::Ready,
        reserved_by_sandbox_id: None,
        backing_image_config: None,
        backing_layers: vec![LayerRef::Managed(vol_layer.clone())],
        read_only_mounts: Vec::new(),
        deleting: false,
    };
    client
        .create_volume(current(), user_vol.clone(), None, Vec::new())
        .await??;
    assert_eq!(
        client
            .get_volume(current(), user_vol.name.clone())
            .await??
            .unwrap()
            .id,
        user_vol.id
    );
    assert!(matches!(
        client
            .create_volume(
                current(),
                VolumeRecord {
                    id: format!("vol_dup_{vol_suffix}"),
                    ..user_vol.clone()
                },
                None,
                Vec::new(),
            )
            .await?,
        Err(MetaError::VolumeNameConflict { .. })
    ));

    // Clone into a read-only build cache seed and rotate seeds.
    let seed1 = VolumeRecord {
        id: format!("vol_s1_{vol_suffix}"),
        name: format!("seed1-{vol_suffix}"),
        purpose: VolumePurpose::BuildCacheWork,
        mode: VolumeMode::ReadOnly,
        size_mb: 256,
        status: VolumeStatus::Ready,
        reserved_by_sandbox_id: None,
        backing_image_config: None,
        backing_layers: Vec::new(),
        read_only_mounts: Vec::new(),
        deleting: false,
    };
    let created_seed1 = client
        .create_volume(
            current(),
            seed1.clone(),
            Some(user_vol.id.clone()),
            Vec::new(),
        )
        .await??;
    assert_eq!(created_seed1.backing_layers, user_vol.backing_layers);
    let state1 = client
        .replace_build_cache_head(current(), seed1.id.clone())
        .await??;
    assert_eq!(state1.current.as_deref(), Some(seed1.id.as_str()));
    assert!(matches!(
        client.delete_volume(current(), seed1.id.clone()).await?,
        Err(MetaError::Unavailable { .. })
    ));

    // Public list hides internal build-cache volumes.
    let user_page = client
        .list_volumes_page(current(), None, 100, false)
        .await??;
    assert!(user_page.records.iter().any(|v| v.id == user_vol.id));
    assert!(user_page.records.iter().all(|v| v.id != seed1.id));

    let seed2 = VolumeRecord {
        id: format!("vol_s2_{vol_suffix}"),
        name: format!("seed2-{vol_suffix}"),
        ..seed1.clone()
    };
    client
        .create_volume(current(), seed2.clone(), Some(seed1.id.clone()), Vec::new())
        .await??;
    let state2 = client
        .replace_build_cache_head(current(), seed2.id.clone())
        .await??;
    assert_eq!(state2.current.as_deref(), Some(seed2.id.as_str()));
    assert!(state2.retired.contains(&seed1.id));
    client
        .reserve_read_only_volume(current(), seed1.id.clone(), "sbx-ro".into())
        .await??;
    assert!(matches!(
        client.delete_volume(current(), seed1.id.clone()).await?,
        Err(MetaError::VolumeReserved { .. })
    ));
    client
        .replace_volume_owner(current(), seed1.id.clone(), "sbx-ro".into(), None)
        .await??;
    client.delete_volume(current(), seed1.id.clone()).await??;

    // Exclusive reservation & transfer.
    assert_eq!(
        client
            .reserve_volume(current(), user_vol.id.clone(), "sbx-1".into())
            .await??,
        None
    );
    assert_eq!(
        client
            .reserve_volume(current(), user_vol.id.clone(), "sbx-2".into())
            .await??
            .as_deref(),
        Some("sbx-1")
    );
    client
        .replace_volume_owner(
            current(),
            user_vol.id.clone(),
            "sbx-1".into(),
            Some("sbx-2".into()),
        )
        .await??;
    assert_eq!(
        client
            .get_volume(current(), user_vol.id.clone())
            .await??
            .unwrap()
            .reserved_by_sandbox_id
            .as_deref(),
        Some("sbx-2")
    );
    client
        .replace_volume_owner(current(), user_vol.id.clone(), "sbx-2".into(), None)
        .await??;

    // 3. Published images & image tags.
    let img_layer = layer('3');
    let image = PublishedImage::new(
        "amd64".into(),
        vec![LayerRef::Managed(img_layer.clone())],
        json!({"Env": ["PATH=/usr/bin"]}),
    );
    let digest = client
        .put_image(current(), image.clone(), Vec::new())
        .await??;
    assert_eq!(
        client.get_image(current(), digest.clone()).await??,
        Some(image)
    );
    let image_heads = sql_rows(
        database,
        &format!(
            "SELECT CAST(layer_uuid AS TEXT) FROM layer_heads WHERE owner_kind = 'image' AND owner_id = '{digest}'"
        ),
    )
    .await?;
    assert_eq!(image_heads, [img_layer.uuid.unwrap()]);
    let tag = ImageTag::parse(&format!("app-{vol_suffix}:v1"))?;
    client
        .put_image_tag(current(), tag.clone(), digest.clone(), None)
        .await??;
    let tag_record = client
        .get_image_tag(current(), tag.clone())
        .await??
        .unwrap();
    assert_eq!(tag_record.image_digest, digest);
    assert_eq!(tag_record.source_manifest_digest, None);

    // 4. Build jobs: concurrent same-alias template builds, atomic finish on register, and tombstone delete.
    let contested_alias = SnapshotAlias::parse(&format!("tpl-{vol_suffix}"))?;
    let target = BuildTarget::Template {
        alias: Some(contested_alias.clone()),
        resources: SandboxResources::default(),
    };
    let job_a = SnapshotId::generate();
    let job_b = SnapshotId::generate();
    let rec_a = client
        .create_build_job(current(), job_a.clone(), target.clone())
        .await??;
    let rec_b = client
        .create_build_job(current(), job_b.clone(), target.clone())
        .await??;
    assert_eq!(rec_a.status, BuildStatus::Waiting);
    assert_eq!(rec_b.status, BuildStatus::Waiting);
    assert!(client.start_build_job(current(), job_a.clone()).await??);
    assert!(!client.start_build_job(current(), job_a.clone()).await??);
    assert!(client.start_build_job(current(), job_b.clone()).await??);

    let tpl_meta_a = SnapshotMetadata {
        id: job_a.clone(),
        source_kind: SnapshotSourceKind::Template,
        visibility: SnapshotVisibility::Public,
        alias: Some(contested_alias.clone()),
        state: SnapshotState::Active,
        placement: Some(SnapshotPlacement::Distributed),
        owner_node_id: None,
        source_sandbox_id: None,
        created_at_unix_ms: now_unix_ms(),
        updated_at_unix_ms: now_unix_ms(),
    };
    client
        .register_snapshot(
            current(),
            tpl_meta_a.clone(),
            Vec::new(),
            SnapshotManifest::mock(),
            Vec::new(),
        )
        .await??;
    let finished_a = client
        .get_build_job(current(), job_a.clone())
        .await??
        .unwrap();
    assert_eq!(finished_a.status, BuildStatus::Ready);
    assert_eq!(finished_a.result_snapshot_id, Some(job_a.clone()));

    let mut tpl_meta_b = tpl_meta_a;
    tpl_meta_b.id = job_b.clone();
    assert!(matches!(
        client
            .register_snapshot(
                current(),
                tpl_meta_b,
                Vec::new(),
                SnapshotManifest::mock(),
                Vec::new(),
            )
            .await?,
        Err(MetaError::AliasConflict { existing, .. }) if existing == job_a
    ));
    let failed_b = client
        .fail_build_job(
            current(),
            job_b.clone(),
            BuildErrorReason::with_step("alias conflict", "commit"),
        )
        .await??
        .unwrap();
    assert_eq!(failed_b.status, BuildStatus::Error);
    assert_eq!(
        failed_b
            .error_reason
            .as_ref()
            .and_then(|r| r.step.as_deref()),
        Some("commit")
    );

    // Fast-fail once an active template owns the alias.
    assert!(matches!(
        client
            .create_build_job(current(), SnapshotId::generate(), target)
            .await?,
        Err(MetaError::AliasConflict { existing, .. }) if existing == job_a
    ));

    // Deleting an in-flight template build lays a `reserved` tombstone in `snapshots`.
    let cancelled = SnapshotId::generate();
    client
        .create_build_job(
            current(),
            cancelled.clone(),
            BuildTarget::Template {
                alias: None,
                resources: SandboxResources::default(),
            },
        )
        .await??;
    assert!(
        client
            .delete_build_job(current(), cancelled.clone(), true)
            .await??
    );
    assert!(client
        .get_build_job(current(), cancelled.clone())
        .await??
        .is_none());
    let late_meta = SnapshotMetadata {
        id: cancelled,
        source_kind: SnapshotSourceKind::Template,
        visibility: SnapshotVisibility::Public,
        alias: None,
        state: SnapshotState::Active,
        placement: Some(SnapshotPlacement::Distributed),
        owner_node_id: None,
        source_sandbox_id: None,
        created_at_unix_ms: now_unix_ms(),
        updated_at_unix_ms: now_unix_ms(),
    };
    assert!(matches!(
        client
            .register_snapshot(
                current(),
                late_meta,
                Vec::new(),
                SnapshotManifest::mock(),
                Vec::new(),
            )
            .await?,
        Err(MetaError::Unavailable { .. })
    ));

    // Image build job lifecycle.
    let img_job = SnapshotId::generate();
    client
        .create_build_job(
            current(),
            img_job.clone(),
            BuildTarget::Image {
                name: format!("app-{vol_suffix}"),
                tags: vec!["v1".into()],
            },
        )
        .await??;
    assert!(client.start_build_job(current(), img_job.clone()).await??);
    let done = client
        .finish_build_job(current(), img_job.clone(), None, Some(digest.clone()))
        .await??
        .unwrap();
    assert_eq!(done.status, BuildStatus::Ready);
    assert_eq!(done.result_image_digest.as_deref(), Some(digest.as_str()));

    Ok(())
}
