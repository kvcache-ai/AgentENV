use agentenv_domain::{
    ExternalLayer, LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotManifest, SnapshotMetadata, SnapshotPlacement,
};
use agentenv_metadata::{
    context::current, transport, DatabaseConfig, LayerState, MetaClient, MetaError, MetaServer,
};
use anyhow::Result;
use sqlx::{ConnectOptions, Connection};

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
    manifest: SnapshotManifest,
}

fn snapshot(rootfs_layers: Vec<LayerRef>) -> TestSnapshot {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    TestSnapshot {
        metadata: SnapshotMetadata {
            id: SnapshotId::generate(),
            alias: None,
            snapshot_type: Some(SnapshotPlacement::Distributed),
            owner_node_id: None,
            source_sandbox_id: Some("sandbox-source".into()),
            retired: false,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        },
        manifest: SnapshotManifest {
            isolated_layers: true,
            rootfs_layers,
            ..SnapshotManifest::mock()
        },
    }
}

fn promoted_record(local: &TestSnapshot, committed: SnapshotManifest) -> TestSnapshot {
    let mut record = local.clone();
    record.metadata.snapshot_type = Some(SnapshotPlacement::Distributed);
    record.metadata.owner_node_id = None;
    record.metadata.updated_at_unix_ms += 1000;
    record.manifest = committed;
    record
}

#[tokio::test]
async fn published_snapshots_share_layers_and_survive_reopen() -> Result<()> {
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
        client.register_snapshot(current(), record.metadata, record.manifest, Vec::new())
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
                 FROM snapshot_heads h JOIN layers tip ON tip.uuid = h.layer_uuid
                 WHERE h.snapshot_id = '{}'",
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
    local.metadata.snapshot_type = Some(SnapshotPlacement::Local);
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
        client.register_snapshot(current(), record.metadata, record.manifest, Vec::new())
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
    exercise_lifecycle(&client, database, &local).await?;
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

    let mut replacement = first;
    replacement.metadata.id = SnapshotId::generate();
    assert!(matches!(
        register(replacement.clone()).await?,
        Err(MetaError::AliasConflict { .. })
    ));

    let _ = stop.send(());
    serving.await??;
    server.close().await;
    Ok(())
}

async fn exercise_lifecycle(
    client: &MetaClient,
    database: &DatabaseConfig,
    local: &TestSnapshot,
) -> Result<()> {
    let register = |record: TestSnapshot| {
        client.register_snapshot(current(), record.metadata, record.manifest, Vec::new())
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
        client.promote_snapshot(current(), record.metadata, record.manifest, Vec::new())
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
        "SELECT CAST(layer_uuid AS TEXT) FROM snapshot_heads WHERE snapshot_id = '{}'",
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
    Box::pin(exercise_pins_and_gc(client)).await
}

async fn exercise_pins_and_gc(client: &MetaClient) -> Result<()> {
    let register = |record: TestSnapshot| {
        Box::pin(client.register_snapshot(current(), record.metadata, record.manifest, Vec::new()))
    };
    let retire = |id: &SnapshotId| Box::pin(client.retire_snapshot(current(), id.clone()));
    // Local records also live in the Repository and must become reapable after retirement.
    let mut local = snapshot(Vec::new());
    local.metadata.snapshot_type = Some(SnapshotPlacement::Local);
    local.metadata.owner_node_id = Some("local-owner".into());
    register(local.clone()).await??;
    retire(&local.metadata.id).await??;
    assert!(client
        .reap_retired_snapshots(current(), 64)
        .await??
        .contains(&local.metadata.id));
    assert!(
        client
            .complete_reaped_snapshot(current(), local.metadata.id.clone())
            .await??
    );
    let reaped = client
        .get_snapshot(current(), local.metadata.id.to_string())
        .await??
        .unwrap();
    assert!(reaped.retired);
    assert_eq!(reaped.snapshot_type, None);
    assert_eq!(reaped.source_sandbox_id, local.metadata.source_sandbox_id);

    // Pin/release tombstones, top-down leaf layer GC, and deleted-layer revival.
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
    let tombstone_pin = uuid::Uuid::now_v7().to_string();
    client
        .release_pin(current(), s0.metadata.id.clone(), tombstone_pin.clone())
        .await??;
    register(s0.clone()).await??;
    register(s1.clone()).await??;
    assert!(matches!(
        client
            .pin_snapshot(current(), s0.metadata.id.clone(), tombstone_pin)
            .await?,
        Err(MetaError::Unavailable { .. })
    ));

    let active_pin = uuid::Uuid::now_v7().to_string();
    let pinned = client
        .pin_snapshot(current(), s0.metadata.id.clone(), active_pin.clone())
        .await??
        .unwrap();
    assert_eq!(pinned.id(), &s0.metadata.id);
    // Repeated pin with the same pin_id is idempotent.
    assert!(client
        .pin_snapshot(current(), s0.metadata.id.clone(), active_pin.clone())
        .await??
        .is_some());

    // Retiring s0 blocks new pins, but active_pin prevents reaping s0's heads.
    retire(&s0.metadata.id).await??;
    assert!(matches!(
        client
            .pin_snapshot(
                current(),
                s0.metadata.id.clone(),
                uuid::Uuid::now_v7().to_string()
            )
            .await?,
        Err(MetaError::Unavailable { .. })
    ));
    let reapable = client.reap_retired_snapshots(current(), 64).await??;
    assert!(!reapable.contains(&s0.metadata.id));
    assert!(
        !client
            .complete_reaped_snapshot(current(), s0.metadata.id.clone())
            .await??
    );

    // Releasing active_pin with a mismatched snapshot_id must be rejected and leave s0 pinned.
    assert!(matches!(
        client
            .release_pin(current(), s1.metadata.id.clone(), active_pin.clone())
            .await?,
        Err(MetaError::InvalidRequest { .. })
    ));
    assert!(!client
        .reap_retired_snapshots(current(), 64)
        .await??
        .contains(&s0.metadata.id));

    // Releasing active_pin makes s0 reapable; s1 still retains z -> y -> x.
    client
        .release_pin(current(), s0.metadata.id.clone(), active_pin.clone())
        .await??;
    assert!(client
        .reap_retired_snapshots(current(), 64)
        .await??
        .contains(&s0.metadata.id));
    assert!(
        client
            .complete_reaped_snapshot(current(), s0.metadata.id.clone())
            .await??
    );
    // Reap other retired snapshots from earlier steps so we isolate the z -> y -> x chain.
    for id in client.reap_retired_snapshots(current(), 64).await?? {
        client.complete_reaped_snapshot(current(), id).await??;
    }

    // Retire s1 and release its heads; GC must claim z -> y -> x one leaf level at a time.
    retire(&s1.metadata.id).await??;
    assert!(
        client
            .complete_reaped_snapshot(current(), s1.metadata.id.clone())
            .await??
    );
    for expected in [&layer_z, &layer_y, &layer_x] {
        let uuid = expected.uuid.clone().unwrap();
        let claimed = client.claim_deletable_layers(current(), 64).await??;
        let matching: Vec<_> = claimed.iter().filter(|item| item.uuid == uuid).collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].digest, expected.digest);
        // Complete all claimed layers in this round so the parent becomes the next leaf.
        let claimed_uuids: Vec<_> = claimed.into_iter().map(|item| item.uuid).collect();
        client
            .complete_deleted_layers(current(), claimed_uuids)
            .await??;
    }

    let checks = client
        .check_layers(
            current(),
            vec![
                layer_x.uuid.clone().unwrap(),
                "01010101-0101-0101-0101-010101010101".into(),
            ],
        )
        .await??;
    assert_eq!(checks[0].state, LayerState::Deleted);
    assert_eq!(checks[1].state, LayerState::Missing);

    // Deleted layers become available in the same transaction as their new heads.
    let s2 = snapshot(vec![LayerRef::Managed(layer_x.clone())]);
    let Err(MetaError::MissingLayers { uuids }) = register(s2.clone()).await? else {
        anyhow::bail!("publication must require the deleted layer's UUID");
    };
    assert_eq!(uuids, vec![layer_x.uuid.clone().unwrap()]);
    client
        .register_snapshot(current(), s2.metadata, s2.manifest, uuids)
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
    assert!(client
        .reap_retired_snapshots(current(), 64)
        .await??
        .contains(&ext_only.metadata.id));
    assert!(
        client
            .complete_reaped_snapshot(current(), ext_only.metadata.id.clone())
            .await??
    );
    assert!(!client
        .reap_retired_snapshots(current(), 64)
        .await??
        .contains(&ext_only.metadata.id));
    let reaped = client
        .get_snapshot(current(), ext_only.metadata.id.to_string())
        .await??
        .unwrap();
    assert!(!reaped.is_visible());
    assert!(reaped.source_sandbox_id().is_some());
    assert_eq!(
        client
            .get_snapshot(
                current(),
                ext_only.metadata.alias.as_ref().unwrap().to_string()
            )
            .await??
            .unwrap()
            .id(),
        &ext_only.metadata.id
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
