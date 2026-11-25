use agentenv_domain::{
    ExternalLayer, LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotManifest, SnapshotMetadata, SnapshotPlacement,
};
use agentenv_metadata::{
    context::current, transport, DatabaseConfig, MetaClient, MetaError, MetaServer,
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

    // Release-before-pin tombstone blocks late pins; active pins are idempotent and
    // reject mismatched snapshot IDs on release.
    let s0 = snapshot(vec![LayerRef::Managed(layer('7'))]);
    let s1 = snapshot(vec![LayerRef::Managed(layer('8'))]);
    let tombstone_pin = uuid::Uuid::now_v7();
    client
        .release_pin(current(), s0.metadata.id.clone(), tombstone_pin)
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
    let active_pin = uuid::Uuid::now_v7();
    assert_eq!(
        client
            .pin_snapshot(
                current(),
                s0.metadata.id.clone(),
                active_pin,
                Some("node-one".into()),
            )
            .await??
            .unwrap()
            .id(),
        &s0.metadata.id
    );
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
            &format!("SELECT owner_node_id FROM snapshot_pins WHERE pin_id = '{active_pin}'")
        )
        .await?,
        ["node-one"]
    );
    retire(&s0.metadata.id).await??;
    assert!(matches!(
        client
            .pin_snapshot(
                current(),
                s0.metadata.id.clone(),
                uuid::Uuid::now_v7(),
                Some("node-one".into()),
            )
            .await?,
        Err(MetaError::Unavailable { .. })
    ));
    assert!(matches!(
        client
            .release_pin(current(), s1.metadata.id.clone(), active_pin)
            .await?,
        Err(MetaError::InvalidRequest { .. })
    ));
    client
        .release_pin(current(), s0.metadata.id.clone(), active_pin)
        .await??;
    Ok(())
}
