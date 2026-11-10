use std::fs::File;
use std::path::Path;

use agentenv_domain::{
    LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotManifest,
    SnapshotPlacement, SnapshotRecord, SnapshotSource, SnapshotVolume, TemplateBuildStatus,
    VolumeMode,
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

fn snapshot(rootfs_layers: Vec<LayerRef>) -> SnapshotRecord {
    let mut manifest = SnapshotManifest::mock();
    manifest.rootfs_layers = rootfs_layers;
    let mut record = SnapshotRecord::mock_ready(manifest);
    record.config.source = SnapshotSource::Sandbox {
        source_sandbox_id: "sandbox-source".into(),
        source_sandbox_metadata: None,
    };
    record
}

// The caller owns the immutable manifest at a deterministic path keyed by snapshot ID.
fn write_manifest(root: &Path, record: &SnapshotRecord) -> Result<()> {
    let path = root.join(format!("{}.json", record.id));
    serde_json::to_writer(File::create_new(path)?, record.manifest.as_ref().unwrap())?;
    Ok(())
}

fn read_manifest(root: &Path, id: &SnapshotId) -> Result<SnapshotManifest> {
    let path = root.join(format!("{id}.json"));
    Ok(serde_json::from_reader(File::open(path)?)?)
}

fn promoted_record(local: &SnapshotRecord, manifest: SnapshotManifest) -> SnapshotRecord {
    let mut config = local.config.clone();
    config.resources.memory_mib += 512;
    SnapshotRecord {
        snapshot_type: SnapshotPlacement::Distributed,
        owner_node_id: None,
        config,
        updated_at_unix_ms: local.updated_at_unix_ms + 1000,
        manifest: Some(manifest),
        ..local.clone()
    }
}

#[tokio::test]
async fn published_snapshots_share_layers_and_survive_reopen() -> Result<()> {
    let root = tempfile::tempdir()?;
    let sqlite = DatabaseConfig::Sqlite {
        path: root.path().join("metadata.sqlite3"),
    };
    exercise_directory(&sqlite, root.path()).await?;
    if let Ok(url) = std::env::var("AENV_TEST_DATABASE_URL") {
        exercise_directory(&DatabaseConfig::Postgres { url }, root.path()).await?;
    }
    Ok(())
}

async fn exercise_directory(database: &DatabaseConfig, root: &Path) -> Result<()> {
    database.migrate().await?;
    let server = MetaServer::open(database).await?;
    let client = transport::embedded(server.clone());

    let [base, left, right] = [layer('a'), layer('b'), layer('c')];
    let external = LayerRef::External(agentenv_domain::ExternalLayer {
        digest: format!("sha256:{}", "e".repeat(64)),
        size: 4096,
        repo_blob_url: "https://registry.example/v2/image/blobs".into(),
    });
    let mut first = snapshot(vec![
        LayerRef::Managed(base.clone()),
        external,
        LayerRef::Managed(left.clone()),
    ]);
    let alias = format!("snap_{}", first.id);
    first.alias = Some(SnapshotAlias::parse(&alias)?);
    let source_sandbox_id = "sandbox-source-one";
    first.config.source = SnapshotSource::Sandbox {
        source_sandbox_id: source_sandbox_id.into(),
        source_sandbox_metadata: None,
    };
    first.config.resources.cpu_count = 3;
    first.config.context.env_vars = [
        ("ALPHA".to_string(), "1".to_string()),
        ("BETA".to_string(), "2".to_string()),
    ]
    .into_iter()
    .collect();
    first.config.custom_extension_params = Some(
        [("ratio".into(), serde_json::json!(1.6930994060828455_f64))]
            .into_iter()
            .collect(),
    );
    let mut second = snapshot(vec![
        LayerRef::Managed(base.clone()),
        LayerRef::Managed(right.clone()),
    ]);
    second.alias = Some(SnapshotAlias::parse(&format!("snap-{}", second.id))?);
    write_manifest(root, &first)?;

    let (published_left, published_right) = tokio::join!(
        client.register_snapshot(current(), first.clone()),
        client.register_snapshot(current(), second.clone()),
    );
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
                record.id
            ),
        )
        .await?;
        let head_id = head.uuid.as_deref().unwrap();
        assert_eq!(chain, [format!("{head_id}/{base_id}")]);
    }

    let mut alias_conflict = first.clone();
    alias_conflict.id = SnapshotId::generate();
    assert!(matches!(
        client.register_snapshot(current(), alias_conflict).await?,
        Err(MetaError::AliasConflict { existing, .. }) if existing == first.id
    ));

    // A non-available parent must roll back the new snapshot as well.
    sql_rows(
        database,
        &format!("UPDATE layers SET state = 'deleting' WHERE uuid = '{base_id}' RETURNING state"),
    )
    .await?;
    let blocked = snapshot(vec![LayerRef::Managed(base), LayerRef::Managed(layer('d'))]);
    assert!(matches!(
        client.register_snapshot(current(), blocked.clone()).await?,
        Err(MetaError::Unavailable { .. })
    ));
    assert!(client
        .get_snapshot(current(), blocked.id.to_string())
        .await??
        .is_none());

    let mut local = SnapshotRecord::mock_ready(SnapshotManifest::mock());
    local.config.custom_extension_params = Some(
        [("counter".into(), serde_json::json!(1e19))]
            .into_iter()
            .collect(),
    );
    local.config.source = SnapshotSource::Sandbox {
        source_sandbox_id: "sandbox-local".into(),
        source_sandbox_metadata: None,
    };
    local.snapshot_type = SnapshotPlacement::Local;
    local.owner_node_id = Some("node-one".into());
    local.manifest = None;
    local.created_at_unix_ms = first.created_at_unix_ms + 1;
    local.updated_at_unix_ms = local.created_at_unix_ms;
    client.register_snapshot(current(), local.clone()).await??;
    let blocked_promotion = promoted_record(&local, first.manifest.clone().unwrap());
    assert!(matches!(
        client
            .promote_snapshot(current(), blocked_promotion)
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
    let entry = client.get_snapshot(current(), alias).await??.unwrap();
    let committed = read_manifest(root, entry.id())?;
    let restored = entry.into_record(committed)?;
    assert_eq!(
        serde_json::to_value(restored)?,
        serde_json::to_value(&first)?
    );

    let alias_filter = SnapshotListFilter {
        alias_prefix: Some("snap_".into()),
        ..SnapshotListFilter::default()
    };
    let source_filter = SnapshotListFilter::sandbox_snapshots(Some(source_sandbox_id.into()), None);
    let mut distributed_ids = [&first.id, &second.id];
    distributed_ids.sort_unstable();
    for (filter, expected) in [
        (
            SnapshotListFilter::default(),
            vec![&local.id, distributed_ids[0], distributed_ids[1]],
        ),
        (alias_filter, vec![&first.id]),
        (source_filter.clone(), vec![&first.id]),
        (
            SnapshotListFilter {
                snapshot_ids: Some(vec![second.id.clone()]),
                ..source_filter
            },
            vec![],
        ),
        (SnapshotListFilter::by_ids([]), vec![]),
    ] {
        let listed = client.list_snapshots(current(), filter).await??;
        assert_eq!(
            listed.iter().map(|entry| entry.id()).collect::<Vec<_>>(),
            expected
        );
    }

    let entry = client
        .get_snapshot(current(), local.id.to_string())
        .await??
        .unwrap();
    assert_eq!(
        serde_json::to_value(entry.into_local_record()?)?,
        serde_json::to_value(&local)?
    );
    exercise_lifecycle(&client, database, &local).await?;
    client
        .retire_snapshot(current(), first.id.clone())
        .await??;
    let filter = SnapshotListFilter::by_ids([first.id.clone(), second.id.clone()]);
    let listed = client.list_snapshots(current(), filter).await??;
    assert_eq!(
        listed.iter().map(|entry| entry.id()).collect::<Vec<_>>(),
        [&second.id]
    );

    let mut replacement = first;
    replacement.id = SnapshotId::generate();
    assert!(matches!(
        client.register_snapshot(current(), replacement).await?,
        Err(MetaError::AliasConflict { .. })
    ));

    let volume_layer = layer('7');
    let mut volume_local = local.clone();
    volume_local.id = SnapshotId::generate();
    volume_local.config.volume_snapshots.push(SnapshotVolume {
        mount_path: "/data".into(),
        mode: VolumeMode::ReadOnly,
        size_mb: 16,
        layers: vec![LayerRef::Managed(volume_layer.clone())],
    });
    client
        .register_snapshot(current(), volume_local.clone())
        .await??;
    let promoted = promoted_record(&volume_local, SnapshotManifest::mock());
    let promoted = client
        .promote_snapshot(current(), promoted)
        .await??
        .unwrap()
        .into_record(SnapshotManifest::mock())?;
    assert_eq!(
        promoted.config.volume_snapshots,
        volume_local.config.volume_snapshots
    );
    assert_eq!(
        sql_rows(
            database,
            &format!(
                "SELECT CAST(layer_uuid AS TEXT) FROM snapshot_heads WHERE snapshot_id = '{}'",
                volume_local.id
            )
        )
        .await?,
        [volume_layer.uuid.unwrap()]
    );
    let mut template = SnapshotRecord::mock_ready(SnapshotManifest::mock());
    template.alias = Some(SnapshotAlias::parse(&format!("template-{}", template.id))?);
    client
        .register_snapshot(current(), template.clone())
        .await??;
    let templates = client
        .list_snapshots(
            current(),
            SnapshotListFilter {
                snapshot_id_or_alias: template.alias.as_ref().map(ToString::to_string),
                template_statuses: Some(vec![TemplateBuildStatus::Ready]),
                ..SnapshotListFilter::templates()
            },
        )
        .await??;
    assert_eq!(
        templates.iter().map(|entry| entry.id()).collect::<Vec<_>>(),
        [&template.id]
    );
    assert!(client
        .list_snapshots(
            current(),
            SnapshotListFilter::sandbox_snapshots(None, Some(template.id.to_string()))
        )
        .await??
        .is_empty());

    let _ = stop.send(());
    serving.await??;
    server.close().await;
    Ok(())
}

async fn exercise_lifecycle(
    client: &MetaClient,
    database: &DatabaseConfig,
    local: &SnapshotRecord,
) -> Result<()> {
    let head = layer('f');
    let content = SnapshotManifest {
        memory_layers: vec![head.clone()],
        ..SnapshotManifest::mock()
    };
    let mut target = promoted_record(local, content.clone());
    target.alias = Some(SnapshotAlias::parse(&format!("promoted-{}", local.id))?);
    let expected = SnapshotRecord {
        config: local.config.clone(),
        ..target.clone()
    };
    let promote = |record| client.promote_snapshot(current(), record);
    let retire = |id: &SnapshotId| client.retire_snapshot(current(), id.clone());

    let entry = promote(target.clone()).await??.unwrap();
    let mut retry_target = target.clone();
    retry_target.updated_at_unix_ms += 500;
    retry_target
        .manifest
        .as_mut()
        .unwrap()
        .memory_layers
        .push(layer('0'));
    let retry = promote(retry_target).await??.unwrap();
    assert_eq!(serde_json::to_value(entry)?, serde_json::to_value(&retry)?);
    assert_eq!(
        serde_json::to_value(retry.into_record(content.clone())?)?,
        serde_json::to_value(&expected)?
    );
    let mut changed_alias = target.clone();
    changed_alias.alias = Some(SnapshotAlias::parse(&format!("other-{}", local.id))?);
    assert!(matches!(
        promote(changed_alias).await?,
        Err(MetaError::SnapshotConflict { .. })
    ));

    retire(&local.id).await??;
    retire(&local.id).await??;
    assert!(matches!(
        promote(target).await?,
        Err(MetaError::Unavailable { .. })
    ));
    let heads = format!(
        "SELECT CAST(layer_uuid AS TEXT) FROM snapshot_heads WHERE snapshot_id = '{}'",
        local.id
    );
    assert_eq!(sql_rows(database, &heads).await?, [head.uuid.unwrap()]);

    let local_record = || SnapshotRecord {
        id: SnapshotId::generate(),
        ..local.clone()
    };
    let late = local_record();
    assert!(promote(promoted_record(&late, content.clone()))
        .await??
        .is_none());
    retire(&late.id).await??;
    retire(&late.id).await??;
    assert!(!client
        .get_snapshot(current(), late.id.to_string())
        .await??
        .unwrap()
        .is_visible());
    assert!(matches!(
        client.register_snapshot(current(), late.clone()).await?,
        Err(MetaError::Unavailable { .. })
    ));
    let mut uuid_aliased = snapshot(vec![LayerRef::Managed(layer('9'))]);
    uuid_aliased.alias = Some(SnapshotAlias::parse(&late.id.to_string())?);
    client
        .register_snapshot(current(), uuid_aliased.clone())
        .await??;
    assert_eq!(
        client
            .get_snapshot(current(), late.id.to_string())
            .await??
            .unwrap()
            .id(),
        &uuid_aliased.id
    );

    let racing = local_record();
    client
        .register_snapshot(current(), racing.clone())
        .await??;
    let (promotion, retirement) = tokio::join!(
        promote(promoted_record(&racing, content.clone())),
        retire(&racing.id),
    );
    retirement??;
    let expected_placement = match promotion? {
        Ok(Some(_)) => SnapshotPlacement::Distributed,
        Err(MetaError::Unavailable { .. }) => SnapshotPlacement::Local,
        other => panic!("unexpected promotion result: {other:?}"),
    };
    let retired = client
        .get_snapshot(current(), racing.id.to_string())
        .await??
        .unwrap();
    assert!(!retired.is_visible());
    assert_eq!(retired.snapshot_placement()?, expected_placement);

    let candidate = local_record();
    client
        .register_snapshot(current(), candidate.clone())
        .await??;
    let shared = layer('e');
    let mut registration = snapshot(vec![LayerRef::Managed(shared.clone())]);
    registration.alias = Some(SnapshotAlias::parse(&format!(
        "contested-{}",
        candidate.id
    ))?);
    let mut promotion = promoted_record(
        &candidate,
        SnapshotManifest {
            memory_layers: vec![shared],
            ..SnapshotManifest::mock()
        },
    );
    promotion.alias = registration.alias.clone();
    let (registered, promoted) = tokio::join!(
        client.register_snapshot(current(), registration),
        promote(promotion),
    );
    assert!(matches!(
        (registered?, promoted?),
        (Ok(_), Err(MetaError::AliasConflict { .. }))
            | (Err(MetaError::AliasConflict { .. }), Ok(Some(_)))
    ));
    Ok(())
}
