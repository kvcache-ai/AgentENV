use std::fs::File;
use std::path::Path;

use agentenv_domain::{
    CommittedSnapshot, LayerRef, ManagedLayerRef, SnapshotAlias, SnapshotId, SnapshotListFilter,
    SnapshotPlacement, SnapshotRecord, SnapshotSource, SnapshotVolume, TemplateBuildStatus,
    VolumeMode,
};
use agentenv_metadata::{context::current, transport, DatabaseConfig, MetaError, MetaServer};
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
    let mut committed = CommittedSnapshot::mock();
    committed.rootfs_layers = rootfs_layers;
    let mut record = SnapshotRecord::mock_ready(committed);
    record.source = SnapshotSource::Sandbox {
        source_sandbox_id: "sandbox-source".into(),
        source_sandbox_metadata: None,
    };
    record
}

// The caller owns the immutable manifest at a deterministic path keyed by snapshot ID.
fn write_manifest(root: &Path, record: &SnapshotRecord) -> Result<()> {
    let path = root.join(format!("{}.json", record.id));
    serde_json::to_writer(File::create_new(path)?, record.committed.as_ref().unwrap())?;
    Ok(())
}

fn read_manifest(root: &Path, id: &SnapshotId) -> Result<CommittedSnapshot> {
    let path = root.join(format!("{id}.json"));
    Ok(serde_json::from_reader(File::open(path)?)?)
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
    let alias = format!("snap-{}", first.id);
    first.alias = Some(SnapshotAlias::parse(&alias)?);
    first.resources.cpu_count = 3;
    first.committed.as_mut().unwrap().context.env_vars = [
        ("ALPHA".to_string(), "1".to_string()),
        ("BETA".to_string(), "2".to_string()),
    ]
    .into_iter()
    .collect();
    first.committed.as_mut().unwrap().custom_extension_params = Some(
        [("ratio".into(), serde_json::json!(1.6930994060828455_f64))]
            .into_iter()
            .collect(),
    );
    let second = snapshot(vec![
        LayerRef::Managed(base.clone()),
        LayerRef::Managed(right.clone()),
    ]);
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

    let mut changed_committed = first.clone();
    changed_committed
        .committed
        .as_mut()
        .unwrap()
        .context
        .workdir = "/changed".into();
    assert!(matches!(
        client
            .register_snapshot(current(), changed_committed)
            .await?,
        Err(MetaError::SnapshotConflict { .. })
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

    let volume_base = layer('6');
    let volume_tip = layer('7');
    let mut volume_record = snapshot(Vec::new());
    volume_record
        .committed
        .as_mut()
        .unwrap()
        .volume_snapshots
        .push(SnapshotVolume {
            mount_path: "/data".into(),
            mode: VolumeMode::ReadOnly,
            size_mb: 16,
            layers: vec![
                LayerRef::Managed(volume_base.clone()),
                LayerRef::Managed(volume_tip.clone()),
            ],
        });
    write_manifest(root, &volume_record)?;
    client
        .register_snapshot(current(), volume_record.clone())
        .await??;
    let mut deleting = volume_record.clone();
    deleting.id = SnapshotId::generate();
    deleting.deleting = true;
    assert!(matches!(
        client.register_snapshot(current(), deleting).await?,
        Err(MetaError::InvalidRequest { .. })
    ));

    let mut template = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
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
    let sandboxes = client
        .list_snapshots(
            current(),
            SnapshotListFilter::sandbox_snapshots(None, Some(template.id.to_string())),
        )
        .await??;
    assert!(sandboxes.is_empty());

    let mut local_content = CommittedSnapshot::mock();
    local_content.custom_extension_params = Some(
        [("counter".into(), serde_json::json!(1e19))]
            .into_iter()
            .collect(),
    );
    let mut local = SnapshotRecord::mock_ready(local_content);
    local.source = first.source.clone();
    local.snapshot_type = SnapshotPlacement::Local;
    local.owner_node_id = Some("node-one".into());
    local.committed.as_mut().unwrap().volume_snapshots = volume_record
        .committed
        .as_ref()
        .unwrap()
        .volume_snapshots
        .clone();
    client.register_snapshot(current(), local.clone()).await??;
    for record in [&volume_record, &local] {
        let relations = sql_rows(database, &format!(
            "SELECT CAST(tip.uuid AS TEXT) || '/' || CAST(tip.managed_parent_uuid AS TEXT) FROM snapshot_heads h JOIN layers tip ON tip.uuid = h.layer_uuid WHERE h.snapshot_id = '{}'", record.id
        )).await?;
        assert_eq!(
            relations,
            [format!(
                "{}/{}",
                volume_tip.uuid.as_deref().unwrap(),
                volume_base.uuid.as_deref().unwrap()
            )]
        );
    }
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
    // Retrying the same record over TCP must preserve float round-trip in committed_digest.
    client.register_snapshot(current(), first.clone()).await??;
    let entry = client.get_snapshot(current(), alias).await??.unwrap();
    let committed = read_manifest(root, entry.id())?;
    let restored = entry.into_record(committed)?;
    assert_eq!(
        serde_json::to_value(restored)?,
        serde_json::to_value(&first)?
    );

    let entry = client
        .get_snapshot(current(), local.id.to_string())
        .await??
        .unwrap();
    assert_eq!(
        serde_json::to_value(entry.into_local_record()?)?,
        serde_json::to_value(local)?
    );
    let volume_entry = client
        .get_snapshot(current(), volume_record.id.to_string())
        .await??
        .unwrap();
    assert_eq!(
        serde_json::to_value(volume_entry.into_record(read_manifest(root, &volume_record.id)?)?)?,
        serde_json::to_value(&volume_record)?
    );
    let filter = SnapshotListFilter::by_ids([first.id.clone()]);
    let listed = client.list_snapshots(current(), filter).await??;
    assert_eq!(
        listed.iter().map(|entry| entry.id()).collect::<Vec<_>>(),
        [&first.id]
    );

    let _ = stop.send(());
    serving.await??;
    server.close().await;
    Ok(())
}
