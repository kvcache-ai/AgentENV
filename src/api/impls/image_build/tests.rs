use super::*;
use crate::api::impls::template_helpers::template_build_record_from_v3_request;
use crate::snapshot::{SnapshotSource, TemplateBuildStatus};
use crate::volume::{VolumeLimits, VolumeMode, VolumeRecord, VolumeStatus};

pub(super) async fn test_api(
    limits: VolumeLimits,
) -> Result<(tempfile::TempDir, ApiImpl, SnapshotRecord)> {
    let (root, api, record, _) = test_api_with_image_cache(limits).await?;
    Ok((root, api, record))
}

pub(super) async fn test_api_with_image_cache(
    limits: VolumeLimits,
) -> Result<(
    tempfile::TempDir,
    ApiImpl,
    SnapshotRecord,
    Arc<crate::image::cache::ImageCacheService>,
)> {
    use crate::{
        api_key::ApiKey,
        cfg::AppConfig,
        image::ImageResolver,
        orchestrator::{FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator},
        sandbox::FirecrackerSandboxFactory,
        snapshot::{
            mock::write_mock_built_artifacts,
            repository::backends::{PosixFsBackend, PosixFsBackendConfig},
            SnapshotManager, SnapshotPublishMetadata,
        },
        template::TemplateBuilder,
        volume::VolumeManager,
    };

    let root = tempfile::tempdir()?;
    let backend = PosixFsBackend::new(PosixFsBackendConfig {
        root: root.path().join("repository"),
        cache_root: Some(root.path().join("cache")),
        runtime_cache_root: None,
    })?;
    let manager = Arc::new(SnapshotManager::from_parts(
        backend.repository(),
        backend.runtime_resolver(),
        None,
    ));
    let (_, _, manifest) = write_mock_built_artifacts(&root.path().join("artifacts"))?;
    let mut metadata = SnapshotPublishMetadata::mock();
    metadata.alias = Some(crate::snapshot::SnapshotAlias::parse("test-template")?);
    let record = manager.publish(metadata, manifest, None).await?;
    let orchestrator = Orchestrator::new(
        InMemoryMetadataStore::new(),
        FirecrackerSandboxFactory::new(),
        FileBackedSandboxPersister::new_for_test(root.path().join("sandboxes")),
    )
    .await?;
    let volumes = VolumeManager::open_with_repository_and_limits(
        root.path().join("volumes/catalog.json"),
        backend.repository(),
        limits,
    )
    .await?;
    let mut image_config = AppConfig::default();
    crate::cfg::ImageConfig::normalize(&mut image_config.image, root.path(), root.path());
    let image_cache = crate::image::cache::ImageCacheService::shared_from_app_config(&image_config);
    let api = ApiImpl::new(
        orchestrator,
        manager.clone(),
        Arc::new(TemplateBuilder::new()),
        Arc::new(ImageResolver::new(&image_config)),
        Arc::new(volumes),
        None,
        Vec::new(),
        ApiKey::new("build-cleanup-test-api-key-0123456789")?,
    );
    let journal =
        LocalKvStore::open(root.path().join("journal"), LocalStoreDurability::Memory).await?;
    api.build_sessions.journal.set(journal).unwrap();
    Ok((root, api, record, image_cache))
}

async fn cache_volume(api: &ApiImpl, name: &str, mode: VolumeMode, owner: &str) -> Result<String> {
    let id = format!("vol_{}", uuid::Uuid::now_v7().simple());
    api.snapshot_manager
        .repository()
        .create_volume(VolumeRecord {
            id: id.clone(),
            name: name.into(),
            mode,
            size_mb: 1024,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: (mode == VolumeMode::Exclusive).then(|| owner.into()),
            backing_image_config: None,
            backing_layers: Vec::new(),
            read_only_mounts: if mode == VolumeMode::ReadOnly {
                vec![owner.into()]
            } else {
                Vec::new()
            },
            deleting: false,
        })
        .await?;
    Ok(id)
}

#[tokio::test]
async fn image_catalog_api_paginates_and_deletes_without_removing_shared_layers() -> Result<()> {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
        Router,
    };
    use serde_json::{json, Value};
    use tower::ServiceExt;
    async fn call(
        app: &Router,
        method: &str,
        uri: &str,
        authorized: bool,
    ) -> Result<(StatusCode, Value)> {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("host", "localhost");
        if authorized {
            request = request.header("x-api-key", "build-cleanup-test-api-key-0123456789");
        }
        let response = app.clone().oneshot(request.body(Body::empty())?).await?;
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 65536).await?;
        Ok((
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        ))
    }
    let (root, api, _) = test_api(VolumeLimits::default()).await?;
    let app = crate::api::server::new(Arc::new(api.clone()));
    assert_eq!(
        call(&app, "GET", "/images", false).await?.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "GET", "/images", true).await?.1,
        json!({"images": []})
    );
    let repository = api.snapshot_manager.repository();
    let (_, _, manifest) =
        crate::snapshot::mock::write_mock_built_artifacts(&root.path().join("image-source"))?;
    let layers = repository
        .publish_image_layers(&manifest.rootfs.image_config_path)
        .await?;
    let image =
        crate::image::PublishedImage::new("amd64".into(), layers.clone(), json!({"Cmd": ["true"]}));
    let (first, repeat) = tokio::join!(repository.put_image(&image), repository.put_image(&image));
    let first = first?;
    assert_eq!(first, repeat?);
    let other =
        crate::image::PublishedImage::new("amd64".into(), layers, json!({"Cmd": ["false"]}));
    let mut digests = [first.clone(), repository.put_image(&other).await?];
    digests.sort();
    let (status, page) = call(&app, "GET", "/images?limit=1", true).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["images"][0]["imageDigest"], digests[0]);
    assert_eq!(page["nextToken"], digests[0]);
    assert!(page["images"][0].get("description").is_none());
    let detail = call(&app, "GET", &format!("/images/{first}"), true).await?;
    assert_eq!(detail.0, StatusCode::OK);
    assert_eq!(detail.1["description"], serde_json::to_value(&image)?);
    let resolved = api.image_resolver.resolve(&digests[0]).await?;
    let config = overlaybd::config::load_image_config(&resolved.overlaybd_config_path)?;
    for _ in 0..2 {
        assert_eq!(
            call(&app, "DELETE", &format!("/images/{}", digests[0]), true)
                .await?
                .0,
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        call(&app, "GET", &format!("/images/{}", digests[0]), true)
            .await?
            .0,
        StatusCode::NOT_FOUND
    );
    assert!(matches!(
        api.image_resolver.resolve(&digests[0]).await,
        Err(crate::image::ImageError::NotFound { .. })
    ));
    assert!(resolved.overlaybd_config_path.exists());
    assert!(config
        .lowers
        .iter()
        .all(|layer| std::path::Path::new(&layer.file).is_file()));
    let page = call(
        &app,
        "GET",
        &format!("/images?limit=1&nextToken={}", digests[0]),
        true,
    )
    .await?
    .1;
    assert_eq!(page["images"][0]["imageDigest"], digests[1]);
    assert!(page.get("nextToken").is_none());
    assert!(api
        .image_resolver
        .resolve(&digests[1])
        .await?
        .overlaybd_config_path
        .is_file());
    for uri in [
        "/images?limit=0",
        "/images?limit=101",
        "/images?nextToken=invalid",
        "/images/sha256:invalid",
    ] {
        assert_eq!(
            call(&app, "GET", uri, true).await?.0,
            StatusCode::BAD_REQUEST,
            "{uri}"
        );
    }
    assert_eq!(
        call(&app, "DELETE", "/images/invalid", true).await?.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, "GET", "/images/builds", true).await?.0,
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        call(&app, "GET", "/images/builds/missing", true).await?.0,
        StatusCode::NOT_FOUND
    );
    assert!(api
        .volume_manager
        .list_page(None, 100)
        .await?
        .records
        .is_empty());
    let deleted = if digests[0] == first { &image } else { &other };
    assert_eq!(repository.put_image(deleted).await?, digests[0]);
    assert_eq!(
        call(&app, "GET", &format!("/images/{}", digests[0]), true)
            .await?
            .0,
        StatusCode::OK
    );
    Ok(())
}

mod content_proto {
    tonic::include_proto!("containerd.services.content.v1");
}

async fn content_server(
    blobs: HashMap<String, Vec<u8>>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    struct Store(HashMap<String, Vec<u8>>);
    #[tonic::async_trait]
    impl content_proto::content_server::Content for Store {
        type ReadStream = futures::stream::Iter<
            std::vec::IntoIter<Result<content_proto::ReadContentResponse, tonic::Status>>,
        >;
        async fn read(
            &self,
            request: tonic::Request<content_proto::ReadContentRequest>,
        ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
            let data = self
                .0
                .get(&request.get_ref().digest)
                .cloned()
                .unwrap_or_default();
            Ok(tonic::Response::new(futures::stream::iter(vec![Ok(
                content_proto::ReadContentResponse { offset: 0, data },
            )])))
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(content_proto::content_server::ContentServer::new(Store(
                blobs,
            )))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    Ok((address, server))
}

async fn reopen_journal(api: &mut ApiImpl, path: &std::path::Path) -> Result<()> {
    api.build_sessions = Arc::new(BuildSessions::default());
    let journal = LocalKvStore::open(path, LocalStoreDurability::Sync).await?;
    api.build_sessions.journal.set(journal).unwrap();
    Ok(())
}

#[tokio::test]
async fn image_only_cleanup_retains_results_and_failure_logs_until_delete() -> Result<()> {
    let digest = format!("sha256:{}", "ab".repeat(32));
    for outcome in ["imported", "failure", "restart", "restart-after-import"] {
        let (root, mut api, existing) = test_api(VolumeLimits::default()).await?;
        let journal_path = root.path().join("durable-journal");
        reopen_journal(&mut api, &journal_path).await?;
        let record =
            SnapshotRecord::template_waiting(SnapshotId::generate(), None, existing.resources);
        let id = record.id.to_string();
        let logs = api
            .build_logs
            .start(record.id.clone(), api.snapshot_manager.repository());
        logs.logger
            .log(crate::logging::LogLevel::Info, None, "build diagnostic");
        logs.finish().await?;
        let entry = BuildJournal {
            cache: cache_volume(&api, "image-only-work", VolumeMode::Exclusive, &id).await?,
            parent: None,
            image_only: true,
        };
        entry.persist(api.build_journal().await?, &id).await?;
        let imported = outcome == "imported" || outcome == "restart-after-import";
        if imported {
            api.build_journal()
                .await?
                .put(
                    format!("image/{id}"),
                    serde_json::to_vec(&ImageBuildResult::Ready {
                        digest: digest.clone(),
                    })?,
                )
                .await?;
        }
        if outcome.starts_with("restart") {
            reopen_journal(&mut api, &journal_path).await?;
        } else {
            let session = BuildSession::new();
            session.state.send_replace(SessionState::Finished(
                (outcome == "failure").then(|| TemplateBuildErrorReason::new("push failed")),
            ));
            api.build_sessions
                .active
                .lock()
                .unwrap()
                .insert(id.clone(), session);
        }
        api.recover_image_builds().await?;
        api.recover_image_builds().await?;
        assert!(api
            .snapshot_manager
            .get(existing.id.to_string())
            .await?
            .is_some());
        assert!(api
            .build_journal()
            .await?
            .get(format!("build/{id}"))
            .await?
            .is_none());
        assert!(!api.build_sessions.contains(&id));
        assert!(api.snapshot_manager.get(&id).await?.is_none());
        // Recovery must retain both the original error and its routing without
        // retaining a worker, even after all in-memory sessions have been lost.
        reopen_journal(&mut api, &journal_path).await?;
        api.recover_image_builds().await?;
        let info = api.image_build_info(&id).await?.expect("retained result");
        if imported {
            assert_eq!(info.status, models::ImageBuildStatus::Ready);
            assert_eq!(info.image_digest.as_deref(), Some(digest.as_str()));
            assert!(info.reason.is_none());
        } else {
            assert_eq!(info.status, models::ImageBuildStatus::Error);
            assert!(info.image_digest.is_none());
            let expected = if outcome == "failure" {
                "push failed"
            } else {
                "build interrupted by server restart"
            };
            assert_eq!(info.reason.unwrap().message, expected);
            let logs = api
                .snapshot_manager
                .repository()
                .read_build_logs(&record.id)
                .await?;
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].message, "build diagnostic");
        }
        assert!(!api.build_sessions.contains(&id));
        assert!(api
            .orchestrator
            .list_sandbox_ids()
            .await?
            .contains(&SandboxId::parse_str(&id)?));
        // Template routes cannot release an independent image build.
        assert_eq!(
            api.cancel_image_build(&id, &id).await.unwrap_err().code,
            404
        );
        api.cancel_build(&id, &id)
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?;
        assert!(api.image_build_info(&id).await?.is_none());
        assert!(api.snapshot_manager.get(&id).await?.is_none());
        assert!(api
            .snapshot_manager
            .repository()
            .read_build_logs(&record.id)
            .await?
            .is_empty());
        api.recover_image_builds().await?;
        assert!(!api
            .orchestrator
            .list_sandbox_ids()
            .await?
            .contains(&SandboxId::parse_str(&id)?));
    }
    let legacy: BuildJournal = serde_json::from_str(r#"{"cache":"old-cache","parent":null}"#)?;
    assert!(!legacy.image_only);
    Ok(())
}

#[tokio::test]
async fn image_only_import_marks_build_ready_and_serves_digest_until_delete() -> Result<()> {
    for metadata in ["complete", "missing", "legacy"] {
        assert_image_only_import(metadata).await?;
    }
    Ok(())
}

async fn assert_image_only_import(metadata: &str) -> Result<()> {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    let (root, api, existing, cache) = test_api_with_image_cache(VolumeLimits::default()).await?;
    let record = SnapshotRecord::template_waiting(SnapshotId::generate(), None, existing.resources);
    let id = record.id.to_string();

    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => anyhow::bail!("unsupported test architecture: {other}"),
    };
    let config = serde_json::to_vec(&serde_json::json!({
        "architecture": arch,
        "os": "linux",
        "config": {"Env": ["A=B"]}
    }))?;
    let config_digest = crate::digest::sha256_digest(&config);
    let layer_digest = crate::digest::sha256_digest(b"layer");
    let manifest = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": config.len()
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "digest": layer_digest,
            "size": 5
        }]
    }))?;
    let manifest_digest = crate::digest::sha256_digest(&manifest);
    let index = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": manifest_digest,
            "size": manifest.len(),
            "platform": {"os": "linux", "architecture": arch}
        }]
    }))?;
    let index_digest = crate::digest::sha256_digest(&index);
    let (address, server) = content_server(HashMap::from([
        (config_digest, config),
        (manifest_digest.clone(), manifest),
        (index_digest.clone(), index),
    ]))
    .await?;
    // Pre-seed the node-local cache so the import short-circuits conversion and
    // no overlaybd tooling runs. The reported digest is the platform manifest
    // the index resolves to, not the digest the client built. A cached config is
    // usable only with a sealed lower backed by a real file.
    let sealed_layer = root.path().join("sealed-layer");
    std::fs::write(&sealed_layer, b"layer")?;
    let conversion = cache.begin_image_conversion(&manifest_digest, None).await?;
    let config_path = cache
        .publish_image_config(
            &manifest_digest,
            None,
            &serde_json::json!({
                "repoBlobUrl": "",
                "lowers": [{
                    "file": sealed_layer.display().to_string(),
                    "digest": layer_digest,
                    "size": 5
                }],
                "upper": {},
                "resultFile": ""
            }),
            crate::image::ImageResolutionMetadata {
                base_context: crate::image::ImageBaseContext::default(),
                raw_config: (metadata != "legacy").then(|| serde_json::json!({"Env": ["A=B"]})),
            },
            conversion,
        )
        .await?;
    if metadata == "missing" {
        let metadata_path = config_path.with_file_name(format!(
            "{}.metadata.json",
            config_path.file_stem().unwrap().to_str().unwrap()
        ));
        std::fs::remove_file(metadata_path)?;
    }

    let imported = api.import_build_image(address, &index_digest).await?;
    let published_digest = api.image_resolver.publish_image(&imported).await?;
    assert_ne!(published_digest, manifest_digest);
    api.build_journal()
        .await?
        .put(
            format!("image/{id}"),
            serde_json::to_vec(&ImageBuildResult::Ready {
                digest: published_digest.clone(),
            })?,
        )
        .await?;
    BuildJournal {
        cache: "missing-cache".into(),
        parent: None,
        image_only: true,
    }
    .persist(api.build_journal().await?, &id)
    .await?;
    api.recover_image_builds().await?;
    server.abort();

    // A successful import must be consumable by the later digest-only lookup,
    // including when it reused a cache created before metadata was persisted.
    let resolved = api.image_resolver.resolve(&published_digest).await?;
    assert_ne!(resolved.overlaybd_config_path, config_path);
    assert_eq!(
        resolved.raw_config,
        Some(serde_json::json!({"Env": ["A=B"]}))
    );

    assert!(api.snapshot_manager.get(&id).await?.is_none());

    let app = crate::api::server::new(Arc::new(api.clone()));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(http::Method::GET)
                .uri(format!("/images/builds/{id}"))
                .header("host", "localhost")
                .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096).await?;
    let info: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(info["status"], serde_json::json!("ready"));
    assert_eq!(info["imageDigest"], serde_json::json!(published_digest));
    assert!(info.get("nodeID").is_none());
    assert!(api
        .orchestrator
        .list_sandbox_ids()
        .await?
        .contains(&SandboxId::parse_str(&id)?));

    // Image build diagnostics do not depend on a template record.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/images/builds/{id}/logs"))
                .header("host", "localhost")
                .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096).await?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body)?,
        serde_json::json!([])
    );

    let response = app
        .oneshot(
            Request::builder()
                .method(http::Method::DELETE)
                .uri(format!("/images/builds/{id}"))
                .header("host", "localhost")
                .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), http::StatusCode::NO_CONTENT);
    assert!(api.image_build_info(&id).await?.is_none());
    // Releasing the build result must preserve the independently published image.
    assert!(api
        .image_resolver
        .resolve(&published_digest)
        .await?
        .overlaybd_config_path
        .is_file());
    assert!(!api
        .orchestrator
        .list_sandbox_ids()
        .await?
        .contains(&SandboxId::parse_str(&id)?));
    Ok(())
}

#[tokio::test]
async fn image_build_status_waits_for_cache_publication_before_reporting_ready() -> Result<()> {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let (_root, api, _) = test_api(VolumeLimits::default()).await?;
    let id = SnapshotId::generate().to_string();
    let digest = crate::digest::sha256_digest(b"published image");
    api.build_journal()
        .await?
        .put(
            format!("image/{id}"),
            serde_json::to_vec(&ImageBuildResult::Ready {
                digest: digest.clone(),
            })?,
        )
        .await?;
    let session = BuildSession::new();
    session.state.send_replace(SessionState::Publishing);
    api.build_sessions
        .active
        .lock()
        .unwrap()
        .insert(id.clone(), session.clone());
    let app = crate::api::server::new(Arc::new(api));
    for expected in ["building", "ready"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/images/builds/{id}"))
                    .header("host", "localhost")
                    .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), http::StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 4096).await?)?;
        assert_eq!(body["status"], expected);
        if expected == "building" {
            assert!(body.get("imageDigest").is_none());
        } else {
            assert_eq!(body["imageDigest"], digest);
        }
        session.state.send_replace(SessionState::Finished(None));
    }
    Ok(())
}

#[tokio::test]
async fn image_build_finalization_retains_logs_without_a_template() -> Result<()> {
    let (_root, api, existing) = test_api(VolumeLimits::default()).await?;
    let record = SnapshotRecord::template_waiting(SnapshotId::generate(), None, existing.resources);
    let id = record.id.to_string();
    BuildJournal {
        cache: "missing-cache".into(),
        parent: None,
        image_only: true,
    }
    .persist(api.build_journal().await?, &id)
    .await?;
    let session = BuildSession::new();
    api.build_sessions
        .active
        .lock()
        .unwrap()
        .insert(id.clone(), session.clone());
    let logs = api.build_logs.start_with_flush_interval(
        record.id.clone(),
        api.snapshot_manager.repository(),
        Duration::from_secs(3600),
    );
    logs.logger
        .log(crate::logging::LogLevel::Info, None, "exported image");
    api.supervise_image_build(&record.id, &session, logs, async { Ok(()) })
        .await;
    assert!(api.snapshot_manager.get(&id).await?.is_none());
    assert_eq!(
        api.snapshot_manager
            .repository()
            .read_build_logs(&record.id)
            .await?
            .len(),
        1
    );
    assert_eq!(
        api.image_build_info(&id).await?.unwrap().status,
        models::ImageBuildStatus::Error
    );
    assert!(api.build_logs.temporary(&record.id).is_none());
    assert!(!api.build_sessions.contains(&id));
    Ok(())
}

#[tokio::test]
async fn buildkit_cleanup_retries_preserve_status_and_release_all_resources() -> Result<()> {
    for (cancel_retry, succeeded) in [(false, true), (true, true), (false, false), (true, false)] {
        let (root, api, mut record) = test_api(VolumeLimits::default()).await?;
        if !succeeded {
            record =
                SnapshotRecord::template_waiting(SnapshotId::generate(), None, record.resources);
            api.snapshot_manager.create(record.clone()).await?;
        }
        let id = record.id.to_string();
        let key = format!("build/{id}").into_bytes();
        let entry = BuildJournal {
            cache: cache_volume(&api, "work", VolumeMode::Exclusive, &id).await?,
            parent: Some(cache_volume(&api, "parent", VolumeMode::ReadOnly, &id).await?),
            image_only: false,
        };
        entry.persist(api.build_journal().await?, &id).await?;
        let session = BuildSession::new();
        session.state.send_replace(SessionState::Publishing);
        api.build_sessions
            .active
            .lock()
            .unwrap()
            .insert(id.clone(), session.clone());
        api.orchestrator
            .register_template_build(SandboxId::parse_str(&id)?)
            .await;

        let limit = ConfigManager::global_config()
            .template_build
            .max_concurrent_builds;
        for _ in 1..limit {
            api.build_sessions
                .reserve(&SnapshotId::generate().to_string())
                .unwrap();
        }
        let next_id = SnapshotId::generate().to_string();
        // An unreadable cache-head record fails cleanup after worker and parent leases are released.
        let fault = root
            .path()
            .join("repository/template-build/cache-head.json");
        tokio::fs::create_dir_all(&fault).await?;
        let logs = api
            .build_logs
            .start(record.id.clone(), api.snapshot_manager.repository());
        api.finish_image_build(
            &record.id,
            &session,
            logs,
            if succeeded {
                Ok(())
            } else {
                Err(anyhow::anyhow!("original build failure"))
            },
        )
        .await;

        let saved = api.snapshot_manager.get(&id).await?.unwrap();
        let expected_status = if succeeded {
            TemplateBuildStatus::Ready
        } else {
            TemplateBuildStatus::Error
        };
        assert_eq!(
            super::super::template::template_build_status(&saved),
            expected_status
        );
        assert!(api.build_journal().await?.get(key.clone()).await?.is_some());
        assert!(matches!(*session.state.borrow(), SessionState::Finished(_)));
        assert!(api.build_sessions.active.lock().unwrap().contains_key(&id));
        assert!(api
            .volume_manager
            .get(&entry.cache)
            .await?
            .reserved_by_sandbox_id
            .is_none());
        assert!(api
            .volume_manager
            .get(entry.parent.as_ref().unwrap())
            .await?
            .read_only_mounts
            .is_empty());

        assert_eq!(
            api.build_sessions.reserve(&next_id).err().unwrap().code,
            429
        );
        assert!(api.cancel_image_build(&id, &id).await.is_err());
        tokio::fs::remove_dir(&fault).await?;
        if cancel_retry {
            api.cancel_image_build(&id, &id)
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?;
        } else {
            api.recover_image_builds().await?;
        }
        assert!(api.build_journal().await?.get(key).await?.is_none());
        assert!(!api.build_sessions.active.lock().unwrap().contains_key(&id));
        assert!(api.build_sessions.reserve(&next_id).is_ok());
        assert!(api
            .volume_manager
            .list_page(None, 100)
            .await?
            .records
            .is_empty());
        assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
        let saved = api.snapshot_manager.get(&id).await?.unwrap();
        assert_eq!(
            super::super::template::template_build_status(&saved),
            expected_status
        );
        let SnapshotSource::Template { build } = saved.source else {
            panic!("expected template")
        };
        assert_eq!(
            build
                .error_reason
                .as_ref()
                .map(|reason| reason.message.as_str()),
            (!succeeded).then_some("original build failure")
        );
        api.cancel_image_build(&id, &id)
            .await
            .map_err(|error| anyhow::anyhow!(error.message))?;
    }
    Ok(())
}

#[tokio::test]
async fn buildkit_recovery_isolates_bad_entries_and_skips_active_builds() -> Result<()> {
    let (_root, api, record) = test_api(VolumeLimits::default()).await?;
    let journal = api.build_journal().await?;
    let bad_key = b"build/00000000-0000-0000-0000-000000000000".to_vec();
    journal
        .put(bad_key.clone(), b"invalid JSON".to_vec())
        .await?;
    journal.put(b"build/\xff".to_vec(), b"{}".to_vec()).await?;
    let entry = BuildJournal {
        cache: "missing-cache".into(),
        parent: None,
        image_only: false,
    };
    let live_id = SnapshotId::generate().to_string();
    let live = BuildSession::new();
    api.build_sessions
        .active
        .lock()
        .unwrap()
        .insert(live_id.clone(), live.clone());
    entry.persist(journal, &live_id).await?;
    entry.persist(journal, &record.id.to_string()).await?;
    let request = serde_json::from_value(serde_json::json!({"name": "interrupted"}))?;
    let interrupted =
        template_build_record_from_v3_request(&request, SnapshotId::generate(), "interrupted")
            .unwrap();
    api.snapshot_manager.create(interrupted.clone()).await?;
    entry.persist(journal, &interrupted.id.to_string()).await?;

    let (first, second) = tokio::join!(api.recover_image_builds(), api.recover_image_builds());
    first?;
    second?;
    assert!(journal.get(bad_key).await?.is_some());
    assert!(journal.get(format!("build/{live_id}")).await?.is_some());
    assert!(matches!(*live.state.borrow(), SessionState::Starting));
    assert!(journal.get(format!("build/{}", record.id)).await?.is_none());
    assert!(!api
        .build_sessions
        .active
        .lock()
        .unwrap()
        .contains_key(&record.id.to_string()));
    assert!(journal
        .get(format!("build/{}", interrupted.id))
        .await?
        .is_none());
    let saved = api
        .snapshot_manager
        .get(interrupted.id.to_string())
        .await?
        .unwrap();
    assert_eq!(
        super::super::template::template_build_status(&saved),
        TemplateBuildStatus::Error
    );
    Ok(())
}

#[tokio::test]
async fn buildkit_worker_panic_releases_journal_and_scheduler_binding() -> Result<()> {
    let (_root, api, record) = test_api(VolumeLimits::default()).await?;
    let record = SnapshotRecord::template_waiting(SnapshotId::generate(), None, record.resources);
    api.snapshot_manager.create(record.clone()).await?;
    let id = record.id.to_string();
    let entry = BuildJournal {
        cache: cache_volume(&api, "work", VolumeMode::Exclusive, &id).await?,
        parent: None,
        image_only: false,
    };
    entry.persist(api.build_journal().await?, &id).await?;
    let session = BuildSession::new();
    api.build_sessions
        .active
        .lock()
        .unwrap()
        .insert(id.clone(), session.clone());
    api.orchestrator
        .register_template_build(SandboxId::parse_str(&id)?)
        .await;
    let logs = api
        .build_logs
        .start(record.id.clone(), api.snapshot_manager.repository());
    api.supervise_image_build(&record.id, &session, logs, async {
        panic!("injected worker panic");
    })
    .await;
    assert!(
        matches!(&*session.state.borrow(), SessionState::Finished(Some(reason)) if reason.message == "build worker panicked")
    );
    assert!(!api.build_sessions.active.lock().unwrap().contains_key(&id));
    assert!(api
        .build_journal()
        .await?
        .get(format!("build/{id}"))
        .await?
        .is_none());
    assert!(api
        .volume_manager
        .list_page(None, 100)
        .await?
        .records
        .is_empty());
    assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
    let saved = api.snapshot_manager.get(&id).await?.unwrap();
    assert_eq!(
        super::super::template::template_build_status(&saved),
        TemplateBuildStatus::Error
    );
    Ok(())
}

#[tokio::test]
async fn build_session_route_preserves_template_named_builds() -> Result<()> {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    let (_root, api, record) = test_api(VolumeLimits::default()).await?;
    let record = SnapshotRecord::template_waiting(
        SnapshotId::generate(),
        Some(crate::snapshot::SnapshotAlias::parse("builds")?),
        record.resources,
    );
    api.snapshot_manager.create(record.clone()).await?;
    let app = crate::api::server::new(Arc::new(api.clone()));
    for (method, expected) in [
        (http::Method::GET, http::StatusCode::OK),
        (http::Method::DELETE, http::StatusCode::NO_CONTENT),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri("/templates/builds")
                    .header("host", "localhost")
                    .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), expected);
    }
    assert!(api
        .snapshot_manager
        .get(record.id.to_string())
        .await?
        .is_none());
    Ok(())
}

#[tokio::test]
async fn builder_allocation_requires_an_existing_waiting_build() -> Result<()> {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    let (_root, api, record) = test_api(VolumeLimits::default()).await?;
    let waiting = SnapshotRecord::template_waiting(SnapshotId::generate(), None, record.resources);
    api.snapshot_manager.create(waiting.clone()).await?;
    // A claim by another node or the existing build API must exclude builder allocation.
    api.snapshot_manager.try_start_build(&waiting.id).await?;
    let app = crate::api::server::new(Arc::new(api.clone()));
    for (id, expected) in [
        (SnapshotId::generate(), http::StatusCode::NOT_FOUND),
        (record.id.clone(), http::StatusCode::CONFLICT),
        (waiting.id.clone(), http::StatusCode::CONFLICT),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::PUT)
                    .uri(format!("/templates/{id}/builds/{id}/builder"))
                    .header("host", "localhost")
                    .header("content-type", "application/json")
                    .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                    .body(Body::from("{}"))?,
            )
            .await?;
        assert_eq!(response.status(), expected);
    }
    assert!(api.build_sessions.active.lock().unwrap().is_empty());
    assert!(api
        .build_journal()
        .await?
        .scan_prefix(b"build/".to_vec())
        .await?
        .is_empty());
    assert_eq!(
        super::super::template::template_build_status(
            &api.snapshot_manager
                .get(waiting.id.to_string())
                .await?
                .unwrap()
        ),
        TemplateBuildStatus::Building
    );
    Ok(())
}

#[tokio::test]
async fn active_build_rejects_template_deletion_by_id_and_alias() -> Result<()> {
    use agentenv_http_server::apis::templates::{Templates, TemplatesTemplateIdDeleteResponse};
    use axum_extra::extract::CookieJar;
    use headers::Host;

    let (_root, api, record) = test_api(VolumeLimits::default()).await?;
    let id = record.id.to_string();
    api.build_sessions
        .active
        .lock()
        .unwrap()
        .insert(id.clone(), BuildSession::new());
    let references = [id.clone(), record.alias.as_ref().unwrap().to_string()];
    for reference in references {
        let response = api
            .templates_template_id_delete(
                &http::Method::DELETE,
                &Host::from(http::uri::Authority::from_static("localhost")),
                &CookieJar::new(),
                &super::super::Claims,
                &models::TemplatesTemplateIdDeletePathParams {
                    template_id: reference,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            response,
            TemplatesTemplateIdDeleteResponse::Status409_Conflict(_)
        ));
        assert!(api.snapshot_manager.get(&id).await?.is_some());
    }
    api.build_sessions.active.lock().unwrap().remove(&id);
    let response = api
        .templates_template_id_delete(
            &http::Method::DELETE,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &super::super::Claims,
            &models::TemplatesTemplateIdDeletePathParams {
                template_id: id.clone(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        response,
        TemplatesTemplateIdDeleteResponse::Status204_TheTemplateWasDeletedSuccessfully
    ));
    assert!(api.snapshot_manager.get(&id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn buildkit_cache_limit_is_checked_before_allocating_build() -> Result<()> {
    let (_root, api, _) = test_api(VolumeLimits {
        max_size_mb: 1024,
        ..VolumeLimits::default()
    })
    .await?;
    let request = serde_json::from_value(serde_json::json!({}))?;
    // UUID spellings normalize to the same reservation key, including error cleanup.
    let id = SnapshotId::generate().to_string().to_uppercase();
    let error = api.start_image_build(&id, &id, &request).await.unwrap_err();
    assert_eq!(error.code, 400);
    assert!(error.message.contains("volume.max_size_mb"));
    assert!(api.build_sessions.active.lock().unwrap().is_empty());
    assert!(api
        .build_journal()
        .await?
        .scan_prefix(b"build/".to_vec())
        .await?
        .is_empty());
    assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
    Ok(())
}

#[test]
fn buildkit_status_waits_for_cache_publication() -> Result<()> {
    let sessions = BuildSessions::default();
    let session = BuildSession::new();
    sessions
        .active
        .lock()
        .unwrap()
        .insert("build".to_owned(), session.clone());
    assert!(!sessions.is_finishing("build"));
    assert!(session.ready("127.0.0.1:1234".parse()?));
    session.publish().unwrap();
    assert!(sessions.is_finishing("build"));
    session.state.send_replace(SessionState::Finished(None));
    assert!(!sessions.is_finishing("build"));
    assert!(!sessions.is_finishing("missing"));
    Ok(())
}

#[test]
fn buildkit_readiness_comes_from_dockerfile_healthcheck() -> Result<()> {
    use serde_json::json;
    assert_eq!(dockerfile_ready_command(None)?, None);
    assert_eq!(
        dockerfile_ready_command(Some(&json!({"Healthcheck": {"Test": ["NONE"]}})))?,
        None
    );
    let shell =
        json!({"Healthcheck": {"Test": ["CMD-SHELL", "test -f /started && test -s /result.txt"]}});
    assert_eq!(
        dockerfile_ready_command(Some(&shell))?.unwrap(),
        "/bin/sh -c 'test -f /started && test -s /result.txt'"
    );
    let exec = json!({"Healthcheck": {"Test": ["CMD", "test", "$literal", "=", "$literal"]}});
    assert_eq!(
        dockerfile_ready_command(Some(&exec))?.unwrap(),
        "test '$literal' = '$literal'"
    );
    let bash = json!({"Shell": ["/bin/bash", "-c"], "Healthcheck": {"Test": ["CMD-SHELL", "[[ -f /started ]]"]}});
    assert_eq!(
        dockerfile_ready_command(Some(&bash))?.unwrap(),
        "/bin/bash -c '[[ -f /started ]]'"
    );
    assert!(dockerfile_ready_command(Some(&json!({"Healthcheck": {"Test": ["CMD"]}}))).is_err());
    Ok(())
}

#[test]
fn buildkit_startup_overrides_take_precedence_independently() -> Result<()> {
    use serde_json::json;
    let context = CommandContext::default()
        .with_entrypoint(Some(vec!["/server".into()]))
        .with_cmd(Some(vec!["--port".into(), "8080".into()]));
    let image = json!({"Healthcheck": {"Test": ["CMD", "test", "-f", "/ready"]}});
    for (start, ready) in [
        (None, None),
        (Some("exec /other"), None),
        (None, Some("test -f /other-ready")),
        (Some(""), Some("")),
    ] {
        let request = serde_json::from_value(json!({
            "startCmd": start, "readyCmd": ready,
        }))?;
        let commands = build_startup_commands(&request, &context, Some(&image))?;
        assert_eq!(
            commands.0.as_deref(),
            Some(start.unwrap_or("/server --port 8080"))
        );
        assert_eq!(
            commands.1.as_deref(),
            Some(ready.unwrap_or("test -f /ready"))
        );
    }
    // An explicit readiness command also bypasses unusable image health checks.
    let request = serde_json::from_value(json!({"readyCmd": "true"}))?;
    let invalid = json!({"Healthcheck": {"Test": ["CMD"]}});
    assert_eq!(
        build_startup_commands(&request, &context, Some(&invalid))?
            .1
            .as_deref(),
        Some("true")
    );
    Ok(())
}

#[test]
fn buildkit_publication_and_cancellation_are_mutually_exclusive() {
    for cancel_first in [false, true] {
        let session = BuildSession::new();
        assert_eq!(session.publish().unwrap_err().code, 409);
        assert!(session.ready("127.0.0.1:1234".parse().unwrap()));
        if cancel_first {
            session.request_cancel().unwrap();
            assert_eq!(session.publish().unwrap_err().code, 409);
            assert!(matches!(*session.state.borrow(), SessionState::Cancelled));
            assert!(!session.ready("127.0.0.1:1234".parse().unwrap()));
            session.request_cancel().unwrap();
        } else {
            session.publish().unwrap();
            assert!(matches!(&*session.state.borrow(), SessionState::Publishing));
            assert_eq!(session.publish().unwrap_err().code, 409);
            assert_eq!(session.request_cancel().unwrap_err().code, 409);
        }
    }
}

#[test]
fn concurrent_build_reservations_never_exceed_node_limit() {
    let sessions = BuildSessions::default();
    let limit = ConfigManager::global_config()
        .template_build
        .max_concurrent_builds;
    let barrier = std::sync::Barrier::new(limit + 8);
    let accepted = std::thread::scope(|scope| {
        let attempts = (0..limit + 8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    match sessions.reserve(&SnapshotId::generate().to_string()) {
                        Ok(_) => true,
                        Err(error) => {
                            assert_eq!(error.code, 429);
                            false
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        attempts
            .into_iter()
            .map(|attempt| usize::from(attempt.join().unwrap()))
            .sum::<usize>()
    });
    assert_eq!(accepted, limit);
    assert_eq!(sessions.active.lock().unwrap().len(), limit);
}

#[tokio::test]
async fn saturated_builder_api_leaves_template_waiting_without_allocating() -> Result<()> {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    let (_root, api, record) = test_api(VolumeLimits::default()).await?;
    let waiting = SnapshotRecord::template_waiting(SnapshotId::generate(), None, record.resources);
    api.snapshot_manager.create(waiting.clone()).await?;
    let limit = ConfigManager::global_config()
        .template_build
        .max_concurrent_builds;
    let mut active_ids = Vec::new();
    for _ in 0..limit {
        let id = SnapshotId::generate().to_string();
        api.build_sessions.reserve(&id).unwrap();
        active_ids.push(id);
    }
    let app = crate::api::server::new(Arc::new(api.clone()));
    for (id, expected) in [
        (waiting.id.to_string(), http::StatusCode::TOO_MANY_REQUESTS),
        (active_ids[0].clone(), http::StatusCode::CONFLICT),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(http::Method::PUT)
                    .uri(format!("/templates/{id}/builds/{id}/builder"))
                    .header("host", "localhost")
                    .header("content-type", "application/json")
                    .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                    .body(Body::from("{}"))?,
            )
            .await?;
        assert_eq!(response.status(), expected);
        let body = axum::body::to_bytes(response.into_body(), 4096).await?;
        let error: models::Error = serde_json::from_slice(&body)?;
        assert_eq!(error.code, i32::from(expected.as_u16()));
    }
    let saved = api
        .snapshot_manager
        .get(waiting.id.to_string())
        .await?
        .unwrap();
    assert_eq!(serde_json::to_value(saved)?, serde_json::to_value(waiting)?);
    assert!(api
        .build_journal()
        .await?
        .scan_prefix(b"build/".to_vec())
        .await?
        .is_empty());
    assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
    assert!(api
        .volume_manager
        .list_page(None, 100)
        .await?
        .records
        .is_empty());
    Ok(())
}

mod build_logs {
    use super::*;
    use crate::logging::LogLevel;
    use crate::template::logs::BuildLogs;
    use axum::body::{to_bytes, Body};
    use http::{Request, StatusCode};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    async fn get(
        app: &axum::Router,
        uri: &str,
        authenticated: bool,
    ) -> Result<(StatusCode, Value)> {
        let mut request = Request::builder().uri(uri).header("host", "localhost");
        if authenticated {
            request = request.header("x-api-key", "build-cleanup-test-api-key-0123456789");
        }
        let response = app.clone().oneshot(request.body(Body::empty())?).await?;
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024).await?;
        Ok((
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        ))
    }

    #[tokio::test]
    async fn build_logs_http_pagination_filtering_and_persistent_fallback() -> Result<()> {
        let (_root, api, record) = test_api(VolumeLimits::default()).await?;
        let record =
            SnapshotRecord::template_waiting(SnapshotId::generate(), None, record.resources);
        api.snapshot_manager.create(record.clone()).await?;
        let id = &record.id;
        let session = api.build_logs.start_with_flush_interval(
            id.clone(),
            api.snapshot_manager.repository(),
            Duration::from_secs(3600),
        );
        for n in 0..230 {
            session.logger.log(
                if n % 2 == 0 {
                    LogLevel::Info
                } else {
                    LogLevel::Warn
                },
                Some("2"),
                format!("line {n}"),
            );
        }
        let app = crate::api::server::new(Arc::new(api.clone()));
        let path = format!("/templates/{id}/builds/{id}");
        let (code, status) = get(&app, &format!("{path}/status"), true).await?;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(status["logs"], json!([]));
        assert_eq!(status["logEntries"].as_array().unwrap().len(), 100);
        assert_eq!(status["logEntries"][0]["message"], "line 0");
        let (_, next) = get(
            &app,
            &format!("{path}/status?logsOffset=100&limit=100"),
            true,
        )
        .await?;
        assert_eq!(next["logEntries"][0]["message"], "line 100");
        let (_, filtered) = get(
            &app,
            &format!("{path}/status?logsOffset=1&limit=1&level=warn"),
            true,
        )
        .await?;
        assert_eq!(filtered["logEntries"][0]["message"], "line 3");
        let (_, backward) = get(
            &app,
            &format!("{path}/logs?direction=backward&limit=2&level=warn"),
            true,
        )
        .await?;
        assert_eq!(backward["logs"][0]["message"], "line 229");
        let (_, empty) = get(&app, &format!("{path}/logs?limit=0"), true).await?;
        assert_eq!(empty, json!({"logs": []}));
        let (_, memory) = get(&app, &format!("{path}/logs?source=temporary"), true).await?;
        assert_eq!(memory["logs"].as_array().unwrap().len(), 100);
        assert_eq!(memory["logs"][0]["message"], "line 0");

        // Explicit persistent reads bypass the active local buffer. Default and
        // temporary reads prefer the live buffer on the build node.
        let repository = api.snapshot_manager.repository();
        let mut persisted = api.build_logs.temporary(id).unwrap();
        persisted[0].message = "persisted replacement".into();
        repository.write_build_logs(id, persisted).await?;
        for suffix in ["logs", "logs?source=temporary"] {
            let (_, result) = get(&app, &format!("{path}/{suffix}"), true).await?;
            assert_eq!(result["logs"][0]["message"], "line 0");
        }
        let (_, result) = get(&app, &format!("{path}/logs?source=persistent"), true).await?;
        assert_eq!(result["logs"][0]["message"], "persisted replacement");
        let (_, result) = get(&app, &format!("{path}/status"), true).await?;
        assert_eq!(result["logEntries"][0]["message"], "line 0");

        // A final new entry guarantees finish performs a final flush even if a
        // periodic flush raced with the persistent replacement above.
        session.logger.log(LogLevel::Info, Some("2"), "line 230");
        session.finish().await?;
        assert_eq!(
            get(&app, &format!("{path}/logs?source=temporary"), true)
                .await?
                .1,
            json!({"logs": []})
        );
        let (_, persistent) = get(&app, &format!("{path}/logs?source=persistent"), true).await?;
        assert_eq!(persistent["logs"][0]["message"], "line 0");
        assert_eq!(
            get(&app, &format!("{path}/logs"), true).await?.1,
            persistent
        );

        let mut other_node = api.clone();
        other_node.build_logs = BuildLogs::default();
        let other = crate::api::server::new(Arc::new(other_node));
        assert_eq!(
            get(&other, &format!("{path}/logs"), true).await?.1,
            persistent
        );
        assert_eq!(
            get(&other, &format!("{path}/logs?source=temporary"), true)
                .await?
                .1,
            json!({"logs": []})
        );
        let (_, tail) = get(&other, &format!("{path}/status?logsOffset=200"), true).await?;
        assert_eq!(tail["logEntries"].as_array().unwrap().len(), 31);
        assert_eq!(
            get(&other, &format!("{path}/status?logsOffset=231"), true)
                .await?
                .1["logEntries"],
            json!([])
        );
        Ok(())
    }

    #[tokio::test]
    async fn build_logs_http_validation_and_authentication() -> Result<()> {
        let (_root, api, record) = test_api(VolumeLimits::default()).await?;
        let record =
            SnapshotRecord::template_waiting(SnapshotId::generate(), None, record.resources);
        api.snapshot_manager.create(record.clone()).await?;
        let id = record.id;
        let app = crate::api::server::new(Arc::new(api));
        let path = format!("/templates/{id}/builds/{id}");
        for suffix in [
            "logs?cursor=-1",
            "logs?cursor=9223372036854775808",
            "logs?limit=-1",
            "logs?limit=101",
            "logs?direction=sideways",
            "logs?source=stdout",
            "logs?level=trace",
            "status?logsOffset=-1",
            "status?limit=101",
            "status?level=trace",
        ] {
            assert_eq!(
                get(&app, &format!("{path}/{suffix}"), true).await?.0,
                StatusCode::BAD_REQUEST,
                "{suffix}"
            );
        }
        for endpoint in ["logs", "status"] {
            assert_eq!(
                get(&app, &format!("{path}/{endpoint}"), false).await?.0,
                StatusCode::UNAUTHORIZED
            );
            assert_eq!(
                get(
                    &app,
                    &format!("/templates/other/builds/{id}/{endpoint}"),
                    true
                )
                .await?
                .0,
                StatusCode::NOT_FOUND
            );
            let absent = SnapshotId::generate();
            assert_eq!(
                get(
                    &app,
                    &format!("/templates/{absent}/builds/{absent}/{endpoint}"),
                    true
                )
                .await?
                .0,
                StatusCode::NOT_FOUND
            );
            assert_eq!(
                get(&app, &format!("/templates/bad/builds/bad/{endpoint}"), true)
                    .await?
                    .0,
                StatusCode::BAD_REQUEST
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn build_logs_terminal_status_does_not_wait_for_flush_and_includes_failed_step(
    ) -> Result<()> {
        let (_root, api, record) = test_api(VolumeLimits::default()).await?;
        let record =
            SnapshotRecord::template_waiting(SnapshotId::generate(), None, record.resources);
        api.snapshot_manager.create(record.clone()).await?;
        let id = &record.id;
        let session = api
            .build_logs
            .start(id.clone(), api.snapshot_manager.repository());
        session
            .logger
            .log(LogLevel::Info, Some("2"), "step started");
        session.logger.log(LogLevel::Error, Some("1"), "other step");
        session
            .logger
            .log(LogLevel::Warn, Some("2"), "failed output");
        api.snapshot_manager
            .mark_build_error(
                id,
                TemplateBuildErrorReason {
                    message: "failed".into(),
                    step: Some("2".into()),
                },
            )
            .await?;
        let app = crate::api::server::new(Arc::new(api.clone()));
        let path = format!("/templates/{id}/builds/{id}/status");
        assert_eq!(get(&app, &path, true).await?.1["status"], "error");
        session.finish().await?;
        let (_, info) = get(
            &app,
            &format!("{path}?logsOffset=100&limit=0&level=error"),
            true,
        )
        .await?;
        assert_eq!(info["status"], "error");
        assert_eq!(info["logEntries"], json!([]));
        assert_eq!(info["reason"]["logEntries"].as_array().unwrap().len(), 1);
        assert_eq!(info["reason"]["logEntries"][0]["message"], "failed output");
        Ok(())
    }
}
