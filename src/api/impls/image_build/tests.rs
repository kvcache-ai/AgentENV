use super::*;
use crate::api::impls::template_helpers::template_build_target_from_v3_request;
use crate::snapshot::{
    mock::{test_snapshot_manager, write_mock_built_artifacts},
    BuildErrorReason, BuildJobRecord, BuildStatus, BuildTarget, SnapshotAlias, SnapshotConfig,
    SnapshotRecord,
};
use crate::types::SandboxResources;
use crate::volume::{VolumeLimits, VolumeMode, VolumePurpose, VolumeRecord, VolumeStatus};
use agentenv_domain::BuildJobTransition;

pub(crate) async fn test_api(
    limits: VolumeLimits,
) -> Result<(tempfile::TempDir, ApiImpl, SnapshotRecord)> {
    use crate::{
        api_key::ApiKey,
        cfg::AppConfig,
        image::ImageResolver,
        orchestrator::{FileBackedSandboxPersister, InMemoryMetadataStore, Orchestrator},
        sandbox::FirecrackerSandboxFactory,
        template::TemplateBuilder,
        volume::VolumeManager,
    };

    let root = tempfile::tempdir()?;
    let (_server, manager) = test_snapshot_manager(root.path()).await;
    let manager = Arc::new(manager);
    let (_, _, manifest) = write_mock_built_artifacts(&root.path().join("artifacts"))?;
    let record = manager
        .publish(
            SnapshotId::generate(),
            Some(SnapshotAlias::parse("test-template")?),
            SnapshotConfig::mock(),
            manifest,
            None,
        )
        .await?;
    let orchestrator = Orchestrator::new(
        InMemoryMetadataStore::new(),
        FirecrackerSandboxFactory::new(),
        FileBackedSandboxPersister::new_for_test(root.path().join("sandboxes")),
    )
    .await?;
    let meta = manager.meta().clone();
    let volumes = VolumeManager::open_with_metadata_and_limits(
        root.path().join("volumes"),
        manager.repository(),
        meta.clone(),
        limits,
    )
    .await?;
    let resolver = ImageResolver::new(&AppConfig::default())
        .with_repository(manager.repository(), meta.clone());
    let api = ApiImpl::new(
        orchestrator,
        manager.clone(),
        Arc::new(TemplateBuilder::new()),
        Arc::new(resolver),
        Arc::new(volumes),
        meta,
        None,
        Vec::new(),
        ApiKey::new("build-cleanup-test-api-key-0123456789")?,
    );
    let journal =
        LocalKvStore::open(root.path().join("journal"), LocalStoreDurability::Memory).await?;
    api.build_sessions.journal.set(journal).unwrap();
    Ok((root, api, record))
}

async fn cache_volume(api: &ApiImpl, name: &str, mode: VolumeMode, owner: &str) -> Result<String> {
    let id = format!("vol_{}", uuid::Uuid::now_v7().simple());
    api.meta()
        .create_volume(
            VolumeRecord {
                id: id.clone(),
                name: name.into(),
                purpose: VolumePurpose::BuildCacheWork,
                mode,
                size_mb: 1024,
                status: VolumeStatus::Ready,
                reserved_by_sandbox_id: (mode == VolumeMode::Exclusive).then(|| owner.into()),
                backing_layers: Vec::new(),
                read_only_mounts: if mode == VolumeMode::ReadOnly {
                    vec![owner.into()]
                } else {
                    Vec::new()
                },
            },
            None,
            Vec::new(),
        )
        .await?;
    Ok(id)
}

async fn reopen_journal(api: &mut ApiImpl, path: &std::path::Path) -> Result<()> {
    api.build_sessions = Arc::new(BuildSessions::default());
    let journal = LocalKvStore::open(path, LocalStoreDurability::Sync).await?;
    api.build_sessions.journal.set(journal).unwrap();
    Ok(())
}

/// Publishes a distinct image description and returns its digest.
pub(crate) async fn publish_test_image(api: &ApiImpl) -> Result<String> {
    use crate::image::PublishedImage;
    use crate::layer::{LayerRef, ManagedLayerRef};
    let data = uuid::Uuid::now_v7().to_string();
    Ok(api
        .meta()
        .put_image(
            PublishedImage::new(
                "amd64".into(),
                vec![LayerRef::Managed(ManagedLayerRef {
                    digest: crate::digest::sha256_digest(data.as_bytes()),
                    size: data.len() as u64,
                    uuid: None,
                })],
                serde_json::json!({}),
            ),
            Vec::new(),
        )
        .await?)
}

/// Creates a waiting imageOnly build of `web` and returns its ID.
async fn image_build(api: &ApiImpl, tags: &[&str]) -> Result<SnapshotId> {
    let id = SnapshotId::generate();
    api.meta()
        .create_build_job(
            id.clone(),
            BuildTarget::Image {
                name: "web".to_owned(),
                tags: tags.iter().map(|tag| tag.to_string()).collect(),
            },
        )
        .await?;
    Ok(id)
}

/// Creates a waiting template build job and returns its ID.
async fn template_build(
    api: &ApiImpl,
    alias: Option<&str>,
    resources: SandboxResources,
) -> Result<SnapshotId> {
    let id = SnapshotId::generate();
    let alias = alias.map(SnapshotAlias::parse).transpose()?;
    api.meta()
        .create_build_job(id.clone(), BuildTarget::Template { alias, resources })
        .await?;
    Ok(id)
}

async fn build_job(api: &ApiImpl, id: &SnapshotId) -> Result<Option<BuildJobRecord>> {
    Ok(api.meta().get_build_job(id.clone()).await?)
}

async fn start_build(api: &ApiImpl, id: &SnapshotId) -> Result<bool> {
    Ok(api
        .meta()
        .update_build_job(id.clone(), BuildJobTransition::Start)
        .await?
        .is_some())
}

async fn finish_image_job(api: &ApiImpl, id: &SnapshotId, digest: &str) -> Result<()> {
    api.meta()
        .update_build_job(
            id.clone(),
            BuildJobTransition::FinishImage {
                digest: digest.to_owned(),
            },
        )
        .await?;
    Ok(())
}

async fn fail_build(api: &ApiImpl, id: &SnapshotId, reason: BuildErrorReason) -> Result<()> {
    api.meta()
        .update_build_job(id.clone(), BuildJobTransition::Fail { reason })
        .await?;
    Ok(())
}

async fn send(
    api: &ApiImpl,
    method: http::Method,
    uri: &str,
    body: Option<serde_json::Value>,
) -> Result<(http::StatusCode, serde_json::Value)> {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let response = crate::api::server::new(Arc::new(api.clone()))
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("host", "localhost")
                .header("content-type", "application/json")
                .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))?,
        )
        .await?;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok((
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    ))
}

#[tokio::test]
async fn image_only_builds_are_created_through_template_creation() -> Result<()> {
    use serde_json::json;
    let (_root, api, _) = test_api(VolumeLimits::default()).await?;
    for (body, names, tags) in [
        (
            json!({"imageOnly": true, "name": "web:v1", "tags": ["latest", "v1"]}),
            json!(["web"]),
            json!(["v1", "latest"]),
        ),
        (
            json!({"imageOnly": true, "name": "web"}),
            json!(["web"]),
            json!(["latest"]),
        ),
    ] {
        let (status, created) = send(&api, http::Method::POST, "/v3/templates", Some(body)).await?;
        assert_eq!(status, http::StatusCode::ACCEPTED, "{created}");
        assert_eq!(created["names"], names);
        assert_eq!(created["tags"], tags);
        assert_eq!(created["aliases"], json!([]));
        let id = created["buildID"].as_str().unwrap().to_owned();
        assert_eq!(created["templateID"], id);
        let job = build_job(&api, &SnapshotId::parse(&id)?).await?.unwrap();
        assert_eq!(job.status, BuildStatus::Waiting);
        match job.target {
            BuildTarget::Image {
                name,
                tags: stored_tags,
            } => {
                assert_eq!(name, "web");
                assert_eq!(json!(stored_tags), tags);
            }
            BuildTarget::Template { .. } => panic!("expected image build target"),
        }
        let (status, info) = send(
            &api,
            http::Method::GET,
            &format!("/templates/{id}/builds/{id}/status"),
            None,
        )
        .await?;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(info["status"], "waiting");
        assert!(info.get("imageDigest").is_none());
        // An image build is not a template.
        let (status, _) = send(&api, http::Method::GET, &format!("/templates/{id}"), None).await?;
        assert_eq!(status, http::StatusCode::NOT_FOUND);
        assert!(api.snapshot_manager.get(&id).await?.is_none());
    }
    for body in [
        json!({"imageOnly": true}),
        json!({"imageOnly": true, "name": "Web"}),
        json!({"imageOnly": true, "name": "web:.v1"}),
        json!({"imageOnly": true, "name": "web", "tags": ["v/1"]}),
        json!({"imageOnly": true, "name": "web", "cpuCount": 2}),
        json!({"imageOnly": true, "name": "web", "memoryMB": 512}),
    ] {
        let (status, _) = send(
            &api,
            http::Method::POST,
            "/v3/templates",
            Some(body.clone()),
        )
        .await?;
        assert_eq!(status, http::StatusCode::BAD_REQUEST, "{body}");
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_same_alias_template_builds_resolve_at_commit_and_reject_active_alias(
) -> Result<()> {
    use serde_json::json;
    let (root, api, _) = test_api(VolumeLimits::default()).await?;

    // Two concurrent POST /v3/templates for the same unbuilt alias both succeed.
    let (s1, b1) = send(
        &api,
        http::Method::POST,
        "/v3/templates",
        Some(json!({"name": "shared-alias"})),
    )
    .await?;
    let (s2, b2) = send(
        &api,
        http::Method::POST,
        "/v3/templates",
        Some(json!({"name": "shared-alias"})),
    )
    .await?;
    assert_eq!(s1, http::StatusCode::ACCEPTED, "{b1}");
    assert_eq!(s2, http::StatusCode::ACCEPTED, "{b2}");
    let id1 = SnapshotId::parse(b1["buildID"].as_str().unwrap())?;
    let id2 = SnapshotId::parse(b2["buildID"].as_str().unwrap())?;
    assert_ne!(id1, id2);

    // Waiting build jobs do not appear in template lists or alias resolution.
    let (st, list) = send(&api, http::Method::GET, "/templates", None).await?;
    assert_eq!(st, http::StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1); // only seed "test-template"
    let (st, _) = send(
        &api,
        http::Method::GET,
        "/templates/aliases/shared-alias",
        None,
    )
    .await?;
    assert_eq!(st, http::StatusCode::NOT_FOUND);
    let (st, _) = send(&api, http::Method::GET, "/templates/shared-alias", None).await?;
    assert_eq!(st, http::StatusCode::NOT_FOUND);

    // Both jobs are queryable by UUID on GET /templates/{id} and /status.
    for id in [&id1, &id2] {
        let (st, tpl) = send(&api, http::Method::GET, &format!("/templates/{id}"), None).await?;
        assert_eq!(st, http::StatusCode::OK);
        assert_eq!(tpl["templateID"], id.to_string());
        assert_eq!(tpl["builds"][0]["status"], "waiting");

        let (st, info) = send(
            &api,
            http::Method::GET,
            &format!("/templates/{id}/builds/{id}/status"),
            None,
        )
        .await?;
        assert_eq!(st, http::StatusCode::OK);
        assert_eq!(info["status"], "waiting");
    }

    // Winner publishes cleanly and atomically marks build_jobs ready.
    assert!(start_build(&api, &id1).await?);
    assert!(start_build(&api, &id2).await?);
    let (_, _, manifest1) = write_mock_built_artifacts(&root.path().join("win"))?;
    api.snapshot_manager
        .publish(
            id1.clone(),
            Some(SnapshotAlias::parse("shared-alias")?),
            SnapshotConfig::mock(),
            manifest1,
            None,
        )
        .await?;

    let (st, info1) = send(
        &api,
        http::Method::GET,
        &format!("/templates/{id1}/builds/{id1}/status"),
        None,
    )
    .await?;
    assert_eq!(st, http::StatusCode::OK);
    assert_eq!(info1["status"], "ready");
    let (st, alias_resp) = send(
        &api,
        http::Method::GET,
        "/templates/aliases/shared-alias",
        None,
    )
    .await?;
    assert_eq!(st, http::StatusCode::OK);
    assert_eq!(alias_resp["templateID"], id1.to_string());

    // Loser hits AliasConflict on final commit.
    let (_, _, manifest2) = write_mock_built_artifacts(&root.path().join("lose"))?;
    let err = api
        .snapshot_manager
        .publish(
            id2.clone(),
            Some(SnapshotAlias::parse("shared-alias")?),
            SnapshotConfig::mock(),
            manifest2,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        crate::repository::RepositoryError::AliasConflict { .. }
    ));

    // Once an active snapshot owns the alias, a new POST /v3/templates fails fast.
    let (st, _) = send(
        &api,
        http::Method::POST,
        "/v3/templates",
        Some(json!({"name": "shared-alias"})),
    )
    .await?;
    assert_eq!(st, http::StatusCode::BAD_REQUEST);

    // Deleting a waiting build reserves its snapshot ID tombstone to block late publish.
    let (st, b3) = send(
        &api,
        http::Method::POST,
        "/v3/templates",
        Some(json!({"name": "deleted-before-publish"})),
    )
    .await?;
    assert_eq!(st, http::StatusCode::ACCEPTED);
    let id3 = SnapshotId::parse(b3["buildID"].as_str().unwrap())?;
    let (st, _) = send(
        &api,
        http::Method::DELETE,
        &format!("/templates/{id3}"),
        None,
    )
    .await?;
    assert_eq!(st, http::StatusCode::NO_CONTENT);
    let (_, _, manifest3) = write_mock_built_artifacts(&root.path().join("late"))?;
    assert!(api
        .snapshot_manager
        .publish(
            id3.clone(),
            Some(SnapshotAlias::parse("deleted-before-publish")?),
            SnapshotConfig::mock(),
            manifest3,
            None,
        )
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
async fn image_only_builder_requires_a_waiting_build_without_startup_overrides() -> Result<()> {
    let (_root, api, _) = test_api(VolumeLimits::default()).await?;
    let waiting = image_build(&api, &["v1"]).await?;
    let started = image_build(&api, &["v1"]).await?;
    // A claim by another node must exclude builder allocation.
    assert!(start_build(&api, &started).await?);
    for (id, body, expected) in [
        (
            waiting.clone(),
            serde_json::json!({"startCmd": "serve"}),
            http::StatusCode::BAD_REQUEST,
        ),
        (
            waiting.clone(),
            serde_json::json!({"readyCmd": "true"}),
            http::StatusCode::BAD_REQUEST,
        ),
        (
            started.clone(),
            serde_json::json!({}),
            http::StatusCode::CONFLICT,
        ),
    ] {
        let (status, _) = send(
            &api,
            http::Method::PUT,
            &format!("/templates/{id}/builds/{id}/builder"),
            Some(body),
        )
        .await?;
        assert_eq!(status, expected);
    }
    assert!(api.build_sessions.active.lock().unwrap().is_empty());
    assert_eq!(
        build_job(&api, &waiting).await?.unwrap().status,
        BuildStatus::Waiting
    );
    assert_eq!(
        build_job(&api, &started).await?.unwrap().status,
        BuildStatus::Building
    );
    Ok(())
}

#[tokio::test]
async fn image_only_cleanup_records_failures_in_the_shared_build_record() -> Result<()> {
    let digest = format!("sha256:{}", "ab".repeat(32));
    for outcome in ["published", "failure", "restart"] {
        let (root, mut api, existing) = test_api(VolumeLimits::default()).await?;
        let journal_path = root.path().join("durable-journal");
        reopen_journal(&mut api, &journal_path).await?;
        let build_id = image_build(&api, &["v1"]).await?;
        assert!(start_build(&api, &build_id).await?);
        let id = build_id.to_string();
        let entry = BuildJournal {
            cache: cache_volume(&api, "image-only-work", VolumeMode::Exclusive, &id).await?,
            parent: None,
        };
        entry.persist(api.build_journal().await?, &id).await?;
        let route = SandboxId::parse_str(&id)?;
        api.orchestrator.register_template_build(route).await;
        if outcome == "published" {
            finish_image_job(&api, &build_id, &digest).await?;
        }
        let mut v2_interrupted = None;
        let mut v2_deferred_err = None;
        if outcome == "restart" {
            let v2_id = template_build(&api, None, SandboxResources::default()).await?;
            assert!(start_build(&api, &v2_id).await?);
            api.build_journal()
                .await?
                .put(format!("v2build/{v2_id}"), Vec::new())
                .await?;
            v2_interrupted = Some(v2_id);

            let v2_err_id = template_build(&api, None, SandboxResources::default()).await?;
            assert!(start_build(&api, &v2_err_id).await?);
            api.build_journal()
                .await?
                .put(
                    format!("v2build/{v2_err_id}"),
                    serde_json::to_vec(&BuildErrorReason::new("image pull failed"))?,
                )
                .await?;
            v2_deferred_err = Some(v2_err_id);
            reopen_journal(&mut api, &journal_path).await?;
        } else {
            let session = BuildSession::new();
            session.state.send_replace(SessionState::Finished(
                (outcome == "failure").then(|| BuildErrorReason::new("push failed")),
            ));
            api.build_sessions
                .active
                .lock()
                .unwrap()
                .insert(id.clone(), session);
        }
        api.recover_image_builds().await?;
        if let Some(v2_id) = v2_interrupted {
            assert!(api
                .build_journal()
                .await?
                .get(format!("v2build/{v2_id}"))
                .await?
                .is_none());
            let v2_job = build_job(&api, &v2_id).await?.unwrap();
            assert_eq!(v2_job.status, BuildStatus::Error);
            assert_eq!(
                v2_job.error_reason.unwrap().message,
                "build interrupted by server restart"
            );
        }
        if let Some(v2_err_id) = v2_deferred_err {
            assert!(api
                .build_journal()
                .await?
                .get(format!("v2build/{v2_err_id}"))
                .await?
                .is_none());
            let v2_job = build_job(&api, &v2_err_id).await?.unwrap();
            assert_eq!(v2_job.status, BuildStatus::Error);
            assert_eq!(v2_job.error_reason.unwrap().message, "image pull failed");
        }
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
        // Like a template build, a finished image build keeps no routing.
        assert!(!api.orchestrator.list_sandbox_ids().await?.contains(&route));
        let job = build_job(&api, &build_id).await?.unwrap();
        match outcome {
            "published" => {
                assert_eq!(job.status, BuildStatus::Ready);
                assert_eq!(job.result_image_digest.as_deref(), Some(digest.as_str()));
            }
            _ => {
                assert_eq!(job.status, BuildStatus::Error);
                let expected = if outcome == "failure" {
                    "push failed"
                } else {
                    "build interrupted by server restart"
                };
                assert_eq!(job.error_reason.unwrap().message, expected);
                assert!(job.result_image_digest.is_none());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn image_build_status_and_deletion_use_template_endpoints() -> Result<()> {
    let (_root, api, _) = test_api(VolumeLimits::default()).await?;
    let meta = api.meta();
    let digest = publish_test_image(&api).await?;
    let build_id = image_build(&api, &["v1"]).await?;
    assert!(start_build(&api, &build_id).await?);
    finish_image_job(&api, &build_id, &digest).await?;
    meta.put_image_tag(ImageTag::parse("web:v1")?, digest.clone(), None)
        .await?;
    let id = build_id.to_string();
    let status_uri = format!("/templates/{id}/builds/{id}/status");

    // A sequential build must observe the new cache seed before ready.
    let session = BuildSession::new();
    session.state.send_replace(SessionState::Publishing);
    api.build_sessions
        .active
        .lock()
        .unwrap()
        .insert(id.clone(), session.clone());
    let (status, info) = send(&api, http::Method::GET, &status_uri, None).await?;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(info["status"], "building");
    assert!(info.get("imageDigest").is_none());
    let (status, _) = send(
        &api,
        http::Method::DELETE,
        &format!("/templates/{id}"),
        None,
    )
    .await?;
    assert_eq!(status, http::StatusCode::CONFLICT);
    api.build_sessions.active.lock().unwrap().remove(&id);

    let (status, info) = send(&api, http::Method::GET, &status_uri, None).await?;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(info["status"], "ready");
    assert_eq!(info["imageDigest"], digest);
    assert_eq!(info["templateID"], id);

    let (status, _) = send(
        &api,
        http::Method::DELETE,
        &format!("/templates/{id}"),
        None,
    )
    .await?;
    assert_eq!(status, http::StatusCode::NO_CONTENT);
    assert!(build_job(&api, &build_id).await?.is_none());
    let (status, _) = send(&api, http::Method::GET, &status_uri, None).await?;
    assert_eq!(status, http::StatusCode::NOT_FOUND);
    // Deleting the build keeps the image it published and its tags.
    assert!(meta.get_image(digest.clone()).await?.is_some());
    assert_eq!(
        meta.get_image_tag(ImageTag::parse("web:v1")?)
            .await?
            .map(|(record, _)| record.image_digest),
        Some(digest)
    );
    Ok(())
}

#[tokio::test]
async fn image_build_finalization_retains_logs_without_a_template() -> Result<()> {
    use crate::template::logs::read_build_logs;
    let (_root, api, _) = test_api(VolumeLimits::default()).await?;
    let build_id = image_build(&api, &["v1"]).await?;
    assert!(start_build(&api, &build_id).await?);
    let id = build_id.to_string();
    BuildJournal {
        cache: "missing-cache".into(),
        parent: None,
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
        build_id.clone(),
        api.snapshot_manager.repository(),
        Duration::from_secs(3600),
    );
    logs.logger
        .log(crate::logging::LogLevel::Info, None, "exported image");
    api.supervise_image_build(&build_id, &session, logs, async {
        Err(anyhow::anyhow!("publication failed"))
    })
    .await;
    assert!(api.snapshot_manager.get(&id).await?.is_none());
    assert_eq!(
        read_build_logs(api.snapshot_manager.repository().as_ref(), &build_id)
            .await?
            .len(),
        1
    );
    let job = build_job(&api, &build_id).await?.unwrap();
    assert_eq!(job.status, BuildStatus::Error);
    assert_eq!(job.error_reason.unwrap().message, "publication failed");
    assert!(api.build_logs.temporary(&build_id).is_none());
    assert!(!api.build_sessions.contains(&id));
    Ok(())
}

#[tokio::test]
async fn buildkit_cleanup_retries_preserve_status_and_release_all_resources() -> Result<()> {
    use std::sync::atomic::Ordering;

    for (cancel_retry, succeeded) in [(false, true), (true, true), (false, false), (true, false)] {
        let (_root, api, record) = test_api(VolumeLimits::default()).await?;
        let build_id = if succeeded {
            record.id.clone()
        } else {
            let id = template_build(&api, None, record.config.resources).await?;
            assert!(start_build(&api, &id).await?);
            id
        };
        let id = build_id.to_string();
        let key = format!("build/{id}").into_bytes();
        let entry = BuildJournal {
            cache: cache_volume(&api, "work", VolumeMode::Exclusive, &id).await?,
            parent: Some(cache_volume(&api, "parent", VolumeMode::ReadOnly, &id).await?),
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
        // Inject a cache-collection failure after worker and parent leases are released.
        api.build_sessions
            .fail_cache_cleanup
            .store(true, Ordering::SeqCst);
        api.finish_image_build(
            &build_id,
            &session,
            if succeeded {
                Ok(())
            } else {
                Err(anyhow::anyhow!("original build failure"))
            },
        )
        .await;

        if succeeded {
            assert!(api.snapshot_manager.get(&id).await?.is_some());
        } else {
            let job = build_job(&api, &build_id).await?.unwrap();
            assert_eq!(job.status, BuildStatus::Error);
        }
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
        api.build_sessions
            .fail_cache_cleanup
            .store(false, Ordering::SeqCst);
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
        assert!(api.volume_manager.list_page(None, 100).await?.0.is_empty());
        assert!(api
            .meta()
            .list_volumes(Vec::new(), None, 100)
            .await?
            .0
            .is_empty());
        assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
        if succeeded {
            assert!(api.snapshot_manager.get(&id).await?.is_some());
        } else {
            let job = build_job(&api, &build_id).await?.unwrap();
            assert_eq!(job.status, BuildStatus::Error);
            assert_eq!(
                job.error_reason
                    .as_ref()
                    .map(|reason| reason.message.as_str()),
                Some("original build failure")
            );
        }
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
    let target = template_build_target_from_v3_request(&request, "interrupted").unwrap();
    let interrupted_id = SnapshotId::generate();
    api.meta()
        .create_build_job(interrupted_id.clone(), target)
        .await?;
    entry.persist(journal, &interrupted_id.to_string()).await?;

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
        .get(format!("build/{interrupted_id}"))
        .await?
        .is_none());
    let saved = build_job(&api, &interrupted_id).await?.unwrap();
    assert_eq!(saved.status, BuildStatus::Error);
    Ok(())
}

#[tokio::test]
async fn buildkit_worker_panic_releases_journal_and_scheduler_binding() -> Result<()> {
    let (_root, api, record) = test_api(VolumeLimits::default()).await?;
    let build_id = template_build(&api, None, record.config.resources).await?;
    assert!(start_build(&api, &build_id).await?);
    let id = build_id.to_string();
    let entry = BuildJournal {
        cache: cache_volume(&api, "work", VolumeMode::Exclusive, &id).await?,
        parent: None,
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
        .start(build_id.clone(), api.snapshot_manager.repository());
    api.supervise_image_build(&build_id, &session, logs, async {
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
    assert!(api.volume_manager.list_page(None, 100).await?.0.is_empty());
    assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
    let saved = build_job(&api, &build_id).await?.unwrap();
    assert_eq!(saved.status, BuildStatus::Error);
    Ok(())
}

#[tokio::test]
async fn build_session_route_preserves_template_named_builds() -> Result<()> {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    let (root, api, _) = test_api(VolumeLimits::default()).await?;
    let (_, _, manifest) = write_mock_built_artifacts(&root.path().join("builds-template"))?;
    let record = api
        .snapshot_manager
        .publish(
            SnapshotId::generate(),
            Some(SnapshotAlias::parse("builds")?),
            SnapshotConfig::mock(),
            manifest,
            None,
        )
        .await?;
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
    let waiting = template_build(&api, None, record.config.resources).await?;
    // A claim by another node or the existing build API must exclude builder allocation.
    assert!(start_build(&api, &waiting).await?);
    let app = crate::api::server::new(Arc::new(api.clone()));
    for (id, expected) in [
        (SnapshotId::generate(), http::StatusCode::NOT_FOUND),
        (record.id.clone(), http::StatusCode::CONFLICT),
        (waiting.clone(), http::StatusCode::CONFLICT),
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
        build_job(&api, &waiting).await?.unwrap().status,
        BuildStatus::Building
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
                &models::TemplatesTemplateIdDeleteQueryParams { r#type: None },
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
            &models::TemplatesTemplateIdDeleteQueryParams { r#type: None },
        )
        .await
        .unwrap();
    assert!(matches!(
        response,
        TemplatesTemplateIdDeleteResponse::Status204_TheTemplateWasDeletedSuccessfully
    ));
    assert!(api.snapshot_manager.get(&id).await?.is_none());

    // Cross-node DELETE hitting a peer whose local `build_sessions` has no entry must still
    // reject an active `building` job atomically in SQL with 409 Conflict.
    let remote_build = SnapshotId::generate();
    api.meta()
        .create_build_job(
            remote_build.clone(),
            agentenv_domain::BuildTarget::Template {
                alias: None,
                resources: Default::default(),
            },
        )
        .await?;
    api.meta()
        .update_build_job(
            remote_build.clone(),
            agentenv_domain::BuildJobTransition::Start,
        )
        .await?;
    let cross_node = api
        .templates_template_id_delete(
            &http::Method::DELETE,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &super::super::Claims,
            &models::TemplatesTemplateIdDeletePathParams {
                template_id: remote_build.to_string(),
            },
            &models::TemplatesTemplateIdDeleteQueryParams { r#type: None },
        )
        .await
        .unwrap();
    assert!(matches!(
        cross_node,
        TemplatesTemplateIdDeleteResponse::Status409_Conflict(_)
    ));
    assert!(api.meta().get_build_job(remote_build).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn template_deletion_is_idempotent_after_retire_and_cleans_build_logs() -> Result<()> {
    use crate::template::logs::{read_build_logs, write_build_logs};
    use agentenv_http_server::apis::templates::{Templates, TemplatesTemplateIdDeleteResponse};
    use axum_extra::extract::CookieJar;
    use headers::Host;

    let (_root, api, record) = test_api(VolumeLimits::default()).await?;
    let alias = record.alias.clone().unwrap();
    write_build_logs(
        api.snapshot_manager.repository().as_ref(),
        &record.id,
        vec![crate::template::logs::BuildLogEntry {
            timestamp: chrono::Utc::now(),
            level: crate::logging::LogLevel::Info,
            message: "build line".into(),
            step: None,
        }],
    )
    .await?;
    api.snapshot_manager.delete(record.id.to_string()).await?;
    assert!(api.snapshot_manager.get(alias.as_ref()).await?.is_none());

    // Calling DELETE again by ID stays idempotent and removes remaining build logs.
    let response = api
        .templates_template_id_delete(
            &http::Method::DELETE,
            &Host::from(http::uri::Authority::from_static("localhost")),
            &CookieJar::new(),
            &super::super::Claims,
            &models::TemplatesTemplateIdDeletePathParams {
                template_id: record.id.to_string(),
            },
            &models::TemplatesTemplateIdDeleteQueryParams { r#type: None },
        )
        .await
        .unwrap();
    assert!(matches!(
        response,
        TemplatesTemplateIdDeleteResponse::Status204_TheTemplateWasDeletedSuccessfully
    ));
    assert!(
        read_build_logs(api.snapshot_manager.repository().as_ref(), &record.id)
            .await?
            .is_empty()
    );
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
    let waiting = template_build(&api, None, record.config.resources).await?;
    let before = build_job(&api, &waiting).await?.unwrap();
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
        (waiting.to_string(), http::StatusCode::TOO_MANY_REQUESTS),
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
    let saved = build_job(&api, &waiting).await?.unwrap();
    assert_eq!(saved, before);
    assert!(api
        .build_journal()
        .await?
        .scan_prefix(b"build/".to_vec())
        .await?
        .is_empty());
    assert!(api.orchestrator.list_sandbox_ids().await?.is_empty());
    assert!(api.volume_manager.list_page(None, 100).await?.0.is_empty());
    Ok(())
}

mod build_logs {
    use super::*;
    use crate::logging::LogLevel;
    use crate::template::logs::{write_build_logs, BuildLogs};
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
        let id = template_build(&api, None, record.config.resources).await?;
        let id = &id;
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
        write_build_logs(repository.as_ref(), id, persisted).await?;
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
        let id = template_build(&api, None, record.config.resources).await?;
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
        let id = template_build(&api, None, record.config.resources).await?;
        let id = &id;
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
        fail_build(
            &api,
            id,
            BuildErrorReason {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disconnected_client_does_not_leak_pending_volume_lease() -> Result<()> {
        use crate::layer::{LayerRef, ManagedLayerRef};
        use crate::repository::{BlobKey, BlobLocation, Repository, RepositoryResult};
        use crate::volume::VolumeManager;
        use std::sync::Mutex as StdMutex;
        use tokio::io::AsyncWriteExt;

        struct BlockingLocateRepo {
            inner: Arc<dyn Repository>,
            hit: StdMutex<Option<tokio::sync::oneshot::Sender<()>>>,
            release: StdMutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        }

        #[async_trait::async_trait]
        impl Repository for BlockingLocateRepo {
            fn locate_blob(&self, key: &BlobKey) -> RepositoryResult<BlobLocation> {
                if matches!(key, BlobKey::Layer { .. }) {
                    if let Some(tx) = self.hit.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    if let Some(rx) = self.release.lock().unwrap().take() {
                        let _ = tokio::task::block_in_place(|| rx.blocking_recv());
                    }
                }
                self.inner.locate_blob(key)
            }
        }

        let (root, mut api, _) = test_api(VolumeLimits::default()).await?;
        let (_, _, mut manifest) =
            write_mock_built_artifacts(&root.path().join("disconnect-artifacts"))?;
        manifest.volume_drive_slots = 4;
        let record = api
            .snapshot_manager
            .publish(
                SnapshotId::generate(),
                Some(SnapshotAlias::parse("disconnect-template")?),
                SnapshotConfig::mock(),
                manifest,
                None,
            )
            .await?;

        let (hit_tx, hit_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let blocking_repo: Arc<dyn Repository> = Arc::new(BlockingLocateRepo {
            inner: api.snapshot_manager.repository(),
            hit: StdMutex::new(Some(hit_tx)),
            release: StdMutex::new(Some(release_rx)),
        });
        let volumes = VolumeManager::open_with_metadata_and_limits(
            root.path().join("volumes-disconnect"),
            blocking_repo,
            api.meta().clone(),
            VolumeLimits::default(),
        )
        .await?;
        api.volume_manager = Arc::new(volumes);

        let digest = crate::digest::sha256_digest(b"exclusive-volume-disconnect-test");
        let volume = api
            .volume_manager
            .create_from_snapshot(
                "disconnect-exclusive".into(),
                VolumeMode::Exclusive,
                1024,
                vec![LayerRef::Managed(ManagedLayerRef {
                    digest,
                    size: 4096,
                    uuid: None,
                })],
            )
            .await?;

        let app = crate::api::server::new(Arc::new(api.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server_task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let body = serde_json::json!({
            "templateID": record.id.to_string(),
            "volumeMounts": [{ "name": volume.id, "path": "/mnt/data" }]
        })
        .to_string();
        let raw_req = format!(
            "POST /sandboxes HTTP/1.1\r\nHost: {addr}\r\nX-API-Key: build-cleanup-test-api-key-0123456789\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        stream.write_all(raw_req.as_bytes()).await?;
        stream.flush().await?;

        // Wait until SQL lease is committed and handler is blocked in materialize_backing.
        tokio::time::timeout(Duration::from_secs(5), hit_rx)
            .await
            .expect("handler should reach volume materialization")?;
        let leased = api.volume_manager.get(&volume.id).await?;
        assert!(
            leased
                .reserved_by_sandbox_id
                .as_deref()
                .is_some_and(|owner| owner.starts_with("pending-")),
            "expected pending lease before client disconnect, got {:?}",
            leased.reserved_by_sandbox_id
        );

        // Drop the HTTP/1.1 client connection while the request is in flight.
        drop(stream);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = release_tx.send(());

        // Detached task must release the pending lease so the volume is reusable/deletable.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let current = api.volume_manager.get(&volume.id).await?;
            if current.reserved_by_sandbox_id.is_none() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "volume remained stuck on lease {:?}",
                current.reserved_by_sandbox_id
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let app = crate::api::server::new(Arc::new(api.clone()));
        let (sbx_status, sbx_body) = get(&app, "/sandboxes", true).await?;
        assert_eq!(sbx_status, StatusCode::OK);
        assert_eq!(sbx_body, serde_json::json!([]));

        let (vol_status, vol_body) = get(&app, &format!("/volumes/{}", volume.id), true).await?;
        assert_eq!(vol_status, StatusCode::OK);
        assert_eq!(vol_body["status"], "ready");

        let delete_resp = app
            .oneshot(
                Request::builder()
                    .method(http::Method::DELETE)
                    .uri(format!("/volumes/{}", volume.id))
                    .header("host", "localhost")
                    .header("x-api-key", "build-cleanup-test-api-key-0123456789")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(delete_resp.status(), StatusCode::NO_CONTENT);

        server_task.abort();
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn only_failed_image_builds_wait_for_buildctl_to_receive_the_error() {
    let session = BuildSession::new();
    // An open BuildKit tunnel holds a connection read lock.
    let _tunnel = session.connections.clone().read_owned().await;
    for (image_only, failed, waits) in [
        (true, true, true),
        (false, true, false),
        (true, false, false),
    ] {
        let started = Instant::now();
        let result = if failed {
            Err(anyhow::anyhow!("push failed"))
        } else {
            Ok(())
        };
        let result = deliver_build_failure(&session, image_only, result).await;
        assert_eq!(result.is_err(), failed);
        assert_eq!(
            started.elapsed() >= Duration::from_secs(10),
            waits,
            "image_only={image_only} failed={failed}"
        );
    }
}
