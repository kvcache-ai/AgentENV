use super::*;
use crate::api::impls::template_helpers::template_build_record_from_v3_request;
use crate::snapshot::{SnapshotSource, TemplateBuildStatus};
use crate::volume::{VolumeLimits, VolumeMode, VolumeRecord, VolumeStatus};

async fn test_api(limits: VolumeLimits) -> Result<(tempfile::TempDir, ApiImpl, SnapshotRecord)> {
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
    let record = manager.publish(metadata, manifest).await?;
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
    let api = ApiImpl::new(
        orchestrator,
        manager.clone(),
        Arc::new(TemplateBuilder::new()),
        Arc::new(ImageResolver::new(&AppConfig::default())),
        Arc::new(volumes),
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
        };
        entry.persist(api.build_journal().await?, &id).await?;
        let session = BuildSession::new();
        session.state.send_replace(SessionState::Publishing);
        api.build_sessions
            .active
            .insert(id.clone(), session.clone());
        api.orchestrator
            .register_template_build(SandboxId::parse_str(&id)?)
            .await;

        // An unreadable cache-head record fails cleanup after worker and parent leases are released.
        let fault = root
            .path()
            .join("repository/template-build/cache-head.json");
        tokio::fs::create_dir_all(&fault).await?;
        api.finish_image_build(
            &record,
            &session,
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
        assert!(api.build_sessions.active.contains_key(&id));
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
        assert!(!api.build_sessions.active.contains_key(&id));
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
    };
    let live_id = SnapshotId::generate().to_string();
    let live = BuildSession::new();
    api.build_sessions
        .active
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
    };
    entry.persist(api.build_journal().await?, &id).await?;
    let session = BuildSession::new();
    api.build_sessions
        .active
        .insert(id.clone(), session.clone());
    api.orchestrator
        .register_template_build(SandboxId::parse_str(&id)?)
        .await;
    api.supervise_image_build(&record, &session, async {
        panic!("injected worker panic");
    })
    .await;
    assert!(
        matches!(&*session.state.borrow(), SessionState::Finished(Some(reason)) if reason.message == "build worker panicked")
    );
    assert!(!api.build_sessions.active.contains_key(&id));
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
    assert!(api.build_sessions.active.is_empty());
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
    api.build_sessions.active.remove(&id);
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
    let error = api
        .allocate_image_build(SnapshotId::generate(), request)
        .await
        .unwrap_err();
    assert_eq!(error.code, 400);
    assert!(error.message.contains("volume.max_size_mb"));
    assert!(api.build_sessions.active.is_empty());
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
    sessions.active.insert("build".to_owned(), session.clone());
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
