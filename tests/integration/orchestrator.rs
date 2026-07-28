use crate::common;

use agentenv::cfg::ConfigManager;
use agentenv::orchestrator::{
    CreateSandboxRequest, FileBackedSandboxPersister, InMemoryMetadataStore, NewTimeout,
    Orchestrator, ProxyLookupResult, SandboxLaunchSource, SandboxState, SandboxTimeoutAction,
};
use agentenv::sandbox::{FirecrackerSandboxFactory, SandboxNetworkPolicy};
use agentenv::snapshot::{
    SnapshotAlias, SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotSource,
    StartupCommand,
};

use anyhow::Result;
use std::path::PathBuf;
use tempfile::tempdir;
use tokio::time::{timeout, Duration};
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_secs(120);

fn host_file_persister(root: PathBuf) -> FileBackedSandboxPersister {
    FileBackedSandboxPersister::new(root, ConfigManager::global_config().virtualization_mode)
}

#[tokio::test]
async fn orchestrator_lifecycle() -> Result<()> {
    common::setup().await;
    timeout(TEST_TIMEOUT, async {
        let root = tempdir()?;
        let (builder, snapshot_manager, _) = common::snapshot_test_parts(root.path());
        let alias = format!("orchestrator-test-shared-{}", Uuid::now_v7());

        let stored = builder
            .build_and_publish(
                &snapshot_manager,
                common::default_rootfs_template_build_spec()
                    .alias(alias)
                    .run("mkdir -p /workspace"),
            )
            .await?;
        let runnable = snapshot_manager.resolve_runnable(stored).await?;

        let store = InMemoryMetadataStore::new();
        let factory = FirecrackerSandboxFactory::new();
        let paused_store = root.path().join("paused-sandboxes");
        let persister = host_file_persister(paused_store.clone());
        let orchestrator = Orchestrator::new(store, factory, persister).await?;
        let case_id = Uuid::now_v7().to_string();

        let request = CreateSandboxRequest {
            source: SandboxLaunchSource::Snapshot(Box::new(runnable)),
            timeout: Some(Duration::from_secs(30)),
            timeout_action: SandboxTimeoutAction::Pause,
            user_metadata: Some(
                [
                    ("team".to_string(), "alpha".to_string()),
                    ("case_id".to_string(), case_id.clone()),
                ]
                .iter()
                .cloned()
                .collect(),
            ),
            env_vars: None,
            network_policy: SandboxNetworkPolicy::default(),
            auto_resume: false,
            custom_extension_params: None,
        };

        let created = orchestrator.create_sandbox(request).await?;
        assert_eq!(created.state, SandboxState::Running);

        let sandbox_id = created.id;
        let lookup = orchestrator.proxy_lookup_for(&sandbox_id).await?;
        assert!(
            matches!(lookup, ProxyLookupResult::Ready(_)),
            "expected proxy lookup Ready for sandbox {sandbox_id}, got {lookup:?}"
        );

        let fetched = orchestrator
            .get_sandbox(&sandbox_id)
            .await?
            .expect("sandbox metadata should exist after create");
        assert_eq!(fetched.id, sandbox_id);
        assert_eq!(fetched.state, SandboxState::Running);

        orchestrator.pause_sandbox(sandbox_id).await?;
        let paused = orchestrator
            .get_sandbox(&sandbox_id)
            .await?
            .expect("sandbox metadata should exist after pause");
        assert_eq!(paused.state, SandboxState::Paused);
        assert_eq!(
            orchestrator.proxy_lookup_for(&sandbox_id).await?,
            ProxyLookupResult::Paused { auto_resume: false }
        );

        orchestrator.shutdown().await?;
        drop(orchestrator);

        let restarted = Orchestrator::new(
            InMemoryMetadataStore::new(),
            FirecrackerSandboxFactory::new(),
            host_file_persister(paused_store),
        )
        .await?;
        let restored = restarted
            .get_sandbox(&sandbox_id)
            .await?
            .expect("persisted paused metadata should be loaded after orchestrator restart");
        assert_eq!(restored.state, SandboxState::Paused);
        assert_eq!(
            restarted.proxy_lookup_for(&sandbox_id).await?,
            ProxyLookupResult::Paused { auto_resume: false }
        );

        let resumed = restarted
            .resume_sandbox(sandbox_id, NewTimeout::Set(Duration::from_secs(120)))
            .await?;
        assert_eq!(resumed.state, SandboxState::Running);
        assert_eq!(resumed.timeout, Some(Duration::from_secs(120)));
        let lookup = restarted.proxy_lookup_for(&sandbox_id).await?;
        assert!(
            matches!(lookup, ProxyLookupResult::Ready(_)),
            "expected proxy lookup Ready after resume for sandbox {sandbox_id}, got {lookup:?}"
        );

        restarted.delete_sandbox(sandbox_id).await?;
        let deleted = restarted.get_sandbox(&sandbox_id).await?;
        assert!(deleted.is_none(), "sandbox should be removed after delete");
        let lookup = restarted.proxy_lookup_for(&sandbox_id).await?;
        assert_eq!(lookup, ProxyLookupResult::NotFound);

        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))?
}

#[tokio::test]
async fn orchestrator_capture_snapshot_can_be_published_and_relaunched() -> Result<()> {
    common::setup().await;
    timeout(TEST_TIMEOUT, async {
        let root = tempdir()?;
        let (builder, snapshot_manager, _) = common::snapshot_test_parts(root.path());
        let base_alias = format!("orchestrator-capture-base-{}", Uuid::now_v7());

        let stored = builder
            .build_and_publish(
                &snapshot_manager,
                common::default_rootfs_template_build_spec()
                    .alias(base_alias)
                    .run("mkdir -p /workspace && echo captured-base > /workspace/base.txt")
                    .env("CAPTURE_CONTEXT", "preserved")
                    .workdir("/workspace")
                    .start_cmd("sleep 1000000")
                    .ready_cmd("test -f base.txt"),
            )
            .await?;
        let runnable = snapshot_manager.resolve_runnable(stored).await?;

        let orchestrator = Orchestrator::with_in_memory_store().await;
        let created = orchestrator
            .create_sandbox(CreateSandboxRequest {
                source: SandboxLaunchSource::Snapshot(Box::new(runnable)),
                timeout: Some(Duration::from_secs(30)),
                timeout_action: SandboxTimeoutAction::Pause,
                user_metadata: None,
                env_vars: None,
                network_policy: SandboxNetworkPolicy::default(),
                auto_resume: false,
                custom_extension_params: None,
            })
            .await?;
        let sandbox_id = created.id;
        let capture = orchestrator.capture_snapshot(sandbox_id).await?;
        assert_eq!(capture.metadata.id, sandbox_id);
        assert_eq!(capture.metadata.state, SandboxState::Running);
        assert_eq!(capture.metadata.context.workdir, "/workspace");
        assert_eq!(
            capture
                .metadata
                .context
                .env_vars
                .get("CAPTURE_CONTEXT")
                .map(String::as_str),
            Some("preserved")
        );
        assert!(matches!(
            capture.metadata.startup.as_ref(),
            Some(StartupCommand {
                start_cmd,
                ready_cmd,
                context,
            }) if start_cmd == "sleep 1000000"
                && ready_cmd == "test -f base.txt"
                && context.workdir == "/workspace"
        ));

        let published_alias = format!("orchestrator-captured-{}", Uuid::now_v7());
        let sandbox_id_str = sandbox_id.to_string();
        let published = snapshot_manager
            .publish_captured(
                SnapshotPublishMetadata {
                    id: SnapshotId::generate(),
                    alias: Some(SnapshotAlias::parse(&published_alias)?),
                    source: SnapshotPublishSource::Sandbox {
                        source_sandbox_id: sandbox_id_str.clone(),
                    },
                    context: capture.metadata.context.clone(),
                    startup: capture.metadata.startup.clone(),
                    resources: capture.metadata.resources,
                    runtime_versions: capture.metadata.runtime_versions.clone(),
                    virtualization_mode: capture.metadata.virtualization_mode,
                    image_configs: capture.metadata.image_configs.clone(),
                    custom_extension_params: None,
                },
                capture.captured_snapshot,
            )
            .await?;
        let record = snapshot_manager
            .get(published.id.to_string())
            .await?
            .expect("published snapshot record should exist");
        assert!(matches!(
            &record.source,
            SnapshotSource::Sandbox {
                source_sandbox_id
            } if source_sandbox_id == &sandbox_id_str
        ));
        let committed = record
            .committed
            .as_ref()
            .expect("published snapshot should be committed");
        assert_eq!(committed.context.workdir, "/workspace");
        assert_eq!(
            committed
                .context
                .env_vars
                .get("CAPTURE_CONTEXT")
                .map(String::as_str),
            Some("preserved")
        );
        assert!(matches!(
            committed.startup.as_ref(),
            Some(StartupCommand {
                start_cmd,
                ready_cmd,
                context,
            }) if start_cmd == "sleep 1000000"
                && ready_cmd == "test -f base.txt"
                && context.workdir == "/workspace"
        ));

        let captured_runnable = snapshot_manager.resolve_runnable(published).await?;
        let relaunched = orchestrator
            .create_sandbox(CreateSandboxRequest {
                source: SandboxLaunchSource::Snapshot(Box::new(captured_runnable)),
                timeout: Some(Duration::from_secs(30)),
                timeout_action: SandboxTimeoutAction::Pause,
                user_metadata: None,
                env_vars: None,
                network_policy: SandboxNetworkPolicy::default(),
                auto_resume: false,
                custom_extension_params: None,
            })
            .await?;
        assert_eq!(relaunched.state, SandboxState::Running);
        assert!(
            matches!(
                orchestrator.proxy_lookup_for(&relaunched.id).await?,
                ProxyLookupResult::Ready(_)
            ),
            "relaunched sandbox should be proxyable through orchestrator"
        );

        orchestrator.delete_sandbox(relaunched.id).await?;
        orchestrator.delete_sandbox(sandbox_id).await?;
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))?
}
