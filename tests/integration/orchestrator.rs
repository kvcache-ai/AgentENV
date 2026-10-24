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
use envd::process::{ListRequest, ProcessClient};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use tokio::time::{timeout, Duration};
use tonic::Request;
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_secs(120);
// These processes share the configured host network rules.
static ORCHESTRATOR_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn envd_process_list_status(
    target: &agentenv::orchestrator::ProxyTarget,
    access_token: Option<&str>,
) -> Result<envd::reqwest::StatusCode> {
    let port = ConfigManager::global_config().tools.control_plane_port;
    let mut request = envd::reqwest::Client::new()
        .post(format!("http://{}:{port}/process.Process/List", target.ip));
    if let Some(access_token) = access_token {
        request = request.header("X-Access-Token", access_token);
    }
    Ok(request.send().await?.status())
}

async fn assert_envd_process_list_succeeds(
    target: &agentenv::orchestrator::ProxyTarget,
    access_token: &str,
) -> Result<()> {
    let port = ConfigManager::global_config().tools.control_plane_port;
    let mut client =
        ProcessClient::connect(&format!("http://{}:{port}", target.ip), Some(access_token)).await?;
    client.list(Request::new(ListRequest {})).await?;
    Ok(())
}

fn host_file_persister(root: PathBuf) -> FileBackedSandboxPersister {
    FileBackedSandboxPersister::new(root, ConfigManager::global_config().virtualization_mode)
}

// Only this opt-in test needs two installed Firecracker versions. It reuses
// the real pause/restart/resume lifecycle rather than constructing VM state.
#[cfg(target_arch = "x86_64")]
#[tokio::test]
#[ignore = "requires isolated KVM host, AENV_TEST_V9_CONFIG and current v1000 runtime"]
async fn orchestrator_v9_snapshot_upgrade() -> Result<()> {
    let _guard = ORCHESTRATOR_TEST_LOCK.lock().await;
    let old_config = std::fs::canonicalize(std::env::var("AENV_TEST_V9_CONFIG")?)?;
    let current = ConfigManager::new()?;
    let current_config = std::fs::canonicalize(current.config_path().expect("config path"))?;
    let old = ConfigManager::new_from_path(&old_config)?;
    assert_eq!(
        old.config().virtualization_mode,
        agentenv::virtualization::VirtualizationMode::Kvm
    );
    assert_eq!(
        current.config().virtualization_mode,
        agentenv::virtualization::VirtualizationMode::Kvm
    );
    let current_binary = current.config().resolved_firecracker_binary_path();
    assert_ne!(
        old.config().resolved_firecracker_binary_path(),
        current_binary
    );
    let root = tempdir()?;
    for (phase, config) in [("pause", old_config), ("resume", current_config)] {
        let mut child = tokio::process::Command::new(std::env::current_exe()?);
        child
            .args([
                "--exact",
                "orchestrator::orchestrator_lifecycle",
                "--nocapture",
            ])
            .env("AENV_CONFIG_PATH", config)
            .env("AENV_ORCHESTRATOR_RESTART_TEST_ROOT", root.path())
            .env("AENV_ORCHESTRATOR_RESTART_TEST_PHASE", phase)
            .env("AENV_ORCHESTRATOR_UPGRADE_TEST", "1")
            .kill_on_drop(true);
        let status = timeout(TEST_TIMEOUT, child.status()).await??;
        anyhow::ensure!(status.success(), "{phase} upgrade process failed: {status}");
        if phase == "pause" {
            // Reading the witness also proves the child executed the pause body.
            let (id, _): (agentenv::types::SandboxId, String) =
                serde_json::from_slice(&std::fs::read(root.path().join("restart-witness.json"))?)?;
            let db = rocksdb::DB::open_default(root.path().join("paused-sandboxes/records.db"))?;
            let key = id.to_string();
            let mut record: serde_json::Value = serde_json::from_slice(
                &db.get(&key)?
                    .ok_or_else(|| anyhow::anyhow!("missing paused record"))?,
            )?;
            assert_snapshot_format(&current_binary, &record["state"], "v9.0.0").await?;
            let missing = root.path().join("removed-old-runtime/firecracker");
            assert!(!missing.exists());
            let binary = record
                .pointer_mut("/state/common/firecracker_binary")
                .ok_or_else(|| anyhow::anyhow!("missing captured binary path"))?;
            *binary = serde_json::to_value(missing)?;
            db.put(&key, serde_json::to_vec(&record)?)?;
            db.flush_wal(true)?;
            // Drop the DB before the resume process opens the paused store.
        } else {
            assert_eq!(std::fs::read(root.path().join("resume-complete"))?, b"ok");
        }
    }
    Ok(())
}

async fn assert_snapshot_format(
    binary: &Path,
    state: &serde_json::Value,
    expected: &str,
) -> Result<()> {
    let path = state["vm_state_path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing VM state path"))?;
    let mut command = tokio::process::Command::new(binary);
    command
        .args(["--describe-snapshot", path])
        .kill_on_drop(true);
    let output = timeout(TEST_TIMEOUT, command.output()).await??;
    anyhow::ensure!(
        output.status.success(),
        "snapshot inspection failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout)?.lines().next(),
        Some(expected)
    );
    Ok(())
}

#[tokio::test]
async fn orchestrator_lifecycle() -> Result<()> {
    let _guard = ORCHESTRATOR_TEST_LOCK.lock().await;
    const ROOT_ENV: &str = "AENV_ORCHESTRATOR_RESTART_TEST_ROOT";
    const PHASE_ENV: &str = "AENV_ORCHESTRATOR_RESTART_TEST_PHASE";

    let Some(root) = std::env::var_os(ROOT_ENV) else {
        // Shutdown is terminal for process-global managers. Restart the test
        // process, retaining the same paused records and every lifecycle check.
        let root = tempdir()?;
        for phase in ["pause", "resume"] {
            let mut child = tokio::process::Command::new(std::env::current_exe()?);
            child
                .args([
                    "--exact",
                    "orchestrator::orchestrator_lifecycle",
                    "--nocapture",
                ])
                .env(ROOT_ENV, root.path())
                .env(PHASE_ENV, phase)
                .kill_on_drop(true);
            let status = timeout(TEST_TIMEOUT, child.status())
                .await
                .map_err(|_| anyhow::anyhow!("{phase} test process timed out"))??;
            anyhow::ensure!(status.success(), "{phase} test process failed: {status}");
            let completed = match phase {
                "pause" => "restart-witness.json",
                _ => "resume-complete",
            };
            anyhow::ensure!(
                root.path().join(completed).is_file(),
                "{phase} child exited without completing the lifecycle test"
            );
        }
        return Ok(());
    };

    common::setup().await;
    timeout(TEST_TIMEOUT, async {
        let root = PathBuf::from(root);
        let phase = std::env::var(PHASE_ENV)?;
        let upgrade = std::env::var_os("AENV_ORCHESTRATOR_UPGRADE_TEST").is_some();
        if phase == "pause" {
            let (builder, snapshot_manager, _) = common::snapshot_test_parts(&root);
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
            let paused_store = &root.join("paused-sandboxes");
            let persister = host_file_persister(paused_store.clone());
            let orchestrator = Orchestrator::new(store, factory, persister).await?;
            let case_id = Uuid::now_v7().to_string();

            let request = CreateSandboxRequest {
                source: SandboxLaunchSource::Snapshot(Box::new(runnable)),
                extra_drives: Vec::new(),
                extra_drives_in_snapshot: false,
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
                secure: true,
                volume_mounts: std::collections::HashMap::new(),
            };

            let created = orchestrator.create_sandbox(request).await?;
            assert_eq!(created.state, SandboxState::Running);
            let access_token = orchestrator
                .get_envd_access_token(&created)
                .expect("secure sandbox access token");

            let sandbox_id = created.id;
            let lookup = orchestrator.proxy_lookup_for(&sandbox_id).await?;
            assert!(
                matches!(lookup, ProxyLookupResult::Ready(_)),
                "expected proxy lookup Ready for sandbox {sandbox_id}, got {lookup:?}"
            );
            let ProxyLookupResult::Ready(target) = lookup else {
                unreachable!();
            };
            assert_envd_process_list_succeeds(&target, access_token.expose()).await?;
            if !upgrade {
                assert_eq!(
                    envd_process_list_status(&target, None).await?,
                    envd::reqwest::StatusCode::UNAUTHORIZED
                );
                assert_eq!(
                    envd_process_list_status(&target, Some("wrong-token")).await?,
                    envd::reqwest::StatusCode::UNAUTHORIZED
                );

                let child = orchestrator
                    .fork_sandbox(sandbox_id, 1, NewTimeout::UseExisting)
                    .await?
                    .pop()
                    .expect("one fork result")?;
                let child_token = orchestrator
                    .get_envd_access_token(&child)
                    .expect("secure fork access token");
                assert_ne!(child_token, access_token);
                let ProxyLookupResult::Ready(child_target) =
                    orchestrator.proxy_lookup_for(&child.id).await?
                else {
                    panic!("secure fork should have a proxy route");
                };
                assert_eq!(
                    envd_process_list_status(&child_target, Some(access_token.expose())).await?,
                    envd::reqwest::StatusCode::UNAUTHORIZED
                );
                assert_envd_process_list_succeeds(&child_target, child_token.expose()).await?;
                orchestrator.delete_sandbox(child.id).await?;
            }

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

            std::fs::write(
                root.join("restart-witness.json"),
                serde_json::to_vec(&(
                    sandbox_id,
                    hex::encode(Sha256::digest(access_token.expose().as_bytes())),
                ))?,
            )?;
            return Ok(());
        }
        anyhow::ensure!(phase == "resume", "unknown lifecycle test phase: {phase}");
        let (sandbox_id, access_token_digest): (agentenv::types::SandboxId, String) =
            serde_json::from_slice(&std::fs::read(root.join("restart-witness.json"))?)?;
        let paused_store = root.join("paused-sandboxes");
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
        let resumed_token = restarted
            .get_envd_access_token(&resumed)
            .expect("resumed secure sandbox access token");
        assert_eq!(
            hex::encode(Sha256::digest(resumed_token.expose().as_bytes())),
            access_token_digest,
            "sandbox access token changed after restart"
        );
        let ProxyLookupResult::Ready(target) = lookup else {
            unreachable!();
        };
        if !upgrade {
            assert_eq!(
                envd_process_list_status(&target, None).await?,
                envd::reqwest::StatusCode::UNAUTHORIZED
            );
            assert_eq!(
                envd_process_list_status(&target, Some("wrong-token")).await?,
                envd::reqwest::StatusCode::UNAUTHORIZED
            );
        }
        assert_envd_process_list_succeeds(&target, resumed_token.expose()).await?;

        if upgrade {
            restarted.pause_sandbox(sandbox_id).await?;
            let paused = restarted
                .get_sandbox(&sandbox_id)
                .await?
                .expect("paused sandbox");
            let state = paused
                .paused_state
                .as_ref()
                .expect("paused state")
                .encode()?;
            let binary = ConfigManager::global_config().resolved_firecracker_binary_path();
            assert_eq!(
                state["common"]["firecracker_binary"],
                serde_json::to_value(&binary)?
            );
            assert_snapshot_format(&binary, &state, "v1000.0.0").await?;
        }
        restarted.delete_sandbox(sandbox_id).await?;
        let deleted = restarted.get_sandbox(&sandbox_id).await?;
        assert!(deleted.is_none(), "sandbox should be removed after delete");
        let lookup = restarted.proxy_lookup_for(&sandbox_id).await?;
        assert_eq!(lookup, ProxyLookupResult::NotFound);
        std::fs::write(root.join("resume-complete"), b"ok")?;

        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))?
}

#[tokio::test]
async fn orchestrator_capture_snapshot_can_be_published_and_relaunched() -> Result<()> {
    let _guard = ORCHESTRATOR_TEST_LOCK.lock().await;
    const CHILD_ENV: &str = "AENV_ORCHESTRATOR_CAPTURE_TEST_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        // Keep the parent free of global daemon clients and cache database locks
        // while the lifecycle test restarts its runtime processes.
        let completion = tempdir()?;
        let completed = completion.path().join("capture-complete");
        let mut child = tokio::process::Command::new(std::env::current_exe()?);
        child
            .args([
                "--exact",
                "orchestrator::orchestrator_capture_snapshot_can_be_published_and_relaunched",
                "--nocapture",
            ])
            .env(CHILD_ENV, &completed)
            .kill_on_drop(true);
        let status = timeout(TEST_TIMEOUT, child.status())
            .await
            .map_err(|_| anyhow::anyhow!("capture test process timed out"))??;
        anyhow::ensure!(status.success(), "capture test process failed: {status}");
        anyhow::ensure!(
            completed.is_file(),
            "capture child exited without completing the test"
        );
        return Ok(());
    }
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
                extra_drives: Vec::new(),
                extra_drives_in_snapshot: false,
                timeout: Some(Duration::from_secs(30)),
                timeout_action: SandboxTimeoutAction::Pause,
                user_metadata: Some(std::collections::HashMap::from([(
                    "team".to_string(),
                    "capture-test".to_string(),
                )])),
                env_vars: None,
                network_policy: SandboxNetworkPolicy::default(),
                auto_resume: false,
                custom_extension_params: None,
                secure: false,
                volume_mounts: std::collections::HashMap::new(),
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
                shell: None,
            }) if start_cmd == "sleep 1000000"
                && ready_cmd == "test -f base.txt"
                && context.workdir == "/workspace"
        ));

        let published_alias = format!("orchestrator-captured-{}", Uuid::now_v7());
        let sandbox_id_str = sandbox_id.to_string();
        let published = agentenv::sandbox::publish_captured(
            &snapshot_manager,
            SnapshotPublishMetadata {
                id: SnapshotId::generate(),
                alias: Some(SnapshotAlias::parse(&published_alias)?),
                source: SnapshotPublishSource::Sandbox {
                    source_sandbox_id: sandbox_id_str.clone(),
                    source_sandbox_metadata: capture.metadata.user_metadata.clone(),
                },
                context: capture.metadata.context.clone(),
                startup: capture.metadata.startup.clone(),
                resources: capture.metadata.resources,
                runtime_versions: capture.metadata.runtime_versions.clone(),
                virtualization_mode: capture.metadata.virtualization_mode,
                image_configs: capture.metadata.image_configs.clone(),
                volume_snapshots: Vec::new(),
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
                source_sandbox_id,
                source_sandbox_metadata: Some(metadata),
            } if source_sandbox_id == &sandbox_id_str
                && metadata.get("team").map(String::as_str) == Some("capture-test")
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
                shell: None,
            }) if start_cmd == "sleep 1000000"
                && ready_cmd == "test -f base.txt"
                && context.workdir == "/workspace"
        ));

        let captured_runnable = snapshot_manager.resolve_runnable(published).await?;
        let relaunched = orchestrator
            .create_sandbox(CreateSandboxRequest {
                source: SandboxLaunchSource::Snapshot(Box::new(captured_runnable)),
                extra_drives: Vec::new(),
                extra_drives_in_snapshot: false,
                timeout: Some(Duration::from_secs(30)),
                timeout_action: SandboxTimeoutAction::Pause,
                user_metadata: None,
                env_vars: None,
                network_policy: SandboxNetworkPolicy::default(),
                auto_resume: false,
                custom_extension_params: None,
                secure: false,
                volume_mounts: std::collections::HashMap::new(),
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
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))??;
    std::fs::write(
        std::env::var_os(CHILD_ENV).expect("capture completion path"),
        b"ok",
    )?;
    Ok(())
}
