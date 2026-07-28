use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use overlaybd::config::UpperMode;
use tempfile::TempDir;
use tokio::time::Instant;
use tracing::{debug, Span};

use super::build_spec::TemplateBuildStep;
use super::errors::{command_output_suffix, TemplateBuildFailure};
use super::step_executor::TemplateStepExecutor;
use crate::sandbox::{
    FirecrackerSandbox, FirecrackerSandboxConfig, FirecrackerSnapshotManifest, ProcessHandle,
    ProcessOpts, SandboxExecutor, SandboxLaunchConfig, UblkConfig,
};
use crate::snapshot::{
    CommandContext, RunnableSnapshot, SnapshotAlias, SnapshotId, SnapshotRuntimeVersions,
    StartupCommand,
};
use crate::types::{ImageConfigs, SandboxId, SandboxResources};
use crate::virtualization::VirtualizationMode;

/// Default command to use for ready check when start command is provided but ready command is not.
/// Use the same default ready command as E2B
const DEFAULT_READY_WITH_START_CMD: &str = "sleep 20";
const READY_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const READY_TIMEOUT: Duration = Duration::from_secs(10 * 60);

fn spawn_with_trace_context<T, F>(span: Span, worker: F) -> JoinHandle<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let dispatcher = tracing::dispatcher::get_default(Clone::clone);
    std::thread::spawn(move || {
        tracing::dispatcher::with_default(&dispatcher, || {
            let _guard = span.enter();
            worker()
        })
    })
}

#[derive(Clone, Debug)]
pub(crate) enum TemplateBuildBase {
    Rootfs {
        launch_rootfs_path: PathBuf,
        ublk_config: Option<UblkConfig>,
        image_configs: ImageConfigs,
    },
    Snapshot {
        base_snapshot: Box<RunnableSnapshot>,
    },
}

#[derive(Debug)]
pub(crate) struct TemplateBuildContext {
    pub build_snapshot_id: SnapshotId,
    pub alias: Option<SnapshotAlias>,
    pub initial_context: CommandContext,
    pub startup: Option<StartupCommand>,
    pub override_startup: bool,
    pub resources: SandboxResources,
    // Keep the tempdir owner alive for the duration of build + publish.
    pub workspace: TempDir,
    pub steps: Vec<TemplateBuildStep>,
    pub base: TemplateBuildBase,
    pub cpu_config_json: Option<String>,
    pub virtualization_mode: VirtualizationMode,
}

impl TemplateBuildContext {
    pub(crate) fn local_dir(&self) -> &Path {
        self.workspace.path()
    }
}

#[derive(Clone, Debug)]
/// Runs snapshot build steps inside a temporary sandbox and captures snapshot artifacts.
pub(crate) struct TemplateBuildRunner {
    step_executor: TemplateStepExecutor,
}

#[derive(Clone, Debug)]
pub(crate) struct TemplateBuildExecution {
    pub runtime_versions: SnapshotRuntimeVersions,
    pub manifest: FirecrackerSnapshotManifest,
    pub build_context: CommandContext,
    pub startup: Option<StartupCommand>,
    pub image_configs: ImageConfigs,
}

impl TemplateBuildRunner {
    pub(crate) fn new() -> Self {
        Self {
            step_executor: TemplateStepExecutor::new(),
        }
    }

    pub(crate) fn execute(&self, context: &TemplateBuildContext) -> Result<TemplateBuildExecution> {
        match &context.base {
            TemplateBuildBase::Rootfs {
                launch_rootfs_path,
                ublk_config,
                image_configs,
            } => self.build_template(
                context,
                launch_rootfs_path,
                ublk_config.clone(),
                image_configs,
            ),
            TemplateBuildBase::Snapshot { base_snapshot } => {
                self.build_template_from_snapshot(context, base_snapshot)
            }
        }
    }

    #[tracing::instrument(skip(self, context, launch_rootfs_path, ublk_config, image_configs))]
    fn build_template(
        &self,
        context: &TemplateBuildContext,
        launch_rootfs_path: &Path,
        ublk_config: Option<UblkConfig>,
        image_configs: &ImageConfigs,
    ) -> Result<TemplateBuildExecution> {
        if !launch_rootfs_path.exists() {
            bail!(
                "snapshot build base is missing: {}",
                launch_rootfs_path.display()
            );
        }

        let user_image_config = crate::sandbox::OverlaybdConfig {
            image_config_path: launch_rootfs_path.to_path_buf(),
            read_only: false,
            runtime_upper_mode: UpperMode::LogStructured,
        };
        let mut config =
            FirecrackerSandboxConfig::from_global_config_with_user_image(user_image_config.clone())
                .context("load sandbox config for snapshot build")?;
        config.vcpu_count = context.resources.cpu_count;
        config.mem_size_mib = context.resources.memory_mib;
        config.common.ublk_config = ublk_config;
        config.common.cpu_config_json = context.cpu_config_json.clone();
        config.common.env_vars = (!context.initial_context.env_vars.is_empty())
            .then_some(context.initial_context.env_vars.clone());
        config.common.default_user = context.initial_context.user.clone();
        config.common.default_workdir = Some(context.initial_context.workdir.clone());
        let sandbox_id = SandboxId::new();
        let image_configs = image_configs.clone();
        let launch_config =
            SandboxLaunchConfig::new(sandbox_id, context.build_snapshot_id.to_string())
                .with_image_configs(&image_configs);
        config = config.apply_launch_config(&launch_config);

        self.run_template_build(
            context,
            sandbox_id,
            context.resources,
            image_configs,
            move || FirecrackerSandbox::new_with_id(config, sandbox_id),
        )
    }

    #[tracing::instrument(
        skip(self, context, base_snapshot),
        fields(base_snapshot_id = %base_snapshot.record().id)
    )]
    fn build_template_from_snapshot(
        &self,
        context: &TemplateBuildContext,
        base_snapshot: &RunnableSnapshot,
    ) -> Result<TemplateBuildExecution> {
        let base_snapshot = base_snapshot.clone();
        let sandbox_id = SandboxId::new();
        let image_configs = base_snapshot.committed().image_configs.clone();
        let launch_config =
            SandboxLaunchConfig::new(sandbox_id, context.build_snapshot_id.to_string())
                .with_image_configs(&image_configs);
        let resources = *base_snapshot.resources();

        self.run_template_build(context, sandbox_id, resources, image_configs, move || {
            FirecrackerSandbox::from_snapshot(&base_snapshot, &launch_config)
        })
    }

    fn run_template_build<F>(
        &self,
        context: &TemplateBuildContext,
        sandbox_id: SandboxId,
        resources: SandboxResources,
        image_configs: ImageConfigs,
        create_sandbox: F,
    ) -> Result<TemplateBuildExecution>
    where
        F: FnOnce() -> Result<FirecrackerSandbox> + Send + 'static,
    {
        let worker_span = tracing::debug_span!("template_build_sandbox", sandbox_id = %sandbox_id);
        let step_executor = self.step_executor.clone();
        let steps = context.steps.clone();
        let output_dir = context.local_dir().to_path_buf();
        let initial_context = context.initial_context.clone();
        let startup = context.startup.clone();
        let override_startup = context.override_startup;

        let handle =
            spawn_with_trace_context(worker_span, move || -> Result<TemplateBuildExecution> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("create tokio runtime")?;
                rt.block_on(async move {
                    let mut sandbox = create_sandbox()?;
                    let run_result = async {
                        debug!(
                            cpu_count = resources.cpu_count,
                            memory_mib = resources.memory_mib,
                            "starting template build sandbox"
                        );
                        sandbox.start().await?;
                        debug!("template build sandbox started");

                        let build_context = step_executor
                            .execute(&sandbox, &steps, initial_context)
                            .await?;
                        let startup = prepare_startup(startup, override_startup, &build_context);
                        run_startup_commands(&sandbox, startup.as_ref()).await?;
                        let runtime_versions = SnapshotRuntimeVersions::probe(&sandbox).await?;

                        debug!("capturing template snapshot");
                        let (_, manifest) = sandbox.pause_to_dir(&output_dir).await?;
                        debug!("template snapshot captured");

                        Ok(TemplateBuildExecution {
                            runtime_versions,
                            manifest,
                            build_context,
                            startup,
                            image_configs,
                        })
                    }
                    .await;

                    let stop_result = sandbox.stop().await;
                    match (run_result, stop_result) {
                        (Ok(result), Ok(())) => Ok(result),
                        (Err(run_err), Ok(())) => Err(run_err),
                        (Ok(_), Err(stop_err)) => Err(stop_err),
                        (Err(run_err), Err(stop_err)) => Err(anyhow!(
                            "{}; additionally failed to stop sandbox: {}",
                            run_err,
                            stop_err
                        )),
                    }
                })
            });
        match handle.join() {
            Ok(result) => result,
            Err(_) => bail!("snapshot build worker thread panicked"),
        }
    }
}

fn prepare_startup(
    startup: Option<StartupCommand>,
    override_startup: bool,
    build_context: &CommandContext,
) -> Option<StartupCommand> {
    // TODO: Temporarily disabled. Deriving start_cmd from ENTRYPOINT/CMD breaks images that
    // require PID 1 (e.g., s6-overlay). These images fail with "can only run as pid 1" because
    // AgentENV executes start_cmd via `/bin/sh -c`, which occupies PID 1.
    // let startup = startup.or_else(|| {
    //     build_context
    //         .effective_start_cmd()
    //         .map(|cmd| StartupCommand {
    //             start_cmd: cmd,
    //             ready_cmd: DEFAULT_READY_WITH_START_CMD.to_string(),
    //             context: build_context.clone(),
    //         })
    // });

    let mut startup = startup?;
    let start_cmd_empty = startup.start_cmd.trim().is_empty();
    let ready_cmd_empty = startup.ready_cmd.trim().is_empty();
    if start_cmd_empty && ready_cmd_empty {
        return None;
    }
    if !start_cmd_empty && ready_cmd_empty {
        startup.ready_cmd = DEFAULT_READY_WITH_START_CMD.to_string();
    }
    if override_startup {
        startup.context = build_context.clone();
    }
    Some(startup)
}

#[tracing::instrument(
    skip(sandbox, startup),
    fields(
        has_start_cmd = startup.is_some_and(|value| !value.start_cmd.trim().is_empty()),
        has_ready_cmd = startup.is_some_and(|value| !value.ready_cmd.trim().is_empty()),
    )
)]
async fn run_startup_commands(
    sandbox: &impl SandboxExecutor,
    startup: Option<&StartupCommand>,
) -> Result<()> {
    let Some(startup) = startup else {
        return Ok(());
    };

    let mut start_handle = if startup.start_cmd.trim().is_empty() {
        None
    } else {
        debug!(command = %startup.start_cmd, "starting startup command");
        let handle = sandbox
            .start_process(
                "/bin/bash",
                &["-lc", startup.start_cmd.as_str()],
                &ProcessOpts {
                    envs: startup.context.env_vars.clone(),
                    cwd: Some(startup.context.workdir.clone()),
                    ..ProcessOpts::default()
                },
            )
            .await
            .with_context(|| {
                TemplateBuildFailure::new(format!(
                    "start command failed: failed to execute '{}'",
                    startup.start_cmd
                ))
            })?;
        debug!(command = %startup.start_cmd, "startup command started");
        Some(handle)
    };

    if !startup.ready_cmd.trim().is_empty() {
        run_ready_command(sandbox, startup, &mut start_handle).await?;
    }

    if let Some(handle) = start_handle.as_mut() {
        ensure_start_command_still_running_or_success(handle).await?;
    }

    Ok(())
}

#[tracing::instrument(skip(sandbox, startup, start_cmd_handle))]
async fn run_ready_command(
    sandbox: &impl SandboxExecutor,
    startup: &StartupCommand,
    start_cmd_handle: &mut Option<ProcessHandle>,
) -> Result<()> {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut attempt = 0_u64;

    let mut opts = ProcessOpts {
        envs: startup.context.env_vars.clone(),
        cwd: Some(startup.context.workdir.clone()),
        timeout: Some(READY_TIMEOUT),
    };

    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(TemplateBuildFailure::new(format!(
                "ready command timed out after {:?}: cmd='{}'",
                READY_TIMEOUT, startup.ready_cmd
            ))
            .into());
        }
        opts.timeout = Some(deadline - now);
        attempt += 1;
        debug!(
            attempt,
            command = %startup.ready_cmd,
            "running ready command"
        );

        if let Some(handle) = start_cmd_handle.as_mut() {
            if ensure_start_command_still_running_or_success(handle).await? {
                *start_cmd_handle = None;
            }
        }

        let output = sandbox
            .run_command_with_opts("/bin/bash", &["-lc", startup.ready_cmd.as_str()], &opts)
            .await;

        match output {
            Ok(output) if output.exit_code == 0 => {
                debug!(attempt, "ready command succeeded");
                return Ok(());
            }
            Ok(output) => {
                debug!(
                    attempt,
                    exit_code = output.exit_code,
                    "ready command not ready"
                );
                if Instant::now() >= deadline {
                    return Err(TemplateBuildFailure::new(format!(
                        "ready command timed out after {:?}: cmd='{}', exit_code={}{}",
                        READY_TIMEOUT,
                        startup.ready_cmd,
                        output.exit_code,
                        command_output_suffix(&output.stdout, &output.stderr)
                    ))
                    .into());
                }
            }
            Err(error) => {
                debug!(
                    attempt,
                    error = %format_args!("{error:#}"),
                    "ready command failed"
                );
                if Instant::now() >= deadline {
                    return Err(error).with_context(|| {
                        TemplateBuildFailure::new(format!(
                            "ready command timed out after {:?}: cmd='{}'",
                            READY_TIMEOUT, startup.ready_cmd
                        ))
                    });
                }
            }
        }

        tokio::time::sleep_until(std::cmp::min(
            Instant::now() + READY_RETRY_INTERVAL,
            deadline,
        ))
        .await;
    }
}

async fn ensure_start_command_still_running_or_success(handle: &mut ProcessHandle) -> Result<bool> {
    match tokio::time::timeout(Duration::from_millis(1), handle.wait()).await {
        Err(_) => Ok(false),
        Ok(Ok(output)) if output.exit_code == 0 => Ok(true),
        Ok(Ok(output)) => Err(TemplateBuildFailure::new(format!(
            "start command failed: command exited with status {}{}",
            output.exit_code,
            command_output_suffix(&output.stdout, &output.stderr)
        ))
        .into()),
        Ok(Err(error)) => Err(error).context(TemplateBuildFailure::new(
            "start command failed while waiting for command",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;

    use anyhow::{anyhow, Result};
    use async_trait::async_trait;

    use super::{prepare_startup, run_ready_command};
    use crate::sandbox::{Executor, ProcessHandle, ProcessOpts, ProcessOutput, SandboxExecutor};
    use crate::snapshot::{CommandContext, StartupCommand};

    struct RecordingSandbox {
        timeouts: Mutex<Vec<Option<Duration>>>,
    }

    #[async_trait(?Send)]
    impl SandboxExecutor for RecordingSandbox {
        fn executor(&self) -> Result<Executor<'_>> {
            Err(anyhow!("not used by this test"))
        }

        async fn run_command_with_opts(
            &self,
            _cmd: &str,
            _args: &[&str],
            opts: &ProcessOpts,
        ) -> Result<ProcessOutput> {
            self.timeouts
                .lock()
                .expect("timeouts mutex should not be poisoned")
                .push(opts.timeout);
            Ok(ProcessOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: 0,
            })
        }

        async fn start_process(
            &self,
            _cmd: &str,
            _args: &[&str],
            _opts: &ProcessOpts,
        ) -> Result<ProcessHandle> {
            Err(anyhow!("not used by this test"))
        }
    }

    #[test]
    fn prepare_startup_defaults_ready_when_start_cmd_is_set() {
        let build_context = CommandContext::new(HashMap::new(), "/work");
        let startup = StartupCommand {
            start_cmd: "python -m http.server".to_string(),
            ready_cmd: String::new(),
            context: CommandContext::default(),
        };

        let startup = prepare_startup(Some(startup), true, &build_context)
            .expect("startup should remain enabled");

        assert_eq!(startup.ready_cmd, "sleep 20");
        assert_eq!(startup.context.workdir, "/work");
    }

    #[test]
    fn prepare_startup_drops_empty_startup() {
        let startup = StartupCommand {
            start_cmd: String::new(),
            ready_cmd: String::new(),
            context: CommandContext::default(),
        };

        assert!(prepare_startup(Some(startup), true, &CommandContext::default()).is_none());
    }

    #[test]
    fn prepare_startup_preserves_inherited_context_without_override() {
        let inherited_context =
            CommandContext::new(HashMap::from([("BASE".into(), "1".into())]), "/base");
        let build_context =
            CommandContext::new(HashMap::from([("BASE".into(), "2".into())]), "/derived");
        let startup = StartupCommand {
            start_cmd: "echo start".to_string(),
            ready_cmd: "echo ready".to_string(),
            context: inherited_context,
        };

        let startup = prepare_startup(Some(startup), false, &build_context)
            .expect("startup should remain enabled");

        assert_eq!(startup.context.workdir, "/base");
        assert_eq!(
            startup.context.env_vars.get("BASE").map(String::as_str),
            Some("1")
        );
    }

    #[tokio::test]
    async fn ready_command_uses_remaining_timeout() {
        let sandbox = RecordingSandbox {
            timeouts: Mutex::new(Vec::new()),
        };
        let startup = StartupCommand {
            start_cmd: String::new(),
            ready_cmd: "echo ready".to_string(),
            context: CommandContext::default(),
        };
        let mut start_handle = None;

        run_ready_command(&sandbox, &startup, &mut start_handle)
            .await
            .expect("ready command should succeed");

        let timeouts = sandbox
            .timeouts
            .lock()
            .expect("timeouts mutex should not be poisoned");
        assert_eq!(timeouts.len(), 1);
        let timeout = timeouts[0].expect("ready command should have a timeout");
        assert!(timeout <= super::READY_TIMEOUT);
        assert!(timeout > super::READY_TIMEOUT - Duration::from_secs(1));
    }
}
