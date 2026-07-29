use std::collections::HashMap;

use anyhow::{Context, Result};
use shell_util::shell_quote;
use tracing::debug;

use super::build_spec::{TemplateBuildStep, TemplateBuildStepKind};
use super::errors::{command_output_suffix, TemplateBuildFailure};
use crate::sandbox::{ProcessOpts, SandboxExecutor};
use crate::snapshot::CommandContext;

#[derive(Clone, Debug, Default)]
pub(crate) struct TemplateStepExecutor;

impl TemplateStepExecutor {
    pub(crate) fn new() -> Self {
        Self
    }

    #[tracing::instrument(
        skip(self, sandbox, steps, initial_context),
        fields(step_count = steps.len())
    )]
    pub(crate) async fn execute(
        &self,
        sandbox: &impl SandboxExecutor,
        steps: &[TemplateBuildStep],
        initial_context: CommandContext,
    ) -> Result<CommandContext> {
        let mut context = initial_context;

        debug!("executing template build steps");
        for step in steps {
            match &step.kind {
                TemplateBuildStepKind::Env { key, value } => {
                    context = context.with_env_var(key.clone(), value.clone());
                }
                TemplateBuildStepKind::Workdir { path } => {
                    // Resolve relative paths against the current workdir, as
                    // Docker does, so both the created directory and the
                    // context agree on the absolute location.
                    let path = path.to_string_lossy();
                    let resolved = if std::path::Path::new(path.as_ref()).is_absolute() {
                        path.into_owned()
                    } else {
                        let base = if context.workdir.is_empty() { "/" } else { &context.workdir };
                        std::path::Path::new(base)
                            .join(path.as_ref())
                            .to_string_lossy()
                            .into_owned()
                    };
                    self.ensure_workdir(sandbox, &context, &resolved).await?;
                    context = context.with_workdir(resolved);
                }
                TemplateBuildStepKind::User { value } => {
                    context = context.with_user(Some(value.clone()));
                }
                TemplateBuildStepKind::ExposedPort { port } => {
                    let mut ports = context.exposed_ports.clone();
                    if !ports.contains(port) {
                        ports.push(port.clone());
                    }
                    context = context.with_exposed_ports(ports);
                }
                TemplateBuildStepKind::Volume { path } => {
                    let mut volumes = context.volumes.clone();
                    if !volumes.contains(path) {
                        volumes.push(path.clone());
                    }
                    context = context.with_volumes(volumes);
                }
                TemplateBuildStepKind::Label { key, value } => {
                    let mut labels = context.labels.clone();
                    labels.insert(key.clone(), value.clone());
                    context = context.with_labels(labels);
                }
                TemplateBuildStepKind::Run { cmd } => {
                    self.run_step(sandbox, &context.workdir, &context.env_vars, cmd)
                        .await?;
                }
            }
        }
        debug!("template build steps completed");

        Ok(context)
    }

    /// Docker's WORKDIR creates the directory when it does not exist, and both
    /// Dockerfile front-ends (`aenv build` and the e2b SDK's `from_dockerfile`,
    /// which also injects a default `WORKDIR /home/user`) map WORKDIR to this
    /// step — so it must materialize the directory, not only record metadata.
    async fn ensure_workdir(
        &self,
        sandbox: &impl SandboxExecutor,
        context: &CommandContext,
        path: &str,
    ) -> Result<()> {
        let cmd = format!("mkdir -p -- {}", shell_quote(path));
        let opts = ProcessOpts {
            envs: context.env_vars.clone(),
            cwd: Some(context.workdir.clone()),
            ..ProcessOpts::default()
        };

        let output = sandbox
            .run_command_with_opts("/bin/bash", &["-lc", &cmd], &opts)
            .await
            .with_context(|| {
                TemplateBuildFailure::with_step("build step failed", format!("WORKDIR {path}"))
            })?;
        if output.exit_code != 0 {
            let message = format!(
                "build step failed: command exited with status {}{}",
                output.exit_code,
                command_output_suffix(&output.stdout, &output.stderr)
            );
            return Err(TemplateBuildFailure::with_step(message, format!("WORKDIR {path}")).into());
        }
        Ok(())
    }

    async fn run_step(
        &self,
        sandbox: &impl SandboxExecutor,
        workdir: &str,
        env: &HashMap<String, String>,
        cmd: &str,
    ) -> Result<()> {
        let opts = ProcessOpts {
            envs: env.clone(),
            cwd: Some(workdir.to_string()),
            ..ProcessOpts::default()
        };

        let output = sandbox
            .run_command_with_opts("/bin/bash", &["-lc", cmd], &opts)
            .await
            .with_context(|| {
                TemplateBuildFailure::with_step("build step failed", format!("RUN {cmd}"))
            })?;
        if output.exit_code != 0 {
            let message = format!(
                "build step failed: command exited with status {}{}",
                output.exit_code,
                command_output_suffix(&output.stdout, &output.stderr)
            );
            return Err(TemplateBuildFailure::with_step(message, format!("RUN {cmd}")).into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use anyhow::{anyhow, Result};
    use async_trait::async_trait;

    use super::TemplateStepExecutor;
    use crate::template::errors::TemplateBuildFailure;
    use crate::sandbox::{Executor, ProcessHandle, ProcessOpts, ProcessOutput, SandboxExecutor};
    use crate::snapshot::CommandContext;
    use crate::template::build_spec::TemplateBuildStep;

    struct NoopSandbox;

    #[async_trait(?Send)]
    impl SandboxExecutor for NoopSandbox {
        fn executor(&self) -> Result<Executor<'_>> {
            Err(anyhow!("not used"))
        }
        async fn run_command_with_opts(
            &self,
            _cmd: &str,
            _args: &[&str],
            _opts: &ProcessOpts,
        ) -> Result<ProcessOutput> {
            Err(anyhow!("not used"))
        }
        async fn start_process(
            &self,
            _cmd: &str,
            _args: &[&str],
            _opts: &ProcessOpts,
        ) -> Result<ProcessHandle> {
            Err(anyhow!("not used"))
        }
    }

    /// Records every command the executor runs and reports success for all.
    struct RecordingSandbox {
        commands: Mutex<Vec<(String, Vec<String>, Option<String>)>>,
        exit_code: i32,
    }

    impl RecordingSandbox {
        fn succeeding() -> Self {
            Self {
                commands: Mutex::new(Vec::new()),
                exit_code: 0,
            }
        }

        fn failing() -> Self {
            Self {
                commands: Mutex::new(Vec::new()),
                exit_code: 1,
            }
        }

        fn commands(&self) -> Vec<(String, Vec<String>, Option<String>)> {
            self.commands
                .lock()
                .expect("commands mutex should not be poisoned")
                .clone()
        }
    }

    #[async_trait(?Send)]
    impl SandboxExecutor for RecordingSandbox {
        fn executor(&self) -> Result<Executor<'_>> {
            Err(anyhow!("not used"))
        }
        async fn run_command_with_opts(
            &self,
            cmd: &str,
            args: &[&str],
            opts: &ProcessOpts,
        ) -> Result<ProcessOutput> {
            self.commands
                .lock()
                .expect("commands mutex should not be poisoned")
                .push((
                    cmd.to_string(),
                    args.iter().map(|a| a.to_string()).collect(),
                    opts.cwd.clone(),
                ));
            Ok(ProcessOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: self.exit_code,
            })
        }
        async fn start_process(
            &self,
            _cmd: &str,
            _args: &[&str],
            _opts: &ProcessOpts,
        ) -> Result<ProcessHandle> {
            Err(anyhow!("not used"))
        }
    }

    async fn run(steps: Vec<TemplateBuildStep>) -> CommandContext {
        TemplateStepExecutor::new()
            .execute(&NoopSandbox, &steps, CommandContext::default())
            .await
            .expect("steps should execute without error")
    }

    #[tokio::test]
    async fn user_step_sets_context_user() {
        let ctx = run(vec![TemplateBuildStep::user("zzz")]).await;
        assert_eq!(ctx.user.as_deref(), Some("zzz"));
    }

    #[tokio::test]
    async fn user_step_overrides_base_image_user() {
        let initial = CommandContext::default().with_user(Some("root".to_string()));
        let ctx = TemplateStepExecutor::new()
            .execute(&NoopSandbox, &[TemplateBuildStep::user("zzz")], initial)
            .await
            .unwrap();
        assert_eq!(ctx.user.as_deref(), Some("zzz"));
    }

    #[tokio::test]
    async fn exposed_port_deduplicates() {
        let ctx = run(vec![
            TemplateBuildStep::exposed_port("8080"),
            TemplateBuildStep::exposed_port("8080"),
            TemplateBuildStep::exposed_port("443"),
        ])
        .await;
        assert_eq!(ctx.exposed_ports, vec!["8080", "443"]);
    }

    #[tokio::test]
    async fn exposed_port_from_base_image_is_not_duplicated() {
        let initial = CommandContext::default().with_exposed_ports(vec!["8080".to_string()]);
        let ctx = TemplateStepExecutor::new()
            .execute(
                &NoopSandbox,
                &[TemplateBuildStep::exposed_port("8080")],
                initial,
            )
            .await
            .unwrap();
        assert_eq!(ctx.exposed_ports, vec!["8080"]);
    }

    #[tokio::test]
    async fn volume_deduplicates() {
        let ctx = run(vec![
            TemplateBuildStep::volume("/data"),
            TemplateBuildStep::volume("/data"),
            TemplateBuildStep::volume("/logs"),
        ])
        .await;
        assert_eq!(ctx.volumes, vec!["/data", "/logs"]);
    }

    #[tokio::test]
    async fn env_step_sets_env_var() {
        let ctx = run(vec![TemplateBuildStep::env("FOO", "bar")]).await;
        assert_eq!(ctx.env_vars.get("FOO").map(String::as_str), Some("bar"));
    }

    #[tokio::test]
    async fn workdir_step_updates_workdir() {
        let sandbox = RecordingSandbox::succeeding();
        let ctx = TemplateStepExecutor::new()
            .execute(
                &sandbox,
                &[TemplateBuildStep::workdir("/workspace")],
                CommandContext::default(),
            )
            .await
            .unwrap();
        assert_eq!(ctx.workdir, "/workspace");
    }

    #[tokio::test]
    async fn workdir_step_creates_the_directory() {
        // Docker's WORKDIR creates missing directories; images built from
        // Dockerfiles (and the e2b SDK's injected default workdir) rely on it.
        let sandbox = RecordingSandbox::succeeding();
        TemplateStepExecutor::new()
            .execute(
                &sandbox,
                &[TemplateBuildStep::workdir("/app")],
                CommandContext::default(),
            )
            .await
            .unwrap();
        let commands = sandbox.commands();
        assert_eq!(commands.len(), 1);
        let (cmd, args, _) = &commands[0];
        assert_eq!(cmd, "/bin/bash");
        // shell_quote leaves safe paths bare and quotes the rest.
        assert_eq!(args[1], "mkdir -p -- /app");
    }

    #[tokio::test]
    async fn workdir_step_resolves_relative_paths_against_current_workdir() {
        let sandbox = RecordingSandbox::succeeding();
        let ctx = TemplateStepExecutor::new()
            .execute(
                &sandbox,
                &[
                    TemplateBuildStep::workdir("/base"),
                    TemplateBuildStep::workdir("nested"),
                ],
                CommandContext::default(),
            )
            .await
            .unwrap();
        // Docker resolves a relative WORKDIR against the previous one; the
        // created directory and the recorded workdir must both be absolute.
        assert_eq!(ctx.workdir, "/base/nested");
        let commands = sandbox.commands();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[1].1[1], "mkdir -p -- /base/nested");
    }

    #[tokio::test]
    async fn workdir_step_failure_is_labelled_with_the_step() {
        let sandbox = RecordingSandbox::failing();
        let error = TemplateStepExecutor::new()
            .execute(
                &sandbox,
                &[TemplateBuildStep::workdir("/app")],
                CommandContext::default(),
            )
            .await
            .expect_err("a failed mkdir should fail the build");
        let failure = error
            .downcast_ref::<TemplateBuildFailure>()
            .expect("the error should be a TemplateBuildFailure");
        assert_eq!(failure.reason.step.as_deref(), Some("WORKDIR /app"));
    }
}
