use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use shell_util::shell_quote;
use tracing::debug;

use super::build_spec::{TemplateBuildStep, TemplateBuildStepKind};
use super::copy_plan::{plan_copy_archive, CopyOwnership, CopyRequest};
use super::errors::{command_output_suffix, TemplateBuildFailure};
use crate::cfg::ConfigManager;
use crate::sandbox::{ProcessOpts, SandboxExecutor};
use crate::snapshot::CommandContext;

/// Validates a Docker-style `--chown` value (`user`, `uid`, `user:group`).
///
/// The value ends up in a shell command inside the build sandbox (quoted), so
/// this stays conservative rather than mirroring every libc name rule.
fn is_valid_chown_spec(spec: &str) -> bool {
    fn valid_part(part: &str) -> bool {
        // A leading '-' would be read as an option by the `id` lookup below.
        !part.is_empty()
            && !part.starts_with('-')
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    }
    let mut parts = spec.split(':');
    let (user, group, extra) = (parts.next(), parts.next(), parts.next());
    if extra.is_some() {
        return false;
    }
    match (user, group) {
        (Some(user), None) => valid_part(user),
        (Some(user), Some(group)) => valid_part(user) && valid_part(group),
        _ => false,
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TemplateStepExecutor;

impl TemplateStepExecutor {
    pub(crate) fn new() -> Self {
        Self
    }

    #[tracing::instrument(
        skip(self, sandbox, steps, initial_context, build_archives),
        fields(step_count = steps.len())
    )]
    pub(crate) async fn execute(
        &self,
        sandbox: &impl SandboxExecutor,
        steps: &[TemplateBuildStep],
        initial_context: CommandContext,
        build_archives: &HashMap<String, PathBuf>,
    ) -> Result<CommandContext> {
        let mut context = initial_context;

        debug!("executing template build steps");
        for step in steps {
            match &step.kind {
                TemplateBuildStepKind::Env { key, value } => {
                    context = context.with_env_var(key.clone(), value.clone());
                }
                TemplateBuildStepKind::Workdir { path } => {
                    let resolved = resolve_workdir(&context.workdir, &path.to_string_lossy());
                    self.ensure_workdir(sandbox, &resolved).await?;
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
                TemplateBuildStepKind::Copy {
                    src,
                    dest,
                    files_hash,
                    user,
                    mode,
                } => {
                    self.copy_step(
                        sandbox,
                        &context,
                        build_archives,
                        src,
                        dest,
                        files_hash,
                        user.as_deref(),
                        *mode,
                    )
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
    /// Creation goes through envd's filesystem service rather than exec'ing
    /// `mkdir`: minimal images (scratch, distroless, Nix-style) may ship no
    /// userland at all, and Docker itself creates WORKDIR without consulting
    /// the image.
    async fn ensure_workdir(&self, sandbox: &impl SandboxExecutor, path: &str) -> Result<()> {
        sandbox.create_dir_all(path).await.with_context(|| {
            TemplateBuildFailure::with_step("build step failed", format!("WORKDIR {path}"))
        })
    }

    /// Applies one COPY step: rewrites the uploaded context archive to final
    /// absolute guest paths on the host, streams it into the sandbox via
    /// envd, and extracts it at `/` inside the guest.
    #[allow(clippy::too_many_arguments)]
    async fn copy_step(
        &self,
        sandbox: &impl SandboxExecutor,
        context: &CommandContext,
        build_archives: &HashMap<String, PathBuf>,
        src: &str,
        dest: &str,
        files_hash: &str,
        user: Option<&str>,
        mode: Option<u32>,
    ) -> Result<()> {
        let step_label = format!("COPY {src} {dest}");
        let with_step = |message: String| TemplateBuildFailure::with_step(message, &step_label);

        let archive = build_archives.get(files_hash).ok_or_else(|| {
            with_step(format!(
                "build step failed: build context archive '{files_hash}' has not been uploaded"
            ))
        })?;

        if let Some(user) = user {
            if !is_valid_chown_spec(user) {
                return Err(
                    with_step(format!("build step failed: invalid COPY user '{user}'")).into(),
                );
            }
        }

        // Resolve --chown against the image's own accounts, like Docker, and
        // bake the numeric result into the archive headers. Applying it after
        // extraction with `chown -R` would also rewrite pre-existing files
        // under the destination.
        let ownership = match user {
            Some(user) => Some(self.resolve_ownership(sandbox, user, &step_label).await?),
            None => None,
        };

        let rewritten = tempfile::Builder::new()
            .prefix("agentenv-copy-")
            .suffix(".tar")
            .tempfile()
            .context("create rewritten copy archive")?;
        let plan = plan_copy_archive(
            &CopyRequest {
                source_tar: archive,
                src,
                dest,
                workdir: &context.workdir,
                mode,
                ownership,
                max_total_bytes: ConfigManager::global_config()
                    .template_build
                    .files_max_context_mib
                    .saturating_mul(1024 * 1024),
            },
            rewritten.path(),
        )
        .map_err(|error| with_step(format!("build step failed: {error:#}")))?;
        debug!(
            files_hash,
            entries = plan.entry_count,
            bytes = plan.total_bytes,
            "prepared copy archive"
        );

        let guest_archive = format!("/tmp/.agentenv-copy-{}.tar", uuid::Uuid::new_v4());
        sandbox
            .upload_file(rewritten.path(), &guest_archive, "root")
            .await
            .with_context(|| with_step("build step failed: upload build context".to_string()))?;

        // The plan never emits a header for the destination root, so a
        // pre-existing destination keeps its metadata. Create it here when it
        // is missing so a fresh destination still gets the requested
        // ownership; `tar -C /` auto-creates any missing intermediate
        // directory. File-only archives carry no directory member for the
        // root, hence the `dest_is_dir` gate rather than `skipped_dest_root`
        // alone.
        let mut script = String::new();
        if plan.skipped_dest_root || plan.dest_is_dir {
            let dest_root = shell_quote(&plan.dest_root);
            script.push_str(&format!("if [ ! -e {dest_root} ]; then\n"));
            script.push_str(&format!("  mkdir -p -- {dest_root} || exit 1\n"));
            if let Some(owner) = ownership {
                script.push_str(&format!(
                    "  chown {}:{} -- {dest_root} || exit 1\n",
                    owner.uid, owner.gid
                ));
            }
            // `--chmod` applies to copied content only. `skipped_dest_root`
            // means the archive itself carried the destination directory, so
            // the mode is the one that entry would have received; a directory
            // synthesized for a single-file copy is not copied content and
            // must not take the file's mode.
            if let Some(mode) = mode.filter(|_| plan.skipped_dest_root) {
                script.push_str(&format!("  chmod {mode:o} -- {dest_root} || exit 1\n"));
            }
            script.push_str("fi\n");
        }
        // `--no-overwrite-dir` (GNU tar) keeps extraction from restoring mode
        // and ownership onto directories that already exist in the image;
        // directories tar creates still receive the archive header's metadata.
        script.push_str(&format!(
            "tar -xp --no-overwrite-dir -f {archive} -C /\nrc=$?\nrm -f {archive}\nif [ $rc -ne 0 ]; then exit $rc; fi\n",
            archive = shell_quote(&guest_archive),
        ));

        let output = sandbox
            .run_command_with_opts("/bin/bash", &["-lc", &script], &ProcessOpts::default())
            .await
            .with_context(|| with_step("build step failed".to_string()))?;
        if output.exit_code != 0 {
            let message = format!(
                "build step failed: extracting the build context exited with status {}{}",
                output.exit_code,
                command_output_suffix(&output.stdout, &output.stderr)
            );
            return Err(with_step(message).into());
        }
        Ok(())
    }

    /// Resolves a `--chown` spec to numeric ids inside the build sandbox.
    ///
    /// Docker resolves names against the image's own `/etc/passwd` and
    /// `/etc/group`, so the lookup has to happen in the guest. A failed lookup
    /// fails the step the way Docker does rather than silently falling back to
    /// root.
    async fn resolve_ownership(
        &self,
        sandbox: &impl SandboxExecutor,
        user: &str,
        step_label: &str,
    ) -> Result<CopyOwnership> {
        let (user_part, group_part) = user.split_once(':').unwrap_or((user, ""));
        let script = format!(
            r#"set -eu
case "{user}" in
  *[!0-9]*) uid=$(id -u -- "{user}"); ugid=$(id -g -- "{user}") ;;
  *) uid="{user}"; ugid="{user}" ;;
esac
if [ -n "{group}" ]; then
  case "{group}" in
    *[!0-9]*) gid=$(awk -F: -v n="{group}" '$1==n{{print $3; f=1}} END{{exit !f}}' /etc/group) ;;
    *) gid="{group}" ;;
  esac
else
  gid="$ugid"
fi
printf '%s %s\n' "$uid" "$gid"
"#,
            user = user_part,
            group = group_part,
        );

        let output = sandbox
            .run_command_with_opts("/bin/bash", &["-lc", &script], &ProcessOpts::default())
            .await
            .with_context(|| {
                TemplateBuildFailure::with_step(
                    "build step failed: resolve COPY ownership".to_string(),
                    step_label,
                )
            })?;
        if output.exit_code != 0 {
            // A nonzero exit also covers a missing group, a `/etc/group` the
            // lookup cannot read, and a guest without `id`/`awk`, so the
            // message must not assert that the user is absent.
            return Err(TemplateBuildFailure::with_step(
                format!(
                    "build step failed: could not resolve COPY ownership '{user}' in the image; \
                     the user or group may not exist{}",
                    command_output_suffix(&output.stdout, &output.stderr)
                ),
                step_label,
            )
            .into());
        }

        let parsed = output
            .stdout
            .split_whitespace()
            .map(str::parse::<u64>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .ok()
            .filter(|ids| ids.len() == 2);
        let Some(ids) = parsed else {
            return Err(TemplateBuildFailure::with_step(
                format!("build step failed: could not resolve COPY user '{user}'"),
                step_label,
            )
            .into());
        };
        Ok(CopyOwnership {
            uid: ids[0],
            gid: ids[1],
        })
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

/// Resolve a WORKDIR value to the absolute, lexically normalized path Docker
/// would record: relative paths join the current workdir, and `.` / `..`
/// components are resolved without consulting the filesystem.
fn resolve_workdir(current: &str, path: &str) -> String {
    use std::path::{Component, Path, PathBuf};

    let base = if current.is_empty() { "/" } else { current };
    let joined = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        Path::new(base).join(path)
    };
    let mut parts: Vec<std::ffi::OsString> = Vec::new();
    for component in joined.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                parts.pop();
            }
            Component::Normal(part) => parts.push(part.to_os_string()),
        }
    }
    let mut resolved = PathBuf::from("/");
    for part in parts {
        resolved.push(part);
    }
    resolved.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use shell_util::shell_quote;

    use super::TemplateStepExecutor;
    use crate::sandbox::{Executor, ProcessHandle, ProcessOpts, ProcessOutput, SandboxExecutor};
    use crate::snapshot::CommandContext;
    use crate::template::build_spec::TemplateBuildStep;
    use crate::template::errors::TemplateBuildFailure;

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

    /// One recorded executor call: (operation, args, cwd).
    type RecordedCall = (String, Vec<String>, Option<String>);

    /// Records every command and directory creation the executor performs.
    struct RecordingSandbox {
        commands: Mutex<Vec<RecordedCall>>,
        /// Stdout every recorded command reports back.
        stdout: String,
        exit_code: i32,
    }

    impl RecordingSandbox {
        fn succeeding() -> Self {
            Self {
                commands: Mutex::new(Vec::new()),
                stdout: String::new(),
                exit_code: 0,
            }
        }

        fn failing() -> Self {
            Self {
                commands: Mutex::new(Vec::new()),
                stdout: String::new(),
                exit_code: 1,
            }
        }

        fn commands(&self) -> Vec<RecordedCall> {
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
                stdout: self.stdout.clone(),
                stderr: String::new(),
                exit_code: self.exit_code,
            })
        }
        async fn create_dir_all(&self, path: &str) -> Result<()> {
            self.commands
                .lock()
                .expect("commands mutex should not be poisoned")
                .push(("create_dir_all".to_string(), vec![path.to_string()], None));
            if self.exit_code == 0 {
                Ok(())
            } else {
                Err(anyhow!("permission denied"))
            }
        }
        async fn start_process(
            &self,
            _cmd: &str,
            _args: &[&str],
            _opts: &ProcessOpts,
        ) -> Result<ProcessHandle> {
            Err(anyhow!("not used"))
        }
        async fn upload_file(
            &self,
            _local_path: &std::path::Path,
            _guest_path: &str,
            _username: &str,
        ) -> Result<()> {
            Ok(())
        }
    }

    /// Writes a one-file build context archive and returns its path.
    fn single_file_archive(dir: &std::path::Path) -> std::path::PathBuf {
        let tar_path = dir.join("context.tar");
        let mut builder =
            tar::Builder::new(std::fs::File::create(&tar_path).expect("create context tar"));
        let contents = b"e2b\n";
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o644);
        header.set_size(contents.len() as u64);
        builder
            .append_data(&mut header, "requirements.txt", &contents[..])
            .expect("append file");
        builder.finish().expect("finish context tar");
        tar_path
    }

    async fn run(steps: Vec<TemplateBuildStep>) -> CommandContext {
        TemplateStepExecutor::new()
            .execute(
                &NoopSandbox,
                &steps,
                CommandContext::default(),
                &HashMap::new(),
            )
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
            .execute(
                &NoopSandbox,
                &[TemplateBuildStep::user("zzz")],
                initial,
                &HashMap::new(),
            )
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
                &HashMap::new(),
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
    async fn copy_step_fails_without_uploaded_archive() {
        let err = TemplateStepExecutor::new()
            .execute(
                &NoopSandbox,
                &[TemplateBuildStep::copy(
                    "hello.txt",
                    "/hello.txt",
                    "aabbccddeeff0011",
                    None,
                    None,
                )],
                CommandContext::default(),
                &HashMap::new(),
            )
            .await
            .expect_err("missing archive should fail the step");
        assert!(err.to_string().contains("has not been uploaded"));
    }

    #[tokio::test]
    async fn copy_step_prepares_a_directory_dest_without_chmod() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut archives = HashMap::new();
        archives.insert(
            "aabbccddeeff0011".to_string(),
            single_file_archive(dir.path()),
        );
        let sandbox = RecordingSandbox {
            stdout: "1000 2000\n".to_string(),
            ..RecordingSandbox::succeeding()
        };

        TemplateStepExecutor::new()
            .execute(
                &sandbox,
                &[TemplateBuildStep::copy(
                    "requirements.txt",
                    "/home/user/",
                    "aabbccddeeff0011",
                    Some("1000:2000".to_string()),
                    Some(0o600),
                )],
                CommandContext::default(),
                &archives,
            )
            .await
            .expect("copy step should execute");

        let commands = sandbox.commands();
        let script = &commands.last().expect("extraction command").1[1];
        let dest_root = shell_quote("/home/user");
        assert!(
            script.contains(&format!("mkdir -p -- {dest_root}")),
            "a missing directory destination must be created: {script}"
        );
        assert!(
            script.contains(&format!("chown 1000:2000 -- {dest_root}")),
            "a created destination must carry the requested ownership: {script}"
        );
        assert!(
            !script.contains("chmod"),
            "--chmod applies to copied content, not to a synthesized destination: {script}"
        );
    }

    #[tokio::test]
    async fn failed_ownership_lookup_does_not_claim_the_user_is_absent() {
        let sandbox = RecordingSandbox::failing();

        let err = TemplateStepExecutor::new()
            .resolve_ownership(&sandbox, "root:missing-group", "COPY a b")
            .await
            .expect_err("a failed ownership lookup must fail the step");

        let message = err.to_string();
        assert!(
            message.contains("could not resolve COPY ownership 'root:missing-group'"),
            "unexpected message: {message}"
        );
        assert!(
            !message.contains("does not exist"),
            "the same exit also covers a missing group or an unusable lookup: {message}"
        );
    }

    #[test]
    fn chown_spec_validation() {
        assert!(super::is_valid_chown_spec("user"));
        assert!(super::is_valid_chown_spec("user:group"));
        assert!(super::is_valid_chown_spec("1000:1000"));
        assert!(super::is_valid_chown_spec("www-data"));
        assert!(!super::is_valid_chown_spec(""));
        assert!(!super::is_valid_chown_spec("user:"));
        assert!(!super::is_valid_chown_spec("user:group:extra"));
        assert!(!super::is_valid_chown_spec("user name"));
        assert!(!super::is_valid_chown_spec("user;rm -rf /"));
        assert!(!super::is_valid_chown_spec("-r"));
        assert!(!super::is_valid_chown_spec("user:-g"));
    }

    #[tokio::test]
    async fn workdir_step_updates_workdir() {
        let sandbox = RecordingSandbox::succeeding();
        let ctx = TemplateStepExecutor::new()
            .execute(
                &sandbox,
                &[TemplateBuildStep::workdir("/workspace")],
                CommandContext::default(),
                &HashMap::new(),
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
                &HashMap::new(),
            )
            .await
            .unwrap();
        let commands = sandbox.commands();
        assert_eq!(commands.len(), 1);
        let (op, args, _) = &commands[0];
        // The directory is created through envd's filesystem service, never
        // by exec'ing a binary from the image (which scratch/distroless
        // images legitimately do not ship).
        assert_eq!(op, "create_dir_all");
        assert_eq!(args, &["/app"]);
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
                &HashMap::new(),
            )
            .await
            .unwrap();
        // Docker resolves a relative WORKDIR against the previous one; the
        // created directory and the recorded workdir must both be absolute.
        assert_eq!(ctx.workdir, "/base/nested");
        let commands = sandbox.commands();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[1].1, vec!["/base/nested"]);
    }

    #[tokio::test]
    async fn workdir_step_normalizes_dot_components() {
        let sandbox = RecordingSandbox::succeeding();
        let ctx = TemplateStepExecutor::new()
            .execute(
                &sandbox,
                &[
                    TemplateBuildStep::workdir("/base/deep"),
                    TemplateBuildStep::workdir("../app"),
                ],
                CommandContext::default(),
                &HashMap::new(),
            )
            .await
            .unwrap();
        // Docker records the normalized path, not /base/deep/../app.
        assert_eq!(ctx.workdir, "/base/app");
        assert_eq!(sandbox.commands()[1].1, vec!["/base/app"]);
    }

    #[tokio::test]
    async fn workdir_step_failure_is_labelled_with_the_step() {
        let sandbox = RecordingSandbox::failing();
        let error = TemplateStepExecutor::new()
            .execute(
                &sandbox,
                &[TemplateBuildStep::workdir("/app")],
                CommandContext::default(),
                &HashMap::new(),
            )
            .await
            .expect_err("a failed directory creation should fail the build");
        let failure = error
            .downcast_ref::<TemplateBuildFailure>()
            .expect("the error should be a TemplateBuildFailure");
        assert_eq!(failure.reason.step.as_deref(), Some("WORKDIR /app"));
    }
}
