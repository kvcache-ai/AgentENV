//! Runs the Compose planner (`services/compose`) on the client.

use super::parse_env;
use anyhow::{bail, Context, Result};
use clap::Args as ClapArgs;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

/// Installed next to `aenv`.
pub(super) const PLANNER: &str = "aenv-compose-plan";
const PLAN_TIMEOUT: Duration = Duration::from_secs(30);
/// Must match exitInvalid in services/compose/cmd/main.go.
const EXIT_INVALID: i32 = 65;

/// Docker Compose project options. The planner loads the project from them,
/// the process environment and the working directory as Docker Compose does.
#[derive(ClapArgs)]
pub(super) struct ProjectArgs {
    /// Compose configuration file (repeatable; - reads stdin) [default:
    /// compose.yaml or docker-compose.yml in the current or a parent directory,
    /// with its override file]
    #[arg(short = 'f', long = "file", value_name = "PATH")]
    pub files: Vec<PathBuf>,
    /// Project directory [default: the first Compose file's directory]
    #[arg(long, value_name = "DIR")]
    pub project_directory: Option<PathBuf>,
    /// Interpolation environment file (repeatable) [default: .env in the
    /// project directory]
    #[arg(long = "env-file", value_name = "PATH")]
    pub env_files: Vec<PathBuf>,
    /// Project name, which also prefixes built image names [default: the
    /// Compose name attribute or the project directory's name]
    #[arg(
        short = 'p',
        long = "project-name",
        alias = "name",
        value_name = "NAME"
    )]
    pub project_name: Option<String>,
    /// Enable an optional Compose profile (repeatable)
    #[arg(long = "profile", value_name = "NAME")]
    pub profiles: Vec<String>,
    /// Set a process environment variable for the project (repeatable; last
    /// value wins)
    #[arg(long = "env", value_name = "KEY=VALUE", value_parser = parse_env)]
    pub environment: Vec<(String, String)>,
    /// Compose planner executable [default: aenv-compose-plan next to aenv]
    #[arg(long, value_name = "PATH")]
    pub planner: Option<PathBuf>,
}

/// `Plan` in services/compose/plan.go.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Plan {
    pub project_name: String,
    pub project_directory: PathBuf,
    /// The loaded Compose files, `-` for stdin.
    pub compose_files: Vec<PathBuf>,
    /// The rendered project, without build sections.
    pub compose: serde_json::Value,
    /// Active services sorted by name.
    pub services: Vec<Service>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Service {
    pub name: String,
    /// Absent for a service that only declares build.
    #[serde(default)]
    pub image: Option<String>,
    pub build: Option<Build>,
    /// The service whose build produces this service's image.
    #[serde(default)]
    pub built_by: Option<String>,
}

/// `Build` in services/compose/plan.go.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Build {
    /// Absolute local directory.
    pub context: PathBuf,
    /// Relative to the context unless absolute; empty with `dockerfile_inline`.
    pub dockerfile: PathBuf,
    #[serde(default)]
    pub dockerfile_inline: Option<String>,
    #[serde(default)]
    pub args: BTreeMap<String, String>,
    pub target: Option<String>,
    #[serde(default)]
    pub no_cache: bool,
}

impl ProjectArgs {
    /// The flags of `aenv-compose-plan`, which match Docker Compose's, and
    /// `--build` to plan only the image builds.
    fn planner_args(&self, build: bool) -> Vec<OsString> {
        let mut args: Vec<OsString> = Vec::new();
        let mut push = |flag: &str, value: &dyn AsRef<std::ffi::OsStr>| {
            args.push(flag.into());
            args.push(value.as_ref().to_owned());
        };
        for file in &self.files {
            push("-f", file);
        }
        if let Some(directory) = &self.project_directory {
            push("--project-directory", directory);
        }
        for file in &self.env_files {
            push("--env-file", file);
        }
        if let Some(name) = &self.project_name {
            push("-p", name);
        }
        for profile in &self.profiles {
            push("--profile", profile);
        }
        if build {
            args.push("--build".into());
        }
        args
    }

    /// Loads the project and checks that the sandbox can run it.
    pub(super) async fn plan(&self) -> Result<Plan> {
        self.run_planner(false).await
    }

    /// Loads the project and, like `docker compose build`, checks only that
    /// its services can be built.
    pub(super) async fn plan_builds(&self) -> Result<Plan> {
        self.run_planner(true).await
    }

    /// The planner's warnings are forwarded to stderr.
    async fn run_planner(&self, build: bool) -> Result<Plan> {
        let planner = match &self.planner {
            Some(planner) => planner.clone(),
            None => std::env::current_exe()?.with_file_name(PLANNER),
        };
        let reads_stdin = self.files.iter().any(|file| file == Path::new("-"));
        let child = tokio::process::Command::new(&planner)
            .args(self.planner_args(build))
            .envs(self.environment.iter().cloned())
            .stdin(if reads_stdin {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "run {}: rerun the aenv installer to install the Compose planner, or set --planner",
                    planner.display()
                )
            })?;
        let output = tokio::time::timeout(PLAN_TIMEOUT, child.wait_with_output())
            .await
            .context("Compose planning timed out")?
            .context("wait for Compose planner")?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() {
            let message = stderr.trim();
            if output.status.code() == Some(EXIT_INVALID) {
                bail!("invalid Compose project: {message}");
            }
            bail!("Compose planner failed: {message}");
        }
        eprint!("{stderr}");
        serde_json::from_slice(&output.stdout).context("invalid Compose planner output")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serde_json::json;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        project: ProjectArgs,
    }

    fn parse(arguments: &[&str]) -> ProjectArgs {
        Cli::try_parse_from(std::iter::once("aenv").chain(arguments.iter().copied()))
            .unwrap()
            .project
    }

    #[test]
    fn passes_docker_compose_project_flags() {
        let project = parse(&[
            "-f",
            "compose.yaml",
            "--file",
            "-",
            "--project-directory",
            "app",
            "--env-file",
            "a.env",
            "--env-file",
            "b.env",
            "--name",
            "task",
            "--profile",
            "web",
            "--env",
            "VALUE=a=b",
        ]);
        assert_eq!(
            project.planner_args(false),
            [
                "-f",
                "compose.yaml",
                "-f",
                "-",
                "--project-directory",
                "app",
                "--env-file",
                "a.env",
                "--env-file",
                "b.env",
                "-p",
                "task",
                "--profile",
                "web"
            ]
            .map(OsString::from)
        );
        assert_eq!(project.environment, [("VALUE".into(), "a=b".into())]);
        assert!(parse(&[]).planner_args(false).is_empty());
        assert_eq!(parse(&[]).planner_args(true), [OsString::from("--build")]);
        assert_eq!(parse(&["-p", "x"]).project_name.as_deref(), Some("x"));
    }

    #[test]
    fn plan_matches_go_plan_json() {
        let plan: Plan = serde_json::from_value(json!({
            "projectName": "app",
            "projectDirectory": "/work/app",
            "composeFiles": ["/work/app/compose.yaml"],
            "compose": {"name": "app", "services": {}},
            "services": [
                {"name": "build", "build": {"context": "/work/app", "dockerfile": "docker/Custom",
                 "args": {"A": "b"}, "target": "final", "noCache": true}},
                {"name": "inline", "build": {"context": "/work/app", "dockerfile": "",
                 "dockerfileInline": "FROM busybox\n", "args": {}}},
                {"name": "sidecar", "image": "example/app:dev", "builtBy": "build"},
                {"name": "web", "image": "busybox"},
            ],
        }))
        .unwrap();
        assert_eq!(plan.project_name, "app");
        assert_eq!(plan.project_directory, PathBuf::from("/work/app"));
        assert_eq!(
            plan.compose_files,
            [PathBuf::from("/work/app/compose.yaml")]
        );
        let build = plan.services[0].build.as_ref().unwrap();
        assert!(plan.services[0].image.is_none());
        assert_eq!(build.context, PathBuf::from("/work/app"));
        assert_eq!(build.dockerfile, PathBuf::from("docker/Custom"));
        assert!(build.dockerfile_inline.is_none());
        assert_eq!(build.args["A"], "b");
        assert_eq!(build.target.as_deref(), Some("final"));
        assert!(build.no_cache);
        let inline = plan.services[1].build.as_ref().unwrap();
        assert_eq!(inline.dockerfile_inline.as_deref(), Some("FROM busybox\n"));
        assert_eq!(plan.services[2].built_by.as_deref(), Some("build"));
        assert_eq!(plan.services[3].image.as_deref(), Some("busybox"));
        assert!(plan.services[3].build.is_none() && plan.services[3].built_by.is_none());
    }

    #[cfg(unix)]
    fn executable_busy(error: &anyhow::Error) -> bool {
        let error = error.downcast_ref::<std::io::Error>();
        error.and_then(std::io::Error::raw_os_error) == Some(libc::ETXTBSY)
    }

    /// Runs the planner, retrying while another test thread's fork still holds
    /// the just-written script open, which makes exec fail with ETXTBSY.
    #[cfg(unix)]
    async fn plan_written(project: &ProjectArgs) -> Result<Plan> {
        for _ in 0..100 {
            match project.plan().await {
                Err(error) if executable_busy(&error) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                result => return result,
            }
        }
        project.plan().await
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn maps_planner_exit_status_and_environment() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let planner = directory.path().join(PLANNER);
        let mut project = parse(&["-f", "compose.yaml", "--env", "AENV_PLAN_TEST=a=b"]);
        project.planner = Some(planner.clone());
        for (script, expected) in [
            (
                "echo \"$* $AENV_PLAN_TEST\" >&2; exit 65",
                "invalid Compose project: -f compose.yaml a=b",
            ),
            (
                "echo crashed >&2; exit 2",
                "Compose planner failed: crashed",
            ),
        ] {
            std::fs::write(&planner, format!("#!/bin/sh\n{script}\n")).unwrap();
            std::fs::set_permissions(&planner, std::fs::Permissions::from_mode(0o755)).unwrap();
            let error = plan_written(&project).await.unwrap_err();
            assert_eq!(error.to_string(), expected);
        }
        std::fs::write(
            &planner,
            "#!/bin/sh\necho 'warning: ignored' >&2\necho '{\"projectName\":\"p\",\"projectDirectory\":\"/p\",\"composeFiles\":[],\"compose\":{},\"services\":[]}'\n",
        )
        .unwrap();
        assert!(plan_written(&project).await.unwrap().services.is_empty());
    }
}
