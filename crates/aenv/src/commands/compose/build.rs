//! `aenv compose build`: plans a Compose project locally, builds every service
//! that declares `build` like `aenv build --image`, and writes an image-only
//! Compose file that pins each built service to its published image.
//! `aenv compose up` builds services the same way.

use super::{parse_env, plan, read_compose};
use crate::client::Client;
use crate::commands::build::{BuildkitArgs, ImageOptions, RegistryPush, ServiceImage};
use anyhow::{ensure, Context, Result};
use clap::Args as ClapArgs;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv compose build -f compose.yaml
  aenv compose build -f compose.yaml --name web --output web.built.yaml
  aenv compose up -f compose.built.yaml

Each service that declares build: is built with the server's builder and
published as aenv/PROJECT-SERVICE:latest. The output pins every built service to
its image digest and is written only after every build succeeds. Only explicit
--env values are used for interpolation and build arguments; the local
environment and .env files are not loaded.")]
pub(super) struct BuildArgs {
    /// Compose YAML or JSON file; build contexts resolve relative to its directory
    #[arg(short = 'f', long, default_value = "compose.yaml", value_name = "PATH")]
    file: PathBuf,
    /// Project name that prefixes service image names [default: the Compose
    /// file's directory name]
    #[arg(long, value_name = "PROJECT")]
    name: Option<String>,
    /// Image-only Compose file to write; must not exist [default:
    /// compose.built.yaml beside the Compose file]
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
    /// Compose interpolation variable (repeatable; last value wins)
    #[arg(long = "env", value_name = "KEY=VALUE", value_parser = parse_env)]
    environment: Vec<(String, String)>,
    /// Enable an optional Compose profile (repeatable)
    #[arg(long = "profile", value_name = "NAME")]
    profiles: Vec<String>,
    /// Also push each image to a unique tag in this registry repository, e.g.
    /// registry.example.com/team/images, and reference that tag
    #[arg(long, value_name = "REGISTRY/REPOSITORY")]
    image_repository: Option<String>,
    /// Allow HTTP or untrusted TLS for image pushes (development registries only)
    #[arg(long, requires = "image_repository")]
    registry_insecure: bool,
    /// Compose planner executable [default: aenv-compose-plan next to aenv]
    #[arg(long, value_name = "PATH")]
    planner: Option<PathBuf>,
    #[command(flatten)]
    buildkit: BuildkitArgs,
}

/// `BuildPlan` in services/compose/plan.go.
#[derive(Debug, Deserialize)]
pub(super) struct BuildPlan {
    pub(super) compose: Value,
    pub(super) services: Vec<BuildService>,
}

/// `BuildService` in services/compose/plan.go.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BuildService {
    name: String,
    context: PathBuf,
    dockerfile: PathBuf,
    #[serde(default)]
    args: BTreeMap<String, String>,
    target: Option<String>,
    #[serde(default)]
    no_cache: bool,
}

fn validate_repository(repository: &str) -> Result<()> {
    let (host, path) = repository
        .split_once('/')
        .context("--image-repository must include a registry host and repository path")?;
    ensure!(
        !host.is_empty()
            && (host.contains('.') || host.contains(':') || host == "localhost")
            && host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".-:".contains(&b))
            && !path.is_empty()
            && path.len() <= 200
            && path.split('/').all(|part| !part.is_empty()
                && part
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))),
        "invalid --image-repository; use REGISTRY/REPOSITORY without a scheme, tag, or digest"
    );
    Ok(())
}

/// Lowercase letters and digits joined by single `.`, `_`, or `-`, as image
/// names require.
fn normalize_name(value: &str) -> String {
    let mut name = String::new();
    for c in value.chars().map(|c| c.to_ascii_lowercase()) {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            name.push(c);
        } else if !name.is_empty() && !name.ends_with(['.', '_', '-']) {
            name.push(if matches!(c, '.' | '_') { c } else { '-' });
        }
    }
    name.trim_end_matches(['.', '_', '-']).to_owned()
}

/// Like Docker Compose, names each service's image PROJECT-SERVICE.
fn image_names<'a>(
    project: &str,
    services: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<String>> {
    let prefix = normalize_name(project);
    ensure!(
        !prefix.is_empty(),
        "cannot derive an image name from project {project:?}; set --name"
    );
    let mut seen = HashSet::new();
    services
        .into_iter()
        .map(|service| {
            let name = normalize_name(&format!("{prefix}-{service}"));
            ensure!(
                name.len() <= 128,
                "image name {name} exceeds 128 characters; set a shorter --name"
            );
            ensure!(
                seen.insert(name.clone()),
                "several services map to image name {name}; rename one of them"
            );
            Ok(name)
        })
        .collect()
}

/// Defaults to the name of the directory that build contexts resolve against.
pub(super) fn project_name(name: Option<String>, base: &Path) -> String {
    name.unwrap_or_else(|| {
        base.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    })
}

/// Points each planned service at the `latest` tag of its image, which
/// `aenv compose build` and `aenv compose up` publish.
pub(super) fn use_latest(plan: &mut BuildPlan, project: &str) -> Result<()> {
    let names = image_names(
        project,
        plan.services.iter().map(|service| service.name.as_str()),
    )?;
    for (service, name) in plan.services.iter().zip(names) {
        plan.compose["services"][&service.name]["image"] =
            Value::String(format!("aenv/{name}:latest"));
    }
    Ok(())
}

/// Builds each planned service like `aenv build --image`, one at a time, and
/// points it at its image digest, or with `repository` at a unique pushed tag.
pub(super) async fn build_services(
    client: &Client,
    plan: &mut BuildPlan,
    base: &Path,
    project: &str,
    buildkit: &BuildkitArgs,
    repository: Option<&str>,
    registry_insecure: bool,
) -> Result<()> {
    let names = image_names(
        project,
        plan.services.iter().map(|service| service.name.as_str()),
    )?;

    // Validate every context and Dockerfile before the first build. As in
    // Compose, contexts resolve against the Compose file's directory and
    // Dockerfiles against their context.
    let id = uuid::Uuid::new_v4().simple().to_string();
    let mut images = Vec::new();
    for (index, (service, name)) in plan.services.drain(..).zip(names).enumerate() {
        let pushed = repository.map(|repository| format!("{repository}:aenv-{id}-{index}"));
        let mut buildkit = buildkit.clone();
        buildkit.no_cache |= service.no_cache;
        let context = base.join(&service.context);
        let image = ServiceImage::prepare(
            name,
            &context,
            &context.join(&service.dockerfile),
            service
                .args
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect(),
            buildkit,
            ImageOptions {
                target: service.target,
                push: pushed.clone().map(|image| RegistryPush {
                    image,
                    insecure: registry_insecure,
                }),
            },
        )
        .with_context(|| format!("service {}", service.name))?;
        images.push((service.name, image, pushed));
    }

    // Each published image remains usable if a later service fails.
    for (service, image, pushed) in images {
        match &pushed {
            Some(tag) => eprintln!(
                "Building Compose service {service} as aenv/{} -> {tag}",
                image.name()
            ),
            None => eprintln!(
                "Building Compose service {service} as aenv/{}",
                image.name()
            ),
        }
        let digest = image
            .build(client)
            .await
            .with_context(|| format!("building Compose service {service}"))?;
        plan.compose["services"][&service]["image"] = Value::String(pushed.unwrap_or(digest));
    }
    Ok(())
}

pub(super) fn run(mut args: BuildArgs) -> Result<()> {
    args.buildkit.check()?;
    if let Some(repository) = &args.image_repository {
        validate_repository(repository)?;
    }
    let source = args
        .file
        .canonicalize()
        .with_context(|| format!("locating Compose file {}", args.file.display()))?;
    let base = source
        .parent()
        .context("Compose file has no parent directory")?;
    let project = project_name(args.name, base);
    let output = args
        .output
        .clone()
        .unwrap_or_else(|| base.join("compose.built.yaml"));
    // Refuse an existing destination before allocating builders. Persisting
    // without clobbering also closes the race with another writer.
    ensure!(
        output.symlink_metadata().is_err(),
        "output {} already exists; choose another --output",
        output.display()
    );
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut destination =
        tempfile::NamedTempFile::new_in(parent).context("preparing the Compose output")?;
    let compose = read_compose(std::fs::File::open(&source)?)?;
    let planner = match args.planner {
        Some(planner) => planner,
        None => std::env::current_exe()?.with_file_name(plan::PLANNER),
    };
    let rt = crate::commands::tokio_rt()?;
    let mut planned: BuildPlan = rt.block_on(plan::prepare(
        &planner,
        &plan::Request {
            mode: Some("build"),
            compose: &compose,
            compose_env: args.environment.into_iter().collect(),
            profiles: args.profiles,
        },
    ))?;
    ensure!(
        !planned.services.is_empty(),
        "no selected service declares build; run aenv compose up directly"
    );
    let client = Client::from_env()?;
    rt.block_on(build_services(
        &client,
        &mut planned,
        base,
        &project,
        &args.buildkit,
        args.image_repository.as_deref(),
        args.registry_insecure,
    ))?;
    let mut encoded = serde_json::to_vec_pretty(&planned.compose)?;
    encoded.push(b'\n');
    // aenv compose up accepts YAML or JSON up to 1 MiB.
    ensure!(
        encoded.len() <= 1024 * 1024,
        "generated Compose file exceeds 1 MiB"
    );
    destination.write_all(&encoded)?;
    destination.as_file().sync_all()?;
    destination
        .persist_noclobber(&output)
        .with_context(|| format!("writing Compose output {}", output.display()))?;
    eprintln!("Built Compose file: {}", output.display());
    eprintln!("Start it with: aenv compose up -f {}", output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};
    use serde_json::json;

    fn parse(arguments: &str) -> Result<BuildArgs, clap::Error> {
        match crate::Cli::try_parse_from(arguments.split_whitespace())?.cmd {
            crate::Cmd::Compose(super::super::Args {
                cmd: super::super::Sub::Build(args),
            }) => Ok(args),
            _ => panic!("expected compose build"),
        }
    }

    #[test]
    fn parses_build_flags() {
        crate::Cli::command().debug_assert();
        let args = parse(
            "aenv compose build -f env/compose.yaml --name task --output out.yaml \
             --env TAG=a=b --profile worker --planner /opt/plan \
             --image-repository example.com/team/images --registry-insecure \
             --no-cache --timeout 600 --progress plain",
        )
        .unwrap();
        assert_eq!(args.file, PathBuf::from("env/compose.yaml"));
        assert_eq!(args.name.as_deref(), Some("task"));
        assert_eq!(args.output, Some(PathBuf::from("out.yaml")));
        assert_eq!(args.environment, [("TAG".into(), "a=b".into())]);
        assert_eq!(args.profiles, ["worker"]);
        assert!(args.registry_insecure && args.buildkit.no_cache);
        assert_eq!(args.planner, Some(PathBuf::from("/opt/plan")));
        assert_eq!(
            args.image_repository.as_deref(),
            Some("example.com/team/images")
        );
        let defaults = parse("aenv compose build").unwrap();
        assert_eq!(defaults.file, PathBuf::from("compose.yaml"));
        assert!(defaults.name.is_none() && defaults.output.is_none());
        assert!(!defaults.buildkit.no_cache);
        for invalid in [
            "aenv compose build --registry-insecure",
            "aenv compose build --env =value",
            "aenv compose build --timeout 0",
            "aenv compose build --build-arg A=B",
            "aenv compose build --image",
        ] {
            assert!(parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn build_plan_matches_go_build_plan_json() {
        let plan: BuildPlan = serde_json::from_value(json!({
            "compose": {"services": {"app": {"image": "aenv-build/app:pending"}}},
            "services": [
                {"name": "app", "context": "./app", "dockerfile": "docker/Custom",
                 "args": {"A": "b"}, "target": "final", "noCache": true},
                {"name": "worker", "context": ".", "dockerfile": "Dockerfile", "args": {}},
            ],
        }))
        .unwrap();
        let app = &plan.services[0];
        assert_eq!(app.context, PathBuf::from("./app"));
        assert_eq!(app.dockerfile, PathBuf::from("docker/Custom"));
        assert_eq!(app.args["A"], "b");
        assert_eq!(app.target.as_deref(), Some("final"));
        assert!(app.no_cache);
        assert!(plan.services[1].target.is_none() && !plan.services[1].no_cache);
    }

    #[test]
    fn service_images_are_named_like_docker_compose() {
        assert_eq!(
            image_names("My_App", ["web", "Worker.1"]).unwrap(),
            ["my_app-web", "my_app-worker.1"]
        );
        assert_eq!(
            image_names("--odd  name--", ["db"]).unwrap(),
            ["odd-name-db"]
        );
        assert!(image_names("___", ["web"]).is_err());
        assert!(image_names("app", ["Web", "web"]).is_err());
        assert!(image_names(&"a".repeat(126), ["web"]).is_err());
    }

    #[test]
    fn project_defaults_to_the_base_directory_name() {
        let base = Path::new("/work/My App");
        assert_eq!(project_name(None, base), "My App");
        assert_eq!(project_name(Some("task".into()), base), "task");
    }

    #[test]
    fn without_building_services_use_their_latest_images() {
        let mut plan: BuildPlan = serde_json::from_value(json!({
            "compose": {"services": {
                "app": {"image": "aenv-build/app:pending"},
                "db": {"image": "postgres:17"},
            }},
            "services": [{"name": "app", "context": ".", "dockerfile": "Dockerfile"}],
        }))
        .unwrap();
        use_latest(&mut plan, "My_App").unwrap();
        assert_eq!(
            plan.compose,
            json!({"services": {
                "app": {"image": "aenv/my_app-app:latest"},
                "db": {"image": "postgres:17"},
            }})
        );
        assert!(use_latest(&mut plan, "___").is_err());
    }

    #[test]
    fn repository_cannot_inject_exporter_options_or_reuse_a_tag() {
        for value in [
            "example.com/team/images",
            "localhost:5000/images",
            "192.0.2.10:5000/build",
        ] {
            assert!(validate_repository(value).is_ok(), "{value}");
        }
        for value in [
            "images",
            "https://example.com/images",
            "example.com/images:latest",
            "example.com/images,push=false",
            "example.com//images",
            "example.com/images@sha256:abc",
            "example.com/Upper",
        ] {
            assert!(validate_repository(value).is_err(), "{value}");
        }
    }
}
