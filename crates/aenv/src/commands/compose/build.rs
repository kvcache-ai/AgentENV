//! `aenv compose build`: plans a Compose project locally, builds every service
//! that declares `build` like `aenv build --image`, and writes an image-only
//! Compose file that pins each built service to its published image.
//! `aenv compose up` builds services the same way.

use super::plan::{self, ProjectArgs};
use crate::client::Client;
use crate::commands::build::{BuildkitArgs, ImageOptions, RegistryPush, ServiceImage};
use anyhow::{ensure, Context, Result};
use clap::Args as ClapArgs;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv compose build
  aenv compose build -f compose.yaml -p web --output web.built.yaml
  aenv compose up -f compose.built.yaml

The project is loaded as Docker Compose loads it, including the process
environment and the project's .env file. Each service that declares build: is
built with the server's builder and published as aenv/PROJECT-SERVICE:latest.
The output pins every built service to its image digest and is written only
after every build succeeds. Like docker compose config, it contains the
resolved environment values and absolute paths.")]
pub(super) struct BuildArgs {
    #[command(flatten)]
    project: ProjectArgs,
    /// Image-only Compose file to write; must not exist [default:
    /// compose.built.yaml in the project directory]
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
    /// Also push each image to a unique tag in this registry repository, e.g.
    /// registry.example.com/team/images, and reference that tag
    #[arg(long, value_name = "REGISTRY/REPOSITORY")]
    image_repository: Option<String>,
    /// Allow HTTP or untrusted TLS for image pushes (development registries only)
    #[arg(long, requires = "image_repository")]
    registry_insecure: bool,
    #[command(flatten)]
    buildkit: BuildkitArgs,
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
        "cannot derive an image name from project {project:?}; set --project-name"
    );
    let mut seen = HashSet::new();
    services
        .into_iter()
        .map(|service| {
            let name = normalize_name(&format!("{prefix}-{service}"));
            ensure!(
                name.len() <= 128,
                "image name {name} exceeds 128 characters; set a shorter --project-name"
            );
            ensure!(
                seen.insert(name.clone()),
                "several services map to image name {name}; rename one of them"
            );
            Ok(name)
        })
        .collect()
}

/// The services that declare build, in plan order.
fn builds(plan: &plan::Plan) -> impl Iterator<Item = (&str, &plan::Build)> {
    plan.services
        .iter()
        .filter_map(|service| Some((service.name.as_str(), service.build.as_ref()?)))
}

/// Maps each service that declares build to the `latest` tag of its image,
/// which `aenv compose build` and `aenv compose up` publish.
pub(super) fn latest_images(plan: &plan::Plan) -> Result<BTreeMap<String, String>> {
    let services: Vec<_> = builds(plan).map(|(name, _)| name).collect();
    let names = image_names(&plan.project_name, services.iter().copied())?;
    Ok(services
        .into_iter()
        .zip(names)
        .map(|(service, name)| (service.to_owned(), format!("aenv/{name}:latest")))
        .collect())
}

/// Builds each service that declares build like `aenv build --image`, one at
/// a time, and maps it to its image digest, or with `repository` to a unique
/// pushed tag.
pub(super) async fn build_services(
    client: &Client,
    plan: &plan::Plan,
    buildkit: &BuildkitArgs,
    repository: Option<&str>,
    registry_insecure: bool,
) -> Result<BTreeMap<String, String>> {
    let services: Vec<_> = builds(plan).collect();
    let names = image_names(&plan.project_name, services.iter().map(|(name, _)| *name))?;

    // Validate every context and Dockerfile before the first build. As in
    // Compose, Dockerfiles resolve against their context.
    let id = uuid::Uuid::new_v4().simple().to_string();
    let mut images = Vec::new();
    for (index, ((service, build), name)) in services.into_iter().zip(names).enumerate() {
        let pushed = repository.map(|repository| format!("{repository}:aenv-{id}-{index}"));
        let mut buildkit = buildkit.clone();
        buildkit.no_cache |= build.no_cache;
        let image = ServiceImage::prepare(
            name,
            &build.context,
            &build.context.join(&build.dockerfile),
            build
                .args
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect(),
            buildkit,
            ImageOptions {
                target: build.target.clone(),
                push: pushed.clone().map(|image| RegistryPush {
                    image,
                    insecure: registry_insecure,
                }),
            },
        )
        .with_context(|| format!("service {service}"))?;
        images.push((service, image, pushed));
    }

    // Each published image remains usable if a later service fails.
    let mut built = BTreeMap::new();
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
        built.insert(service.to_owned(), pushed.unwrap_or(digest));
    }
    Ok(built)
}

pub(super) fn run(mut args: BuildArgs) -> Result<()> {
    args.buildkit.check()?;
    if let Some(repository) = &args.image_repository {
        validate_repository(repository)?;
    }
    let rt = crate::commands::tokio_rt()?;
    let mut planned = rt.block_on(args.project.plan_builds())?;
    ensure!(
        builds(&planned).next().is_some(),
        "no selected service declares build; run aenv compose up directly"
    );
    let output = args
        .output
        .clone()
        .unwrap_or_else(|| planned.project_directory.join("compose.built.yaml"));
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
    let client = Client::from_env()?;
    let images = rt.block_on(build_services(
        &client,
        &planned,
        &args.buildkit,
        args.image_repository.as_deref(),
        args.registry_insecure,
    ))?;
    for (service, image) in images {
        planned.compose["services"][&service]["image"] = Value::String(image);
    }
    let mut encoded = serde_json::to_vec_pretty(&planned.compose)?;
    encoded.push(b'\n');
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
            "aenv compose build -f env/compose.yaml -f env/extra.yaml -p task --output out.yaml \
             --env TAG=a=b --env-file prod.env --profile worker --planner /opt/plan \
             --image-repository example.com/team/images --registry-insecure \
             --no-cache --timeout 600 --progress plain",
        )
        .unwrap();
        assert_eq!(
            args.project.files,
            [
                PathBuf::from("env/compose.yaml"),
                PathBuf::from("env/extra.yaml")
            ]
        );
        assert_eq!(args.project.project_name.as_deref(), Some("task"));
        assert_eq!(args.output, Some(PathBuf::from("out.yaml")));
        assert_eq!(args.project.environment, [("TAG".into(), "a=b".into())]);
        assert_eq!(args.project.env_files, [PathBuf::from("prod.env")]);
        assert_eq!(args.project.profiles, ["worker"]);
        assert!(args.registry_insecure && args.buildkit.no_cache);
        assert_eq!(args.project.planner, Some(PathBuf::from("/opt/plan")));
        assert_eq!(
            args.image_repository.as_deref(),
            Some("example.com/team/images")
        );
        let defaults = parse("aenv compose build").unwrap();
        assert!(defaults.project.files.is_empty());
        assert!(defaults.project.project_name.is_none() && defaults.output.is_none());
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
    fn without_building_services_use_their_latest_images() {
        let plan: plan::Plan = serde_json::from_value(json!({
            "projectName": "my_app",
            "projectDirectory": "/work/my_app",
            "compose": {"services": {"app": {}, "db": {"image": "postgres:17"}}},
            "services": [
                {"name": "app", "build": {"context": "/work/my_app", "dockerfile": "Dockerfile"}},
                {"name": "db", "image": "postgres:17"},
            ],
        }))
        .unwrap();
        assert_eq!(
            latest_images(&plan).unwrap(),
            BTreeMap::from([("app".to_owned(), "aenv/my_app-app:latest".to_owned())])
        );
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
