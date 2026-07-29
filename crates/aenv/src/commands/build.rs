use crate::client::{
    templates::{CreateTemplateV3, StartTemplateBuildV2, TemplateStep},
    Client,
};
use anyhow::{bail, Context, Result};
use clap::Args as ClapArgs;
use shell_util::shell_quote;
use std::path::{Path, PathBuf};

#[derive(ClapArgs)]
pub struct Args {
    /// Path to the Dockerfile used to build the template
    dockerfile: PathBuf,
    /// Template name, optionally with `:tag`
    #[arg(short = 't', long = "tag")]
    name: Option<String>,
    #[command(flatten)]
    resources: super::CpuMemoryArgs,
    /// Override the Dockerfile FROM image used as the template rootfs base. Shortnames like `ubuntu:22.04` are supported.
    #[arg(long = "image", alias = "user-image")]
    user_image: Option<String>,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let dockerfile = std::fs::read_to_string(&args.dockerfile)
        .with_context(|| format!("reading {}", args.dockerfile.display()))?;
    let alias = parse_alias(args.name.as_deref(), &args.dockerfile);
    let user_image = args.user_image.or_else(|| first_from_image(&dockerfile));
    let build_plan = dockerfile_build_plan(&dockerfile)?;

    let req = CreateTemplateV3 {
        name: alias.to_string(),
        tags: Vec::new(),
        cpu_count: args.resources.cpu_count,
        memory_mb: args.resources.memory_mb,
    };
    let resp = client.create_template_v3(&req)?;
    client.start_template_build_v2(
        &resp.template_id,
        &resp.build_id,
        &StartTemplateBuildV2 {
            from_image: user_image,
            steps: build_plan.steps,
            start_cmd: build_plan.start_cmd,
            ready_cmd: None,
        },
    )?;
    println!(
        "Created template {} (build {})",
        resp.template_id, resp.build_id
    );
    println!("Build started.");
    println!("Watch with: aenv template watch {}", resp.template_id);
    Ok(())
}

pub(crate) fn parse_alias(flag: Option<&str>, dockerfile: &Path) -> String {
    flag.map(|s| s.to_string()).unwrap_or_else(|| {
        dockerfile
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .unwrap_or("template")
            .to_string()
    })
}

pub(crate) fn first_from_image(dockerfile: &str) -> Option<String> {
    dockerfile.lines().find_map(|raw| {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let mut parts = line.split_whitespace();
        let instruction = parts.next()?;
        if !instruction.eq_ignore_ascii_case("FROM") {
            return None;
        }
        parts
            .find(|part| !part.starts_with("--"))
            .filter(|image| !image.is_empty())
            .filter(|image| *image != "scratch" && !image.starts_with('$'))
            .map(str::to_string)
    })
}

#[derive(Debug)]
struct DockerfileBuildPlan {
    steps: Vec<TemplateStep>,
    start_cmd: Option<String>,
}

fn dockerfile_build_plan(dockerfile: &str) -> Result<DockerfileBuildPlan> {
    let mut steps = Vec::new();
    let mut entrypoint = None;
    let mut cmd = None;

    for (instruction, args) in dockerfile.lines().filter_map(dockerfile_instruction) {
        let step = match instruction.to_ascii_uppercase().as_str() {
            "FROM" => None,
            "RUN" | "WORKDIR" | "USER" => Some(TemplateStep {
                r#type: instruction.to_ascii_uppercase(),
                args: vec![args.to_string()],
            }),
            "ENV" | "ARG" => Some(TemplateStep {
                r#type: instruction.to_ascii_uppercase(),
                args: key_value_args(instruction, args)?,
            }),
            "ENTRYPOINT" => {
                entrypoint = dockerfile_command(args);
                None
            }
            "CMD" => {
                cmd = dockerfile_command(args);
                None
            }
            "SHELL" | "STOPSIGNAL" => None,
            "EXPOSE" | "VOLUME" | "LABEL" => {
                eprintln!(
                    "warning: {instruction} instruction is not supported and will be ignored"
                );
                None
            }
            "COPY" | "ADD" => {
                bail!("{instruction} instructions are not supported by AENV template builds yet")
            }
            _ => bail!("Dockerfile instruction {instruction} is not supported"),
        };
        if let Some(step) = step {
            steps.push(step);
        }
    }

    Ok(DockerfileBuildPlan {
        steps,
        start_cmd: entrypoint.or(cmd),
    })
}

fn dockerfile_command(args: &str) -> Option<String> {
    let args = args.trim();
    if args.is_empty() {
        return None;
    }
    if args.starts_with('[') {
        if let Ok(parts) = serde_json::from_str::<Vec<String>>(args) {
            if parts.is_empty() {
                return None;
            }
            return Some(
                parts
                    .iter()
                    .map(|s| shell_quote(s))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        }
    }
    Some(args.to_string())
}

fn dockerfile_instruction(line: &str) -> Option<(&str, &str)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (instruction, args) = line
        .split_once(char::is_whitespace)
        .map(|(instruction, args)| (instruction, args.trim()))
        .unwrap_or((line, ""));
    Some((instruction, args))
}

fn key_value_args(instruction: &str, args: &str) -> Result<Vec<String>> {
    let args = args.trim();
    if args.is_empty() {
        bail!("{instruction} instruction requires arguments");
    }

    if instruction.eq_ignore_ascii_case("ARG") {
        let (key, value) = args.split_once('=').unwrap_or((args, ""));
        let key = key.trim();
        if key.is_empty() {
            bail!("ARG instruction requires a non-empty key");
        }
        return Ok(vec![key.to_string(), value.to_string()]);
    }

    if let Some((key, value)) = args.split_once('=') {
        let key = key.trim();
        if !key.is_empty() && !key.contains(char::is_whitespace) {
            return Ok(vec![key.to_string(), value.to_string()]);
        }
    }

    let parts = args.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 2 {
        bail!("ENV instruction requires key/value arguments");
    }
    Ok(vec![parts[0].to_string(), parts[1..].join(" ")])
}

#[cfg(test)]
mod tests {
    use super::{dockerfile_build_plan, first_from_image};

    #[test]
    fn first_from_image_reads_basic_from() {
        assert_eq!(
            first_from_image("FROM ubuntu:24.04\nRUN true"),
            Some("ubuntu:24.04".to_string())
        );
    }

    #[test]
    fn first_from_image_skips_from_options_and_stage_alias() {
        assert_eq!(
            first_from_image("FROM --platform=linux/amd64 ghcr.io/acme/app:latest AS base"),
            Some("ghcr.io/acme/app:latest".to_string())
        );
    }

    #[test]
    fn first_from_image_skips_scratch_and_arg_refs() {
        assert_eq!(
            first_from_image("FROM scratch AS builder\nFROM ubuntu:24.04"),
            Some("ubuntu:24.04".to_string())
        );
        assert_eq!(
            first_from_image("ARG BASE_IMAGE=ubuntu:24.04\nFROM $BASE_IMAGE\nFROM node:20"),
            Some("node:20".to_string())
        );
    }

    #[test]
    fn dockerfile_build_plan_converts_supported_instructions() {
        let plan = dockerfile_build_plan(
            r#"
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app
RUN apt-get update
ARG NODE_ENV=production
"#,
        )
        .unwrap();

        let steps = plan.steps;
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0].r#type, "ENV");
        assert_eq!(steps[0].args, ["DEBIAN_FRONTEND", "noninteractive"]);
        assert_eq!(steps[1].r#type, "WORKDIR");
        assert_eq!(steps[1].args, ["/app"]);
        assert_eq!(steps[2].r#type, "RUN");
        assert_eq!(steps[2].args, ["apt-get update"]);
        assert_eq!(steps[3].r#type, "ARG");
        assert_eq!(steps[3].args, ["NODE_ENV", "production"]);
    }

    #[test]
    fn dockerfile_build_plan_uses_entrypoint_as_start_cmd() {
        let plan = dockerfile_build_plan(
            r#"
FROM ubuntu:24.04
CMD ["ignored"]
ENTRYPOINT ["/usr/bin/env", "bash"]
"#,
        )
        .unwrap();

        assert_eq!(plan.start_cmd, Some("/usr/bin/env bash".to_string()));
    }

    #[test]
    fn dockerfile_build_plan_exec_form_args_with_spaces_are_quoted() {
        let plan = dockerfile_build_plan(
            r#"
FROM ubuntu:24.04
ENTRYPOINT ["sh", "-c", "echo hello world"]
"#,
        )
        .unwrap();

        assert_eq!(plan.start_cmd, Some("sh -c 'echo hello world'".to_string()),);
    }

    #[test]
    fn dockerfile_build_plan_exec_form_cmd_used_when_no_entrypoint() {
        let plan = dockerfile_build_plan(
            r#"
FROM ubuntu:24.04
CMD ["python3", "app.py"]
"#,
        )
        .unwrap();

        assert_eq!(plan.start_cmd, Some("python3 app.py".to_string()));
    }

    #[test]
    fn dockerfile_build_plan_uses_cmd_when_entrypoint_is_absent() {
        let plan = dockerfile_build_plan(
            r#"
FROM ubuntu:24.04
CMD sleep infinity
"#,
        )
        .unwrap();

        assert_eq!(plan.start_cmd, Some("sleep infinity".to_string()));
    }

    #[test]
    fn dockerfile_build_plan_user_produces_step() {
        let plan = dockerfile_build_plan(
            r#"
FROM ubuntu:24.04
USER alice
"#,
        )
        .unwrap();

        let steps = &plan.steps;
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].r#type, "USER");
        assert_eq!(steps[0].args, ["alice"]);
    }

    #[test]
    fn dockerfile_build_plan_skips_expose_volume_label() {
        // These instructions are metadata-only and silently skipped so that
        // standard Dockerfiles (which commonly include them) continue to work.
        let plan = dockerfile_build_plan(
            r#"
FROM ubuntu:24.04
EXPOSE 8080
VOLUME /data
LABEL maintainer=test
RUN echo hi
"#,
        )
        .unwrap();
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].r#type, "RUN");
    }

    #[test]
    fn dockerfile_build_plan_rejects_copy() {
        let err = dockerfile_build_plan("FROM ubuntu:24.04\nCOPY . /app").unwrap_err();
        assert!(err.to_string().contains("COPY"), "{err}");
    }

    #[test]
    fn dockerfile_build_plan_rejects_add() {
        let err = dockerfile_build_plan("FROM ubuntu:24.04\nADD file.tar /app").unwrap_err();
        assert!(err.to_string().contains("ADD"), "{err}");
    }

    #[test]
    fn dockerfile_build_plan_last_user_wins() {
        let plan = dockerfile_build_plan(
            r#"
FROM ubuntu:24.04
USER root
USER alice
"#,
        )
        .unwrap();

        let users: Vec<_> = plan.steps.iter().filter(|s| s.r#type == "USER").collect();
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].args, ["root"]);
        assert_eq!(users[1].args, ["alice"]);
    }
}
