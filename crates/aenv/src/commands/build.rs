use crate::client::{
    templates::{CreateTemplateV3, StartTemplateBuildV2, TemplateStep},
    Client,
};
use anyhow::{bail, Context, Result};
use clap::Args as ClapArgs;
use parse_dockerfile::{Command, HereDoc, Instruction, RunInstruction};
use shell_util::shell_quote;
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to the Dockerfile used to build the template
    dockerfile: PathBuf,
    /// Template name
    #[arg(long)]
    name: String,
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
    let user_image = args.user_image.or_else(|| first_from_image(&dockerfile));
    let build_plan = dockerfile_build_plan(&dockerfile)?;

    let req = CreateTemplateV3 {
        name: args.name,
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

pub(crate) fn first_from_image(dockerfile: &str) -> Option<String> {
    let parsed = parse_dockerfile::parse(dockerfile).ok()?;
    parsed.instructions.iter().find_map(|instruction| {
        let Instruction::From(from) = instruction else {
            return None;
        };
        let image = from.image.value.as_ref();
        (!image.is_empty() && image != "scratch" && !image.starts_with('$'))
            .then(|| image.to_string())
    })
}

#[derive(Debug)]
struct DockerfileBuildPlan {
    steps: Vec<TemplateStep>,
    start_cmd: Option<String>,
}

fn dockerfile_build_plan(dockerfile: &str) -> Result<DockerfileBuildPlan> {
    let parsed = parse_dockerfile::parse(dockerfile).context("parsing Dockerfile")?;
    let escape = parsed
        .parser_directives
        .escape
        .as_ref()
        .map_or('\\', |directive| directive.value.value);
    let mut steps = Vec::new();
    let mut entrypoint = None;
    let mut cmd = None;

    for instruction in &parsed.instructions {
        let step = match instruction {
            Instruction::From(_) => None,
            Instruction::Run(run) => Some(TemplateStep {
                r#type: "RUN".to_string(),
                args: vec![dockerfile_run_command(run, escape)?],
            }),
            Instruction::Workdir(workdir) => Some(TemplateStep {
                r#type: "WORKDIR".to_string(),
                args: vec![workdir.arguments.value.to_string()],
            }),
            Instruction::User(user) => Some(TemplateStep {
                r#type: "USER".to_string(),
                args: vec![user.arguments.value.to_string()],
            }),
            Instruction::Env(env) => Some(TemplateStep {
                r#type: "ENV".to_string(),
                args: key_value_args("ENV", &env.arguments.value)?,
            }),
            Instruction::Arg(arg) => Some(TemplateStep {
                r#type: "ARG".to_string(),
                args: key_value_args("ARG", &arg.arguments.value)?,
            }),
            Instruction::Entrypoint(instruction) => {
                entrypoint = dockerfile_command(&instruction.arguments);
                None
            }
            Instruction::Cmd(instruction) => {
                cmd = dockerfile_command(&instruction.arguments);
                None
            }
            Instruction::Shell(_) | Instruction::Stopsignal(_) => None,
            Instruction::Expose(_) => {
                warn_ignored_instruction("EXPOSE");
                None
            }
            Instruction::Volume(_) => {
                warn_ignored_instruction("VOLUME");
                None
            }
            Instruction::Label(_) => {
                warn_ignored_instruction("LABEL");
                None
            }
            Instruction::Maintainer(_) => {
                warn_ignored_instruction("MAINTAINER");
                None
            }
            Instruction::Copy(_) => {
                bail!("COPY instructions are not supported by AENV template builds yet")
            }
            Instruction::Add(_) => {
                bail!("ADD instructions are not supported by AENV template builds yet")
            }
            Instruction::Healthcheck(_) => {
                bail!("Dockerfile instruction HEALTHCHECK is not supported")
            }
            Instruction::Onbuild(_) => {
                bail!("Dockerfile instruction ONBUILD is not supported")
            }
            _ => bail!("Dockerfile instruction is not supported"),
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

fn dockerfile_run_command(run: &RunInstruction<'_>, escape: char) -> Result<String> {
    match (&run.arguments, run.here_docs.as_slice()) {
        (Command::Exec(_), []) => dockerfile_command(&run.arguments)
            .context("RUN instruction requires a non-empty command"),
        (Command::Shell(command), []) => Ok(normalize_run_continuations(command.value, escape)),
        (Command::Shell(command), [here_doc]) if command.value.trim().is_empty() => {
            Ok(here_doc.value.to_string())
        }
        (Command::Shell(command), [here_doc]) => {
            Ok(render_run_heredoc(command.value.trim(), here_doc))
        }
        (_, here_docs) if here_docs.len() > 1 => {
            bail!("multiple RUN heredocs are not supported")
        }
        _ => bail!("RUN heredocs are only supported for shell-form commands"),
    }
}

fn render_run_heredoc(command: &str, here_doc: &HereDoc<'_>) -> String {
    let delimiter = unique_heredoc_delimiter(&here_doc.value);
    let opening_delimiter = if here_doc.expand {
        delimiter.clone()
    } else {
        format!("'{delimiter}'")
    };
    let body_newline = if here_doc.value.is_empty() || here_doc.value.ends_with('\n') {
        ""
    } else {
        "\n"
    };

    format!(
        "<<{opening_delimiter} {command}\n{}{body_newline}{delimiter}",
        here_doc.value
    )
}

fn unique_heredoc_delimiter(body: &str) -> String {
    const BASE: &str = "AENV_HEREDOC";

    (0..)
        .map(|suffix| {
            if suffix == 0 {
                BASE.to_string()
            } else {
                format!("{BASE}_{suffix}")
            }
        })
        .find(|delimiter| body.lines().all(|line| line != delimiter))
        .expect("the unbounded delimiter sequence must contain an unused value")
}

fn normalize_run_continuations(command: &str, escape: char) -> String {
    if escape == '\\' {
        return command.to_string();
    }

    command
        .replace(&format!("{escape}\r\n"), "\\\r\n")
        .replace(&format!("{escape}\n"), "\\\n")
}

fn dockerfile_command(command: &Command<'_>) -> Option<String> {
    match command {
        Command::Exec(parts) => (!parts.value.is_empty()).then(|| {
            parts
                .value
                .iter()
                .map(|part| shell_quote(&part.value))
                .collect::<Vec<_>>()
                .join(" ")
        }),
        Command::Shell(command) => {
            let command = command.value.trim();
            (!command.is_empty()).then(|| command.to_string())
        }
        _ => None,
    }
}

fn warn_ignored_instruction(instruction: &str) {
    eprintln!("warning: {instruction} instruction is not supported and will be ignored");
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
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        args: super::Args,
    }

    #[test]
    fn build_requires_name_flag() {
        let cli = TestCli::try_parse_from(["test", "Dockerfile", "--name", "my-template"])
            .expect("--name should be accepted");
        assert_eq!(cli.args.name, "my-template");

        assert!(TestCli::try_parse_from(["test", "Dockerfile"]).is_err());
        assert!(TestCli::try_parse_from(["test", "Dockerfile", "--tag", "old"]).is_err());
    }

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
    fn dockerfile_build_plan_parses_multiline_run() {
        let plan = dockerfile_build_plan(
            r#"FROM ubuntu:24.04
RUN apt-get update && \
    apt-get install -y curl
WORKDIR /app
"#,
        )
        .unwrap();

        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.steps[0].r#type, "RUN");
        assert_eq!(
            plan.steps[0].args,
            ["apt-get update && \\\n    apt-get install -y curl"]
        );
        assert_eq!(plan.steps[1].r#type, "WORKDIR");
    }

    #[test]
    fn dockerfile_build_plan_honors_escape_directive_for_multiline_run() {
        let plan = dockerfile_build_plan(
            r#"# escape=`
FROM ubuntu:24.04
RUN echo first && `
    echo second
"#,
        )
        .unwrap();

        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].r#type, "RUN");
        assert_eq!(
            plan.steps[0].args[0],
            r#"echo first && \
    echo second"#
        );
    }

    #[test]
    fn dockerfile_build_plan_parses_bare_run_heredoc_as_script() {
        let plan = dockerfile_build_plan(
            r#"FROM ubuntu:24.04
RUN <<EOF
set -eu
echo hello
EOF
"#,
        )
        .unwrap();

        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].r#type, "RUN");
        assert_eq!(plan.steps[0].args, ["set -eu\necho hello\n"]);
    }

    #[test]
    fn dockerfile_build_plan_preserves_run_heredoc_command() {
        let plan = dockerfile_build_plan(
            r#"FROM ubuntu:24.04
RUN <<EOF bash
set -eu
echo hello
EOF
"#,
        )
        .unwrap();

        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].r#type, "RUN");
        assert_eq!(
            plan.steps[0].args,
            ["<<AENV_HEREDOC bash\nset -eu\necho hello\nAENV_HEREDOC"]
        );
    }

    #[test]
    fn dockerfile_build_plan_renders_safe_quoted_heredoc_delimiter() {
        let plan = dockerfile_build_plan(
            r#"FROM ubuntu:24.04
RUN <<'EOF' cat
AENV_HEREDOC
EOF
"#,
        )
        .unwrap();

        assert_eq!(plan.steps.len(), 1);
        assert_eq!(
            plan.steps[0].args,
            ["<<'AENV_HEREDOC_1' cat\nAENV_HEREDOC\nAENV_HEREDOC_1"]
        );
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
    fn dockerfile_build_plan_skips_maintainer() {
        let plan = dockerfile_build_plan(
            r#"
FROM ubuntu:24.04
MAINTAINER AgentENV
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
