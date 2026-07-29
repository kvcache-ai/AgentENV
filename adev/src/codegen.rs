use std::fs;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};

use crate::config;
use crate::util;

#[derive(Args)]
pub struct CodegenArgs {
    #[command(subcommand)]
    pub target: Option<CodegenTarget>,

    /// Only ensure dependencies are installed, don't run codegen
    #[arg(long)]
    pub ensure_deps_only: bool,
}

#[derive(Subcommand)]
pub enum CodegenTarget {
    /// Regenerate Firecracker API client
    Firecracker,
    /// Regenerate envd HTTP client
    Envd,
    /// Regenerate AENV HTTP server stubs
    Server,
    /// Regenerate custom extension HTTP client
    CustomExtension,
}

/// npm wrapper version for @openapitools/openapi-generator-cli.
/// The actual generator jar version is controlled by openapitools.json in the project root.
const OPENAPI_GENERATOR_CLI_VERSION: &str = "2.32.0";
const OPENAPI_TEMPLATE_NAME: &str = "openapi";

pub fn run(args: CodegenArgs) -> Result<()> {
    let project_root = config::project_root()?;

    if args.ensure_deps_only {
        let cfg = config::load_config_from_root(&project_root)?;
        // Also ensure protoc
        crate::ensure_tool::ensure_protoc(&cfg.protoc.version, &cfg.protoc.url)?;
        util::info("All codegen dependencies are ready.");
        return Ok(());
    }

    match args.target {
        Some(CodegenTarget::Firecracker) => run_firecracker(&project_root),
        Some(CodegenTarget::Envd) => run_envd(&project_root),
        Some(CodegenTarget::Server) => run_server(&project_root),
        Some(CodegenTarget::CustomExtension) => run_custom_extension(&project_root),
        None => {
            run_firecracker(&project_root)?;
            run_envd(&project_root)?;
            run_server(&project_root)?;
            run_custom_extension(&project_root)?;
            Ok(())
        }
    }
}

/// Regenerate the custom extension HTTP client into
/// `src/custom_extension_api/generated`.
fn run_custom_extension(project_root: &std::path::Path) -> Result<()> {
    let ext_dir = project_root.join("src/custom_extension_api/generated");
    let spec = project_root.join("src/custom_extension_api/openapi.yml");

    util::info("Regenerating custom extension HTTP client...");
    run_openapi_generator(
        project_root,
        &[
            "generate",
            "-i",
            &spec.to_string_lossy(),
            "-g",
            "rust",
            "-o",
            &ext_dir.to_string_lossy(),
            "--additional-properties=packageName=custom_extension_client,hideGenerationTimestamp=true",
            "--skip-validate-spec",
        ],
    )?;

    prepend_allow_attrs(&ext_dir.join("src/lib.rs"))?;
    util::cmd("cargo", &["fmt", "-p", "custom_extension_client"])?;
    util::info("custom extension client generated.");
    Ok(())
}

fn render_server_spec_source(spec_dir: &std::path::Path) -> Result<String> {
    let template_path = spec_dir.join("openapi.tmpl");
    let template_source = fs::read_to_string(&template_path)
        .with_context(|| format!("read OpenAPI template {}", template_path.display()))?;
    let security = read_section_file(
        &spec_dir.join("components/security.yml"),
        "securitySchemes",
        2,
    )?;
    let parameters =
        read_section_file(&spec_dir.join("components/parameters.yml"), "parameters", 2)?;
    let responses = read_section_file(&spec_dir.join("components/responses.yml"), "responses", 2)?;
    let tags = template_tag_order(&template_source)?;
    let schemas = read_schema_sections(&spec_dir.join("components/schemas"), &tags)?;
    let paths = read_tagged_sections(&spec_dir.join("paths"), &tags, "paths", 2, &[])?;

    let mut engine = upon::Engine::new();
    engine
        .add_template(OPENAPI_TEMPLATE_NAME, template_source)
        .with_context(|| format!("compile OpenAPI template {}", template_path.display()))?;
    engine
        .template(OPENAPI_TEMPLATE_NAME)
        .render(upon::value! {
            security: security,
            parameters: parameters,
            responses: responses,
            schemas: schemas,
            paths: paths,
        })
        .to_string()
        .with_context(|| format!("render OpenAPI template {}", template_path.display()))
}

fn template_tag_order(template: &str) -> Result<Vec<String>> {
    let mut lines = template.lines();
    lines
        .find(|line| *line == "tags:")
        .ok_or_else(|| anyhow::anyhow!("OpenAPI template is missing a top-level `tags` section"))?;
    let tags = lines
        .take_while(|line| line.is_empty() || line.starts_with(' '))
        .filter_map(|line| line.strip_prefix("  - name: "))
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    if tags.is_empty() {
        anyhow::bail!("OpenAPI template does not define any tags");
    }
    if tags
        .iter()
        .enumerate()
        .any(|(index, tag)| tags[..index].contains(tag))
    {
        anyhow::bail!("OpenAPI template contains duplicate tags");
    }
    Ok(tags)
}

fn read_schema_sections(dir: &std::path::Path, tags: &[String]) -> Result<String> {
    let common = read_section_file(&dir.join("common.yml"), "schemas", 2)?;
    let tagged = read_tagged_sections(dir, tags, "schemas", 2, &["common"])?;
    if tagged.is_empty() {
        Ok(common)
    } else {
        Ok(format!("{common}\n\n{tagged}"))
    }
}

fn read_tagged_sections(
    dir: &std::path::Path,
    tags: &[String],
    expected_header: &str,
    indent: usize,
    ignored_stems: &[&str],
) -> Result<String> {
    let mut paths = yaml_files(dir)?;
    paths.sort();
    paths.retain(|path| {
        !ignored_stems.iter().any(|ignored| {
            path.file_stem()
                .is_some_and(|file_stem| file_stem == *ignored)
        })
    });

    let mut sections = Vec::with_capacity(paths.len());
    for tag in tags {
        let Some(index) = paths.iter().position(|path| {
            path.file_stem()
                .is_some_and(|file_stem| file_stem == tag.as_str())
        }) else {
            anyhow::bail!(
                "missing {expected_header} section for tag `{tag}` in {}",
                dir.display(),
            );
        };
        sections.push(read_section_file(
            &paths.remove(index),
            expected_header,
            indent,
        )?);
    }

    if let Some(path) = paths.first() {
        anyhow::bail!(
            "{expected_header} section `{}` has no matching tag in the OpenAPI template",
            path.display(),
        );
    }
    Ok(sections.join("\n\n"))
}

fn yaml_files(dir: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    let entries = fs::read_dir(dir)
        .with_context(|| format!("read OpenAPI section directory {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("read entries from {}", dir.display()))?;

    Ok(entries
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yml" || extension == "yaml")
        })
        .collect())
}

fn read_section_file(
    path: &std::path::Path,
    expected_header: &str,
    indent: usize,
) -> Result<String> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("read OpenAPI section {}", path.display()))?;
    render_section_body(&content, expected_header, indent)
        .with_context(|| format!("render OpenAPI section {}", path.display()))
}

fn render_section_body(content: &str, expected_header: &str, indent: usize) -> Result<String> {
    let (header, body) = content
        .split_once('\n')
        .ok_or_else(|| anyhow::anyhow!("section must contain a header and body"))?;
    let expected = format!("{expected_header}:");
    if header.trim_end() != expected {
        anyhow::bail!("expected first line `{expected}`, found `{header}`");
    }
    if body.trim().is_empty() {
        anyhow::bail!("section `{expected_header}` has an empty body");
    }

    let prefix = " ".repeat(indent);
    Ok(body
        .trim_end()
        .lines()
        .map(|line| {
            if line.is_empty() {
                String::new()
            } else {
                format!("{prefix}{line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

/// Run openapi-generator-cli via npx.
/// The generator jar version is read from openapitools.json in the project root.
fn run_openapi_generator(project_root: &std::path::Path, args: &[&str]) -> Result<()> {
    let package = format!(
        "@openapitools/openapi-generator-cli@{}",
        OPENAPI_GENERATOR_CLI_VERSION
    );
    let mut cmd_args: Vec<&str> = vec!["--yes", &package, "--"];
    cmd_args.extend_from_slice(args);
    util::cmd_in_dir("npx", &cmd_args, project_root)
}

fn run_firecracker(project_root: &std::path::Path) -> Result<()> {
    let fc_dir = project_root.join("thirdparty/firecracker-client");
    let spec = fc_dir.join("firecracker.yaml");

    util::info("Regenerating Firecracker API client...");
    run_openapi_generator(
        project_root,
        &[
            "generate",
            "-i",
            &spec.to_string_lossy(),
            "-g",
            "rust",
            "-o",
            &fc_dir.to_string_lossy(),
            "--global-property",
            "models,supportingFiles",
            "--additional-properties=packageName=firecracker_client,hideGenerationTimestamp=true",
        ],
    )?;

    prepend_allow_attrs(&fc_dir.join("src/lib.rs"))?;
    util::cmd("cargo", &["fmt", "-p", "firecracker_client"])?;
    util::info("Firecracker client generated.");
    Ok(())
}

fn run_envd(project_root: &std::path::Path) -> Result<()> {
    let envd_dir = project_root.join("thirdparty/envd/http-client");
    let spec = envd_dir.join("envd.yaml");

    util::info("Regenerating envd HTTP client...");
    run_openapi_generator(
        project_root,
        &[
            "generate",
            "-i",
            &spec.to_string_lossy(),
            "-g",
            "rust",
            "-o",
            &envd_dir.to_string_lossy(),
            "--additional-properties=packageName=http_client,hideGenerationTimestamp=true",
            "--skip-validate-spec",
        ],
    )?;

    prepend_allow_attrs(&envd_dir.join("src/lib.rs"))?;
    util::cmd("cargo", &["fmt", "-p", "http_client"])?;
    util::info("envd HTTP client generated.");
    Ok(())
}

fn run_server(project_root: &std::path::Path) -> Result<()> {
    let server_dir = project_root.join("src/api/generated");
    let spec = project_root.join("src/api/openapi.yml");
    let spec_dir = project_root.join("src/api/spec");

    let rendered = render_server_spec_source(&spec_dir)?;
    fs::write(&spec, rendered)
        .with_context(|| format!("write rendered OpenAPI spec {}", spec.display()))?;
    util::info(&format!(
        "AENV OpenAPI spec rendered to {}.",
        spec.display()
    ));
    util::info("Regenerating AENV HTTP server...");
    run_openapi_generator(
        project_root,
        &[
            "generate",
            "-g",
            "rust-axum",
            "-i",
            &spec.to_string_lossy(),
            "-o",
            &server_dir.to_string_lossy(),
            "--additional-properties=packageName=agentenv_http_server,hideGenerationTimestamp=true",
        ],
    )?;

    // Port of fix_rust_axum_duplicate_auth_trait.py
    let mod_rs = server_dir.join("src/apis/mod.rs");
    fix_duplicate_auth_trait(&mod_rs)?;

    util::cmd("cargo", &["fmt", "-p", "agentenv_http_server"])?;
    util::info("AENV server generated.");
    Ok(())
}

/// Prepend #![allow(clippy::all)] and #![allow(warnings)] to a file if not already present.
fn prepend_allow_attrs(path: &std::path::Path) -> Result<()> {
    let content = std::fs::read_to_string(path)?;
    if content.starts_with("#![allow(clippy::all)]") {
        return Ok(());
    }
    let new_content = format!("#![allow(clippy::all)]\n#![allow(warnings)]\n{content}");
    std::fs::write(path, new_content)?;
    Ok(())
}

/// Port of scripts/fix_rust_axum_duplicate_auth_trait.py
/// Removes duplicate ApiKeyAuthHeader trait blocks from generated code.
fn fix_duplicate_auth_trait(path: &std::path::Path) -> Result<()> {
    use std::sync::LazyLock;

    if !path.exists() {
        anyhow::bail!("file not found: {}", path.display());
    }

    let content = std::fs::read_to_string(path)?;

    static PATTERN: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(concat!(
            r"(?s)/// API Key Authentication - Header\.\r?\n",
            r"\s*#\[async_trait::async_trait\]\r?\n",
            r"\s*pub trait ApiKeyAuthHeader \{\r?\n",
            r"\s+type Claims;\r?\n\r?\n",
            r"\s*/// Extracting Claims from Header\. Return None if the Claims are invalid\.\r?\n",
            r"\s+async fn extract_claims_from_header\(&self, headers: &axum::http::header::HeaderMap, key: &str\) -> Option<Self::Claims>;\r?\n",
            r"\s*\}\r?\n\r?\n"
        ))
        .unwrap()
    });

    let matches: Vec<_> = PATTERN.find_iter(&content).collect();

    if matches.is_empty() {
        anyhow::bail!(
            "ApiKeyAuthHeader trait block not found in {}",
            path.display()
        );
    }

    if matches.len() == 1 {
        util::info(&format!("No duplicate trait blocks in {}", path.display()));
        return Ok(());
    }

    // Keep the first match, remove subsequent duplicates
    let first_end = matches[0].end();
    let before = &content[..first_end];
    let after = PATTERN.replace_all(&content[first_end..], "");
    let deduped = format!("{before}{after}");

    std::fs::write(path, deduped)?;
    util::info(&format!(
        "Removed {} duplicate ApiKeyAuthHeader trait block(s) in {}",
        matches.len() - 1,
        path.display()
    ));
    Ok(())
}
