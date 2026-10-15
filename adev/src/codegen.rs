use anyhow::Result;
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
    fix_registry_auth_discriminators(&server_dir.join("src/models.rs"))?;

    util::cmd("cargo", &["fmt", "-p", "agentenv_http_server"])?;
    util::info("AENV server generated.");
    Ok(())
}

/// rust-axum 7.22 emits Rust model names as discriminator values when
/// serializing these oneOf children, even with an explicit OpenAPI mapping.
/// Keep this correction in codegen so regeneration preserves the wire contract.
fn fix_registry_auth_discriminators(path: &std::path::Path) -> Result<()> {
    let mut content = std::fs::read_to_string(path)?;
    for (model, wire_value) in [
        ("AwsRegistry", "aws"),
        ("GcpRegistry", "gcp"),
        ("GeneralRegistry", "registry"),
    ] {
        let expected = format!(
            "impl {model} {{\n    fn _name_for_r_type() -> String {{\n        String::from(\"{model}\")\n    }}"
        );
        let count = content.matches(&expected).count();
        anyhow::ensure!(
            count == 1,
            "expected exactly one {model} discriminator method, found {count}; check OpenAPI generator output"
        );
        let replacement = format!(
            "impl {model} {{\n    fn _name_for_r_type() -> String {{\n        String::from(\"{wire_value}\")\n    }}"
        );
        content = content.replacen(&expected, &replacement, 1);
    }
    std::fs::write(path, content)?;
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
