/// Smoke test: resolve an OCI image via regctl, then read a small range from
/// its first blob layer through RegistryFsV2.
/// Useful for testing authentication and range requests.
///
/// Usage:
///   cargo run -p overlaybd --example test_registry_read -- <image>
///
use anyhow::{bail, Context, Result};
use overlaybd::backend::registryfs_v2::{RegistryFsV2, RegistryFsV2Options};
use overlaybd::config::CredentialConfig;
use overlaybd::virtual_file::VirtualFile;

fn docker_config_path() -> Result<String> {
    if let Ok(d) = std::env::var("DOCKER_CONFIG") {
        return Ok(format!("{d}/config.json"));
    }
    let home = std::env::var("HOME").context("HOME not set")?;
    Ok(format!("{home}/.docker/config.json"))
}

/// Parse "host/repo:tag" or "host/repo@digest" into (host, repo, ref).
fn parse_image_ref(image: &str) -> Result<(String, String, String)> {
    let image = image.strip_prefix("docker://").unwrap_or(image);
    let (host, rest) = image
        .split_once('/')
        .context("expected host/repo[:tag|@digest]")?;
    let (repo, reference) = if let Some((r, d)) = rest.split_once('@') {
        (r, d)
    } else if let Some((r, t)) = rest.rsplit_once(':') {
        (r, t)
    } else {
        (rest, "latest")
    };
    Ok((host.to_string(), repo.to_string(), reference.to_string()))
}

/// Use `regctl manifest get` to fetch the raw manifest, resolve through any
/// index, and return `(host, repo, vec![(digest, size)])`.
fn resolve_layers(image: &str) -> Result<Vec<(String, u64)>> {
    let image_ref = image.strip_prefix("docker://").unwrap_or(image);
    let raw = run_regctl(&["manifest", "get", "--format", "raw-body", image_ref])?;
    let manifest: serde_json::Value = serde_json::from_str(&raw)?;

    // If it's an index, pick the amd64 image manifest and re-inspect by digest.
    if let Some(manifests) = manifest.get("manifests").and_then(|v| v.as_array()) {
        let desc = manifests
            .iter()
            .find(|m| {
                m.get("platform")
                    .and_then(|p| p.get("architecture"))
                    .and_then(|a| a.as_str())
                    == Some("amd64")
            })
            .or_else(|| manifests.first())
            .context("empty manifest index")?;
        let digest = desc["digest"].as_str().context("missing digest")?;
        println!("  Index → amd64 manifest: {digest}");

        let (host, repo, _) = parse_image_ref(image)?;
        let by_digest = format!("{host}/{repo}@{digest}");
        return resolve_layers(&by_digest);
    }

    // Image manifest – extract layers.
    let layers = manifest
        .get("layers")
        .and_then(|v| v.as_array())
        .context("manifest has no layers")?;
    let result: Vec<(String, u64)> = layers
        .iter()
        .filter_map(|l| {
            let digest = l.get("digest")?.as_str()?.to_string();
            let size = l.get("size")?.as_u64().unwrap_or(0);
            Some((digest, size))
        })
        .collect();
    if result.is_empty() {
        bail!("no layers found");
    }
    Ok(result)
}

fn run_regctl(args: &[&str]) -> Result<String> {
    let out = std::process::Command::new("regctl")
        .args(args)
        .output()
        .context("failed to run regctl")?;
    if !out.status.success() {
        bail!(
            "regctl {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8(out.stdout)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let image = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: test_registry_read <image>");
        std::process::exit(1);
    });

    let (host, repo, reference) = parse_image_ref(&image)?;
    println!("Registry  : {host}");
    println!("Repository: {repo}");
    println!("Reference : {reference}");

    // 1. Resolve layers via regctl
    println!("\n[1] Resolving layers via regctl…");
    let layers = resolve_layers(&image)?;
    println!("    {} layer(s):", layers.len());
    for (i, (digest, size)) in layers.iter().enumerate() {
        println!("      [{i}] {digest}  ({size} bytes)");
    }

    // 2. Read from each layer via RegistryFsV2
    let docker_cfg = docker_config_path()?;
    println!("\n[2] Reading first 64 bytes of each layer via RegistryFsV2 (creds: {docker_cfg})…");

    let fs = RegistryFsV2::with_options(RegistryFsV2Options {
        credential: CredentialConfig {
            mode: "file".to_string(),
            path: docker_cfg,
            ..Default::default()
        },
        ..Default::default()
    })?;

    let blob_base = format!("https://{host}/v2/{repo}/blobs");
    for (i, (digest, size)) in layers.iter().enumerate() {
        let url = format!("{blob_base}/{digest}");
        let file = fs.open_file_with_size_hint(&url, Some(*size));
        let reported_size = file.size().await?;
        let n = std::cmp::min(64, reported_size as usize);
        let data = file.read_at(0, n).await?;
        print!("    [{i}] ");
        for b in &data {
            print!("{b:02x}");
        }
        println!("  ({reported_size} bytes)");
    }

    println!("\nSuccess!");
    Ok(())
}
