use anyhow::{bail, Context, Result};
use clap::Parser;
use overlaybd::config::{ImageConfig, LayerConfig};
use reqwest::header::{ACCEPT, AUTHORIZATION, WWW_AUTHENTICATE};
use reqwest::StatusCode;
use serde::Deserialize;
use std::path::PathBuf;

const DOCKER_MANIFEST_V2: &str = "application/vnd.docker.distribution.manifest.v2+json";
const DOCKER_MANIFEST_LIST_V2: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
const OCI_MANIFEST_V1: &str = "application/vnd.oci.image.manifest.v1+json";
const OCI_INDEX_V1: &str = "application/vnd.oci.image.index.v1+json";

#[derive(Debug, Parser)]
#[command(about = "Generate overlaybd image config from a container image reference")]
struct Args {
    /// Image reference, e.g. docker.io/overlaybd/redis:latest
    #[arg(long)]
    image: String,

    /// Output path for the generated image config JSON
    #[arg(long)]
    output: PathBuf,

    /// Use HTTP instead of HTTPS
    #[arg(long, default_value_t = false)]
    insecure: bool,

    /// Target platform (os/arch)
    #[arg(long, default_value = "linux/amd64")]
    platform: String,
}

/// Parsed image reference.
struct ImageRef {
    host: String,
    repo: String,
    reference: String,
}

fn parse_image_ref(raw: &str) -> ImageRef {
    // Handle digest references like redis@sha256:abc123...
    let (rest, reference) = if let Some((name, digest)) = raw.split_once('@') {
        (name, digest.to_string())
    } else if let Some((r, tag)) = raw.rsplit_once(':') {
        // Avoid splitting on port numbers like registry.io:5000/repo
        if tag.contains('/') {
            (raw, "latest".to_string())
        } else {
            (r, tag.to_string())
        }
    } else {
        (raw, "latest".to_string())
    };

    let (host, repo) = if let Some((h, r)) = rest.split_once('/') {
        // Heuristic: if the first component has a dot or colon, it's a host
        if h.contains('.') || h.contains(':') {
            (h.to_string(), r.to_string())
        } else {
            // No explicit host — default to docker.io
            ("docker.io".to_string(), rest.to_string())
        }
    } else {
        // Single name like "redis" → docker.io/library/redis
        ("docker.io".to_string(), format!("library/{rest}"))
    };

    // Normalize docker.io short names: "redis" → "library/redis"
    let repo = if host == "docker.io" && !repo.contains('/') {
        format!("library/{repo}")
    } else {
        repo
    };

    // Map docker.io to the actual registry host
    let host = if host == "docker.io" {
        "registry-1.docker.io".to_string()
    } else {
        host
    };

    ImageRef {
        host,
        repo,
        reference,
    }
}

/// Parse WWW-Authenticate bearer challenge.
/// Example: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`
fn parse_bearer_challenge(header: &str) -> Option<(String, String)> {
    let bearer = header.strip_prefix("Bearer ")?;
    let mut realm = None;
    let mut service = None;
    for part in bearer.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("realm=") {
            realm = Some(v.trim_matches('"').to_string());
        } else if let Some(v) = part.strip_prefix("service=") {
            service = Some(v.trim_matches('"').to_string());
        }
    }
    Some((realm?, service.unwrap_or_default()))
}

#[derive(Deserialize)]
struct TokenResponse {
    token: Option<String>,
    access_token: Option<String>,
}

impl TokenResponse {
    fn get_token(&self) -> Option<&str> {
        self.token.as_deref().or(self.access_token.as_deref())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestLayer {
    digest: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    #[serde(default)]
    #[allow(dead_code)]
    media_type: String,
    #[serde(default)]
    layers: Vec<ManifestLayer>,
    #[serde(default)]
    manifests: Vec<ManifestListEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestListEntry {
    digest: String,
    #[serde(default)]
    #[allow(dead_code)]
    media_type: String,
    #[serde(default)]
    platform: Option<Platform>,
}

#[derive(Deserialize)]
struct Platform {
    #[serde(default)]
    architecture: String,
    #[serde(default)]
    os: String,
}

async fn fetch_anonymous_token(
    client: &reqwest::Client,
    realm: &str,
    service: &str,
    repo: &str,
) -> Result<String> {
    let mut url = reqwest::Url::parse(realm).context("invalid auth realm URL")?;
    url.query_pairs_mut()
        .append_pair("service", service)
        .append_pair("scope", &format!("repository:{repo}:pull"));

    let resp = client
        .get(url)
        .send()
        .await
        .context("token request failed")?;

    if !resp.status().is_success() {
        bail!("token endpoint returned {}", resp.status());
    }

    let body: TokenResponse = resp.json().await.context("parse token response")?;
    body.get_token()
        .map(|s| s.to_string())
        .context("no token in response")
}

async fn get_bearer_token(client: &reqwest::Client, base_url: &str, repo: &str) -> Result<String> {
    let resp = client
        .get(format!("{base_url}/v2/"))
        .send()
        .await
        .context("registry ping failed")?;

    if resp.status() == StatusCode::UNAUTHORIZED {
        let www_auth = resp
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .context("missing WWW-Authenticate header")?;

        let (realm, service) =
            parse_bearer_challenge(www_auth).context("failed to parse bearer challenge")?;

        fetch_anonymous_token(client, &realm, &service, repo).await
    } else if resp.status().is_success() {
        // No auth required
        Ok(String::new())
    } else {
        bail!("registry ping returned {}", resp.status());
    }
}

async fn fetch_manifest(
    client: &reqwest::Client,
    base_url: &str,
    repo: &str,
    reference: &str,
    token: &str,
) -> Result<(Manifest, String)> {
    let url = format!("{base_url}/v2/{repo}/manifests/{reference}");
    let accept = [
        DOCKER_MANIFEST_V2,
        OCI_MANIFEST_V1,
        DOCKER_MANIFEST_LIST_V2,
        OCI_INDEX_V1,
    ]
    .join(", ");

    let mut req = client.get(&url).header(ACCEPT, &accept);
    if !token.is_empty() {
        req = req.header(AUTHORIZATION, format!("Bearer {token}"));
    }

    let resp = req.send().await.context("manifest request failed")?;
    if !resp.status().is_success() {
        bail!("manifest request returned {}", resp.status());
    }

    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let manifest: Manifest = resp.json().await.context("parse manifest")?;
    Ok((manifest, content_type))
}

fn is_manifest_list(content_type: &str, manifest: &Manifest) -> bool {
    content_type.contains("manifest.list")
        || content_type.contains("image.index")
        || !manifest.manifests.is_empty()
}

fn select_platform<'a>(
    manifests: &'a [ManifestListEntry],
    platform: &str,
) -> Result<&'a ManifestListEntry> {
    let (target_os, target_arch) = platform.split_once('/').unwrap_or(("linux", "amd64"));

    manifests
        .iter()
        .find(|m| {
            m.platform
                .as_ref()
                .map(|p| p.os == target_os && p.architecture == target_arch)
                .unwrap_or(false)
        })
        .context(format!(
            "no manifest found for platform {target_os}/{target_arch}"
        ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let image_ref = parse_image_ref(&args.image);
    let scheme = if args.insecure {
        eprintln!("WARNING: using insecure HTTP connection");
        "http"
    } else {
        "https"
    };
    let base_url = format!("{scheme}://{}", image_ref.host);

    println!(
        "Resolving {}:{} from {}",
        image_ref.repo, image_ref.reference, image_ref.host
    );

    let client = reqwest::Client::builder()
        .user_agent("agentenv-overlaybd/0.1")
        .build()
        .context("build HTTP client")?;

    // Get anonymous bearer token
    let token = get_bearer_token(&client, &base_url, &image_ref.repo).await?;

    // Fetch manifest
    let (manifest, content_type) = fetch_manifest(
        &client,
        &base_url,
        &image_ref.repo,
        &image_ref.reference,
        &token,
    )
    .await?;

    // Resolve manifest list to a single manifest if needed
    let layers = if is_manifest_list(&content_type, &manifest) {
        let entry = select_platform(&manifest.manifests, &args.platform)?;
        println!("Selected platform manifest: {}", entry.digest);
        let (resolved, _) =
            fetch_manifest(&client, &base_url, &image_ref.repo, &entry.digest, &token).await?;
        resolved.layers
    } else {
        manifest.layers
    };

    if layers.is_empty() {
        bail!("manifest has no layers");
    }

    println!("Found {} layers", layers.len());

    // Build ImageConfig
    let repo_blob_url = format!("{base_url}/v2/{}/blobs", image_ref.repo);
    let lowers: Vec<LayerConfig> = layers
        .iter()
        .map(|l| LayerConfig {
            digest: l.digest.clone(),
            size: l.size,
            ..Default::default()
        })
        .collect();

    let config = ImageConfig {
        repo_blob_url,
        lowers,
        ..Default::default()
    };

    // Validate
    overlaybd::config::validate_image_config(&config)
        .context("generated config failed validation")?;

    // Write output
    let json = serde_json::to_string_pretty(&config).context("serialize config")?;
    if let Some(parent) = args.output.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create directory {}", parent.display()))?;
    }
    tokio::fs::write(&args.output, &json)
        .await
        .with_context(|| format!("write {}", args.output.display()))?;

    println!("Written to {}", args.output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_image_ref_full() {
        let r = parse_image_ref("docker.io/overlaybd/redis:latest").unwrap();
        assert_eq!(r.host, "registry-1.docker.io");
        assert_eq!(r.repo, "overlaybd/redis");
        assert_eq!(r.reference, "latest");
    }

    #[test]
    fn test_parse_image_ref_short() {
        let r = parse_image_ref("redis").unwrap();
        assert_eq!(r.host, "registry-1.docker.io");
        assert_eq!(r.repo, "library/redis");
        assert_eq!(r.reference, "latest");
    }

    #[test]
    fn test_parse_image_ref_custom_registry() {
        let r = parse_image_ref("ghcr.io/owner/repo:v1.0").unwrap();
        assert_eq!(r.host, "ghcr.io");
        assert_eq!(r.repo, "owner/repo");
        assert_eq!(r.reference, "v1.0");
    }

    #[test]
    fn test_parse_image_ref_with_port() {
        let r = parse_image_ref("localhost:5000/myrepo:dev").unwrap();
        assert_eq!(r.host, "localhost:5000");
        assert_eq!(r.repo, "myrepo");
        assert_eq!(r.reference, "dev");
    }

    #[test]
    fn test_parse_image_ref_with_tag() {
        let r = parse_image_ref("redis:7").unwrap();
        assert_eq!(r.host, "registry-1.docker.io");
        assert_eq!(r.repo, "library/redis");
        assert_eq!(r.reference, "7");
    }

    #[test]
    fn test_parse_image_ref_no_tag() {
        let r = parse_image_ref("docker.io/overlaybd/redis").unwrap();
        assert_eq!(r.host, "registry-1.docker.io");
        assert_eq!(r.repo, "overlaybd/redis");
        assert_eq!(r.reference, "latest");
    }

    #[test]
    fn test_parse_image_ref_digest() {
        let r = parse_image_ref("redis@sha256:abcdef1234567890").unwrap();
        assert_eq!(r.host, "registry-1.docker.io");
        assert_eq!(r.repo, "library/redis");
        assert_eq!(r.reference, "sha256:abcdef1234567890");
    }

    #[test]
    fn test_parse_image_ref_digest_with_host() {
        let r = parse_image_ref("ghcr.io/owner/repo@sha256:abcdef1234567890").unwrap();
        assert_eq!(r.host, "ghcr.io");
        assert_eq!(r.repo, "owner/repo");
        assert_eq!(r.reference, "sha256:abcdef1234567890");
    }

    #[test]
    fn test_parse_bearer_challenge() {
        let header = r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io""#;
        let (realm, service) = parse_bearer_challenge(header).unwrap();
        assert_eq!(realm, "https://auth.docker.io/token");
        assert_eq!(service, "registry.docker.io");
    }
}
