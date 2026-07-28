use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, trace, warn};

use super::cache::{local_image_services_from_app_config, CachedImageConfig, SourceImageStore};
use super::oci_image::{self, ResolvedImage};
use super::reference::{image_ref_candidates, registry_host_of};
use super::{ImageBaseContext, ImageError, ImageResolutionMetadata, ImageResult};
use crate::cfg::AppConfig;
use crate::image::oci_image::ImageFormat;
use crate::observability::prometheus::MetricGuard;

const IMAGE_RESOLVE_STAGE_DURATION: &str = "agentenv_image_resolve_stage_duration_seconds";

/// artifactType published by accelerated-container-image (`obdconv`) for
/// overlaybd-native images.
const OVERLAYBD_NATIVE_ARTIFACT_TYPE: &str = "application/vnd.containerd.overlaybd.native.v1+json";

/// artifactType published by Azure Container Registry artifact streaming.
///
/// ACR converts an image to overlaybd and attaches the result as an OCI
/// referrer, but labels it with its own artifactType instead of
/// [`OVERLAYBD_NATIVE_ARTIFACT_TYPE`]. The referrer manifest itself is an
/// ordinary overlaybd-native manifest: each layer carries a tar `mediaType`
/// plus a `containerd.io/snapshot/overlaybd/blob-digest` annotation equal to
/// the layer's own digest, which [`ImageFormat::OverlaybdNative`] already
/// recognises. Only the discovery label differs, so accepting this
/// artifactType is enough to stream ACR images.
const ACR_ARTIFACT_STREAMING_ARTIFACT_TYPE: &str = "application/vnd.azure.artifact.streaming.v1";

/// artifactTypes that may front an overlaybd-native referrer, in preference
/// order. The referrer manifest is always re-validated by
/// [`try_resolve_overlaybd_referrer`] before it is used, so this list only
/// controls discovery.
const OVERLAYBD_REFERRER_ARTIFACT_TYPES: &[&str] = &[
    OVERLAYBD_NATIVE_ARTIFACT_TYPE,
    ACR_ARTIFACT_STREAMING_ARTIFACT_TYPE,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedBlockImage {
    pub image_ref: String,
    pub overlaybd_config_path: PathBuf,
    pub base_context: ImageBaseContext,
    /// Raw source image config JSON, `None` when the image source has no config
    /// (e.g. bare overlaybd config path) or when loaded from a legacy cache entry.
    pub raw_config: Option<serde_json::Value>,
}

#[derive(Debug)]
pub struct ImageResolver {
    store: Arc<dyn SourceImageStore>,
    overlaybd_install_root: PathBuf,
    overlaybd_global_config: PathBuf,
    overlaybd_oci_converter_id: String,
    regctl_binary: PathBuf,
    default_image: String,
    search_registries: Vec<String>,
    allowed_registries: Option<Vec<String>>,
    try_referrers_overlaybd_prefixes: Vec<String>,
    convert_standard_oci: bool,
}

impl ImageResolver {
    pub fn new(config: &AppConfig) -> Self {
        let store = local_image_services_from_app_config(config).source_images;
        Self {
            store,
            overlaybd_install_root: config.deps_path.join("overlaybd"),
            overlaybd_global_config: config.ublk.overlaybd.global_config_path.clone(),
            overlaybd_oci_converter_id: config.resolved_overlaybd_oci_converter_id(),
            regctl_binary: config.resolved_regctl_binary(),
            default_image: config.image.resolver.default_image.clone(),
            search_registries: config.image.resolver.search_registries.clone(),
            allowed_registries: config.image.resolver.allowed_registries.clone(),
            try_referrers_overlaybd_prefixes: config
                .image
                .resolver
                .try_referrers_overlaybd_prefixes
                .clone(),
            convert_standard_oci: config.image.resolver.convert_standard_oci,
        }
    }

    pub fn default_image(&self) -> &str {
        &self.default_image
    }

    pub async fn resolve(&self, image_ref: &str) -> ImageResult<ResolvedBlockImage> {
        let candidates = image_ref_candidates(
            image_ref,
            &self.search_registries,
            self.allowed_registries.as_deref(),
        )?;
        let arch = detect_arch()?;
        let mut manifest_failures = Vec::new();
        // Track whether every candidate failed specifically because the
        // registry returned HTTP 404. If so, the overall failure is a user
        // error (the image does not exist) rather than a server error, and we
        // return a typed `NotFound` so the API layer can map it to a 4xx.
        let mut all_not_found = true;

        for candidate in candidates {
            // The manifest fetch is the only "wrong registry, try the next one"
            // stage. Once a manifest is fetched, any later failure is fatal and
            // aborts the whole resolve.
            let mut metric = MetricGuard::stage(IMAGE_RESOLVE_STAGE_DURATION, "manifest_fetch");
            let fetched =
                oci_image::fetch_oci_manifest(&self.regctl_binary, &candidate, &arch).await;
            metric.finish(&fetched);
            let fetched = match fetched {
                Ok(fetched) => fetched,
                Err(ImageError::NotFound { reason }) => {
                    warn!(
                        image = %candidate,
                        error = %reason,
                        "image resolver candidate does not exist; trying next candidate"
                    );
                    manifest_failures.push(format!("{candidate}: {reason}"));
                    continue;
                }
                // The manifest was fetched successfully but describes an image
                // AgentENV cannot run (e.g. overlaybd turbo-OCI). The image
                // exists, so trying other registries is pointless; surface the
                // typed error directly so the API maps it to a 4xx.
                Err(err @ ImageError::UnsupportedImage { .. }) => {
                    return Err(err.context(format!("resolve image '{candidate}'")));
                }
                Err(err) => {
                    warn!(
                        image = %candidate,
                        error = %format_args!("{err:#}"),
                        "image resolver candidate manifest fetch failed; trying next candidate"
                    );
                    all_not_found = false;
                    manifest_failures.push(format!("{candidate}: {err:#}"));
                    continue;
                }
            };
            return self
                .resolve_fetched_manifest(&candidate, &arch, fetched)
                .await;
        }

        let message = format!(
            "resolve image '{}' to overlaybd: all registry candidates failed during manifest fetch: {}",
            image_ref.trim(),
            manifest_failures.join("; ")
        );
        if all_not_found {
            Err(ImageError::NotFound { reason: message })
        } else {
            Err(ImageError::Other(anyhow!(message)))
        }
    }

    /// Resolve a successfully-fetched manifest into a block image. Every failure
    /// here is fatal (the manifest already proved the candidate registry has
    /// the image), so all errors collapse into [`ImageError::Other`].
    async fn resolve_fetched_manifest(
        &self,
        image_ref: &str,
        arch: &str,
        fetched: oci_image::FetchedManifest,
    ) -> ImageResult<ResolvedBlockImage> {
        let source_image_ref = fetched.selected_image_ref.clone();
        // Config metadata comes from the source image even when layer
        // resolution is redirected to an overlaybd referrer below.
        let source_config_digest = fetched.config_digest().to_string();
        let mut overlaybd_image_ref = source_image_ref.clone();
        let fetched = if should_try_overlaybd_referrers(
            &source_image_ref,
            &self.try_referrers_overlaybd_prefixes,
        ) && fetched.format() != ImageFormat::OverlaybdNative
        {
            match try_resolve_overlaybd_referrer(&self.regctl_binary, &source_image_ref, arch).await
            {
                Ok(Some((referrer, artifact_type))) => {
                    overlaybd_image_ref = referrer.selected_image_ref.clone();
                    info!(
                        image = %image_ref,
                        subject = %source_image_ref,
                        overlaybd_referrer = %overlaybd_image_ref,
                        artifact_type,
                        "using overlaybd-native OCI referrer instead of source image"
                    );
                    referrer
                }
                Ok(None) => {
                    trace!(
                        image = %image_ref,
                        subject = %source_image_ref,
                        artifact_types = ?OVERLAYBD_REFERRER_ARTIFACT_TYPES,
                        "no overlaybd-native OCI referrer found; continuing with source image"
                    );
                    fetched
                }
                Err(err) => {
                    warn!(
                        image = %image_ref,
                        subject = %source_image_ref,
                        error = %err,
                        "failed to use overlaybd OCI referrer; continuing with source image"
                    );
                    fetched
                }
            }
        } else {
            fetched
        };
        if fetched.format() == ImageFormat::StandardOci && !self.convert_standard_oci {
            return Err(ImageError::InvalidReference {
                reason: format!(
                    "image '{image_ref}' is standard OCI, but AgentENV standard OCI to overlaybd conversion is disabled by image.resolver.convert_standard_oci=false; publish an overlaybd-native image or enable conversion"
                ),
            });
        }
        let manifest_digest = fetched.manifest_digest.clone();
        let repository_scope = fetched.repository_scope.clone();
        let scope = repository_scope.as_deref();
        let source = self.store.open(&manifest_digest, scope).await?;

        match source.cached_config().await? {
            CachedImageConfig::Found {
                image_config_path,
                metadata: Some(metadata),
            } => {
                metrics::counter!(
                    "agentenv_image_resolve_cache_total",
                    "result" => "hit",
                    "image_format" => fetched.format().to_string(),
                    "registry" => registry_label(&source_image_ref),
                )
                .increment(1);
                return Ok(resolved_from_cached_config(
                    &source_image_ref,
                    image_config_path,
                    *metadata,
                ));
            }
            CachedImageConfig::Found {
                image_config_path,
                metadata: None,
            } => {
                metrics::counter!(
                    "agentenv_image_resolve_cache_total",
                    "result" => "hit",
                    "image_format" => fetched.format().to_string(),
                    "registry" => registry_label(&source_image_ref),
                )
                .increment(1);
                let metadata = oci_image::fetch_oci_image_config_metadata(
                    &self.regctl_binary,
                    &source_image_ref,
                    &source_config_digest,
                )
                .await
                .map_err(|e| e.context(format!("fetch image metadata for '{source_image_ref}'")))?;
                source.write_metadata(metadata.clone()).await?;
                return Ok(resolved_from_cached_config(
                    &source_image_ref,
                    image_config_path,
                    metadata,
                ));
            }
            CachedImageConfig::Missing => {}
        }
        metrics::counter!(
            "agentenv_image_resolve_cache_total",
            "result" => "miss",
            "image_format" => fetched.format().to_string(),
            "registry" => registry_label(&source_image_ref),
        )
        .increment(1);

        let mut conversion = source.begin_conversion().await?;

        let mut metric = MetricGuard::stage(IMAGE_RESOLVE_STAGE_DURATION, "config_fetch");
        let image_config_metadata = oci_image::fetch_oci_image_config_metadata(
            &self.regctl_binary,
            &source_image_ref,
            &source_config_digest,
        )
        .await
        .map_err(|e| e.context(format!("fetch image metadata for '{source_image_ref}'")));
        metric.finish(&image_config_metadata);
        let image_config_metadata = image_config_metadata?;

        let mut metric = MetricGuard::stage(IMAGE_RESOLVE_STAGE_DURATION, "layer_convert");
        let resolved = oci_image::convert_fetched_oci_image_to_overlaybd(
            &overlaybd_image_ref,
            fetched,
            oci_image::OverlaybdConversionEnv {
                install_root: &self.overlaybd_install_root,
                global_config: &self.overlaybd_global_config,
                converter_id: &self.overlaybd_oci_converter_id,
                regctl_binary: &self.regctl_binary,
            },
            &mut *conversion,
            arch,
        )
        .await
        .map_err(|e| e.context(format!("resolve image '{image_ref}' to overlaybd")));
        metric.finish(&resolved);
        let resolved = resolved?;
        let overlaybd_config = overlaybd_image_config_json(&resolved);
        let image_config_path = source
            .publish_config(&overlaybd_config, image_config_metadata.clone(), conversion)
            .await?;

        info!(
            image = %image_ref,
            source_image = %source_image_ref,
            resolved_image = %overlaybd_image_ref,
            manifest_digest = %manifest_digest,
            repository_scope = ?scope,
            config = %image_config_path.display(),
            "image resolved to overlaybd config"
        );

        Ok(resolved_from_cached_config(
            &source_image_ref,
            image_config_path,
            image_config_metadata,
        ))
    }
}

fn resolved_from_cached_config(
    source_image_ref: &str,
    image_config_path: PathBuf,
    metadata: ImageResolutionMetadata,
) -> ResolvedBlockImage {
    ResolvedBlockImage {
        image_ref: source_image_ref.to_string(),
        overlaybd_config_path: image_config_path,
        base_context: metadata.base_context,
        raw_config: metadata.raw_config,
    }
}

fn should_try_overlaybd_referrers(image_ref: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|prefix| image_ref.starts_with(prefix))
}

async fn try_resolve_overlaybd_referrer(
    regctl_binary: &std::path::Path,
    subject_ref: &str,
    arch: &str,
) -> Result<Option<(oci_image::FetchedManifest, &'static str)>> {
    let Some((referrer_digest, artifact_type)) =
        discover_overlaybd_referrer(regctl_binary, subject_ref).await?
    else {
        return Ok(None);
    };
    let referrer_ref = oci_image::image_ref_with_digest(subject_ref, &referrer_digest)
        .with_context(|| format!("build overlaybd referrer reference for {subject_ref}"))?;
    let referrer = oci_image::fetch_oci_manifest(regctl_binary, &referrer_ref, arch)
        .await
        .with_context(|| format!("fetch overlaybd referrer manifest for {referrer_ref}"))?;
    if referrer.format() != ImageFormat::OverlaybdNative {
        bail!(
            "OCI referrer {referrer_ref} advertised artifactType {artifact_type} but its manifest is not overlaybd-native"
        );
    }
    Ok(Some((referrer, artifact_type)))
}

async fn discover_overlaybd_referrer(
    regctl_binary: &std::path::Path,
    subject_ref: &str,
) -> Result<Option<(String, &'static str)>> {
    oci_image::ensure_regctl_binary(regctl_binary)
        .context("regctl is required to query OCI referrers")?;

    // The referrers index is fetched unfiltered and matched locally against
    // OVERLAYBD_REFERRER_ARTIFACT_TYPES: `regctl artifact list` takes a single
    // `--filter-artifact-type`, which cannot express "any of these types" in
    // one call.
    let output = oci_image::regctl_command(regctl_binary)
        .arg("artifact")
        .arg("list")
        .arg("--format")
        .arg("body")
        .arg(subject_ref)
        .output()
        .await
        .context("spawn regctl artifact list")?;

    if !output.status.success() {
        bail!(
            "regctl artifact list failed for {subject_ref}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let body = String::from_utf8(output.stdout).context("regctl output is not UTF-8")?;
    parse_overlaybd_referrer(&body)
        .with_context(|| format!("parse regctl referrers response for {subject_ref}"))
}

fn parse_overlaybd_referrer(body: &str) -> Result<Option<(String, &'static str)>> {
    let index: ReferrersIndex = serde_json::from_str(body).context("parse referrers index JSON")?;
    for &artifact_type in OVERLAYBD_REFERRER_ARTIFACT_TYPES {
        let mut matches = index
            .manifests
            .iter()
            .filter(|descriptor| descriptor.artifact_type.as_deref() == Some(artifact_type));
        let Some(selected) = matches.next() else {
            continue;
        };
        let candidates = matches.count() + 1;
        if candidates > 1 {
            warn!(
                artifact_type,
                selected_digest = %selected.digest,
                candidates,
                "multiple overlaybd-native OCI referrers found; using first"
            );
        }
        return Ok(Some((selected.digest.clone(), artifact_type)));
    }
    Ok(None)
}

#[derive(Debug, Deserialize)]
struct ReferrersIndex {
    #[serde(default)]
    manifests: Vec<ReferrerDescriptor>,
}

#[derive(Debug, Deserialize)]
struct ReferrerDescriptor {
    digest: String,
    #[serde(rename = "artifactType", default)]
    artifact_type: Option<String>,
}

fn detect_arch() -> Result<String> {
    match std::env::consts::ARCH {
        "x86_64" => Ok("x86_64".into()),
        "aarch64" => Ok("aarch64".into()),
        other => bail!("unsupported architecture: {other}"),
    }
}

/// Registry-host label for resolve metrics. Falls back to `"unknown"` when the
/// reference has no recognizable host segment, keeping cardinality bounded.
fn registry_label(image_ref: &str) -> String {
    registry_host_of(image_ref).unwrap_or("unknown").to_string()
}

fn overlaybd_image_config_json(resolved: &ResolvedImage) -> Value {
    let (repo_blob_url, lowers_json): (&str, Vec<serde_json::Value>) = match resolved {
        ResolvedImage::Local(paths) => {
            let lowers = paths
                .iter()
                .map(|layer| {
                    json!({
                        "file": layer.path.to_string_lossy().into_owned(),
                        "digest": layer.digest.clone(),
                        "size": layer.size
                    })
                })
                .collect();
            ("", lowers)
        }
        ResolvedImage::Remote {
            repo_blob_url,
            layers,
        } => {
            let lowers = layers
                .iter()
                .map(|l| {
                    json!({
                        "digest": l.digest,
                        "size": l.size,
                        "dir": l.dir.to_string_lossy().into_owned()
                    })
                })
                .collect();
            (repo_blob_url.as_str(), lowers)
        }
    };
    json!({
        "repoBlobUrl": repo_blob_url,
        "lowers": lowers_json,
        "upper": {},
        "resultFile": ""
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{ImageConfig, ImageResolverConfig};
    use tempfile::TempDir;

    fn test_resolver_with_search(temp: &TempDir, search_registries: Vec<&str>) -> ImageResolver {
        let mut config = AppConfig {
            image: ImageConfig {
                resolver: ImageResolverConfig {
                    search_registries: search_registries
                        .into_iter()
                        .map(ToString::to_string)
                        .collect(),
                    ..ImageResolverConfig::default()
                },
                ..ImageConfig::default()
            },
            ..AppConfig::default()
        };
        ImageConfig::normalize(&mut config.image, temp.path(), temp.path());
        ImageResolver::new(&config)
    }

    #[test]
    fn image_config_normalize_resolver_fields() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = ImageConfig {
            resolver: ImageResolverConfig {
                default_image: " ghcr.io/example/base:latest ".to_string(),
                search_registries: vec![
                    " registry.internal:5000/team/ ".to_string(),
                    "ghcr.io".to_string(),
                ],
                try_referrers_overlaybd_prefixes: vec![
                    " registry.example.com/team/ ".to_string(),
                    "".to_string(),
                    "registry.example.com/".to_string(),
                ],
                ..ImageResolverConfig::default()
            },
            ..ImageConfig::default()
        };
        ImageConfig::normalize(&mut config, temp.path(), temp.path());

        assert_eq!(config.resolver.default_image, "ghcr.io/example/base:latest");
        assert_eq!(
            config.resolver.search_registries,
            vec![
                "registry.internal:5000/team".to_string(),
                "ghcr.io".to_string()
            ]
        );
        assert_eq!(
            config.resolver.try_referrers_overlaybd_prefixes,
            vec![
                "registry.example.com/team/".to_string(),
                "registry.example.com/".to_string()
            ]
        );
    }

    #[test]
    fn image_config_normalize_allowed_registries_distinguishes_unset_and_empty() {
        let temp = tempfile::tempdir().expect("tempdir");

        // Unset key => None (no restriction).
        let mut unset = ImageConfig::default();
        ImageConfig::normalize(&mut unset, temp.path(), temp.path());
        assert_eq!(unset.resolver.allowed_registries, None);

        // Explicit empty list => Some([]) (deny all), distinct from None.
        let mut empty = ImageConfig {
            resolver: ImageResolverConfig {
                allowed_registries: Some(vec![]),
                ..ImageResolverConfig::default()
            },
            ..ImageConfig::default()
        };
        ImageConfig::normalize(&mut empty, temp.path(), temp.path());
        assert_eq!(empty.resolver.allowed_registries, Some(vec![]));

        // Non-empty list is trimmed, trailing slashes stripped, empties dropped.
        let mut set = ImageConfig {
            resolver: ImageResolverConfig {
                allowed_registries: Some(vec![
                    " registry.example.com/ ".to_string(),
                    "".to_string(),
                    "   ".to_string(),
                    "ghcr.io".to_string(),
                ]),
                ..ImageResolverConfig::default()
            },
            ..ImageConfig::default()
        };
        ImageConfig::normalize(&mut set, temp.path(), temp.path());
        assert_eq!(
            set.resolver.allowed_registries,
            Some(vec![
                "registry.example.com".to_string(),
                "ghcr.io".to_string()
            ])
        );
    }

    #[test]
    fn image_config_normalize_blank_default_image_to_schema_default() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = ImageConfig {
            resolver: ImageResolverConfig {
                default_image: "   ".to_string(),
                ..ImageResolverConfig::default()
            },
            ..ImageConfig::default()
        };
        ImageConfig::normalize(&mut config, temp.path(), temp.path());

        assert_eq!(
            config.resolver.default_image,
            ImageResolverConfig::default().default_image
        );
    }

    #[test]
    fn resolver_uses_normalized_config_fields() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut config = AppConfig {
            image: ImageConfig {
                resolver: ImageResolverConfig {
                    default_image: " ghcr.io/example/base:latest ".to_string(),
                    ..ImageResolverConfig::default()
                },
                ..ImageConfig::default()
            },
            ..AppConfig::default()
        };
        ImageConfig::normalize(&mut config.image, temp.path(), temp.path());
        let resolver = ImageResolver::new(&config);

        assert_eq!(resolver.default_image(), "ghcr.io/example/base:latest");
    }

    #[test]
    fn should_try_overlaybd_referrers_matches_domain_or_namespace_prefixes() {
        let prefixes = vec![
            "registry.example.com/team/".to_string(),
            "other.example.com/".to_string(),
        ];

        assert!(should_try_overlaybd_referrers(
            "registry.example.com/team/app:tag",
            &prefixes
        ));
        assert!(should_try_overlaybd_referrers(
            "registry.example.com/team/app@sha256:abc",
            &prefixes
        ));
        assert!(should_try_overlaybd_referrers(
            "other.example.com/app:tag",
            &prefixes
        ));
        assert!(!should_try_overlaybd_referrers(
            "registry.example.com/team2/app:tag",
            &prefixes
        ));
    }

    #[test]
    fn parse_overlaybd_referrer_picks_matching_artifact() {
        let body = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:other",
                    "size": 10,
                    "artifactType": "application/vnd.example.other"
                },
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:overlaybd",
                    "size": 20,
                    "artifactType": OVERLAYBD_NATIVE_ARTIFACT_TYPE
                }
            ]
        });

        assert_eq!(
            parse_overlaybd_referrer(&body.to_string())
                .expect("parse")
                .map(|(digest, _)| digest),
            Some("sha256:overlaybd".to_string())
        );
    }

    #[test]
    fn parse_overlaybd_referrer_keeps_first_matching_artifact() {
        let body = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:first",
                    "size": 10,
                    "artifactType": OVERLAYBD_NATIVE_ARTIFACT_TYPE
                },
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:second",
                    "size": 20,
                    "artifactType": OVERLAYBD_NATIVE_ARTIFACT_TYPE
                }
            ]
        });

        assert_eq!(
            parse_overlaybd_referrer(&body.to_string())
                .expect("parse")
                .map(|(digest, _)| digest),
            Some("sha256:first".to_string())
        );
    }

    #[test]
    fn parse_overlaybd_referrer_returns_none_without_match() {
        let body = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": []
        });

        assert_eq!(
            parse_overlaybd_referrer(&body.to_string()).expect("parse"),
            None
        );
    }

    #[test]
    fn parse_overlaybd_referrer_accepts_acr_artifact_streaming() {
        // Shape emitted by `az acr artifact-streaming create`, annotations
        // included. Only the artifactType differs from
        // accelerated-container-image: the referrer it points at is an ordinary
        // overlaybd-native manifest whose layers carry a tar mediaType plus a
        // blob-digest annotation equal to the layer's own digest.
        let body = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:0a21030948e9223e054ab830dd82b0ad85f921df34f11c5bf769bb0ed636d72a",
                    "size": 1234,
                    "artifactType": ACR_ARTIFACT_STREAMING_ARTIFACT_TYPE,
                    "annotations": {
                        "streaming.format": "overlaybd",
                        "streaming.version": "v1",
                        "streaming.platform.os": "linux",
                        "streaming.platform.arch": "amd64"
                    }
                }
            ]
        });

        assert_eq!(
            parse_overlaybd_referrer(&body.to_string()).expect("parse"),
            Some((
                "sha256:0a21030948e9223e054ab830dd82b0ad85f921df34f11c5bf769bb0ed636d72a"
                    .to_string(),
                ACR_ARTIFACT_STREAMING_ARTIFACT_TYPE
            ))
        );
    }

    #[test]
    fn parse_overlaybd_referrer_prefers_containerd_native_over_acr_streaming() {
        let body = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:acr",
                    "size": 10,
                    "artifactType": ACR_ARTIFACT_STREAMING_ARTIFACT_TYPE
                },
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:native",
                    "size": 20,
                    "artifactType": OVERLAYBD_NATIVE_ARTIFACT_TYPE
                }
            ]
        });

        assert_eq!(
            parse_overlaybd_referrer(&body.to_string()).expect("parse"),
            Some(("sha256:native".to_string(), OVERLAYBD_NATIVE_ARTIFACT_TYPE))
        );
    }

    #[test]
    fn parse_overlaybd_referrer_ignores_unrelated_artifact_types() {
        // Turbo-OCI in particular must never be selected: AgentENV's overlaybd
        // runtime does not implement the turbo read path.
        let body = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:sbom",
                    "size": 10,
                    "artifactType": "application/spdx+json"
                },
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:turbo",
                    "size": 20,
                    "artifactType": "application/vnd.containerd.overlaybd.turbo.v1+json"
                }
            ]
        });

        assert_eq!(
            parse_overlaybd_referrer(&body.to_string()).expect("parse"),
            None
        );
    }

    #[tokio::test]
    async fn resolve_rejects_empty_configured_search_list_for_shortnames() {
        let temp = TempDir::new().expect("tempdir");
        let resolver = test_resolver_with_search(&temp, vec![]);

        let err = resolver
            .resolve("ubuntu")
            .await
            .expect_err("empty search list should fail");

        assert!(matches!(err, ImageError::InvalidReference { .. }));
        assert!(err.is_user_error());
        assert!(err.to_string().contains("search registry list"));
    }

    #[test]
    fn image_resolve_error_classifies_user_vs_server_errors() {
        // InvalidReference is covered by the resolve() test above; here we check
        // the NotFound (user) vs Other (server) classification.
        assert!(ImageError::NotFound {
            reason: "missing".to_string(),
        }
        .is_user_error());
        assert!(
            !ImageError::Other(anyhow!("network down")).is_user_error(),
            "Other should be a server error"
        );
    }

    #[test]
    fn overlaybd_image_config_json_preserves_local_layer_descriptor() {
        let temp = TempDir::new().expect("tempdir");
        let layer_path = temp.path().join("layer.commit");
        let resolved = ResolvedImage::Local(vec![crate::image::local_layer::LocalLayer {
            path: layer_path.clone(),
            digest: "sha256:commit".to_string(),
            size: 123,
        }]);

        let config = overlaybd_image_config_json(&resolved);
        let expected_file = layer_path.display().to_string();
        assert_eq!(
            config["lowers"][0]["file"].as_str(),
            Some(expected_file.as_str())
        );
        assert_eq!(
            config["lowers"][0]["digest"].as_str(),
            Some("sha256:commit")
        );
        assert_eq!(config["lowers"][0]["size"].as_u64(), Some(123));
    }

    #[test]
    fn overlaybd_image_config_json_emits_remote_layer_dir() {
        let temp = TempDir::new().expect("tempdir");
        let layer_dir = temp.path().join("commits/sha256-abc");
        let resolved = ResolvedImage::Remote {
            repo_blob_url: "https://registry.example/v2/repo/blobs".to_string(),
            layers: vec![crate::image::oci_image::RemoteLayer {
                digest: "sha256:abc".to_string(),
                size: 123,
                dir: layer_dir.clone(),
            }],
        };

        let config = overlaybd_image_config_json(&resolved);
        let expected_layer_dir = layer_dir.display().to_string();
        assert_eq!(
            config["lowers"][0]["dir"].as_str(),
            Some(expected_layer_dir.as_str())
        );
        assert!(config["lowers"][0].get("cacheFile").is_none());
    }
}
