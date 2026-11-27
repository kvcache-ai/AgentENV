//! Published OverlayBD image descriptions and mutable `name:tag` pointers.

use std::fmt::{self, Display};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::LayerRef;

/// The tag an image reference without `:tag` means.
pub const DEFAULT_IMAGE_TAG: &str = "latest";

/// Longest name of a pulled image's copy, so `aenv/<name>` stays within the
/// 255-character OCI name limit.
const MAX_PULLED_NAME_LEN: usize = 250;

/// Returns whether `value` is a canonical lowercase `sha256:<64hex>` digest.
pub fn is_sha256_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// Computes a canonical `sha256:<64hex>` digest over `bytes`.
pub fn sha256_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{}", hex::encode(digest))
}

/// The name of a published image, which owns its tags. Referenced in image
/// fields as `aenv/name[:tag]`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ImageName(String);

impl<'de> Deserialize<'de> for ImageName {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        if raw.contains('/') {
            Self::parse_pulled(&raw)
        } else {
            Self::parse(&raw)
        }
        .map_err(serde::de::Error::custom)
    }
}

impl ImageName {
    /// Parses a name of lowercase letters and digits joined by single `.`,
    /// `_`, or `-` separators, at most 128 characters.
    pub fn parse(name: &str) -> Result<Self> {
        ensure!(
            name.len() <= 128
                && name.split(['.', '_', '-']).all(|part| {
                    !part.is_empty()
                        && part
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                }),
            "invalid image name '{name}': use lowercase letters and digits joined by single '.', '_', or '-'"
        );
        Ok(Self(name.to_owned()))
    }

    /// Parses the name of a pulled registry image's published copy: its
    /// `host/path`, such as `docker.io/library/python`.
    pub fn parse_pulled(name: &str) -> Result<Self> {
        ensure!(
            name.len() <= MAX_PULLED_NAME_LEN
                && name
                    .split_once('/')
                    .is_some_and(|(host, _)| host.contains('.'))
                && name.split('/').all(is_oci_path_component),
            "invalid pulled image name '{name}': use a registry host with a '.' followed by OCI path components, at most {MAX_PULLED_NAME_LEN} characters"
        );
        Ok(Self(name.to_owned()))
    }
}

/// `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*`, as in the OCI distribution spec.
fn is_oci_path_component(segment: &str) -> bool {
    let alphanumeric = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    segment.starts_with(alphanumeric)
        && segment.ends_with(alphanumeric)
        && segment.split(alphanumeric).all(|separator| {
            matches!(separator, "" | "." | "_" | "__") || separator.bytes().all(|b| b == b'-')
        })
}

impl Display for ImageName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ImageName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A mutable `name:tag` pointer to one image published under its name,
/// referenced in image fields as `aenv/name:tag`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ImageTag {
    name: ImageName,
    tag: String,
}

impl<'de> Deserialize<'de> for ImageTag {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawImageTag {
            name: ImageName,
            tag: String,
        }

        let raw = RawImageTag::deserialize(deserializer)?;
        Self::new(&raw.name, &raw.tag).map_err(serde::de::Error::custom)
    }
}

/// Returns whether `tag` satisfies the OCI distribution tag grammar
/// (`[A-Za-z0-9_][A-Za-z0-9._-]{0,127}`).
pub fn is_valid_oci_tag(tag: &str) -> bool {
    tag.len() <= 128
        && tag
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
        && tag
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b))
}

impl ImageTag {
    /// Parses `name[:tag]`; an omitted tag means [`DEFAULT_IMAGE_TAG`]. A
    /// name with a `/` must be a pulled image's name.
    pub fn parse(input: &str) -> Result<Self> {
        let (name, tag) = input.split_once(':').unwrap_or((input, DEFAULT_IMAGE_TAG));
        let name = if name.contains('/') {
            ImageName::parse_pulled(name)?
        } else {
            ImageName::parse(name)?
        };
        Self::new(&name, tag)
    }

    /// The tag `tag` of `name`. The tag starts with a letter, digit, or `_`
    /// and continues with letters, digits, `.`, `_`, or `-`, at most 128
    /// characters.
    pub fn new(name: &ImageName, tag: &str) -> Result<Self> {
        ensure!(
            is_valid_oci_tag(tag),
            "invalid image tag '{tag}': use letters, digits, '.', '_', or '-', not starting with '.' or '-'"
        );
        Ok(Self {
            name: name.clone(),
            tag: tag.to_owned(),
        })
    }

    pub fn name(&self) -> &ImageName {
        &self.name
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }
}

impl Display for ImageTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.tag)
    }
}

/// Portable description binding ordered repository layers and OCI runtime configuration.
/// Local paths and build identities are excluded from its content address.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublishedImage {
    pub schema_version: u32,
    pub os: String,
    pub architecture: String,
    pub layers: Vec<LayerRef>,
    pub config: Value,
}

impl PublishedImage {
    pub fn new(architecture: String, layers: Vec<LayerRef>, config: Value) -> Self {
        Self {
            schema_version: 1,
            os: "linux".into(),
            architecture,
            layers,
            config,
        }
    }

    pub fn encode(&self) -> Result<(String, Vec<u8>)> {
        self.validate()?;
        let mut value = serde_json::to_value(self)?;
        value.sort_all_objects();
        let bytes = serde_json::to_vec(&value)?;
        Ok((sha256_digest(&bytes), bytes))
    }

    pub fn decode(digest: &str, bytes: &[u8]) -> Result<Self> {
        ensure!(is_sha256_digest(digest), "invalid image digest '{digest}'");
        ensure!(
            sha256_digest(bytes) == digest,
            "OverlayBD image description digest mismatch"
        );
        let image: Self =
            serde_json::from_slice(bytes).context("decode OverlayBD image description")?;
        image.validate()?;
        Ok(image)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported OverlayBD image description version"
        );
        ensure!(self.os == "linux", "unsupported image operating system");
        ensure!(
            matches!(self.architecture.as_str(), "amd64" | "arm64"),
            "unsupported image architecture"
        );
        ensure!(
            self.config.is_object(),
            "image runtime configuration must be an object"
        );
        ensure!(
            !self.layers.is_empty() && self.layers.len() <= 1024,
            "invalid image layer count"
        );
        for layer in &self.layers {
            let LayerRef::Managed(layer) = layer else {
                anyhow::bail!("published images must own their layers in the repository");
            };
            ensure!(
                is_sha256_digest(&layer.digest),
                "invalid image layer digest '{}'",
                layer.digest
            );
            ensure!(layer.size > 0, "empty image layer");
        }
        Ok(())
    }
}

/// Where an image tag points, and since when.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageTagRecord {
    pub image_digest: String,
    pub created_at_unix_ms: i64,
    /// For a pulled image's copy, the registry manifest the image was
    /// converted from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_manifest_digest: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ManagedLayerRef;
    use serde_json::json;

    #[test]
    fn published_image_identity_binds_canonical_fields() -> Result<()> {
        let image = PublishedImage::new(
            "amd64".into(),
            [b"base".as_slice(), b"delta".as_slice()]
                .into_iter()
                .map(|data| {
                    LayerRef::Managed(ManagedLayerRef {
                        digest: sha256_digest(data),
                        size: data.len() as u64,
                        uuid: None,
                    })
                })
                .collect(),
            json!({"Env": ["A=1"], "Cmd": ["serve"]}),
        );
        let (digest, bytes) = image.encode()?;
        assert_eq!(PublishedImage::decode(&digest, &bytes)?.encode()?.0, digest);
        Ok(())
    }

    #[test]
    fn parses_canonical_name_and_tag() {
        for (input, canonical) in [
            ("app", "app:latest"),
            ("app:v1", "app:v1"),
            ("my-app.web_1:V1.2-rc_3", "my-app.web_1:V1.2-rc_3"),
            ("app:_build", "app:_build"),
        ] {
            assert_eq!(ImageTag::parse(input).unwrap().to_string(), canonical);
        }
        let long = format!("{}:{}", "a".repeat(128), "b".repeat(128));
        assert_eq!(ImageTag::parse(&long).unwrap().to_string(), long);
        let tag = ImageTag::parse("my-app.web_1:V1.2-rc_3").unwrap();
        assert_eq!(tag.name(), &ImageName::parse("my-app.web_1").unwrap());
        assert_eq!(tag.tag(), "V1.2-rc_3");
        assert_eq!(
            ImageTag::new(tag.name(), "v2").unwrap().to_string(),
            "my-app.web_1:v2"
        );
    }

    #[test]
    fn rejects_invalid_names_and_tags() {
        for input in [
            "",
            ":v1",
            "App:v1",
            "team/app:v1",
            "../app",
            "app..web",
            "-app",
            "app-",
            "app:",
            "app:.v1",
            "app:-v1",
            "app:v1:extra",
            "app:v/1",
            "sha256:../x",
            &format!("{}:v1", "a".repeat(129)),
            &format!("app:{}", "b".repeat(129)),
        ] {
            assert!(
                ImageTag::parse(input).is_err(),
                "{input} should be rejected"
            );
        }
        let name = ImageName::parse("app").unwrap();
        for tag in ["", ".v1", "v1:extra", "v/1", "../x"] {
            assert!(
                ImageTag::new(&name, tag).is_err(),
                "{tag} should be rejected"
            );
        }
        for name in ["App", "app:v1", "team/app", "docker.io/library/python", ""] {
            assert!(ImageName::parse(name).is_err(), "{name} should be rejected");
        }
        assert!(serde_json::from_str::<ImageName>(r#""../escape""#).is_err());
        assert!(serde_json::from_str::<ImageName>(r#""docker.io/library/python""#).is_ok());
        assert!(serde_json::from_str::<ImageName>(r#""my-app""#).is_ok());
        assert!(serde_json::from_str::<ImageTag>(r#"{"name":"app","tag":"../bad"}"#).is_err());
        assert!(serde_json::from_str::<ImageTag>(r#"{"name":"App","tag":"v1"}"#).is_err());
        let ok: ImageTag = serde_json::from_str(r#"{"name":"app","tag":"v1"}"#).unwrap();
        assert_eq!(ok.to_string(), "app:v1");
    }

    #[test]
    fn parses_pulled_image_names() {
        for (input, name, tag) in [
            (
                "docker.io/library/python:3.12",
                "docker.io/library/python",
                "3.12",
            ),
            (
                "registry.example.com/base/ubuntu",
                "registry.example.com/base/ubuntu",
                "latest",
            ),
            ("ghcr.io/a__b/c--d.e_f:v1", "ghcr.io/a__b/c--d.e_f", "v1"),
        ] {
            let parsed = ImageTag::parse(input).unwrap();
            assert_eq!(parsed.name(), &ImageName::parse_pulled(name).unwrap());
            assert_eq!(parsed.tag(), tag);
        }
        let long = format!("docker.io/{}", "a".repeat(240));
        assert!(ImageName::parse_pulled(&long).is_ok());
        for name in [
            "docker.io",
            "team/app",
            "localhost:5000/app",
            "docker.io/Library/python",
            "docker.io//python",
            "docker.io/python/",
            "/docker.io/python",
            "docker.io/../python",
            "docker.io/a..b",
            "docker.io/a___b",
            "docker.io/-a",
            "docker.io/a-",
            "docker.io/a@b",
            &format!("docker.io/{}", "a".repeat(241)),
        ] {
            assert!(
                ImageName::parse_pulled(name).is_err(),
                "{name} should be rejected"
            );
        }
    }
}
