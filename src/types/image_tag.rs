use std::{fmt, fmt::Display};

use anyhow::{ensure, Result};

/// The tag an image reference without `:tag` means.
pub const DEFAULT_IMAGE_TAG: &str = "latest";

/// Longest name of a pulled image's copy, so `aenv/<name>` stays within the
/// 255-character OCI name limit.
const MAX_PULLED_NAME_LEN: usize = 250;

/// The name of a published image, which owns its tags. Referenced in image
/// fields as `aenv/name[:tag]`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImageName(String);

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
    /// `host/path`, such as `docker.io/library/python`. Every segment follows
    /// the OCI path-component grammar, and the first is a host containing a
    /// `.`. Names from [`Self::parse`] have one segment, so the two never meet.
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImageTag {
    name: ImageName,
    tag: String,
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
            tag.len() <= 128
                && tag
                    .bytes()
                    .next()
                    .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
                && tag
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b)),
            "invalid image tag '{tag}': use letters, digits, '.', '_', or '-', not starting with '.' or '-'"
        );
        Ok(Self {
            name: name.clone(),
            tag: tag.to_owned(),
        })
    }

    /// The image name that owns the tag.
    pub fn name(&self) -> &ImageName {
        &self.name
    }

    /// The tag without its name.
    pub fn tag(&self) -> &str {
        &self.tag
    }
}

impl Display for ImageTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.tag)
    }
}

#[cfg(test)]
mod tests {
    use super::{ImageName, ImageTag};

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
