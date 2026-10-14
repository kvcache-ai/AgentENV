use std::{fmt, fmt::Display};

use anyhow::{ensure, Result};

/// A mutable `name:tag` pointer to one published image, referenced in image
/// fields as `aenv/name:tag`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImageTag(String);

impl ImageTag {
    /// Parses `name[:tag]`; an omitted tag means `latest`.
    ///
    /// The name uses lowercase letters and digits joined by single `.`, `_`, or
    /// `-` separators. The tag starts with a letter, digit, or `_` and continues
    /// with letters, digits, `.`, `_`, or `-`. Each is at most 128 characters.
    pub fn parse(input: &str) -> Result<Self> {
        let (name, tag) = input.split_once(':').unwrap_or((input, "latest"));
        ensure!(
            name.len() <= 128
                && name.split(['.', '_', '-']).all(|part| {
                    !part.is_empty()
                        && part
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                }),
            "invalid image tag name '{name}': use lowercase letters and digits joined by single '.', '_', or '-'"
        );
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
        Ok(Self(format!("{name}:{tag}")))
    }
}

impl Display for ImageTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ImageTag {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::ImageTag;

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
        assert_eq!(ImageTag::parse(&long).unwrap().as_ref(), long);
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
    }
}
