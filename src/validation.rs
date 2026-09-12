//! Shared identifier and scalar validation helpers.
//!
//! Several entry points accept user-supplied identifiers (sandbox names, template
//! names, snapshot tag components). They previously validated inline and drifted
//! apart, so this module centralizes the rules that the rest of the crate relies on.

use anyhow::{bail, Result};

/// Longest identifier accepted anywhere in the runtime.
pub const MAX_IDENTIFIER_LEN: usize = 63;

/// Returns true when `value` matches the project-wide identifier grammar:
/// `[a-z_][a-z0-9_-]*`, as already documented for setup runtime users.
pub fn is_valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// Validate an identifier and report why it was rejected.
pub fn validate_identifier(kind: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{kind} must not be empty");
    }
    if value.len() > MAX_IDENTIFIER_LEN {
        bail!(
            "{kind} '{}' is too long ({} > {})",
            value,
            value.len(),
            MAX_IDENTIFIER_LEN
        );
    }
    if !is_valid_identifier(value) {
        bail!("{kind} '{value}' is invalid (must match [a-z_][a-z0-9_-]*)");
    }
    Ok(())
}

/// Validate a TCP port.
pub fn validate_port(kind: &str, port: u16) -> Result<()> {
    if port == 0 {
        bail!("{kind} must not be 0");
    }
    Ok(())
}

/// Validate a `KEY=VALUE` environment entry.
pub fn validate_env_entry(entry: &str) -> Result<()> {
    match entry.split_once('=') {
        None => bail!("environment entry {entry:?} must be in KEY=VALUE form"),
        Some((key, _)) => {
            if key.is_empty() {
                bail!("environment entry {entry:?} has an empty key");
            }
            // Avoid indexing the first char: an empty key is already rejected above,
            // and pattern-matching keeps this free of panicking unwraps.
            let starts_ok = key
                .chars()
                .next()
                .map(|c| c.is_ascii_alphabetic() || c == '_')
                .unwrap_or(false);
            if !starts_ok {
                bail!("environment key {key:?} must start with a letter or underscore");
            }
            Ok(())
        }
    }
}

/// Metadata pairs attached to a sandbox at creation time.
///
/// Keys and values both come from the request, so they are validated together
/// before the metadata is handed to the sandbox store.
pub fn validate_metadata(pairs: &[(String, String)]) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for (key, value) in pairs {
        if key.is_empty() {
            continue;
        }
        let normalized = format!("{}={}", key.trim().to_lowercase(), value.trim());
        if normalized.len() > MAX_METADATA_ENTRY_LEN {
            bail!("metadata entry {key:?} exceeds {MAX_METADATA_ENTRY_LEN} bytes");
        }
        out.push((
            key.trim().to_lowercase(),
            value.trim().to_string(),
        ));
    }
    Ok(out)
}

/// Longest accepted `key=value` metadata entry.
pub const MAX_METADATA_ENTRY_LEN: usize = 512;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_project_style_identifiers() {
        for ok in ["a", "_x", "sandbox-1", "snap_2024", "a1_b2-c3"] {
            assert!(is_valid_identifier(ok), "{ok} should be valid");
        }
    }

    #[test]
    fn rejects_identifiers_outside_the_grammar() {
        for bad in ["", "A", "1abc", "-lead", "has space", "dot.name", "trailing\n"] {
            assert!(!is_valid_identifier(bad), "{bad} should be invalid");
        }
    }

    #[test]
    fn length_bound_is_enforced() {
        let ok = "a".repeat(MAX_IDENTIFIER_LEN);
        let too_long = "a".repeat(MAX_IDENTIFIER_LEN + 1);
        assert!(validate_identifier("sandbox name", &ok).is_ok());
        assert!(validate_identifier("sandbox name", &too_long).is_err());
    }

    #[test]
    fn port_zero_is_rejected() {
        assert!(validate_port("listen port", 0).is_err());
        assert!(validate_port("listen port", 8080).is_ok());
    }

    #[test]
    fn metadata_normalizes_keys_and_drops_empty_ones() {
        let pairs = vec![
            ("  USER  ".to_string(), " alice ".to_string()),
            (String::new(), "ignored".to_string()),
            ("App".to_string(), "prod".to_string()),
        ];
        let out = validate_metadata(&pairs).unwrap();
        assert_eq!(
            out,
            vec![
                ("user".to_string(), "alice".to_string()),
                ("app".to_string(), "prod".to_string()),
            ]
        );
    }

    #[test]
    fn metadata_entry_length_is_bounded() {
        let long = "v".repeat(MAX_METADATA_ENTRY_LEN + 1);
        let pairs = vec![("k".to_string(), long)];
        assert!(validate_metadata(&pairs).is_err());
    }

    #[test]
    fn env_entries_require_key_and_value() {
        assert!(validate_env_entry("PATH=/usr/bin").is_ok());
        assert!(validate_env_entry("_PRIVATE=1").is_ok());
        assert!(validate_env_entry("NOEQUALS").is_err());
        assert!(validate_env_entry("=novalue").is_err());
        assert!(validate_env_entry("1BAD=1").is_err());
    }
}
