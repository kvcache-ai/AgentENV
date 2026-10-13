use std::{fmt, fmt::Display};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Stable identifier for a committed snapshot.
/// Inner type is `Uuid`: Display/serde output is always lowercase-hyphenated.
/// Snapshot directory names on disk must match; uppercase IDs from external
/// tools or manual edits will cause lookup misses in the PosixFs backend.
pub struct SnapshotId(pub(crate) Uuid);

impl SnapshotId {
    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }

    /// Parses a UUID string as a snapshot ID.
    pub fn parse(input: &str) -> Result<Self> {
        let uuid = Uuid::parse_str(input)
            .map_err(|e| anyhow::anyhow!("invalid snapshot ID '{}': {e}", input))?;
        Ok(Self(uuid))
    }

    /// Returns the max-UUID sentinel used exclusively for open-ended pagination cursors.
    pub(crate) fn max() -> Self {
        Self(Uuid::max())
    }
}

impl Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<&str> for SnapshotId {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Human-readable alias that can point to a committed snapshot.
pub struct SnapshotAlias(pub(crate) String);

impl SnapshotAlias {
    /// Parses and validates a user-provided snapshot alias.
    pub fn parse(input: &str) -> Result<Self> {
        if input.is_empty() {
            bail!("snapshot alias cannot be empty");
        }
        if !input
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            bail!("snapshot alias only supports ASCII letters, digits, hyphens, and underscores, got: {input}");
        }
        Ok(Self(input.to_string()))
    }
}

impl Display for SnapshotAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for SnapshotAlias {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{SnapshotAlias, SnapshotId};

    #[test]
    fn alias_parse_accepts_valid_chars() {
        let parsed = SnapshotAlias::parse("alias_1-ok").expect("alias should parse");
        assert_eq!(parsed.0, "alias_1-ok");
    }

    #[test]
    fn alias_parse_rejects_invalid_chars() {
        let err = SnapshotAlias::parse("bad alias").expect_err("alias with spaces should fail");
        assert!(err.to_string().contains("only supports"));
    }

    #[test]
    fn snapshot_id_parse_accepts_valid_uuid_v7() {
        let id = SnapshotId::generate();
        let parsed = SnapshotId::parse(&id.to_string()).expect("generated UUID v7 should parse");
        assert_eq!(parsed.to_string(), id.to_string());
    }

    #[test]
    fn snapshot_id_parse_accepts_uuid_v4() {
        SnapshotId::parse("550e8400-e29b-41d4-a716-446655440000")
            .expect("UUID v4 should be accepted");
    }
}
