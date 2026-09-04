use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SandboxId(Uuid);

impl SandboxId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn parse_str(id: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(id)?))
    }

    pub fn into_inner(self) -> Uuid {
        self.0
    }

    pub fn max() -> Self {
        Self(Uuid::max())
    }

    pub fn min() -> Self {
        Self(Uuid::nil())
    }
}

impl Default for SandboxId {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<String> for SandboxId {
    type Error = uuid::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse_str(&value)
    }
}

impl TryFrom<&str> for SandboxId {
    type Error = uuid::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse_str(value)
    }
}

impl From<SandboxId> for String {
    fn from(value: SandboxId) -> Self {
        value.0.to_string()
    }
}

impl fmt::Display for SandboxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialOrd for SandboxId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SandboxId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
