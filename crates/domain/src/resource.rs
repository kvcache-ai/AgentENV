//! Shared resource identity kinds for layer ownership and GC pins.

use std::fmt;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// Durable layer-bearing resource kinds tracked in `layer_heads` and `gc_pins`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Snapshot,
    Volume,
    Image,
}

impl ResourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Volume => "volume",
            Self::Image => "image",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "snapshot" => Ok(Self::Snapshot),
            "volume" => Ok(Self::Volume),
            "image" => Ok(Self::Image),
            other => bail!("unknown resource kind '{other}'"),
        }
    }
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
