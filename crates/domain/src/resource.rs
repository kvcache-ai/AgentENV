//! Shared resource identity kinds for managed-layer ownership (`layer_heads`).

use serde::{Deserialize, Serialize};

/// Durable layer-bearing resource kinds tracked in `layer_heads`.
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
}
