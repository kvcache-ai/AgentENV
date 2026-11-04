//! Persistent layer identities and origins.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LayerRef {
    Managed(ManagedLayerRef),
    External(ExternalLayer),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedLayerRef {
    pub digest: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalLayer {
    pub digest: String,
    pub repo_blob_url: String,
    pub size: u64,
}
