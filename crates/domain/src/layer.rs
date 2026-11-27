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

impl ManagedLayerRef {
    /// The directory needs a stable identity even for old sealed layers with
    /// a nil UUID. Use a UUIDv8 derived from the digest without changing the
    /// physical layer's UUID or what is passed to OverlayBD.
    pub fn directory_uuid(&self) -> Result<uuid::Uuid, String> {
        if let Some(value) = &self.uuid {
            let uuid = uuid::Uuid::parse_str(value)
                .map_err(|_| format!("managed layer '{}' has an invalid UUID", self.digest))?;
            if !uuid.is_nil() {
                return Ok(uuid);
            }
        }
        let hash = self
            .digest
            .strip_prefix("sha256:")
            .filter(|_| crate::is_sha256_digest(&self.digest))
            .ok_or_else(|| {
                "managed layer without UUID requires a canonical SHA-256 digest".to_owned()
            })?;
        let mut bytes = uuid::Uuid::parse_str(&hash[..32])
            .expect("validated digest prefix")
            .into_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Ok(uuid::Uuid::from_bytes(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalLayer {
    pub digest: String,
    pub repo_blob_url: String,
    pub size: u64,
}
