use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};

use crate::sandbox::EnvdAccessToken;
use crate::types::SandboxId;

const EMPTY_ACCESS_TOKEN: &str = "";
const RESERVED_FIELDS: [&str; 4] = ["instanceID", "envID", "address", "accessTokenHash"];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MmdsMetadata {
    #[serde(rename = "instanceID")]
    pub sandbox_id: String,
    #[serde(rename = "envID")]
    pub snapshot_id: String,
    #[serde(rename = "address")]
    pub logs_collector_address: String,
    #[serde(rename = "accessTokenHash")]
    pub access_token_hash: String,
    /// Extra key-value pairs merged into the top-level MMDS JSON object.
    /// API layers use this to pass opaque data (e.g. image configs)
    /// through to the VM without the sandbox layer interpreting it. Reserved
    /// MMDS fields are ignored so extras cannot override runtime identity or auth.
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

impl MmdsMetadata {
    pub fn new(sandbox_id: SandboxId, snapshot_id: impl Into<String>) -> Self {
        Self {
            sandbox_id: sandbox_id.to_string(),
            snapshot_id: snapshot_id.into(),
            logs_collector_address: String::new(),
            access_token_hash: hash_access_token(EMPTY_ACCESS_TOKEN),
            extra: serde_json::Map::new(),
        }
    }

    /// Merge additional key-value pairs into the extra MMDS metadata.
    pub fn with_extra(mut self, mut extra: serde_json::Map<String, serde_json::Value>) -> Self {
        extra.retain(|key, _| !RESERVED_FIELDS.contains(&key.as_str()));
        self.extra.extend(extra);
        self
    }

    pub(crate) fn with_access_token(mut self, token: Option<&EnvdAccessToken>) -> Self {
        self.set_access_token(token);
        self
    }

    pub(crate) fn set_access_token(&mut self, token: Option<&EnvdAccessToken>) {
        self.access_token_hash = hash_access_token(token.map_or("", EnvdAccessToken::expose));
    }
}

fn hash_access_token(token: &str) -> String {
    let digest = Sha512::digest(token.as_bytes());
    format!("{digest:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn serializes_with_e2b_mmds_field_names() {
        let sandbox_id = SandboxId::from_uuid(Uuid::nil());
        let metadata = MmdsMetadata::new(sandbox_id, "snapshot-123");

        let value = serde_json::to_value(metadata).expect("serialize metadata");

        assert_eq!(
            value,
            json!({
                "instanceID": "00000000-0000-0000-0000-000000000000",
                "envID": "snapshot-123",
                "address": "",
                "accessTokenHash": hash_access_token(""),
            })
        );
    }

    #[test]
    fn hashes_secure_access_token_with_sha512() {
        let sandbox_id = SandboxId::from_uuid(Uuid::nil());
        let token = crate::sandbox::SandboxAccessTokenGenerator::new("mmds-test-seed")
            .unwrap()
            .generate(sandbox_id);

        let metadata =
            MmdsMetadata::new(sandbox_id, "snapshot-123").with_access_token(Some(&token));

        assert_eq!(
            metadata.access_token_hash,
            hash_access_token(token.expose())
        );
        assert!(!metadata.access_token_hash.contains(token.expose()));
    }

    #[test]
    fn extra_fields_are_flattened_into_top_level() {
        let sandbox_id = SandboxId::from_uuid(Uuid::nil());
        let mut extra = serde_json::Map::new();
        extra.insert(
            "imageConfigs".to_string(),
            json!({ "rootfs": { "Cmd": ["/bin/sh"] } }),
        );
        let metadata = MmdsMetadata::new(sandbox_id, "snap-1").with_extra(extra);

        let value = serde_json::to_value(&metadata).expect("serialize");
        assert_eq!(
            value["imageConfigs"],
            json!({ "rootfs": { "Cmd": ["/bin/sh"] } })
        );
        // Core fields still present.
        assert_eq!(
            value["instanceID"],
            json!("00000000-0000-0000-0000-000000000000")
        );
    }

    #[test]
    fn extra_fields_cannot_override_reserved_fields() {
        let sandbox_id = SandboxId::from_uuid(Uuid::nil());
        let token = crate::sandbox::SandboxAccessTokenGenerator::new("mmds-test-seed")
            .unwrap()
            .generate(sandbox_id);
        let expected =
            MmdsMetadata::new(sandbox_id, "snapshot-123").with_access_token(Some(&token));
        let mut extra = serde_json::Map::new();
        for field in RESERVED_FIELDS {
            extra.insert(field.to_string(), json!("attacker-controlled"));
        }

        let metadata = expected.clone().with_extra(extra);
        let value = serde_json::to_value(&metadata).expect("serialize");

        assert_eq!(metadata, expected);
        assert_eq!(value["instanceID"], json!(sandbox_id.to_string()));
        assert_eq!(value["envID"], json!("snapshot-123"));
        assert_eq!(value["address"], json!(""));
        assert_eq!(value["accessTokenHash"], json!(expected.access_token_hash));
    }

    #[test]
    fn round_trips_with_extra_fields() {
        let sandbox_id = SandboxId::from_uuid(Uuid::nil());
        let mut extra = serde_json::Map::new();
        extra.insert("custom".to_string(), json!(42));
        let original = MmdsMetadata::new(sandbox_id, "snap-1").with_extra(extra);

        let json = serde_json::to_string(&original).expect("serialize");
        let restored: MmdsMetadata = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, restored);
    }

    #[test]
    fn deserializes_legacy_json_without_extra_fields() {
        let legacy = json!({
            "instanceID": "00000000-0000-0000-0000-000000000000",
            "envID": "snapshot-123",
            "address": "",
            "accessTokenHash": hash_access_token(""),
        });
        let metadata: MmdsMetadata = serde_json::from_value(legacy).expect("deserialize legacy");
        assert!(metadata.extra.is_empty());
        assert_eq!(metadata.sandbox_id, "00000000-0000-0000-0000-000000000000");
    }
}
