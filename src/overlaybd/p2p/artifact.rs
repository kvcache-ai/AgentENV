use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::p2p::{P2pArtifactDescriptor, P2pArtifactKey};

pub(super) const SHA256_HEX_LEN: usize = 64;
const PROTOCOL: &str = "agentenv-overlaybd-layer-v1";
const KEY_PREFIX: &str = "overlaybd-layer/v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CanonicalBlobIdentity {
    pub(super) key: String,
    pub(super) digest: Option<String>,
}

impl CanonicalBlobIdentity {
    pub(super) fn from_digest(digest: &str) -> Self {
        let digest = digest.to_ascii_lowercase();
        Self {
            key: format!("digest/{digest}"),
            digest: Some(digest),
        }
    }

    pub(super) fn from_uuid(uuid: &Uuid) -> Self {
        Self {
            key: format!("uuid/{uuid}"),
            digest: None,
        }
    }

    pub(super) fn from_origin_url(url: &str) -> Result<Self> {
        let parsed = url::Url::parse(url).with_context(|| format!("invalid origin url {url}"))?;
        if let Some(digest) = extract_sha256_digest(parsed.path()) {
            return Ok(Self::from_digest(&digest));
        }
        let host = parsed.host_str().context("origin url missing host")?;
        let host = match parsed.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        Ok(Self {
            key: format!("url/{}://{}{}", parsed.scheme(), host, parsed.path()),
            digest: None,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LayerMetadata {
    protocol: String,
    canonical_key: String,
    digest: Option<String>,
    uuid: Option<String>,
    size: Option<u64>,
    source_url_hash: Option<String>,
}

impl LayerMetadata {
    pub(crate) fn from_digest(
        digest: impl Into<String>,
        size: Option<u64>,
        source_url_hash: Option<String>,
    ) -> Self {
        let digest = digest.into().to_ascii_lowercase();
        let canonical = CanonicalBlobIdentity::from_digest(&digest);
        Self {
            protocol: PROTOCOL.to_string(),
            canonical_key: canonical.key,
            digest: Some(digest),
            uuid: None,
            size,
            source_url_hash,
        }
    }

    pub(crate) fn from_uuid(uuid: Uuid, size: Option<u64>) -> Self {
        let canonical = CanonicalBlobIdentity::from_uuid(&uuid);
        Self {
            protocol: PROTOCOL.to_string(),
            canonical_key: canonical.key,
            digest: None,
            uuid: Some(uuid.to_string()),
            size,
            source_url_hash: None,
        }
    }

    pub(super) fn from_descriptor(descriptor: &P2pArtifactDescriptor) -> Result<Self> {
        serde_json::from_value(descriptor.metadata.clone()).context("parse layer metadata")
    }

    pub(super) fn validate(&self, canonical: &CanonicalBlobIdentity) -> Result<()> {
        if self.protocol != PROTOCOL {
            bail!("unexpected protocol {}", self.protocol);
        }
        if self.canonical_key != canonical.key {
            bail!("canonical key mismatch");
        }
        if let (Some(expected), Some(actual)) =
            (canonical.digest.as_deref(), self.digest.as_deref())
        {
            if expected != actual {
                bail!("digest mismatch");
            }
        }
        if let Some(expected) = canonical.key.strip_prefix("uuid/") {
            match self.uuid.as_deref() {
                Some(actual) if actual == expected => {}
                _ => bail!("uuid mismatch"),
            }
        }
        Ok(())
    }

    pub(super) fn size(&self) -> Option<u64> {
        self.size
    }

    #[expect(
        clippy::expect_used,
        reason = "serializing this plain derived struct to JSON cannot fail"
    )]
    pub(crate) fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("layer metadata serialization should not fail")
    }
}

pub(crate) fn layer_key_from_digest(digest: &str) -> P2pArtifactKey {
    layer_artifact_key(&CanonicalBlobIdentity::from_digest(digest))
}

pub(crate) fn layer_key_from_uuid(uuid: &Uuid) -> P2pArtifactKey {
    layer_artifact_key(&CanonicalBlobIdentity::from_uuid(uuid))
}

pub(super) fn layer_artifact_key(canonical: &CanonicalBlobIdentity) -> P2pArtifactKey {
    if let Some(uuid) = canonical.key.strip_prefix("uuid/") {
        return format!("{KEY_PREFIX}/uuid/{uuid}");
    }
    match canonical.digest.as_deref() {
        Some(digest) => format!("{KEY_PREFIX}/{digest}"),
        None => format!(
            "{KEY_PREFIX}/url/{}",
            crate::digest::sha256_hex(canonical.key.as_bytes())
        ),
    }
}

pub(super) fn extract_sha256_digest(path: &str) -> Option<String> {
    let pos = path.find("sha256:")?;
    let candidate = &path[pos + "sha256:".len()..];
    let hex: String = candidate
        .chars()
        .take_while(|ch| ch.is_ascii_hexdigit())
        .take(SHA256_HEX_LEN)
        .collect();
    if hex.len() != SHA256_HEX_LEN {
        return None;
    }
    let boundary = candidate[hex.len()..]
        .chars()
        .next()
        .is_none_or(|ch| matches!(ch, '/' | '?' | '&' | '#' | ':' | '@' | ',' | ';'));
    boundary.then(|| format!("sha256:{}", hex.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_key_ignores_presigned_query_and_prefers_digest() {
        let url_a = "https://example.com/v2/ns/repo/blobs/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?X-Amz-Signature=one";
        let url_b = "https://example.com/v2/ns/repo/blobs/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?X-Amz-Signature=two";
        assert_eq!(
            CanonicalBlobIdentity::from_origin_url(url_a).unwrap(),
            CanonicalBlobIdentity::from_origin_url(url_b).unwrap()
        );
        assert_eq!(
            layer_artifact_key(&CanonicalBlobIdentity::from_origin_url(url_a).unwrap()),
            "overlaybd-layer/v1/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn canonical_key_falls_back_to_scheme_host_path_without_query() {
        let url_a = "https://example.com:8443/signed/object/path?sig=one";
        let url_b = "https://example.com:8443/signed/object/path?sig=two";
        assert_eq!(
            CanonicalBlobIdentity::from_origin_url(url_a).unwrap(),
            CanonicalBlobIdentity::from_origin_url(url_b).unwrap()
        );
        assert_eq!(
            CanonicalBlobIdentity::from_origin_url(url_a).unwrap().key,
            "url/https://example.com:8443/signed/object/path"
        );
    }

    #[test]
    fn canonical_key_requires_digest_boundary() {
        let url = "https://example.com/v2/ns/repo/blobs/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaff";
        let identity = CanonicalBlobIdentity::from_origin_url(url).unwrap();
        assert!(identity.digest.is_none());
        assert_eq!(
            identity.key,
            "url/https://example.com/v2/ns/repo/blobs/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaff"
        );
    }

    #[test]
    fn uuid_key_does_not_overlap_digest_key() {
        let uuid = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        assert_eq!(
            layer_key_from_uuid(&uuid),
            "overlaybd-layer/v1/uuid/11111111-2222-3333-4444-555555555555"
        );
        assert_eq!(
            layer_key_from_digest(
                "sha256:1111111122223333444455555555555566666666777788889999aaaabbbbcccc"
            ),
            "overlaybd-layer/v1/sha256:1111111122223333444455555555555566666666777788889999aaaabbbbcccc"
        );
    }

    #[test]
    fn uuid_metadata_validates_canonical_uuid() {
        let uuid = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let metadata = LayerMetadata::from_uuid(uuid, Some(1234));
        let canonical = CanonicalBlobIdentity::from_uuid(&uuid);
        metadata.validate(&canonical).expect("uuid should validate");
    }
}
