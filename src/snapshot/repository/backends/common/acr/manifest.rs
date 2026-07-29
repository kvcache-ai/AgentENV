use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::digest;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};

pub(crate) const OCI_IMAGE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
pub(crate) const OCI_IMAGE_CONFIG_MEDIA_TYPE: &str = "application/vnd.oci.image.config.v1+json";
pub(crate) const OCI_TAR_LAYER_MEDIA_TYPE: &str = "application/vnd.oci.image.layer.v1.tar";
const OVERLAYBD_BLOB_DIGEST_ANNOTATION: &str = "containerd.io/snapshot/overlaybd/blob-digest";
const OVERLAYBD_BLOB_SIZE_ANNOTATION: &str = "containerd.io/snapshot/overlaybd/blob-size";
const SNAPSHOT_TAG_ANNOTATION: &str = "io.agentenv.snapshot.tag";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OciDescriptor {
    pub(crate) media_type: String,
    pub(crate) digest: String,
    pub(crate) size: u64,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub(crate) annotations: BTreeMap<String, String>,
}

impl OciDescriptor {
    pub(crate) fn overlaybd_layer(digest: String, size: u64) -> Self {
        let mut annotations = BTreeMap::new();
        annotations.insert(OVERLAYBD_BLOB_DIGEST_ANNOTATION.to_string(), digest.clone());
        annotations.insert(OVERLAYBD_BLOB_SIZE_ANNOTATION.to_string(), size.to_string());
        Self {
            media_type: OCI_TAR_LAYER_MEDIA_TYPE.to_string(),
            digest,
            size,
            annotations,
        }
    }

    pub(crate) fn config(digest: String, size: u64) -> Self {
        Self {
            media_type: OCI_IMAGE_CONFIG_MEDIA_TYPE.to_string(),
            digest,
            size,
            annotations: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciManifest {
    schema_version: u32,
    media_type: String,
    config: OciDescriptor,
    layers: Vec<OciDescriptor>,
    annotations: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct MinimalOciConfig<'a> {
    created: &'a str,
    architecture: &'a str,
    os: &'a str,
    config: MinimalRuntimeConfig,
    rootfs: MinimalRootfs,
    history: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct MinimalRuntimeConfig {
    env: Vec<String>,
    working_dir: String,
}

#[derive(Debug, Serialize)]
struct MinimalRootfs {
    #[serde(rename = "type")]
    rootfs_type: &'static str,
    diff_ids: Vec<String>,
}

pub(crate) fn minimal_oci_config_blob(
    architecture: &str,
) -> RepositoryResult<(Vec<u8>, String, u64)> {
    let config = MinimalOciConfig {
        created: "1970-01-01T00:00:00Z",
        architecture,
        os: "linux",
        config: MinimalRuntimeConfig {
            env: Vec::new(),
            working_dir: String::new(),
        },
        rootfs: MinimalRootfs {
            rootfs_type: "layers",
            // OverlayBD snapshot layers are not ordinary uncompressed OCI tar
            // diffs, so this intentionally stays empty for AgentENV-only use.
            diff_ids: Vec::new(),
        },
        history: Vec::new(),
    };
    let bytes = serde_json::to_vec(&config)
        .map_err(|e| RepositoryError::backend("serialize minimal OCI config", e))?;
    let digest = digest::sha256_digest(&bytes);
    let size = bytes.len() as u64;
    Ok((bytes, digest, size))
}

pub(crate) fn build_oci_image_manifest(
    config: OciDescriptor,
    layers: Vec<OciDescriptor>,
    publication_tag: &str,
) -> RepositoryResult<Vec<u8>> {
    let annotations = BTreeMap::from([(
        SNAPSHOT_TAG_ANNOTATION.to_string(),
        publication_tag.to_string(),
    )]);
    let manifest = OciManifest {
        schema_version: 2,
        media_type: OCI_IMAGE_MANIFEST_MEDIA_TYPE.to_string(),
        config,
        layers,
        annotations,
    };
    serde_json::to_vec(&manifest)
        .map_err(|e| RepositoryError::backend("serialize OCI image manifest", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_config_has_documented_empty_diff_ids() {
        let (bytes, digest, size) = minimal_oci_config_blob("amd64").unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(value["architecture"], "amd64");
        assert_eq!(value["os"], "linux");
        assert_eq!(value["rootfs"]["type"], "layers");
        assert_eq!(value["rootfs"]["diff_ids"].as_array().unwrap().len(), 0);
        assert_eq!(size, bytes.len() as u64);
        assert_eq!(digest, crate::digest::sha256_digest(&bytes));
    }

    #[test]
    fn manifest_uses_self_referential_overlaybd_annotations() {
        let layer = OciDescriptor::overlaybd_layer("sha256:abc".to_string(), 123);
        let manifest = build_oci_image_manifest(
            OciDescriptor::config("sha256:config".to_string(), 2),
            vec![layer],
            "agentenv-snapshot-s1",
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&manifest).unwrap();

        assert_eq!(
            value["mediaType"],
            "application/vnd.oci.image.manifest.v1+json"
        );
        assert_eq!(
            value["layers"][0]["mediaType"],
            "application/vnd.oci.image.layer.v1.tar"
        );
        assert_eq!(
            value["layers"][0]["annotations"]["containerd.io/snapshot/overlaybd/blob-digest"],
            "sha256:abc"
        );
        assert_eq!(
            value["layers"][0]["annotations"]["containerd.io/snapshot/overlaybd/blob-size"],
            "123"
        );
        assert_eq!(
            value["annotations"]["io.agentenv.snapshot.tag"],
            "agentenv-snapshot-s1"
        );
    }

    #[test]
    fn publication_tag_makes_manifest_digest_unique() {
        let config = OciDescriptor::config("sha256:config".to_string(), 2);
        let layers = vec![OciDescriptor::overlaybd_layer(
            "sha256:abc".to_string(),
            123,
        )];

        let first =
            build_oci_image_manifest(config.clone(), layers.clone(), "agentenv-snapshot-s1")
                .unwrap();
        let second = build_oci_image_manifest(config, layers, "agentenv-snapshot-s2").unwrap();

        assert_ne!(
            crate::digest::sha256_digest(&first),
            crate::digest::sha256_digest(&second)
        );
    }
}
