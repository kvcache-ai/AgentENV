use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::digest;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::CommandContext;

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

/// Effective runtime metadata plus the optional source rootfs OCI config.
#[derive(Clone, Copy)]
pub(crate) struct SnapshotOciConfigInput<'a> {
    context: &'a CommandContext,
    raw_config: Option<&'a Value>,
}

impl<'a> SnapshotOciConfigInput<'a> {
    pub(crate) fn new(context: &'a CommandContext, raw_config: Option<&'a Value>) -> Self {
        Self {
            context,
            raw_config,
        }
    }
}

#[derive(Debug, Serialize)]
struct OciConfig<'a> {
    created: &'a str,
    architecture: &'a str,
    os: &'a str,
    config: Value,
    rootfs: MinimalRootfs,
    history: Vec<Value>,
}

#[derive(Debug, Serialize)]
struct MinimalRootfs {
    #[serde(rename = "type")]
    rootfs_type: &'static str,
    // OverlayBD snapshot layers are not ordinary uncompressed OCI tar diffs,
    // so this intentionally stays empty for AgentENV-only use.
    diff_ids: Vec<String>,
}

fn oci_config_blob(
    architecture: &str,
    config: Value,
    operation: &'static str,
) -> RepositoryResult<(Vec<u8>, String, u64)> {
    let config = OciConfig {
        created: "1970-01-01T00:00:00Z",
        architecture,
        os: "linux",
        config,
        rootfs: MinimalRootfs {
            rootfs_type: "layers",
            diff_ids: Vec::new(),
        },
        history: Vec::new(),
    };
    let bytes =
        serde_json::to_vec(&config).map_err(|error| RepositoryError::backend(operation, error))?;
    let digest = digest::sha256_digest(&bytes);
    let size = bytes.len() as u64;
    Ok((bytes, digest, size))
}

fn set_optional(config: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        config.insert(key.to_string(), value);
    } else {
        config.remove(key);
    }
}

fn empty_object_map(values: &[String]) -> Value {
    Value::Object(
        values
            .iter()
            .map(|value| (value.clone(), Value::Object(Map::new())))
            .collect(),
    )
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize_json).collect()),
        Value::Object(values) => {
            let mut entries: Vec<_> = values.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize_json(value)))
                    .collect(),
            )
        }
        value => value,
    }
}

fn merged_runtime_config(input: SnapshotOciConfigInput<'_>) -> Value {
    let mut config = input
        .raw_config
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let context = input.context;

    let mut env: Vec<_> = context
        .env_vars
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    env.sort();
    config.insert("Env".to_string(), serde_json::json!(env));
    config.insert(
        "WorkingDir".to_string(),
        Value::String(context.workdir.clone()),
    );
    set_optional(&mut config, "User", context.user.clone().map(Value::String));
    set_optional(
        &mut config,
        "Entrypoint",
        context
            .entrypoint
            .clone()
            .map(|value| serde_json::json!(value)),
    );
    set_optional(
        &mut config,
        "Cmd",
        context.cmd.clone().map(|value| serde_json::json!(value)),
    );
    set_optional(
        &mut config,
        "ExposedPorts",
        (!context.exposed_ports.is_empty()).then(|| empty_object_map(&context.exposed_ports)),
    );
    set_optional(
        &mut config,
        "Volumes",
        (!context.volumes.is_empty()).then(|| empty_object_map(&context.volumes)),
    );
    set_optional(
        &mut config,
        "Labels",
        (!context.labels.is_empty()).then(|| serde_json::json!(context.labels)),
    );

    canonicalize_json(Value::Object(config))
}

pub(crate) fn snapshot_oci_config_blob(
    architecture: &str,
    input: SnapshotOciConfigInput<'_>,
) -> RepositoryResult<(Vec<u8>, String, u64)> {
    oci_config_blob(
        architecture,
        merged_runtime_config(input),
        "serialize snapshot OCI config",
    )
}

pub(crate) fn minimal_oci_config_blob(
    architecture: &str,
) -> RepositoryResult<(Vec<u8>, String, u64)> {
    oci_config_blob(
        architecture,
        serde_json::json!({"Env": [], "WorkingDir": ""}),
        "serialize minimal OCI config",
    )
}

/// OCI architecture string of the host running this binary.
pub(crate) fn host_architecture_for_oci() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "arm" => "arm",
        "riscv64" => "riscv64",
        other => other,
    }
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
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;
    use crate::snapshot::CommandContext;

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
    fn snapshot_config_merges_context_and_raw_config_deterministically() {
        let raw = json!({
            "Env": ["STALE=1"],
            "WorkingDir": "/old",
            "User": "root",
            "Entrypoint": ["/old-entrypoint"],
            "Cmd": ["old"],
            "ExposedPorts": {"80/tcp": {}},
            "Volumes": {"/old-data": {}},
            "Labels": {"old": "label"},
            "StopSignal": "SIGTERM",
            "Healthcheck": {"Test": ["CMD", "true"]}
        });
        let cases = [
            (
                CommandContext::new(
                    HashMap::from([
                        ("B".to_string(), "two".to_string()),
                        ("A".to_string(), "one".to_string()),
                    ]),
                    "/workspace",
                )
                .with_user(Some("1000:1000".to_string()))
                .with_exposed_ports(vec!["8080/tcp".to_string()])
                .with_entrypoint(Some(vec!["/app".to_string()]))
                .with_cmd(Some(vec!["serve".to_string()]))
                .with_volumes(vec!["/data".to_string()])
                .with_labels(HashMap::from([(
                    "org.example.name".to_string(),
                    "snapshot".to_string(),
                )])),
                json!({
                    "Env": ["A=one", "B=two"],
                    "WorkingDir": "/workspace",
                    "User": "1000:1000",
                    "Entrypoint": ["/app"],
                    "Cmd": ["serve"],
                    "ExposedPorts": {"8080/tcp": {}},
                    "Volumes": {"/data": {}},
                    "Labels": {"org.example.name": "snapshot"},
                    "StopSignal": "SIGTERM",
                    "Healthcheck": {"Test": ["CMD", "true"]}
                }),
            ),
            (
                CommandContext::default(),
                json!({
                    "Env": [],
                    "WorkingDir": "/",
                    "StopSignal": "SIGTERM",
                    "Healthcheck": {"Test": ["CMD", "true"]}
                }),
            ),
        ];

        for (context, expected_config) in cases {
            let (bytes, _, _) = snapshot_oci_config_blob(
                "amd64",
                SnapshotOciConfigInput::new(&context, Some(&raw)),
            )
            .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["config"], expected_config);
            assert_eq!(value["rootfs"]["diff_ids"], json!([]));
        }
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
