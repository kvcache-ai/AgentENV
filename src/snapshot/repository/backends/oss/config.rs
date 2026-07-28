use anyhow::{Context, Result};
use object_store_operator::{
    credential_source_from_fields, normalized_credential, CredentialFields, CredentialSource,
    CredentialSourceOptions,
};

use crate::cfg::{OssBackendConfig, SnapshotImageStoragePolicy};

#[derive(Debug, Clone)]
pub(crate) struct NormalizedOssConfig {
    bucket: String,
    endpoint: String,
    region: String,
    prefix: String,
    credential_source: CredentialSource,
    snapshot_image_storage: SnapshotImageStoragePolicy,
}

impl NormalizedOssConfig {
    pub(crate) fn new(
        config: &OssBackendConfig,
        snapshot_image_storage: SnapshotImageStoragePolicy,
    ) -> Result<Self> {
        let bucket = normalized_credential(Some(config.bucket.as_str()))
            .context("backend.oss.bucket cannot be empty")?
            .to_string();
        let endpoint = normalized_credential(Some(config.endpoint.as_str()))
            .context("backend.oss.endpoint cannot be empty")?
            .to_string();
        // Region is only consumed later by overlaybd global-config generation.
        // The runtime backend keeps the validation here so incomplete OSS config
        // still fails fast during backend construction.
        let region = normalized_credential(config.region.as_deref())
            .context("backend.oss.region is required")?
            .to_string();
        let prefix = config
            .prefix
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        let credential_source = credential_source_from_fields(
            CredentialFields {
                access_key_id: config.access_key_id.as_deref(),
                secret_access_key: config.access_key_secret.as_deref(),
                security_token: config.security_token.as_deref(),
                credential_process: config.credential_process.as_deref(),
            },
            CredentialSourceOptions {
                scope: "backend.oss",
                allow_anonymous: false,
                required_access_key_id_label: "backend.oss.access_key_id",
                required_secret_access_key_label: "backend.oss.access_key_secret",
            },
        )?;
        Ok(Self {
            bucket,
            endpoint,
            region,
            prefix,
            credential_source,
            snapshot_image_storage,
        })
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    pub(crate) fn region(&self) -> &str {
        &self.region
    }

    pub(crate) fn credential_source(&self) -> CredentialSource {
        self.credential_source.clone()
    }

    pub(crate) fn snapshot_image_storage(&self) -> SnapshotImageStoragePolicy {
        self.snapshot_image_storage
    }

    pub(crate) fn managed_layers_repo_blob_url(&self) -> String {
        // overlaybd expects an S3-compatible repo blob URL here, including for
        // Alibaba OSS, so the scheme remains `s3://` rather than `oss://`.
        if self.prefix.is_empty() {
            format!("s3://{}/managed-layers", self.bucket)
        } else {
            format!("s3://{}/{}/managed-layers", self.bucket, self.prefix)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::NormalizedOssConfig;
    use crate::cfg::{OssBackendConfig, SnapshotImageStoragePolicy};

    fn sample_config() -> OssBackendConfig {
        OssBackendConfig {
            endpoint: " https://oss-cn-hangzhou.aliyuncs.com ".to_string(),
            bucket: " demo-bucket ".to_string(),
            prefix: Some(" /snapshots/managed/ ".to_string()),
            credential_process: None,
            access_key_id: Some(" ak ".to_string()),
            access_key_secret: Some(" sk ".to_string()),
            security_token: Some(" token ".to_string()),
            region: Some(" cn-hangzhou ".to_string()),
            cache_max_size_gb: Some(8),
        }
    }

    #[test]
    fn normalized_config_trims_and_derives_shared_values() {
        let normalized =
            NormalizedOssConfig::new(&sample_config(), SnapshotImageStoragePolicy::ObjectStorage)
                .expect("normalize config");

        assert_eq!(normalized.bucket(), "demo-bucket");
        assert_eq!(
            normalized.endpoint(),
            "https://oss-cn-hangzhou.aliyuncs.com"
        );
        assert_eq!(normalized.prefix(), "snapshots/managed");
        assert_eq!(
            normalized.managed_layers_repo_blob_url(),
            "s3://demo-bucket/snapshots/managed/managed-layers"
        );
        assert_eq!(
            normalized.snapshot_image_storage(),
            crate::cfg::SnapshotImageStoragePolicy::ObjectStorage
        );
    }

    #[test]
    fn normalized_config_accepts_snapshot_image_publish_policy() {
        let normalized =
            NormalizedOssConfig::new(&sample_config(), SnapshotImageStoragePolicy::SourceRegistry)
                .expect("normalize config");

        assert_eq!(
            normalized.snapshot_image_storage(),
            crate::cfg::SnapshotImageStoragePolicy::SourceRegistry
        );
    }

    #[test]
    fn normalized_config_rejects_mixed_credential_sources() {
        let mut config = sample_config();
        config.credential_process = Some("echo creds".to_string());

        let err = NormalizedOssConfig::new(&config, SnapshotImageStoragePolicy::ObjectStorage)
            .expect_err("mixed credentials must fail");
        assert!(err
            .to_string()
            .contains("credential_process cannot be combined"));
    }

    #[test]
    fn normalized_config_requires_region() {
        let mut config = sample_config();
        config.region = Some(" ".to_string());

        let err = NormalizedOssConfig::new(&config, SnapshotImageStoragePolicy::ObjectStorage)
            .expect_err("missing region must fail");
        assert!(err.to_string().contains("backend.oss.region is required"));
    }
}
