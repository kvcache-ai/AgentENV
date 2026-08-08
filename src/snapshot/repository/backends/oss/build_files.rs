use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;

use super::client::OssClient;
use crate::snapshot::repository::build_files::{
    generate_upload_token, is_valid_build_files_hash, is_valid_upload_token,
    TemplateBuildFileStore, TemplateBuildUploadGrant,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};

const BUILD_FILES_PREFIX: &str = "template-build-files";

/// Build-context archive store backed by the OSS repository bucket.
///
/// Layout: `template-build-files/{hash}.tar` plus durable bearer grants under
/// `template-build-files/upload-grants/`. Operators must configure a bucket
/// lifecycle expiration rule for this prefix; AgentENV does not provision or
/// validate one. A seven-day expiration matches the POSIX cache retention and
/// safely covers the maximum supported upload-link TTL. Archives are cache
/// entries the SDK re-uploads when absent.
pub(crate) struct OssTemplateBuildFileStore {
    client: Arc<OssClient>,
}

impl OssTemplateBuildFileStore {
    pub(crate) fn new(client: Arc<OssClient>) -> Arc<Self> {
        Arc::new(Self { client })
    }

    fn archive_key(hash: &str) -> RepositoryResult<String> {
        if !is_valid_build_files_hash(hash) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("invalid build files hash '{hash}'"),
            });
        }
        Ok(format!("{BUILD_FILES_PREFIX}/{hash}.tar"))
    }

    fn grant_key(token: &str) -> Option<String> {
        is_valid_upload_token(token)
            .then(|| format!("{BUILD_FILES_PREFIX}/upload-grants/{token}.json"))
    }

    /// Reads a grant record, mapping an absent object to `None`.
    async fn read_grant(&self, key: &str) -> RepositoryResult<Option<TemplateBuildUploadGrant>> {
        let bytes = match self.client.get_bytes(key).await {
            Ok(bytes) => bytes,
            Err(error) if OssClient::is_not_found_error(&error) => return Ok(None),
            Err(error) => return Err(RepositoryError::backend("read upload grant", error)),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| RepositoryError::backend("parse upload grant", error))
    }
}

#[async_trait]
impl TemplateBuildFileStore for OssTemplateBuildFileStore {
    async fn exists(&self, hash: &str) -> RepositoryResult<bool> {
        let key = Self::archive_key(hash)?;
        self.client
            .exists(&key)
            .await
            .map_err(|error| RepositoryError::backend("check build archive", error))
    }

    async fn import(&self, hash: &str, staged: &Path) -> RepositoryResult<()> {
        let key = Self::archive_key(hash)?;
        // This is a bandwidth-saving fast path, not an atomic check-and-create:
        // concurrent uploads may both complete. Each object publication is
        // atomic, and the upload protocol treats repeated uploads for one hash
        // as equivalent build input.
        if self
            .client
            .exists(&key)
            .await
            .map_err(|error| RepositoryError::backend("check build archive", error))?
        {
            return Ok(());
        }
        self.client
            .put_file(&key, staged)
            .await
            .map_err(|error| RepositoryError::backend("upload build archive", error))
    }

    async fn materialize(
        &self,
        hash: &str,
        scratch_dir: &Path,
    ) -> RepositoryResult<Option<PathBuf>> {
        let key = Self::archive_key(hash)?;
        let dest = scratch_dir.join(format!("{hash}.tar"));
        match self.client.get_to_file(&key, &dest).await {
            Ok(_) => Ok(Some(dest)),
            Err(error) if OssClient::is_not_found_error(&error) => Ok(None),
            Err(error) => Err(RepositoryError::backend("download build archive", error)),
        }
    }

    async fn create_upload_grant(
        &self,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
    ) -> RepositoryResult<String> {
        let token = generate_upload_token();
        let key = Self::grant_key(&token).expect("generated token is valid");
        let grant = serde_json::to_vec(&TemplateBuildUploadGrant::new(
            template_id,
            hash,
            expires_unix,
        ))
        .map_err(|error| RepositoryError::backend("serialize upload grant", error))?;
        self.client
            .put_bytes(&key, grant)
            .await
            .map_err(|error| RepositoryError::backend("write upload grant", error))?;
        Ok(token)
    }

    async fn verify_upload_grant(
        &self,
        token: &str,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
        now_unix: i64,
    ) -> RepositoryResult<bool> {
        let Some(key) = Self::grant_key(token) else {
            return Ok(false);
        };
        // Deliberately does not delete the object: verification must leave the
        // upload URL usable for a retry.
        let Some(grant) = self.read_grant(&key).await? else {
            return Ok(false);
        };
        Ok(grant.authorizes(template_id, hash, expires_unix, now_unix))
    }

    async fn claim_upload_grant(
        &self,
        token: &str,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
        now_unix: i64,
    ) -> RepositoryResult<bool> {
        let Some(key) = Self::grant_key(token) else {
            return Ok(false);
        };
        let Some(grant) = self.read_grant(&key).await? else {
            return Ok(false);
        };
        if !grant.authorizes(template_id, hash, expires_unix, now_unix) {
            return Ok(false);
        }
        // Consume the grant so the upload URL cannot normally be replayed.
        // S3-compatible stores offer no conditional delete, so simultaneous
        // replays of one token can both observe the grant and publish the same
        // logical build input.
        self.client
            .delete(&key)
            .await
            .map_err(|error| RepositoryError::backend("consume upload grant", error))?;
        Ok(true)
    }
}
