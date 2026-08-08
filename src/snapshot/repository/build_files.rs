use std::path::{Path, PathBuf};

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use super::errors::RepositoryResult;

/// Number of random bytes in an upload bearer token.
pub const UPLOAD_TOKEN_LEN: usize = 32;

/// Durable authorization record for one build-context upload URL.
///
/// Grants live in the same shared repository as build archives. That makes a
/// URL issued by one node verifiable by any other node without coordinating a
/// deployment-wide in-memory signing secret.
#[derive(Debug, Deserialize, Serialize)]
pub struct TemplateBuildUploadGrant {
    pub template_id: String,
    pub hash: String,
    pub expires_unix: i64,
}

impl TemplateBuildUploadGrant {
    pub fn new(template_id: &str, hash: &str, expires_unix: i64) -> Self {
        Self {
            template_id: template_id.to_string(),
            hash: hash.to_string(),
            expires_unix,
        }
    }

    pub fn authorizes(
        &self,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
        now_unix: i64,
    ) -> bool {
        now_unix < expires_unix
            && self.expires_unix == expires_unix
            && self.template_id == template_id
            && self.hash == hash
    }
}

/// Durable store for template build-context archives.
///
/// The E2B SDK resolves every `COPY` step through
/// `GET /templates/{templateID}/files/{hash}` and then `PUT`s a tar archive of
/// the matching context files to the returned URL. This store owns those
/// archives, addressed by the SDK-computed content hash, so that:
///
/// - any node can answer the upload-link request (`exists`),
/// - any node can accept the upload (`import`), and
/// - the node that runs the build can read the archive back (`materialize`).
///
/// Implementations must place the archives in storage shared by all nodes of
/// the deployment, mirroring the visibility rules of committed snapshots.
#[async_trait]
pub trait TemplateBuildFileStore: Send + Sync {
    /// Returns whether an archive for `hash` is already stored.
    async fn exists(&self, hash: &str) -> RepositoryResult<bool>;

    /// Imports a fully written local file as the archive for `hash`.
    ///
    /// Implementations must publish each upload atomically: concurrent readers
    /// may observe either the previous complete archive or the newly imported
    /// complete archive, but never a partially imported archive. `hash` is the
    /// SDK-supplied cache key rather than a digest the store verifies, so the
    /// protocol assumes repeated uploads for one hash describe equivalent
    /// build input; the store does not provide content authenticity.
    async fn import(&self, hash: &str, staged: &Path) -> RepositoryResult<()>;

    /// Materializes the archive for `hash` as a node-local file.
    ///
    /// `scratch_dir` is a caller-owned directory the implementation may use
    /// for downloads; implementations backed by a shared filesystem may return
    /// the shared path directly. Callers must treat the returned file as
    /// read-only. Returns `None` when no archive is stored for `hash`.
    async fn materialize(
        &self,
        hash: &str,
        scratch_dir: &Path,
    ) -> RepositoryResult<Option<PathBuf>>;

    /// Creates a durable bearer grant for one upload URL and returns its
    /// URL-safe token.
    async fn create_upload_grant(
        &self,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
    ) -> RepositoryResult<String>;

    /// Verifies a durable bearer grant without consuming it, returning
    /// whether it authorizes this upload.
    ///
    /// Verification never removes the grant, so a request that fails before
    /// the archive is stored can be retried with the same upload URL. Callers
    /// must `claim_upload_grant` after publishing the archive, so a failed
    /// publication leaves the URL retryable.
    async fn verify_upload_grant(
        &self,
        token: &str,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
        now_unix: i64,
    ) -> RepositoryResult<bool>;

    /// Claims a durable bearer grant, returning whether it authorized this
    /// upload.
    ///
    /// A successful claim consumes the grant, so later requests cannot reuse
    /// it. Implementations must make the claim itself atomic wherever the
    /// backend offers an atomic primitive (a POSIX filesystem does, via
    /// unlink), so concurrent requests carrying the same token cannot both
    /// report a successful claim.
    ///
    /// Claiming is deliberately not a pre-publication reservation: callers
    /// verify first so failed staging or publication remains retryable, then
    /// claim after publication. Simultaneous holders of the same bearer token
    /// can therefore race publication before one claim succeeds. Possession of
    /// the token already authorizes publication for its bound
    /// (template_id, hash); this protocol does not add content authenticity,
    /// and repeated uploads for one hash must describe equivalent build input.
    ///
    /// S3-compatible backends have no conditional delete and therefore
    /// degrade the claim itself to best-effort single-use within the grant TTL.
    async fn claim_upload_grant(
        &self,
        token: &str,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
        now_unix: i64,
    ) -> RepositoryResult<bool>;
}

/// Returns whether `hash` is acceptable as a build-file content hash.
///
/// The E2B SDK sends a lowercase hex SHA-256, but the value is treated as an
/// opaque cache key; this only enforces a path- and URL-safe shape.
pub fn is_valid_build_files_hash(hash: &str) -> bool {
    (16..=128).contains(&hash.len()) && hash.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Generates a cryptographically random URL-safe upload bearer token.
pub fn generate_upload_token() -> String {
    let mut token = [0u8; UPLOAD_TOKEN_LEN];
    rand::fill(&mut token);
    URL_SAFE_NO_PAD.encode(token)
}

/// Returns whether `token` has the exact shape generated for upload grants.
pub fn is_valid_upload_token(token: &str) -> bool {
    URL_SAFE_NO_PAD
        .decode(token)
        .is_ok_and(|decoded| decoded.len() == UPLOAD_TOKEN_LEN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_validation_accepts_sha256_hex() {
        assert!(is_valid_build_files_hash(
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        ));
        assert!(is_valid_build_files_hash("ABCDEF0123456789"));
    }

    #[test]
    fn hash_validation_rejects_path_unsafe_values() {
        assert!(!is_valid_build_files_hash(""));
        assert!(!is_valid_build_files_hash("short"));
        assert!(!is_valid_build_files_hash("../../../../etc/passwd"));
        assert!(!is_valid_build_files_hash("deadbeef/deadbeef"));
        assert!(!is_valid_build_files_hash(&"a".repeat(129)));
    }

    #[test]
    fn upload_token_has_expected_shape() {
        let token = generate_upload_token();
        assert!(is_valid_upload_token(&token));
        assert!(!is_valid_upload_token("not-a-valid-token"));
    }

    #[test]
    fn upload_grant_is_bound_to_request_and_expiry() {
        let grant = TemplateBuildUploadGrant::new("tmpl", "aabbccddeeff0011", 1000);
        assert!(grant.authorizes("tmpl", "aabbccddeeff0011", 1000, 999));
        assert!(!grant.authorizes("tmpl", "aabbccddeeff0011", 1000, 1000));
        assert!(!grant.authorizes("tmpl", "aabbccddeeff0011", 1000, 1001));
        assert!(!grant.authorizes("other", "aabbccddeeff0011", 1000, 999));
        assert!(!grant.authorizes("tmpl", "aabbccddeeff0012", 1000, 999));
        assert!(!grant.authorizes("tmpl", "aabbccddeeff0011", 2000, 999));
    }
}
