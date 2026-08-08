use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use tokio::task;
use tracing::{debug, warn};

use crate::snapshot::repository::build_files::{
    generate_upload_token, is_valid_build_files_hash, is_valid_upload_token,
    TemplateBuildFileStore, TemplateBuildUploadGrant,
};
use crate::snapshot::repository::{RepositoryError, RepositoryResult};

/// How long imported build-context archives and upload grants are retained.
/// Archives are cache entries keyed by content hash; the SDK re-uploads any
/// archive that has been pruned, so expiry only costs one extra upload.
/// Grants expire after `template_build.files_url_ttl_secs` anyway, so this
/// only bounds how long the spent grant files linger on disk.
const BUILD_FILE_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

const GRANTS_DIR_NAME: &str = "upload-grants";

/// Build-context archive store rooted on the shared POSIX repository.
///
/// Layout: `{repository_root}/template-build-files/{hash}.tar` plus durable
/// upload grants under `upload-grants/`. Both live on the shared filesystem,
/// so every node observes the same archives and verifies the same upload URLs.
pub(crate) struct PosixFsTemplateBuildFileStore {
    root: PathBuf,
}

impl PosixFsTemplateBuildFileStore {
    pub(crate) fn new(repository_root: &Path) -> Arc<Self> {
        Arc::new(Self {
            root: repository_root.join("template-build-files"),
        })
    }

    fn archive_path(&self, hash: &str) -> RepositoryResult<PathBuf> {
        if !is_valid_build_files_hash(hash) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("invalid build files hash '{hash}'"),
            });
        }
        Ok(self.root.join(format!("{hash}.tar")))
    }

    fn ensure_root(root: &Path) -> RepositoryResult<()> {
        fs::create_dir_all(root).map_err(|error| {
            RepositoryError::backend(
                format!("create template build files dir '{}'", root.display()),
                error,
            )
        })
    }

    /// Removes archives whose modification time is older than the retention
    /// window. Runs opportunistically on import and scans a bounded number of
    /// entries per call; failures only log.
    fn prune_expired(root: &Path) {
        let cutoff = SystemTime::now() - BUILD_FILE_RETENTION;
        Self::prune_dir_older_than(root, "tar", cutoff);
    }

    /// Removes upload grants that have passed their own `expires_unix`. Runs
    /// opportunistically whenever a new grant is written, so the grants
    /// directory stays bounded by upload-link traffic; the scan is bounded per
    /// call and drains the backlog over successive requests, and failures only
    /// log.
    ///
    /// Pruning by the record rather than by mtime keeps grants alive for
    /// exactly their TTL even when `template_build.files_url_ttl_secs` is
    /// configured beyond the retention window.
    fn prune_expired_grants(root: &Path) {
        let cutoff = SystemTime::now() - BUILD_FILE_RETENTION;
        let now_unix = chrono::Utc::now().timestamp();
        Self::prune_dir(&Self::grants_dir(root), "json", |path, modified| {
            match fs::read(path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<TemplateBuildUploadGrant>(&bytes).ok())
            {
                Some(grant) => grant.expires_unix < now_unix,
                // Unparseable leftovers fall back to the mtime rule.
                None => modified.is_some_and(|modified| modified < cutoff),
            }
        });
    }

    fn prune_dir_older_than(dir: &Path, extension: &str, cutoff: SystemTime) {
        Self::prune_dir(dir, extension, |_, modified| {
            modified.is_some_and(|modified| modified < cutoff)
        });
    }

    /// Pruning is opportunistic and bounded: at most `MAX_PRUNE_SCAN` matching
    /// entries are inspected per call, so the cost a request pays stays
    /// constant no matter how many records the directory holds. Anything left
    /// over is reclaimed by later calls.
    fn prune_dir(
        dir: &Path,
        extension: &str,
        is_expired: impl Fn(&Path, Option<SystemTime>) -> bool,
    ) {
        const MAX_PRUNE_SCAN: usize = 256;

        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        let mut scanned: usize = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != extension) {
                continue;
            }
            if scanned >= MAX_PRUNE_SCAN {
                break;
            }
            scanned += 1;
            let modified = entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok();
            if is_expired(&path, modified) {
                if let Err(error) = fs::remove_file(&path) {
                    warn!(
                        path = %path.display(),
                        error = %error,
                        "failed to prune expired template build file"
                    );
                } else {
                    debug!(path = %path.display(), "pruned expired template build file");
                }
            }
        }
    }

    fn grants_dir(root: &Path) -> PathBuf {
        root.join(GRANTS_DIR_NAME)
    }

    fn grant_path(root: &Path, token: &str) -> Option<PathBuf> {
        is_valid_upload_token(token).then(|| Self::grants_dir(root).join(format!("{token}.json")))
    }

    /// Reads a grant record, mapping an absent file to `None`.
    fn read_grant(path: &Path) -> RepositoryResult<Option<TemplateBuildUploadGrant>> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(RepositoryError::backend("read upload grant", error)),
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| RepositoryError::backend("parse upload grant", error))
    }

    /// Best-effort mtime refresh, so retention means "unused for the window"
    /// and an archive a build is still reading stays outside the prune
    /// horizon. Read-only repository mounts must keep working, so failures
    /// only log.
    fn touch(path: &Path) {
        let refreshed = fs::File::options()
            .write(true)
            .open(path)
            .and_then(|file| file.set_times(fs::FileTimes::new().set_modified(SystemTime::now())));
        if let Err(error) = refreshed {
            debug!(
                path = %path.display(),
                error = %error,
                "failed to refresh build archive mtime"
            );
        }
    }

    fn write_grant(
        root: &Path,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
    ) -> RepositoryResult<String> {
        let grants_dir = Self::grants_dir(root);
        fs::create_dir_all(&grants_dir).map_err(|error| {
            RepositoryError::backend(
                format!("create upload grants dir '{}'", grants_dir.display()),
                error,
            )
        })?;
        Self::prune_expired_grants(root);
        let bytes = serde_json::to_vec(&TemplateBuildUploadGrant::new(
            template_id,
            hash,
            expires_unix,
        ))
        .map_err(|error| RepositoryError::backend("serialize upload grant", error))?;

        for _ in 0..3 {
            let token = generate_upload_token();
            let path = Self::grant_path(root, &token).expect("generated token is valid");
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    file.write_all(&bytes)
                        .and_then(|()| file.sync_all())
                        .map_err(|error| {
                            let _ = fs::remove_file(&path);
                            RepositoryError::backend("write upload grant", error)
                        })?;
                    return Ok(token);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(RepositoryError::backend("create upload grant", error)),
            }
        }
        Err(RepositoryError::Backend {
            message: "failed to allocate a unique upload grant token".to_string(),
            source: None,
        })
    }
}

#[async_trait]
impl TemplateBuildFileStore for PosixFsTemplateBuildFileStore {
    async fn exists(&self, hash: &str) -> RepositoryResult<bool> {
        let path = self.archive_path(hash)?;
        task::spawn_blocking(move || -> RepositoryResult<bool> {
            match fs::metadata(&path) {
                Ok(_) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(RepositoryError::backend(
                    format!("stat build archive '{}'", path.display()),
                    error,
                )),
            }
        })
        .await
        .map_err(|error| RepositoryError::backend("join build file exists task", error))?
    }

    async fn import(&self, hash: &str, staged: &Path) -> RepositoryResult<()> {
        let final_path = self.archive_path(hash)?;
        let root = self.root.clone();
        let staged = staged.to_path_buf();
        task::spawn_blocking(move || -> RepositoryResult<()> {
            // Archives are immutable: the hash addresses the content, so a
            // repeat upload cannot change what an in-flight build reads.
            if final_path.exists() {
                return Ok(());
            }
            Self::ensure_root(&root)?;
            Self::prune_expired(&root);
            // Copy into the store filesystem first (the staged file usually
            // lives on node-local tmp), then link it into place within the
            // store directory so readers only ever observe complete archives.
            let store_staged = root.join(format!(".import-{}.tmp", uuid::Uuid::new_v4()));
            fs::copy(&staged, &store_staged).map_err(|error| {
                let _ = fs::remove_file(&store_staged);
                RepositoryError::backend("copy build archive into store", error)
            })?;
            // The archive is only ever published once, so its data must reach
            // stable storage before the name does: a directory entry that
            // outlives the bytes would pin a truncated archive forever behind
            // the `exists` fast path.
            fs::File::open(&store_staged)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    let _ = fs::remove_file(&store_staged);
                    RepositoryError::backend("sync build archive", error)
                })?;
            // Link rather than rename so a concurrent import cannot replace an
            // archive a running build is already reading: the first writer
            // wins and everyone else observes `AlreadyExists`.
            let published = match fs::hard_link(&store_staged, &final_path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
                Err(error) => Err(RepositoryError::backend("publish build archive", error)),
            };
            if published.is_ok() {
                // Best effort: filesystems that reject a directory fsync must
                // keep working, and a lost entry only costs one re-upload.
                if let Err(error) = fs::File::open(&root).and_then(|dir| dir.sync_all()) {
                    debug!(
                        path = %root.display(),
                        error = %error,
                        "failed to sync build archive store directory"
                    );
                }
            }
            let _ = fs::remove_file(&store_staged);
            published
        })
        .await
        .map_err(|error| RepositoryError::backend("join build file import task", error))?
    }

    async fn materialize(
        &self,
        hash: &str,
        _scratch_dir: &Path,
    ) -> RepositoryResult<Option<PathBuf>> {
        let path = self.archive_path(hash)?;
        task::spawn_blocking(move || -> RepositoryResult<Option<PathBuf>> {
            match fs::metadata(&path) {
                Ok(_) => {
                    Self::touch(&path);
                    Ok(Some(path))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(RepositoryError::backend(
                    format!("stat build archive '{}'", path.display()),
                    error,
                )),
            }
        })
        .await
        .map_err(|error| RepositoryError::backend("join build file materialize task", error))?
    }

    async fn create_upload_grant(
        &self,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
    ) -> RepositoryResult<String> {
        let root = self.root.clone();
        let template_id = template_id.to_string();
        let hash = hash.to_string();
        task::spawn_blocking(move || Self::write_grant(&root, &template_id, &hash, expires_unix))
            .await
            .map_err(|error| RepositoryError::backend("join create upload grant task", error))?
    }

    async fn verify_upload_grant(
        &self,
        token: &str,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
        now_unix: i64,
    ) -> RepositoryResult<bool> {
        let Some(path) = Self::grant_path(&self.root, token) else {
            return Ok(false);
        };
        let template_id = template_id.to_string();
        let hash = hash.to_string();
        task::spawn_blocking(move || -> RepositoryResult<bool> {
            // Reads only: the grant file must survive so an upload that fails
            // before the archive is stored can be retried with the same URL.
            let Some(grant) = Self::read_grant(&path)? else {
                return Ok(false);
            };
            Ok(grant.authorizes(&template_id, &hash, expires_unix, now_unix))
        })
        .await
        .map_err(|error| RepositoryError::backend("join verify upload grant task", error))?
    }

    async fn claim_upload_grant(
        &self,
        token: &str,
        template_id: &str,
        hash: &str,
        expires_unix: i64,
        now_unix: i64,
    ) -> RepositoryResult<bool> {
        let Some(path) = Self::grant_path(&self.root, token) else {
            return Ok(false);
        };
        let template_id = template_id.to_string();
        let hash = hash.to_string();
        task::spawn_blocking(move || -> RepositoryResult<bool> {
            let Some(grant) = Self::read_grant(&path)? else {
                return Ok(false);
            };
            if !grant.authorizes(&template_id, &hash, expires_unix, now_unix) {
                return Ok(false);
            }
            // Consume the grant. `remove_file` succeeds for exactly one
            // caller, so it is the claim: concurrent replays of the same
            // token lose the race and are rejected.
            match fs::remove_file(&path) {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(RepositoryError::backend("consume upload grant", error)),
            }
        })
        .await
        .map_err(|error| RepositoryError::backend("join claim upload grant task", error))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    fn staged_file(dir: &Path, contents: &[u8]) -> PathBuf {
        let path = dir.join("staged.tar");
        fs::write(&path, contents).expect("write staged file");
        path
    }

    #[tokio::test]
    async fn import_then_exists_and_materialize() {
        let tempdir = TempDir::new().expect("tempdir");
        let store = PosixFsTemplateBuildFileStore::new(tempdir.path());

        assert!(!store.exists(HASH).await.expect("exists should work"));
        assert_eq!(
            store
                .materialize(HASH, tempdir.path())
                .await
                .expect("materialize should work"),
            None
        );

        let staged = staged_file(tempdir.path(), b"tar-bytes");
        store
            .import(HASH, &staged)
            .await
            .expect("import should work");

        assert!(store.exists(HASH).await.expect("exists should work"));
        let materialized = store
            .materialize(HASH, tempdir.path())
            .await
            .expect("materialize should work")
            .expect("archive should exist");
        assert_eq!(
            fs::read(materialized).expect("read materialized"),
            b"tar-bytes"
        );
    }

    #[tokio::test]
    async fn import_rejects_invalid_hash() {
        let tempdir = TempDir::new().expect("tempdir");
        let store = PosixFsTemplateBuildFileStore::new(tempdir.path());
        let staged = staged_file(tempdir.path(), b"tar-bytes");

        let err = store
            .import("../escape", &staged)
            .await
            .expect_err("invalid hash should fail");
        assert!(matches!(err, RepositoryError::InvalidRequest { .. }));
    }

    #[tokio::test]
    async fn writing_a_grant_prunes_expired_grant_files() {
        let tempdir = TempDir::new().expect("tempdir");
        let store = PosixFsTemplateBuildFileStore::new(tempdir.path());

        let fresh_token = store
            .create_upload_grant("template", HASH, i64::MAX)
            .await
            .expect("fresh grant should be created");

        // Plant a grant file that predates the retention window.
        let grants_dir = tempdir
            .path()
            .join("template-build-files")
            .join("upload-grants");
        let stale_path = grants_dir.join(format!("{}.json", generate_upload_token()));
        fs::write(&stale_path, b"{}").expect("write stale grant");
        let stale_mtime = SystemTime::now() - BUILD_FILE_RETENTION - Duration::from_secs(60);
        let stale_file = fs::File::options()
            .write(true)
            .open(&stale_path)
            .expect("open stale grant");
        stale_file
            .set_times(fs::FileTimes::new().set_modified(stale_mtime))
            .expect("set stale mtime");
        drop(stale_file);

        store
            .create_upload_grant("template", HASH, i64::MAX)
            .await
            .expect("new grant should be created");

        assert!(!stale_path.exists(), "expired grant file should be pruned");
        assert!(
            store
                .claim_upload_grant(&fresh_token, "template", HASH, i64::MAX, 0)
                .await
                .expect("validation should work"),
            "unexpired grants must survive pruning"
        );
    }

    #[tokio::test]
    async fn upload_grant_is_shared_across_instances() {
        let tempdir = TempDir::new().expect("tempdir");
        let first = PosixFsTemplateBuildFileStore::new(tempdir.path());
        let second = PosixFsTemplateBuildFileStore::new(tempdir.path());

        // A mismatched or expired claim leaves the grant usable.
        let token = first
            .create_upload_grant("template", HASH, 1000)
            .await
            .expect("grant should be created");
        assert!(!second
            .claim_upload_grant(&token, "other", HASH, 1000, 999)
            .await
            .expect("mismatched grant should be rejected"));
        assert!(!second
            .claim_upload_grant(&token, "template", HASH, 1000, 1001)
            .await
            .expect("expired grant should be rejected"));
        assert!(second
            .claim_upload_grant(&token, "template", HASH, 1000, 999)
            .await
            .expect("grant issued by another instance should claim"));
    }

    #[tokio::test]
    async fn upload_grant_is_single_use() {
        let tempdir = TempDir::new().expect("tempdir");
        let store = PosixFsTemplateBuildFileStore::new(tempdir.path());
        let token = store
            .create_upload_grant("template", HASH, 1000)
            .await
            .expect("grant should be created");

        assert!(store
            .claim_upload_grant(&token, "template", HASH, 1000, 999)
            .await
            .expect("first claim should succeed"));
        assert!(
            !store
                .claim_upload_grant(&token, "template", HASH, 1000, 999)
                .await
                .expect("replay should be rejected"),
            "an upload URL must not be replayable"
        );
    }

    #[tokio::test]
    async fn archives_are_immutable_once_stored() {
        let tempdir = TempDir::new().expect("tempdir");
        let store = PosixFsTemplateBuildFileStore::new(tempdir.path());

        let first = staged_file(tempdir.path(), b"original");
        store.import(HASH, &first).await.expect("first import");

        let replacement = tempdir.path().join("replacement.tar");
        fs::write(&replacement, b"replaced").expect("write replacement");
        store
            .import(HASH, &replacement)
            .await
            .expect("repeat import should be accepted");

        // Two imports racing for a hash neither has stored yet must both
        // succeed; the loser's hard link hits AlreadyExists and is dropped.
        // A fresh hash keeps both calls off the exists() fast path.
        const FRESH_HASH: &str = "f00ff00ff00ff00ff00ff00ff00ff00f";
        let concurrent = tempdir.path().join("concurrent.tar");
        fs::write(&concurrent, b"concurrent").expect("write concurrent");
        let (left, right) = tokio::join!(
            store.import(FRESH_HASH, &replacement),
            store.import(FRESH_HASH, &concurrent)
        );
        left.expect("concurrent import should be accepted");
        right.expect("concurrent import should be accepted");
        let winner = store
            .materialize(FRESH_HASH, tempdir.path())
            .await
            .expect("materialize should work")
            .expect("archive should exist");
        let winner_bytes = fs::read(winner).expect("read winner");
        assert!(
            winner_bytes == b"replaced" || winner_bytes == b"concurrent",
            "stored bytes must come from one of the racing imports"
        );

        let materialized = store
            .materialize(HASH, tempdir.path())
            .await
            .expect("materialize should work")
            .expect("archive should exist");
        assert_eq!(
            fs::read(materialized).expect("read materialized"),
            b"original",
            "a stored archive must never be replaced underneath a build"
        );

        let leftovers = fs::read_dir(tempdir.path().join("template-build-files"))
            .expect("read store dir")
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(".import-"))
            .count();
        assert_eq!(leftovers, 0, "import must not leak staging files");
    }

    #[tokio::test]
    async fn materialize_refreshes_the_archive_mtime() {
        let tempdir = TempDir::new().expect("tempdir");
        let store = PosixFsTemplateBuildFileStore::new(tempdir.path());

        let staged = staged_file(tempdir.path(), b"tar-bytes");
        store.import(HASH, &staged).await.expect("import");

        let archive = tempdir
            .path()
            .join("template-build-files")
            .join(format!("{HASH}.tar"));
        let stale = SystemTime::now() - BUILD_FILE_RETENTION - Duration::from_secs(60);
        let file = fs::File::options()
            .write(true)
            .open(&archive)
            .expect("open archive");
        file.set_times(fs::FileTimes::new().set_modified(stale))
            .expect("set stale mtime");
        drop(file);

        store
            .materialize(HASH, tempdir.path())
            .await
            .expect("materialize should work")
            .expect("archive should exist");

        let modified = fs::metadata(&archive)
            .and_then(|metadata| metadata.modified())
            .expect("read archive mtime");
        assert!(
            modified > stale,
            "materializing an archive must keep it outside the prune horizon"
        );
    }

    #[tokio::test]
    async fn verifying_a_grant_does_not_consume_it() {
        let tempdir = TempDir::new().expect("tempdir");
        let store = PosixFsTemplateBuildFileStore::new(tempdir.path());
        let token = store
            .create_upload_grant("template", HASH, 1000)
            .await
            .expect("grant should be created");

        for _ in 0..2 {
            assert!(
                store
                    .verify_upload_grant(&token, "template", HASH, 1000, 999)
                    .await
                    .expect("verification should work"),
                "verification must not consume the grant"
            );
        }
        assert!(!store
            .verify_upload_grant(&token, "other", HASH, 1000, 999)
            .await
            .expect("mismatched grant should be rejected"));

        // An upload that failed after verification can still be retried.
        assert!(store
            .claim_upload_grant(&token, "template", HASH, 1000, 999)
            .await
            .expect("claim should succeed"));
        assert!(
            !store
                .verify_upload_grant(&token, "template", HASH, 1000, 999)
                .await
                .expect("verification should work"),
            "a consumed grant must no longer verify"
        );
    }

    #[tokio::test]
    async fn concurrent_claims_pick_a_single_winner() {
        let tempdir = TempDir::new().expect("tempdir");
        let store = PosixFsTemplateBuildFileStore::new(tempdir.path());
        let token = store
            .create_upload_grant("template", HASH, 1000)
            .await
            .expect("grant should be created");

        let (left, right) = tokio::join!(
            store.claim_upload_grant(&token, "template", HASH, 1000, 999),
            store.claim_upload_grant(&token, "template", HASH, 1000, 999)
        );
        let claims = [
            left.expect("claim should work"),
            right.expect("claim should work"),
        ];
        assert_eq!(
            claims.iter().filter(|claimed| **claimed).count(),
            1,
            "exactly one concurrent claim may win"
        );
    }

    #[tokio::test]
    async fn grants_are_pruned_once_their_own_expiry_passes() {
        let tempdir = TempDir::new().expect("tempdir");
        let store = PosixFsTemplateBuildFileStore::new(tempdir.path());

        // Expired long ago in grant terms, but freshly written on disk, so the
        // mtime rule alone would keep it for the whole retention window.
        let expired_token = store
            .create_upload_grant("template", HASH, 1000)
            .await
            .expect("grant should be created");
        let expired_path = tempdir
            .path()
            .join("template-build-files")
            .join("upload-grants")
            .join(format!("{expired_token}.json"));
        assert!(expired_path.exists());

        store
            .create_upload_grant("template", HASH, i64::MAX)
            .await
            .expect("new grant should be created");

        assert!(
            !expired_path.exists(),
            "a grant past its own expiry should be pruned"
        );
    }
}
