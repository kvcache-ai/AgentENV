use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::digest;

const INDEX_SCHEMA_VERSION: u32 = 1;
const OCI_SOURCE_KIND: &str = "oci-layer";
const MAX_DIGEST_SLUG_LEN: usize = 160;
pub(crate) const OVERLAYBD_COMMIT_FILE: &str = "overlaybd.commit";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommitIndex {
    schema: u32,
    source_kind: String,
    source_digest: String,
    pub(crate) commit_digest: String,
    pub(crate) size: u64,
    converter: String,
    virtual_size_gib: u64,
    mkfs: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_commit_digest: Option<String>,
}

impl CommitIndex {
    pub(crate) fn oci_layer(
        source_digest: String,
        commit_digest: String,
        size: u64,
        converter: String,
        virtual_size_gib: u64,
        mkfs: bool,
        parent_commit_digest: Option<String>,
    ) -> Self {
        Self {
            schema: INDEX_SCHEMA_VERSION,
            source_kind: OCI_SOURCE_KIND.to_string(),
            source_digest,
            commit_digest,
            size,
            converter,
            virtual_size_gib,
            mkfs,
            parent_commit_digest,
        }
    }

    pub(crate) fn matches_oci_context(
        &self,
        source_digest: &str,
        converter: &str,
        virtual_size_gib: u64,
        mkfs: bool,
        parent_commit_digest: Option<&str>,
    ) -> bool {
        self.schema == INDEX_SCHEMA_VERSION
            && self.source_kind == OCI_SOURCE_KIND
            && self.source_digest == source_digest
            && self.converter == converter
            && self.virtual_size_gib == virtual_size_gib
            && self.mkfs == mkfs
            && self.parent_commit_digest.as_deref() == parent_commit_digest
    }

    pub(crate) async fn read(path: &Path) -> Result<Option<Self>> {
        let bytes = match tokio::fs::read(path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(err).with_context(|| format!("read commit index {}", path.display()));
            }
        };
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parse commit index {}", path.display()))
            .map(Some)
    }

    pub(crate) async fn write(&self, path: &Path) -> Result<()> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create commit index dir {}", parent.display()))?;

        let tmp = tempfile::NamedTempFile::new_in(parent)
            .with_context(|| format!("create temp commit index in {}", parent.display()))?;
        let tmp_path = tmp.path().to_path_buf();

        tokio::fs::write(&tmp_path, serde_json::to_vec_pretty(self)?)
            .await
            .with_context(|| format!("write temp commit index {}", tmp_path.display()))?;
        tmp.persist(path)
            .map_err(|err| err.error)
            .with_context(|| {
                format!(
                    "move commit index {} to {}",
                    tmp_path.display(),
                    path.display()
                )
            })?;
        Ok(())
    }
}

pub(crate) fn digest_slug(digest: &str) -> String {
    let slug = match digest.split_once(':') {
        Some((algo, hex)) if !algo.is_empty() && !hex.is_empty() => format!("{algo}-{hex}"),
        _ => format!("unknown-{digest}"),
    };
    sanitize_filename_component(&slug, MAX_DIGEST_SLUG_LEN)
}

pub(crate) fn commit_dir(commit_store: &Path, digest: &str) -> PathBuf {
    commit_store.join(digest_slug(digest))
}

pub(crate) fn commit_file(commit_store: &Path, digest: &str) -> PathBuf {
    commit_dir(commit_store, digest).join(OVERLAYBD_COMMIT_FILE)
}

pub(crate) fn cached_commit_file_if_usable(
    commit_store: &Path,
    digest: &str,
    size: u64,
) -> Option<PathBuf> {
    let path = commit_file(commit_store, digest);
    let metadata = fs::metadata(&path).ok()?;
    if metadata.is_file() && metadata.len() == size {
        Some(path)
    } else {
        None
    }
}

pub(crate) fn accept_existing_commit_file(
    destination: &Path,
    digest: &str,
    size: u64,
) -> Result<PathBuf> {
    let metadata = fs::metadata(destination)
        .with_context(|| format!("stat existing commit cache file {}", destination.display()))?;
    if !metadata.is_file() {
        bail!(
            "commit cache file {} for {digest} is not a regular file",
            destination.display()
        );
    }
    let existing_size = metadata.len();
    if existing_size != size {
        bail!(
            "commit cache file {} size mismatch for {digest}: expected {size}, found {existing_size}",
            destination.display()
        );
    }
    Ok(destination.to_path_buf())
}

pub(crate) fn seed_commit_file(
    commit_store: &Path,
    source: &Path,
    digest: &str,
    size: u64,
) -> Result<PathBuf> {
    let destination = commit_file(commit_store, digest);
    if let Some(existing) = cached_commit_file_if_usable(commit_store, digest, size) {
        return Ok(existing);
    }

    let parent = destination
        .parent()
        .with_context(|| format!("resolve commit cache parent for {}", destination.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create commit cache dir {}", parent.display()))?;

    let source_len = fs::metadata(source)
        .with_context(|| format!("stat source commit {}", source.display()))?
        .len();
    if source_len != size {
        bail!(
            "commit cache seed size mismatch for {}: descriptor says {size}, file has {source_len}",
            source.display()
        );
    }

    let mut source_file = fs::File::open(source)
        .with_context(|| format!("open source commit {}", source.display()))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temp commit cache file in {}", parent.display()))?;
    let temp_path = temp.path().to_path_buf();
    let copied;
    let actual_digest;
    {
        let temp_file = temp.as_file_mut();
        let (actual_digest_hex, copied_bytes) =
            digest::copy_with_sha256_hex(&mut source_file, &mut *temp_file).with_context(|| {
                format!(
                    "copy source commit {} to temp commit cache file {}",
                    source.display(),
                    temp_path.display()
                )
            })?;
        actual_digest = format!("sha256:{actual_digest_hex}");
        copied = copied_bytes;
        temp_file
            .sync_all()
            .with_context(|| format!("sync temp commit cache file {}", temp_path.display()))?;
    }
    if copied != size {
        bail!(
            "short commit cache seed copy for {}: expected {size} bytes, copied {copied}",
            source.display()
        );
    }
    if actual_digest != digest {
        bail!(
            "commit cache seed digest mismatch for {}: expected {digest}, got {actual_digest}",
            source.display()
        );
    }
    match temp.persist_noclobber(&destination) {
        Ok(_) => Ok(destination),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            accept_existing_commit_file(&destination, digest, size)
        }
        Err(error) => Err(error.error).with_context(|| {
            format!(
                "persist temp commit cache file {} to {}",
                error.file.path().display(),
                destination.display()
            )
        }),
    }
}

pub(crate) fn seed_commit_file_trusted_descriptor(
    commit_store: &Path,
    source: &Path,
    digest: &str,
    size: u64,
) -> Result<PathBuf> {
    let destination = commit_file(commit_store, digest);
    if let Some(existing) = cached_commit_file_if_usable(commit_store, digest, size) {
        return Ok(existing);
    }

    let parent = destination
        .parent()
        .with_context(|| format!("resolve commit cache parent for {}", destination.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create commit cache dir {}", parent.display()))?;

    let source_len = fs::metadata(source)
        .with_context(|| format!("stat source commit {}", source.display()))?
        .len();
    if source_len != size {
        bail!(
            "trusted commit cache seed size mismatch for {}: descriptor says {size}, file has {source_len}",
            source.display()
        );
    }

    match fs::hard_link(source, &destination) {
        Ok(()) => return Ok(destination),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return accept_existing_commit_file(&destination, digest, size);
        }
        Err(error) => {
            debug!(
                source = %source.display(),
                destination = %destination.display(),
                error = %error,
                "hard-link overlaybd commit cache seed failed; falling back to copy"
            );
        }
    }

    let temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temp commit cache file in {}", parent.display()))?;
    fs::copy(source, temp.path()).with_context(|| {
        format!(
            "copy trusted source commit {} to temp {}",
            source.display(),
            temp.path().display()
        )
    })?;
    if temp
        .as_file()
        .metadata()
        .with_context(|| format!("stat temp commit cache file {}", temp.path().display()))?
        .len()
        != size
    {
        bail!(
            "trusted commit cache seed temp size mismatch for {}: expected {size}",
            source.display()
        );
    }
    temp.as_file()
        .sync_all()
        .with_context(|| format!("sync temp commit cache file {}", temp.path().display()))?;

    match temp.persist_noclobber(&destination) {
        Ok(_) => Ok(destination),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            accept_existing_commit_file(&destination, digest, size)
        }
        Err(error) => Err(error.error).with_context(|| {
            format!(
                "persist temp commit cache file {} to {}",
                error.file.path().display(),
                destination.display()
            )
        }),
    }
}

#[expect(
    clippy::expect_used,
    reason = "serializing this plain derived struct to JSON cannot fail"
)]
pub(crate) fn index_path(
    index_dir: &Path,
    source_digest: &str,
    converter: &str,
    virtual_size_gib: u64,
    mkfs: bool,
    parent_commit_digest: Option<&str>,
) -> PathBuf {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct IndexPathContext<'a> {
        converter: &'a str,
        virtual_size_gib: u64,
        mkfs: bool,
        parent_commit_digest: Option<&'a str>,
    }

    let context = serde_json::to_vec(&IndexPathContext {
        converter,
        virtual_size_gib,
        mkfs,
        parent_commit_digest,
    })
    .expect("serialize commit index path context");
    index_dir
        .join(digest_slug(source_digest))
        .join(format!("sha256-{}.json", digest::sha256_hex(&context)))
}

pub(crate) fn sanitize_filename_component(value: &str, max_len: usize) -> String {
    let mut component = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    component.truncate(max_len);
    component
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_slug_uses_algo_prefix() {
        assert_eq!(digest_slug("sha256:abc123"), "sha256-abc123");
    }

    #[test]
    fn digest_slug_is_bounded() {
        let digest = format!("sha256:{}", "a".repeat(MAX_DIGEST_SLUG_LEN * 2));
        assert_eq!(digest_slug(&digest).len(), MAX_DIGEST_SLUG_LEN);
    }

    #[test]
    fn seed_commit_file_copies_and_verifies_digest() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("snapshot.commit");
        std::fs::write(&source, b"sealed delta").expect("write source");
        let digest = digest::sha256_digest(b"sealed delta");

        let cached =
            seed_commit_file(&temp.path().join("commits"), &source, &digest, 12).expect("seed");

        assert_eq!(
            cached,
            temp.path()
                .join("commits")
                .join(digest_slug(&digest))
                .join(OVERLAYBD_COMMIT_FILE)
        );
        assert_eq!(std::fs::read(cached).expect("read cached"), b"sealed delta");
    }

    #[test]
    fn seed_commit_file_rejects_digest_mismatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("snapshot.commit");
        std::fs::write(&source, b"sealed delta").expect("write source");

        let err = seed_commit_file(&temp.path().join("commits"), &source, "sha256:0000", 12)
            .expect_err("digest mismatch should fail");

        assert!(err.to_string().contains("digest mismatch"));
    }

    #[test]
    fn accept_existing_commit_file_checks_size() {
        let temp = tempfile::tempdir().expect("tempdir");
        let destination = temp.path().join("overlaybd.commit");
        std::fs::write(&destination, b"sealed delta").expect("write destination");

        let accepted =
            accept_existing_commit_file(&destination, "sha256:existing", 12).expect("accept");
        assert_eq!(accepted, destination);

        let err = accept_existing_commit_file(&accepted, "sha256:existing", 11)
            .expect_err("size mismatch should fail");
        assert!(err.to_string().contains("size mismatch"));
    }

    #[test]
    fn seed_commit_file_trusted_descriptor_links_without_hashing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("snapshot.commit");
        std::fs::write(&source, b"sealed delta").expect("write source");

        let cached = seed_commit_file_trusted_descriptor(
            &temp.path().join("commits"),
            &source,
            "sha256:trusted",
            12,
        )
        .expect("trusted seed");

        assert_eq!(
            cached,
            temp.path()
                .join("commits")
                .join(digest_slug("sha256:trusted"))
                .join(OVERLAYBD_COMMIT_FILE)
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            assert_eq!(
                std::fs::metadata(&source).expect("source metadata").ino(),
                std::fs::metadata(&cached).expect("cached metadata").ino()
            );
        }
        assert_eq!(std::fs::read(cached).expect("read cached"), b"sealed delta");
    }

    #[test]
    fn seed_commit_file_trusted_descriptor_rejects_size_mismatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("snapshot.commit");
        std::fs::write(&source, b"sealed delta").expect("write source");

        let err = seed_commit_file_trusted_descriptor(
            &temp.path().join("commits"),
            &source,
            "sha256:trusted",
            11,
        )
        .expect_err("size mismatch should fail");

        assert!(err.to_string().contains("size mismatch"));
    }

    #[test]
    fn oci_index_context_checks_parent_commit() {
        let index = CommitIndex::oci_layer(
            "sha256:oci".to_string(),
            "sha256:commit".to_string(),
            123,
            "converter".to_string(),
            64,
            false,
            Some("sha256:parent".to_string()),
        );

        assert!(index.matches_oci_context(
            "sha256:oci",
            "converter",
            64,
            false,
            Some("sha256:parent")
        ));
        assert!(!index.matches_oci_context("sha256:oci", "converter", 64, false, None));

        let root = Path::new("/cache/indexes");
        let without_parent = index_path(root, "sha256:oci", "converter", 64, false, None);
        let with_parent = index_path(
            root,
            "sha256:oci",
            "converter",
            64,
            false,
            Some("sha256:parent"),
        );
        assert_eq!(with_parent.parent().unwrap(), root.join("sha256-oci"));
        assert_ne!(without_parent, with_parent);
    }
}
