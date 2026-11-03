use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tracing::warn;

use super::graph::{load_cache_owned_image_config_facts, non_empty, stable_path_identity};
use crate::digest;
use crate::image::commit_index::sanitize_filename_component;
use crate::image::ImageResolutionMetadata;

const MAX_SOURCE_IMAGE_SCOPE_COMPONENT_LEN: usize = 96;
const SOURCE_IMAGE_SCOPE_HASH_HEX_LEN: usize = 16;

#[derive(Default)]
pub(super) struct SourceImageCache {
    locks: DashMap<PathBuf, Arc<AsyncMutex<()>>>,
}

impl SourceImageCache {
    pub(super) async fn acquire_lock(
        self: &Arc<Self>,
        image_config_path: &Path,
    ) -> SourceImageLock {
        let lock_key = stable_path_identity(image_config_path);
        let lock = self
            .locks
            .entry(lock_key.clone())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        let guard = lock.clone().lock_owned().await;
        SourceImageLock {
            source_cache: Arc::clone(self),
            lock_key,
            lock,
            guard: Some(guard),
        }
    }
}

pub(super) struct SourceImageLock {
    source_cache: Arc<SourceImageCache>,
    lock_key: PathBuf,
    lock: Arc<AsyncMutex<()>>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for SourceImageLock {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.source_cache
            .locks
            .remove_if(&self.lock_key, |_, existing| {
                Arc::ptr_eq(existing, &self.lock) && Arc::strong_count(existing) <= 2
            });
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct ImageCacheSourceImageKey {
    manifest_digest: String,
    repository_scope: Option<String>,
}

impl ImageCacheSourceImageKey {
    pub(crate) fn new(
        manifest_digest: impl Into<String>,
        repository_scope: Option<String>,
    ) -> Result<Self> {
        let repository_scope = repository_scope.and_then(|scope| {
            let trimmed = scope.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        Ok(Self {
            manifest_digest: non_empty(
                "image cache source manifest digest",
                manifest_digest.into(),
            )?,
            repository_scope,
        })
    }

    pub(crate) fn repository_scope(&self) -> Option<&str> {
        self.repository_scope.as_deref()
    }
}

pub(super) async fn read_source_image_metadata(
    metadata_path: &Path,
) -> Result<Option<ImageResolutionMetadata>> {
    if !file_exists_nonempty(metadata_path) {
        return Ok(None);
    }
    let bytes = tokio::fs::read(metadata_path)
        .await
        .with_context(|| format!("read image metadata {}", metadata_path.display()))?;
    Ok(serde_json::from_slice(&bytes).ok())
}

pub(super) fn source_image_config_filename(key: &ImageCacheSourceImageKey) -> String {
    let slug = sanitize_filename_component(key.manifest_digest.trim(), usize::MAX);
    let slug = if slug.is_empty() {
        String::from("image")
    } else {
        slug
    };
    match key.repository_scope() {
        Some(scope) => match source_image_scope_component(scope) {
            Some(scope) => format!("{slug}-{scope}-image.json"),
            None => format!("{slug}-image.json"),
        },
        None => format!("{slug}-image.json"),
    }
}

fn source_image_scope_component(scope: &str) -> Option<String> {
    let scope = scope.trim();
    if scope.is_empty() {
        return None;
    }

    let mut component = sanitize_filename_component(scope, usize::MAX);
    if component.is_empty() {
        return None;
    }
    if component.len() <= MAX_SOURCE_IMAGE_SCOPE_COMPONENT_LEN {
        return Some(component);
    }

    let hash = digest::sha256_hex(scope.as_bytes());
    let suffix = &hash[..SOURCE_IMAGE_SCOPE_HASH_HEX_LEN];
    let prefix_len =
        MAX_SOURCE_IMAGE_SCOPE_COMPONENT_LEN.saturating_sub(SOURCE_IMAGE_SCOPE_HASH_HEX_LEN + 1);
    component.truncate(prefix_len);
    Some(format!("{component}-{suffix}"))
}

pub(super) fn source_image_metadata_path_for_config_path(path: &Path) -> PathBuf {
    let mut metadata_path = path.to_path_buf();
    let stem = path
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or("image");
    metadata_path.set_file_name(format!("{stem}.metadata.json"));
    metadata_path
}

pub(super) fn source_image_config_is_usable(path: &Path) -> bool {
    match source_image_config_usable(path) {
        Ok(usable) => usable,
        Err(error) => {
            warn!(
                config = %path.display(),
                error = %error,
                "cached image config is unusable; regenerating"
            );
            false
        }
    }
}

fn source_image_config_usable(path: &Path) -> Result<bool> {
    if !file_exists_nonempty(path) {
        return Ok(false);
    }

    let facts = load_cache_owned_image_config_facts(path)?;
    for lower in facts.local_file_lowers {
        let metadata = lower
            .file
            .metadata()
            .with_context(|| format!("stat local lower {}", lower.file.display()))?;
        if !metadata.is_file() || metadata.len() == 0 {
            bail!(
                "local lower {} is empty or not a file",
                lower.file.display()
            );
        }
        if metadata.len() != lower.size {
            bail!(
                "local lower {} size mismatch: expected {}, got {}",
                lower.file.display(),
                lower.size,
                metadata.len()
            );
        }
    }

    if let Some(upper_path) = facts.upper_file {
        if !file_exists_nonempty(&upper_path) {
            bail!("local upper {} is missing or empty", upper_path.display());
        }
    }

    Ok(true)
}

fn file_exists_nonempty(path: &Path) -> bool {
    path.metadata()
        .map(|metadata| metadata.len() > 0)
        .unwrap_or(false)
}

pub(super) async fn write_json_atomic<T: Serialize>(
    path: &Path,
    value: &T,
    label: &str,
) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("{label} path has no parent: {}", path.display()))?;
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("create {label} parent {}", parent.display()))?;
    let mut tmp_path = path.to_path_buf();
    let mut tmp_name = path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_else(|| OsString::from(label));
    tmp_name.push(".tmp");
    tmp_path.set_file_name(tmp_name);
    tokio::fs::write(&tmp_path, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| format!("write temp {label} {}", tmp_path.display()))?;
    tokio::fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("move {label} {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn source_image_config_filename_scopes_and_bounds_remote_repository() {
        let first_scope = format!("registry.example.com/team/{}", "x".repeat(160));
        let second_scope = format!("registry.example.com/team/{}", "y".repeat(160));
        let first = source_image_config_filename(
            &ImageCacheSourceImageKey::new("sha256:abc123", Some(first_scope.clone()))
                .expect("first source key"),
        );
        let first_again = source_image_config_filename(
            &ImageCacheSourceImageKey::new("sha256:abc123", Some(first_scope))
                .expect("first source key again"),
        );
        let second = source_image_config_filename(
            &ImageCacheSourceImageKey::new("sha256:abc123", Some(second_scope))
                .expect("second source key"),
        );

        assert!(!first.contains('/'));
        assert!(first.len() < 255);
        assert_eq!(first, first_again);
        assert_ne!(first, second);
    }

    #[test]
    fn source_image_config_cache_usability_tracks_local_file_requirements() {
        let temp = TempDir::new().expect("tempdir");
        let missing_local = temp.path().join("missing-local.json");
        let remote_only = temp.path().join("remote-only.json");
        std::fs::write(
            &missing_local,
            serde_json::to_vec(&json!({
                "repoBlobUrl": "",
                "lowers": [{
                    "file": temp.path().join("missing.commit"),
                    "digest": "sha256:missing",
                    "size": 7
                }],
                "upper": {},
                "resultFile": ""
            }))
            .unwrap(),
        )
        .expect("write missing-local config");
        std::fs::write(
            &remote_only,
            serde_json::to_vec(&json!({
                "repoBlobUrl": "https://registry.example/v2/repo/blobs",
                "lowers": [{ "digest": "sha256:abc", "size": 123 }],
                "upper": {},
                "resultFile": ""
            }))
            .unwrap(),
        )
        .expect("write remote-only config");

        assert!(!source_image_config_is_usable(&missing_local));
        assert!(source_image_config_is_usable(&remote_only));
    }
}
