use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};

use crate::config::{ImageConfig, UpperMode};
use crate::lsmt::file::{initialize_file_rw_paths, RwLayout};

fn normalize_path(path: &Path) -> Result<PathBuf> {
    // Lexical normalization only. OverlayBD image configs can reference files
    // that are created later, so canonicalize would be too strict and would
    // also resolve symlinks in ways that change the persisted relative layout.
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolve current dir for overlaybd path")?
            .join(path)
    };

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

pub fn resolve_config_path(base: &Path, raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        normalize_path(path)
    } else {
        normalize_path(&base.join(path))
    }
}

pub fn relative_path(from_dir: &Path, to_path: &Path) -> Result<PathBuf> {
    let from_dir = normalize_path(from_dir)?;
    let to_path = normalize_path(to_path)?;

    let from_components: Vec<_> = from_dir.components().collect();
    let to_components: Vec<_> = to_path.components().collect();

    let mut shared = 0usize;
    while shared < from_components.len()
        && shared < to_components.len()
        && from_components[shared] == to_components[shared]
    {
        shared += 1;
    }

    let mut relative = PathBuf::new();
    for _ in shared..from_components.len() {
        relative.push("..");
    }
    for component in &to_components[shared..] {
        relative.push(component.as_os_str());
    }

    if relative.as_os_str().is_empty() {
        relative.push(".");
    }
    Ok(relative)
}

fn rewrite_relative_path(value: &mut String, source_base: &Path, runtime_dir: &Path) -> Result<()> {
    if value.is_empty() || value.contains("://") {
        return Ok(());
    }
    let resolved = resolve_config_path(source_base, value)?;
    *value = relative_path(runtime_dir, &resolved)?
        .to_string_lossy()
        .into_owned();
    Ok(())
}

pub fn rewrite_overlaybd_lower_paths(
    image_config: &mut ImageConfig,
    source_base: &Path,
    runtime_dir: &Path,
) -> Result<()> {
    for lower in &mut image_config.lowers {
        for path in [
            &mut lower.file,
            &mut lower.target_file,
            &mut lower.gzip_index,
            &mut lower.dir,
        ] {
            rewrite_relative_path(path, source_base, runtime_dir)?;
        }
    }
    Ok(())
}

pub fn prepare_runtime_upper(
    data_path: &Path,
    index_path: Option<&Path>,
    virtual_size: u64,
    mode: UpperMode,
) -> Result<()> {
    let rw_layout = RwLayout::from(mode);
    initialize_file_rw_paths(data_path, index_path, virtual_size, rw_layout).with_context(
        || {
            let index_desc = index_path
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<none>".to_string());
            format!(
                "initialize overlaybd upper failed: {} / {}",
                data_path.display(),
                index_desc
            )
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn relative_path_uses_lexical_normalization_without_requiring_files() -> Result<()> {
        let temp = tempdir()?;
        let from = temp.path().join("runtime/./overlaybd");
        let to = temp.path().join("source/layers/../base.commit");

        let relative = relative_path(&from, &to)?;

        assert_eq!(relative, PathBuf::from("../../source/base.commit"));
        Ok(())
    }

    #[test]
    fn rewrite_overlaybd_lower_paths_rewrites_local_paths_and_skips_urls() -> Result<()> {
        let temp = tempdir()?;
        let source_base = temp.path().join("source");
        let runtime_dir = temp.path().join("runtime");
        let mut image: ImageConfig = serde_json::from_value(json!({
            "repoBlobUrl": "https://registry.example/v2/repo/blobs",
            "lowers": [
                {
                    "file": "layers/../base.commit",
                    "targetFile": "https://registry.example/v2/layer",
                    "gzipIndex": "",
                    "dir": "chunks"
                },
                {
                    "digest": "sha256:abc",
                    "size": 10
                }
            ]
        }))?;

        rewrite_overlaybd_lower_paths(&mut image, &source_base, &runtime_dir)?;

        assert_eq!(image.lowers[0].file, "../source/base.commit");
        assert_eq!(
            image.lowers[0].target_file,
            "https://registry.example/v2/layer"
        );
        assert!(image.lowers[0].gzip_index.is_empty());
        assert_eq!(image.lowers[0].dir, "../source/chunks");
        assert_eq!(image.lowers[1].digest, "sha256:abc");
        assert_eq!(image.lowers[1].size, 10);
        assert_eq!(
            image.repo_blob_url,
            "https://registry.example/v2/repo/blobs"
        );
        Ok(())
    }

    #[test]
    fn relative_path_returns_dot_for_same_path() -> Result<()> {
        let temp = tempdir()?;

        let relative = relative_path(temp.path(), temp.path())?;

        assert_eq!(relative, PathBuf::from("."));
        Ok(())
    }
}
