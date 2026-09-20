use std::fs;
use std::path::{Path, PathBuf};

use agentenv::cfg::AppConfig;
use agentenv::snapshot::SnapshotId;
use anyhow::{ensure, Context, Result};

/// The previous container's Firecracker and ublk resources must have stopped.
/// Run once before restoring state or creating any new runtime resources.
pub(super) async fn cleanup_previous_runtime(config: &AppConfig) -> Result<()> {
    let config = config.clone();
    tokio::task::spawn_blocking(move || {
        let sandboxes = &config.orchestrator.persisted_sandbox_store_path;
        let snapshots = &config.snapshot.local_root;
        let work = config
            .firecracker
            .work_dir
            .as_deref()
            .context("Firecracker work root")?;
        let roots = [sandboxes.as_path(), snapshots.as_path(), work]
            .map(resolve_root)
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        for (index, left) in roots.iter().enumerate() {
            for right in &roots[index + 1..] {
                ensure_disjoint(left, right)?;
            }
        }
        let volume_state = config.home_path.join("volumes");
        let protected = [
            Some(volume_state.as_path()),
            Some(config.image.cache.root_dir.as_path()),
            config.firecracker.serial_dir.as_deref(),
            config
                .backend
                .posix_fs
                .as_ref()
                .map(|backend| backend.snapshot_store.as_path()),
            Some(config.deps_path.as_path()),
            Some(config.p2p.store_dir.as_path()),
        ];
        for path in protected.into_iter().flatten() {
            let protected = resolve_root(path)?;
            for root in &roots {
                ensure_disjoint(root, &protected)?;
            }
        }

        remove_stale_directories(snapshots, |name| SnapshotId::parse(name).is_ok())?;
        remove_stale_directories(work, |name| {
            name.starts_with("agentenv-fc-") || name.starts_with("agentenv-sandboxes-")
        })
    })
    .await
    .context("finish previous runtime cleanup")?
}

fn ensure_disjoint(left: &Path, right: &Path) -> Result<()> {
    ensure!(
        !left.starts_with(right) && !right.starts_with(left),
        "runtime cleanup roots must not overlap retained storage: {} and {}",
        left.display(),
        right.display(),
    );
    Ok(())
}

fn resolve_root(path: &Path) -> Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    for ancestor in absolute.ancestors() {
        match fs::canonicalize(ancestor) {
            Ok(resolved) => {
                return Ok(overlaybd::config::lexically_normalize_path(
                    &resolved.join(absolute.strip_prefix(ancestor)?),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).with_context(|| format!("resolve {}", path.display())),
        }
    }
    anyhow::bail!("cannot resolve cleanup root {}", path.display())
}

fn remove_stale_directories(root: &Path, owned: impl Fn(&str) -> bool) -> Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("scan {}", root.display())),
    };
    for entry in entries {
        let entry = entry?;
        if owned(&entry.file_name().to_string_lossy()) && entry.file_type()?.is_dir() {
            tracing::info!(path = %entry.path().display(), "removing previous runtime directory");
            fs::remove_dir_all(entry.path()).with_context(|| {
                format!(
                    "remove previous runtime directory {}",
                    entry.path().display()
                )
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentenv::cfg::ConfigManager;

    fn config(root: &Path) -> Result<AppConfig> {
        let path = root.join("config.toml");
        fs::write(
            &path,
            "home_path = './home'\ndeps_path = './deps'\nruntime_path = './run'\n",
        )?;
        Ok(ConfigManager::new_from_path(&path)?.config().clone())
    }

    #[tokio::test]
    async fn cleanup_preserves_state_cache_logs_and_unknown_entries() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let config = config(temp.path())?;
        let snapshots = &config.snapshot.local_root;
        let work = config.firecracker.work_dir.as_ref().unwrap();
        let stale = [
            snapshots.join(SnapshotId::generate().to_string()),
            work.join("agentenv-fc-old"),
            work.join("agentenv-sandboxes-old"),
        ];
        let retained = [
            config
                .orchestrator
                .persisted_sandbox_store_path
                .join("records.db"),
            config
                .orchestrator
                .persisted_sandbox_store_path
                .join(agentenv::types::SandboxId::new().to_string()),
            snapshots.join("cache"),
            snapshots.join("operator-files"),
            work.join("operator-files"),
            config.firecracker.serial_dir.clone().unwrap(),
        ];
        for path in stale.iter().chain(retained.iter()) {
            fs::create_dir_all(path)?;
            fs::write(path.join("keep"), b"content")?;
        }
        let alias = snapshots.join(SnapshotId::generate().to_string());
        std::os::unix::fs::symlink(&retained[1], &alias)?;
        // Removing a source name must not affect adopted private layers.
        fs::hard_link(stale[0].join("keep"), retained[1].join("adopted"))?;
        for _ in 0..2 {
            cleanup_previous_runtime(&config).await?;
        }
        assert!(stale.iter().all(|path| !path.exists()));
        assert!(alias.is_symlink());
        for path in retained {
            assert_eq!(fs::read(path.join("keep"))?, b"content");
        }
        Ok(())
    }

    #[tokio::test]
    async fn conflicting_roots_and_scan_failures_abort_before_cleanup() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut config = config(temp.path())?;
        let work = config.firecracker.work_dir.clone().unwrap();
        let stale = work.join("agentenv-fc-old");
        fs::create_dir_all(&stale)?;
        let snapshots = config.snapshot.local_root.clone();
        fs::create_dir_all(&snapshots)?;
        let alias = temp.path().join("aliased-work");
        std::os::unix::fs::symlink(&work, &alias)?;
        config.snapshot.local_root = alias.join("not-created-yet");
        assert!(cleanup_previous_runtime(&config).await.is_err());
        assert!(stale.is_dir());
        config.snapshot.local_root = snapshots;
        fs::remove_dir(&config.snapshot.local_root)?;
        fs::write(&config.snapshot.local_root, b"not a directory")?;
        assert!(cleanup_previous_runtime(&config).await.is_err());
        assert!(stale.is_dir());
        Ok(())
    }
}
