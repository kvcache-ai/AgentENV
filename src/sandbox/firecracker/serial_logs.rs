//! Lifecycle and retention of the managed, flat per-sandbox serial log tree.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::cfg::FirecrackerConfig;
use crate::types::SandboxId;

const LOG_NAMES: [&str; 3] = [
    "firecracker-stdout.log",
    "firecracker-stderr.log",
    "firecracker.log",
];
const INACTIVE_MARKER: &str = ".inactive";
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

fn lock_file(file: &File, operation: libc::c_int) -> io::Result<()> {
    // SAFETY: file owns a valid descriptor, and flock does not retain pointers.
    if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn open_directory(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
}

/// Held before opening or relocating logs, through the process's lifetime.
/// A collector takes an exclusive lock on this same directory before pruning.
pub(super) struct SerialLogDir {
    path: PathBuf,
    directory: File,
}

impl SerialLogDir {
    pub(super) async fn acquire(path: PathBuf) -> io::Result<Self> {
        loop {
            fs::create_dir_all(&path)?;
            let directory = match open_directory(&path) {
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                result => result?,
            };
            match lock_file(&directory, libc::LOCK_SH | libc::LOCK_NB) {
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }
                result => result?,
            }
            // A collector may have removed the directory between mkdir/open
            // and flock. Never start writing through a lock on an old inode.
            let metadata = directory.metadata()?;
            match fs::symlink_metadata(&path) {
                Ok(current)
                    if current.dev() == metadata.dev() && current.ino() == metadata.ino() =>
                {
                    // Resuming an ID makes its retained logs active again.
                    match fs::remove_file(path.join(INACTIVE_MARKER)) {
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                        result => result?,
                    }
                    return Ok(Self { path, directory });
                }
                Ok(_) => continue,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Call only after the process has exited and closed every log descriptor.
    pub(super) fn finish(self) -> io::Result<()> {
        File::create(self.path.join(INACTIVE_MARKER))?;
        self.directory.set_modified(SystemTime::now())?;
        let path = self.path.clone();
        drop(self);
        clean_directory(&path, None, SystemTime::now()).map(|_| ())
    }
}

impl Drop for SerialLogDir {
    fn drop(&mut self) {
        // Abnormal drop does not prove process exit. Leave its logs unmarked.
        // Release explicitly because concurrent spawns may briefly inherit this
        // CLOEXEC descriptor before exec.
        let _ = lock_file(&self.directory, libc::LOCK_UN);
    }
}

pub(super) fn clean_directory(
    path: &Path,
    retention: Option<Duration>,
    now: SystemTime,
) -> io::Result<usize> {
    let directory = match open_directory(path) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        result => result?,
    };
    match lock_file(&directory, libc::LOCK_EX | libc::LOCK_NB) {
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(0),
        result => result?,
    }

    let metadata = directory.metadata()?;
    let current = match fs::symlink_metadata(path) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        result => result?,
    };
    if current.dev() != metadata.dev() || current.ino() != metadata.ino() {
        return Ok(0);
    }

    // Only an explicit stop can prove that all writers, including Firecracker's
    // internal logger, have exited. Legacy and abnormal-drop logs stay untouched.
    let marker = match fs::symlink_metadata(path.join(INACTIVE_MARKER)) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        result => result?,
    };
    if !marker.is_file() {
        return Ok(0);
    }

    let mut newest = metadata.modified()?;
    let mut files = Vec::new();
    for name in LOG_NAMES {
        let file_path = path.join(name);
        let metadata = match fs::symlink_metadata(&file_path) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            result => result?,
        };
        // Unexpected entries, including symlinks, are never removed.
        if !metadata.is_file() {
            return Ok(0);
        }
        newest = newest.max(metadata.modified()?);
        files.push((file_path, metadata.len()));
    }
    let expired = retention
        .is_some_and(|retention| now.duration_since(newest).is_ok_and(|age| age >= retention));
    let mut removed = 0;
    for (file_path, len) in &files {
        if *len == 0 || expired {
            fs::remove_file(file_path)?;
            removed += 1;
        }
    }
    if removed == files.len() {
        fs::remove_file(path.join(INACTIVE_MARKER))?;
    }
    // Never recursively delete: custom files and subdirectories are preserved.
    match fs::remove_dir(path) {
        Ok(()) => removed += 1,
        Err(err) if err.kind() == io::ErrorKind::DirectoryNotEmpty => {}
        Err(err) => return Err(err),
    }
    Ok(removed)
}

fn sweep(root: &Path, retention: Option<Duration>, cancelled: &AtomicBool) -> io::Result<usize> {
    let entries = match fs::read_dir(root) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        result => result?,
    };
    let now = SystemTime::now();
    let mut removed = 0;
    for entry in entries {
        if cancelled.load(Ordering::Relaxed) {
            break;
        }
        let entry = match entry {
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            result => result?,
        };
        let file_type = match entry.file_type() {
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            result => result?,
        };
        if !file_type.is_dir()
            || entry
                .file_name()
                .to_str()
                .is_none_or(|name| SandboxId::parse_str(name).is_err())
        {
            continue;
        }
        match clean_directory(&entry.path(), retention, now) {
            Ok(count) => removed += count,
            Err(err) => {
                warn!(path = %entry.path().display(), error = %err, "failed to clean serial logs");
            }
        }
    }
    Ok(removed)
}

/// Background maintenance for managed Firecracker serial logs.
pub struct SerialLogCleanup {
    cancelled: Arc<AtomicBool>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl SerialLogCleanup {
    pub fn start(config: &FirecrackerConfig) -> Self {
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let root = config.serial_dir.clone();
        let retention = (config.serial_log_retention_secs > 0)
            .then(|| Duration::from_secs(config.serial_log_retention_secs));
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let Some(root) = root else { return };
            loop {
                let root = root.clone();
                let cancelled = Arc::clone(&worker_cancelled);
                match tokio::task::spawn_blocking(move || sweep(&root, retention, &cancelled)).await
                {
                    Ok(Ok(removed)) => debug!(removed, "serial log cleanup completed"),
                    Ok(Err(err)) => warn!(error = %err, "serial log cleanup failed"),
                    Err(err) => warn!(error = %err, "serial log cleanup task failed"),
                }
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = tokio::time::sleep(CLEANUP_INTERVAL) => {}
                }
            }
        });
        Self {
            cancelled,
            stop: Some(stop),
            task: Some(task),
        }
    }

    pub async fn shutdown(mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.stop.take();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for SerialLogCleanup {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.stop.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use tempfile::tempdir;

    fn log_dir(root: &Path) -> PathBuf {
        root.join(SandboxId::new().to_string())
    }

    fn age_logs(path: &Path, age: Duration) -> io::Result<SystemTime> {
        let now = SystemTime::now();
        let old = now - age;
        for entry in fs::read_dir(path)? {
            File::open(entry?.path())?.set_modified(old)?;
        }
        File::open(path)?.set_modified(old)?;
        Ok(now)
    }

    #[tokio::test]
    async fn inactive_empty_logs_are_removed_and_nonempty_logs_survive() -> io::Result<()> {
        let root = tempdir()?;
        let path = log_dir(root.path());
        let guard = SerialLogDir::acquire(path.clone()).await?;
        fs::write(path.join(LOG_NAMES[0]), [])?;
        fs::write(path.join(LOG_NAMES[1]), b"boot failed")?;
        fs::write(path.join(LOG_NAMES[2]), [])?;
        assert_eq!(
            clean_directory(&path, Some(Duration::ZERO), SystemTime::now())?,
            0
        );
        assert!(path.join(LOG_NAMES[0]).exists());
        guard.finish()?;
        assert!(!path.join(LOG_NAMES[0]).exists());
        assert!(!path.join(LOG_NAMES[2]).exists());
        assert_eq!(fs::read(path.join(LOG_NAMES[1]))?, b"boot failed");

        let empty = log_dir(root.path());
        let guard = SerialLogDir::acquire(empty.clone()).await?;
        fs::write(empty.join(LOG_NAMES[1]), [])?;
        guard.finish()?;
        assert!(!empty.exists());
        Ok(())
    }

    #[test]
    fn retention_uses_latest_output_and_can_be_disabled() -> io::Result<()> {
        let root = tempdir()?;
        let path = log_dir(root.path());
        fs::create_dir(&path)?;
        fs::write(path.join(LOG_NAMES[0]), b"old boot")?;
        fs::write(path.join(LOG_NAMES[1]), b"later error")?;
        File::create(path.join(INACTIVE_MARKER))?;
        let retention = Duration::from_secs(60);
        let now = age_logs(&path, retention * 2)?;
        assert_eq!(clean_directory(&path, None, now)?, 0);
        File::open(path.join(LOG_NAMES[1]))?.set_modified(now)?;
        assert_eq!(clean_directory(&path, Some(retention), now)?, 0);
        assert_eq!(clean_directory(&path, Some(retention), now + retention)?, 3);
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn release_starts_a_full_retention_period_and_resume_protects_history() -> io::Result<()>
    {
        let root = tempdir()?;
        let path = log_dir(root.path());
        let guard = SerialLogDir::acquire(path.clone()).await?;
        fs::write(path.join(LOG_NAMES[0]), b"boot")?;
        age_logs(&path, Duration::from_secs(3600))?;
        guard.finish()?;
        let retention = Duration::from_secs(60);
        assert_eq!(
            clean_directory(&path, Some(retention), SystemTime::now())?,
            0
        );
        let resumed = SerialLogDir::acquire(path.clone()).await?;
        assert_eq!(
            clean_directory(&path, Some(Duration::ZERO), SystemTime::now())?,
            0
        );
        resumed.finish()?;
        assert_eq!(fs::read(path.join(LOG_NAMES[0]))?, b"boot");
        Ok(())
    }

    #[tokio::test]
    async fn abnormal_drop_preserves_unlocked_logger_and_legacy_logs() -> io::Result<()> {
        let root = tempdir()?;
        let path = log_dir(root.path());
        let guard = SerialLogDir::acquire(path.clone()).await?;
        // With explicit stdio overrides, only the internal logger is managed.
        let mut logger = File::create(path.join(LOG_NAMES[2]))?;
        drop(guard);
        assert!(path.join(LOG_NAMES[2]).exists());
        assert_eq!(
            sweep(root.path(), Some(Duration::ZERO), &AtomicBool::new(false))?,
            0
        );
        logger.write_all(b"late diagnostics")?;
        drop(logger);
        // A legacy directory is likewise unmarked even after its writer exits.
        assert_eq!(
            sweep(root.path(), Some(Duration::ZERO), &AtomicBool::new(false))?,
            0
        );
        assert_eq!(fs::read(path.join(LOG_NAMES[2]))?, b"late diagnostics");
        Ok(())
    }

    #[tokio::test]
    async fn abandoned_resume_clears_previous_cleanup_eligibility() -> io::Result<()> {
        let root = tempdir()?;
        let path = log_dir(root.path());
        let guard = SerialLogDir::acquire(path.clone()).await?;
        fs::write(path.join(LOG_NAMES[1]), b"previous run")?;
        guard.finish()?;
        let resumed = SerialLogDir::acquire(path.clone()).await?;
        drop(resumed);
        assert_eq!(
            sweep(root.path(), Some(Duration::ZERO), &AtomicBool::new(false))?,
            0
        );
        assert_eq!(fs::read(path.join(LOG_NAMES[1]))?, b"previous run");
        Ok(())
    }

    #[tokio::test]
    async fn contended_serial_directory_acquisition_can_be_cancelled() -> io::Result<()> {
        let root = tempdir()?;
        let path = log_dir(root.path());
        fs::create_dir(&path)?;
        let collector = open_directory(&path)?;
        lock_file(&collector, libc::LOCK_EX | libc::LOCK_NB)?;
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            SerialLogDir::acquire(path.clone())
        )
        .await
        .is_err());
        drop(collector);
        SerialLogDir::acquire(path).await?.finish()
    }

    #[test]
    fn sweep_preserves_unmanaged_entries_and_does_not_follow_symlinks() -> io::Result<()> {
        let root = tempdir()?;
        let outside = tempdir()?;
        fs::write(outside.path().join(LOG_NAMES[0]), b"outside")?;
        symlink(outside.path(), log_dir(root.path()))?;
        let path = log_dir(root.path());
        fs::create_dir(&path)?;
        symlink(outside.path().join(LOG_NAMES[0]), path.join(LOG_NAMES[0]))?;
        let custom = log_dir(root.path());
        fs::create_dir(&custom)?;
        fs::write(custom.join("operator-note"), b"keep")?;
        fs::write(custom.join(LOG_NAMES[1]), [])?;
        File::create(custom.join(INACTIVE_MARKER))?;
        File::create(path.join(INACTIVE_MARKER))?;
        fs::create_dir(root.path().join("custom-directory"))?;
        fs::write(root.path().join("custom-directory").join(LOG_NAMES[1]), [])?;
        assert_eq!(
            sweep(root.path(), Some(Duration::ZERO), &AtomicBool::new(false))?,
            1
        );
        assert_eq!(fs::read(outside.path().join(LOG_NAMES[0]))?, b"outside");
        assert!(path.join(LOG_NAMES[0]).is_symlink());
        assert!(custom.join("operator-note").exists());
        assert!(root
            .path()
            .join("custom-directory")
            .join(LOG_NAMES[1])
            .exists());
        Ok(())
    }

    #[tokio::test]
    async fn repeated_launch_cleanup_does_not_accumulate_directories() -> io::Result<()> {
        let root = tempdir()?;
        for _ in 0..100 {
            let path = log_dir(root.path());
            let guard = SerialLogDir::acquire(path.clone()).await?;
            fs::write(path.join(LOG_NAMES[0]), [])?;
            fs::write(path.join(LOG_NAMES[1]), [])?;
            guard.finish()?;
        }
        assert_eq!(fs::read_dir(root.path())?.count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn acquisition_and_collection_can_race_on_a_reused_sandbox_id() -> io::Result<()> {
        let root = tempdir()?;
        let path = log_dir(root.path());
        let cancelled = Arc::new(AtomicBool::new(false));
        let collector_cancelled = Arc::clone(&cancelled);
        let collector_root = root.path().to_path_buf();
        let collector = std::thread::spawn(move || -> io::Result<()> {
            while !collector_cancelled.load(Ordering::Relaxed) {
                sweep(&collector_root, Some(Duration::ZERO), &collector_cancelled)?;
                std::thread::yield_now();
            }
            Ok(())
        });
        let result = async {
            for _ in 0..100 {
                let guard = SerialLogDir::acquire(path.clone()).await?;
                fs::write(path.join(LOG_NAMES[0]), b"live")?;
                assert_eq!(fs::read(path.join(LOG_NAMES[0]))?, b"live");
                guard.finish()?;
            }
            Ok(())
        }
        .await;
        cancelled.store(true, Ordering::Relaxed);
        collector.join().expect("collector panicked")?;
        result
    }

    #[tokio::test]
    async fn background_worker_cleans_existing_logs_and_shuts_down() -> anyhow::Result<()> {
        use confique::Config;

        let root = tempdir()?;
        let path = log_dir(root.path());
        fs::create_dir(&path)?;
        fs::write(path.join(LOG_NAMES[1]), [])?;
        File::create(path.join(INACTIVE_MARKER))?;
        assert_eq!(sweep(root.path(), None, &AtomicBool::new(true))?, 0);
        assert!(path.exists());
        let mut config = FirecrackerConfig::builder().load()?;
        assert_eq!(config.serial_log_retention_secs, 7 * 24 * 60 * 60);
        config.serial_dir = Some(root.path().to_path_buf());
        config.serial_log_retention_secs = 0;
        let worker = SerialLogCleanup::start(&config);
        tokio::time::timeout(Duration::from_secs(5), async {
            while path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        tokio::time::timeout(Duration::from_secs(5), worker.shutdown()).await?;
        Ok(())
    }
}
