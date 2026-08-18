use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use tokio::time::Instant;
use tracing::warn;

/// Total time budget for retrying a RocksDB open that fails because the
/// database lock is still held by a leftover process from a previous server
/// incarnation (for example, an old container that has not finished
/// terminating while a replacement pod already started on the same hostPath).
/// Without this, a transient overlap turns into a CrashLoopBackOff even though
/// the lock is released as soon as the previous process exits.
const LOCK_RETRY_BUDGET: Duration = Duration::from_secs(120);

/// Durability policy for writes made through [`LocalKvStore`].
///
/// The levels intentionally expose only the knobs AgentENV needs instead of
/// leaking RocksDB's full `WriteOptions` surface to callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalStoreDurability {
    /// Skip the write-ahead log.
    ///
    /// This is useful for tests and regenerable local indexes where process or
    /// machine crashes may lose the latest writes.
    Memory,
    /// Use RocksDB's write-ahead log without fsyncing each write.
    ///
    /// This is a good default for local catalogs that should survive normal
    /// process restarts but do not need power-loss guarantees.
    Wal,
    /// Use the write-ahead log and sync each write before it is acknowledged.
    ///
    /// This is intended for small, critical metadata records where recovery is
    /// more important than write throughput.
    Sync,
}

impl LocalStoreDurability {
    /// Build the RocksDB write options for this durability policy.
    pub fn write_options(self) -> WriteOptions {
        let mut options = WriteOptions::new();
        options.disable_wal(matches!(self, Self::Memory));
        options.set_sync(matches!(self, Self::Sync));
        options
    }
}

/// Small async-friendly wrapper around a single RocksDB key-value database.
///
/// RocksDB's Rust API is synchronous. All potentially blocking database calls
/// are run on Tokio's blocking thread pool so callers can use this helper from
/// async orchestrator and P2P paths without parking a runtime worker thread.
#[derive(Clone)]
pub struct LocalKvStore {
    db: Arc<DB>,
    durability: LocalStoreDurability,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalKvBatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl LocalKvBatchOp {
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self::Delete { key: key.into() }
    }
}

impl std::fmt::Debug for LocalKvStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalKvStore")
            .field("durability", &self.durability)
            .finish_non_exhaustive()
    }
}

impl LocalKvStore {
    /// Open or create a RocksDB database at `path`.
    ///
    /// The parent directory is created automatically. The database itself uses
    /// RocksDB's default column family and stores opaque byte keys and values.
    ///
    /// If the database lock is still held by a leftover process from a
    /// previous server incarnation, the open is retried with exponential
    /// backoff for up to [`LOCK_RETRY_BUDGET`] before failing.
    pub async fn open(
        path: impl Into<PathBuf>,
        durability: LocalStoreDurability,
    ) -> anyhow::Result<Self> {
        Self::open_with_lock_retry_budget(path.into(), durability, LOCK_RETRY_BUDGET).await
    }

    async fn open_with_lock_retry_budget(
        path: PathBuf,
        durability: LocalStoreDurability,
        lock_retry_budget: Duration,
    ) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create RocksDB parent dir {}", parent.display()))?;
        }

        let started = Instant::now();
        let mut backoff = Duration::from_millis(500);
        let db = loop {
            let attempt_path = path.clone();
            let result = tokio::task::spawn_blocking(move || {
                let mut options = Options::default();
                options.create_if_missing(true);
                DB::open(&options, &attempt_path)
                    .with_context(|| format!("open RocksDB {}", attempt_path.display()))
            })
            .await
            .context("join RocksDB open task")?;

            match result {
                Ok(db) => break db,
                Err(err) => {
                    let elapsed = started.elapsed();
                    if !is_lock_contention(&err) || elapsed >= lock_retry_budget {
                        return Err(err);
                    }
                    warn!(
                        path = %path.display(),
                        error = %err,
                        "RocksDB lock still held by another process; retrying"
                    );
                    tokio::time::sleep(backoff.min(lock_retry_budget - elapsed)).await;
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        };

        Ok(Self {
            db: Arc::new(db),
            durability,
        })
    }

    /// Read a value by key.
    ///
    /// Returns `Ok(None)` when the key is absent.
    pub async fn get(&self, key: impl Into<Vec<u8>>) -> anyhow::Result<Option<Vec<u8>>> {
        let db = Arc::clone(&self.db);
        let key = key.into();
        tokio::task::spawn_blocking(move || db.get(key).context("get RocksDB value"))
            .await
            .context("join RocksDB get task")?
    }

    /// Insert or replace a key-value pair using the store's durability policy.
    pub async fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let db = Arc::clone(&self.db);
        let key = key.into();
        let value = value.into();
        let write_options = self.durability.write_options();
        tokio::task::spawn_blocking(move || {
            db.put_opt(key, value, &write_options)
                .context("put RocksDB value")
        })
        .await
        .context("join RocksDB put task")?
    }

    /// Remove a key using the store's durability policy.
    ///
    /// Deleting a missing key is treated as success by RocksDB.
    pub async fn delete(&self, key: impl Into<Vec<u8>>) -> anyhow::Result<()> {
        let db = Arc::clone(&self.db);
        let key = key.into();
        let write_options = self.durability.write_options();
        tokio::task::spawn_blocking(move || {
            db.delete_opt(key, &write_options)
                .context("delete RocksDB value")
        })
        .await
        .context("join RocksDB delete task")?
    }

    /// Apply multiple key mutations atomically using the store's durability policy.
    pub async fn write_batch(
        &self,
        ops: impl IntoIterator<Item = LocalKvBatchOp>,
    ) -> anyhow::Result<()> {
        let ops = ops.into_iter().collect::<Vec<_>>();
        if ops.is_empty() {
            return Ok(());
        }

        let db = Arc::clone(&self.db);
        let write_options = self.durability.write_options();
        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            for op in ops {
                match op {
                    LocalKvBatchOp::Put { key, value } => batch.put(key, value),
                    LocalKvBatchOp::Delete { key } => batch.delete(key),
                }
            }
            db.write_opt(batch, &write_options)
                .context("write RocksDB batch")
        })
        .await
        .context("join RocksDB batch task")?
    }

    /// Iterate over all entries in key order and accumulate a result.
    ///
    /// The callback runs on a blocking thread and must remain synchronous. Use
    /// this when entries can be decoded or accumulated without awaiting. If the
    /// caller needs async cleanup or side effects per entry, use [`Self::entries`]
    /// and process the returned vector in async code.
    pub async fn fold<T, F>(&self, mut init: T, mut visit: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnMut(&mut T, Vec<u8>, Vec<u8>) -> anyhow::Result<()> + Send + 'static,
    {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            for item in db.iterator(IteratorMode::Start) {
                let (key, value) = item.context("iterate RocksDB values")?;
                visit(&mut init, key.into_vec(), value.into_vec())?;
            }
            Ok(init)
        })
        .await
        .context("join RocksDB iterator task")?
    }

    /// Load all key-value pairs into memory.
    ///
    /// This is intentionally explicit for call sites that need to leave the
    /// blocking iterator before doing async work.
    pub async fn entries(&self) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.fold(Vec::new(), |entries, key, value| {
            entries.push((key, value));
            Ok(())
        })
        .await
    }

    /// Load all entries whose key starts with `prefix`, in key order.
    ///
    /// Seeks to `prefix` and stops at the first key that no longer matches, so
    /// prefix-scoped lookups don't load the whole database the way
    /// [`Self::entries`] does.
    pub async fn scan_prefix(
        &self,
        prefix: impl Into<Vec<u8>>,
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = Arc::clone(&self.db);
        let prefix = prefix.into();
        tokio::task::spawn_blocking(move || {
            let mut entries = Vec::new();
            for item in db.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
                let (key, value) = item.context("iterate RocksDB prefix")?;
                if !key.starts_with(&prefix) {
                    break;
                }
                entries.push((key.into_vec(), value.into_vec()));
            }
            Ok(entries)
        })
        .await
        .context("join RocksDB prefix scan task")?
    }
}

/// Returns `true` when `err` looks like RocksDB failing to acquire the
/// database `LOCK` file because another process still holds it. RocksDB
/// surfaces this as an IO error such as
/// `lock hold by current process ... /LOCK: No locks available` (same host)
/// or `While lock file: <path>/LOCK: Resource temporarily unavailable`.
fn is_lock_contention(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let message = cause.to_string().to_ascii_lowercase();
        message.contains("lock")
            && (message.contains("no locks available")
                || message.contains("resource temporarily unavailable"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn lock_contention_detection_matches_rocksdb_lock_errors() {
        let lock_err = anyhow::anyhow!(
            "IO error: While lock file: /var/lib/aenv/env/persisted-sandboxes/records.db/LOCK: Resource temporarily unavailable"
        );
        assert!(is_lock_contention(&lock_err));

        let same_host_err = anyhow::anyhow!(
            "IO error: lock hold by current process, acquire time 1700000000 acquiring thread 42: /var/lib/aenv/env/persisted-sandboxes/records.db/LOCK: No locks available"
        );
        assert!(is_lock_contention(&same_host_err));

        let other_err = anyhow::anyhow!("IO error: No such file or directory");
        assert!(!is_lock_contention(&other_err));
    }

    #[tokio::test]
    async fn open_retries_until_lock_is_released() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("records.db");
        let holder = LocalKvStore::open(&db_path, LocalStoreDurability::Memory).await?;

        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            drop(holder);
        });
        let reopened = LocalKvStore::open_with_lock_retry_budget(
            db_path,
            LocalStoreDurability::Memory,
            Duration::from_secs(30),
        )
        .await?;
        release.await?;

        reopened.put(b"key".to_vec(), b"value".to_vec()).await?;
        assert_eq!(
            reopened.get(b"key".to_vec()).await?,
            Some(b"value".to_vec())
        );
        Ok(())
    }

    #[tokio::test]
    async fn open_fails_after_lock_retry_budget_is_exhausted() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("records.db");
        let _holder = LocalKvStore::open(&db_path, LocalStoreDurability::Memory).await?;

        let started = Instant::now();
        let err = LocalKvStore::open_with_lock_retry_budget(
            db_path,
            LocalStoreDurability::Memory,
            Duration::from_millis(1200),
        )
        .await
        .expect_err("open should keep failing while the lock is held");

        assert!(is_lock_contention(&err));
        assert!(started.elapsed() >= Duration::from_millis(1200));
        Ok(())
    }
}
