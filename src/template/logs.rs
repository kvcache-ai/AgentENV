use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{sync::oneshot, task::JoinHandle};

pub use crate::logging::BuildLogger;
use crate::logging::{BuildLogSink, LogLevel};
use crate::repository::{DocumentKey, Repository, RepositoryError, RepositoryResult};
use crate::snapshot::SnapshotId;

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 32_768;
const MAX_MESSAGE_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BuildLogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

/// Typed access to persisted build logs.
#[async_trait]
pub trait BuildLogStore {
    /// Atomically replaces the complete build log file.
    async fn write_build_logs(
        &self,
        id: &SnapshotId,
        entries: Vec<BuildLogEntry>,
    ) -> RepositoryResult<()>;

    /// Reads the complete build log file, which is empty when absent.
    async fn read_build_logs(&self, id: &SnapshotId) -> RepositoryResult<Vec<BuildLogEntry>>;
}

#[async_trait]
impl<R: Repository + ?Sized> BuildLogStore for R {
    async fn write_build_logs(
        &self,
        id: &SnapshotId,
        entries: Vec<BuildLogEntry>,
    ) -> RepositoryResult<()> {
        let bytes = serde_json::to_vec(&entries)
            .map_err(|error| RepositoryError::backend("encode build logs", error))?;
        self.put_document(DocumentKey::BuildLog(id.clone()), bytes)
            .await
    }

    async fn read_build_logs(&self, id: &SnapshotId) -> RepositoryResult<Vec<BuildLogEntry>> {
        let Some(bytes) = self.get_document(DocumentKey::BuildLog(id.clone())).await? else {
            return Ok(Vec::new());
        };
        serde_json::from_slice(&bytes)
            .map_err(|error| RepositoryError::backend("decode build logs", error))
    }
}

#[derive(Default, Debug)]
struct Buffer {
    entries: Vec<BuildLogEntry>,
    bytes: usize,
    truncated: bool,
    sealed: bool,
}

impl BuildLogSink for Mutex<Buffer> {
    fn log(&self, level: LogLevel, step: Option<&str>, message: String) {
        let mut buffer = self.lock().unwrap();
        if buffer.truncated || buffer.sealed {
            return;
        }
        let mut message = message;
        if message.len() > MAX_MESSAGE_BYTES {
            let mut end = MAX_MESSAGE_BYTES;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
            message.push_str(" [truncated]");
        }
        let mut level = level;
        let mut step = step.map(str::to_owned);
        if buffer.entries.len() >= MAX_ENTRIES || buffer.bytes + message.len() > MAX_BYTES {
            message = "Build log limit reached; subsequent output is omitted".into();
            level = LogLevel::Warn;
            step = None;
            buffer.truncated = true;
        }
        // Clock adjustments and concurrent producers must not change offset order.
        let timestamp = buffer
            .entries
            .last()
            .map_or_else(Utc::now, |last| Utc::now().max(last.timestamp));
        buffer.bytes += message.len();
        buffer.entries.push(BuildLogEntry {
            timestamp,
            level,
            message,
            step,
        });
    }
}

#[derive(Clone, Default)]
pub(crate) struct BuildLogs {
    buffers: Arc<Mutex<HashMap<SnapshotId, Arc<Mutex<Buffer>>>>>,
}

pub(crate) struct BuildLogSession {
    pub logger: BuildLogger,
    stop: Option<oneshot::Sender<()>>,
    writer: JoinHandle<Result<()>>,
}

impl BuildLogSession {
    pub async fn finish(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.writer.await.context("join build log writer")?
    }
}

impl BuildLogs {
    pub fn start(&self, id: SnapshotId, repository: Arc<dyn Repository>) -> BuildLogSession {
        self.start_inner(id, repository, Duration::from_secs(1))
    }

    #[cfg(test)]
    pub(crate) fn start_with_flush_interval(
        &self,
        id: SnapshotId,
        repository: Arc<dyn Repository>,
        flush_interval: Duration,
    ) -> BuildLogSession {
        self.start_inner(id, repository, flush_interval)
    }

    fn start_inner(
        &self,
        id: SnapshotId,
        repository: Arc<dyn Repository>,
        flush_interval: Duration,
    ) -> BuildLogSession {
        let buffer = Arc::new(Mutex::new(Buffer::default()));
        self.buffers
            .lock()
            .unwrap()
            .insert(id.clone(), buffer.clone());
        let sink = Arc::downgrade(&buffer) as Weak<dyn BuildLogSink>;
        crate::logging::route_build_logs(id.to_string(), sink.clone());
        let logger = BuildLogger::new(buffer.clone());
        let (stop, mut stopped) = oneshot::channel();
        let buffers = self.buffers.clone();
        let writer = tokio::spawn(async move {
            let mut offset = 0;
            let result = loop {
                let finishing = tokio::select! {
                    _ = &mut stopped => true,
                    _ = tokio::time::sleep(flush_interval) => false,
                };
                if finishing {
                    buffer.lock().unwrap().sealed = true;
                }
                let mut result = flush(&repository, &id, &buffer, &mut offset).await;
                if finishing {
                    if result.is_err() {
                        for _ in 0..3 {
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                            result = flush(&repository, &id, &buffer, &mut offset).await;
                            if result.is_ok() {
                                break;
                            }
                        }
                    }
                    break result;
                }
                if let Err(error) = result {
                    tracing::warn!(build_id = %id, %error, "build log flush failed; will retry");
                }
            };
            let mut buffers = buffers.lock().unwrap();
            if buffers
                .get(&id)
                .is_some_and(|current| Arc::ptr_eq(current, &buffer))
            {
                buffers.remove(&id);
            }
            crate::logging::stop_build_log_routing(&id.to_string(), &sink);
            result
        });
        BuildLogSession {
            logger,
            stop: Some(stop),
            writer,
        }
    }

    pub fn temporary(&self, id: &SnapshotId) -> Option<Vec<BuildLogEntry>> {
        self.buffers
            .lock()
            .unwrap()
            .get(id)
            .map(|b| b.lock().unwrap().entries.clone())
    }

    pub fn remove(&self, id: &SnapshotId) {
        let removed = self.buffers.lock().unwrap().remove(id);
        let Some(removed) = removed else { return };
        let sink = Arc::downgrade(&removed) as Weak<dyn BuildLogSink>;
        crate::logging::stop_build_log_routing(&id.to_string(), &sink);
    }
}

async fn flush(
    repository: &Arc<dyn Repository>,
    id: &SnapshotId,
    buffer: &Mutex<Buffer>,
    offset: &mut usize,
) -> Result<()> {
    let entries = {
        let buffer = buffer.lock().unwrap();
        if buffer.entries.len() == *offset {
            return Ok(());
        }
        buffer.entries.clone()
    };
    let end = entries.len();
    repository.write_build_logs(id, entries).await?;
    *offset = end;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::BuildLogLayer;
    use crate::snapshot::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::SnapshotCatalog;

    #[tokio::test]
    async fn tracing_layer_routes_only_agentenv_events_from_the_matching_build_span() -> Result<()>
    {
        use tracing_subscriber::layer::SubscriberExt;

        let root = tempfile::tempdir()?;
        let repository = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.path().join("repository"),
            cache_root: Some(root.path().join("cache")),
            runtime_cache_root: None,
        })?
        .repository();
        let id = SnapshotId::generate();
        let logs = BuildLogs::default();
        let session = logs.start(id.clone(), repository.clone());
        let subscriber = tracing_subscriber::registry().with(BuildLogLayer);

        tracing::subscriber::with_default(subscriber, || {
            let build = tracing::info_span!("template_build", build_id = %id);
            build.in_scope(|| {
                tracing::debug!(answer = 42, "debug event");
                tracing::info!("info event");
                tracing::warn!("warn event");
                tracing::error!("error event");
                tracing::debug!(target: "hyper_util::client", "third-party event");
            });
            tracing::info!("outside build");
        });

        let entries = logs.temporary(&id).expect("active build buffer");
        assert_eq!(
            entries.iter().map(|entry| entry.level).collect::<Vec<_>>(),
            vec![
                LogLevel::Debug,
                LogLevel::Info,
                LogLevel::Warn,
                LogLevel::Error,
            ]
        );
        assert!(entries[0].message.contains("debug event"));
        assert!(entries[0].message.contains("answer=42"));
        assert!(!entries
            .iter()
            .any(|entry| entry.message.contains("third-party")));
        assert!(!entries
            .iter()
            .any(|entry| entry.message.contains("outside build")));

        session.finish().await?;
        assert_eq!(repository.read_build_logs(&id).await?, entries);
        Ok(())
    }

    #[tokio::test]
    async fn build_logs_persist_in_one_file_and_survive_new_reader_and_delete() -> Result<()> {
        let root = tempfile::tempdir()?;
        let config = PosixFsBackendConfig {
            root: root.path().join("repository"),
            cache_root: Some(root.path().join("cache")),
            runtime_cache_root: None,
        };
        let repository = PosixFsBackend::new(config.clone())?.repository();
        let id = SnapshotId::generate();
        repository
            .create(crate::snapshot::SnapshotRecord::template_waiting(
                id.clone(),
                None,
                crate::types::SandboxResources::default(),
            ))
            .await?;
        let logs = BuildLogs::default();
        let session = logs.start(id.clone(), repository.clone());
        for n in 0..600 {
            session
                .logger
                .log(LogLevel::Info, None, format!("line {n}"));
        }
        assert!(logs.temporary(&id).is_some());
        let logger = session.logger.clone();
        session.finish().await?;
        assert!(logs.temporary(&id).is_none());
        logger.log(
            LogLevel::Info,
            None,
            "late write must not change the closed stream",
        );
        let reader = PosixFsBackend::new(config)?.repository();
        let persisted = reader.read_build_logs(&id).await?;
        assert_eq!(persisted.len(), 600);
        repository.write_build_logs(&id, persisted.clone()).await?;
        assert_eq!(reader.read_build_logs(&id).await?, persisted);
        assert!(root
            .path()
            .join("repository/snapshots/build-logs")
            .join(format!("{id}.json"))
            .is_file());
        repository.delete(&id.to_string()).await?;
        assert!(reader.read_build_logs(&id).await?.is_empty());

        // Deleting by ID also cleans an orphaned log after its catalog record is gone.
        repository.write_build_logs(&id, persisted).await?;
        repository.delete(&id.to_string()).await?;
        assert!(reader.read_build_logs(&id).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn build_logs_dropped_session_flushes_and_releases_buffer() -> Result<()> {
        let root = tempfile::tempdir()?;
        let repository = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.path().join("repository"),
            cache_root: Some(root.path().join("cache")),
            runtime_cache_root: None,
        })?
        .repository();
        let id = SnapshotId::generate();
        let logs = BuildLogs::default();
        let session = logs.start(id.clone(), repository.clone());
        session.logger.log(LogLevel::Info, None, "interrupted task");
        drop(session);
        tokio::time::timeout(Duration::from_secs(5), async {
            while logs.temporary(&id).is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert_eq!(repository.read_build_logs(&id).await?.len(), 1);
        Ok(())
    }

    #[test]
    fn build_log_limits_emit_one_notice() {
        let buffer = Arc::new(Mutex::new(Buffer::default()));
        let logger = BuildLogger::new(buffer.clone());
        for _ in 0..MAX_ENTRIES + 2 {
            logger.log(LogLevel::Info, None, "");
        }
        let buffer = buffer.lock().unwrap();
        assert_eq!(buffer.entries.len(), MAX_ENTRIES + 1);
        assert_eq!(buffer.entries.last().unwrap().level, LogLevel::Warn);
    }
}
