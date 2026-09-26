use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{sync::oneshot, task::JoinHandle};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id},
    Event, Subscriber,
};
use tracing_subscriber::{layer::Context as LayerContext, registry::LookupSpan, Layer};

use crate::snapshot::{repository::SnapshotRepository, SnapshotId};

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_ENTRIES: usize = 32_768;
const MAX_MESSAGE_BYTES: usize = 16 * 1024;

static ACTIVE_BUILD_LOGS: OnceLock<Mutex<HashMap<String, Weak<Mutex<Buffer>>>>> = OnceLock::new();

fn active_build_logs() -> &'static Mutex<HashMap<String, Weak<Mutex<Buffer>>>> {
    ACTIVE_BUILD_LOGS.get_or_init(Default::default)
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum BuildLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BuildLogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: BuildLogLevel,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

#[derive(Default, Debug)]
struct Buffer {
    entries: Vec<BuildLogEntry>,
    bytes: usize,
    truncated: bool,
    sealed: bool,
}

#[derive(Clone, Default, Debug)]
pub struct BuildLogger(Option<Arc<Mutex<Buffer>>>);

impl BuildLogger {
    pub fn log(&self, level: BuildLogLevel, step: Option<&str>, message: impl Into<String>) {
        let Some(buffer) = &self.0 else { return };
        let mut buffer = buffer.lock().unwrap();
        if buffer.truncated || buffer.sealed {
            return;
        }
        let mut message = message.into();
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
            level = BuildLogLevel::Warn;
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

    pub(crate) fn output(&self, step: Option<&str>, stderr: bool, bytes: &[u8]) {
        for line in String::from_utf8_lossy(bytes).lines() {
            self.log(
                if stderr {
                    BuildLogLevel::Warn
                } else {
                    BuildLogLevel::Info
                },
                step,
                line,
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BuildLogLayer;

#[derive(Clone, Debug)]
struct BuildSpan {
    build_id: String,
}

#[derive(Default)]
struct FieldVisitor {
    build_id: Option<String>,
    message: Option<String>,
    fields: Vec<String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}");
        match field.name() {
            "build_id" => self.build_id = Some(value.trim_matches('"').to_owned()),
            "message" => self.message = Some(value),
            name => self.fields.push(format!("{name}={value}")),
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "build_id" => self.build_id = Some(value.to_owned()),
            "message" => self.message = Some(value.to_owned()),
            name => self.fields.push(format!("{name}={value:?}")),
        }
    }
}

impl<S> Layer<S> for BuildLogLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        let inherited = if attrs.is_contextual() {
            ctx.lookup_current()
        } else {
            attrs.parent().and_then(|parent| ctx.span(parent))
        }
        .and_then(|span| {
            span.extensions()
                .get::<BuildSpan>()
                .map(|build| build.build_id.clone())
        });
        let Some(build_id) = visitor.build_id.or(inherited) else {
            return;
        };
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(BuildSpan { build_id });
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        if !event.metadata().target().starts_with("agentenv::") {
            return;
        }
        let Some(build_id) = ctx.event_scope(event).and_then(|scope| {
            scope.from_root().find_map(|span| {
                span.extensions()
                    .get::<BuildSpan>()
                    .map(|build| build.build_id.clone())
            })
        }) else {
            return;
        };
        let buffer = active_build_logs()
            .lock()
            .unwrap()
            .get(&build_id)
            .and_then(Weak::upgrade);
        let Some(buffer) = buffer else { return };

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let mut message = visitor
            .message
            .unwrap_or_else(|| event.metadata().name().to_owned());
        if !visitor.fields.is_empty() {
            message.push(' ');
            message.push_str(&visitor.fields.join(" "));
        }
        let level = match *event.metadata().level() {
            tracing::Level::DEBUG | tracing::Level::TRACE => BuildLogLevel::Debug,
            tracing::Level::INFO => BuildLogLevel::Info,
            tracing::Level::WARN => BuildLogLevel::Warn,
            tracing::Level::ERROR => BuildLogLevel::Error,
        };
        BuildLogger(Some(buffer)).log(level, None, message);
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
    pub fn start(
        &self,
        id: SnapshotId,
        repository: Arc<dyn SnapshotRepository>,
    ) -> BuildLogSession {
        let buffer = Arc::new(Mutex::new(Buffer::default()));
        self.buffers
            .lock()
            .unwrap()
            .insert(id.clone(), buffer.clone());
        active_build_logs()
            .lock()
            .unwrap()
            .insert(id.to_string(), Arc::downgrade(&buffer));
        let logger = BuildLogger(Some(buffer.clone()));
        let (stop, mut stopped) = oneshot::channel();
        let buffers = self.buffers.clone();
        let writer = tokio::spawn(async move {
            let mut offset = 0;
            let result = loop {
                let finishing = tokio::select! {
                    _ = &mut stopped => true,
                    _ = tokio::time::sleep(Duration::from_millis(1000)) => false,
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
            let mut active = active_build_logs().lock().unwrap();
            if active
                .get(&id.to_string())
                .and_then(Weak::upgrade)
                .is_some_and(|current| Arc::ptr_eq(&current, &buffer))
            {
                active.remove(&id.to_string());
            }
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
        let mut active = active_build_logs().lock().unwrap();
        if active
            .get(&id.to_string())
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, &removed))
        {
            active.remove(&id.to_string());
        }
    }
}

async fn flush(
    repository: &Arc<dyn SnapshotRepository>,
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
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};

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
                BuildLogLevel::Debug,
                BuildLogLevel::Info,
                BuildLogLevel::Warn,
                BuildLogLevel::Error,
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
                .log(BuildLogLevel::Info, None, format!("line {n}"));
        }
        assert!(logs.temporary(&id).is_some());
        let logger = session.logger.clone();
        session.finish().await?;
        assert!(logs.temporary(&id).is_none());
        logger.log(
            BuildLogLevel::Info,
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
        session
            .logger
            .log(BuildLogLevel::Info, None, "interrupted task");
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
        let logger = BuildLogger(Some(buffer.clone()));
        for _ in 0..MAX_ENTRIES + 2 {
            logger.log(BuildLogLevel::Info, None, "");
        }
        let buffer = buffer.lock().unwrap();
        assert_eq!(buffer.entries.len(), MAX_ENTRIES + 1);
        assert_eq!(buffer.entries.last().unwrap().level, BuildLogLevel::Warn);
    }
}
