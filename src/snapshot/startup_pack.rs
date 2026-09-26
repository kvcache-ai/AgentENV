//! Startup memory pack artifact constants and the committed-record
//! descriptor. The wire format itself lives in the overlaybd crate (shared
//! with the ublk daemon); this module re-exports what agentenv needs.
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// File name of the per-snapshot startup memory pack stored under
/// `artifacts/{snapshot_id}/` in the snapshot repository.
pub const MEMORY_STARTUP_PACK_ARTIFACT: &str = "memory-startup.pack";

/// File name of the local first-touch trace the recorder emits next to the
/// snapshot artifacts. The publisher expands it into the v3 startup manifest
/// (exact-order prefix plus merged ranges); it never leaves the node as-is.
pub const MEMORY_STARTUP_TRACE_ARTIFACT: &str = "memory-startup.trace";

/// A startup-manifest recording in flight plus the lease that keeps the
/// captured artifacts alive while the manifest is being built and uploaded
/// (the whole continuation can outlive the synchronous publish flow).
pub struct StartupRecording {
    /// Completes with the trace path on success, `None` on any failure.
    pub trace: tokio::task::JoinHandle<Option<std::path::PathBuf>>,
    /// Opaque hold-only lease (the capture root guard for sandbox captures;
    /// a unit placeholder for template builds).
    pub keep_alive: Box<dyn std::any::Any + Send>,
}

impl StartupRecording {
    /// Give both the recorder and its later publisher a capture-root lease.
    /// Dropping the publication future detaches the recorder, not its lease.
    pub(crate) fn spawn_with_capture_lease<F, T>(recorder: F, lease: std::sync::Arc<T>) -> Self
    where
        F: std::future::Future<Output = Option<PathBuf>> + Send + 'static,
        T: std::any::Any + Send + Sync + 'static,
    {
        let task_lease = std::sync::Arc::clone(&lease);
        Self {
            trace: tokio::spawn(async move {
                let _task_lease = task_lease;
                recorder.await
            }),
            keep_alive: Box::new(lease),
        }
    }
}

// ── Shutdown coordination for detached manifest work ────────────────────────

use std::sync::{Arc, Mutex, OnceLock};

/// Process-global coordination for detached startup-manifest work. Graceful
/// shutdown first stops NEW continuations from spawning, then drains the
/// in-flight ones (bounded): their recordings complete and manifests land
/// when possible. A drain timeout sends an abort notification; tasks still
/// own their leases until their own recorder and continuation cleanup ends.
struct StartupManifestShutdown {
    state: Mutex<StartupManifestShutdownState>,
    abort: tokio::sync::watch::Sender<bool>,
    in_flight: tokio::sync::watch::Sender<usize>,
}

#[derive(Default)]
struct StartupManifestShutdownState {
    stop_new: bool,
    abort: bool,
    in_flight: usize,
}

impl StartupManifestShutdown {
    fn new() -> Self {
        Self {
            state: Mutex::new(StartupManifestShutdownState::default()),
            abort: tokio::sync::watch::channel(false).0,
            in_flight: tokio::sync::watch::channel(0).0,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StartupManifestShutdownState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn try_register(self: &Arc<Self>) -> Option<StartupManifestTaskGuard> {
        let mut state = self.lock();
        if state.stop_new {
            return None;
        }
        state.in_flight += 1;
        self.in_flight.send_replace(state.in_flight);
        Some(StartupManifestTaskGuard {
            state: Arc::clone(self),
        })
    }

    async fn drain(&self, timeout: std::time::Duration) {
        let mut receiver = self.in_flight.subscribe();
        self.lock().stop_new = true;
        let drained = async {
            while *receiver.borrow_and_update() != 0 {
                if receiver.changed().await.is_err() {
                    break;
                }
            }
        };
        if tokio::time::timeout(timeout, drained).await.is_err() {
            let mut inner = self.lock();
            if inner.in_flight != 0 {
                inner.abort = true;
                self.abort.send_replace(true);
            }
        }
    }
}

fn startup_manifest_shutdown() -> &'static Arc<StartupManifestShutdown> {
    static SHUTDOWN: OnceLock<Arc<StartupManifestShutdown>> = OnceLock::new();
    SHUTDOWN.get_or_init(|| Arc::new(StartupManifestShutdown::new()))
}

/// True once shutdown was requested: the publish flow must not spawn new
/// startup-manifest continuations after this point.
pub fn startup_manifest_shutdown_requested() -> bool {
    startup_manifest_shutdown().lock().stop_new
}

/// True once the drain timed out and remaining work was told to cancel.
pub fn startup_manifest_abort_requested() -> bool {
    startup_manifest_shutdown().lock().abort
}

/// Notified once [`startup_manifest_abort_requested`] turns true.
pub async fn startup_manifest_abort_notify() {
    let mut receiver = startup_manifest_shutdown().abort.subscribe();
    while !*receiver.borrow_and_update() {
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

/// Request shutdown: stop spawning new startup-manifest continuations, then
/// wait (bounded by `timeout`) for the in-flight ones to drain so their
/// manifests still land. A timeout requests cancellation but does not
/// synchronously revoke resources still owned by those tasks.
pub async fn drain_startup_manifest_tasks(timeout: std::time::Duration) {
    startup_manifest_shutdown().drain(timeout).await;
}

/// RAII guard counting one in-flight startup-manifest continuation.
pub(crate) struct StartupManifestTaskGuard {
    state: Arc<StartupManifestShutdown>,
}

impl StartupManifestTaskGuard {
    pub(crate) fn try_register() -> Option<Self> {
        startup_manifest_shutdown().try_register()
    }
}

impl Drop for StartupManifestTaskGuard {
    fn drop(&mut self) {
        let mut inner = self.state.lock();
        inner.in_flight -= 1;
        self.state.in_flight.send_replace(inner.in_flight);
    }
}

/// Startup pack descriptor persisted in the committed record when — and only
/// when — the pack was recorded and its repository backend saved the manifest.
/// It is absent for older snapshots and best-effort recording failures.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryStartupPackInfo {
    pub pack_size: u64,
    pub mem_virtual_size: u64,
    /// Hex-encoded sha256 of the pack index section (v1: page offsets + block
    /// crcs; v2: object table + entry table + frame crcs). The consumer
    /// verifies the received index against this digest before trusting any
    /// entry.
    pub index_sha256: String,
}

/// Runtime-only startup pack reference handed from a repository resolver to
/// sandbox start. The source explicitly distinguishes OSS URLs from local
/// POSIX paths. Never trusted on its own: the consumer verifies the pack's
/// size, digest and memory geometry before use.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolvedStartupPackSource {
    OssUrl(String),
    LocalPath(PathBuf),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedStartupPack {
    pub source: ResolvedStartupPackSource,
    pub pack_size: u64,
    pub index_sha256: String,
    pub mem_virtual_size: u64,
}

/// Gate a committed descriptor into a runtime reference, only when
/// consumption is enabled. Missing descriptors and disabled consumption
/// fall back to plain on-demand resume; a manifest the consumer cannot
/// parse (including an unknown format version) is skipped the same way.
pub fn resolve_startup_pack_ref(
    info: Option<&MemoryStartupPackInfo>,
    consume_enabled: bool,
    url: impl FnOnce() -> String,
) -> Option<ResolvedStartupPack> {
    if !consume_enabled {
        return None;
    }
    let info = info?;
    Some(ResolvedStartupPack {
        source: ResolvedStartupPackSource::OssUrl(url()),
        pack_size: info.pack_size,
        index_sha256: info.index_sha256.clone(),
        mem_virtual_size: info.mem_virtual_size,
    })
}

/// Resolve a POSIX repository manifest to an explicit local source.
pub fn resolve_local_startup_pack_ref(
    info: Option<&MemoryStartupPackInfo>,
    consume_enabled: bool,
    path: PathBuf,
) -> Option<ResolvedStartupPack> {
    if !consume_enabled {
        return None;
    }
    let info = info?;
    Some(ResolvedStartupPack {
        source: ResolvedStartupPackSource::LocalPath(path),
        pack_size: info.pack_size,
        index_sha256: info.index_sha256.clone(),
        mem_virtual_size: info.mem_virtual_size,
    })
}

pub(crate) fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_admission_and_abort_are_race_free() {
        let coordinator = Arc::new(StartupManifestShutdown::new());
        let guard = coordinator
            .try_register()
            .expect("admitted before shutdown");
        let mut in_flight = coordinator.in_flight.subscribe();
        coordinator.drain(std::time::Duration::ZERO).await;
        assert!(coordinator.try_register().is_none());
        assert_eq!(*in_flight.borrow_and_update(), 1);
        let mut abort = coordinator.abort.subscribe();
        assert!(
            *abort.borrow_and_update(),
            "late subscriber sees prior abort"
        );
        drop(guard);
        in_flight
            .changed()
            .await
            .expect("guard release is observable");
        assert_eq!(*in_flight.borrow_and_update(), 0);
    }

    #[tokio::test]
    async fn drain_observes_last_guard_release_without_lost_notification() {
        let coordinator = Arc::new(StartupManifestShutdown::new());
        let guard = coordinator.try_register().expect("admitted");
        let draining = Arc::clone(&coordinator);
        let task = tokio::spawn(async move {
            draining.drain(std::time::Duration::from_secs(1)).await;
        });
        while !coordinator.lock().stop_new {
            tokio::task::yield_now().await;
        }
        assert!(coordinator.try_register().is_none());
        drop(guard);
        task.await.expect("drain task");
        assert_eq!(coordinator.lock().in_flight, 0);
        assert!(!coordinator.lock().abort);
    }

    #[test]
    fn committed_snapshot_without_memory_startup_deserializes() {
        // A record written before the field existed (mock() skips it) must
        // still parse, with the field defaulting to None.
        let json = serde_json::to_value(crate::snapshot::CommittedSnapshot::mock())
            .expect("serialize mock");
        assert!(json.get("memory_startup").is_none());
        let committed: crate::snapshot::CommittedSnapshot =
            serde_json::from_value(json).expect("old record must parse");
        assert!(committed.memory_startup.is_none());
    }

    #[test]
    fn committed_snapshot_with_memory_startup_roundtrips() {
        let mut committed = crate::snapshot::CommittedSnapshot::mock();
        committed.memory_startup = Some(MemoryStartupPackInfo {
            pack_size: 1024,
            mem_virtual_size: 1 << 30,
            index_sha256: "ab".repeat(32),
        });
        let json = serde_json::to_value(&committed).expect("serialize");
        let back: crate::snapshot::CommittedSnapshot = serde_json::from_value(json).expect("parse");
        assert_eq!(back.memory_startup, committed.memory_startup);
    }

    #[test]
    fn resolve_startup_pack_ref_gates_only_consumption() {
        let info = MemoryStartupPackInfo {
            pack_size: 4096,
            mem_virtual_size: 1 << 30,
            index_sha256: "ab".repeat(32),
        };
        // Consumption disabled → no reference.
        assert!(crate::snapshot::startup_pack::resolve_startup_pack_ref(
            Some(&info),
            false,
            || "s3://b/k".to_string()
        )
        .is_none());
        // No descriptor → no reference.
        assert!(
            crate::snapshot::startup_pack::resolve_startup_pack_ref(None, true, || "s3://b/k"
                .to_string())
            .is_none()
        );
        // Any descriptor with consumption enabled resolves; the daemon
        // rejects non-manifest objects by magic instead.
        let resolved =
            crate::snapshot::startup_pack::resolve_startup_pack_ref(Some(&info), true, || {
                "s3://bucket/aenv-bk/artifacts/id/memory-startup.pack".to_string()
            })
            .expect("a descriptor with consumption enabled must resolve");
        assert_eq!(resolved.pack_size, 4096);
        assert_eq!(resolved.mem_virtual_size, 1 << 30);
        assert_eq!(resolved.index_sha256, "ab".repeat(32));
        assert!(
            matches!(resolved.source, ResolvedStartupPackSource::OssUrl(ref url) if url.ends_with("memory-startup.pack"))
        );
    }

    #[test]
    fn resolve_local_startup_pack_gates_consumption_and_marks_local_source() {
        let info = MemoryStartupPackInfo {
            pack_size: 4096,
            mem_virtual_size: 1 << 30,
            index_sha256: "ab".repeat(32),
        };
        assert!(
            resolve_local_startup_pack_ref(Some(&info), false, PathBuf::from("/repo/pack"))
                .is_none()
        );
        assert!(resolve_local_startup_pack_ref(None, true, PathBuf::from("/repo/pack")).is_none());
        let resolved = resolve_local_startup_pack_ref(
            Some(&info),
            true,
            PathBuf::from("/repo/artifacts/id/memory-startup.pack"),
        )
        .expect("local descriptor must resolve when consumption is enabled");
        assert!(matches!(
            resolved.source,
            ResolvedStartupPackSource::LocalPath(ref path)
                if path.ends_with("memory-startup.pack")
        ));
    }
}
