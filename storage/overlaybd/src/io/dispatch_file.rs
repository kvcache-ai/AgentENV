//! Dispatch `VirtualFile` operations onto a fixed tokio runtime.
//!
//! # Why this exists
//!
//! The remote backends (OSS via opendal/reqwest, registryfs via reqwest)
//! spawn per-connection driver tasks with a bare `tokio::spawn`, which binds
//! the task — and therefore the pooled HTTP connection — to whatever runtime
//! happens to poll the request. Meanwhile every ublk queue worker runs a
//! dedicated per-device `current_thread` runtime that is dropped when the
//! device is torn down. The reqwest connection pool is effectively shared
//! process-wide (opendal 0.55 defaults every operator to a global static
//! client, and `ImageService` caches operators per bucket on top of that),
//! so without dispatch, pooled connections end up owned by a random device's
//! runtime:
//!
//! * reads issued by device B may be driven by device A's queue thread, so
//!   B's latency couples to A's scheduling (and to anything blocking A's
//!   `LocalSet` thread), and
//! * tearing down device A drops its runtime and kills every connection
//!   task spawned there — including connections device B is actively using.
//!
//! Wrapping remote files in [`RuntimeDispatchFile`] pins all network I/O to
//! a fixed, long-lived runtime — a dedicated per-`ImageService` runtime, or
//! the `ImageService`'s constructing runtime when `remoteIoWorkers` is 0 —
//! decoupling connection lifetimes from any single device's lifecycle.

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::Handle;

use super::virtual_file::VirtualFile;

/// Build the dedicated runtime that drives all remote image I/O of one
/// `ImageService`: [`RuntimeDispatchFile`] operations, the file-cache
/// background workers, and the background-download scheduler all run here
/// (see the module docs above). Only used when the overlaybd global config
/// sets `remoteIoWorkers` to a positive value; with `remoteIoWorkers = 0`
/// the `ImageService` dispatches onto the runtime that constructed it
/// instead (e.g. unit tests).
///
/// The owner must shut the runtime down with
/// [`Runtime::shutdown_background`] once it is done with it: `Runtime::drop`
/// shuts the blocking pool down with a blocking wait that panics inside an
/// async context, and `ImageService` teardown paths run inside one (tests,
/// the daemon's main runtime). See `ImageServiceInner`'s `Drop` impl.
pub fn build_remote_io_runtime(workers: usize) -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("obd-remote-io")
        .enable_all()
        .build()
        .context("build remote io runtime")
}

/// [`VirtualFile`] wrapper that runs every I/O operation on a fixed runtime
/// via `Handle::spawn` and awaits the result on the caller's runtime.
///
/// Only the operations remote backends actually implement are forwarded;
/// the remaining `VirtualFile` methods keep their defaults (which match the
/// remote backends' behavior: read-only, no xattr, no seek/discard).
pub struct RuntimeDispatchFile {
    inner: Arc<dyn VirtualFile>,
    handle: Handle,
}

impl RuntimeDispatchFile {
    pub fn new(inner: Arc<dyn VirtualFile>, handle: Handle) -> Arc<Self> {
        Arc::new(Self { inner, handle })
    }

    /// Run `fut` on the target runtime and await its completion here.
    async fn dispatch<T, F>(&self, op: &str, fut: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        self.handle
            .spawn(fut)
            .await
            .with_context(|| format!("remote io {op} task failed to join"))?
    }
}

impl fmt::Debug for RuntimeDispatchFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeDispatchFile")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl VirtualFile for RuntimeDispatchFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let inner = Arc::clone(&self.inner);
        self.dispatch("read_at", async move { inner.read_at(offset, len).await })
            .await
    }

    // `read_at_into` intentionally keeps the default (`read_at` + one copy):
    // the caller's `&mut [u8]` cannot cross into a spawned task, and remote
    // reads are bounded blocks, so the extra copy is negligible compared to
    // the network round trip.

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let inner = Arc::clone(&self.inner);
        let data = data.to_vec();
        self.dispatch(
            "write_at",
            async move { inner.write_at(offset, &data).await },
        )
        .await
    }

    async fn write_bytes_at(&self, offset: u64, data: Bytes) -> Result<usize> {
        let inner = Arc::clone(&self.inner);
        self.dispatch("write_bytes_at", async move {
            inner.write_bytes_at(offset, data).await
        })
        .await
    }

    async fn size(&self) -> Result<u64> {
        let inner = Arc::clone(&self.inner);
        self.dispatch("size", async move { inner.size().await })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MemoryFile {
        data: Mutex<Vec<u8>>,
        /// Records the thread each operation ran on, to prove dispatch.
        op_threads: Mutex<Vec<std::thread::ThreadId>>,
    }

    impl MemoryFile {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data: Mutex::new(data),
                op_threads: Mutex::new(Vec::new()),
            }
        }

        fn record(&self) {
            self.op_threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
        }
    }

    #[async_trait]
    impl VirtualFile for MemoryFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
            self.record();
            let data = self.data.lock().unwrap();
            let offset = offset as usize;
            if offset >= data.len() {
                return Ok(Bytes::new());
            }
            let end = (offset + len).min(data.len());
            Ok(Bytes::copy_from_slice(&data[offset..end]))
        }

        async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
            self.record();
            let mut guard = self.data.lock().unwrap();
            let offset = offset as usize;
            let end = offset + data.len();
            if guard.len() < end {
                guard.resize(end, 0);
            }
            guard[offset..end].copy_from_slice(data);
            Ok(data.len())
        }

        async fn size(&self) -> Result<u64> {
            self.record();
            Ok(self.data.lock().unwrap().len() as u64)
        }
    }

    fn spawn_shared_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("shared runtime")
    }

    #[tokio::test]
    async fn test_remote_io_runtime_worker_count() {
        let runtime = build_remote_io_runtime(2).expect("build runtime");
        assert_eq!(runtime.metrics().num_workers(), 2);
        // `Runtime::drop` panics inside an async context; owners shut down
        // in the background instead.
        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_dispatch_on_remote_io_runtime() {
        let runtime = build_remote_io_runtime(1).expect("build runtime");
        let inner = Arc::new(MemoryFile::new(b"dedicated".to_vec()));
        let file = RuntimeDispatchFile::new(inner.clone(), runtime.handle().clone());

        let data = file.read_at(0, 9).await.expect("read_at");
        assert_eq!(&data[..], b"dedicated");

        let op_threads = inner.op_threads.lock().unwrap();
        assert!(op_threads
            .iter()
            .all(|tid| *tid != std::thread::current().id()));
        drop(op_threads);
        runtime.shutdown_background();
    }

    #[tokio::test]
    async fn test_dispatch_operations_run_on_target_runtime() {
        let shared = spawn_shared_runtime();
        let inner = Arc::new(MemoryFile::new(b"hello dispatch".to_vec()));
        let file = RuntimeDispatchFile::new(inner.clone(), shared.handle().clone());

        let caller_thread = std::thread::current().id();

        let size = file.size().await.expect("size");
        assert_eq!(size, 14);
        let data = file.read_at(0, 5).await.expect("read_at");
        assert_eq!(&data[..], b"hello");
        let data = file.read_at(6, 8).await.expect("read_at tail");
        assert_eq!(&data[..], b"dispatch");
        let written = file.write_at(0, b"HELLO").await.expect("write_at");
        assert_eq!(written, 5);
        let data = file.read_at(0, 5).await.expect("re-read");
        assert_eq!(&data[..], b"HELLO");

        // Every operation must have executed on a shared-runtime worker,
        // never on the caller's (test runtime) thread.
        let op_threads = inner.op_threads.lock().unwrap();
        assert!(!op_threads.is_empty());
        assert!(op_threads.iter().all(|tid| *tid != caller_thread));

        // Dropping a runtime inside an async context is not allowed; shut it
        // down in the background instead.
        shared.shutdown_background();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_dispatch_survives_caller_runtime_drop() {
        let shared = spawn_shared_runtime();
        let inner: Arc<dyn VirtualFile> = Arc::new(MemoryFile::new(b"stable".to_vec()));
        let file = RuntimeDispatchFile::new(inner, shared.handle().clone());

        // Simulate a per-device current_thread runtime on its own OS thread
        // (the ublk queue worker model): it issues I/O and is then torn down.
        let device_file = Arc::clone(&file);
        let (tx, rx) = std::sync::mpsc::channel();
        let device_thread = std::thread::spawn(move || {
            let device_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("device runtime");
            let result = device_rt.block_on(async { device_file.read_at(0, 6).await });
            let _ = tx.send(result);
            // `device_rt` is dropped here, on a plain thread (not inside an
            // async context), mirroring queue worker teardown.
        });
        let data = rx
            .recv()
            .expect("device result")
            .expect("read from device runtime");
        assert_eq!(&data[..], b"stable");
        device_thread.join().expect("join device thread");

        // The dispatch target is untouched; later reads from any other
        // runtime still work.
        let data = file.read_at(0, 6).await.expect("read after device drop");
        assert_eq!(&data[..], b"stable");

        shared.shutdown_background();
    }

    #[tokio::test]
    async fn test_read_at_into_uses_dispatched_read() {
        let shared = spawn_shared_runtime();
        let inner = Arc::new(MemoryFile::new(b"buffered".to_vec()));
        let file = RuntimeDispatchFile::new(inner.clone(), shared.handle().clone());

        let mut buf = [0u8; 8];
        let n = file.read_at_into(0, &mut buf).await.expect("read_at_into");
        assert_eq!(n, 8);
        assert_eq!(&buf, b"buffered");

        let op_threads = inner.op_threads.lock().unwrap();
        assert!(op_threads
            .iter()
            .all(|tid| *tid != std::thread::current().id()));

        shared.shutdown_background();
    }
}
