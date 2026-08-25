use super::super::local::LocalFile;
use super::*;
use crate::io::virtual_file::VirtualFile;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use nix::errno::Errno;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::HashMap;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::{Notify, Semaphore};

#[derive(Debug, Clone)]
struct SlowFirstReadSource {
    inner: Arc<SlowFirstReadSourceInner>,
}

#[derive(Debug)]
struct SlowFirstReadSourceInner {
    data: Vec<u8>,
    block_size: u64,
    hang: Duration,
    calls: Mutex<HashMap<u64, usize>>,
    attempts: AtomicUsize,
}

impl SlowFirstReadSource {
    fn new(data: Vec<u8>, block_size: u64, hang: Duration) -> Self {
        Self {
            inner: Arc::new(SlowFirstReadSourceInner {
                data,
                block_size,
                hang,
                calls: Mutex::new(HashMap::new()),
                attempts: AtomicUsize::new(0),
            }),
        }
    }

    fn attempts(&self) -> usize {
        self.inner.attempts.load(Ordering::Acquire)
    }
}

#[async_trait]
impl VirtualFile for SlowFirstReadSource {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let block_id = offset / self.inner.block_size;
        self.inner.attempts.fetch_add(1, Ordering::AcqRel);
        let first_for_block = {
            let mut calls = self.inner.calls.lock().unwrap();
            let calls = calls.entry(block_id).or_default();
            *calls += 1;
            *calls == 1
        };
        if first_for_block {
            tokio::time::sleep(self.inner.hang).await;
        }
        let size = self.inner.data.len() as u64;
        if offset >= size || len == 0 {
            return Ok(Bytes::new());
        }
        let end = offset.saturating_add(len as u64).min(size);
        Ok(Bytes::copy_from_slice(
            &self.inner.data[offset as usize..end as usize],
        ))
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("slow first read source is read-only");
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.inner.data.len() as u64)
    }
}

#[derive(Debug, Clone)]
struct ControlledSource {
    inner: Arc<ControlledSourceInner>,
}

#[derive(Debug)]
struct ControlledSourceInner {
    data: Vec<u8>,
    block_size: u64,
    calls: Mutex<HashMap<u64, usize>>,
    failures: Mutex<HashMap<u64, usize>>,
    panic_next: AtomicBool,
    gates: Mutex<HashMap<u64, Arc<Semaphore>>>,
    changed: Notify,
    completed: AtomicUsize,
}

impl ControlledSource {
    fn new(data: Vec<u8>, block_size: u64) -> Self {
        Self {
            inner: Arc::new(ControlledSourceInner {
                data,
                block_size,
                calls: Mutex::new(HashMap::new()),
                failures: Mutex::new(HashMap::new()),
                panic_next: AtomicBool::new(false),
                gates: Mutex::new(HashMap::new()),
                changed: Notify::new(),
                completed: AtomicUsize::new(0),
            }),
        }
    }

    fn fail_block(&self, block_id: u64, times: usize) {
        self.inner.failures.lock().unwrap().insert(block_id, times);
    }

    fn panic_next_read(&self) {
        self.inner.panic_next.store(true, Ordering::Release);
    }

    fn gate_block(&self, block_id: u64) {
        self.inner
            .gates
            .lock()
            .unwrap()
            .insert(block_id, Arc::new(Semaphore::new(0)));
    }

    fn release_block(&self, block_id: u64) {
        if let Some(gate) = self.inner.gates.lock().unwrap().get(&block_id) {
            gate.add_permits(1);
        }
    }

    fn calls(&self, block_id: u64) -> usize {
        self.inner
            .calls
            .lock()
            .unwrap()
            .get(&block_id)
            .copied()
            .unwrap_or(0)
    }

    fn completed(&self) -> usize {
        self.inner.completed.load(Ordering::Acquire)
    }

    async fn wait_calls(&self, block_id: u64, expected: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if self.calls(block_id) >= expected {
                    break;
                }
                let notified = self.inner.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.calls(block_id) >= expected {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("timed out waiting for source calls");
    }
}

#[async_trait]
impl VirtualFile for ControlledSource {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let block_id = offset / self.inner.block_size;
        {
            let mut calls = self.inner.calls.lock().unwrap();
            *calls.entry(block_id).or_default() += 1;
        }
        self.inner.changed.notify_waiters();
        let gate = self.inner.gates.lock().unwrap().get(&block_id).cloned();
        if let Some(gate) = gate {
            let permit = gate.acquire().await.expect("gate open");
            permit.forget();
        }
        assert!(
            !self.inner.panic_next.swap(false, Ordering::AcqRel),
            "forced source panic"
        );
        let should_fail = {
            let mut failures = self.inner.failures.lock().unwrap();
            let remaining = failures.entry(block_id).or_default();
            if *remaining > 0 {
                *remaining -= 1;
                true
            } else {
                false
            }
        };
        if should_fail {
            bail!("forced source failure for block {block_id}");
        }
        let size = self.inner.data.len() as u64;
        if offset >= size || len == 0 {
            return Ok(Bytes::new());
        }
        let end = offset.saturating_add(len as u64).min(size);
        self.inner.completed.fetch_add(1, Ordering::Release);
        self.inner.changed.notify_waiters();
        Ok(Bytes::copy_from_slice(
            &self.inner.data[offset as usize..end as usize],
        ))
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("controlled source is read-only");
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.inner.data.len() as u64)
    }
}

#[derive(Debug, Clone)]
struct MockSource {
    inner: Arc<MockSourceInner>,
}

#[derive(Debug)]
struct MockSourceInner {
    data: Vec<u8>,
    read_calls: AtomicUsize,
    delay: Duration,
}

impl MockSource {
    fn new(data: Vec<u8>, delay: Duration) -> Self {
        Self {
            inner: Arc::new(MockSourceInner {
                data,
                read_calls: AtomicUsize::new(0),
                delay,
            }),
        }
    }

    fn read_calls(&self) -> usize {
        self.inner.read_calls.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
struct EvictTrackingSource {
    inner: Arc<EvictTrackingSourceInner>,
}

#[derive(Debug)]
struct EvictTrackingSourceInner {
    data: Vec<u8>,
    evict_range_calls: AtomicUsize,
    evict_all_calls: AtomicUsize,
}

impl EvictTrackingSource {
    fn new(data: Vec<u8>) -> Self {
        Self {
            inner: Arc::new(EvictTrackingSourceInner {
                data,
                evict_range_calls: AtomicUsize::new(0),
                evict_all_calls: AtomicUsize::new(0),
            }),
        }
    }

    fn evict_range_calls(&self) -> usize {
        self.inner.evict_range_calls.load(Ordering::Relaxed)
    }

    fn evict_all_calls(&self) -> usize {
        self.inner.evict_all_calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl VirtualFile for EvictTrackingSource {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let size = self.inner.data.len() as u64;
        if offset >= size || len == 0 {
            return Ok(Bytes::new());
        }
        let end = offset.saturating_add(len as u64).min(size);
        Ok(Bytes::copy_from_slice(
            &self.inner.data[offset as usize..end as usize],
        ))
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("evict tracking source is read-only");
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.inner.data.len() as u64)
    }

    async fn evict_range(&self, _offset: u64, _len: u64) -> Result<()> {
        self.inner.evict_range_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn evict_all(&self) -> Result<()> {
        self.inner.evict_all_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct CountingFsSource {
    inner: Arc<CountingFsSourceInner>,
}

#[derive(Debug)]
struct CountingFsSourceInner {
    data: HashMap<String, Vec<u8>>,
    open_calls: AtomicUsize,
    fail_open: AtomicBool,
}

impl CountingFsSource {
    fn new(entries: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
        Self {
            inner: Arc::new(CountingFsSourceInner {
                data: entries.into_iter().collect(),
                open_calls: AtomicUsize::new(0),
                fail_open: AtomicBool::new(false),
            }),
        }
    }

    fn open_calls(&self) -> usize {
        self.inner.open_calls.load(Ordering::Relaxed)
    }

    fn set_fail_open(&self, value: bool) {
        self.inner.fail_open.store(value, Ordering::Relaxed);
    }
}

#[async_trait]
impl CachedFsSource for CountingFsSource {
    async fn open(&self, pathname: &str, _flags: u32, _mode: u32) -> Result<Arc<dyn VirtualFile>> {
        self.inner.open_calls.fetch_add(1, Ordering::Relaxed);
        if self.inner.fail_open.load(Ordering::Relaxed) {
            bail!("forced source open failure");
        }
        let data = self
            .inner
            .data
            .get(pathname)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("source path not found"))?;
        Ok(Arc::new(MockSource::new(data, Duration::ZERO)))
    }

    async fn access(&self, pathname: &str, _mode: i32) -> Result<()> {
        if self.inner.data.contains_key(pathname) {
            Ok(())
        } else {
            bail!("source path not found");
        }
    }
}

#[async_trait]
impl VirtualFile for MockSource {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        self.inner.read_calls.fetch_add(1, Ordering::Relaxed);
        if !self.inner.delay.is_zero() {
            tokio::time::sleep(self.inner.delay).await;
        }
        let size = self.inner.data.len() as u64;
        if offset >= size || len == 0 {
            return Ok(Bytes::new());
        }
        let end = offset.saturating_add(len as u64).min(size);
        Ok(Bytes::copy_from_slice(
            &self.inner.data[offset as usize..end as usize],
        ))
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("mock source is read-only");
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.inner.data.len() as u64)
    }
}

fn test_options(root: &Path) -> FileCacheBackendOptions {
    FileCacheBackendOptions {
        cache_dir: root.to_path_buf(),
        capacity_bytes: 1024 * 1024,
        block_size: 256 * 4096,
        ..Default::default()
    }
}

fn uniform_char_random_data(size: usize, seed: u64) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..size).map(|_| rng.random::<u8>()).collect()
}

async fn write_local_file(path: &Path, data: &[u8]) -> Result<()> {
    let file = LocalFile::new(path)?;
    let _ = file.write_at(0, data).await?;
    file.sync().await?;
    Ok(())
}

fn assert_err_contains(err: &anyhow::Error, needle: &str) {
    let message = err.to_string();
    assert!(
        message.contains(needle),
        "expected error containing `{needle}`, got `{message}`"
    );
}

fn err_is_errno(err: &anyhow::Error, errno: Errno) -> bool {
    err.downcast_ref::<Errno>()
        .is_some_and(|value| *value == errno)
}

fn background_config() -> crate::config::DownloadConfig {
    crate::config::DownloadConfig {
        delay: 0,
        delay_extra: 0,
        max_mbps: 0,
        try_cnt: 1,
        block_size: 4096,
        concurrency: 1,
        max_inflight_blocks: 1,
        ..Default::default()
    }
}

async fn wait_cached(file: &CachedFile, offset: u64, len: usize) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while file.query(offset, len).await.expect("query cache") != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for cached range");
}

async fn wait_not_registered(backend: &FileCacheBackend, cache_id: &str) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while backend.bk_download_registered(cache_id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out waiting for download registration cleanup");
}

#[tokio::test]
async fn test_background_download_exposes_completed_block_before_task_finishes() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let payload = vec![7u8; 8192];
    let source = Arc::new(ControlledSource::new(payload.clone(), 4096));
    source.gate_block(1);
    let file = backend
        .open_file_with_source_size(
            "exposes-completed-block",
            source.clone(),
            payload.len() as u64,
        )
        .await
        .expect("open cached file");

    backend
        .submit_bk_download(file.clone(), background_config(), None)
        .expect("submit background download");
    wait_cached(file.as_ref(), 0, 4096).await;
    source.wait_calls(1, 1).await;

    let cache_only = backend
        .open_cache_only("exposes-completed-block", payload.len() as u64)
        .await
        .expect("open cache-only handle");
    let first = cache_only
        .read_at(0, 4096)
        .await
        .expect("read completed background block from cache only");
    assert_eq!(first.as_ref(), &payload[..4096]);
    assert!(cache_only.read_at(4096, 4096).await.is_err());
    assert_eq!(
        source.calls(0),
        1,
        "block 0 must land from exactly one source read"
    );

    source.release_block(1);
    wait_cached(file.as_ref(), 0, payload.len()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_background_download_registers_dedup_key_before_worker_poll() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let source = Arc::new(ControlledSource::new(vec![1u8; 4096], 4096));
    source.gate_block(0);
    let file = backend
        .open_file_with_source_size("submit-dedup", source.clone(), 4096)
        .await
        .expect("open cached file");

    backend
        .submit_bk_download(file.clone(), background_config(), None)
        .expect("first submit");
    backend
        .submit_bk_download(file.clone(), background_config(), None)
        .expect("duplicate submit");
    assert!(backend.bk_download_registered(file.cache_id()));
    assert_eq!(
        backend.bk_download_registered_count(),
        1,
        "duplicate submit must not register a second task"
    );

    source.release_block(0);
    wait_cached(file.as_ref(), 0, 4096).await;
    wait_not_registered(&backend, file.cache_id()).await;
    assert_eq!(source.calls(0), 1);
}

#[tokio::test]
async fn test_background_download_panic_releases_dedup_registration() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let panicking = Arc::new(ControlledSource::new(vec![12u8; 4096], 4096));
    panicking.panic_next_read();
    let file = backend
        .open_file_with_source_size("panic-cleanup", panicking.clone(), 4096)
        .await
        .expect("open cached file");
    backend
        .submit_bk_download(file.clone(), background_config(), None)
        .expect("submit panicking task");
    panicking.wait_calls(0, 1).await;
    wait_not_registered(&backend, file.cache_id()).await;

    let healthy = Arc::new(ControlledSource::new(vec![13u8; 4096], 4096));
    file.set_source(Some(healthy.clone()));
    backend
        .submit_bk_download(file.clone(), background_config(), None)
        .expect("resubmit after panic");
    wait_cached(file.as_ref(), 0, 4096).await;
    assert_eq!(healthy.calls(0), 1);
}

#[tokio::test]
async fn test_background_download_retry_reads_only_missing_blocks() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let payload = vec![4u8; 8192];
    let source = Arc::new(ControlledSource::new(payload.clone(), 4096));
    source.fail_block(1, 1);
    let file = backend
        .open_file_with_source_size("retry-missing", source.clone(), payload.len() as u64)
        .await
        .expect("open cached file");
    let mut config = background_config();
    config.try_cnt = 2;
    backend
        .submit_bk_download(file.clone(), config, None)
        .expect("submit");

    source.wait_calls(1, 2).await;
    wait_cached(file.as_ref(), 0, payload.len()).await;
    assert_eq!(source.calls(0), 1);
    assert_eq!(source.calls(1), 2);
}

#[tokio::test]
async fn test_active_background_download_prevents_eviction() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let source = Arc::new(ControlledSource::new(vec![5u8; 8192], 4096));
    source.gate_block(1);
    let file = backend
        .open_file_with_source_size("active-eviction", source.clone(), 8192)
        .await
        .expect("open cached file");
    backend
        .submit_bk_download(file.clone(), background_config(), None)
        .expect("submit");
    source.wait_calls(1, 1).await;
    drop(file);

    assert_eq!(backend.evict_by_size(4096).await.expect("evict"), 0);
    assert_eq!(
        backend.file_stats("active-eviction").unwrap().bytes_used,
        4096
    );
    source.release_block(1);
    // Drain the scheduler before the test tears down its runtime/tempdir.
    tokio::time::timeout(Duration::from_secs(3), backend.shutdown_bk_downloads())
        .await
        .expect("background download shutdown did not drain");
}

#[tokio::test]
async fn test_background_download_shutdown_drains_started_io_without_new_dispatch() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let source = Arc::new(ControlledSource::new(vec![7u8; 8192], 4096));
    source.gate_block(0);
    let file = backend
        .open_file_with_source_size("shutdown-started", source.clone(), 8192)
        .await
        .expect("open cached file");
    backend
        .submit_bk_download(file.clone(), background_config(), None)
        .expect("submit");
    source.wait_calls(0, 1).await;

    drop(file);
    // Let the started block read finish, then close the scheduler. On the
    // current-thread runtime the shutdown leader flips `running` before the
    // woken read continues, so no new block is dispatched after the drain.
    source.release_block(0);
    tokio::time::timeout(Duration::from_secs(3), backend.shutdown_bk_downloads())
        .await
        .expect("shutdown did not drain");
    assert_eq!(source.completed(), 1, "started I/O was not drained");
    assert_eq!(source.calls(1), 0, "shutdown dispatched a new block");
}

#[tokio::test]
async fn test_background_download_saturated_scheduler_completes_all_tasks() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    options.bk_download_max_concurrent_files = 1;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");

    // Three independent files share a single file slot, so their downloads
    // run serially; saturation must queue, never skip or reject.
    let mut files = Vec::new();
    let mut payloads = Vec::new();
    for index in 0..3u64 {
        let payload = uniform_char_random_data(2 * 4096, 0x5a00 + index);
        let src_path = tmp.path().join(format!("saturated-{index}"));
        write_local_file(&src_path, &payload)
            .await
            .expect("write source");
        let source = Arc::new(LocalFile::open_ro(&src_path).expect("open source"));
        let file = backend
            .open_file_with_source_size(format!("saturated-{index}"), source, payload.len() as u64)
            .await
            .expect("open cached file");
        backend
            .submit_bk_download(file.clone(), background_config(), None)
            .expect("submit must not fail under saturation");
        files.push(file);
        payloads.push(payload);
    }

    tokio::time::timeout(Duration::from_secs(10), async {
        while backend.bk_download_registered_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("saturated scheduler did not drain all tasks");

    for (file, payload) in files.iter().zip(payloads.iter()) {
        wait_cached(file.as_ref(), 0, payload.len()).await;
        let got = file
            .read_at(0, payload.len())
            .await
            .expect("read cached file");
        assert_eq!(got.as_ref(), payload.as_slice());
    }
}

#[tokio::test]
async fn test_pending_background_download_keeps_entry_evictable() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let source = Arc::new(ControlledSource::new(vec![14u8; 8192], 4096));
    let file = backend
        .open_file_with_source_size("pending-eviction", source.clone(), 8192)
        .await
        .expect("open cached file");
    file.refill_range(0, 4096)
        .await
        .expect("prefill first block");
    assert_eq!(source.calls(0), 1);

    // A task with a huge configured delay never leaves the waiting phase
    // during the test: it is registered but holds no cache entry, so the
    // entry stays evictable.
    let mut config = background_config();
    config.delay = 3600;
    backend
        .submit_bk_download(file.clone(), config, None)
        .expect("submit");
    assert!(backend.bk_download_registered(file.cache_id()));
    drop(file);

    assert_eq!(backend.evict_by_size(4096).await.expect("evict"), 4096);
    assert!(backend.file_stats("pending-eviction").is_none());

    // Do not leak the pending timer past the test.
    backend.shutdown_bk_downloads().await;
}

#[tokio::test]
async fn test_background_download_block_timeout_reissues_read() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    options.bk_download_hedge_timeout = Duration::from_millis(50);
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let payload = vec![15u8; 4096];
    // The first read attempt per block hangs past the hedge timeout; the
    // reissued read returns immediately.
    let source = Arc::new(SlowFirstReadSource::new(
        payload.clone(),
        4096,
        Duration::from_millis(300),
    ));
    let file = backend
        .open_file_with_source_size("hedge-reissue", source.clone(), 4096)
        .await
        .expect("open cached file");
    backend
        .submit_bk_download(file.clone(), background_config(), None)
        .expect("submit");

    wait_cached(file.as_ref(), 0, 4096).await;
    assert!(
        source.attempts() >= 2,
        "timed-out block read must be reissued, attempts={}",
        source.attempts()
    );
    let cache_only = backend
        .open_cache_only("hedge-reissue", 4096)
        .await
        .expect("cache-only handle");
    assert_eq!(
        cache_only
            .read_at(0, 4096)
            .await
            .expect("read cached block")
            .as_ref(),
        payload.as_slice()
    );
}

#[tokio::test]
async fn test_background_download_chunk_fills_multiple_cache_blocks_per_read() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let payload: Vec<u8> = (0..16384).map(|v| (v % 251) as u8).collect();
    let source = Arc::new(ControlledSource::new(payload.clone(), 4096));
    let file = backend
        .open_file_with_source_size("chunk-coalesce", source.clone(), payload.len() as u64)
        .await
        .expect("open cached file");
    let mut config = background_config();
    config.block_size = 16384; // one chunk covers four 4096-byte cache blocks
    backend
        .submit_bk_download(file.clone(), config, None)
        .expect("submit");

    wait_cached(file.as_ref(), 0, payload.len()).await;
    // One source request filled all four cache blocks.
    assert_eq!(source.calls(0), 1);
    assert_eq!(source.calls(1), 0);
    assert_eq!(source.calls(2), 0);
    assert_eq!(source.calls(3), 0);
    let data = file.read_at(0, payload.len()).await.expect("cached read");
    assert_eq!(data.as_ref(), payload.as_slice());
}

#[tokio::test]
async fn test_background_download_chunk_skips_cached_blocks_and_splits_runs() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let payload: Vec<u8> = (0..16384).map(|v| (v % 251) as u8).collect();
    let source = Arc::new(ControlledSource::new(payload.clone(), 4096));
    let file = backend
        .open_file_with_source_size("chunk-runs", source.clone(), payload.len() as u64)
        .await
        .expect("open cached file");
    // Foreground pre-fill of block 1 before the background download starts.
    let pre = file.read_at(4096, 4096).await.expect("foreground prefill");
    assert_eq!(pre.as_ref(), &payload[4096..8192]);
    assert_eq!(source.calls(1), 1);

    let mut config = background_config();
    config.block_size = 16384; // chunk [0,4) splits into runs [0] and [2,3]
    backend
        .submit_bk_download(file.clone(), config, None)
        .expect("submit");
    wait_cached(file.as_ref(), 0, payload.len()).await;

    assert_eq!(source.calls(1), 1, "cached block must not be re-read");
    assert_eq!(source.calls(0), 1, "run [0] is a single source read");
    assert_eq!(
        source.calls(2),
        1,
        "run [2,3] is a single source read at offset 8192"
    );
    assert_eq!(source.calls(3), 0);
    let data = file.read_at(0, payload.len()).await.expect("cached read");
    assert_eq!(data.as_ref(), payload.as_slice());
}

#[tokio::test]
async fn test_foreground_waits_for_background_loader_at_refill_limit() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    // Two 64-block chunks elect 128 cache blocks, reaching
    // DEFAULT_MAX_REFILLING while both source reads are gated.
    let payload = vec![43u8; 128 * 4096];
    let source = Arc::new(ControlledSource::new(payload.clone(), 4096));
    source.gate_block(0);
    source.gate_block(64);
    let file = backend
        .open_file_with_source_size(
            "foreground-dedup-limit",
            source.clone(),
            payload.len() as u64,
        )
        .await
        .expect("open cached file");
    let mut config = background_config();
    config.block_size = 64 * 4096;
    config.concurrency = 2;
    config.max_inflight_blocks = 2;
    backend
        .submit_bk_download(file.clone(), config, None)
        .expect("submit");
    source.wait_calls(0, 1).await;
    source.wait_calls(64, 1).await;

    let target_block = 32u64;
    let foreground_file = file.clone();
    let foreground =
        tokio::spawn(async move { foreground_file.read_at(target_block * 4096, 4096).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        source.calls(target_block),
        0,
        "foreground must wait for the elected background loader, not read the source again"
    );
    source.release_block(0);
    source.release_block(64);
    let data = tokio::time::timeout(Duration::from_secs(2), foreground)
        .await
        .expect("foreground read must complete")
        .expect("foreground task")
        .expect("foreground read");
    assert_eq!(
        data.as_ref(),
        &payload[target_block as usize * 4096..(target_block as usize + 1) * 4096]
    );
}

#[tokio::test]
async fn test_dropping_backend_clone_does_not_stop_background_downloads() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    drop(backend.clone());

    let payload = vec![41u8; 4096];
    let source = Arc::new(ControlledSource::new(payload.clone(), 4096));
    let file = backend
        .open_file_with_source_size("clone-drop", source, payload.len() as u64)
        .await
        .expect("open cached file");
    backend
        .submit_bk_download(file.clone(), background_config(), None)
        .expect("submit after clone drop");

    wait_cached(file.as_ref(), 0, payload.len()).await;
    assert_eq!(
        file.read_at(0, payload.len())
            .await
            .expect("cached read")
            .as_ref(),
        payload.as_slice()
    );
}

#[tokio::test]
async fn test_shutdown_can_resume_after_first_waiter_is_aborted() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let source = Arc::new(ControlledSource::new(vec![42u8; 4096], 4096));
    let file = backend
        .open_file_with_source_size("abort-shutdown", source, 4096)
        .await
        .expect("open cached file");
    let mut config = background_config();
    config.delay = 3600;
    backend
        .submit_bk_download(file, config, None)
        .expect("submit delayed task");

    let first_backend = backend.clone();
    let first = tokio::spawn(async move {
        first_backend.shutdown_bk_downloads().await;
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !backend.bk_download_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first shutdown caller must become the drain leader");
    assert!(!first.is_finished(), "delayed task must keep drain pending");
    first.abort();
    let _ = first.await;

    tokio::time::timeout(Duration::from_secs(2), backend.shutdown_bk_downloads())
        .await
        .expect("a later shutdown caller must finish the abandoned drain");
}

#[tokio::test]
async fn test_cache_hit_after_first_read() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");

    let payload: Vec<u8> = (0..2048).map(|x| (x % 251) as u8).collect();
    let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file = backend
        .open_file("cache-hit", src.clone())
        .await
        .expect("open cached file");

    let first = file.read_at(100, 120).await.expect("first read");
    let second = file.read_at(100, 120).await.expect("second read");
    assert_eq!(first, second);
    assert_eq!(src.read_calls(), 1);

    let stats = backend.stats();
    assert!(stats.hits > 0);
    assert!(stats.misses > 0);
}

#[tokio::test]
async fn test_cache_persistence_reload() {
    let tmp = tempdir().expect("create tempdir");
    let payload: Vec<u8> = (0..1024).map(|x| (x % 127) as u8).collect();

    {
        let backend = FileCacheBackend::with_options(test_options(tmp.path()))
            .await
            .expect("backend init");
        let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
        let file = backend
            .open_file("persist-key", src.clone())
            .await
            .expect("open cached file");
        let _ = file.read_at(0, 128).await.expect("populate cache");
        assert_eq!(src.read_calls(), 1);
        // Checkpoint writes meta.bin with crash-safe ordering.
        if let Some(entry) = backend.get_cache_entry(&super::cache_key_digest("persist-key")) {
            entry.checkpoint().await.expect("checkpoint");
        }
    }

    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend 2");
    let src2 = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file2 = backend
        .open_file("persist-key", src2.clone())
        .await
        .expect("open cached file 2");
    let got = file2.read_at(0, 128).await.expect("read cached");
    assert_eq!(got.as_ref(), &payload[..128]);
    assert_eq!(src2.read_calls(), 0);
}

#[tokio::test]
async fn test_load_from_disk_preserves_premerged_index_dir() {
    let tmp = tempdir().expect("create tempdir");
    let reserved = tmp.path().join(crate::lsmt::file::PREMERGED_INDEX_DIR);
    std::fs::create_dir_all(&reserved).expect("create premerged-index dir");
    let artifact = reserved.join("deadbeef.pmidx");
    std::fs::write(&artifact, b"pmidx-bytes").expect("write artifact");

    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend init");
    drop(backend);

    assert!(
        artifact.try_exists().expect("stat artifact"),
        "recovery must not remove the premerged-index artifact cache"
    );
}

#[tokio::test]
async fn test_inflight_refill_dedup() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");

    let payload: Vec<u8> = (0..1024).map(|x| (x % 211) as u8).collect();
    let src = Arc::new(MockSource::new(payload.clone(), Duration::from_millis(100)));
    let file = backend
        .open_file("dedup-key", src.clone())
        .await
        .expect("open cached file");

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let f = file.clone();
        tasks.push(tokio::spawn(async move { f.read_at(32, 96).await }));
    }
    for task in tasks {
        let got = task.await.expect("join task").expect("read result");
        assert_eq!(got.len(), 96);
    }
    assert_eq!(src.read_calls(), 1);
}

#[tokio::test]
async fn test_inflight_refill_overlap_serialized() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");

    let payload: Vec<u8> = (0..2048).map(|x| (x % 197) as u8).collect();
    let src = Arc::new(MockSource::new(payload, Duration::from_millis(120)));
    let file = backend
        .open_file("overlap-key", src.clone())
        .await
        .expect("open cached file");

    let a = {
        let f = file.clone();
        tokio::spawn(async move { f.read_at(192, 128).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let b = {
        let f = file.clone();
        tokio::spawn(async move { f.read_at(0, 64).await })
    };
    let _ = a.await.expect("join a").expect("read a");
    let _ = b.await.expect("join b").expect("read b");

    // Overlap refill ranges should be serialized by waker-based dedup.
    assert_eq!(src.read_calls(), 1);
}

#[tokio::test]
async fn test_lru_eviction() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.capacity_bytes = 12 * 1024; // hold two 4KiB cache blocks without immediate eviction
    opt.block_size = 4096;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let data_a = vec![1u8; 4096];
    let data_b = vec![2u8; 4096];
    let data_c = vec![3u8; 4096];

    {
        let a = Arc::new(MockSource::new(data_a.clone(), Duration::ZERO));
        let f = backend
            .open_file("key-a", a.clone())
            .await
            .expect("open key-a");
        let _ = f.read_at(0, 4096).await.expect("read key-a");
        assert_eq!(a.read_calls(), 1);
    }
    {
        let b = Arc::new(MockSource::new(data_b.clone(), Duration::ZERO));
        let f = backend
            .open_file("key-b", b.clone())
            .await
            .expect("open key-b");
        let _ = f.read_at(0, 4096).await.expect("read key-b");
        assert_eq!(b.read_calls(), 1);
    }
    {
        let c = Arc::new(MockSource::new(data_c.clone(), Duration::ZERO));
        let f = backend
            .open_file("key-c", c.clone())
            .await
            .expect("open key-c");
        let _ = f.read_at(0, 4096).await.expect("read key-c");
        assert_eq!(c.read_calls(), 1);
    }

    let a2 = Arc::new(MockSource::new(data_a.clone(), Duration::ZERO));
    let f = backend
        .open_file("key-a", a2.clone())
        .await
        .expect("reopen key-a");
    let _ = f.read_at(0, 4096).await.expect("read key-a again");
    assert_eq!(a2.read_calls(), 1);
}

#[tokio::test]
async fn test_no_checkpoint_means_cold_restart() {
    let tmp = tempdir().expect("create tempdir");
    let payload: Vec<u8> = (0..1024).map(|x| (x % 127) as u8).collect();

    {
        let backend = FileCacheBackend::with_options(test_options(tmp.path()))
            .await
            .expect("backend init");
        let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
        let file = backend
            .open_file("nocheckpoint-key", src.clone())
            .await
            .expect("open cached file");
        let _ = file.read_at(0, 128).await.expect("populate cache");
        assert_eq!(src.read_calls(), 1);
        // Drop without checkpoint → no meta.bin persisted.
    }

    // Reload should treat entries as cold (no meta.bin).
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend 2");
    let src2 = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file2 = backend
        .open_file("nocheckpoint-key", src2.clone())
        .await
        .expect("open cached file 2");
    let got = file2
        .read_at(0, 128)
        .await
        .expect("read after cold restart");
    assert_eq!(got.as_ref(), &payload[..128]);
    // Source must be called again because no checkpoint was done.
    assert_eq!(src2.read_calls(), 1);
}

#[tokio::test]
async fn test_checkpoint_preserves_cache() {
    let tmp = tempdir().expect("create tempdir");
    let payload: Vec<u8> = (0..4096).map(|x| (x % 127) as u8).collect();

    {
        let backend = FileCacheBackend::with_options(test_options(tmp.path()))
            .await
            .expect("backend init");
        let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
        let file = backend
            .open_file("checkpoint-key", src.clone())
            .await
            .expect("open cached file");
        let _ = file.read_at(0, 4096).await.expect("populate cache");
        assert_eq!(src.read_calls(), 1);
        // Checkpoint with crash-safe ordering: fsync data, write meta.bin.
        if let Some(entry) = backend.get_cache_entry(&super::cache_key_digest("checkpoint-key")) {
            entry.checkpoint().await.expect("checkpoint");
        }
    }

    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend 2");
    let src2 = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file2 = backend
        .open_file("checkpoint-key", src2.clone())
        .await
        .expect("open cached file 2");
    let got = file2.read_at(0, 4096).await.expect("read after checkpoint");
    assert_eq!(got.as_ref(), &payload[..4096]);
    // No source calls needed — cache is warm from checkpoint.
    assert_eq!(src2.read_calls(), 0);
}

#[tokio::test]
async fn test_open_entry_is_not_removed_by_eviction() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.capacity_bytes = 6 * 1024;
    opt.block_size = 4096;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload_a = uniform_char_random_data(8192, 0xaaa1);
    let payload_b = uniform_char_random_data(8192, 0xbbb2);
    let src_a = Arc::new(MockSource::new(payload_a.clone(), Duration::ZERO));
    let src_b = Arc::new(MockSource::new(payload_b.clone(), Duration::ZERO));

    let file_a = backend
        .open_file("open-a", src_a.clone())
        .await
        .expect("open a");
    let file_b = backend
        .open_file("open-b", src_b.clone())
        .await
        .expect("open b");

    let _ = file_a.read_at(0, 4096).await.expect("fill a");
    let _ = file_b.read_at(0, 4096).await.expect("fill b");
    assert_eq!(src_a.read_calls(), 1);
    assert_eq!(src_b.read_calls(), 1);

    let a_stats = backend.file_stats("open-a").expect("stats for a");
    // Capacity eviction must skip open entries, so A's cached block remains present.
    assert_eq!(a_stats.bytes_used, 4096);

    let reread = file_a
        .read_at(0, 4096)
        .await
        .expect("reread a after eviction");
    assert_eq!(reread.as_ref(), &payload_a[..4096]);
    // The reread should hit A's cache instead of refilling from the source.
    assert_eq!(src_a.read_calls(), 1);
}

#[tokio::test]
async fn test_closed_entry_is_removed_by_evict_size() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.capacity_bytes = 32 * 4096;
    opt.block_size = 4096;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = uniform_char_random_data(4096, 0xccc3);
    {
        let src = Arc::new(MockSource::new(payload, Duration::ZERO));
        let file = backend
            .open_file("closed-entry", src)
            .await
            .expect("open cached file");
        let _ = file.read_at(0, 4096).await.expect("fill cache");
    }

    let cache_id = super::cache_key_digest("closed-entry");
    assert!(backend.get_cache_entry(&cache_id).is_some());

    let evicted = backend.evict_by_size(4096).await.expect("evict by size");
    assert_eq!(evicted, 4096);
    assert!(backend.get_cache_entry(&cache_id).is_none());
}

// Ported from C++ RoCachedFs.Basic (commonTest(false, false, false)).
#[tokio::test]
async fn test_ro_cached_fs_basic() {
    let tmp = tempdir().expect("create tempdir");
    let src_path = tmp.path().join("file_1");
    let page_size = 4 * 1024usize;
    let aligned_size = page_size * 1024; // 4MiB, lighter than C++ 64MiB for CI speed.
    let unaligned_len = 750usize;
    let payload = uniform_char_random_data(aligned_size + unaligned_len, 1213);
    write_local_file(&src_path, &payload)
        .await
        .expect("write source");

    let source = Arc::new(LocalFile::open_ro(&src_path).expect("open source"));
    let mut opt = test_options(tmp.path());
    opt.block_size = 1024 * 1024;
    opt.capacity_bytes = 512 * 1024 * 1024;
    let backend = FileCacheBackend::with_options(opt)
        .await
        .expect("create backend");
    let cached = backend
        .open_file("/testDir/file_1", source)
        .await
        .expect("open cached file");

    let last_offset = aligned_size as u64;
    let got = cached
        .read_at(last_offset, page_size)
        .await
        .expect("read tail");
    assert_eq!(got.len(), unaligned_len);
    assert_eq!(
        got.as_ref(),
        &payload[aligned_size..aligned_size + unaligned_len]
    );
    let got_again = cached
        .read_at(last_offset, page_size)
        .await
        .expect("read tail again");
    assert_eq!(got_again, got);

    let mixed_offset = (aligned_size - 2 * page_size) as u64;
    let mixed = cached
        .read_at(mixed_offset, page_size * 4)
        .await
        .expect("read mixed range");
    let mixed_len = 2 * page_size + unaligned_len;
    assert_eq!(mixed.len(), mixed_len);
    assert_eq!(
        mixed.as_ref(),
        &payload[(aligned_size - 2 * page_size)..(aligned_size - 2 * page_size + mixed_len)]
    );
    let mixed_again = cached
        .read_at(mixed_offset, page_size * 4)
        .await
        .expect("read mixed range again");
    assert_eq!(mixed_again, mixed);

    for i in 0..5usize {
        let off = i * page_size;
        let got = cached
            .read_at(off as u64, page_size)
            .await
            .expect("sequential read");
        assert_eq!(got.as_ref(), &payload[off..off + page_size]);
    }

    let mut rng = StdRng::seed_from_u64(1213);
    for i in 0..2048usize {
        let off = rng.random_range(0..=(payload.len() + page_size));
        let mut size = rng.random_range(0..=(8 * page_size));
        if off >= payload.len() {
            size = 0;
        } else {
            size = size.min(payload.len() - off);
        }
        let got = cached.read_at(off as u64, size).await.expect("random read");
        assert_eq!(got.len(), size, "iteration {i}, off {off}, size {size}");
        if off >= payload.len() {
            assert!(got.is_empty());
        } else {
            assert_eq!(got.as_ref(), &payload[off..off + size]);
        }
    }

    let small_path = tmp.path().join("small");
    let small_payload = uniform_char_random_data(102, 1214);
    write_local_file(&small_path, &small_payload)
        .await
        .expect("write small source");
    let small_source = Arc::new(LocalFile::open_ro(&small_path).expect("open small"));
    let small_cached = backend
        .open_file("/testDir/small", small_source)
        .await
        .expect("open small cached");
    let small_read_1 = small_cached
        .read_at(0, page_size)
        .await
        .expect("read small first");
    let small_read_2 = small_cached
        .read_at(0, page_size)
        .await
        .expect("read small second");
    assert_eq!(small_read_1.as_ref(), small_payload.as_slice());
    assert_eq!(small_read_2.as_ref(), small_payload.as_slice());

    let refill_path = tmp.path().join("refill");
    let refill_payload = uniform_char_random_data(4097, 1215);
    write_local_file(&refill_path, &refill_payload)
        .await
        .expect("write refill source");
    let refill_source = Arc::new(LocalFile::open_ro(&refill_path).expect("open refill"));
    let refill_cached = backend
        .open_file("/testDir/refill", refill_source)
        .await
        .expect("open refill cached");
    let first_page = refill_cached
        .read_at(0, page_size)
        .await
        .expect("read first page");
    assert_eq!(first_page.as_ref(), &refill_payload[..page_size]);
    let full = refill_cached
        .read_at(0, page_size * 2)
        .await
        .expect("read full refill");
    assert_eq!(full.as_ref(), refill_payload.as_slice());

    // Ported from C++ commonTest refill(2) + fadvise(POSIX_FADV_WILLNEED).
    cached.evict_all().await.expect("evict all");
    assert_eq!(
        cached
            .refill_range(page_size as u64, page_size * 2)
            .await
            .expect("refill range"),
        page_size * 2
    );
    let cache_only = backend
        .open_cache_only("/testDir/file_1", payload.len() as u64)
        .await
        .expect("open cache-only on same key");
    let prefetched = cache_only
        .read_at(page_size as u64, page_size * 2)
        .await
        .expect("read prefetched from cache-only");
    assert_eq!(prefetched.as_ref(), &payload[page_size..page_size * 3]);

    cached
        .fadvise(234, (5000 * page_size) as u64, libc::POSIX_FADV_WILLNEED)
        .await
        .expect("fadvise large prefetch");
    cached
        .fadvise(
            (aligned_size - page_size) as u64,
            (5000 * page_size) as u64,
            libc::POSIX_FADV_WILLNEED,
        )
        .await
        .expect("fadvise tail prefetch");

    // Ported from C++ commonTest refill(3): refill from user buffer.
    cache_only.evict_all().await.expect("evict cache-only");
    let first_page = &payload[..page_size];
    assert_eq!(
        cache_only
            .refill_from_slice(0, first_page)
            .await
            .expect("manual refill first page"),
        page_size
    );
    let first_read = cache_only
        .read_at(0, page_size)
        .await
        .expect("read manual refill first page");
    assert_eq!(first_read.as_ref(), first_page);

    let tail_offset = aligned_size - page_size;
    let tail_len = page_size + unaligned_len;
    let tail = &payload[tail_offset..tail_offset + tail_len];
    assert_eq!(
        cache_only
            .refill_from_slice(tail_offset as u64, tail)
            .await
            .expect("manual refill tail"),
        tail.len()
    );
    let tail_read = cache_only
        .read_at(tail_offset as u64, page_size * 3)
        .await
        .expect("read manual refill tail");
    assert_eq!(tail_read.as_ref(), tail);
}

// Ported from C++ RoCachedFs.BasicCacheFull (commonTest(true, false, false)).
#[tokio::test]
async fn test_ro_cached_fs_basic_cache_full() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4 * 1024;
    opt.capacity_bytes = 0; // simulate cache full / disabled writes to cache
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = uniform_char_random_data(128 * 1024, 1216);
    let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let cached = backend
        .open_file("/testDir/file_1", src.clone())
        .await
        .expect("open cached");

    let a = cached.read_at(4096, 8192).await.expect("read first");
    let b = cached.read_at(4096, 8192).await.expect("read second");
    assert_eq!(a, b);
    assert_eq!(src.read_calls(), 4);

    let stats = backend.stats();
    assert_eq!(stats.refills, 0);
    assert_eq!(stats.hits, 0);
    assert!(stats.misses >= 4);
}

// Ported from C++ RoCachedFs.CacheWithOutSrcFile:
// use cache-only mode to mirror srcFs==nullptr behavior.
#[tokio::test]
async fn test_ro_cached_fs_cache_without_src_file() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    const BLOCK_SIZE: usize = 256 * 1024;
    opt.block_size = BLOCK_SIZE as u64;
    opt.capacity_bytes = 512 * 1024 * 1024;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let cached1 = backend
        .open_cache_only("/testDir/file_1", 1024 * 1024)
        .await
        .expect("open cached file_1");

    let buf = vec![b'a'; BLOCK_SIZE];
    assert_eq!(
        cached1
            .write_at(BLOCK_SIZE as u64, &buf)
            .await
            .expect("write at offset"),
        BLOCK_SIZE,
    );
    let got_half = cached1
        .read_at(BLOCK_SIZE as u64, BLOCK_SIZE / 2)
        .await
        .expect("read half");
    assert_eq!(got_half.as_ref(), &buf[..BLOCK_SIZE / 2]);
    let err = cached1
        .read_at(0, BLOCK_SIZE)
        .await
        .expect_err("read hole should fail");
    assert_err_contains(&err, "cache miss without source file");

    let cached2 = backend
        .open_cache_only("/testDir/file_2", 1024 * 1024)
        .await
        .expect("open cached file_2");
    assert_eq!(
        cached2.write_at(0, &buf).await.expect("write 0"),
        BLOCK_SIZE
    );
    assert_eq!(
        cached2
            .write_at(BLOCK_SIZE as u64, &buf)
            .await
            .expect("write len"),
        BLOCK_SIZE
    );
    let r0 = cached2
        .read_at(0, BLOCK_SIZE)
        .await
        .expect("read first chunk");
    let r1 = cached2
        .read_at(BLOCK_SIZE as u64, BLOCK_SIZE)
        .await
        .expect("read second chunk");
    let err = cached2
        .read_at((BLOCK_SIZE * 2) as u64, BLOCK_SIZE)
        .await
        .expect_err("read hole should fail");
    assert_eq!(r0.as_ref(), buf.as_slice());
    assert_eq!(r1.as_ref(), buf.as_slice());
    assert_err_contains(&err, "cache miss without source file");
}

// Ported from C++ RoCachedFS.xattr.
#[tokio::test]
async fn test_ro_cached_fs_xattr() {
    let tmp = tempdir().expect("create tempdir");
    let path = tmp.path().join("filexattr");
    let payload = b"xattr-payload";
    write_local_file(&path, payload)
        .await
        .expect("write source");

    let path_c = CString::new(path.as_os_str().as_bytes()).expect("path cstr");
    let name = CString::new("user.testxattr").expect("name cstr");
    let value = b"yes";

    unsafe {
        // SAFETY: pointers are valid and lengths are exact for libc xattr calls.
        let ret = libc::setxattr(
            path_c.as_ptr(),
            name.as_ptr(),
            value.as_ptr() as *const libc::c_void,
            value.len(),
            0,
        );
        if ret != 0 {
            let err = Errno::last();
            let unsupported = err == Errno::ENOTSUP
                || (libc::EOPNOTSUPP != libc::ENOTSUP && err == Errno::EOPNOTSUPP);
            if unsupported {
                return;
            }
            panic!("setxattr failed: {err}");
        }

        let mut out = [0u8; 32];
        let got = libc::getxattr(
            path_c.as_ptr(),
            name.as_ptr(),
            out.as_mut_ptr() as *mut libc::c_void,
            out.len(),
        );
        assert_eq!(got as usize, value.len());
        assert_eq!(&out[..value.len()], value);

        let ret = libc::removexattr(path_c.as_ptr(), name.as_ptr());
        assert_eq!(ret, 0);
    }

    let source = Arc::new(LocalFile::open_rw(&path, false).expect("open source"));
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");
    let cached = backend
        .open_file("/filexattr", source)
        .await
        .expect("open cached");
    let got = cached.read_at(0, payload.len()).await.expect("cached read");
    assert_eq!(got.as_ref(), payload);

    let fname = "user.cachedxattr";
    let fvalue = b"cached-yes";
    if let Err(err) = cached.fsetxattr(fname, fvalue, 0).await {
        let unsupported = err_is_errno(&err, Errno::ENOTSUP)
            || (libc::EOPNOTSUPP != libc::ENOTSUP && err_is_errno(&err, Errno::EOPNOTSUPP));
        if unsupported {
            return;
        }
        panic!("fsetxattr failed: {err}");
    }
    let names = cached.flistxattr().await.expect("flistxattr");
    assert!(names.iter().any(|n| n == fname));
    let got = cached.fgetxattr(fname).await.expect("fgetxattr");
    assert_eq!(got.as_slice(), fvalue);
    cached.fremovexattr(fname).await.expect("fremovexattr");
}

// Ported from C++ CachedFS.write_while_full.
#[tokio::test]
async fn test_cached_fs_write_while_full() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 64 * 1024;
    opt.capacity_bytes = 1024 * 1024; // intentionally tiny
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = Arc::new(uniform_char_random_data(32 * 1024 * 1024, 1217));
    let source = Arc::new(MockSource::new(payload.to_vec(), Duration::ZERO));
    let cached = backend
        .open_file("/huge", source)
        .await
        .expect("open cached");

    let mut handles = Vec::new();
    for worker_id in 0..2u64 {
        let file = cached.clone();
        let expected = payload.clone();
        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(0x5eed + worker_id);
            let chunk = 256 * 1024usize;
            for _ in 0..64usize {
                let max_off = expected.len().saturating_sub(chunk);
                let off = if max_off == 0 {
                    0
                } else {
                    rng.random_range(0..=max_off)
                };
                let got = file
                    .read_at(off as u64, chunk)
                    .await
                    .expect("concurrent read");
                assert_eq!(got.as_ref(), &expected[off..off + chunk]);
                let _ = file.evict_all().await;
                tokio::task::yield_now().await;
            }
        }));
    }
    for h in handles {
        h.await.expect("join worker");
    }
}

// Ported from C++ CachedFS.fn_trans_func.
#[tokio::test]
async fn test_cached_fs_fn_trans_func() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4 * 1024;
    opt.capacity_bytes = 32 * 1024 * 1024;
    let trans: CacheFnTransFunc = Arc::new(|src: &str| {
        src.find("/sha256:")
            .map(|idx| src[idx..].to_string())
            .filter(|s| !s.is_empty())
    });
    let backend = FileCacheBackend::with_options_and_trans_func(opt, Some(trans))
        .await
        .expect("backend");

    let payload = uniform_char_random_data(1024, 1218);
    let src_a = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let src_b = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));

    let f1 = backend
        .open_src_name_with_flags("/path_aaa/sha256:test", src_a.clone(), 0)
        .await
        .expect("open file a");
    let a = f1.read_at(0, 1024).await.expect("read a");
    assert_eq!(src_a.read_calls(), 1);

    let f2 = backend
        .open_src_name_with_flags("/path_bbb/sha256:test", src_b.clone(), 0)
        .await
        .expect("open file b");
    let b = f2.read_at(0, 1024).await.expect("read b");
    assert_eq!(a, b);
    assert_eq!(src_b.read_calls(), 0);

    let stats = backend.stats();
    assert_eq!(stats.entries, 1);
    assert!(backend
        .file_stats_by_src_name("/path_aaa/sha256:test")
        .is_some());
}

#[tokio::test]
async fn test_cache_pool_style_rename_and_evict() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4 * 1024;
    opt.capacity_bytes = 32 * 1024 * 1024;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = uniform_char_random_data(8192, 1220);
    {
        let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
        let file = backend
            .open_file("/cache/old", src.clone())
            .await
            .expect("open old");
        let _ = file.read_at(0, 4096).await.expect("populate old cache");
    }
    assert!(backend.file_stats("/cache/old").is_some());

    backend
        .rename_store_key("/cache/old", "/cache/new")
        .await
        .expect("rename cache key");
    assert!(backend.file_stats("/cache/old").is_none());
    assert!(backend.file_stats("/cache/new").is_some());

    let cache_only = backend
        .open_cache_only("/cache/new", payload.len() as u64)
        .await
        .expect("open renamed cache entry");
    let got = cache_only
        .read_at(0, 4096)
        .await
        .expect("read renamed cache");
    assert_eq!(got.as_ref(), &payload[..4096]);
    drop(cache_only);

    backend
        .evict_store_key("/cache/new")
        .await
        .expect("evict renamed key");
    assert!(backend.file_stats("/cache/new").is_none());

    {
        let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
        let file = backend
            .open_src_name_with_flags("/test/evict-src", src.clone(), 0)
            .await
            .expect("open src-name key");
        let _ = file.read_at(0, 1024).await.expect("populate src-name");
    }
    assert!(backend.file_stats_by_src_name("/test/evict-src").is_some());
    backend
        .evict_src_name("/test/evict-src")
        .await
        .expect("evict src-name");
    assert!(backend.file_stats_by_src_name("/test/evict-src").is_none());

    {
        let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
        let file = backend
            .open_file("/cache/global-a", src.clone())
            .await
            .expect("open global a");
        let _ = file.read_at(0, 1024).await.expect("populate global a");
    }
    {
        let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
        let file = backend
            .open_file("/cache/global-b", src.clone())
            .await
            .expect("open global b");
        let _ = file.read_at(0, 1024).await.expect("populate global b");
    }
    assert!(backend.stats().entries >= 2);
    backend.evict_global().await.expect("evict global");
    assert_eq!(backend.stats().entries, 0);
}

#[tokio::test]
async fn test_cache_only_read_flags_semantics() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");
    let payload = uniform_char_random_data(8192, 1221);
    let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file = backend
        .open_file_with_flags("flags-key", src, O_CACHE_ONLY)
        .await
        .expect("open with cache-only flag");

    // With mmap-based cache, source_size is known at open time (queried from
    // source), so a cache-only read on uncached blocks returns NotFound
    // instead of empty bytes.
    let miss = file.read_at_with_flags(0, 4096, RW_V2_CACHE_ONLY).await;
    assert!(miss.is_err());

    // Write to cache and then cache-only read should hit.
    assert_eq!(
        file.write_at_with_flags(0, &payload[..4096], 0)
            .await
            .expect("write cache"),
        4096
    );
    let hit = file
        .read_at_with_flags(0, 4096, RW_V2_CACHE_ONLY)
        .await
        .expect("cache-only read hit");
    assert_eq!(hit.as_ref(), &payload[..4096]);
}

#[tokio::test]
async fn test_write_alignment_semantics() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");
    let file = backend
        .open_cache_only("align-key", 16 * 1024)
        .await
        .expect("open cache-only");

    let bad_off = file
        .write_at(1, &[1u8; 4096])
        .await
        .expect_err("unaligned offset should fail");
    assert_err_contains(&bad_off, "not aligned");

    let bad_size = file
        .write_at(4 * 1024, &[2u8; 1234])
        .await
        .expect_err("unaligned size before eof should fail");
    assert_err_contains(&bad_size, "not aligned");

    // NOTE: ftruncate to grow beyond the initial mmap region is not
    // supported with the mmap-based cache (source_size is fixed at
    // creation). The write-past-boundary test case has been removed.
}

#[tokio::test]
async fn test_source_backed_write_updates_cache_not_source() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");

    let origin = vec![0x5au8; 8192];
    let patch = vec![0x9cu8; 4096];
    let src = Arc::new(MockSource::new(origin.clone(), Duration::ZERO));
    let file = backend
        .open_file("src-write-key", src.clone())
        .await
        .expect("open cached file");

    assert_eq!(
        file.write_at(0, &patch).await.expect("write cache"),
        patch.len()
    );

    let cached = file.read_at(0, 4096).await.expect("read cached");
    assert_eq!(cached.as_ref(), patch.as_slice());

    // Source remains unchanged.
    let source_bytes = src.read_at(0, 4096).await.expect("read source");
    assert_eq!(source_bytes.as_ref(), &origin[..4096]);
}

#[tokio::test]
async fn test_cached_file_set_source_and_query_semantics() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4096;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = uniform_char_random_data(8192, 1222);
    let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file = backend
        .open_file("/set-source/query", src.clone())
        .await
        .expect("open cached");

    let miss = file.query(0, 64).await.expect("query before fill");
    assert!(miss > 0);
    let _ = file.read_at(0, 64).await.expect("fill by read");
    let hit = file.query(0, 64).await.expect("query after fill");
    assert_eq!(hit, 0);

    let source = file.get_source().expect("has source");
    file.set_source(None);
    let _ = file.read_at(0, 64).await.expect("read from cache-only hit");
    let err = file
        .read_at(4096, 64)
        .await
        .expect_err("cache miss without source should fail");
    assert_err_contains(&err, "cache miss without source file");

    file.set_source(Some(source));
    let _ = file
        .read_at(4096, 64)
        .await
        .expect("read after reattach source");
}

#[tokio::test]
async fn test_cache_pool_controls_stat_list_and_evict_size() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4096;
    opt.capacity_bytes = 32 * 4096;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = uniform_char_random_data(16 * 1024, 1223);
    {
        let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
        let f = backend
            .open_file("/pool/a", src.clone())
            .await
            .expect("open a");
        let _ = f.read_at(0, 4096).await.expect("fill a");
    }
    {
        let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
        let f = backend
            .open_file("/pool/b", src.clone())
            .await
            .expect("open b");
        let _ = f.read_at(0, 4096).await.expect("fill b");
    }

    let list = backend
        .list("/pool", CacheListType::All, None, 10)
        .expect("list");
    assert_eq!(list.len(), 2);

    let before = backend.stat_path(None).expect("global stat");
    assert!(before.used_size > 0);
    let evicted = backend.evict_by_size(4096).await.expect("evict by size");
    assert!(evicted >= 4096);
    let after = backend.stat_path(None).expect("global stat after evict");
    assert!(after.evict_user >= evicted);
    assert!(after.used_size <= before.used_size);

    let unsupported = backend
        .set_quota("/pool", 1024 * 1024)
        .await
        .expect_err("set_quota unsupported");
    assert_err_contains(&unsupported, "set_quota is not supported");
    let unsupported = backend.resize(1, 0).await.expect_err("resize unsupported");
    assert_err_contains(&unsupported, "resize is not supported");
}

#[tokio::test]
async fn test_overlaybd_watermark_formula_alignment() {
    let tmp = tempdir().expect("create tempdir");
    let cap = 4 * GIB;
    let water_mark = FileCacheBackend::calc_water_mark(cap);
    assert_eq!(water_mark, cap * WATERMARK_RATIO / 100);

    let opt = FileCacheBackendOptions {
        cache_dir: tmp.path().join("formula"),
        capacity_bytes: cap,
        block_size: DEFAULT_BLOCK_SIZE,
        ..Default::default()
    };
    let backend = FileCacheBackend::with_options_and_trans_func(opt, None)
        .await
        .expect("backend");
    let expected_risk = cap
        .saturating_sub(EVICTION_MARK_BYTES)
        .max((water_mark + cap) / 2);
    assert_eq!(backend.risk_mark(), expected_risk);
}

#[tokio::test]
async fn test_full_file_cache_rejects_invalid_block_size() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4097;
    let err = FileCacheBackend::with_options(opt)
        .await
        .expect_err("unaligned block_size should fail");
    assert_err_contains(&err, "block_size must be");

    let mut opt = test_options(tmp.path());
    opt.block_size = 12 * 1024;
    let err = FileCacheBackend::with_options(opt)
        .await
        .expect_err("non-power-of-two block_size should fail");
    assert_err_contains(&err, "block_size must be");
}

#[tokio::test]
async fn test_capacity_zero_rejects_cache_write() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4096;
    opt.capacity_bytes = 0;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = uniform_char_random_data(8192, 1440);
    let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file = backend
        .open_file("capacity-zero-write", src)
        .await
        .expect("open cached file");

    let err = file
        .write_at(0, &payload[..4096])
        .await
        .expect_err("cache write should fail");
    assert!(err_is_errno(&err, Errno::ENOSPC));
}

#[tokio::test]
async fn test_cached_fs_facade_open_access_rename_and_unlink() {
    let tmp = tempdir().expect("create tempdir");
    let src_root = tmp.path().join("src");
    tokio::fs::create_dir_all(src_root.join("dir"))
        .await
        .expect("create src dir");
    let src_path = src_root.join("dir").join("file");
    let payload = uniform_char_random_data(8192, 1224);
    write_local_file(&src_path, &payload)
        .await
        .expect("write source");

    let backend = FileCacheBackend::with_options(test_options(&tmp.path().join("cache")))
        .await
        .expect("backend");
    let source_fs = Arc::new(LocalFsSource::new(&src_root));
    let cached_fs = CachedFs::new(Some(source_fs.clone()), backend.clone());

    let cached = cached_fs.open("/dir/file", 0).await.expect("open cached");
    let got = cached.read_at(0, 4096).await.expect("cached read");
    assert_eq!(got.as_ref(), &payload[..4096]);
    assert!(backend.contains_src_name("/dir/file"));
    drop(cached);

    cached_fs
        .rename("/dir/file", "/dir/file_renamed")
        .await
        .expect("rename cache entry");
    assert!(!backend.contains_src_name("/dir/file"));
    assert!(backend.contains_src_name("/dir/file_renamed"));

    cached_fs.set_source(None);
    cached_fs
        .access("/dir/file_renamed", libc::F_OK)
        .await
        .expect("access renamed cache");
    let err = cached_fs
        .access("/dir/file", libc::F_OK)
        .await
        .expect_err("old cache key should be gone");
    assert_err_contains(&err, "cache entry not found");

    let cache_only = cached_fs
        .open("/dir/file_renamed", 0)
        .await
        .expect("open renamed cache-only");
    let got = cache_only
        .read_at(0, 4096)
        .await
        .expect("read renamed cache");
    assert_eq!(got.as_ref(), &payload[..4096]);
    drop(cache_only);

    cached_fs
        .unlink("/dir/file_renamed")
        .await
        .expect("unlink cache-only key");
    let err = cached_fs
        .access("/dir/file_renamed", libc::F_OK)
        .await
        .expect_err("renamed key should be evicted");
    assert_err_contains(&err, "cache entry not found");
}

#[tokio::test]
async fn test_cached_fs_facade_open_is_lazy_like_cpp() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");
    let payload = uniform_char_random_data(4096, 1441);
    let source_fs = Arc::new(CountingFsSource::new([(
        "/lazy/file".to_string(),
        payload.clone(),
    )]));
    let cached_fs = CachedFs::new(Some(source_fs.clone()), backend);

    let cached = cached_fs.open("/lazy/file", 0).await.expect("open cached");
    // With mmap-based cache, source.size() is queried at open time to
    // create the mmap region, so the lazy source is opened during open().
    assert_eq!(source_fs.open_calls(), 1);

    let got = cached
        .read_at(0, 4096)
        .await
        .expect("read source-backed miss");
    assert_eq!(got.as_ref(), payload.as_slice());
    assert_eq!(source_fs.open_calls(), 1);

    drop(cached);
    source_fs.set_fail_open(true);

    let cached2 = cached_fs
        .open("/lazy/file", 0)
        .await
        .expect("reopen hot cache");
    // Re-open reuses existing cache entry, no new source open needed.
    assert_eq!(source_fs.open_calls(), 1);
    let got = cached2.read_at(0, 4096).await.expect("read hot cache");
    assert_eq!(got.as_ref(), payload.as_slice());
    assert_eq!(source_fs.open_calls(), 1);
}

#[tokio::test]
async fn test_cached_fs_facade_unlink_prefers_source_fs_status() {
    let tmp = tempdir().expect("create tempdir");
    let src_root = tmp.path().join("src");
    tokio::fs::create_dir_all(&src_root)
        .await
        .expect("create src root");
    let src_path = src_root.join("unlink-me");
    let payload = uniform_char_random_data(4096, 1225);
    write_local_file(&src_path, &payload)
        .await
        .expect("write source");

    let backend = FileCacheBackend::with_options(test_options(&tmp.path().join("cache")))
        .await
        .expect("backend");
    let source_fs = Arc::new(LocalFsSource::new(&src_root));
    let cached_fs = CachedFs::new(Some(source_fs), backend.clone());

    let cached = cached_fs.open("/unlink-me", 0).await.expect("open cached");
    let _ = cached.read_at(0, 1024).await.expect("populate cache");
    assert!(backend.contains_src_name("/unlink-me"));
    drop(cached);

    cached_fs
        .unlink("/unlink-me")
        .await
        .expect("unlink through source fs");
    assert!(!src_path.exists());
    assert!(!backend.contains_src_name("/unlink-me"));

    let err = cached_fs
        .unlink("/unlink-me")
        .await
        .expect_err("unlink should return source fs status");
    assert_err_contains(&err, "No such file or directory");
}

#[tokio::test]
async fn test_cached_fs_facade_path_xattr_passthrough() {
    let tmp = tempdir().expect("create tempdir");
    let src_root = tmp.path().join("src");
    tokio::fs::create_dir_all(&src_root)
        .await
        .expect("create src root");
    let src_path = src_root.join("filexattr");
    write_local_file(&src_path, b"path-xattr")
        .await
        .expect("write source");

    let backend = FileCacheBackend::with_options(test_options(&tmp.path().join("cache")))
        .await
        .expect("backend");
    let source_fs = Arc::new(LocalFsSource::new(&src_root));
    let cached_fs = CachedFs::new(Some(source_fs), backend);

    let name = "user.cachedfsxattr";
    let value = b"cached-fs";
    if let Err(err) = cached_fs.setxattr("/filexattr", name, value, 0).await {
        let unsupported = err_is_errno(&err, Errno::ENOTSUP)
            || (libc::EOPNOTSUPP != libc::ENOTSUP && err_is_errno(&err, Errno::EOPNOTSUPP));
        if unsupported {
            return;
        }
        panic!("setxattr failed: {err}");
    }

    let names = cached_fs.listxattr("/filexattr").await.expect("listxattr");
    assert!(names.iter().any(|n| n == name));
    let got = cached_fs
        .getxattr("/filexattr", name)
        .await
        .expect("getxattr");
    assert_eq!(got.as_slice(), value);
    cached_fs
        .removexattr("/filexattr", name)
        .await
        .expect("removexattr");
}

#[tokio::test]
async fn test_cached_file_evict_does_not_forward_to_source() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");
    let payload = uniform_char_random_data(8192, 1442);
    let source = Arc::new(EvictTrackingSource::new(payload.clone()));
    let file = backend
        .open_file("/evict/no-forward", source.clone())
        .await
        .expect("open cached file");

    let got = file.read_at(0, payload.len()).await.expect("fill cache");
    assert_eq!(got.as_ref(), payload.as_slice());

    file.evict_range(0, 4096).await.expect("evict range");
    file.evict_all().await.expect("evict all");

    assert_eq!(source.evict_range_calls(), 0);
    assert_eq!(source.evict_all_calls(), 0);
}

#[tokio::test]
async fn test_cached_fs_facade_source_fs_passthrough_misc() {
    let tmp = tempdir().expect("create tempdir");
    let src_root = tmp.path().join("src");
    tokio::fs::create_dir_all(&src_root)
        .await
        .expect("create src root");

    let backend = FileCacheBackend::with_options(test_options(&tmp.path().join("cache")))
        .await
        .expect("backend");
    let source_fs = Arc::new(LocalFsSource::new(&src_root));
    let cached_fs = CachedFs::new(Some(source_fs), backend);

    cached_fs
        .mkdir("/dir", 0o755)
        .await
        .expect("mkdir through cached fs");
    assert!(src_root.join("dir").exists());

    let file_path = src_root.join("dir").join("file");
    write_local_file(&file_path, b"abc")
        .await
        .expect("write test file");
    let st = cached_fs.stat("/dir/file").await.expect("stat file");
    assert_eq!(st.len(), 3);

    let link_path = src_root.join("dir").join("link");
    std::os::unix::fs::symlink("file", &link_path).expect("create symlink");
    let target = cached_fs.readlink("/dir/link").await.expect("readlink");
    assert_eq!(target, PathBuf::from("file"));
    let lst = cached_fs.lstat("/dir/link").await.expect("lstat symlink");
    assert!(lst.file_type().is_symlink());

    let dirents = cached_fs.opendir("/dir").await.expect("opendir");
    assert!(dirents.iter().any(|s| s == "file"));
    assert!(dirents.iter().any(|s| s == "link"));

    let fs1 = cached_fs.statfs("/").await.expect("statfs");
    let fs2 = cached_fs.statvfs("/").await.expect("statvfs");
    assert!(fs1.f_bsize > 0);
    assert!(fs2.f_bsize > 0);

    cached_fs.mkdir("/empty", 0o755).await.expect("mkdir empty");
    cached_fs.rmdir("/empty").await.expect("rmdir empty");
    assert!(!src_root.join("empty").exists());
}

#[tokio::test]
async fn test_cached_fs_facade_misc_without_source_is_unsupported() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");
    let cached_fs = CachedFs::new(None, backend);

    assert_eq!(
        cached_fs
            .mkdir("/d", 0o755)
            .await
            .expect_err("mkdir should fail")
            .to_string(),
        "mkdir is not supported without source fs"
    );
    assert_eq!(
        cached_fs
            .rmdir("/d")
            .await
            .expect_err("rmdir should fail")
            .to_string(),
        "rmdir is not supported without source fs"
    );
    assert_eq!(
        cached_fs
            .readlink("/l")
            .await
            .expect_err("readlink should fail")
            .to_string(),
        "readlink is not supported without source fs"
    );
    assert_eq!(
        cached_fs
            .stat("/x")
            .await
            .expect_err("stat should fail")
            .to_string(),
        "stat is not supported without source fs"
    );
    assert_eq!(
        cached_fs
            .lstat("/x")
            .await
            .expect_err("lstat should fail")
            .to_string(),
        "lstat is not supported without source fs"
    );
    assert_eq!(
        cached_fs
            .statfs("/")
            .await
            .expect_err("statfs should fail")
            .to_string(),
        "statfs is not supported without source fs"
    );
    assert_eq!(
        cached_fs
            .statvfs("/")
            .await
            .expect_err("statvfs should fail")
            .to_string(),
        "statvfs is not supported without source fs"
    );
    assert_eq!(
        cached_fs
            .opendir("/")
            .await
            .expect_err("opendir should fail")
            .to_string(),
        "opendir is not supported without source fs"
    );
}

#[tokio::test]
async fn test_sparse_file_evict_all_resets() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4096;
    opt.capacity_bytes = 1024 * 1024;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = uniform_char_random_data(8192, 9999);
    let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file = backend
        .open_file("sparse-evict", src.clone())
        .await
        .expect("open cached file");

    // Fill block 0.
    let _ = file.read_at(0, 4096).await.expect("fill block 0");
    let stats_before = backend.stats();
    assert!(
        stats_before.bytes_used >= 4096,
        "bytes_used={}, expected >= 4096",
        stats_before.bytes_used
    );

    // Evict all blocks.
    file.evict_all().await.expect("evict all");
    let stats_after = backend.stats();
    assert!(stats_after.bytes_used < stats_before.bytes_used);

    // Re-read should go to source again.
    let got = file.read_at(0, 4096).await.expect("re-read after evict");
    assert_eq!(got.as_ref(), &payload[..4096]);
    assert!(src.read_calls() >= 2);
}

/// Helper: assert read_at and read_at_into return the same data on a VirtualFile.
async fn assert_read_at_into_matches(file: &dyn VirtualFile, offset: u64, len: usize) {
    let bytes_result = file.read_at(offset, len).await.expect("read_at failed");
    let mut into_buf = vec![0u8; len];
    let n = file
        .read_at_into(offset, &mut into_buf)
        .await
        .expect("read_at_into failed");
    assert_eq!(
        n,
        bytes_result.len(),
        "read_at_into length mismatch at offset={offset} len={len}"
    );
    assert_eq!(
        &into_buf[..n],
        &bytes_result[..],
        "read_at_into data mismatch at offset={offset} len={len}"
    );
}

#[tokio::test]
async fn test_cached_file_read_at_into_single_block() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4096;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = uniform_char_random_data(8192, 1234);
    let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file = backend
        .open_file("read-into-single", src.clone())
        .await
        .expect("open cached file");

    // Single-block read
    assert_read_at_into_matches(file.as_ref(), 0, 4096).await;
    // Partial within a single block
    assert_read_at_into_matches(file.as_ref(), 100, 200).await;
}

#[tokio::test]
async fn test_cached_file_read_at_into_multi_block() {
    let tmp = tempdir().expect("create tempdir");
    let mut opt = test_options(tmp.path());
    opt.block_size = 4096;
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let payload = uniform_char_random_data(16384, 5678);
    let src = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file = backend
        .open_file("read-into-multi", src.clone())
        .await
        .expect("open cached file");

    // Multi-block read spanning 2 blocks
    assert_read_at_into_matches(file.as_ref(), 0, 8192).await;
    // Multi-block read spanning 3 blocks with partial first/last
    assert_read_at_into_matches(file.as_ref(), 1000, 10000).await;
}

#[tokio::test]
async fn test_cached_file_read_at_into_empty() {
    let tmp = tempdir().expect("create tempdir");
    let backend = FileCacheBackend::with_options(test_options(tmp.path()))
        .await
        .expect("backend");

    let payload = vec![0x42; 4096];
    let src = Arc::new(MockSource::new(payload, Duration::ZERO));
    let file = backend
        .open_file("read-into-empty", src)
        .await
        .expect("open cached file");

    let mut buf = [];
    let n = file.read_at_into(0, &mut buf).await.expect("empty read");
    assert_eq!(n, 0);
}

/// Regression test for the eviction race condition:
/// concurrent readers + capacity pressure must never observe all-zero
/// (punched-hole) data from the mmap region.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "concurrency stress test; run manually with --ignored"]
async fn test_concurrent_read_during_capacity_eviction() {
    const NUM_KEYS: usize = 16;
    const BLOCK_SIZE: u64 = 4096;
    const DATA_SIZE: usize = BLOCK_SIZE as usize;
    const READER_ITERS: usize = 400;
    const NUM_READERS: usize = 32;

    let tmp = tempdir().expect("create tempdir");
    let opt = FileCacheBackendOptions {
        cache_dir: tmp.path().to_path_buf(),
        capacity_bytes: 4 * BLOCK_SIZE,
        block_size: BLOCK_SIZE,
        ..Default::default()
    };
    let backend = FileCacheBackend::with_options(opt).await.expect("backend");

    let mut expected: HashMap<String, Arc<Vec<u8>>> = HashMap::new();
    for i in 0..NUM_KEYS {
        let key = format!("/race/entry-{i}");
        let data = Arc::new(uniform_char_random_data(DATA_SIZE, 0xface + i as u64));
        assert!(data.iter().any(|&b| b != 0), "test data must be non-zero");
        expected.insert(key, data);
    }
    let expected = Arc::new(expected);

    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();

    // Reader tasks: open, read, verify data for random keys.
    for reader_id in 0..NUM_READERS {
        let backend = backend.clone();
        let expected = expected.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(0xdead + reader_id as u64);
            let keys: Vec<String> = expected.keys().cloned().collect();
            for iter in 0..READER_ITERS {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let key = &keys[rng.random_range(0..keys.len())];
                let data = &expected[key];
                let src = Arc::new(MockSource::new(data.to_vec(), Duration::ZERO));
                let file = backend
                    .open_file(key.clone(), src)
                    .await
                    .expect("open cached file");
                let got = file
                    .read_at(0, DATA_SIZE)
                    .await
                    .expect("read cached block");
                assert_eq!(
                    got.as_ref(),
                    data.as_slice(),
                    "reader {reader_id} iter {iter} key {key}: data mismatch (possible eviction race)"
                );
            }
        }));
    }

    // Pressure task: keep filling cache to trigger eviction.
    {
        let backend = backend.clone();
        let expected = expected.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let keys: Vec<String> = expected.keys().cloned().collect();
            let mut idx = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let key = &keys[idx % keys.len()];
                let data = &expected[key];
                let src = Arc::new(MockSource::new(data.to_vec(), Duration::ZERO));
                {
                    let file = backend
                        .open_file(key.clone(), src)
                        .await
                        .expect("open pressure file");
                    let _ = file.read_at(0, DATA_SIZE).await;
                }
                if idx.is_multiple_of(8) {
                    let _ = backend.evict_by_size(BLOCK_SIZE).await;
                }
                idx += 1;
                if idx > READER_ITERS * NUM_READERS {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in &mut handles {
        if let Err(e) = h.await {
            stop.store(true, Ordering::Relaxed);
            panic!("task failed: {e}");
        }
    }
}

/// Regression for the 2026-08-11 cache-poison incident: eviction must clear
/// the in-memory bitmap before any destructive filesystem operation, so a
/// failed eviction can never leave a bitmap claiming blocks whose data is
/// already gone (which would serve zeroed bytes to every later read).
#[tokio::test]
async fn test_eviction_clears_bitmap_before_destroying_data() {
    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let payload = uniform_char_random_data(4096, 31);
    let source = Arc::new(MockSource::new(payload.clone(), Duration::ZERO));
    let file = backend
        .open_file_with_source_size("evict-order", source, payload.len() as u64)
        .await
        .expect("open cached file");
    assert_eq!(
        file.read_at(0, 4096).await.expect("warm read").as_ref(),
        payload.as_slice()
    );

    // Force eviction to fail at metadata removal: meta.bin is replaced by a
    // directory (fresh entries have no meta.bin until the first checkpoint).
    let entry = backend
        .get_cache_entry(&super::cache_key_digest("evict-order"))
        .expect("entry");
    let meta_path = entry.paths.meta_path.clone();
    let _ = std::fs::remove_file(&meta_path);
    std::fs::create_dir(&meta_path).expect("replace meta.bin with a directory");
    entry
        .evict_all_blocks()
        .await
        .expect_err("eviction must fail when meta removal fails");

    assert!(
        entry.index.read().is_empty(),
        "a failed eviction must leave the in-memory bitmap cleared"
    );
    std::fs::remove_dir(&meta_path).expect("cleanup meta directory");
    assert_eq!(
        file.read_at(0, 4096)
            .await
            .expect("read after failed eviction")
            .as_ref(),
        payload.as_slice(),
        "reads after a failed eviction must fall back to the source"
    );
}

#[tokio::test]
async fn test_reserve_range_guard_punches_unless_committed() {
    use std::os::unix::fs::MetadataExt;

    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let source = Arc::new(ControlledSource::new(vec![1u8; 8192], 4096));
    let _file = backend
        .open_file_with_source_size("reserve-guard", source, 8192)
        .await
        .expect("open cached file");
    let entry = backend
        .get_cache_entry(&super::cache_key_digest("reserve-guard"))
        .expect("entry");
    let data_path = entry.paths.data_path.clone();
    let allocated_blocks = move || {
        std::fs::metadata(&data_path)
            .expect("stat cache data file")
            .blocks()
    };

    let baseline = allocated_blocks();
    let reservation = entry.reserve_range(0, 4096).expect("reserve");
    if allocated_blocks() == baseline {
        // Filesystem without fallocate support: reservation and punch are
        // no-ops and there is nothing further to verify.
        return;
    }

    drop(reservation);
    assert_eq!(
        allocated_blocks(),
        baseline,
        "dropping an uncommitted reservation must punch the range back"
    );

    entry.reserve_range(0, 4096).expect("reserve").commit();
    assert!(
        allocated_blocks() > baseline,
        "a committed reservation must keep the disk blocks"
    );
}

#[tokio::test]
async fn test_failed_foreground_refill_leaves_no_reserved_blocks() {
    use std::os::unix::fs::MetadataExt;

    let tmp = tempdir().expect("create tempdir");
    let mut options = test_options(tmp.path());
    options.block_size = 4096;
    let backend = FileCacheBackend::with_options(options)
        .await
        .expect("backend");
    let payload = uniform_char_random_data(8192, 42);
    let source = Arc::new(ControlledSource::new(payload.clone(), 4096));
    // Fail the refill's source read once; the foreground read then falls
    // back to a second, successful source read.
    source.fail_block(0, 1);
    let file = backend
        .open_file_with_source_size("refill-rollback", source.clone(), payload.len() as u64)
        .await
        .expect("open cached file");

    let bytes = file
        .read_at(0, 4096)
        .await
        .expect("read must fall back to the source after the failed refill");
    assert_eq!(bytes.as_ref(), &payload[..4096]);
    assert_eq!(source.calls(0), 2, "refill attempt plus fallback read");

    // The failed refill must neither publish the block nor account bytes...
    let stats = backend.file_stats("refill-rollback").expect("file stats");
    assert_eq!(stats.bytes_used, 0, "failed refill must not publish blocks");
    assert_eq!(
        backend.stats().bytes_used,
        0,
        "failed refill must not change capacity accounting"
    );

    // ...and must roll its reservation back: no allocated disk blocks may
    // remain in the sparse data file.
    let entry = backend
        .get_cache_entry(&super::cache_key_digest("refill-rollback"))
        .expect("entry");
    assert_eq!(
        std::fs::metadata(&entry.paths.data_path)
            .expect("stat cache data file")
            .blocks(),
        0,
        "failed refill must leave the data file fully sparse"
    );
}
