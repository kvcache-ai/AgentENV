use crate::io::virtual_file::VirtualFile;
#[cfg(feature = "io-uring")]
use crate::io::virtual_file::{IoCtx, LocalBoxFuture};
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

const TRACE_MAGIC: u32 = 3_270_449_184;
const TRACE_HEADER_SIZE: usize = 24;
const TRACE_RECORD_SIZE: usize = 24;
const POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrefetchMode {
    Disabled,
    Record,
    Replay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TraceOp {
    Read,
    Write,
}

impl TraceOp {
    fn as_byte(self) -> u8 {
        match self {
            Self::Read => b'R',
            Self::Write => b'W',
        }
    }

    fn from_byte(value: u8) -> Result<Self> {
        match value {
            b'R' => Ok(Self::Read),
            b'W' => Ok(Self::Write),
            other => bail!("invalid trace op byte: {other}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TraceRecord {
    op: TraceOp,
    layer_index: u32,
    count: u64,
    offset: i64,
}

impl TraceRecord {
    fn new_read(layer_index: u32, count: usize, offset: u64) -> Result<Self> {
        Ok(Self {
            op: TraceOp::Read,
            layer_index,
            count: u64::try_from(count).context("trace count does not fit in u64")?,
            offset: i64::try_from(offset).context("trace offset does not fit in i64")?,
        })
    }

    fn encode(self) -> [u8; TRACE_RECORD_SIZE] {
        let mut out = [0u8; TRACE_RECORD_SIZE];
        out[0] = self.op.as_byte();
        out[4..8].copy_from_slice(&self.layer_index.to_le_bytes());
        out[8..16].copy_from_slice(&self.count.to_le_bytes());
        out[16..24].copy_from_slice(&self.offset.to_le_bytes());
        out
    }

    fn decode(raw: &[u8]) -> Result<Self> {
        ensure!(raw.len() == TRACE_RECORD_SIZE, "invalid trace record size",);
        let op = TraceOp::from_byte(raw[0])?;
        let layer_index = u32::from_le_bytes(raw[4..8].try_into().expect("trace layer index"));
        let count = u64::from_le_bytes(raw[8..16].try_into().expect("trace count"));
        let offset = i64::from_le_bytes(raw[16..24].try_into().expect("trace offset"));
        Ok(Self {
            op,
            layer_index,
            count,
            offset,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TraceHeader {
    data_size: u64,
    checksum: u32,
}

impl TraceHeader {
    fn encode(self) -> [u8; TRACE_HEADER_SIZE] {
        let mut out = [0u8; TRACE_HEADER_SIZE];
        out[0..4].copy_from_slice(&TRACE_MAGIC.to_le_bytes());
        out[8..16].copy_from_slice(&self.data_size.to_le_bytes());
        out[16..20].copy_from_slice(&self.checksum.to_le_bytes());
        out
    }

    fn decode(raw: &[u8]) -> Result<Self> {
        ensure!(raw.len() == TRACE_HEADER_SIZE, "invalid trace header size",);
        let magic = u32::from_le_bytes(raw[0..4].try_into().expect("trace magic"));
        ensure!(magic == TRACE_MAGIC, "prefetch trace magic mismatch");
        Ok(Self {
            data_size: u64::from_le_bytes(raw[8..16].try_into().expect("trace data size")),
            checksum: u32::from_le_bytes(raw[16..20].try_into().expect("trace checksum")),
        })
    }
}

struct PrefetcherInner {
    mode: PrefetchMode,
    trace_file_path: PathBuf,
    lock_file_path: PathBuf,
    ok_file_path: PathBuf,
    record_array: Mutex<Vec<TraceRecord>>,
    replay_queue: Mutex<VecDeque<TraceRecord>>,
    src_files: Mutex<HashMap<u32, Arc<dyn VirtualFile>>>,
    record_stopped: AtomicBool,
    replay_stopped: AtomicBool,
    dumped: AtomicBool,
    replay_started: AtomicBool,
    concurrency: usize,
}

impl fmt::Debug for PrefetcherInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefetcherInner")
            .field("mode", &self.mode)
            .field("trace_file_path", &self.trace_file_path)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

impl PrefetcherInner {
    fn new(trace_file_path: PathBuf, mode: PrefetchMode, concurrency: usize) -> Self {
        Self {
            mode,
            lock_file_path: PathBuf::from(format!("{}.lock", trace_file_path.display())),
            ok_file_path: PathBuf::from(format!("{}.ok", trace_file_path.display())),
            trace_file_path,
            record_array: Mutex::new(Vec::new()),
            replay_queue: Mutex::new(VecDeque::new()),
            src_files: Mutex::new(HashMap::new()),
            record_stopped: AtomicBool::new(false),
            replay_stopped: AtomicBool::new(false),
            dumped: AtomicBool::new(false),
            replay_started: AtomicBool::new(false),
            concurrency: concurrency.max(1),
        }
    }

    fn register_src_file(&self, layer_index: u32, src_file: Arc<dyn VirtualFile>) {
        self.src_files.lock().insert(layer_index, src_file);
    }

    fn record(&self, trace: TraceRecord) {
        if self.record_stopped.load(Ordering::SeqCst) {
            return;
        }
        self.record_array.lock().push(trace);
    }

    fn dump_records(&self) -> Result<()> {
        if self.mode != PrefetchMode::Record {
            return Ok(());
        }
        if self.dumped.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let records = self.record_array.lock().clone();
        let data_size = u64::try_from(records.len().saturating_mul(TRACE_RECORD_SIZE))
            .context("trace data size overflow")?;
        let mut checksum: u32 = 0;
        let mut encoded = Vec::with_capacity(records.len());
        for record in &records {
            let raw = record.encode();
            checksum = crc32c::crc32c_append(checksum, &raw);
            encoded.push(raw);
        }

        if self.ok_file_path.exists() {
            let _ = std::fs::remove_file(&self.ok_file_path);
        }

        let mut trace_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&self.trace_file_path)?;

        // FIXME: why not write header with checksum directly?
        let header = TraceHeader {
            data_size,
            checksum: 0,
        };
        trace_file.write_all(&header.encode())?;
        for record in &encoded {
            trace_file.write_all(record)?;
        }

        let header = TraceHeader {
            data_size,
            checksum,
        };
        trace_file.seek(SeekFrom::Start(0))?;
        trace_file.write_all(&header.encode())?;
        trace_file.flush()?;

        // FIXME: why remove lock_file_path?
        // start_detect_lock_thread only enter this function if lock_file_path does not exists.
        let _ = std::fs::remove_file(&self.lock_file_path);
        let _ = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.ok_file_path)?;
        Ok(())
    }

    fn load_trace_file(path: &Path) -> Result<VecDeque<TraceRecord>> {
        let raw = std::fs::read(path)?;
        ensure!(
            raw.len() >= TRACE_HEADER_SIZE,
            "prefetch trace file is smaller than header",
        );

        let header = TraceHeader::decode(&raw[..TRACE_HEADER_SIZE])?;
        let expected_len = usize::try_from(header.data_size)
            .context("trace data size overflow")?
            .saturating_add(TRACE_HEADER_SIZE);
        ensure!(
            raw.len() == expected_len,
            "prefetch trace file size mismatch",
        );
        ensure!(
            header.data_size % TRACE_RECORD_SIZE as u64 == 0,
            "prefetch trace payload is not record-aligned",
        );

        let mut checksum: u32 = 0;
        let mut queue = VecDeque::new();
        for chunk in raw[TRACE_HEADER_SIZE..].as_chunks::<TRACE_RECORD_SIZE>().0 {
            checksum = crc32c::crc32c_append(checksum, chunk);
            queue.push_back(TraceRecord::decode(chunk)?);
        }
        ensure!(
            checksum == header.checksum,
            "prefetch trace checksum mismatch",
        );
        Ok(queue)
    }
}

#[derive(Debug)]
pub struct Prefetcher {
    inner: Arc<PrefetcherInner>,
    detect_thread: Option<JoinHandle<()>>,
    replay_thread: Option<JoinHandle<()>>,
}

impl Prefetcher {
    pub fn detect_mode(trace_file_path: impl AsRef<Path>) -> PrefetchMode {
        match std::fs::metadata(trace_file_path.as_ref()) {
            Err(_) => PrefetchMode::Disabled,
            Ok(meta) if meta.len() == 0 => PrefetchMode::Record,
            Ok(_) => PrefetchMode::Replay,
        }
    }

    pub fn mode(&self) -> PrefetchMode {
        self.inner.mode
    }

    pub fn new_prefetch_file(
        &self,
        src_file: Arc<dyn VirtualFile>,
        layer_index: u32,
    ) -> Arc<dyn VirtualFile> {
        if self.inner.mode == PrefetchMode::Replay {
            self.inner.register_src_file(layer_index, src_file.clone());
        }
        Arc::new(PrefetchFile {
            src_file,
            layer_index,
            prefetcher: self.inner.clone(),
        })
    }

    pub fn replay(&mut self) -> Result<()> {
        if self.inner.mode != PrefetchMode::Replay {
            return Ok(());
        }
        if self.inner.replay_started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if self.inner.replay_queue.lock().is_empty() || self.inner.src_files.lock().is_empty() {
            return Ok(());
        }

        let inner = self.inner.clone();
        self.replay_thread = Some(
            thread::Builder::new()
                .name("overlaybd-prefetch-replay".to_string())
                .spawn(move || {
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(_) => return,
                    };

                    runtime.block_on(async move {
                        let mut workers = Vec::with_capacity(inner.concurrency);
                        for _ in 0..inner.concurrency {
                            let worker_inner = inner.clone();
                            workers.push(tokio::spawn(async move {
                                replay_worker(worker_inner).await;
                            }));
                        }

                        for worker in workers {
                            let _ = worker.await;
                        }
                    });
                })?,
        );
        Ok(())
    }
}

pub fn new_prefetcher(trace_file_path: impl Into<PathBuf>, concurrency: i32) -> Result<Prefetcher> {
    let trace_file_path = trace_file_path.into();
    let mode = Prefetcher::detect_mode(&trace_file_path);
    let inner = Arc::new(PrefetcherInner::new(
        trace_file_path.clone(),
        mode,
        usize::try_from(concurrency.max(1)).unwrap_or(1),
    ));

    match mode {
        PrefetchMode::Disabled => bail!(
            "prefetch trace `{}` does not exist",
            trace_file_path.display()
        ),
        PrefetchMode::Record => {
            let _ = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&trace_file_path)?;
            let _ = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&inner.lock_file_path)?;
            let detect_thread = start_detect_lock_thread(inner.clone())?;
            Ok(Prefetcher {
                inner,
                detect_thread: Some(detect_thread),
                replay_thread: None,
            })
        }
        PrefetchMode::Replay => {
            let replay_queue = PrefetcherInner::load_trace_file(&trace_file_path)
                .context("dynamic prefetch list is not migrated in Rust yet")?;
            *inner.replay_queue.lock() = replay_queue;
            Ok(Prefetcher {
                inner,
                detect_thread: None,
                replay_thread: None,
            })
        }
    }
}

fn start_detect_lock_thread(inner: Arc<PrefetcherInner>) -> Result<JoinHandle<()>> {
    Ok(thread::Builder::new()
        .name("overlaybd-prefetch-detect".to_string())
        .spawn(move || {
            while !inner.record_stopped.load(Ordering::SeqCst) {
                thread::sleep(POLL_INTERVAL);
                if inner.record_stopped.load(Ordering::SeqCst) {
                    break;
                }
                if !inner.lock_file_path.exists() {
                    inner.record_stopped.store(true, Ordering::SeqCst);
                    let _ = inner.dump_records();
                    break;
                }
            }
        })?)
}

async fn replay_worker(inner: Arc<PrefetcherInner>) {
    loop {
        if inner.replay_stopped.load(Ordering::SeqCst) {
            return;
        }

        let Some(trace) = inner.replay_queue.lock().pop_front() else {
            return;
        };
        let Some(src_file) = inner.src_files.lock().get(&trace.layer_index).cloned() else {
            continue;
        };
        if trace.op != TraceOp::Read {
            continue;
        }

        let Ok(offset) = u64::try_from(trace.offset) else {
            continue;
        };
        let Ok(count) = usize::try_from(trace.count) else {
            continue;
        };
        let _ = src_file.read_at(offset, count).await;
    }
}

impl Drop for Prefetcher {
    fn drop(&mut self) {
        self.inner.record_stopped.store(true, Ordering::SeqCst);
        self.inner.replay_stopped.store(true, Ordering::SeqCst);

        if let Some(handle) = self.detect_thread.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.replay_thread.take() {
            let _ = handle.join();
        }
        let _ = self.inner.dump_records();
    }
}

struct PrefetchFile {
    src_file: Arc<dyn VirtualFile>,
    layer_index: u32,
    prefetcher: Arc<PrefetcherInner>,
}

impl fmt::Debug for PrefetchFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefetchFile")
            .field("layer_index", &self.layer_index)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl VirtualFile for PrefetchFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let data = self.src_file.read_at(offset, len).await?;
        if data.len() == len && self.prefetcher.mode == PrefetchMode::Record {
            if let Ok(trace) = TraceRecord::new_read(self.layer_index, len, offset) {
                self.prefetcher.record(trace);
            }
        }
        Ok(data)
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let n = self.src_file.read_at_into(offset, dst).await?;
        if n == dst.len() && self.prefetcher.mode == PrefetchMode::Record {
            if let Ok(trace) = TraceRecord::new_read(self.layer_index, n, offset) {
                self.prefetcher.record(trace);
            }
        }
        Ok(n)
    }

    async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.src_file.write_at(offset, data).await
    }

    async fn size(&self) -> Result<u64> {
        self.src_file.size().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.src_file.truncate(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.src_file.sync().await
    }

    async fn seek_data(&self, offset: u64) -> Result<Option<u64>> {
        self.src_file.seek_data(offset).await
    }

    async fn seek_hole(&self, offset: u64) -> Result<Option<u64>> {
        self.src_file.seek_hole(offset).await
    }

    async fn evict_range(&self, offset: u64, len: u64) -> Result<()> {
        self.src_file.evict_range(offset, len).await
    }

    async fn evict_all(&self) -> Result<()> {
        self.src_file.evict_all().await
    }

    async fn fgetxattr(&self, name: &str) -> Result<Vec<u8>> {
        self.src_file.fgetxattr(name).await
    }

    async fn flistxattr(&self) -> Result<Vec<String>> {
        self.src_file.flistxattr().await
    }

    async fn fsetxattr(&self, name: &str, value: &[u8], flags: i32) -> Result<()> {
        self.src_file.fsetxattr(name, value, flags).await
    }

    async fn fremovexattr(&self, name: &str) -> Result<()> {
        self.src_file.fremovexattr(name).await
    }

    #[cfg(feature = "io-uring")]
    fn read_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        len: usize,
    ) -> LocalBoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let data = self.src_file.read_at_with_ctx(ctx, offset, len).await?;
            if data.len() == len && self.prefetcher.mode == PrefetchMode::Record {
                if let Ok(trace) = TraceRecord::new_read(self.layer_index, len, offset) {
                    self.prefetcher.record(trace);
                }
            }
            Ok(data)
        })
    }

    #[cfg(feature = "io-uring")]
    fn read_at_into_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        dst: &'a mut [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            let dst_len = dst.len();
            let n = self
                .src_file
                .read_at_into_with_ctx(ctx, offset, dst)
                .await?;
            if n == dst_len && self.prefetcher.mode == PrefetchMode::Record {
                if let Ok(trace) = TraceRecord::new_read(self.layer_index, n, offset) {
                    self.prefetcher.record(trace);
                }
            }
            Ok(n)
        })
    }

    #[cfg(feature = "io-uring")]
    fn write_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: &'a [u8],
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(self.src_file.write_at_with_ctx(ctx, offset, data))
    }

    #[cfg(feature = "io-uring")]
    fn write_bytes_at_with_ctx<'a>(
        &'a self,
        ctx: IoCtx<'a>,
        offset: u64,
        data: Bytes,
    ) -> LocalBoxFuture<'a, Result<usize>> {
        Box::pin(self.src_file.write_bytes_at_with_ctx(ctx, offset, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use tokio::time::{sleep, Duration};

    #[derive(Debug)]
    struct MemoryFile {
        data: Vec<u8>,
        hits: Arc<AtomicUsize>,
        writes: Mutex<Vec<(u64, Vec<u8>)>>,
    }

    impl MemoryFile {
        fn new(data: Vec<u8>, hits: Arc<AtomicUsize>) -> Self {
            Self {
                data,
                hits,
                writes: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl VirtualFile for MemoryFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
            self.hits.fetch_add(1, Ordering::Relaxed);
            if len == 0 || offset >= self.data.len() as u64 {
                return Ok(Bytes::new());
            }
            let end = offset
                .saturating_add(len as u64)
                .min(self.data.len() as u64) as usize;
            Ok(Bytes::copy_from_slice(&self.data[offset as usize..end]))
        }

        async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
            self.writes.lock().await.push((offset, data.to_vec()));
            Ok(data.len())
        }

        async fn size(&self) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    #[tokio::test]
    async fn test_prefetch_record_and_replay_trace() {
        let tmp = TempDir::new().expect("tempdir");
        let trace_path = tmp.path().join("trace");
        std::fs::write(&trace_path, []).expect("create empty trace");

        let record_hits = Arc::new(AtomicUsize::new(0));
        let src_file: Arc<dyn VirtualFile> =
            Arc::new(MemoryFile::new(vec![0x5a; 4096], record_hits.clone()));

        let prefetch = new_prefetcher(&trace_path, 2).expect("record prefetcher");
        assert_eq!(prefetch.mode(), PrefetchMode::Record);
        let wrapped = prefetch.new_prefetch_file(src_file, 3);
        let got = wrapped.read_at(256, 1024).await.expect("recorded read");
        assert_eq!(got.len(), 1024);
        drop(wrapped);
        drop(prefetch);

        let raw = std::fs::read(&trace_path).expect("read trace file");
        assert_eq!(record_hits.load(Ordering::Relaxed), 1);
        assert_eq!(raw.len(), TRACE_HEADER_SIZE + TRACE_RECORD_SIZE);
        assert!(
            trace_path.with_extension("trace.ok").exists()
                || PathBuf::from(format!("{}.ok", trace_path.display())).exists()
        );

        let replay_hits = Arc::new(AtomicUsize::new(0));
        let replay_src: Arc<dyn VirtualFile> =
            Arc::new(MemoryFile::new(vec![0x5a; 4096], replay_hits.clone()));
        let mut replay = new_prefetcher(&trace_path, 2).expect("replay prefetcher");
        assert_eq!(replay.mode(), PrefetchMode::Replay);
        let _wrapped = replay.new_prefetch_file(replay_src, 3);
        replay.replay().expect("spawn replay");
        sleep(Duration::from_millis(100)).await;
        drop(replay);

        assert!(replay_hits.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_prefetch_rejects_dynamic_prefetch_list() {
        let tmp = TempDir::new().expect("tempdir");
        let trace_path = tmp.path().join("prefetch.list");
        std::fs::write(&trace_path, b"/bin/sh\n").expect("write list");

        let err = new_prefetcher(&trace_path, 1).expect_err("dynamic prefetch should fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("dynamic prefetch list is not migrated in Rust yet"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn test_prefetch_read_at_into_forwards_and_records() {
        let tmp = TempDir::new().expect("tempdir");
        let trace_path = tmp.path().join("trace_into");
        std::fs::write(&trace_path, []).expect("create empty trace");

        let hits = Arc::new(AtomicUsize::new(0));
        let src_data = vec![0x42; 8192];
        let src_file: Arc<dyn VirtualFile> =
            Arc::new(MemoryFile::new(src_data.clone(), hits.clone()));

        let prefetch = new_prefetcher(&trace_path, 2).expect("record prefetcher");
        let wrapped = prefetch.new_prefetch_file(src_file, 5);

        // Test read_at_into returns the same data as read_at
        let bytes_result = wrapped.read_at(128, 2048).await.expect("read_at");
        let mut into_buf = vec![0u8; 2048];
        let n = wrapped
            .read_at_into(128, &mut into_buf)
            .await
            .expect("read_at_into");
        assert_eq!(n, bytes_result.len());
        assert_eq!(&into_buf[..n], &bytes_result[..]);

        // Both calls should have been recorded (2 hits from the underlying file)
        assert!(hits.load(Ordering::Relaxed) >= 2);
    }
}
