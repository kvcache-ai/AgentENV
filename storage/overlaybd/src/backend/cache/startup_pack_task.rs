//! Startup manifest prefetch task: downloads one v3 startup manifest (a tiny
//! page list), translates its pages into final-object cache blocks with the
//! same planner the publish side uses (tar unwrap, zfile jump-table extents,
//! merged LSMT index — the raw container bytes are not LSMT), and refills
//! those blocks concurrently through the same batched source-read path
//! background downloads use (few large requests instead of per-block RTTs).
//!
//! The task runs inside the cache backend's download scheduler so manifest
//! downloads, on-demand refills and regular background downloads share the
//! same admission gate, block slots and cache entries. All reads fill the
//! same cache the on-demand path uses; loader election dedups with guest
//! faults.

use anyhow::{ensure, Context, Result};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use super::full_file_cache::cache_store::CachedFile;

/// Concurrent workers for one manifest prefetch.
const MANIFEST_WORKERS: usize = 8;

/// One import target of a startup manifest, bound by the caller through the
/// normal image-service cache entry point (same cache-key transform the
/// on-demand path uses).
pub struct StartupPackLayer {
    pub digest: String,
    pub size: u64,
    pub file: Arc<CachedFile>,
    /// The raw source behind `file` (used by manifest-range batched refills).
    pub source: Arc<dyn crate::virtual_file::VirtualFile>,
}

/// Everything needed to register one startup manifest prefetch.
pub struct StartupPackSubmission {
    /// Dedup identity, bound to the immutable manifest description (its
    /// sha256). Concurrent resumes of the same snapshot share one task.
    pub task_key: String,
    /// The manifest object itself, opened on the shared OSS backend WITHOUT
    /// the full-file cache (it is consumed once; only layer blocks belong in
    /// the cache). The size hint avoids a stat probe.
    pub pack_source: Arc<dyn crate::virtual_file::VirtualFile>,
    pub pack_size: u64,
    /// Hex sha256 of the whole manifest body, from the committed record; the
    /// manifest is trusted only after the body matches this digest.
    pub index_sha256: String,
    pub mem_virtual_size: u64,
    /// Import targets in the memory image's bottom-to-top layer order.
    pub layers: Vec<StartupPackLayer>,
    /// Hard bound on queueing plus downloading: a manifest that has not
    /// completed by this point fails, so it can never hold scheduler
    /// capacity forever.
    pub timeout: Duration,
}

/// Terminal phase of a startup prefetch task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupPackPhase {
    Queued,
    Running,
    Done,
    Failed,
    Canceled,
}

/// Per-task accounting, kept atomically so handles can snapshot it anytime.
#[derive(Default)]
pub(crate) struct StartupPackStats {
    /// Source range reads issued, including retried attempts.
    pub oss_gets: AtomicU64,
    /// Bytes received from the source, including retried attempts.
    pub oss_bytes: AtomicU64,
}

impl StartupPackStats {
    pub(crate) fn snapshot(&self) -> StartupPackStatsSnapshot {
        StartupPackStatsSnapshot {
            oss_gets: self.oss_gets.load(Ordering::Relaxed),
            oss_bytes: self.oss_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StartupPackStatsSnapshot {
    pub oss_gets: u64,
    pub oss_bytes: u64,
}

pub(crate) struct StartupPackTask {
    pub task_key: String,
    pub pack_source: Arc<dyn crate::virtual_file::VirtualFile>,
    pub pack_size: u64,
    pub index_sha256: String,
    pub mem_virtual_size: u64,
    pub layers: Vec<StartupPackLayer>,
    pub(crate) stats: StartupPackStats,
    phase: StdMutex<StartupPackPhase>,
    pub(crate) phase_notify: tokio::sync::Notify,
    /// Open handle count; the last release of a non-terminal task cancels it.
    holders: AtomicUsize,
    canceled: AtomicBool,
    deadline: tokio::time::Instant,
}

impl StartupPackTask {
    pub(crate) fn new(submission: StartupPackSubmission) -> Arc<Self> {
        Arc::new(Self {
            task_key: submission.task_key,
            pack_source: submission.pack_source,
            pack_size: submission.pack_size,
            index_sha256: submission.index_sha256,
            mem_virtual_size: submission.mem_virtual_size,
            layers: submission.layers,
            stats: StartupPackStats::default(),
            phase: StdMutex::new(StartupPackPhase::Queued),
            phase_notify: tokio::sync::Notify::new(),
            holders: AtomicUsize::new(1),
            canceled: AtomicBool::new(false),
            deadline: tokio::time::Instant::now() + submission.timeout,
        })
    }

    pub(crate) fn phase(&self) -> StartupPackPhase {
        *self.phase.lock().expect("phase lock poisoned")
    }

    fn set_phase(&self, phase: StartupPackPhase) {
        *self.phase.lock().expect("phase lock poisoned") = phase;
        self.phase_notify.notify_waiters();
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self.phase(),
            StartupPackPhase::Done | StartupPackPhase::Failed | StartupPackPhase::Canceled
        )
    }

    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }

    /// Mark the task failed when its queue+download deadline has passed.
    /// Returns true when the task is (now) terminal and must not run.
    pub(crate) fn fail_if_past_deadline(&self) -> bool {
        if self.is_terminal() {
            return true;
        }
        if tokio::time::Instant::now() >= self.deadline {
            self.set_phase(StartupPackPhase::Failed);
            return true;
        }
        false
    }

    /// Add one holder (a shared device attaching to the same manifest).
    pub(crate) fn retain(&self) {
        self.holders.fetch_add(1, Ordering::AcqRel);
    }

    /// Drop one holder. Returns true when this was the last holder and the
    /// task was not terminal (the caller cancels and deregisters it).
    pub(crate) fn release(&self) -> bool {
        self.holders.fetch_sub(1, Ordering::AcqRel) == 1 && !self.is_terminal()
    }

    pub(crate) fn cancel(&self) {
        self.canceled.store(true, Ordering::Release);
    }
}

/// Execute one startup prefetch task to completion (or failure/cancel). The
/// scheduler owns dispatch; this function owns the task lifecycle.
pub(crate) async fn run_startup_pack_task(
    task: &Arc<StartupPackTask>,
    scheduler: &super::bk_download::BkDownloadScheduler,
) {
    let outcome = execute(task, scheduler).await;
    let stats = task.stats.snapshot();
    match outcome {
        Ok(()) => {
            task.set_phase(StartupPackPhase::Done);
            tracing::info!(
                task_key = %task.task_key,
                oss_gets = stats.oss_gets,
                oss_bytes = stats.oss_bytes,
                "startup pack prefetch done"
            );
        }
        Err(error) if task.is_canceled() => {
            task.set_phase(StartupPackPhase::Canceled);
            tracing::debug!(task_key = %task.task_key, %error, "startup pack prefetch canceled");
        }
        Err(error) => {
            task.set_phase(StartupPackPhase::Failed);
            tracing::warn!(task_key = %task.task_key, error = ?error, "startup pack prefetch failed");
        }
    }
}

async fn execute(
    task: &Arc<StartupPackTask>,
    scheduler: &super::bk_download::BkDownloadScheduler,
) -> Result<()> {
    task.set_phase(StartupPackPhase::Running);

    // Probe the magic first: legacy (v1/v2) or corrupt startup objects are
    // rejected after a tiny read instead of a full download.
    let probe_len = (task.pack_size as usize).min(64);
    let probe = task
        .pack_source
        .read_at(0, probe_len)
        .await
        .context("read manifest magic probe")?;
    task.stats.oss_gets.fetch_add(1, Ordering::Relaxed);
    task.stats
        .oss_bytes
        .fetch_add(probe.len() as u64, Ordering::Relaxed);
    ensure!(
        probe.len() >= crate::startup_manifest::MANIFEST_MAGIC.len()
            && probe[..crate::startup_manifest::MANIFEST_MAGIC.len()]
                == crate::startup_manifest::MANIFEST_MAGIC,
        "unsupported startup pack format (not a startup manifest)"
    );

    // The manifest itself is tiny: one more read for the rest of the body.
    let rest = task
        .pack_source
        .read_at(probe_len as u64, task.pack_size as usize - probe_len)
        .await
        .context("read manifest body")?;
    task.stats.oss_gets.fetch_add(1, Ordering::Relaxed);
    task.stats
        .oss_bytes
        .fetch_add(rest.len() as u64, Ordering::Relaxed);
    ensure!(
        probe_len + rest.len() == task.pack_size as usize,
        "short manifest: expect {}, got {}",
        task.pack_size,
        probe_len + rest.len()
    );
    let mut manifest_bytes = probe.to_vec();
    manifest_bytes.extend_from_slice(&rest);
    execute_manifest_prefetch(task, scheduler, &manifest_bytes).await
}

/// Plan the manifest's pages into final-object blocks and refill them
/// concurrently through the batched source-read path.
async fn execute_manifest_prefetch(
    task: &Arc<StartupPackTask>,
    scheduler: &super::bk_download::BkDownloadScheduler,
    manifest_bytes: &[u8],
) -> Result<()> {
    use crate::pack_planner::{plan_ranges, FinalLayerBytes, FinalLayerSource};
    use crate::startup_manifest::decode_manifest;

    ensure!(
        !task.layers.is_empty(),
        "manifest prefetch without bound layers"
    );
    // Trust gate: the committed record's sha256 must match the body before
    // any entry is acted on.
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(manifest_bytes);
    let mut body_sha256 = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(body_sha256, "{byte:02x}");
    }
    ensure!(
        body_sha256 == task.index_sha256,
        "manifest sha256 mismatch with the committed descriptor"
    );

    let manifest = decode_manifest(manifest_bytes).context("decode manifest")?;
    ensure!(
        manifest.mem_virtual_size == task.mem_virtual_size,
        "manifest mem size mismatch: descriptor {} vs header {}",
        task.mem_virtual_size,
        manifest.mem_virtual_size
    );

    // Translate in first-need order — one merged-index lookup per span
    // instead of one per page. The manifest's table order IS the first-need
    // order: prefix pages first, then the rank-sorted ranges.
    let mut spans: Vec<(u64, u64)> =
        Vec::with_capacity(manifest.prefix_pages.len() + manifest.ranges.len());
    for &offset in &manifest.prefix_pages {
        spans.push((offset, 4096));
    }
    spans.extend_from_slice(&manifest.ranges);

    let layers: Vec<FinalLayerSource> = task
        .layers
        .iter()
        .map(|layer| FinalLayerSource {
            digest: layer.digest.clone(),
            size: layer.size,
            source: FinalLayerBytes::VFile(
                layer.file.clone() as Arc<dyn crate::virtual_file::VirtualFile>
            ),
        })
        .collect();
    let plan = plan_ranges(&spans, manifest.mem_virtual_size, &layers, u64::MAX)
        .await
        .context("plan manifest blocks")?;

    // Merge the first-need block plan into per-layer adjacent runs (capped)
    // so one source read covers a contiguous span; runs are delivered sorted
    // by each run's earliest first-need rank so boot-critical blocks (the
    // manifest's exact-order prefix) land first instead of in arbitrary
    // physical order.
    let work = Arc::new(merge_runs_by_first_need(&plan));

    let running = scheduler.running().clone();
    let cursor = Arc::new(AtomicUsize::new(0));
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..MANIFEST_WORKERS.min(work.len().max(1)) {
        let task = Arc::clone(task);
        let work = Arc::clone(&work);
        let cursor = Arc::clone(&cursor);
        let running = running.clone();
        let block_slots = scheduler.block_slots().clone();
        workers.spawn(async move {
            loop {
                if !running.load(Ordering::Acquire) {
                    anyhow::bail!("scheduler shutting down");
                }
                if task.is_canceled() {
                    anyhow::bail!("manifest prefetch canceled");
                }
                if tokio::time::Instant::now() >= task.deadline {
                    anyhow::bail!("manifest prefetch timed out");
                }
                let index = cursor.fetch_add(1, Ordering::Relaxed);
                let Some((layer_idx, start_block, len_blocks)) = work.get(index).copied() else {
                    return Ok(());
                };
                let layer = &task.layers[layer_idx];
                let _slot = block_slots
                    .acquire()
                    .await
                    .context("manifest prefetch block slots closed")?;
                let Some(_gate) = crate::download_gate::gate_startup_block_read(&running).await
                else {
                    anyhow::bail!("manifest prefetch canceled before chunk dispatch");
                };
                let bytes = layer
                    .file
                    .background_refill_range(&layer.source, layer.size, start_block, len_blocks)
                    .await?;
                task.stats.oss_gets.fetch_add(1, Ordering::Relaxed);
                task.stats.oss_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        });
    }
    let mut first_error = None;
    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(error) => {
                first_error.get_or_insert(anyhow::anyhow!(error));
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Merge the first-need-ordered block plan into per-layer adjacent runs
/// (capped at 64 blocks = 16 MiB) so one source read covers a contiguous
/// span. Runs are returned sorted by each run's earliest first-need rank:
/// same runs and request count as physical order, but boot-critical blocks
/// are delivered first.
fn merge_runs_by_first_need(plan: &crate::pack_planner::PackPlan) -> Vec<(usize, u64, u32)> {
    const RUN_BLOCKS: u32 = 64;
    let mut items: Vec<(usize, u64, u32, u64)> = plan
        .blocks
        .iter()
        .enumerate()
        .map(|(rank, block)| {
            (
                plan.objects[block.object as usize].layer,
                u64::from(block.block_id),
                1u32,
                rank as u64,
            )
        })
        .collect();
    items.sort_unstable_by_key(|(layer, block, _, _)| (*layer, *block));
    let mut runs: Vec<(usize, u64, u32, u64)> = Vec::new();
    for (layer_idx, block, len, rank) in items {
        match runs.last_mut() {
            Some((l, b, n, r))
                if *l == layer_idx && *b + u64::from(*n) == block && *n + len <= RUN_BLOCKS =>
            {
                *n += len;
                *r = (*r).min(rank);
            }
            _ => runs.push((layer_idx, block, len, rank)),
        }
    }
    runs.sort_by_key(|(_, _, _, rank)| *rank);
    runs.into_iter().map(|(l, b, n, _)| (l, b, n)).collect()
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_file::VirtualFile;
    use anyhow::bail;

    use crate::pack_planner::OBJECT_BLOCK_BYTES;

    async fn make_lsmt_layer(
        dir: &tempfile::TempDir,
        name: &str,
        pages: &[(u32, u8)],
    ) -> std::path::PathBuf {
        use crate::lsmt::file::{CommitArgs, LSMTFile};
        let data = Arc::new(
            crate::backend::local::LocalFile::new(dir.path().join(format!("{name}.data"))).unwrap(),
        );
        let index = Arc::new(
            crate::backend::local::LocalFile::new(dir.path().join(format!("{name}.index")))
                .unwrap(),
        );
        let lsmt = LSMTFile::create(data, Some(index), 1 << 30, false)
            .await
            .unwrap();
        for (page, fill) in pages {
            let buf = vec![*fill; 4096];
            lsmt.write_at(u64::from(*page) * 4096, &buf).await.unwrap();
        }
        let path = dir.path().join(name);
        let dest: Arc<dyn crate::virtual_file::VirtualFile> =
            Arc::new(crate::backend::local::LocalFile::new(&path).unwrap());
        lsmt.commit_with_args(CommitArgs::new(dest.clone()))
            .await
            .unwrap();
        path
    }

    #[test]
    fn merged_runs_are_delivered_by_earliest_first_need_rank() {
        use crate::pack_planner::{PackPlan, PlanStats, PlannedBlock, PlannedObject};
        let mk = |block_id: u32| PlannedBlock {
            object: 0,
            block_id,
            metadata: false,
        };
        // First-need order: block 5, block 1, block 2, block 6. Merging
        // physically adjacent blocks yields run(1,len2) and run(5,len2);
        // delivery must put the run containing the earliest page (5) first.
        let plan = PackPlan {
            objects: vec![PlannedObject {
                digest: "sha256:x".to_string(),
                size: 1 << 20,
                layer: 0,
            }],
            blocks: vec![mk(5), mk(1), mk(2), mk(6)],
            stats: PlanStats::default(),
        };
        let work = merge_runs_by_first_need(&plan);
        assert_eq!(work, vec![(0, 5, 2), (0, 1, 2)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manifest_prefetch_reads_listed_positions() {
        let dir = tempfile::tempdir().unwrap();
        // 200 pages (physical blocks 0-3; block 1 is a middle block the
        // metadata open never touches) with page-granular fills
        let pages: Vec<(u32, u8)> = (0..200u32).map(|page| (page, page as u8)).collect();
        let layer_path = make_lsmt_layer(&dir, "mem", &pages).await;
        let layer_bytes = std::fs::read(&layer_path).unwrap();
        let layer_size = layer_bytes.len() as u64;

        let backend = crate::backend::cache::FileCacheBackend::with_options(
            crate::backend::cache::FileCacheBackendOptions {
                cache_dir: dir.path().join("task-cache"),
                capacity_bytes: 64 << 20,
                block_size: OBJECT_BLOCK_BYTES,
                ..Default::default()
            },
        )
        .await
        .expect("backend");
        let layer_source = Arc::new(CountingVecFile::new(layer_bytes));
        let layer_file = backend
            .open_file_with_source_size("manifest-layer", layer_source.clone(), layer_size)
            .await
            .expect("open layer");

        // manifest: prefix pages [0, 1] (block 0), ranges [page 99,
        // page 150] (middle block 1 and tail-side block 2)
        let manifest = crate::startup_manifest::StartupManifest {
            mem_virtual_size: 1 << 30,
            prefix_pages: vec![0, 4096],
            ranges: vec![(99 * 4096, 4096), (150 * 4096, 4096)],
        };
        let manifest_bytes = crate::startup_manifest::encode_manifest(&manifest).unwrap();
        let index_sha256 = {
            use sha2::Digest as _;
            let digest = sha2::Sha256::digest(&manifest_bytes);
            let mut out = String::with_capacity(64);
            for byte in digest {
                use std::fmt::Write as _;
                let _ = write!(out, "{byte:02x}");
            }
            out
        };
        let submission = StartupPackSubmission {
            task_key: "manifest-1".to_string(),
            pack_source: Arc::new(VecFile {
                data: manifest_bytes.clone(),
            }),
            pack_size: manifest_bytes.len() as u64,
            index_sha256,
            mem_virtual_size: 1 << 30,
            layers: vec![StartupPackLayer {
                digest: "sha256:mem".to_string(),
                size: layer_size,
                file: layer_file.clone(),
                source: layer_source.clone(),
            }],
            timeout: Duration::from_secs(30),
        };
        let handle = backend.submit_startup_pack(submission).expect("submit");
        assert_eq!(handle.wait_terminal().await, StartupPackPhase::Done);
        let stats = handle.stats().expect("stats");
        assert!(
            stats.oss_bytes > manifest_bytes.len() as u64,
            "prefetch must read the listed blocks from source"
        );

        // The listed positions now read back with correct content, all
        // served from the prefetched cache.
        let files: Vec<Arc<dyn crate::virtual_file::VirtualFile>> = vec![layer_file];
        let merged = crate::lsmt::file::open_files_ro(&files).await.unwrap();
        let before_verify = layer_source.read_calls();
        for (page, fill) in [(0u32, 0u8), (1u32, 1u8), (99u32, 99u8), (150u32, 150u8)] {
            let data = merged
                .read_at(u64::from(page) * 4096, 4096)
                .await
                .expect("read page");
            assert_eq!(data.as_ref(), &vec![fill; 4096][..]);
        }
        let verify_reads = layer_source.read_calls() - before_verify;
        assert_eq!(
            verify_reads, 0,
            "all listed pages must be served from the prefetched cache"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manifest_prefetch_consumes_rank_ordered_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let pages: Vec<(u32, u8)> = (0..200u32).map(|page| (page, page as u8)).collect();
        let layer_path = make_lsmt_layer(&dir, "mem", &pages).await;
        let layer_bytes = std::fs::read(&layer_path).unwrap();
        let layer_size = layer_bytes.len() as u64;

        let backend = crate::backend::cache::FileCacheBackend::with_options(
            crate::backend::cache::FileCacheBackendOptions {
                cache_dir: dir.path().join("task-cache"),
                capacity_bytes: 64 << 20,
                block_size: OBJECT_BLOCK_BYTES,
                ..Default::default()
            },
        )
        .await
        .expect("backend");
        let layer_source = Arc::new(CountingVecFile::new(layer_bytes));
        let layer_file = backend
            .open_file_with_source_size("manifest-layer", layer_source.clone(), layer_size)
            .await
            .expect("open layer");

        // No prefix; ranges in first-touch-rank order (not address order).
        let manifest = crate::startup_manifest::StartupManifest {
            mem_virtual_size: 1 << 30,
            prefix_pages: vec![],
            ranges: vec![(150 * 4096, 4096), (0, 8192), (99 * 4096, 4096)],
        };
        let manifest_bytes = crate::startup_manifest::encode_manifest(&manifest).unwrap();
        let index_sha256 = {
            use sha2::Digest as _;
            let digest = sha2::Sha256::digest(&manifest_bytes);
            let mut out = String::with_capacity(64);
            for byte in digest {
                use std::fmt::Write as _;
                let _ = write!(out, "{byte:02x}");
            }
            out
        };
        let submission = StartupPackSubmission {
            task_key: "manifest-v4".to_string(),
            pack_source: Arc::new(VecFile {
                data: manifest_bytes.clone(),
            }),
            pack_size: manifest_bytes.len() as u64,
            index_sha256,
            mem_virtual_size: 1 << 30,
            layers: vec![StartupPackLayer {
                digest: "sha256:mem".to_string(),
                size: layer_size,
                file: layer_file.clone(),
                source: layer_source.clone(),
            }],
            timeout: Duration::from_secs(30),
        };
        let handle = backend.submit_startup_pack(submission).expect("submit");
        assert_eq!(handle.wait_terminal().await, StartupPackPhase::Done);

        let files: Vec<Arc<dyn crate::virtual_file::VirtualFile>> = vec![layer_file];
        let merged = crate::lsmt::file::open_files_ro(&files).await.unwrap();
        let before_verify = layer_source.read_calls();
        for (page, fill) in [(0u32, 0u8), (1u32, 1u8), (99u32, 99u8), (150u32, 150u8)] {
            let data = merged
                .read_at(u64::from(page) * 4096, 4096)
                .await
                .expect("read page");
            assert_eq!(data.as_ref(), &vec![fill; 4096][..]);
        }
        assert_eq!(
            layer_source.read_calls() - before_verify,
            0,
            "v4-listed pages must be served from the prefetched cache"
        );
    }

    /// VecFile with a read counter.
    struct CountingVecFile {
        data: Vec<u8>,
        reads: AtomicUsize,
    }

    impl CountingVecFile {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data,
                reads: AtomicUsize::new(0),
            }
        }

        fn read_calls(&self) -> usize {
            self.reads.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl crate::virtual_file::VirtualFile for CountingVecFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<bytes::Bytes> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let size = self.data.len() as u64;
            if offset >= size || len == 0 {
                return Ok(bytes::Bytes::new());
            }
            let end = (offset + len as u64).min(size);
            Ok(bytes::Bytes::copy_from_slice(
                &self.data[offset as usize..end as usize],
            ))
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            bail!("counting vec file is read-only")
        }

        async fn size(&self) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    /// Minimal in-memory VirtualFile for pack/layer sources.
    struct VecFile {
        data: Vec<u8>,
    }

    #[async_trait::async_trait]
    impl crate::virtual_file::VirtualFile for VecFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<bytes::Bytes> {
            let size = self.data.len() as u64;
            if offset >= size || len == 0 {
                return Ok(bytes::Bytes::new());
            }
            let end = (offset + len as u64).min(size);
            Ok(bytes::Bytes::copy_from_slice(
                &self.data[offset as usize..end as usize],
            ))
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            bail!("vec file is read-only")
        }

        async fn size(&self) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }
}
