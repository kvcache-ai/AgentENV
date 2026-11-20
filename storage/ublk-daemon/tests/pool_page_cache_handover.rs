//! Regression test for <https://github.com/kvcache-ai/AgentENV/issues/302>:
//! a pooled ublk device can serve stale placeholder pages after being rebound
//! to a nonzero business image.
//!
//! The host block-device page cache is keyed by the device's inode, not by the
//! ublk target binding. When a released device idles on the all-zero pool
//! placeholder, any buffered read populates the cache with zero pages.
//! Reacquiring swaps the target back to the business image, but nothing
//! invalidates those cached pages, so a buffered reader keeps seeing zeros
//! while `O_DIRECT` reads return the correct content.
//!
//! This test reduces the original guest-memory-corruption failure to a
//! single-device cache experiment (no Firecracker/envd):
//!
//!   1. acquire a shared device for a read-only image with a known nonzero
//!      marker page, and record the page via `O_DIRECT`;
//!   2. release the device (it swaps to the placeholder and stays pooled);
//!   3. through a retained buffered FD, verify the placeholder reads as zeros
//!      and deliberately fill the placeholder page cache;
//!   4. reacquire the same image (same dev_id) and compare buffered vs
//!      `O_DIRECT` reads at the marker offset;
//!   5. as a control, verify `BLKFLSBUF` after the switch restores the
//!      buffered path, then clean up before the final regression assertion.
//!
//! Requires Linux with ublk (`make test-ublk` supplies CAP_SYS_ADMIN through
//! scripts/run-with-capabilities.sh). Ignored by default: the final buffered
//! assertion fails until the image-switch handover invalidates stale cache
//! pages; un-ignore together with the fix for issue #302.

use std::alloc::{dealloc, Layout};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::GlobalConfig;
use overlaybd::index_file::{create_file_rw, LayerInfo};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::ImageService;
use storage_util::io_ring::spawn_io_ring_worker;
use tempfile::TempDir;
use tokio::net::UnixStream;

use uvm_ublk_daemon::protocol::{recv_message, send_message};
use uvm_ublk_daemon::{AccessMode, DaemonRequest, DaemonResponse, PoolConfig, UblkDaemonServer};

const VIRTUAL_SIZE: u64 = 2 * 1024 * 1024 * 1024;
/// Same marker offset as the issue's reduced experiment (PUD index 0 of the
/// faulting guest page-table walk). Aligned to both 4 KiB and 64 KiB pages.
const MARKER_OFFSET: u64 = 0x7ffce000;
/// Length of the marker content written into the image; independent of the
/// host page size used for read buffers.
const MARKER_LEN: usize = 4096;
/// BLKFLSBUF ioctl request: Linux asm-generic/ioctl.h _IO(0x12, 97).
const BLKFLSBUF: u32 = 0x1261;
/// Deadline for any single RPC or blocking device operation: a stalled daemon
/// or wedged ublk read must fail the test, never hang it.
const IO_TIMEOUT: Duration = Duration::from_secs(30);
/// Mirrors the daemon protocol's message size cap.
const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

/// Host page size, used for `O_DIRECT` buffer alignment and read sizing.
/// Some kernels (e.g. arm64 builds) use 64 KiB pages instead of 4 KiB.
fn host_page_size() -> usize {
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    assert!(size > 0, "sysconf(_SC_PAGESIZE) failed");
    size as usize
}

/// Build a sealed LSMT lower of `virtual_size` bytes whose only content is a
/// known nonzero page at `MARKER_OFFSET`, plus the read-only image config
/// (file lower, empty `repoBlobUrl`, no upper) referencing it.
async fn create_marker_image(dir: &Path) -> Result<PathBuf> {
    let lower_data = dir.join("lower.data");
    let lower_index = dir.join("lower.index");
    let data_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&lower_data)?);
    let index_file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&lower_index)?);
    let info = LayerInfo::new(data_file, Some(index_file), VIRTUAL_SIZE);
    let lsmt = create_file_rw(info).await.context("create rw layer")?;

    let mut page = vec![0u8; MARKER_LEN];
    page[..8].copy_from_slice(&MARKER_OFFSET.to_le_bytes());
    for (i, byte) in page.iter_mut().enumerate().skip(8) {
        *byte = ((i * 13 + 7) % 251) as u8;
    }
    lsmt.write_at(MARKER_OFFSET, &page)
        .await
        .context("write marker page")?;
    lsmt.close_seal().await.context("seal lower")?;

    let image_config = dir.join("image.json");
    let json = serde_json::json!({
        "repoBlobUrl": "",
        "lowers": [{ "file": lower_data }],
        "upper": {},
        "resultFile": "",
    });
    std::fs::write(&image_config, serde_json::to_vec_pretty(&json)?)
        .context("write image config")?;
    Ok(image_config)
}

/// Write the daemon's overlaybd global config with `cache_dir` as the file
/// cache, returning its path. `cache_type` must be set explicitly: when it is
/// empty, config normalization falls back to the legacy `registry_cache_dir`
/// (`/opt/overlaybd/registry_cache`) and the isolated directory is lost.
fn write_global_config(dir: &Path) -> Result<PathBuf> {
    let cache_dir = dir.join("cache");
    std::fs::create_dir_all(&cache_dir).context("create cache dir")?;
    let mut global = GlobalConfig::default();
    global.cache_config.cache_type = "file".to_string();
    global.cache_config.cache_dir = cache_dir.to_string_lossy().into_owned();
    let path = dir.join("global.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&global)?).context("write global config")?;
    Ok(path)
}

/// Send one request on a fresh connection and return the response, bounded by
/// `IO_TIMEOUT` so a stalled daemon cannot hang the test.
async fn rpc(socket_path: &Path, request: &DaemonRequest) -> Result<DaemonResponse> {
    tokio::time::timeout(IO_TIMEOUT, async {
        let mut stream = UnixStream::connect(socket_path)
            .await
            .context("connect daemon socket")?;
        send_message(&mut stream, request).await?;
        recv_message(&mut stream)
            .await?
            .context("daemon closed connection without a response")
    })
    .await
    .context("daemon RPC timed out")?
}

async fn acquire(
    socket_path: &Path,
    image_config: &Path,
    global_config: &Path,
) -> Result<(u32, PathBuf)> {
    let response = rpc(
        socket_path,
        &DaemonRequest::AcquireOverlaybd {
            image_config: image_config.to_path_buf(),
            global_config: global_config.to_path_buf(),
            virtual_size: Some(VIRTUAL_SIZE),
            access_mode: AccessMode::Shared,
        },
    )
    .await?;
    match response {
        DaemonResponse::DeviceAcquired {
            dev_id,
            device_path,
        } => Ok((dev_id, device_path)),
        other => anyhow::bail!("acquire failed: {other:?}"),
    }
}

async fn release(socket_path: &Path, dev_id: u32) -> Result<()> {
    let response = rpc(socket_path, &DaemonRequest::ReleaseOverlaybd { dev_id }).await?;
    match response {
        DaemonResponse::Released => Ok(()),
        other => anyhow::bail!("release failed: {other:?}"),
    }
}

/// Run blocking device I/O off the async runtime workers so a stalled read or
/// ioctl cannot delay the in-process daemon sharing this runtime. Bounded by
/// `IO_TIMEOUT`: if the ublk read itself wedges, the test fails instead of
/// hanging. A timeout cannot cancel an already-running blocking closure —
/// that is inherent to `spawn_blocking`; the abandoned thread holds its FD
/// until process exit and the runtime may wait for it during shutdown. That
/// residue is accepted here: a truly wedged ublk read indicates a
/// kernel-side fault, where failing loudly (and letting the CI job timeout
/// kill the process) is the correct outcome.
async fn blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::time::timeout(IO_TIMEOUT, tokio::task::spawn_blocking(f))
        .await
        .context("blocking device I/O timed out")?
        .context("join blocking device I/O")?
}

/// Read one host page at `MARKER_OFFSET` with `O_DIRECT`, bypassing the page
/// cache. Buffer alignment is required by `O_DIRECT`.
fn read_direct(device_path: &Path) -> Result<Vec<u8>> {
    let page_size = host_page_size();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(device_path)
        .context("open device for O_DIRECT read")?;
    let layout = Layout::from_size_align(page_size, page_size).expect("page layout");
    let raw = unsafe { std::alloc::alloc_zeroed(layout) };
    anyhow::ensure!(!raw.is_null(), "aligned page allocation failed");
    // The closure returns `Err` instead of panicking on short reads, and
    // `dealloc` runs unconditionally afterward, so the aligned page cannot
    // leak and I/O failures keep their context.
    let result = (|| -> Result<Vec<u8>> {
        let buf = unsafe { std::slice::from_raw_parts_mut(raw, page_size) };
        let n = file
            .read_at(buf, MARKER_OFFSET)
            .context("O_DIRECT read at marker offset")?;
        anyhow::ensure!(n == page_size, "short O_DIRECT read: {n} bytes");
        Ok(buf.to_vec())
    })();
    unsafe { dealloc(raw, layout) };
    result
}

/// Read one host page through a buffered FD, requiring a full page: a short
/// read must fail loudly instead of passing with a partially zeroed buffer.
fn read_buffered_page(file: &std::fs::File) -> Result<Vec<u8>> {
    let page_size = host_page_size();
    let mut buf = vec![0u8; page_size];
    let n = file
        .read_at(&mut buf, MARKER_OFFSET)
        .context("buffered read at marker offset")?;
    anyhow::ensure!(n == page_size, "short buffered read: {n} bytes");
    Ok(buf)
}

/// Read the marker page through the retained FD on a blocking thread,
/// returning FD ownership for the next operation.
async fn read_retained(fd: std::fs::File) -> Result<(std::fs::File, Vec<u8>)> {
    blocking(move || {
        let page = read_buffered_page(&fd)?;
        Ok((fd, page))
    })
    .await
}

/// Best-effort BLKFLSBUF, mirroring the daemon's `clear_page_cache`.
fn flush_buffer_cache(device_path: &Path) -> Result<()> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(device_path)
        .context("open device for BLKFLSBUF")?;
    let ret = unsafe { libc::ioctl(file.as_raw_fd(), BLKFLSBUF as _) };
    anyhow::ensure!(
        ret >= 0,
        "BLKFLSBUF failed: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}

fn first_u64le(page: &[u8]) -> u64 {
    u64::from_le_bytes(page[..8].try_into().expect("page prefix"))
}

/// Synchronous best-effort RPC for the cleanup guard, executed on the guard's
/// dedicated thread (async I/O is unavailable in `Drop`). Read/write deadlines
/// keep a wedged daemon from hanging the test process, and daemon-side error
/// responses are surfaced instead of counting as successful cleanup.
fn sync_rpc(socket_path: &Path, request: &DaemonRequest) -> Result<()> {
    use std::io::{Read, Write};
    let mut stream =
        std::os::unix::net::UnixStream::connect(socket_path).context("connect daemon socket")?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .context("set read timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .context("set write timeout")?;
    let payload = serde_json::to_vec(request).context("serialize request")?;
    stream.write_all(&(payload.len() as u32).to_be_bytes())?;
    stream.write_all(&payload)?;
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len);
    anyhow::ensure!(len <= MAX_MESSAGE_SIZE, "daemon message too large: {len}");
    let mut body = vec![0u8; len as usize];
    stream.read_exact(&mut body)?;
    match serde_json::from_slice::<DaemonResponse>(&body).context("deserialize response")? {
        DaemonResponse::Error { message }
        | DaemonResponse::InvalidRequest { message }
        | DaemonResponse::TerminalError { message } => {
            anyhow::bail!("daemon reported an error: {message}")
        }
        _ => Ok(()),
    }
}

/// Best-effort cleanup on every exit path: release the pooled device and shut
/// the daemon down so a failed (or intentionally failing) run never leaks a
/// live ublk device or a running server task into later tests or the host.
struct DaemonCleanup {
    socket_path: PathBuf,
    dev_id: Option<u32>,
    server_task: Option<tokio::task::JoinHandle<Result<()>>>,
    /// Owns the daemon's working directory. On error-path drops the directory
    /// moves into the cleanup thread, so the socket path still exists while
    /// the cleanup RPCs run and is removed only when that thread finishes.
    workdir: Option<TempDir>,
}

impl Drop for DaemonCleanup {
    fn drop(&mut self) {
        let socket_path = self.socket_path.clone();
        let dev_id = self.dev_id.take();
        let mut task = self.server_task.take();
        if dev_id.is_none() && task.is_none() {
            return;
        }
        let workdir = self.workdir.take();
        // Run cleanup on a dedicated thread: `Drop` may execute on a runtime
        // worker (including mid-unwind), where blocking RPCs must never run.
        std::thread::spawn(move || {
            // Removed when this thread finishes, never before the RPCs.
            let _workdir = workdir;
            if let Some(dev_id) = dev_id {
                let _ = sync_rpc(&socket_path, &DaemonRequest::ReleaseOverlaybd { dev_id });
            }
            let _ = sync_rpc(&socket_path, &DaemonRequest::Shutdown);
            if let Some(task) = task.as_mut() {
                // The shutdown RPC replies before `stop_all_devices()` runs,
                // so give the daemon a bounded window to tear down devices
                // instead of aborting it mid-cleanup and leaking the device.
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !task.is_finished() && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(50));
                }
                task.abort();
            }
        });
    }
}

// Multi-thread runtime like the daemon binary: device setup performs blocking
// work that would stall a current-thread test runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "issue #302: fails until the image-switch handover invalidates stale page cache; un-ignore with the fix"]
async fn pooled_device_must_not_serve_stale_placeholder_pages() -> Result<()> {
    if !uvm_ublk::ublk_available() {
        eprintln!("skipping: ublk is not available on this host");
        return Ok(());
    }

    let tmp = TempDir::new().context("create tempdir")?;
    let image_config = create_marker_image(tmp.path()).await?;
    let global_config = write_global_config(tmp.path())?;
    let socket_path = tmp.path().join("daemon.sock");

    let image_service = ImageService::from_config_path(&global_config)
        .await
        .context("create image service")?;
    let (ctrl_ring, _ctrl_ring_handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);

    let mut server = UblkDaemonServer::new(
        socket_path.clone(),
        ctrl_ring,
        image_service,
        global_config.clone(),
    );
    let features = server
        .detect_ublk_features()
        .await
        .context("detect ublk features")?;
    // Issue's isolated pool shape: no prewarm, single idle slot.
    server.enable_pool(
        PoolConfig {
            low_watermark: 0,
            high_watermark: 1,
            maintenance_enabled: false,
            startup_prewarm: false,
        },
        features,
    );

    let server = Arc::new(server);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let mut cleanup = {
        let server = Arc::clone(&server);
        let task = tokio::spawn(async move {
            server
                .run_with_ready_signal(|| {
                    ready_tx
                        .send(())
                        .map_err(|_| anyhow::anyhow!("ready receiver dropped"))
                })
                .await
        });
        DaemonCleanup {
            socket_path: socket_path.clone(),
            dev_id: None,
            server_task: Some(task),
            workdir: Some(tmp),
        }
    };

    // Wait asynchronously for readiness and surface an early daemon exit
    // instead of only reporting a timeout.
    let server_task = cleanup.server_task.as_mut().expect("server task");
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            joined = server_task => {
                joined
                    .context("join daemon task")?
                    .context("daemon exited before becoming ready")
            }
            ready = ready_rx => ready.context("daemon exited before signaling readiness"),
        }
    })
    .await
    .context("daemon did not become ready")??;

    // Step 1: acquire and record the marker page through O_DIRECT.
    let (dev_id, device_path) = acquire(&socket_path, &image_config, &global_config).await?;
    cleanup.dev_id = Some(dev_id);
    let original = {
        let path = device_path.clone();
        blocking(move || read_direct(&path)).await?
    };
    assert_eq!(
        first_u64le(&original),
        MARKER_OFFSET,
        "baseline O_DIRECT read must return the nonzero marker page"
    );

    // Step 2: release; the device swaps to the zero placeholder and stays pooled.
    // The guard must not release it a second time if a later step fails.
    release(&socket_path, dev_id).await?;
    cleanup.dev_id = None;

    // Step 3: keep a buffered FD open across the reacquire, modeling a reader
    // whose cache lifetime spans the handover. Verify the placeholder reads
    // as zeros, discard any business-image read-ahead, then deliberately fill
    // the placeholder page cache.
    let mut retained_fd = {
        let path = device_path.clone();
        blocking(move || {
            std::fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .context("open retained buffered fd")
        })
        .await?
    };
    let idle_direct = {
        let path = device_path.clone();
        blocking(move || read_direct(&path)).await?
    };
    assert_eq!(
        idle_direct,
        vec![0u8; host_page_size()],
        "idle placeholder must read as zeros via O_DIRECT"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let path = device_path.clone();
        blocking(move || flush_buffer_cache(&path)).await?;
    }
    let (fd, idle_buffered) = read_retained(retained_fd).await?;
    retained_fd = fd;
    assert_eq!(
        idle_buffered,
        vec![0u8; host_page_size()],
        "idle placeholder must read as zeros via the buffered path"
    );

    // Step 4: reacquire; the pool must hand back the same device. Track the new
    // device in the guard before asserting: if the assertions fail, cleanup
    // must release the device we actually hold now, not the previous one.
    let (dev_id2, device_path2) = acquire(&socket_path, &image_config, &global_config).await?;
    cleanup.dev_id = Some(dev_id2);
    assert_eq!(dev_id, dev_id2, "pool must reuse the same device");
    assert_eq!(device_path, device_path2);

    // Control: direct reads must see the business image again.
    let direct_after = {
        let path = device_path.clone();
        blocking(move || read_direct(&path)).await?
    };
    assert_eq!(
        direct_after, original,
        "post-switch O_DIRECT read must return the marker page"
    );

    // The stale buffered page is captured, not yet asserted: the recovery
    // control and cleanup below must run even when the defect reproduces.
    let (fd, stale_buffered) = read_retained(retained_fd).await?;
    retained_fd = fd;

    // Control: invalidating the device cache after the switch restores the
    // buffered path.
    {
        let path = device_path.clone();
        blocking(move || flush_buffer_cache(&path)).await?;
    }
    let (fd, recovered) = read_retained(retained_fd).await?;
    retained_fd = fd;
    assert_eq!(
        recovered, original,
        "buffered read after BLKFLSBUF must return the marker page"
    );

    // Cleanup before the regression assertion: close the retained FD, release
    // the device, and shut the daemon down gracefully.
    drop(retained_fd);
    release(&socket_path, dev_id2).await?;
    cleanup.dev_id = None;
    rpc(&socket_path, &DaemonRequest::Shutdown).await?;
    if let Some(task) = cleanup.server_task.take() {
        task.await
            .context("join daemon task")?
            .context("daemon run failed")?;
    }

    // The regression assertion, intentionally last: buffered reads must not
    // serve pages cached while the device was bound to the placeholder.
    assert_eq!(
        first_u64le(&stale_buffered),
        MARKER_OFFSET,
        "post-switch buffered read served a stale placeholder page (issue #302)"
    );
    assert_eq!(stale_buffered, original);
    Ok(())
}
