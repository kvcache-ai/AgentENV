//! Safe wrappers over `qpl-sys`; the only place in the QPL codec with `unsafe`.

use std::alloc::{self, Layout};
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use qpl_sys as sys;

const PAGE_SIZE: usize = 4096;
/// Log once per call when the work queues stay busy this long.
const BUSY_WARN_AFTER: Duration = Duration::from_secs(1);

/// Execution path of a job, fixed when the job is initialized.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Path {
    Hardware,
    Software,
}

impl Path {
    fn raw(self) -> sys::qpl_path_t {
        match self {
            Path::Hardware => sys::qpl_path_hardware,
            Path::Software => sys::qpl_path_software,
        }
    }
}

/// What a job does with one block: one raw DEFLATE stream in or out.
#[derive(Clone, Copy, Debug)]
pub(super) enum Op {
    Compress,
    Decompress,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Compress => "compress",
            Op::Decompress => "decompress",
        }
    }

    fn raw(self) -> sys::qpl_operation {
        match self {
            Op::Compress => sys::qpl_op_compress,
            Op::Decompress => sys::qpl_op_decompress,
        }
    }

    fn flags(self) -> u32 {
        match self {
            Op::Compress => {
                sys::QPL_FLAG_FIRST
                    | sys::QPL_FLAG_LAST
                    | sys::QPL_FLAG_DYNAMIC_HUFFMAN
                    | sys::QPL_FLAG_OMIT_VERIFY
            }
            Op::Decompress => sys::QPL_FLAG_FIRST | sys::QPL_FLAG_LAST,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("QPL {op} failed with qpl_status {status}")]
pub(super) struct QplError {
    op: &'static str,
    status: sys::qpl_status,
}

fn check(op: &'static str, status: sys::qpl_status) -> Result<(), QplError> {
    if status == sys::QPL_STS_OK {
        Ok(())
    } else {
        Err(QplError { op, status })
    }
}

/// Worst-case QPL compression output (one raw DEFLATE stream) for
/// `src_len` input bytes.
pub(super) fn qpl_compress_bound(src_len: u32) -> u32 {
    // SAFETY: a pure size computation; it takes no pointers and no QPL state.
    unsafe { sys::qpl_get_safe_deflate_compression_buffer_size(src_len) }
}

/// Page-aligned heap buffer of whole pages for QPL job state and hardware
/// data (bounce) buffers. Every page is faulted in at allocation.
pub(super) struct DeviceBuf {
    ptr: NonNull<u8>,
    layout: Layout,
    locked: bool,
}

impl DeviceBuf {
    pub(super) fn new(len: usize) -> Self {
        assert!(len > 0, "DeviceBuf length must be non-zero");
        // Whole pages, so unlocking this buffer never unlocks another allocation.
        let layout = Layout::from_size_align(len.next_multiple_of(PAGE_SIZE), PAGE_SIZE)
            .expect("DeviceBuf layout");
        // SAFETY: `layout` has a non-zero size.
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        let Some(ptr) = NonNull::new(ptr) else {
            alloc::handle_alloc_error(layout)
        };
        // A zeroed allocation may still be unmapped zero pages; a volatile
        // write per page forces each one in.
        for offset in (0..layout.size()).step_by(PAGE_SIZE) {
            // SAFETY: `offset < layout.size()`, inside the allocation.
            unsafe { ptr.as_ptr().add(offset).write_volatile(0) };
        }
        Self {
            ptr,
            layout,
            locked: false,
        }
    }

    /// A buffer IAA accesses: also locked in memory, if `RLIMIT_MEMLOCK`
    /// allows, so it is never swapped out while the device may use it.
    pub(super) fn new_locked(len: usize) -> Self {
        let mut buf = Self::new(len);
        // SAFETY: the range is exactly this allocation.
        if unsafe { libc::mlock(buf.as_ptr().cast(), buf.layout.size()) } == 0 {
            buf.locked = true;
        } else {
            warn_unlocked(io::Error::last_os_error());
        }
        buf
    }

    fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: the allocation is `layout.size()` initialized bytes, and
        // `&mut self` makes this the only live reference to it.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl Drop for DeviceBuf {
    fn drop(&mut self) {
        if self.locked {
            // SAFETY: the range `new_locked` locked, still this allocation.
            unsafe { libc::munlock(self.as_ptr().cast(), self.layout.size()) };
        }
        // SAFETY: allocated in `new` with exactly this layout.
        unsafe { alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

fn warn_unlocked(error: io::Error) {
    static WARNED: AtomicBool = AtomicBool::new(false);
    if !WARNED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            %error,
            "cannot lock QPL buffers in memory; raise RLIMIT_MEMLOCK to keep them from being swapped out"
        );
    }
}

// SAFETY: `DeviceBuf` uniquely owns its allocation; nothing in it is tied to a
// thread, so ownership may move between threads.
unsafe impl Send for DeviceBuf {}

/// One initialized `qpl_job`. A job is only in flight inside `run` or
/// `run_batch`, and both wait for it before returning.
pub(super) struct Job {
    buf: DeviceBuf,
}

impl Job {
    pub(super) fn new(path: Path) -> Result<Self, QplError> {
        let mut size = 0u32;
        // SAFETY: `size` is a valid out-pointer for the duration of the call.
        check("get_job_size", unsafe {
            sys::qpl_get_job_size(path.raw(), &mut size)
        })?;
        let len = (size as usize).max(std::mem::size_of::<sys::qpl_job>());
        let buf = match path {
            Path::Hardware => DeviceBuf::new_locked(len),
            Path::Software => DeviceBuf::new(len),
        };
        // SAFETY: `buf` holds at least `qpl_get_job_size` bytes, is page
        // aligned (stricter than `qpl_job` needs) and is exclusively ours. On
        // failure the buffer is freed without `qpl_fini_job`, as QPL expects.
        check("init_job", unsafe {
            sys::qpl_init_job(path.raw(), buf.as_ptr().cast())
        })?;
        let mut job = Self { buf };
        // Skips the cpuid topology lookup the default SOCKET policy does per off-node device.
        // QPL 1.9.0 applies it only in qpl_submit_job; qpl_execute_job always uses SOCKET.
        job.job_mut().numa_id = sys::QPL_DEVICE_NUMA_ID_CURRENT;
        Ok(job)
    }

    /// Compresses `src` into one raw DEFLATE stream in `dst` and returns its
    /// length; input that does not compress is stored.
    pub(super) fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize, QplError> {
        let result = self.run(Op::Compress, src, dst);
        compressed_or_stored(result, src, dst)
    }

    /// Decompresses one raw DEFLATE stream; returns the decompressed length.
    pub(super) fn decompress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize, QplError> {
        self.run(Op::Decompress, src, dst)
    }

    fn run(&mut self, op: Op, src: &[u8], dst: &mut [u8]) -> Result<usize, QplError> {
        self.prepare(op, src, dst)?;
        // SAFETY: `prepare` pointed the job at buffers borrowed for this whole
        // call, and `qpl_execute_job` finishes the job (or fails to submit it)
        // before returning, so the device never outlives them.
        self.retry_busy(op, |job| unsafe { sys::qpl_execute_job(job) })?;
        Ok(self.job_mut().total_out as usize)
    }

    /// Submits the job without waiting for it.
    ///
    /// # Safety
    ///
    /// On success the job is in flight: `src` and `dst` must stay valid and
    /// unused until `qpl_wait_job` has returned for this job.
    unsafe fn submit(&mut self, op: Op, src: &[u8], dst: &mut [u8]) -> Result<(), QplError> {
        self.prepare(op, src, dst)?;
        // SAFETY: the caller keeps `src` and `dst` alive until the job is waited for.
        self.retry_busy(op, |job| unsafe { sys::qpl_submit_job(job) })
    }

    /// Points the job at `src` and `dst`, resetting every field a previous
    /// operation on this job may have changed.
    fn prepare(&mut self, op: Op, src: &[u8], dst: &mut [u8]) -> Result<(), QplError> {
        let too_large = || QplError {
            op: op.name(),
            status: sys::QPL_STS_BUFFER_TOO_LARGE_ERR,
        };
        let available_in = u32::try_from(src.len()).map_err(|_| too_large())?;
        let available_out = u32::try_from(dst.len()).map_err(|_| too_large())?;

        let job = self.job_mut();
        job.op = op.raw();
        job.flags = op.flags();
        job.level = sys::qpl_default_level;
        job.next_in_ptr = src.as_ptr().cast_mut();
        job.available_in = available_in;
        job.total_in = 0;
        job.next_out_ptr = dst.as_mut_ptr();
        job.available_out = available_out;
        job.total_out = 0;
        job.crc = 0;
        job.xor_checksum = 0;
        Ok(())
    }

    /// Calls `call` until the work queues take the job. A busy queue means
    /// nothing was submitted, and QPL expects the same job to be resubmitted
    /// as is.
    fn retry_busy(
        &mut self,
        op: Op,
        mut call: impl FnMut(&mut sys::qpl_job) -> sys::qpl_status,
    ) -> Result<(), QplError> {
        let started = Instant::now();
        let mut attempt = 0u32;
        let mut warned = false;
        loop {
            let status = call(self.job_mut());
            if status != sys::QPL_STS_QUEUES_ARE_BUSY_ERR {
                return check(op.name(), status);
            }
            if !warned && started.elapsed() >= BUSY_WARN_AFTER {
                tracing::warn!(op = op.name(), "IAA work queues stay busy; still retrying");
                warned = true;
            }
            backoff(attempt);
            attempt = attempt.saturating_add(1);
        }
    }

    fn job_mut(&mut self) -> &mut sys::qpl_job {
        // SAFETY: the buffer holds a `qpl_job` initialized by `qpl_init_job`
        // and aligned for it; `&mut self` makes the access exclusive.
        unsafe { &mut *self.buf.as_ptr().cast::<sys::qpl_job>() }
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        // SAFETY: the job was initialized in `new` and is not in flight.
        unsafe { sys::qpl_fini_job(self.job_mut()) };
    }
}

/// One block of a batch: the job that runs it, its input and its output.
pub(super) struct BatchItem<'a> {
    pub(super) job: &'a mut Job,
    pub(super) src: &'a [u8],
    pub(super) dst: &'a mut [u8],
}

/// Runs `op` on every item at once: submits them all, then waits for each in
/// submission order. Returns each item's output length or error; compression
/// stores input that does not compress, as `Job::compress` does.
pub(super) fn run_batch(op: Op, items: &mut [BatchItem<'_>]) -> Vec<Result<usize, QplError>> {
    let mut results = Vec::with_capacity(items.len());
    let mut in_flight = InFlight {
        jobs: Vec::with_capacity(items.len()),
        waited: 0,
    };
    for item in items.iter_mut() {
        // SAFETY: `items` lends `src` and `dst` until this function returns,
        // and `in_flight` waits for every submitted job before that, also
        // when unwinding.
        let submitted = unsafe { item.job.submit(op, item.src, item.dst) };
        if submitted.is_ok() {
            in_flight.jobs.push(&mut *item.job);
        }
        results.push(submitted.map(|()| 0));
    }
    for result in results.iter_mut().filter(|result| result.is_ok()) {
        *result = in_flight.wait_next(op);
    }
    drop(in_flight);
    if let Op::Compress = op {
        for (result, item) in results.iter_mut().zip(items.iter_mut()) {
            let done = std::mem::replace(result, Ok(0));
            *result = compressed_or_stored(done, item.src, item.dst);
        }
    }
    results
}

/// Jobs submitted by `run_batch`. Dropping it waits for any not waited for
/// yet, so a panic never frees buffers the device is still using.
struct InFlight<'a> {
    jobs: Vec<&'a mut Job>,
    waited: usize,
}

impl InFlight<'_> {
    fn wait_next(&mut self, op: Op) -> Result<usize, QplError> {
        let job = &mut *self.jobs[self.waited];
        self.waited += 1;
        // SAFETY: the job was submitted and has not been waited for yet.
        check(op.name(), unsafe { sys::qpl_wait_job(job.job_mut()) })?;
        Ok(job.job_mut().total_out as usize)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        for job in &mut self.jobs[self.waited..] {
            // SAFETY: as in `wait_next`; only completion matters here.
            unsafe { sys::qpl_wait_job(job.job_mut()) };
        }
    }
}

/// Spin briefly, then yield, then sleep between retries.
fn backoff(attempt: u32) {
    match attempt {
        0..16 => std::hint::spin_loop(),
        16..32 => std::thread::yield_now(),
        _ => std::thread::sleep(Duration::from_micros(50)),
    }
}

/// Keeps a compressed stream that is no longer than its input; otherwise,
/// also when it did not fit `dst`, writes the input as stored blocks.
fn compressed_or_stored(
    result: Result<usize, QplError>,
    src: &[u8],
    dst: &mut [u8],
) -> Result<usize, QplError> {
    match result {
        Ok(len) if len <= src.len() => Ok(len),
        Err(error) if error.status != sys::QPL_STS_MORE_OUTPUT_NEEDED => Err(error),
        _ => write_stored(src, dst),
    }
}

/// Writes `src` as DEFLATE stored blocks of at most 65535 bytes each and
/// returns the stream length.
fn write_stored(src: &[u8], dst: &mut [u8]) -> Result<usize, QplError> {
    const MAX_BLOCK: usize = u16::MAX as usize;
    const HEADER: usize = 5;
    let needed = src.len() + HEADER * src.len().div_ceil(MAX_BLOCK).max(1);
    if dst.len() < needed {
        return Err(QplError {
            op: Op::Compress.name(),
            status: sys::QPL_STS_MORE_OUTPUT_NEEDED,
        });
    }
    let mut out = 0;
    let mut rest = src;
    loop {
        let (block, tail) = rest.split_at(rest.len().min(MAX_BLOCK));
        let len = u16::try_from(block.len()).expect("stored block length fits u16");
        // BFINAL in bit 0 and BTYPE 00 (stored); the rest of the byte is padding.
        dst[out] = u8::from(tail.is_empty());
        dst[out + 1..out + 3].copy_from_slice(&len.to_le_bytes());
        dst[out + 3..out + HEADER].copy_from_slice(&(!len).to_le_bytes());
        dst[out + HEADER..out + HEADER + block.len()].copy_from_slice(block);
        out += HEADER + block.len();
        rest = tail;
        if rest.is_empty() {
            return Ok(out);
        }
    }
}
