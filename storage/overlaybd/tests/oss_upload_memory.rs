//! Peak heap of [`upload_file_streaming`] must stay bounded by its part size and
//! the concurrency it actually uses, not grow with the size of the file.
//!
//! The upload goes through the production operator (S3 service, timeout and
//! retry layers) to an S3 endpoint that answers at once. The source is a FIFO
//! fed one part at a time, and the feeder waits for the endpoint to acknowledge
//! every part the writer could have sent before feeding the next, so earlier
//! part uploads have finished before more of the file exists. That is the case
//! where the endpoint keeps up with the reader, which is the normal state of a
//! snapshot upload from local disk.
//!
//! The endpoint and the feeder run in a child process, this test binary run
//! again as [`endpoint_and_feeder`], so the counting allocator sees only the
//! upload and the runtime it runs on. No docker or network setup is required.
//! The FIFO needs `mkfifo`, so the test runs on Unix only.
#![cfg(unix)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::ffi::CString;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use object_store_operator::{
    build_object_store_operator, AddressingStyle, ObjectStoreOperatorConfig, ObjectStoreService,
    ResolvedCredential,
};
use overlaybd::backend::oss::{upload_file_streaming, MAX_UPLOAD_CONCURRENCY};
use tokio::net::TcpListener;

struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

fn record_alloc(size: usize) {
    let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if new_size > layout.size() {
                record_alloc(new_size - layout.size());
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const MIB: usize = 1024 * 1024;
/// Above S3's 5 MiB floor, which OpenDAL would otherwise raise it to, and a
/// multiple of the 2 MiB read size so parts come out exactly this size.
const PART_SIZE: usize = 6 * MIB;
const CONCURRENCY: usize = 8;
const PARTS: usize = 40;
/// Parts the writer keeps before uploading: it starts the multipart upload
/// only once a second part exists, and it fills one part while another waits.
const HELD_BACK: usize = 2;
/// Where [`endpoint_and_feeder`] finds the FIFO; set only in the child process.
const SOURCE_ENV: &str = "OSS_UPLOAD_MEMORY_SOURCE";
/// Starts the line on which the child process reports its endpoint's port.
const PORT_LINE: &str = "endpoint port: ";

/// Bytes and parts the endpoint has fully received, so the feeder can wait for
/// an upload to finish instead of guessing how long it takes.
#[derive(Default)]
struct Received {
    progress: Mutex<(u64, usize)>,
    changed: Condvar,
}

impl Received {
    fn record_part(&self, length: u64) {
        let mut progress = self.progress.lock().unwrap();
        progress.0 += length;
        progress.1 += 1;
        self.changed.notify_all();
    }

    fn wait_for_parts(&self, parts: usize) {
        let progress = self.progress.lock().unwrap();
        let (_progress, timeout) = self
            .changed
            .wait_timeout_while(progress, Duration::from_secs(30), |progress| {
                progress.1 < parts
            })
            .unwrap();
        assert!(!timeout.timed_out(), "endpoint never received part {parts}");
    }

    fn bytes(&self) -> u64 {
        self.progress.lock().unwrap().0
    }
}

/// The parts of a minimal S3 multipart endpoint that a write uses. Bodies are
/// drained frame by frame and dropped, so the endpoint holds no part in memory.
async fn s3_endpoint(
    State(received): State<Arc<Received>>,
    method: Method,
    uri: Uri,
    body: Body,
) -> Response {
    let query = uri.query().unwrap_or_default().to_string();
    let mut stream = body.into_data_stream();
    let mut length = 0;
    while let Some(frame) = stream.next().await {
        length += frame.expect("read request body").len() as u64;
    }
    if method == Method::PUT {
        received.record_part(length);
    }
    let xml = |body: String| ([(header::CONTENT_TYPE, "application/xml")], body).into_response();
    match method {
        Method::POST if query.contains("uploads") => xml(
            "<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket>\
             <Key>layer</Key><UploadId>test-upload</UploadId>\
             </InitiateMultipartUploadResult>"
                .to_string(),
        ),
        Method::PUT => ([(header::ETAG, "\"test-etag\"")], "").into_response(),
        Method::POST if query.contains("uploadId") => xml(
            "<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket>\
             <Key>layer</Key><ETag>\"test-etag\"</ETag></CompleteMultipartUploadResult>"
                .to_string(),
        ),
        _ => (
            StatusCode::BAD_REQUEST,
            format!("unexpected {method} {uri}"),
        )
            .into_response(),
    }
}

fn make_fifo(path: &Path) {
    let c_path = CString::new(path.as_os_str().as_bytes()).expect("fifo path");
    // SAFETY: `c_path` is a valid NUL-terminated path for the call's duration.
    let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    assert_eq!(rc, 0, "mkfifo: {}", std::io::Error::last_os_error());
}

/// The child process of [`upload_memory_does_not_grow_with_file_size`]. Serves
/// the endpoint and feeds the FIFO until the test closes its stdin, which it
/// does once the upload has returned.
#[test]
#[ignore = "run in a child process by upload_memory_does_not_grow_with_file_size"]
fn endpoint_and_feeder() {
    let Some(source) = std::env::var_os(SOURCE_ENV) else {
        return;
    };
    let received = Arc::new(Received::default());
    let runtime = tokio::runtime::Runtime::new().expect("endpoint runtime");
    let listener = runtime
        .block_on(TcpListener::bind("127.0.0.1:0"))
        .expect("bind endpoint");
    let port = listener.local_addr().expect("endpoint addr").port();
    let app = axum::Router::new()
        .fallback(s3_endpoint)
        .with_state(received.clone());
    runtime.spawn(async move { axum::serve(listener, app).await });
    println!("{PORT_LINE}{port}");

    // Not joined: if the upload failed, the feeder may be blocked on the FIFO,
    // and it ends with the process.
    let feeder_received = received.clone();
    std::thread::spawn(move || {
        let mut fifo = std::fs::OpenOptions::new()
            .write(true)
            .open(source)
            .expect("open fifo for writing");
        let part = vec![0x5a_u8; PART_SIZE];
        for fed in 1..=PARTS {
            fifo.write_all(&part).expect("feed part");
            feeder_received.wait_for_parts(fed.saturating_sub(HELD_BACK));
        }
    });

    std::io::stdin()
        .read_to_end(&mut Vec::new())
        .expect("wait for the upload to return");
    assert_eq!(
        received.bytes(),
        (PARTS * PART_SIZE) as u64,
        "the endpoint must receive every byte of the parts"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_memory_does_not_grow_with_file_size() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source = dir.path().join("layer.fifo");
    make_fifo(&source);
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args(["endpoint_and_feeder", "--exact", "--ignored", "--nocapture"])
        .env(SOURCE_ENV, &source)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start endpoint and feeder");
    let mut child_stdout = BufReader::new(child.stdout.take().expect("child stdout"));
    let port: u16 = child_stdout
        .by_ref()
        .lines()
        .find_map(|line| {
            line.expect("read child stdout")
                .strip_prefix(PORT_LINE)?
                .parse()
                .ok()
        })
        .expect("child never reported its endpoint port");

    let operator = build_object_store_operator(
        &ObjectStoreOperatorConfig {
            service: ObjectStoreService::S3,
            bucket: "test-bucket".to_string(),
            endpoint: format!("http://127.0.0.1:{port}"),
            region: "us-east-1".to_string(),
            addressing_style: AddressingStyle::Path,
            timeout: Some(Duration::from_secs(30)),
            max_retries: Some(1),
        },
        Some(&ResolvedCredential {
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            security_token: None,
            expires_at: None,
        }),
    )
    .expect("build operator");

    let baseline = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(baseline, Ordering::Relaxed);
    tokio::time::timeout(
        Duration::from_secs(120),
        upload_file_streaming(&operator, "layer", &source, PART_SIZE, CONCURRENCY, None),
    )
    .await
    .expect("upload timed out")
    .expect("upload");
    let growth = PEAK_BYTES.load(Ordering::Relaxed).saturating_sub(baseline);

    drop(child.stdin.take());
    std::io::copy(&mut child_stdout, &mut std::io::sink()).expect("drain child stdout");
    assert!(
        child.wait().expect("wait for child").success(),
        "the endpoint or feeder failed; see its output above"
    );

    // A part per upload in flight, the part the writer holds back and the one it
    // is filling, plus two 2 MiB read buffers: upload_file_streaming's own and
    // the one tokio's `File` reads into. With one part in flight the writer
    // fills nothing while it uploads, which leaves room for HTTP and runtime
    // overhead.
    let concurrency = CONCURRENCY.min(MAX_UPLOAD_CONCURRENCY);
    let bound = (concurrency + 2) * PART_SIZE + 4 * MIB;
    assert!(
        growth < bound,
        "uploading {} MiB in {} MiB parts at concurrency {CONCURRENCY}, capped at \
         {MAX_UPLOAD_CONCURRENCY}, raised peak heap by {} MiB; it must stay under {} MiB \
         regardless of file size",
        PARTS * PART_SIZE / MIB,
        PART_SIZE / MIB,
        growth / MIB,
        bound / MIB,
    );
}
