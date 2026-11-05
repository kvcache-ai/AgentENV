//! Peak heap of [`upload_file_streaming`] must stay bounded by its part size and
//! concurrency, not grow with the size of the file.
//!
//! The upload goes through the production operator (S3 service, timeout and
//! retry layers) to an in-process S3 endpoint that answers at once. The source
//! is a FIFO fed one part at a time, and the feeder waits for the endpoint to
//! acknowledge every part the writer could have sent before feeding the next,
//! so earlier part uploads have finished before more of the file exists. That
//! is the case where the endpoint keeps up with the reader, which is the normal
//! state of a snapshot upload from local disk. No docker or network setup is
//! required. The FIFO needs `mkfifo`, so the test runs on Unix only.
#![cfg(unix)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::ffi::CString;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
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
use overlaybd::backend::oss::upload_file_streaming;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_memory_does_not_grow_with_file_size() {
    let received = Arc::new(Received::default());
    let app = axum::Router::new()
        .fallback(s3_endpoint)
        .with_state(received.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind endpoint");
    let port = listener.local_addr().expect("endpoint addr").port();
    tokio::spawn(async move { axum::serve(listener, app).await });

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

    let dir = tempfile::tempdir().expect("tempdir");
    let source = dir.path().join("layer.fifo");
    make_fifo(&source);
    let feeder_path = source.clone();
    let feeder_received = received.clone();
    let feeder = std::thread::spawn(move || {
        let mut fifo = std::fs::OpenOptions::new()
            .write(true)
            .open(feeder_path)
            .expect("open fifo for writing");
        let part = vec![0x5a_u8; PART_SIZE];
        for fed in 1..=PARTS {
            fifo.write_all(&part).expect("feed part");
            feeder_received.wait_for_parts(fed.saturating_sub(HELD_BACK));
        }
    });

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
    feeder.join().expect("feeder thread");

    assert_eq!(
        received.bytes(),
        (PARTS * PART_SIZE) as u64,
        "the endpoint must receive every byte of the parts"
    );
    // Parts in flight, the part the writer holds back, the one it is filling,
    // and slack for the read buffer and HTTP machinery.
    let bound = (2 * CONCURRENCY + 4) * PART_SIZE;
    assert!(
        growth < bound,
        "uploading {} MiB in {} MiB parts at concurrency {CONCURRENCY} raised peak heap by \
         {} MiB; it must stay under {} MiB regardless of file size",
        PARTS * PART_SIZE / MIB,
        PART_SIZE / MIB,
        growth / MIB,
        bound / MIB,
    );
}
