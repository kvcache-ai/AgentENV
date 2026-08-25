use agentenv_test_support::minio::{MinioFixture, MINIO_PASS, MINIO_USER};
use anyhow::Error;
use overlaybd::backend::oss::OssBackend;
use overlaybd::config::OssConfig;

/// Upload shape for the probes. 16 MiB is what a layer upload uses in practice;
/// the seam assertions below are derived from it rather than hardcoded.
const PROBE_PART_SIZE: usize = 16 * 1024 * 1024;
const PROBE_CONCURRENCY: usize = 4;

// Run with: cargo test -p overlaybd --test oss_backend_minio -- --ignored

fn backend(fixture: &MinioFixture) -> OssBackend {
    let config = OssConfig {
        enable: true,
        access_key_id: MINIO_USER.to_string(),
        secret_access_key: MINIO_PASS.to_string(),
        security_token: String::new(),
        credential_process: String::new(),
        default_region: fixture.region.clone(),
        default_endpoint: fixture.endpoint.clone(),
        default_addressing_style: String::new(),
        timeout_secs: 30,
        retry_count: 3,
    };
    OssBackend::new(&config).expect("create oss backend")
}

fn backend_with_bad_credentials(fixture: &MinioFixture) -> OssBackend {
    let config = OssConfig {
        enable: true,
        access_key_id: "wrong-key".to_string(),
        secret_access_key: "wrong-secret".to_string(),
        security_token: String::new(),
        credential_process: String::new(),
        default_region: fixture.region.clone(),
        default_endpoint: fixture.endpoint.clone(),
        default_addressing_style: String::new(),
        timeout_secs: 10,
        retry_count: 0,
    };
    OssBackend::new(&config).expect("create bad backend")
}

fn error_chain_contains(err: &Error, needle: &str) -> bool {
    err.chain().any(|cause| cause.to_string().contains(needle))
}

#[tokio::test]
#[ignore = "requires docker"]
async fn minio_upload_and_read_back() {
    let fixture = MinioFixture::start().await.expect("start minio");
    let backend = backend(&fixture);

    let payload = b"hello from minio integration test".to_vec();
    let url = fixture.object_url("layers/test-object");
    backend
        .upload_bytes(&url, payload.clone())
        .await
        .expect("upload to minio");

    let file = backend
        .open_with_size_hint(&url, None)
        .expect("open from minio");
    let size = file.size().await.expect("get size");
    assert_eq!(size, payload.len() as u64);

    let got = file.read_at(0, payload.len()).await.expect("read full");
    assert_eq!(got.as_ref(), payload.as_slice());

    let got = file.read_at(6, 4).await.expect("read partial");
    assert_eq!(got.as_ref(), b"from");

    let got = file
        .read_at(payload.len() as u64 - 5, 100)
        .await
        .expect("read past eof");
    assert_eq!(got.len(), 5);

    let got = file
        .read_at(payload.len() as u64, 10)
        .await
        .expect("read at eof");
    assert!(got.is_empty());
}

#[tokio::test]
#[ignore = "requires docker"]
async fn minio_wrong_credentials_returns_error() {
    let fixture = MinioFixture::start().await.expect("start minio");
    let backend = backend(&fixture);
    let url = fixture.object_url("auth-test/object");

    backend
        .upload_bytes(&url, b"secret data".to_vec())
        .await
        .expect("upload");

    let bad_backend = backend_with_bad_credentials(&fixture);
    let file = bad_backend
        .open_with_size_hint(&url, None)
        .expect("construct file with bad credentials");
    let err = file
        .size()
        .await
        .expect_err("should fail with bad credentials");
    assert!(
        error_chain_contains(&err, "403")
            || error_chain_contains(&err, "Access denied")
            || error_chain_contains(&err, "PermissionDenied")
            || error_chain_contains(&err, "permission denied")
            || error_chain_contains(&err, "credential"),
        "expected auth error, got: {err}"
    );
}

#[tokio::test]
#[ignore = "requires docker"]
async fn minio_head_returns_correct_size() {
    let fixture = MinioFixture::start().await.expect("start minio");
    let backend = backend(&fixture);

    for &sz in &[0usize, 1, 512, 4096, 1024 * 1024] {
        let data = vec![0xABu8; sz];
        let url = fixture.object_url(&format!("size-test/{sz}"));
        backend
            .upload_bytes(&url, data)
            .await
            .unwrap_or_else(|e| panic!("upload size {sz} failed: {e}"));
        let file = backend.open_with_size_hint(&url, None).expect("open size");
        let got = file
            .size()
            .await
            .unwrap_or_else(|e| panic!("size {sz} failed: {e}"));
        assert_eq!(got, sz as u64, "size mismatch for {sz}");
    }
}

#[tokio::test]
#[ignore = "requires docker"]
async fn minio_upload_overwrite() {
    let fixture = MinioFixture::start().await.expect("start minio");
    let backend = backend(&fixture);
    let url = fixture.object_url("overwrite-test/obj");

    backend
        .upload_bytes(&url, b"version-1".to_vec())
        .await
        .expect("upload v1");
    backend
        .upload_bytes(&url, b"version-2-longer".to_vec())
        .await
        .expect("upload v2");

    let file = backend
        .open_with_size_hint(&url, None)
        .expect("open after overwrite");
    let size = file.size().await.expect("size");
    assert_eq!(size, b"version-2-longer".len() as u64);
    let got = file.read_at(0, size as usize).await.expect("read");
    assert_eq!(got.as_ref(), b"version-2-longer");
}

#[tokio::test]
#[ignore = "requires docker"]
async fn minio_read_nonexistent_object_returns_error() {
    let fixture = MinioFixture::start().await.expect("start minio");
    let backend = backend(&fixture);
    let url = fixture.object_url("does-not-exist/phantom");

    let file = backend
        .open_with_size_hint(&url, None)
        .expect("construct file for nonexistent object");
    let err = file
        .size()
        .await
        .expect_err("nonexistent object should fail");
    assert!(
        error_chain_contains(&err, "Object not found")
            || error_chain_contains(&err, "not found")
            || error_chain_contains(&err, "Not Found")
            || error_chain_contains(&err, "NotFound"),
        "expected not-found error, got: {err}"
    );
}

#[tokio::test]
#[ignore = "requires docker"]
async fn minio_upload_path_streams_a_file_larger_than_one_part() {
    let fixture = MinioFixture::start().await.expect("start minio");
    let backend = backend(&fixture);
    let url = fixture.object_url("stream-test/layer");

    // Larger than the part size, so the upload actually takes the multipart
    // path rather than a single PUT. A byte pattern that varies with
    // the offset would catch parts being uploaded in the wrong order.
    let size = 70 * 1024 * 1024usize;
    let temp = tempfile::NamedTempFile::new().expect("create upload source");
    let payload: Vec<u8> = (0..size).map(|offset| (offset % 251) as u8).collect();
    std::fs::write(temp.path(), &payload).expect("write upload source");

    backend
        .upload_path(&url, temp.path(), PROBE_PART_SIZE, PROBE_CONCURRENCY)
        .await
        .expect("stream upload to minio");

    let file = backend
        .open_with_size_hint(&url, None)
        .expect("open the streamed object");
    assert_eq!(file.size().await.expect("size"), size as u64);
    let head = file.read_at(0, 4096).await.expect("read the first chunk");
    assert_eq!(head.as_ref(), &payload[..4096]);
    // Straddles the first part boundary, where a mis-ordered or truncated part
    // would show up.
    let seam = PROBE_PART_SIZE - 2048;
    let got = file
        .read_at(seam as u64, 4096)
        .await
        .expect("read the seam");
    assert_eq!(got.as_ref(), &payload[seam..seam + 4096]);
}

#[tokio::test]
#[ignore = "requires docker"]
async fn minio_object_size_distinguishes_missing_from_empty() {
    let fixture = MinioFixture::start().await.expect("start minio");
    let backend = backend(&fixture);

    let missing = fixture.object_url("size-probe/absent");
    assert_eq!(
        backend.object_size(&missing).await.expect("probe missing"),
        None,
        "a missing object must be None rather than an error"
    );

    // An empty object exists and has length zero, which must not be confused
    // with absence: callers skip an upload on Some(_) and perform one on None.
    let empty = fixture.object_url("size-probe/empty");
    backend
        .upload_bytes(&empty, Vec::new())
        .await
        .expect("upload an empty object");
    assert_eq!(
        backend.object_size(&empty).await.expect("probe empty"),
        Some(0)
    );

    let present = fixture.object_url("size-probe/present");
    backend
        .upload_bytes(&present, b"twelve bytes".to_vec())
        .await
        .expect("upload");
    assert_eq!(
        backend.object_size(&present).await.expect("probe present"),
        Some(12)
    );
}
