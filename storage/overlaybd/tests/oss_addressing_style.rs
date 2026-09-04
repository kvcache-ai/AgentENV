//! Wire-level proof that `ossConfig.defaultAddressingStyle` propagates from the
//! backend config all the way into the HTTP requests issued for remote layer
//! reads. No docker or DNS setup required.
//!
//! The setup is chosen so that auto-detection and the explicit override
//! disagree: with endpoint `http://test-bucket.localhost:{port}` and bucket
//! `test-bucket`, auto-detection picks virtual-host style. An explicit `"path"`
//! override must strip the bucket prefix from the endpoint and send
//! `GET /test-bucket/<key>` to `localhost`. If the override were dropped, the
//! client would instead target the bucket-qualified host and the request
//! assertions would fail.

use overlaybd::backend::oss::OssBackend;
use overlaybd::config::OssConfig;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Accept one connection, capture the request head, and answer with a valid
/// four-byte range response.
async fn record_one_request(listener: TcpListener) -> String {
    let (mut socket, _) = listener.accept().await.expect("accept recorder connection");
    let mut buf = vec![0u8; 8192];
    // A single read may return a partial head, so keep reading until the
    // header terminator, EOF, or the buffer limit.
    let mut n = 0;
    while n < buf.len() && !buf[..n].windows(4).any(|window| window == b"\r\n\r\n") {
        let read = socket.read(&mut buf[n..]).await.expect("read request head");
        if read == 0 {
            break;
        }
        n += read;
    }
    let head = String::from_utf8_lossy(&buf[..n]).into_owned();
    socket
        .write_all(
            b"HTTP/1.1 206 Partial Content\r\ncontent-length: 4\r\ncontent-range: bytes 0-3/4\r\nconnection: close\r\n\r\ntest",
        )
        .await
        .expect("write range response");
    head
}

#[tokio::test]
async fn explicit_path_override_reaches_the_wire() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind recorder listener");
    let port = listener.local_addr().expect("recorder addr").port();
    let recorder = tokio::spawn(record_one_request(listener));

    let config = OssConfig {
        enable: true,
        access_key_id: "test-access-key".to_string(),
        secret_access_key: "test-secret-key".to_string(),
        default_region: "auto".to_string(),
        default_endpoint: format!("http://test-bucket.localhost:{port}"),
        default_addressing_style: "path".to_string(),
        timeout_secs: 5,
        retry_count: 1,
        ..Default::default()
    };
    let backend = OssBackend::new(&config).expect("create oss backend");
    let file = backend
        .open_with_size_hint("s3://test-bucket/layers/explicit-style-object", Some(4))
        .expect("open remote layer file");

    // A successful response avoids coupling this propagation test to the S3
    // client's retry behavior for synthetic error responses.
    let (data, head) = tokio::time::timeout(Duration::from_secs(30), async {
        let data = file.read_at(0, 4).await.expect("read recorded object");
        let head = recorder.await.expect("recorder task");
        (data, head)
    })
    .await
    .expect("OSS request did not complete — was the addressing override dropped?");
    assert_eq!(data.as_ref(), b"test");
    let request_line = head.lines().next().unwrap_or_default();
    assert!(
        request_line.contains("/test-bucket/layers/explicit-style-object"),
        "expected a path-style request for bucket 'test-bucket', got: {request_line}"
    );
    assert!(
        request_line.starts_with("GET "),
        "expected a range GET request, got: {request_line}"
    );
    let expected_host = format!("Host: localhost:{port}");
    assert!(
        head.lines()
            .any(|line| line.eq_ignore_ascii_case(&expected_host)),
        "expected the plain endpoint host, got request head: {head}"
    );
}
