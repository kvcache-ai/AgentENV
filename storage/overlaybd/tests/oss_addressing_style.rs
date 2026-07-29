//! Wire-level proof that `ossConfig.defaultAddressingStyle` propagates from the
//! backend config all the way into the HTTP requests issued for remote layer
//! reads. No docker or DNS setup required.
//!
//! The setup is chosen so that auto-detection and the explicit override
//! disagree: with endpoint `http://127.0.0.1:{port}` and bucket `127`, the
//! endpoint host starts with `"127."` and auto-detection therefore picks
//! virtual-host style. An explicit `"path"` override must flip the request to
//! path style (`GET /127/<key>` against the plain endpoint host). If the
//! override were dropped anywhere along the way, the client would target the
//! unresolvable virtual host `127.127.0.0.1` and the recorder below would never
//! see a path-style request.

use overlaybd::backend::oss::OssBackend;
use overlaybd::config::OssConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn config_with_style(endpoint: String, style: &str) -> OssConfig {
    OssConfig {
        enable: true,
        access_key_id: String::new(),
        secret_access_key: String::new(),
        security_token: String::new(),
        credential_process: String::new(),
        default_region: "auto".to_string(),
        default_endpoint: endpoint,
        default_addressing_style: style.to_string(),
        timeout_secs: 5,
        retry_count: 1,
    }
}

/// Accept one connection, capture the request head, and answer with an error
/// status; the test only cares about what the request looked like.
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
    let _ = socket
        .write_all(
            b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        )
        .await;
    head
}

#[tokio::test]
async fn explicit_path_override_reaches_the_wire() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind recorder listener");
    let port = listener.local_addr().expect("recorder addr").port();
    let recorder = tokio::spawn(record_one_request(listener));

    let config = config_with_style(format!("http://127.0.0.1:{port}"), "path");
    let backend = OssBackend::new(&config).expect("create oss backend");
    let file = backend
        .open_with_size_hint("s3://127/layers/explicit-style-object", None)
        .expect("open remote layer file");

    // One bound covers the whole interaction: the read (which fails with the
    // recorder's 500 response; only the request matters) and the capture.
    let head = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let _ = file.read_at(0, 4).await;
        recorder.await.expect("recorder task")
    })
    .await
    .expect("no request reached the recorder — was the addressing override dropped?");
    let request_line = head.lines().next().unwrap_or_default().to_string();
    assert!(
        request_line.contains("/127/layers/explicit-style-object"),
        "expected a path-style request for bucket '127', got: {request_line}"
    );
    assert!(
        head.lines()
            .any(|line| line.to_ascii_lowercase() == format!("host: 127.0.0.1:{port}")),
        "expected the plain endpoint host, got request head: {head}"
    );
}

#[test]
fn invalid_addressing_style_is_rejected() {
    let config = config_with_style("http://127.0.0.1:9000".to_string(), "bogus");
    let err = OssBackend::new(&config).expect_err("invalid style must fail");
    assert!(
        err.to_string().contains("defaultAddressingStyle"),
        "unexpected error: {err}"
    );
}
