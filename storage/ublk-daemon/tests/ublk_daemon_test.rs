//! Integration tests for the `uvm-ublk-daemon` crate.
//!
//! These tests exercise the client-server RPC protocol using a lightweight
//! mock server that speaks the same length-prefixed JSON wire format. This
//! lets us thoroughly test client-side RPC logic, response mapping, and
//! error handling without requiring ublk hardware or kernel modules.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use overlaybd::config::UpperMode;
use tokio::net::UnixListener;
use tokio::sync::oneshot;

use uvm_ublk_daemon::protocol::{recv_message, send_message, DaemonRequest, DaemonResponse};
use uvm_ublk_daemon::{
    CreateOverlaybdRuntimeDeviceRequest, InvalidRequestError, RestackSnapshotTerminalFailure,
    UblkDaemonClient,
};

// ════════════════════════════════════════════════════════════════════════════
// Mock server infrastructure
// ════════════════════════════════════════════════════════════════════════════

/// A handler function that takes a request and returns a response.
type MockHandler = Box<dyn Fn(DaemonRequest) -> DaemonResponse + Send + Sync>;

/// A lightweight mock server that listens on a Unix socket and dispatches
/// incoming requests to a user-provided handler function.
struct MockServer {
    socket_path: PathBuf,
    _dir: tempfile::TempDir,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MockServer {
    /// Start a mock server with the given handler. Returns immediately;
    /// the server runs in a background task.
    async fn start(handler: MockHandler) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let handler = Arc::new(handler);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        match accept {
                            Ok((mut stream, _)) => {
                                let handler = Arc::clone(&handler);
                                tokio::spawn(async move {
                                    if let Ok(Some(request)) =
                                        recv_message::<DaemonRequest>(&mut stream).await
                                    {
                                        let response = handler(request);
                                        let _ = send_message(&mut stream, &response).await;
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });

        Self {
            socket_path,
            _dir: dir,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start a mock server that accepts a connection but never responds.
    async fn start_hanging() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        match accept {
                            Ok((_stream, _)) => {
                                // Hold the stream open but never respond.
                                // Keep it alive until shutdown.
                                tokio::time::sleep(Duration::from_secs(300)).await;
                            }
                            Err(_) => break,
                        }
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });

        Self {
            socket_path,
            _dir: dir,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start a mock server that accepts and immediately closes the connection.
    async fn start_drop_connection() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        match accept {
                            Ok((_stream, _)) => {
                                // Drop the stream immediately → EOF for client.
                            }
                            Err(_) => break,
                        }
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
        });

        Self {
            socket_path,
            _dir: dir,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Create a `UblkDaemonClient` pointing at this mock server.
    fn client(&self) -> Arc<UblkDaemonClient> {
        UblkDaemonClient::new_for_test(self.socket_path.clone(), false)
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Protocol integration tests
// ════════════════════════════════════════════════════════════════════════════

mod protocol_tests {
    use super::*;

    /// Test bidirectional exchange: client sends request, server sends response.
    #[tokio::test]
    async fn bidirectional_request_response() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("bidir.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        let sock_path_clone = sock_path.clone();
        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::UnixStream::connect(&sock_path_clone)
                .await
                .unwrap();
            let req = DaemonRequest::Delete { dev_id: 42 };
            send_message(&mut stream, &req).await.unwrap();

            let resp: DaemonResponse = recv_message(&mut stream).await.unwrap().unwrap();
            resp
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        let req: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
        assert!(matches!(req, DaemonRequest::Delete { dev_id: 42 }));

        let resp = DaemonResponse::Deleted;
        send_message(&mut stream, &resp).await.unwrap();

        let client_resp = client_task.await.unwrap();
        assert!(matches!(client_resp, DaemonResponse::Deleted));
    }

    /// Test sending a request with a long path (exercising larger payloads).
    #[tokio::test]
    async fn large_payload_path() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("large.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        // Create a path string with 100K characters.
        let long_path = "/".to_string() + &"a".repeat(100_000);

        let sock_path_clone = sock_path.clone();
        let long_path_clone = long_path.clone();
        let sender = tokio::spawn(async move {
            let mut stream = tokio::net::UnixStream::connect(&sock_path_clone)
                .await
                .unwrap();
            let req = DaemonRequest::CreateOverlaybd {
                image_config: PathBuf::from(&long_path_clone),
                global_config: PathBuf::from("/etc/overlaybd/global.json"),
            };
            send_message(&mut stream, &req).await.unwrap();
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        let msg: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
        sender.await.unwrap();

        match msg {
            DaemonRequest::CreateOverlaybd { image_config, .. } => {
                assert_eq!(image_config.to_str().unwrap().len(), long_path.len());
            }
            _ => panic!("unexpected variant"),
        }
    }

    /// Test concurrent clients sending on separate connections.
    #[tokio::test]
    async fn concurrent_clients() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("concurrent.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        let num_clients = 10;

        // Spawn N clients that each send a Delete request.
        let mut client_handles = Vec::new();
        for i in 0..num_clients {
            let path = sock_path.clone();
            client_handles.push(tokio::spawn(async move {
                let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
                let req = DaemonRequest::Delete { dev_id: i };
                send_message(&mut stream, &req).await.unwrap();
            }));
        }

        // Accept all N connections and verify.
        let mut received_ids = Vec::new();
        for _ in 0..num_clients {
            let (mut stream, _) = listener.accept().await.unwrap();
            let msg: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
            match msg {
                DaemonRequest::Delete { dev_id } => received_ids.push(dev_id),
                _ => panic!("unexpected variant"),
            }
        }

        for handle in client_handles {
            handle.await.unwrap();
        }

        received_ids.sort();
        let expected: Vec<u32> = (0..num_clients).collect();
        assert_eq!(received_ids, expected);
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Client RPC tests against mock server
// ════════════════════════════════════════════════════════════════════════════

mod client_tests {
    use super::*;

    // ── create_overlaybd ────────────────────────────────────────────────

    #[tokio::test]
    async fn create_overlaybd_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::CreateOverlaybd { .. } => DaemonResponse::DeviceCreated {
                dev_id: 5,
                device_path: PathBuf::from("/dev/ublkb5"),
            },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let (dev_id, path) = client
            .create_overlaybd(Path::new("/tmp/image.json"), Path::new("/global.json"))
            .await
            .unwrap();
        assert_eq!(dev_id, 5);
        assert_eq!(path, PathBuf::from("/dev/ublkb5"));
    }

    #[tokio::test]
    async fn create_overlaybd_error() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Error {
            message: "disk full".into(),
        }))
        .await;

        let client = server.client();
        let err = client
            .create_overlaybd(Path::new("/img.json"), Path::new("/global.json"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("disk full"),
            "error should contain message from daemon: {msg}"
        );
    }

    #[tokio::test]
    async fn create_overlaybd_unexpected_response() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Deleted)).await;

        let client = server.client();
        let err = client
            .create_overlaybd(Path::new("/img.json"), Path::new("/global.json"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unexpected"),
            "error should mention unexpected response: {msg}"
        );
    }

    #[tokio::test]
    async fn create_overlaybd_runtime_device_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::CreateOverlaybdRuntimeDevice {
                source_image_config,
                global_config,
                runtime_dir,
                read_only,
                runtime_upper_mode,
                requested_virtual_size,
                known_source_virtual_size,
                allow_shrink,
            } => {
                assert_eq!(source_image_config, PathBuf::from("/src/image.json"));
                assert_eq!(global_config, PathBuf::from("/global.json"));
                assert_eq!(runtime_dir, PathBuf::from("/work/overlaybd"));
                assert!(!read_only);
                assert_eq!(runtime_upper_mode, UpperMode::Sparse);
                assert_eq!(requested_virtual_size, None);
                assert_eq!(known_source_virtual_size, Some(8192));
                assert!(!allow_shrink);
                DaemonResponse::OverlaybdRuntimeDeviceCreated {
                    dev_id: 11,
                    device_path: PathBuf::from("/dev/ublkb11"),
                    actual_virtual_size: 8192,
                    runtime_image_config_path: PathBuf::from("/work/overlaybd/image.json"),
                }
            }
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let device = client
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: Path::new("/src/image.json"),
                global_config: Path::new("/global.json"),
                runtime_dir: Path::new("/work/overlaybd"),
                read_only: false,
                runtime_upper_mode: UpperMode::Sparse,
                requested_virtual_size: None,
                known_source_virtual_size: Some(8192),
                allow_shrink: false,
            })
            .await
            .unwrap();
        assert_eq!(device.dev_id, 11);
        assert_eq!(device.device_path, PathBuf::from("/dev/ublkb11"));
        assert_eq!(device.actual_virtual_size, 8192);
        assert_eq!(
            device.runtime_image_config_path,
            PathBuf::from("/work/overlaybd/image.json")
        );
    }

    #[tokio::test]
    async fn create_overlaybd_runtime_device_error() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::CreateOverlaybdRuntimeDevice { .. } => DaemonResponse::Error {
                message: "bad runtime".into(),
            },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let err = client
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: Path::new("/src/image.json"),
                global_config: Path::new("/global.json"),
                runtime_dir: Path::new("/work/overlaybd"),
                read_only: false,
                runtime_upper_mode: UpperMode::LogStructured,
                requested_virtual_size: None,
                known_source_virtual_size: None,
                allow_shrink: false,
            })
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("bad runtime"));
    }

    #[tokio::test]
    async fn create_overlaybd_runtime_device_preserves_invalid_request_type() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::InvalidRequest {
            message: "disk size is invalid".into(),
        }))
        .await;
        let err = server
            .client()
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: Path::new("/src/image.json"),
                global_config: Path::new("/global.json"),
                runtime_dir: Path::new("/work/overlaybd"),
                read_only: false,
                runtime_upper_mode: UpperMode::LogStructured,
                requested_virtual_size: None,
                known_source_virtual_size: None,
                allow_shrink: false,
            })
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<InvalidRequestError>().is_some());
    }

    #[tokio::test]
    async fn create_overlaybd_runtime_device_unexpected_response() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Deleted)).await;

        let client = server.client();
        let err = client
            .create_overlaybd_runtime_device(CreateOverlaybdRuntimeDeviceRequest {
                source_image_config: Path::new("/src/image.json"),
                global_config: Path::new("/global.json"),
                runtime_dir: Path::new("/work/overlaybd"),
                read_only: false,
                runtime_upper_mode: UpperMode::LogStructured,
                requested_virtual_size: None,
                known_source_virtual_size: None,
                allow_shrink: false,
            })
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("unexpected response"));
    }

    // ── delete ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn delete_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::Delete { .. } => DaemonResponse::Deleted,
            _ => DaemonResponse::Error {
                message: "unexpected".into(),
            },
        }))
        .await;

        let client = server.client();
        client.delete(7).await.unwrap();
    }

    #[tokio::test]
    async fn delete_error() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Error {
            message: "device 99 not found".into(),
        }))
        .await;

        let client = server.client();
        let err = client.delete(99).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("not found"), "error: {msg}");
    }

    #[tokio::test]
    async fn delete_unexpected_response() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::DeviceCreated {
            dev_id: 0,
            device_path: PathBuf::from("/dev/ublkb0"),
        }))
        .await;

        let client = server.client();
        let err = client.delete(1).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("unexpected"), "error: {msg}");
    }

    // ── snapshot ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn snapshot_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::RestackSnapshot { .. } => DaemonResponse::RestackSnapshotCreated {
                descriptor: Some(overlaybd::LayerDescriptor {
                    digest: "sha256:abc".to_string(),
                    size: 4096,
                }),
                data_stat: None,
                ext4_used_bytes: None,
            },
            _ => DaemonResponse::Error {
                message: "unexpected".into(),
            },
        }))
        .await;

        let client = server.client();
        let stats = client
            .restack_snapshot(2, Path::new("/snapshots/layer0"))
            .await
            .unwrap();
        assert_eq!(
            stats.descriptor,
            Some(overlaybd::LayerDescriptor {
                digest: "sha256:abc".to_string(),
                size: 4096,
            })
        );
    }

    #[tokio::test]
    async fn snapshot_error() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Error {
            message: "snapshot failed".into(),
        }))
        .await;

        let client = server.client();
        let err = client
            .restack_snapshot(2, Path::new("/out"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("snapshot failed"), "error: {msg}");
    }

    #[tokio::test]
    async fn snapshot_terminal_error() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::TerminalError {
            message: "sealed upper left live runtime mutated".into(),
        }))
        .await;

        let client = server.client();
        let err = client
            .restack_snapshot(2, Path::new("/out"))
            .await
            .unwrap_err();
        assert!(
            err.downcast_ref::<RestackSnapshotTerminalFailure>()
                .is_some(),
            "expected terminal restack failure, got: {err:#}"
        );
    }

    #[tokio::test]
    async fn snapshot_unexpected_response() {
        let server = MockServer::start(Box::new(|_| DaemonResponse::Deleted)).await;

        let client = server.client();
        let err = client
            .restack_snapshot(1, Path::new("/out"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("unexpected"), "error: {msg}");
    }

    // ── shutdown ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn shutdown_succeeds_even_without_response() {
        let server = MockServer::start_drop_connection().await;
        let client = server.client();

        // shutdown() is best-effort — should succeed even if daemon doesn't respond.
        client.shutdown().await.unwrap();
    }

    // ── connection error cases ──────────────────────────────────────────

    #[tokio::test]
    async fn connection_refused_no_server() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("nonexistent.sock");
        let client = UblkDaemonClient::new_for_test(sock_path, false);

        let err = client
            .create_overlaybd(Path::new("/img"), Path::new("/global.json"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("connect") || msg.contains("No such file"),
            "should fail with connection error: {msg}"
        );
    }

    #[tokio::test]
    async fn server_drops_connection_without_response() {
        let server = MockServer::start_drop_connection().await;
        let client = server.client();

        let err = client
            .create_overlaybd(Path::new("/img"), Path::new("/global.json"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        // The client should get an error about missing response or EOF.
        assert!(
            msg.contains("closed") || msg.contains("EOF") || msg.contains("response"),
            "should fail with connection closed error: {msg}"
        );
    }

    #[tokio::test]
    async fn rpc_timeout() {
        let server = MockServer::start_hanging().await;

        // Create client with a very short timeout by calling the private call method
        // indirectly. Since we can't change the timeout on public methods, we test
        // that the server hanging eventually hits the 30s default timeout.
        // For practical test speed, we just verify the hanging server doesn't
        // cause an immediate success.
        let client = server.client();

        // Use a tokio timeout shorter than the default 30s to verify the client
        // is actually blocked waiting.
        let result =
            tokio::time::timeout(Duration::from_millis(500), async { client.delete(0).await })
                .await;

        assert!(
            result.is_err(),
            "should time out because mock server never responds"
        );
    }

    #[tokio::test]
    async fn daemon_dead_prevents_all_rpcs() {
        // Even with a valid server running, a dead daemon flag should prevent calls.
        let server = MockServer::start(Box::new(|_| DaemonResponse::Deleted)).await;

        // Create client with daemon_dead=true.
        let client = UblkDaemonClient::new_for_test(server.socket_path.clone(), true);

        let err = client.delete(0).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("not running"), "error: {msg}");

        let err = client
            .create_overlaybd(Path::new("/img"), Path::new("/global.json"))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("not running"));

        let err = client
            .restack_snapshot(0, Path::new("/out"))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("not running"));
    }

    // ── request dispatch verification ───────────────────────────────────

    #[tokio::test]
    async fn server_receives_correct_request_fields() {
        use std::sync::Mutex;

        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_for_server = Arc::clone(&captured);

        let server = MockServer::start(Box::new(move |req| {
            // Capture the request for later inspection.
            captured_for_server.lock().unwrap().push(format!("{req:?}"));
            match req {
                DaemonRequest::CreateOverlaybd { .. } => DaemonResponse::DeviceCreated {
                    dev_id: 10,
                    device_path: PathBuf::from("/dev/ublkb10"),
                },
                DaemonRequest::Delete { .. } => DaemonResponse::Deleted,
                DaemonRequest::RestackSnapshot { .. } => DaemonResponse::RestackSnapshotCreated {
                    descriptor: None,
                    data_stat: None,
                    ext4_used_bytes: None,
                },
                DaemonRequest::Shutdown => DaemonResponse::Ok,
                DaemonRequest::GetFeatures => DaemonResponse::Features { flags: 0 },
                DaemonRequest::AcquireOverlaybd { .. } => DaemonResponse::DeviceAcquired {
                    dev_id: 99,
                    device_path: PathBuf::from("/dev/ublkb99"),
                },
                DaemonRequest::CreateOverlaybdRuntimeDevice { .. } => {
                    DaemonResponse::OverlaybdRuntimeDeviceCreated {
                        dev_id: 100,
                        device_path: PathBuf::from("/dev/ublkb100"),
                        actual_virtual_size: 4096,
                        runtime_image_config_path: PathBuf::from("/work/overlaybd/image.json"),
                    }
                }
                DaemonRequest::ReleaseOverlaybd { .. } => DaemonResponse::Released,
                DaemonRequest::UpdateSize { .. } => DaemonResponse::SizeUpdated,
                DaemonRequest::NotifySandboxReady { .. } => DaemonResponse::Ok,
            }
        }))
        .await;

        let client = server.client();

        client
            .create_overlaybd(Path::new("/config/img.json"), Path::new("/global.json"))
            .await
            .unwrap();
        client.delete(30).await.unwrap();
        client
            .restack_snapshot(40, Path::new("/snap/output"))
            .await
            .unwrap();
        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 3);

        assert!(requests[0].contains("CreateOverlaybd"));
        assert!(requests[0].contains("img.json"));
        assert!(requests[0].contains("global.json"));
        assert!(!requests[0].contains("dev_id"));

        assert!(requests[1].contains("Delete"));
        assert!(requests[1].contains("30"));

        assert!(requests[2].contains("RestackSnapshot"));
        assert!(requests[2].contains("40"));
        assert!(requests[2].contains("output"));
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Server tests (accept loop, shutdown, stale socket)
// ════════════════════════════════════════════════════════════════════════════

mod server_tests {
    use super::*;
    use uvm_ublk_daemon::UblkDaemonServer;

    use overlaybd::image_service::ImageService;
    use storage_util::io_ring::spawn_io_ring_worker;

    /// Helper to create a minimal `ImageService` for tests.
    ///
    /// We write a minimal global config JSON and construct from it. The
    /// image service won't be used for actual image operations in these
    /// tests — we only test the server's accept loop and shutdown.
    async fn test_image_service(dir: &Path) -> ImageService {
        let cache_dir = dir.join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let config_path = dir.join("global_config.json");
        let config = serde_json::json!({
            "registryFsVersion": "v2",
            "ioEngine": 0,
            "cacheConfig": {
                "cacheType": "file",
                "cacheDir": cache_dir.to_str().unwrap(),
                "cacheSizeGB": 1,
                "refillSize": 262144,
                "blockSize": 65536
            }
        });
        std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();
        ImageService::from_config_path(&config_path).await.unwrap()
    }

    #[tokio::test]
    async fn server_shutdown_via_request() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let (ctrl_ring, _handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let server = UblkDaemonServer::new(
            sock_path.clone(),
            ctrl_ring,
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        );

        // Run server in background.
        let server_task = tokio::spawn(async move { server.run().await });

        // Wait a bit for the server to start listening.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send a Shutdown request.
        let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
        send_message(&mut stream, &DaemonRequest::Shutdown)
            .await
            .unwrap();

        // Server should exit cleanly.
        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server should stop within 5s")
            .expect("task should not panic");
        assert!(result.is_ok(), "server should exit without error");

        // Socket file should be cleaned up.
        assert!(
            !sock_path.exists(),
            "socket file should be removed after shutdown"
        );
    }

    #[tokio::test]
    async fn server_shutdown_via_notify() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let (ctrl_ring, _handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            ctrl_ring,
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        // Wait for the server to start.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Request shutdown via the notify mechanism.
        server.request_shutdown();

        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server should stop within 5s")
            .expect("task should not panic");
        assert!(result.is_ok(), "server should exit without error");
    }

    #[tokio::test]
    async fn server_cleans_up_stale_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        // Create a stale socket file (not actively listened on).
        let _stale = std::os::unix::net::UnixListener::bind(&sock_path).unwrap();
        drop(_stale);
        // The file exists but nobody is listening.
        assert!(sock_path.exists(), "stale socket should exist");

        let image_service = test_image_service(dir.path()).await;
        let (ctrl_ring, _handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            ctrl_ring,
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        // Wait for server to start (it should clean up the stale socket and rebind).
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify we can connect to the new server.
        let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
        send_message(&mut stream, &DaemonRequest::Shutdown)
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("server should stop within 5s")
            .expect("task should not panic");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn server_rejects_in_use_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        // Create a socket that IS actively being listened on.
        let _active_listener = std::os::unix::net::UnixListener::bind(&sock_path).unwrap();

        let image_service = test_image_service(dir.path()).await;
        let (ctrl_ring, _handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let server = UblkDaemonServer::new(
            sock_path.clone(),
            ctrl_ring,
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        );

        // Server should fail because the socket is in use.
        let result = server.run().await;
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(msg.contains("in use"), "expected 'in use' error: {msg}");
    }

    #[tokio::test]
    async fn server_handles_client_disconnect() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let (ctrl_ring, _handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            ctrl_ring,
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect and disconnect without sending anything.
        {
            let _stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
            // Drop immediately.
        }

        // Give server a moment to handle the disconnection.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Server should still be running.
        assert!(
            !server_task.is_finished(),
            "server should survive client disconnect"
        );

        // Shut down cleanly.
        server.request_shutdown();
        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("should stop")
            .expect("no panic");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn server_handles_invalid_request() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let (ctrl_ring, _handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            ctrl_ring,
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send garbage data.
        {
            let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
            let garbage = b"not json at all";
            let len = garbage.len() as u32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(garbage).await.unwrap();
            stream.flush().await.unwrap();
            // Give server time to process.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Server should still be running (error logged, connection closed).
        assert!(
            !server_task.is_finished(),
            "server should survive invalid request"
        );

        server.request_shutdown();
        let result = tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("should stop")
            .expect("no panic");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn server_handles_delete_nonexistent_device() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let (ctrl_ring, _handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            ctrl_ring,
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send a Delete request for a device that doesn't exist.
        let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
        send_message(&mut stream, &DaemonRequest::Delete { dev_id: 999 })
            .await
            .unwrap();

        let resp: DaemonResponse = recv_message(&mut stream).await.unwrap().unwrap();
        match resp {
            DaemonResponse::Error { message } => {
                assert!(
                    message.contains("not found"),
                    "expected 'not found' in error: {message}"
                );
            }
            other => panic!("expected Error response, got: {other:?}"),
        }

        // Clean up.
        server.request_shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;
    }

    #[tokio::test]
    async fn server_handles_snapshot_nonexistent_device() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("server.sock");

        let image_service = test_image_service(dir.path()).await;
        let (ctrl_ring, _handle) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let server = Arc::new(UblkDaemonServer::new(
            sock_path.clone(),
            ctrl_ring,
            image_service,
            dir.path().join("resize-overlaybd-global.json"),
        ));

        let server_clone = Arc::clone(&server);
        let server_task = tokio::spawn(async move { server_clone.run().await });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut stream = tokio::net::UnixStream::connect(&sock_path).await.unwrap();
        send_message(
            &mut stream,
            &DaemonRequest::RestackSnapshot {
                dev_id: 888,
                output_layer_path: PathBuf::from("/tmp/snap"),
            },
        )
        .await
        .unwrap();

        let resp: DaemonResponse = recv_message(&mut stream).await.unwrap().unwrap();
        match resp {
            DaemonResponse::Error { message } => {
                assert!(
                    message.contains("not found"),
                    "expected 'not found' in error: {message}"
                );
            }
            other => panic!("expected Error response, got: {other:?}"),
        }

        server.request_shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;
    }

    // ── Pool RPC tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_features_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::GetFeatures => DaemonResponse::Features { flags: 0x0001 },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let flags = client.get_features().await.unwrap();
        assert_eq!(flags, 0x0001);
    }

    #[tokio::test]
    async fn acquire_overlaybd_exclusive_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::AcquireOverlaybd { .. } => DaemonResponse::DeviceAcquired {
                dev_id: 10,
                device_path: PathBuf::from("/dev/ublkb10"),
            },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let (dev_id, path) = client
            .acquire_overlaybd(
                Path::new("/tmp/image.json"),
                Path::new("/global.json"),
                1024 * 1024 * 1024, // 1GB
                uvm_ublk_daemon::AccessMode::Exclusive,
            )
            .await
            .unwrap();
        assert_eq!(dev_id, 10);
        assert_eq!(path, PathBuf::from("/dev/ublkb10"));
    }

    #[tokio::test]
    async fn acquire_overlaybd_shared_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::AcquireOverlaybd { access_mode, .. } => {
                assert_eq!(access_mode, uvm_ublk_daemon::AccessMode::Shared);
                DaemonResponse::DeviceAcquired {
                    dev_id: 20,
                    device_path: PathBuf::from("/dev/ublkb20"),
                }
            }
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let (dev_id, path) = client
            .acquire_overlaybd(
                Path::new("/tmp/mem.json"),
                Path::new("/global.json"),
                128 * 1024 * 1024, // 128MB
                uvm_ublk_daemon::AccessMode::Shared,
            )
            .await
            .unwrap();
        assert_eq!(dev_id, 20);
        assert_eq!(path, PathBuf::from("/dev/ublkb20"));
    }

    #[tokio::test]
    async fn release_overlaybd_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::ReleaseOverlaybd { dev_id } => {
                assert_eq!(dev_id, 15);
                DaemonResponse::Released
            }
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        client.release_overlaybd(15).await.unwrap();
    }

    #[tokio::test]
    async fn update_size_success() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::UpdateSize {
                dev_id,
                new_sectors,
            } => {
                assert_eq!(dev_id, 25);
                assert_eq!(new_sectors, 2048);
                DaemonResponse::SizeUpdated
            }
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        client.update_size(25, 2048).await.unwrap();
    }

    #[tokio::test]
    async fn acquire_overlaybd_error() {
        let server = MockServer::start(Box::new(|req| match req {
            DaemonRequest::AcquireOverlaybd { .. } => DaemonResponse::Error {
                message: "pool not enabled".into(),
            },
            _ => DaemonResponse::Error {
                message: "unexpected request".into(),
            },
        }))
        .await;

        let client = server.client();
        let result = client
            .acquire_overlaybd(
                Path::new("/tmp/image.json"),
                Path::new("/global.json"),
                1024 * 1024 * 1024,
                uvm_ublk_daemon::AccessMode::Exclusive,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("pool not enabled"));
    }
}
