use std::path::PathBuf;

use anyhow::{Context, Result};
use overlaybd::config::UpperMode;
use overlaybd::index_file::DataStat;
use overlaybd::LayerDescriptor;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// Maximum message size (16 MiB). Protects against corrupt length prefixes.
const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

// ── Request / Response ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DaemonRequest {
    /// Create a raw overlaybd ublk device from an already-materialized image
    /// config. This does not create runtime upper files or rewrite image.json.
    /// Sandbox rootfs and extra drives should use
    /// `CreateOverlaybdRuntimeDevice` instead.
    CreateOverlaybd {
        image_config: PathBuf,
        /// Path to the overlaybd global config JSON for this device.
        /// The daemon lazily creates an `ImageService` per unique config path.
        global_config: PathBuf,
    },
    /// Materialize an OverlayBD runtime directory and create/acquire the ublk
    /// device for it. This is the rootfs/extra-drive block-device path.
    CreateOverlaybdRuntimeDevice {
        source_image_config: PathBuf,
        global_config: PathBuf,
        runtime_dir: PathBuf,
        read_only: bool,
        #[serde(default = "default_runtime_upper_mode")]
        runtime_upper_mode: UpperMode,
        requested_virtual_size: Option<u64>,
        known_source_virtual_size: Option<u64>,
        #[serde(default)]
        allow_shrink: bool,
    },
    Delete {
        dev_id: u32,
    },
    RestackSnapshot {
        dev_id: u32,
        output_layer_path: PathBuf,
    },
    /// Query daemon capabilities (e.g., dynamic resize support).
    GetFeatures,
    /// Report that the sandbox owning a memory-snapshot device finished
    /// booting (envd ready), releasing its held background downloads.
    /// `device_key` is the image.json path the device was opened with.
    NotifySandboxReady {
        device_key: String,
    },
    /// Acquire a warm overlaybd device from the pool.
    AcquireOverlaybd {
        image_config: PathBuf,
        global_config: PathBuf,
        virtual_size: u64,
        access_mode: AccessMode,
    },
    /// Release an overlaybd device back to the pool.
    ReleaseOverlaybd {
        dev_id: u32,
    },
    /// Update the virtual size of a ublk device (requires UBLK_F_UPDATE_SIZE).
    UpdateSize {
        dev_id: u32,
        new_sectors: u64,
    },
    Shutdown,
}

fn default_runtime_upper_mode() -> UpperMode {
    UpperMode::LogStructured
}

const fn default_resize_timeout_secs() -> u64 {
    120
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResizeToolSpec {
    pub binary: PathBuf,
    #[serde(default)]
    pub lib_dir: Option<PathBuf>,
    #[serde(default = "default_resize_timeout_secs")]
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    /// Exclusive access (rootfs, snapshot resume).
    Exclusive,
    /// Shared access (memory snapshot, refcounted).
    Shared,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DaemonResponse {
    DeviceCreated {
        dev_id: u32,
        device_path: PathBuf,
    },
    OverlaybdRuntimeDeviceCreated {
        dev_id: u32,
        device_path: PathBuf,
        actual_virtual_size: u64,
        runtime_image_config_path: PathBuf,
    },
    DeviceAcquired {
        dev_id: u32,
        device_path: PathBuf,
    },
    Released,
    SizeUpdated,
    Features {
        flags: u64,
    },
    Deleted,
    RestackSnapshotCreated {
        descriptor: Option<LayerDescriptor>,
        /// Best-effort device usage stats captured after the restack. Absent
        /// when produced by an older daemon or when computation failed.
        #[serde(default)]
        data_stat: Option<DataStat>,
        /// Guest ext4 used bytes probed from the device, when it looks like ext4.
        #[serde(default)]
        ext4_used_bytes: Option<u64>,
    },
    Ok,
    TerminalError {
        message: String,
    },
    InvalidRequest {
        message: String,
    },
    Error {
        message: String,
    },
}

// ── Wire helpers ────────────────────────────────────────────────────────────

/// Client-side outcome of a restack snapshot RPC.
#[derive(Debug, Clone, Default)]
pub struct RestackSnapshotStats {
    pub descriptor: Option<LayerDescriptor>,
    pub data_stat: Option<DataStat>,
    pub ext4_used_bytes: Option<u64>,
}

/// Send a length-prefixed JSON message over a Unix stream.
///
/// Wire format: 4-byte big-endian length prefix followed by JSON bytes.
pub async fn send_message<T: Serialize>(stream: &mut UnixStream, msg: &T) -> Result<()> {
    let payload = serde_json::to_vec(msg).context("serialize daemon message")?;
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .context("write message length prefix")?;
    stream
        .write_all(&payload)
        .await
        .context("write message body")?;
    stream.flush().await.context("flush message")?;
    Ok(())
}

/// Receive a length-prefixed JSON message from a Unix stream.
///
/// Returns `Ok(None)` on clean EOF (peer closed without sending).
pub async fn recv_message<T: DeserializeOwned>(stream: &mut UnixStream) -> Result<Option<T>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e).context("read message length prefix"),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_MESSAGE_SIZE {
        anyhow::bail!("daemon message too large: {len} bytes (max {MAX_MESSAGE_SIZE})");
    }
    let mut payload = vec![0u8; len as usize];
    stream
        .read_exact(&mut payload)
        .await
        .context("read message body")?;
    let msg = serde_json::from_slice(&payload).context("deserialize daemon message")?;
    Ok(Some(msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Existing serde round-trip tests ─────────────────────────────────

    #[test]
    fn request_serialization_round_trip() {
        let req = DaemonRequest::CreateOverlaybd {
            image_config: PathBuf::from("/tmp/image.json"),
            global_config: PathBuf::from("/etc/overlaybd/global.json"),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: DaemonRequest = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonRequest::CreateOverlaybd { image_config, .. } => {
                assert_eq!(image_config, PathBuf::from("/tmp/image.json"));
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn response_serialization_round_trip() {
        let resp = DaemonResponse::DeviceCreated {
            dev_id: 42,
            device_path: PathBuf::from("/dev/ublkb0"),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonResponse::DeviceCreated {
                dev_id,
                device_path,
            } => {
                assert_eq!(dev_id, 42);
                assert_eq!(device_path, PathBuf::from("/dev/ublkb0"));
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn error_response_round_trip() {
        let resp = DaemonResponse::Error {
            message: "something went wrong".into(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonResponse::Error { message } => assert_eq!(message, "something went wrong"),
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn terminal_error_response_round_trip() {
        let resp = DaemonResponse::TerminalError {
            message: "live state mutated".into(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonResponse::TerminalError { message } => {
                assert_eq!(message, "live state mutated")
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[tokio::test]
    async fn send_recv_round_trip_over_unix_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let sock_path_clone = sock_path.clone();
        let sender = tokio::spawn(async move {
            let mut stream = UnixStream::connect(&sock_path_clone).await.unwrap();
            let req = DaemonRequest::Delete { dev_id: 7 };
            send_message(&mut stream, &req).await.unwrap();
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        let msg: Option<DaemonRequest> = recv_message(&mut stream).await.unwrap();
        sender.await.unwrap();

        match msg {
            Some(DaemonRequest::Delete { dev_id }) => assert_eq!(dev_id, 7),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── New serde round-trip tests for pool RPCs ────────────────────────

    #[test]
    fn request_get_features_round_trip() {
        let req = DaemonRequest::GetFeatures;
        let json = serde_json::to_string(&req).unwrap();
        let decoded: DaemonRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded, DaemonRequest::GetFeatures));
    }

    #[test]
    fn request_create_overlaybd_runtime_device_round_trip() {
        let req = DaemonRequest::CreateOverlaybdRuntimeDevice {
            source_image_config: PathBuf::from("/src/image.json"),
            global_config: PathBuf::from("/global.json"),
            runtime_dir: PathBuf::from("/work/overlaybd"),
            read_only: false,
            runtime_upper_mode: UpperMode::Sparse,
            requested_virtual_size: Some(8192),
            known_source_virtual_size: Some(8192),
            allow_shrink: true,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: DaemonRequest = serde_json::from_str(&json).unwrap();
        match decoded {
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
                assert_eq!(requested_virtual_size, Some(8192));
                assert_eq!(known_source_virtual_size, Some(8192));
                assert!(allow_shrink);
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn runtime_resize_fields_default_for_old_requests() {
        let json = r#"{
            "kind":"create_overlaybd_runtime_device",
            "source_image_config":"/src/image.json",
            "global_config":"/global.json",
            "runtime_dir":"/work/overlaybd",
            "read_only":false,
            "requested_virtual_size":null,
            "known_source_virtual_size":null
        }"#;
        let decoded: DaemonRequest = serde_json::from_str(json).unwrap();
        match decoded {
            DaemonRequest::CreateOverlaybdRuntimeDevice { allow_shrink, .. } => {
                assert!(!allow_shrink);
            }
            other => panic!("unexpected request: {other:?}"),
        }
    }

    #[test]
    fn resize_tool_timeout_defaults_for_old_requests() {
        let spec: ResizeToolSpec =
            serde_json::from_str(r#"{"binary":"/opt/overlaybd-resize","lib_dir":null}"#).unwrap();
        assert_eq!(spec.timeout_secs, 120);
    }

    #[test]
    fn request_acquire_overlaybd_round_trip() {
        let req = DaemonRequest::AcquireOverlaybd {
            image_config: PathBuf::from("/img.json"),
            global_config: PathBuf::from("/global.json"),
            virtual_size: 1024 * 1024 * 1024,
            access_mode: AccessMode::Exclusive,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: DaemonRequest = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonRequest::AcquireOverlaybd {
                virtual_size,
                access_mode,
                ..
            } => {
                assert_eq!(virtual_size, 1024 * 1024 * 1024);
                assert_eq!(access_mode, AccessMode::Exclusive);
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn request_release_overlaybd_round_trip() {
        let req = DaemonRequest::ReleaseOverlaybd { dev_id: 7 };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: DaemonRequest = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonRequest::ReleaseOverlaybd { dev_id } => assert_eq!(dev_id, 7),
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn request_update_size_round_trip() {
        let req = DaemonRequest::UpdateSize {
            dev_id: 3,
            new_sectors: 2048,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: DaemonRequest = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonRequest::UpdateSize {
                dev_id,
                new_sectors,
            } => {
                assert_eq!(dev_id, 3);
                assert_eq!(new_sectors, 2048);
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn response_device_acquired_round_trip() {
        let resp = DaemonResponse::DeviceAcquired {
            dev_id: 5,
            device_path: PathBuf::from("/dev/ublkb5"),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonResponse::DeviceAcquired {
                dev_id,
                device_path,
            } => {
                assert_eq!(dev_id, 5);
                assert_eq!(device_path, PathBuf::from("/dev/ublkb5"));
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn response_overlaybd_runtime_device_created_round_trip() {
        let resp = DaemonResponse::OverlaybdRuntimeDeviceCreated {
            dev_id: 7,
            device_path: PathBuf::from("/dev/ublkb7"),
            actual_virtual_size: 4096,
            runtime_image_config_path: PathBuf::from("/work/overlaybd/image.json"),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonResponse::OverlaybdRuntimeDeviceCreated {
                dev_id,
                device_path,
                actual_virtual_size,
                runtime_image_config_path,
            } => {
                assert_eq!(dev_id, 7);
                assert_eq!(device_path, PathBuf::from("/dev/ublkb7"));
                assert_eq!(actual_virtual_size, 4096);
                assert_eq!(
                    runtime_image_config_path,
                    PathBuf::from("/work/overlaybd/image.json")
                );
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn response_released_round_trip() {
        let resp = DaemonResponse::Released;
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded, DaemonResponse::Released));
    }

    #[test]
    fn response_size_updated_round_trip() {
        let resp = DaemonResponse::SizeUpdated;
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded, DaemonResponse::SizeUpdated));
    }

    #[test]
    fn response_features_round_trip() {
        let resp = DaemonResponse::Features { flags: 0x1234 };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonResponse::Features { flags } => assert_eq!(flags, 0x1234),
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn access_mode_serde_round_trip() {
        let exclusive = AccessMode::Exclusive;
        let json = serde_json::to_string(&exclusive).unwrap();
        assert_eq!(json, r#""exclusive""#);
        let decoded: AccessMode = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, AccessMode::Exclusive);

        let shared = AccessMode::Shared;
        let json = serde_json::to_string(&shared).unwrap();
        assert_eq!(json, r#""shared""#);
        let decoded: AccessMode = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, AccessMode::Shared);
    }

    // ── New serde round-trip tests for remaining variants ───────────────

    #[test]
    fn request_restack_snapshot_round_trip() {
        let req = DaemonRequest::RestackSnapshot {
            dev_id: 5,
            output_layer_path: PathBuf::from("/snapshots/layer0"),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: DaemonRequest = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonRequest::RestackSnapshot {
                dev_id,
                output_layer_path,
            } => {
                assert_eq!(dev_id, 5);
                assert_eq!(output_layer_path, PathBuf::from("/snapshots/layer0"));
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn request_shutdown_round_trip() {
        let req = DaemonRequest::Shutdown;
        let json = serde_json::to_string(&req).unwrap();
        let decoded: DaemonRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded, DaemonRequest::Shutdown));
    }

    #[test]
    fn response_deleted_round_trip() {
        let resp = DaemonResponse::Deleted;
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded, DaemonResponse::Deleted));
    }

    #[test]
    fn response_ok_round_trip() {
        let resp = DaemonResponse::Ok;
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(decoded, DaemonResponse::Ok));
    }

    #[test]
    fn response_restack_snapshot_created_round_trip() {
        let resp = DaemonResponse::RestackSnapshotCreated {
            descriptor: Some(LayerDescriptor {
                digest: "sha256:abc".to_string(),
                size: 4096,
            }),
            data_stat: Some(DataStat {
                total_data_size: 8192,
                valid_data_size: 4096,
            }),
            ext4_used_bytes: Some(2048),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonResponse::RestackSnapshotCreated {
                descriptor,
                data_stat,
                ext4_used_bytes,
            } => {
                assert_eq!(
                    descriptor,
                    Some(LayerDescriptor {
                        digest: "sha256:abc".to_string(),
                        size: 4096,
                    })
                );
                assert_eq!(
                    data_stat,
                    Some(DataStat {
                        total_data_size: 8192,
                        valid_data_size: 4096,
                    })
                );
                assert_eq!(ext4_used_bytes, Some(2048));
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn response_restack_snapshot_created_without_stats_round_trip() {
        let resp = DaemonResponse::RestackSnapshotCreated {
            descriptor: None,
            data_stat: None,
            ext4_used_bytes: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            decoded,
            DaemonResponse::RestackSnapshotCreated {
                descriptor: None,
                ..
            }
        ));
    }

    #[test]
    fn response_restack_snapshot_created_from_older_daemon() {
        // Rolling upgrades: an old daemon's response carries no stats fields.
        let json = r#"{"status":"restack_snapshot_created","descriptor":null}"#;
        let decoded: DaemonResponse = serde_json::from_str(json).unwrap();
        match decoded {
            DaemonResponse::RestackSnapshotCreated {
                descriptor,
                data_stat,
                ext4_used_bytes,
            } => {
                assert!(descriptor.is_none());
                assert!(data_stat.is_none());
                assert!(ext4_used_bytes.is_none());
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    // ── Wire protocol edge case tests ──────────────────────────────────

    #[tokio::test]
    async fn send_recv_multiple_messages_in_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("multi.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let sock_path_clone = sock_path.clone();
        let sender = tokio::spawn(async move {
            let mut stream = UnixStream::connect(&sock_path_clone).await.unwrap();
            let messages: Vec<DaemonRequest> = vec![
                DaemonRequest::Delete { dev_id: 1 },
                DaemonRequest::Shutdown,
                DaemonRequest::CreateOverlaybd {
                    image_config: PathBuf::from("/img.json"),
                    global_config: PathBuf::from("/etc/overlaybd/global.json"),
                },
            ];
            for msg in &messages {
                send_message(&mut stream, msg).await.unwrap();
            }
        });

        let (mut stream, _) = listener.accept().await.unwrap();

        // Receive all three messages in order.
        let msg1: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
        assert!(matches!(msg1, DaemonRequest::Delete { dev_id: 1 }));

        let msg2: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
        assert!(matches!(msg2, DaemonRequest::Shutdown));

        let msg3: DaemonRequest = recv_message(&mut stream).await.unwrap().unwrap();
        match msg3 {
            DaemonRequest::CreateOverlaybd { image_config, .. } => {
                assert_eq!(image_config, PathBuf::from("/img.json"));
            }
            _ => panic!("unexpected variant"),
        }

        sender.await.unwrap();
    }

    #[tokio::test]
    async fn recv_clean_eof_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("eof.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let sock_path_clone = sock_path.clone();
        let sender = tokio::spawn(async move {
            let _stream = UnixStream::connect(&sock_path_clone).await.unwrap();
            // Drop immediately without sending anything.
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        sender.await.unwrap();

        // Peer closed without sending → should return Ok(None).
        let msg: Option<DaemonRequest> = recv_message(&mut stream).await.unwrap();
        assert!(msg.is_none());
    }

    #[tokio::test]
    async fn message_too_large_rejected() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("large.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let sock_path_clone = sock_path.clone();
        let sender = tokio::spawn(async move {
            let mut stream = UnixStream::connect(&sock_path_clone).await.unwrap();
            // Send a length prefix that exceeds MAX_MESSAGE_SIZE (16 MiB + 1).
            let huge_len: u32 = MAX_MESSAGE_SIZE + 1;
            stream.write_all(&huge_len.to_be_bytes()).await.unwrap();
            // Don't need to send actual payload — recv should reject based on length.
            // Keep stream alive so the receiver can read the prefix.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        let result: Result<Option<DaemonRequest>> = recv_message(&mut stream).await;
        assert!(result.is_err());
        let err_msg = format!("{:#}", result.unwrap_err());
        assert!(
            err_msg.contains("too large"),
            "expected 'too large' in error: {err_msg}"
        );

        sender.abort();
    }

    #[tokio::test]
    async fn invalid_json_deserialization_error() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("badjson.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let sock_path_clone = sock_path.clone();
        let sender = tokio::spawn(async move {
            let mut stream = UnixStream::connect(&sock_path_clone).await.unwrap();
            // Send a valid length prefix but invalid JSON payload.
            let payload = b"this is not valid json!!!";
            let len = payload.len() as u32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(payload).await.unwrap();
            stream.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        let result: Result<Option<DaemonRequest>> = recv_message(&mut stream).await;
        assert!(result.is_err());
        let err_msg = format!("{:#}", result.unwrap_err());
        assert!(
            err_msg.contains("deserialize"),
            "expected 'deserialize' in error: {err_msg}"
        );

        sender.abort();
    }
}
