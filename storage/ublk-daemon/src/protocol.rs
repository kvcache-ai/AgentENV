use std::path::PathBuf;

use anyhow::{Context, Result};
use overlaybd::config::{LayerConfig, UpperMode};
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
        /// Open a configured sparse upper read-only while retaining it as the
        /// top layer. Used by Temporal memory resumes.
        #[serde(default)]
        read_only_upper: bool,
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
        /// Whether source-owned state is consumed into the new runtime
        /// directory before opening the device.
        #[serde(default)]
        source_state_strategy: SourceStateStrategy,
    },
    Delete {
        dev_id: u32,
    },
    RestackSnapshot {
        dev_id: u32,
        output_layer_path: PathBuf,
        /// Merge this suffix of existing lowers before sealing the upper.
        compact_from: Option<usize>,
    },
    /// Flush a live image while excluding concurrent image I/O.
    FlushForCheckpoint {
        dev_id: u32,
    },
    /// Query daemon capabilities (e.g., dynamic resize support).
    GetFeatures,
    /// Report that the sandbox owning a memory-snapshot device finished
    /// booting (envd ready), releasing its held background downloads.
    /// `device_key` is the image.json path the device was opened with.
    NotifySandboxReady {
        device_key: String,
    },
    /// Arm a startup-pack first-touch recorder on an existing device and
    /// start the recording window state machine. When the window ends the
    /// daemon writes the recorded first-touch trace into `output`
    /// (atomically, via a `.tmp` sibling) and reports the result through
    /// `PackRecordingStatus`.
    StartPackRecording {
        dev_id: u32,
        /// Trace output path (the `memory-startup.trace` intermediate).
        output: PathBuf,
        max_pages: u32,
        min_window_ms: u64,
        quiet_ms: u64,
        max_window_ms: u64,
    },
    /// Poll the state of the pack recording running on `dev_id`.
    PackRecordingStatus {
        dev_id: u32,
    },
    /// Abort a pack recording (idempotent): detach the recorder, stop the
    /// window task, and remove any partial pack output.
    AbortPackRecording {
        dev_id: u32,
    },
    /// Best-effort: prefetch a v3 startup manifest for the memory image
    /// at `image_config`, binding the manifest's layers to the image's OSS
    /// lowers inside the cache of the `ImageService` for `global_config`.
    /// Registration is deduplicated by pack identity; the daemon reports
    /// failures in its log and always answers `Ok`.
    PrefetchStartupPack {
        image_config: PathBuf,
        global_config: PathBuf,
        url: String,
        pack_size: u64,
        index_sha256: String,
        mem_virtual_size: u64,
        timeout_secs: u64,
    },
    /// Acquire a warm overlaybd device from the pool.
    AcquireOverlaybd {
        image_config: PathBuf,
        global_config: PathBuf,
        /// Expected capacity; omit to use the published image capacity.
        virtual_size: Option<u64>,
        access_mode: AccessMode,
        #[serde(default)]
        read_only_upper: bool,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceStateStrategy {
    /// Reuse local state referenced by the source image config.
    #[default]
    Reuse,
    /// Transfer source-owned state into the new runtime. Upper files are
    /// renamed on the same filesystem, or copied then removed across mounts.
    Consume,
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

/// State of a daemon-side startup pack recording.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum PackRecordingState {
    Recording,
    Done {
        pages: u32,
        bytes: u64,
        remote_bytes: u64,
        path: PathBuf,
    },
    Failed {
        reason: String,
    },
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
        /// Configuration path used to open the acquired device, including a reused one.
        image_config_path: PathBuf,
    },
    Released,
    SizeUpdated,
    Features {
        flags: u64,
    },
    Deleted,
    RestackSnapshotCreated {
        descriptor: Option<LayerDescriptor>,
        /// Actual live lowers after restack, including any compaction.
        lowers: Vec<LayerConfig>,
        /// Best-effort device usage stats captured after the restack. Absent
        /// when produced by an older daemon or when computation failed.
        #[serde(default)]
        data_stat: Option<DataStat>,
        /// Guest ext4 used bytes probed from the device, when it looks like ext4.
        #[serde(default)]
        ext4_used_bytes: Option<u64>,
    },
    PackRecording {
        state: PackRecordingState,
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
pub struct RestackSnapshotResult {
    pub descriptor: Option<LayerDescriptor>,
    pub lowers: Vec<LayerConfig>,
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

    // ── Wire-format compatibility tests (fixed JSON from older peers) ───

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
            DaemonRequest::CreateOverlaybdRuntimeDevice {
                allow_shrink,
                source_state_strategy,
                ..
            } => {
                assert!(!allow_shrink);
                assert_eq!(source_state_strategy, SourceStateStrategy::Reuse);
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
            virtual_size: Some(1024 * 1024 * 1024),
            access_mode: AccessMode::Exclusive,
            read_only_upper: false,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: DaemonRequest = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonRequest::AcquireOverlaybd {
                virtual_size,
                access_mode,
                ..
            } => {
                assert_eq!(virtual_size, Some(1024 * 1024 * 1024));
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
            image_config_path: PathBuf::from("/image.json"),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: DaemonResponse = serde_json::from_str(&json).unwrap();
        match decoded {
            DaemonResponse::DeviceAcquired {
                dev_id,
                device_path,
                ..
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
                    read_only_upper: false,
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
