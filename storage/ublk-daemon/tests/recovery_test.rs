//! Privileged tests for ublk device recovery across daemon restarts.
//!
//! These spawn the real `uvm-ublk-daemon` and need `/dev/ublk-control` plus
//! `CAP_SYS_ADMIN` (run through `make test-ublk`). They skip otherwise.

use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use overlaybd::config::{GlobalConfig, ImageConfig, UpperConfig, UpperMode};
use overlaybd::helper::prepare_runtime_upper;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::net::UnixStream;
use uvm_ublk_daemon::protocol::{recv_message, send_message};
use uvm_ublk_daemon::{AccessMode, DaemonRequest, DaemonResponse, DeviceRole, DeviceSpec};

const BLOCK: usize = 4096;
const DEVICE_SIZE: u64 = 64 << 20;
const WRITERS: usize = 4;
const BLOCKS_PER_WRITER: u64 = 64;

fn ublk_usable() -> bool {
    uvm_ublk::ublk_available()
        && std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/ublk-control")
            .is_ok()
        && linux_cap::has_effective_capabilities(&[linux_cap::CAP_SYS_ADMIN]).unwrap_or(false)
}

/// Serializes the tests. The kernel hands a deleted device's id to the next
/// device created, so a test waiting for its device to disappear could
/// otherwise see another test's new device under the same id.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Skip the test without ublk access; otherwise evaluate to a guard that
/// keeps other tests from running until it is dropped.
macro_rules! require_ublk {
    () => {{
        if !ublk_usable() {
            eprintln!("skipping: ublk or CAP_SYS_ADMIN unavailable");
            return;
        }
        SERIAL.lock().await
    }};
}

struct Env {
    dir: TempDir,
    global_config: PathBuf,
}

impl Env {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let mut global = GlobalConfig {
            enable_audit: false,
            log_path: String::new(),
            audit_path: String::new(),
            credential_file_path: String::new(),
            nr_io_rings: 1,
            ..GlobalConfig::default()
        };
        global.cache_config.cache_dir = dir.path().join("cache").to_string_lossy().into_owned();
        let global_config = dir.path().join("global.json");
        std::fs::write(&global_config, serde_json::to_vec(&global).unwrap()).unwrap();
        Self { dir, global_config }
    }

    /// A writable log-structured image with no lowers.
    fn image(&self, name: &str) -> PathBuf {
        let data = self.dir.path().join(format!("{name}.data"));
        let index = self.dir.path().join(format!("{name}.index"));
        prepare_runtime_upper(&data, Some(&index), DEVICE_SIZE, UpperMode::LogStructured).unwrap();
        let config = ImageConfig {
            lowers: Vec::new(),
            upper: UpperConfig {
                mode: Some(UpperMode::LogStructured),
                data: data.to_string_lossy().into_owned(),
                index: index.to_string_lossy().into_owned(),
                ..Default::default()
            },
            ..Default::default()
        };
        let path = self.dir.path().join(format!("{name}.json"));
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        path
    }

    fn socket(&self) -> PathBuf {
        self.dir.path().join("daemon.sock")
    }

    async fn spawn_daemon(&self, app_config: Option<&Path>) -> tokio::process::Child {
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_uvm-ublk-daemon"));
        cmd.arg("--socket-path")
            .arg(self.socket())
            .arg("--global-config")
            .arg(&self.global_config)
            .arg("--resize-global-config")
            .arg(&self.global_config)
            .arg("--metrics-listen-addr")
            .arg("")
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true);
        if let Some(path) = app_config {
            cmd.arg("--config").arg(path);
        }
        let mut child = cmd.spawn().unwrap();
        let mut lines = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
        let ready = tokio::time::timeout(Duration::from_secs(30), lines.next_line())
            .await
            .expect("daemon ready timeout")
            .unwrap();
        assert_eq!(ready.as_deref(), Some("ready"));
        child
    }

    async fn call(&self, request: DaemonRequest) -> DaemonResponse {
        let mut stream = UnixStream::connect(self.socket()).await.unwrap();
        send_message(&mut stream, &request).await.unwrap();
        recv_message(&mut stream).await.unwrap().unwrap()
    }

    async fn create(&self, image_config: &Path) -> (u32, PathBuf) {
        match self
            .call(DaemonRequest::CreateOverlaybd {
                image_config: image_config.to_path_buf(),
                global_config: self.global_config.clone(),
            })
            .await
        {
            DaemonResponse::DeviceCreated {
                dev_id,
                device_path,
            } => (dev_id, device_path),
            other => panic!("unexpected create response: {other:?}"),
        }
    }

    fn spec(&self, dev_id: u32, image_config: &Path) -> DeviceSpec {
        DeviceSpec {
            dev_id,
            image_config: image_config.to_path_buf(),
            global_config: self.global_config.clone(),
            role: DeviceRole::Raw,
        }
    }

    async fn detach(&self, mut child: tokio::process::Child) {
        assert!(matches!(
            self.call(DaemonRequest::Detach).await,
            DaemonResponse::Ok
        ));
        let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
            .await
            .expect("detached daemon did not exit")
            .unwrap();
        assert!(status.success(), "detached daemon exited with {status}");
    }

    async fn recover(&self, devices: Vec<DeviceSpec>) -> Vec<u32> {
        match self.call(DaemonRequest::Recover { devices }).await {
            DaemonResponse::Recovered { failed } => {
                failed.into_iter().map(|failed| failed.dev_id).collect()
            }
            other => panic!("unexpected recover response: {other:?}"),
        }
    }
}

fn pattern(fill: u64) -> Vec<u8> {
    fill.to_le_bytes().repeat(BLOCK / 8)
}

/// A buffer holding one block of `pattern(fill)` at an O_DIRECT-aligned
/// offset, returned with that offset.
fn aligned_block(fill: u64) -> (Vec<u8>, usize) {
    let mut buf = vec![0u8; BLOCK * 2];
    let offset = buf.as_ptr().align_offset(BLOCK);
    buf[offset..offset + BLOCK].copy_from_slice(&pattern(fill));
    (buf, offset)
}

fn open_direct(path: &Path) -> std::fs::File {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
        .unwrap()
}

/// O_DIRECT writers on disjoint block ranges, each recording the last value
/// written to every block, I/O errors, and the longest single write.
struct Writers {
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<(Vec<u64>, usize, Duration)>>,
}

impl Writers {
    fn start(device: &Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let threads = (0..WRITERS as u64)
            .map(|writer| {
                let stop = Arc::clone(&stop);
                let file = open_direct(device);
                std::thread::spawn(move || {
                    let mut last = vec![0u64; BLOCKS_PER_WRITER as usize];
                    let mut errors = 0;
                    let mut longest = Duration::ZERO;
                    let mut seq = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        seq += 1;
                        let block = seq % BLOCKS_PER_WRITER;
                        let value = (writer << 48) | seq;
                        let (buf, offset) = aligned_block(value);
                        let started = Instant::now();
                        let result = file.write_at(
                            &buf[offset..offset + BLOCK],
                            (writer * BLOCKS_PER_WRITER + block) * BLOCK as u64,
                        );
                        longest = longest.max(started.elapsed());
                        match result {
                            Ok(_) => last[block as usize] = value,
                            Err(_) => errors += 1,
                        }
                    }
                    (last, errors, longest)
                })
            })
            .collect();
        Self { stop, threads }
    }

    /// Stop the writers, assert no write failed, and verify every block
    /// holds the last value written to it.
    fn finish_and_verify(self, device: &Path) -> Duration {
        self.stop.store(true, Ordering::Relaxed);
        let file = open_direct(device);
        let mut longest = Duration::ZERO;
        for (writer, thread) in self.threads.into_iter().enumerate() {
            let (last, errors, writer_longest) = thread.join().unwrap();
            assert_eq!(errors, 0, "writer {writer} saw I/O errors");
            longest = longest.max(writer_longest);
            for (block, expected) in last.into_iter().enumerate() {
                let (mut buf, offset) = aligned_block(0);
                let pos = (writer as u64 * BLOCKS_PER_WRITER + block as u64) * BLOCK as u64;
                file.read_exact_at(&mut buf[offset..offset + BLOCK], pos)
                    .unwrap();
                assert!(
                    buf[offset..offset + BLOCK] == pattern(expected),
                    "writer {writer} block {block} lost its last write {expected:#x}"
                );
            }
        }
        longest
    }
}

fn device_exists(dev_id: u32) -> bool {
    Path::new(&format!("/sys/class/ublk-char/ublkc{dev_id}")).exists()
}

async fn wait_device_gone(dev_id: u32) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while device_exists(dev_id) {
        assert!(
            Instant::now() < deadline,
            "ublk device {dev_id} still exists"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn detach_and_crash_recover_keep_writes_and_device() {
    let _serial = require_ublk!();
    let env = Env::new();
    let image = env.image("rootfs");

    let daemon = env.spawn_daemon(None).await;
    let (dev_id, device_path) = env.create(&image).await;

    // Graceful restart: detach, then recover in a new daemon.
    let writers = Writers::start(&device_path);
    tokio::time::sleep(Duration::from_millis(200)).await;
    env.detach(daemon).await;
    let mut daemon = env.spawn_daemon(None).await;
    assert!(env.recover(vec![env.spec(dev_id, &image)]).await.is_empty());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stall = writers.finish_and_verify(&device_path);
    eprintln!("detach restart: longest write {stall:?}");

    // Crash: kill -9 the daemon while writes are in flight.
    let writers = Writers::start(&device_path);
    tokio::time::sleep(Duration::from_millis(200)).await;
    daemon.start_kill().unwrap();
    daemon.wait().await.unwrap();
    let daemon = env.spawn_daemon(None).await;
    assert!(env.recover(vec![env.spec(dev_id, &image)]).await.is_empty());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stall = writers.finish_and_verify(&device_path);
    eprintln!("crash restart: longest write {stall:?}");

    assert!(matches!(
        env.call(DaemonRequest::Delete { dev_id }).await,
        DaemonResponse::Deleted
    ));
    drop(daemon);
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_sweeps_devices_it_was_not_given() {
    let _serial = require_ublk!();
    let env = Env::new();
    let kept_image = env.image("kept");
    let orphan_image = env.image("orphan");

    let daemon = env.spawn_daemon(None).await;
    let (kept, _) = env.create(&kept_image).await;
    let (orphan, _) = env.create(&orphan_image).await;
    env.detach(daemon).await;

    let daemon = env.spawn_daemon(None).await;
    assert!(env
        .recover(vec![env.spec(kept, &kept_image)])
        .await
        .is_empty());
    wait_device_gone(orphan).await;
    assert!(device_exists(kept));

    assert!(matches!(
        env.call(DaemonRequest::Delete { dev_id: kept }).await,
        DaemonResponse::Deleted
    ));
    drop(daemon);
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_deletes_devices_created_without_recovery() {
    let _serial = require_ublk!();
    let env = Env::new();
    let image = env.image("plain");
    let app_config = env.dir.path().join("aenv.toml");
    std::fs::write(&app_config, "[ublk]\nrecovery_enabled = false\n").unwrap();

    let daemon = env.spawn_daemon(Some(&app_config)).await;
    let (dev_id, _) = env.create(&image).await;
    env.detach(daemon).await;

    let daemon = env.spawn_daemon(None).await;
    assert_eq!(
        env.recover(vec![env.spec(dev_id, &image)]).await,
        vec![dev_id]
    );
    wait_device_gone(dev_id).await;
    drop(daemon);
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_leaves_other_instances_devices_alone() {
    let _serial = require_ublk!();
    let owner = Env::new();
    let other = Env::new();
    let image = owner.image("owned");

    let owner_daemon = owner.spawn_daemon(None).await;
    let (dev_id, device_path) = owner.create(&image).await;
    let other_daemon = other.spawn_daemon(None).await;
    assert_eq!(
        other.recover(vec![owner.spec(dev_id, &image)]).await,
        vec![dev_id]
    );

    // The owner still serves the device.
    let writers = Writers::start(&device_path);
    tokio::time::sleep(Duration::from_millis(200)).await;
    writers.finish_and_verify(&device_path);
    assert!(matches!(
        owner.call(DaemonRequest::Delete { dev_id }).await,
        DaemonResponse::Deleted
    ));
    drop(other_daemon);
    drop(owner_daemon);
}

#[tokio::test(flavor = "multi_thread")]
async fn recover_restores_pool_devices_and_shared_refcount() {
    let _serial = require_ublk!();
    let env = Env::new();
    let rootfs = env.image("rootfs");
    let memory = env.image("memory");
    let app_config = env.dir.path().join("aenv.toml");
    std::fs::write(
        &app_config,
        "[ublk]\nrecovery_concurrency = 1\n\n[pool.block]\nenabled = true\nstartup_prewarm = false\n",
    )
    .unwrap();

    let acquire = |image: PathBuf, access_mode| DaemonRequest::AcquireOverlaybd {
        image_config: image,
        global_config: env.global_config.clone(),
        virtual_size: None,
        access_mode,
    };
    let acquired = |response| match response {
        DaemonResponse::DeviceAcquired { dev_id, .. } => dev_id,
        other => panic!("unexpected acquire response: {other:?}"),
    };

    let daemon = env.spawn_daemon(Some(&app_config)).await;
    let exclusive = acquired(
        env.call(acquire(rootfs.clone(), AccessMode::Exclusive))
            .await,
    );
    let shared = acquired(env.call(acquire(memory.clone(), AccessMode::Shared)).await);
    assert_eq!(
        acquired(env.call(acquire(memory.clone(), AccessMode::Shared)).await),
        shared
    );
    env.detach(daemon).await;

    let daemon = env.spawn_daemon(Some(&app_config)).await;
    let specs = vec![
        DeviceSpec {
            role: DeviceRole::Exclusive,
            ..env.spec(exclusive, &rootfs)
        },
        DeviceSpec {
            role: DeviceRole::Shared { refcount: 2 },
            ..env.spec(shared, &memory)
        },
    ];
    assert!(env.recover(specs).await.is_empty());

    // Both devices are back in the pool's active maps, and the shared device
    // keeps both references.
    let release = |dev_id| DaemonRequest::ReleaseOverlaybd { dev_id };
    for dev_id in [exclusive, shared, shared] {
        assert!(
            matches!(env.call(release(dev_id)).await, DaemonResponse::Released),
            "release of {dev_id} failed"
        );
    }
    assert!(matches!(
        env.call(release(shared)).await,
        DaemonResponse::Error { .. }
    ));

    assert!(matches!(
        env.call(DaemonRequest::Shutdown).await,
        DaemonResponse::Ok
    ));
    let mut daemon = daemon;
    tokio::time::timeout(Duration::from_secs(30), daemon.wait())
        .await
        .expect("daemon did not exit after shutdown")
        .unwrap();
    wait_device_gone(exclusive).await;
    wait_device_gone(shared).await;
}
