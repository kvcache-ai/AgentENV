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
use uvm_ublk_daemon::{
    AccessMode, DaemonRequest, DaemonResponse, DeviceRole, DeviceSpec, PoolConfig,
    UblkDaemonClient, UblkDaemonSpawnConfig,
};

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

impl Env {
    async fn client(&self, binary: &Path, pool: Option<&PoolConfig>) -> Arc<UblkDaemonClient> {
        UblkDaemonClient::new(UblkDaemonSpawnConfig {
            binary_path: binary,
            socket_path: self.socket(),
            global_config: &self.global_config,
            resize_global_config: &self.global_config,
            app_config: None,
            log_file: None,
            metrics_listen_addr: "",
            pool_config: pool,
            p2p_publish_url: None,
            runtime_device_timeout: Duration::from_secs(60),
            recovery_enabled: true,
        })
        .await
        .unwrap()
    }

    /// Pid of a daemon serving this environment's socket, other than
    /// `except`.
    fn find_daemon_pid(&self, except: &[i32]) -> Option<i32> {
        let socket = self.socket();
        let socket = socket.to_str().unwrap();
        std::fs::read_dir("/proc")
            .unwrap()
            .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse::<i32>().ok())
            .find(|pid| {
                !except.contains(pid)
                    && std::fs::read(format!("/proc/{pid}/cmdline")).is_ok_and(|cmdline| {
                        let cmdline = String::from_utf8_lossy(&cmdline);
                        cmdline.contains("uvm-ublk-daemon") && cmdline.contains(socket)
                    })
            })
    }

    fn daemon_pid(&self) -> i32 {
        self.find_daemon_pid(&[]).expect("daemon process not found")
    }

    fn kill_daemon(&self) -> i32 {
        let pid = self.daemon_pid();
        sigkill(pid);
        pid
    }
}

fn sigkill(pid: i32) {
    // The process may already be gone.
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGKILL,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn client_restart_and_crash_respawn_keep_devices() {
    let _serial = require_ublk!();
    let env = Env::new();
    let image = env.image("rootfs");
    let client = env
        .client(Path::new(env!("CARGO_BIN_EXE_uvm-ublk-daemon")), None)
        .await;
    let (dev_id, device_path) = client
        .create_overlaybd(&image, &env.global_config)
        .await
        .unwrap();

    let writers = Writers::start(&device_path);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let first_pid = env.daemon_pid();
    client.restart().await.unwrap();
    assert_ne!(env.daemon_pid(), first_pid);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stall = writers.finish_and_verify(&device_path);
    eprintln!("client restart: longest write {stall:?}");

    let writers = Writers::start(&device_path);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let killed = env.kill_daemon();
    // RPCs wait for the watchdog's replacement instead of failing.
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.get_features().await.unwrap();
    assert_ne!(env.daemon_pid(), killed);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stall = writers.finish_and_verify(&device_path);
    eprintln!("client crash respawn: longest write {stall:?}");

    client.delete(dev_id).await.unwrap();
    client.shutdown().await.unwrap();
    wait_device_gone(dev_id).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn client_deletes_devices_when_daemon_cannot_be_replaced() {
    let _serial = require_ublk!();
    let env = Env::new();
    let image = env.image("rootfs");
    let binary = env.dir.path().join("uvm-ublk-daemon");
    std::fs::copy(env!("CARGO_BIN_EXE_uvm-ublk-daemon"), &binary).unwrap();
    let client = env.client(&binary, None).await;
    let (dev_id, device_path) = client
        .create_overlaybd(&image, &env.global_config)
        .await
        .unwrap();

    let device = open_direct(&device_path);
    std::fs::remove_file(&binary).unwrap();
    env.kill_daemon();
    // Queued by the kernel while no daemon serves the device.
    let write = tokio::task::spawn_blocking(move || {
        let (buf, offset) = aligned_block(1);
        device.write_at(&buf[offset..offset + BLOCK], 0)
    });

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match client.get_features().await {
            Err(err) if format!("{err:#}").contains("not running") => break,
            _ => {
                assert!(Instant::now() < deadline, "client never gave up");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    wait_device_gone(dev_id).await;

    // Guest I/O fails instead of waiting for a daemon that never comes.
    let result = tokio::time::timeout(Duration::from_secs(10), write)
        .await
        .expect("write to a deleted device hung")
        .unwrap();
    assert!(result.is_err());
}

/// The daemon's owner tag for `socket` (FNV-1a, as in the server).
fn owner_tag(socket: &Path) -> u64 {
    use std::os::unix::ffi::OsStrExt;

    socket
        .as_os_str()
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
        .max(1)
}

/// Every ublk device carrying the owner tag of the daemon serving `socket`.
async fn tagged_devices(socket: &Path) -> Vec<u32> {
    let tag = owner_tag(socket);
    let (ring, _ring_thread) =
        storage_util::io_ring::spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
    let mut ctrl = uvm_ublk::UVMUblkCtrlBuilder::new().build(ring).unwrap();
    let dev_ids: Vec<u32> = std::fs::read_dir("/sys/class/ublk-char")
        .unwrap()
        .filter_map(|entry| {
            entry
                .ok()?
                .file_name()
                .to_str()?
                .strip_prefix("ublkc")?
                .parse()
                .ok()
        })
        .collect();
    let mut tagged = Vec::new();
    for dev_id in dev_ids {
        ctrl.dev_info.dev_id = dev_id;
        if ctrl.get_dev_info().await.is_ok() && ctrl.dev_info.ublksrv_flags == tag {
            tagged.push(dev_id);
        }
    }
    tagged
}

/// Deterministic generator, so a failing sequence replays from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn millis(&mut self, max: u64) -> Duration {
        Duration::from_millis(self.below(max + 1))
    }
}

fn env_u64(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    match value.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => value.parse().ok(),
    }
}

const SHARED_BLOCKS: u64 = 256;
const SHARED_READERS: usize = 2;

fn shared_fill(block: u64) -> u64 {
    0x5aed_0000_0000_0000 | block
}

/// O_DIRECT readers checking the shared device's fixed content.
struct Readers {
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<(usize, usize)>>,
}

impl Readers {
    fn start(device: &Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let threads = (0..SHARED_READERS as u64)
            .map(|reader| {
                let stop = Arc::clone(&stop);
                let file = open_direct(device);
                std::thread::spawn(move || {
                    let (mut errors, mut mismatches) = (0, 0);
                    let mut block = reader;
                    while !stop.load(Ordering::Relaxed) {
                        block = (block * 37 + 11) % SHARED_BLOCKS;
                        let (mut buf, offset) = aligned_block(0);
                        match file
                            .read_exact_at(&mut buf[offset..offset + BLOCK], block * BLOCK as u64)
                        {
                            Ok(())
                                if buf[offset..offset + BLOCK] == pattern(shared_fill(block)) => {}
                            Ok(()) => mismatches += 1,
                            Err(_) => errors += 1,
                        }
                    }
                    (errors, mismatches)
                })
            })
            .collect();
        Self { stop, threads }
    }

    fn finish_and_verify(self) {
        self.stop.store(true, Ordering::Relaxed);
        for (reader, thread) in self.threads.into_iter().enumerate() {
            let (errors, mismatches) = thread.join().unwrap();
            assert_eq!(errors, 0, "shared reader {reader} saw I/O errors");
            assert_eq!(mismatches, 0, "shared reader {reader} read wrong data");
        }
    }
}

/// A simulated sandbox: a private writable device under constant writes,
/// plus a reference to the shared device under constant reads.
struct Sandbox {
    dev_id: u32,
    path: PathBuf,
    pooled: bool,
    writers: Writers,
    readers: Readers,
}

#[derive(Clone, Copy, Debug)]
enum Chaos {
    /// SIGKILL the daemon.
    Kill,
    /// SIGKILL the daemon, then its replacement, which may be recovering.
    KillTwice,
    /// Detach and replace the daemon.
    Restart,
    /// SIGKILL a daemon while a restart is detaching or recovering.
    RestartRacingKill,
    /// Start a sandbox.
    Add,
    /// SIGKILL the daemon while a device is being created.
    AddRacingKill,
    /// Stop a sandbox, verifying its data.
    Remove,
}

struct Chaotic {
    env: Env,
    client: Arc<UblkDaemonClient>,
    rng: Rng,
    shared_image: PathBuf,
    shared_dev: u32,
    shared_path: PathBuf,
    sandboxes: Vec<Sandbox>,
    next_image: usize,
    killed: Vec<i32>,
}

impl Chaotic {
    /// Wait until RPCs reach a daemon again, which is after any recovery.
    async fn wait_healthy(&self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match self.client.get_features().await {
                Ok(_) => return,
                Err(err) => {
                    assert!(
                        Instant::now() < deadline,
                        "ublk daemon never came back: {err:#}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    fn kill(&mut self) {
        if let Some(pid) = self.env.find_daemon_pid(&self.killed) {
            sigkill(pid);
            self.killed.push(pid);
        }
    }

    /// Kill the next daemon to appear within `window`, if any.
    async fn kill_next(&mut self, window: Duration) {
        let deadline = Instant::now() + window;
        while Instant::now() < deadline {
            if self.env.find_daemon_pid(&self.killed).is_some() {
                self.kill();
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    async fn create_device(
        client: &UblkDaemonClient,
        image: &Path,
        global_config: &Path,
        pooled: bool,
    ) -> anyhow::Result<(u32, PathBuf)> {
        if pooled {
            client
                .acquire_overlaybd(image, global_config, None, AccessMode::Exclusive)
                .await
        } else {
            client.create_overlaybd(image, global_config).await
        }
    }

    /// Start a sandbox on a device created by `create`.
    async fn attach(&mut self, dev_id: u32, path: PathBuf, pooled: bool) {
        let (shared_dev, _) = self
            .client
            .acquire_overlaybd(
                &self.shared_image,
                &self.env.global_config,
                None,
                AccessMode::Shared,
            )
            .await
            .unwrap();
        assert_eq!(shared_dev, self.shared_dev, "shared device was replaced");
        self.sandboxes.push(Sandbox {
            dev_id,
            writers: Writers::start(&path),
            readers: Readers::start(&self.shared_path),
            path,
            pooled,
        });
    }

    fn next_image(&mut self) -> PathBuf {
        self.next_image += 1;
        self.env.image(&format!("sandbox{}", self.next_image))
    }

    async fn add(&mut self) {
        let image = self.next_image();
        let pooled = self.rng.below(2) == 0;
        let (dev_id, path) =
            Self::create_device(&self.client, &image, &self.env.global_config, pooled)
                .await
                .unwrap();
        self.attach(dev_id, path, pooled).await;
    }

    /// Create a device while the daemon is killed. The create may fail;
    /// whatever the daemon made of it must not leak.
    async fn add_racing_kill(&mut self) {
        let image = self.next_image();
        let pooled = self.rng.below(2) == 0;
        let delay = self.rng.millis(20);
        let client = Arc::clone(&self.client);
        let global_config = self.env.global_config.clone();
        let create = tokio::spawn(async move {
            Self::create_device(&client, &image, &global_config, pooled).await
        });
        tokio::time::sleep(delay).await;
        self.kill();
        match create.await.unwrap() {
            Ok((dev_id, path)) => {
                self.wait_healthy().await;
                self.attach(dev_id, path, pooled).await;
            }
            Err(err) => eprintln!("  create racing a kill failed: {err:#}"),
        }
    }

    async fn remove(&mut self, index: usize) {
        let sandbox = self.sandboxes.swap_remove(index);
        sandbox.writers.finish_and_verify(&sandbox.path);
        sandbox.readers.finish_and_verify();
        if sandbox.pooled {
            self.client.release_overlaybd(sandbox.dev_id).await.unwrap();
        } else {
            self.client.delete(sandbox.dev_id).await.unwrap();
        }
        self.client
            .release_overlaybd(self.shared_dev)
            .await
            .unwrap();
    }

    async fn step(&mut self, chaos: Chaos) {
        match chaos {
            Chaos::Kill => self.kill(),
            Chaos::KillTwice => {
                self.kill();
                let window = self.rng.millis(300);
                self.kill_next(window).await;
            }
            Chaos::Restart => self.client.restart().await.unwrap(),
            Chaos::RestartRacingKill => {
                let client = Arc::clone(&self.client);
                let restart = tokio::spawn(async move { client.restart().await });
                let delay = self.rng.millis(150);
                tokio::time::sleep(delay).await;
                self.kill();
                restart.await.unwrap().unwrap();
            }
            Chaos::Add => self.add().await,
            Chaos::AddRacingKill => self.add_racing_kill().await,
            Chaos::Remove => {
                let index = self.rng.below(self.sandboxes.len() as u64) as usize;
                self.remove(index).await;
            }
        }
    }

    fn pick(&mut self) -> Chaos {
        const MAX_SANDBOXES: usize = 8;
        let mut choices = vec![
            Chaos::Kill,
            Chaos::Kill,
            Chaos::Kill,
            Chaos::KillTwice,
            Chaos::KillTwice,
            Chaos::Restart,
            Chaos::RestartRacingKill,
            Chaos::RestartRacingKill,
        ];
        if self.sandboxes.len() < MAX_SANDBOXES {
            choices.extend([Chaos::Add, Chaos::AddRacingKill]);
        }
        if self.sandboxes.len() > 2 {
            choices.push(Chaos::Remove);
        }
        choices[self.rng.below(choices.len() as u64) as usize]
    }
}

/// Run a random sequence of daemon crashes, restarts, and device churn
/// against many devices under I/O. Set `UBLK_RECOVERY_SEED` to replay a
/// sequence and `UBLK_RECOVERY_STEPS` to change its length.
#[tokio::test(flavor = "multi_thread")]
async fn random_crash_sequences_keep_every_device() {
    let _serial = require_ublk!();
    let seed = env_u64("UBLK_RECOVERY_SEED").unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });
    let steps = env_u64("UBLK_RECOVERY_STEPS").unwrap_or(30);
    eprintln!("random crash sequence: UBLK_RECOVERY_SEED={seed:#x} steps={steps}");

    let env = Env::new();
    let pool = PoolConfig {
        low_watermark: 1,
        high_watermark: 2,
        maintenance_enabled: true,
        startup_prewarm: false,
    };
    let client = env
        .client(
            Path::new(env!("CARGO_BIN_EXE_uvm-ublk-daemon")),
            Some(&pool),
        )
        .await;

    // Fill the shared image through a private device, then hold one
    // reference to it for the whole run, as a template would.
    let shared_image = env.image("shared");
    let (fill_dev, fill_path) = client
        .create_overlaybd(&shared_image, &env.global_config)
        .await
        .unwrap();
    let file = open_direct(&fill_path);
    for block in 0..SHARED_BLOCKS {
        let (buf, offset) = aligned_block(shared_fill(block));
        file.write_all_at(&buf[offset..offset + BLOCK], block * BLOCK as u64)
            .unwrap();
    }
    drop(file);
    client.delete(fill_dev).await.unwrap();
    let (shared_dev, shared_path) = client
        .acquire_overlaybd(&shared_image, &env.global_config, None, AccessMode::Shared)
        .await
        .unwrap();

    let mut chaos = Chaotic {
        env,
        client: Arc::clone(&client),
        rng: Rng(seed),
        shared_image,
        shared_dev,
        shared_path,
        sandboxes: Vec::new(),
        next_image: 0,
        killed: Vec::new(),
    };
    for _ in 0..4 {
        chaos.add().await;
    }

    for step in 0..steps {
        let action = chaos.pick();
        eprintln!(
            "step {step}: {action:?} ({} sandboxes)",
            chaos.sandboxes.len()
        );
        chaos.step(action).await;
        let pause = chaos.rng.millis(300);
        tokio::time::sleep(pause).await;
        chaos.wait_healthy().await;
    }

    // Every surviving sandbox kept its device and data, and the shared
    // device kept exactly one reference per holder.
    while !chaos.sandboxes.is_empty() {
        chaos.remove(0).await;
    }
    client.release_overlaybd(shared_dev).await.unwrap();
    assert!(
        client.release_overlaybd(shared_dev).await.is_err(),
        "shared device had more references than holders"
    );

    client.shutdown().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let leaked = tagged_devices(&chaos.env.socket()).await;
        if leaked.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "devices leaked across crashes: {leaked:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
