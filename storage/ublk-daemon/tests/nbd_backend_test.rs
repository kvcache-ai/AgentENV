//! Daemon tests against real `/dev/nbdN` devices. They need the `nbd`
//! module, `CAP_SYS_ADMIN`, and read/write access to `/dev/nbd*`, and skip
//! when any is missing.

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use overlaybd::config::UpperMode;
use overlaybd::helper::prepare_runtime_upper;
use overlaybd::image_service::ImageService;
use overlaybd::virtual_file::VirtualFile;
use tokio::task::JoinHandle;
use uvm_ublk_daemon::{AccessMode, DeviceBackend, PoolConfig, UblkDaemonClient, UblkDaemonServer};

const IMAGE_SIZE: u64 = 4 << 20;
const BLOCK: usize = 4096;

fn nbd_usable() -> bool {
    if !Path::new("/sys/module/nbd").exists() {
        eprintln!("skipping nbd daemon test: nbd module not loaded");
        return false;
    }
    if !linux_cap::has_effective_capabilities(&[linux_cap::CAP_SYS_ADMIN]).unwrap_or(false) {
        eprintln!("skipping nbd daemon test: CAP_SYS_ADMIN is not effective");
        return false;
    }
    // access(2) instead of open: opening an unconfigured device makes the
    // kernel log partition-scan I/O errors.
    // SAFETY: the path is a valid NUL-terminated string.
    if unsafe { libc::access(c"/dev/nbd0".as_ptr(), libc::R_OK | libc::W_OK) } != 0 {
        eprintln!(
            "skipping nbd daemon test: /dev/nbd0 is not accessible: {}",
            std::io::Error::last_os_error()
        );
        return false;
    }
    true
}

fn write_global_config(dir: &Path) -> PathBuf {
    let cache_dir = dir.join("cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let path = dir.join("global_config.json");
    let config = serde_json::json!({
        "registryFsVersion": "v2",
        "ioEngine": 0,
        "cacheConfig": {
            "cacheType": "file",
            "cacheDir": cache_dir,
            "cacheSizeGB": 1,
            "refillSize": 262144,
            "blockSize": 65536
        }
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    path
}

/// A local sparse OverlayBD image with no lowers.
fn write_image(dir: &Path, name: &str) -> PathBuf {
    let data = dir.join(format!("{name}.data"));
    prepare_runtime_upper(&data, None, IMAGE_SIZE, UpperMode::Sparse).unwrap();
    let config = dir.join(format!("{name}.json"));
    let image = serde_json::json!({
        "lowers": [],
        "upper": { "mode": "sparse", "data": data }
    });
    std::fs::write(&config, serde_json::to_vec(&image).unwrap()).unwrap();
    config
}

async fn image_block(global_config: &Path, image_config: &Path, offset: u64) -> Vec<u8> {
    let service = ImageService::from_config_path(global_config).await.unwrap();
    let image = service.create_image_file(image_config).await.unwrap();
    image.read_at(offset, BLOCK).await.unwrap().to_vec()
}

async fn start_daemon(
    dir: &Path,
    pool: bool,
) -> (Arc<UblkDaemonClient>, JoinHandle<anyhow::Result<()>>) {
    let global_config = write_global_config(dir);
    let socket_path = dir.join("daemon.sock");
    let mut server = UblkDaemonServer::new_with_backend(
        socket_path.clone(),
        DeviceBackend::Nbd,
        ImageService::from_config_path(&global_config)
            .await
            .unwrap(),
        dir.join("resize-overlaybd-global.json"),
        None,
    );
    if pool {
        let features = server.detect_ublk_features().await.unwrap();
        assert_eq!(features, 0, "nbd backend reports no ublk features");
        server.enable_pool(
            PoolConfig {
                low_watermark: 0,
                high_watermark: 2,
                maintenance_enabled: false,
                startup_prewarm: false,
            },
            features,
        );
    }
    let task = tokio::spawn(async move { server.run().await });

    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket_path.exists() {
        assert!(Instant::now() < deadline, "daemon did not bind its socket");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (UblkDaemonClient::new_for_test(socket_path, false), task)
}

async fn stop_daemon(client: &UblkDaemonClient, task: JoinHandle<anyhow::Result<()>>) {
    client.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("daemon should stop")
        .unwrap()
        .unwrap();
}

/// Run blocking device I/O off the async runtime that serves the device.
async fn with_device<T: Send + 'static>(
    path: PathBuf,
    io: impl FnOnce(&std::fs::File) -> T + Send + 'static,
) -> T {
    tokio::task::spawn_blocking(move || {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        io(&file)
    })
    .await
    .unwrap()
}

fn sysfs_sectors(index: u32) -> u64 {
    std::fs::read_to_string(format!("/sys/block/nbd{index}/size"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nbd_backend_serves_raw_overlaybd_device() {
    if !nbd_usable() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let image = write_image(dir.path(), "raw");
    let (client, task) = start_daemon(dir.path(), false).await;
    let global_config = dir.path().join("global_config.json");

    let (dev_id, device_path) = client
        .create_overlaybd(&image, &global_config)
        .await
        .unwrap();
    assert_eq!(device_path, PathBuf::from(format!("/dev/nbd{dev_id}")));
    assert_eq!(sysfs_sectors(dev_id) * 512, IMAGE_SIZE);

    with_device(device_path, |file| {
        file.write_all_at(&[0x5c; BLOCK], 2 * BLOCK as u64).unwrap();
        file.sync_all().unwrap();
    })
    .await;
    client.delete(dev_id).await.unwrap();

    // The write went through the NBD export into the image's upper.
    assert_eq!(
        image_block(&global_config, &image, 2 * BLOCK as u64).await,
        vec![0x5c; BLOCK]
    );
    stop_daemon(&client, task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nbd_backend_pool_reuses_device_across_images() {
    if !nbd_usable() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let image_a = write_image(dir.path(), "a");
    let image_b = write_image(dir.path(), "b");
    let (client, task) = start_daemon(dir.path(), true).await;
    let global_config = dir.path().join("global_config.json");
    {
        let service = ImageService::from_config_path(&global_config)
            .await
            .unwrap();
        let image = service.create_image_file(&image_b).await.unwrap();
        image.write_at(0, &[0xbb; BLOCK]).await.unwrap();
        image.sync().await.unwrap();
    }

    let (dev_a, path_a) = client
        .acquire_overlaybd(&image_a, &global_config, None, AccessMode::Exclusive)
        .await
        .unwrap();
    with_device(path_a, |file| {
        file.write_all_at(&[0xaa; BLOCK], 0).unwrap();
        file.sync_all().unwrap();
        // Buffered read: leaves image A's data in the device page cache.
        let mut buf = vec![0u8; BLOCK];
        file.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(buf, vec![0xaa; BLOCK]);
    })
    .await;
    client.release_overlaybd(dev_a).await.unwrap();

    let (dev_b, path_b) = client
        .acquire_overlaybd(&image_b, &global_config, None, AccessMode::Exclusive)
        .await
        .unwrap();
    assert_eq!(dev_b, dev_a, "released same-size device should be reused");
    let data = with_device(path_b, |file| {
        let mut buf = vec![0u8; BLOCK];
        file.read_exact_at(&mut buf, 0).unwrap();
        buf
    })
    .await;
    assert_eq!(
        data,
        vec![0xbb; BLOCK],
        "reused device must not serve stale image A data"
    );
    client.release_overlaybd(dev_b).await.unwrap();

    stop_daemon(&client, task).await;
    assert_eq!(
        image_block(&global_config, &image_a, 0).await,
        vec![0xaa; BLOCK]
    );
}
