//! Kernel attach tests. They need the `nbd` module, `CAP_SYS_ADMIN`, and
//! read/write access to `/dev/nbd*`; they skip when any is missing.
#![cfg(target_os = "linux")]

use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use storage_util::AlignedBuffer;
use uvm_nbd::kernel::NbdDevice;
use uvm_nbd::NbdExport;

const EXPORT_SIZE: usize = 4 << 20;
const BLOCK: usize = 4096;

struct MemExport {
    data: Mutex<Vec<u8>>,
}

#[async_trait]
impl NbdExport for MemExport {
    fn size_bytes(&self) -> u64 {
        EXPORT_SIZE as u64
    }

    fn block_size(&self) -> u32 {
        BLOCK as u32
    }

    fn supports_trim(&self) -> bool {
        true
    }

    async fn read(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        let start = offset as usize;
        Ok(Bytes::copy_from_slice(
            &self.data.lock().unwrap()[start..start + len],
        ))
    }

    async fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let start = offset as usize;
        self.data.lock().unwrap()[start..start + data.len()].copy_from_slice(data);
        Ok(())
    }

    async fn trim(&self, offset: u64, len: usize) -> io::Result<()> {
        let start = offset as usize;
        self.data.lock().unwrap()[start..start + len].fill(0);
        Ok(())
    }
}

fn sysfs_sectors(index: u32) -> u64 {
    std::fs::read_to_string(format!("/sys/block/nbd{index}/size"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// The kernel clears a disconnected device once its last opener (for
/// example a udev probe after our writes) closes it.
fn wait_cleared(index: u32) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while sysfs_sectors(index) != 0 {
        assert!(
            Instant::now() < deadline,
            "nbd{index} still configured after disconnect"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn open_device(path: &Path) -> io::Result<std::fs::File> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
        {
            Ok(file) => return Ok(file),
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                ) && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(err) => return Err(err),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kernel_device_round_trip() {
    let mut initial = vec![0u8; EXPORT_SIZE];
    initial[2 * BLOCK..3 * BLOCK].fill(0x5a);
    let export = Arc::new(MemExport {
        data: Mutex::new(initial),
    });

    let device = match NbdDevice::connect(Arc::clone(&export)).await {
        Ok(device) => device,
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
            ) =>
        {
            eprintln!("skipping nbd kernel test: {err}");
            return;
        }
        Err(err) => panic!("connect nbd device: {err}"),
    };
    let index = device.index();
    let path = device.device_path();
    assert_eq!(sysfs_sectors(index), EXPORT_SIZE as u64 / 512);

    let file = match open_device(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
            device.disconnect().await.unwrap();
            eprintln!(
                "skipping nbd kernel test: {} is not accessible: {err}",
                path.display()
            );
            return;
        }
        Err(err) => panic!("open {}: {err}", path.display()),
    };

    let io_result = tokio::task::spawn_blocking(move || {
        // Read served by the export.
        let mut buf = AlignedBuffer::new(BLOCK, BLOCK)?;
        file.read_exact_at(buf.as_mut(), 2 * BLOCK as u64)?;
        assert!(buf.as_ref().iter().all(|b| *b == 0x5a));

        // O_DIRECT write reaches the export before pwrite returns.
        buf.as_mut().fill(0xc3);
        file.write_all_at(buf.as_ref(), 0)?;
        Ok::<_, io::Error>(())
    })
    .await
    .unwrap();
    io_result.unwrap();

    assert!(export.data.lock().unwrap()[..BLOCK]
        .iter()
        .all(|b| *b == 0xc3));

    device.disconnect().await.unwrap();
    wait_cleared(index);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_device_disconnects_it() {
    let export = Arc::new(MemExport {
        data: Mutex::new(vec![0u8; EXPORT_SIZE]),
    });
    let device = match NbdDevice::connect(export).await {
        Ok(device) => device,
        Err(err) => {
            eprintln!("skipping nbd kernel test: {err}");
            return;
        }
    };
    let index = device.index();
    drop(device);
    wait_cleared(index);
}
