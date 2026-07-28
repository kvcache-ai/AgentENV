use anyhow::{bail, Context, Result};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;
use tracing_log::{log::LevelFilter, LogTracer};
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};

mod ctrl;
mod dev;
pub mod impls;
mod io_buffer;
pub mod io_weight_scheduler;
mod queue;
pub mod ublk_caps;

pub use ctrl::{
    load_ublk_module, ublk_available, ublk_module_loaded, UVMUblkCtrl, UVMUblkCtrlBuilder,
};
pub use dev::{UVMUblkDev, UVMUblkDevBuilder, UVMUblkTarget};
pub use impls::cow::{BasicCowConfig, BasicCowTarget};
pub use impls::{OverlaybdTarget, OverlaybdTargetConfig};
pub use io_buffer::{AutoRegBuffer, IOBuffer, IOBufferView, UserBuffer};
use queue::UBLK_QUEUE_URING;
pub use queue::{UVMUblkQueue, UblkDescOperation};
use storage_util::io_ring::IoRingHandle;

/// Spawn a dedicated io_uring worker thread for data-plane I/O and return
/// an [`IoRingHandle`] that can be used cross-thread.
///
/// The returned `JoinHandle` keeps the worker thread alive; dropping it will
/// cause the worker to exit once all outstanding requests complete.
pub fn spawn_data_io_ring_worker(worker_id: usize) -> (IoRingHandle, std::thread::JoinHandle<()>) {
    storage_util::io_ring::spawn_io_ring_worker::<io_uring::squeue::Entry>(worker_id)
}

pub async fn delete_dev(
    ctrl_ring: IoRingHandle<io_uring::squeue::Entry128>,
    dev_id: u32,
) -> Result<()> {
    let mut ctrl = UVMUblkCtrlBuilder::new()
        .dev_id(dev_id)
        .build(ctrl_ring)
        .context("build ctrl")?;
    tracing::debug!("start to stop the device");
    if let Err(err) = ctrl.stop_dev().await {
        if let Some(e) = err.root_cause().downcast_ref::<std::io::Error>() {
            if e.raw_os_error() == Some(libc::ENODEV) {
                // ignore the enodev error, the device not exists
                // means we do not need to delete
                tracing::info!(id = dev_id, "stop device get ENODEV");
                return Ok(());
            }
        }
        return Err(err);
    }
    tracing::debug!(id = dev_id, "device has been stopped");
    ctrl.del_dev().await
}

/// Wait for ublk with id `dev_id` to become readable by the current process.
///
/// The block node can appear before udev applies its group and mode rules. A
/// mere existence check therefore races non-root consumers such as
/// Firecracker's snapshot memory backend.
pub fn wait_for_ublk_dev(dev_id: u32) -> Result<()> {
    let p = format!("/dev/ublkb{}", dev_id);
    let p = Path::new(&p);
    // Total around 30 seconds timeout
    let interval_ms = [10, 100, 500, 1000];
    let retry_times = [5, 10, 10, 24];
    let mut warned_permission = false;
    for (interval, retry) in interval_ms.into_iter().zip(retry_times) {
        for _ in 0..retry {
            match std::fs::File::open(p) {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    if !warned_permission {
                        tracing::warn!(
                            path = %p.display(),
                            "ublk device is not readable yet; udev rules may still be applying"
                        );
                        warned_permission = true;
                    }
                }
                Err(error) => return Err(error).with_context(|| format!("open {}", p.display())),
            }
            std::thread::sleep(Duration::from_millis(interval));
        }
    }
    bail!(
        "ublk device {dev_id} did not become accessible at {}",
        p.display()
    );
}

const LOG_FORMAT_ENV: &str = "AENV_LOG_FORMAT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Compact,
    Pretty,
    Json,
}

impl LogFormat {
    fn from_env() -> Self {
        let raw = std::env::var(LOG_FORMAT_ENV).unwrap_or_default();
        match raw.trim().to_ascii_lowercase().as_str() {
            "json" => Self::Json,
            "pretty" => Self::Pretty,
            "compact" | "" => Self::Compact,
            _ => Self::Compact,
        }
    }
}

/// - `dst`: The destination for logging file. If `None`, logging to stderr.
pub fn setup_tracing(dst: Option<PathBuf>, level: LevelFilter) -> Result<()> {
    static INIT: OnceLock<()> = OnceLock::new();
    if INIT.set(()).is_err() {
        // Already initialized
        return Ok(());
    }
    // only one subscriber is allowed
    // take ownership of `log::` operations
    LogTracer::init().context("add log to tracing")?;

    let default_filter = format!(
        "agentenv=info,envd=info,uvm_ublk={}",
        level.to_string().to_ascii_lowercase()
    );
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    // Keep one open fd and clone it per write event.
    let writer = if let Some(dst) = dst {
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&dst)
            .context("open log file")?;
        BoxMakeWriter::new(move || -> Box<dyn Write + Send> {
            match log_file.try_clone() {
                Ok(file) => Box::new(file),
                Err(_) => Box::new(std::io::stderr()),
            }
        })
    } else {
        BoxMakeWriter::new(std::io::stderr)
    };

    let format = LogFormat::from_env();
    let fmt_layer = match format {
        LogFormat::Compact => fmt::layer().compact().with_writer(writer).boxed(),
        LogFormat::Pretty => fmt::layer().pretty().with_writer(writer).boxed(),
        LogFormat::Json => fmt::layer().json().with_writer(writer).boxed(),
    };

    // If another subscriber is already installed, keep that subscriber.
    // `try_init` only fails when a global subscriber already exists.
    if tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .try_init()
        .is_err()
    {
        return Ok(());
    }

    Ok(())
}
