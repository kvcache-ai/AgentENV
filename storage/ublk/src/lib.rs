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
/// Poll schedule as `(interval_ms, retries)`, totalling ~30s.
///
/// The block node normally appears within about a millisecond, so poll finely
/// first and back off afterwards. The previous schedule started at 10ms, which
/// charged nearly every device creation a full 10ms of dead wait.
const WAIT_SCHEDULE: [(u64, u32); 5] = [(1, 20), (5, 16), (50, 18), (500, 10), (1000, 24)];

/// One non-blocking readiness probe. `Ok(true)` means the node is open-able.
fn probe_ublk_dev(path: &Path, warned_permission: &mut bool) -> Result<bool> {
    match std::fs::File::open(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            if !*warned_permission {
                tracing::warn!(
                    path = %path.display(),
                    "ublk device is not readable yet; udev rules may still be applying"
                );
                *warned_permission = true;
            }
            Ok(false)
        }
        Err(error) => Err(error).with_context(|| format!("open {}", path.display())),
    }
}

fn wait_timed_out(dev_id: u32, path: &Path) -> anyhow::Error {
    anyhow::anyhow!(
        "ublk device {dev_id} did not become accessible at {}",
        path.display()
    )
}

/// Blocking variant, for synchronous callers (CLI, tests).
///
/// Do not call this from an async task: it parks the executor thread. Use
/// [`wait_for_ublk_dev_async`] there instead.
pub fn wait_for_ublk_dev(dev_id: u32) -> Result<()> {
    let p = format!("/dev/ublkb{}", dev_id);
    let p = Path::new(&p);
    let mut warned_permission = false;
    for (interval, retry) in WAIT_SCHEDULE {
        for _ in 0..retry {
            if probe_ublk_dev(p, &mut warned_permission)? {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(interval));
        }
    }
    Err(wait_timed_out(dev_id, p))
}

/// Async variant that yields instead of parking the worker thread.
///
/// Device creation runs on the daemon's shared multi-thread runtime; blocking a
/// worker here caps concurrent creations at the worker-thread count, which was
/// the dominant limit on per-node sandbox create throughput.
pub async fn wait_for_ublk_dev_async(dev_id: u32) -> Result<()> {
    let p = format!("/dev/ublkb{}", dev_id);
    let p = Path::new(&p);
    let mut warned_permission = false;
    for (interval, retry) in WAIT_SCHEDULE {
        for _ in 0..retry {
            if probe_ublk_dev(p, &mut warned_permission)? {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(interval)).await;
        }
    }
    Err(wait_timed_out(dev_id, p))
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
