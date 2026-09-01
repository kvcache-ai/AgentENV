//! Firecracker-based sandbox backend implementation.
//!
//! Provides [`FirecrackerSandbox`] (the concrete VM-backed sandbox) and
//! [`FirecrackerSandboxFactory`] which wires sandbox configuration from the
//! global [`ConfigManager`][crate::cfg::ConfigManager].

mod config;
mod connector;
mod factory;
mod instance;
mod manifest;
mod mmds;
mod overlaybd_snapshot;
mod pool;
mod process_vm_reader;
mod sandbox;
mod socket;

pub use config::{
    FirecrackerCommonConfig, FirecrackerRuntimePolicy, FirecrackerSandboxConfig,
    FirecrackerSnapshotConfig,
};
pub use factory::FirecrackerSandboxFactory;
pub(super) use instance::FirecrackerInstance;
pub use instance::{MemoryHotplugStatus, MemoryResizeResult};
pub use manifest::FirecrackerSnapshotManifest;
pub use pool::FirecrackerPool;
pub use sandbox::{FirecrackerCapturedSnapshot, FirecrackerPausedState, FirecrackerSandbox};
