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
mod mincore_tracking;
mod mmds;
mod overlaybd_snapshot;
mod pool;
mod prefault;
mod prefault_stats;
mod process_vm_reader;
mod sandbox;
mod socket;

pub use config::{
    FirecrackerCommonConfig, FirecrackerRuntimePolicy, FirecrackerSandboxConfig,
    FirecrackerSnapshotConfig,
};
pub use factory::FirecrackerSandboxFactory;
pub(super) use instance::FirecrackerInstance;
pub use manifest::{
    FirecrackerSnapshotManifest, GuestMemoryRange, GuestMemoryWorkingSet,
    GuestMemoryWorkingSetLimits,
};
pub use pool::FirecrackerPool;
pub use prefault_stats::PrefaultCompletionStats;
pub use sandbox::{
    FirecrackerCapturedSnapshot, FirecrackerPausedState, FirecrackerSandbox, SnapshotMincoreStage,
    SnapshotPrefaultCandidate, SnapshotResumeTimings,
};
