//! Node-level observability primitives.
//!
//! This module combines:
//! - static node identity and build metadata
//! - static machine information detected from the host
//! - request-time host resource collection
//! - orchestrator-published runtime counters
//!
//! The resulting [`NodeSnapshot`] is used by the admin/node APIs so requests can
//! read an already-projected view of node state without rescanning all
//! sandboxes on every call.

mod host;
mod machine;
mod model;
mod reporter;
mod service;

pub use host::{DiskMetric, HostMetrics, HostMetricsCollector};
pub use model::{MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
pub use reporter::ObservabilityReporter;
pub use service::ObservabilityService;
