//! Unified SQL metadata service over embedded dispatch and HTTP JSON transport.
//!
//! Stores shared snapshots (sandbox, template, internal builder), snapshot GC
//! pins, volumes and build-cache seeds, published OverlayBD images and tags,
//! asynchronous build jobs, and the common managed-layer DAG substrate.
mod build_jobs;
mod db;
mod error;
mod gc;
mod images;
mod layers;
mod server;
mod snapshots;
pub mod transport;
mod volumes;

pub use db::{migrations::MigrationStatus, DatabaseConfig};
pub use error::{MetaError, MetaResult};
pub use gc::{GcReport, GcStorage};
pub use server::MetaServer;
pub use snapshots::{SnapshotEntry, SnapshotState};
pub use transport::MetaClient;
