//! Unified SQL metadata service over embedded and TCP transports.
//!
//! Stores shared snapshots (sandbox, template, internal builder), volumes and
//! build-cache seeds, published OverlayBD images and tags, asynchronous build
//! jobs, and the common managed-layer DAG / GC pin substrate.
mod build_jobs;
mod database;
mod entry;
mod error;
mod gc;
mod images;
mod layers;
mod pins;
mod server;
mod snapshots;
pub mod transport;
mod volumes;

pub use database::{migrations::MigrationStatus, DatabaseConfig};
pub use entry::{SnapshotEntry, SnapshotState};
pub use error::{MetaError, MetaResult};
pub use gc::{GcReport, GcStorage};
pub use server::MetaServer;
pub use transport::MetaClient;
