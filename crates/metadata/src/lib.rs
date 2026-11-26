//! Unified SQL metadata service over embedded and TCP transports.
//!
//! Stores shared snapshots (sandbox, template, internal builder), volumes and
//! build-cache seeds, published OverlayBD images and tags, asynchronous build
//! jobs, and the common managed-layer DAG / GC pin substrate.
mod build_jobs;
mod database;
mod error;
mod gc;
mod images;
mod layer_graph;
mod protocol;
mod server;
mod snapshots;
pub mod transport;
mod volumes;

use agentenv_domain::SnapshotMetadata;
pub use database::{migrations::MigrationStatus, DatabaseConfig};
pub use error::{LayerCheck, LayerState, MetaError, MetaResult};
pub use gc::{GcReport, GcStorage};
pub use server::MetaServer;
pub use tarpc::{client::RpcError, context};
pub use transport::MetaClient;
