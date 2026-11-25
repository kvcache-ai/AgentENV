//! Snapshot registration and lookup over embedded and TCP transports.
//!
//! Entries contain identity, ownership, source sandbox ID and timestamps.
//! The caller retains the complete record, including configuration and artifact inventory.
//! The service collects unreferenced artifacts through its configured storage backend.
mod database;
mod error;
mod gc;
mod protocol;
mod server;
mod snapshot_directory;
pub mod transport;

use agentenv_domain::SnapshotMetadata;
pub use database::{migrations::MigrationStatus, DatabaseConfig};
pub use error::{LayerCheck, LayerState, MetaError, MetaResult};
pub use gc::{GcReport, GcStorage};
pub use protocol::MetaClient;
pub use server::MetaServer;
pub use tarpc::{client::RpcError, context};
