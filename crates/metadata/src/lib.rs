//! Snapshot registration and lookup over embedded and TCP transports.
//!
//! Local and Distributed entries share captured [`agentenv_domain::SnapshotConfig`].
//! Identity, ownership and timestamps remain directory state. Distributed artifact inventories
//! are supplied by the caller when reconstructing records; Local records have no published
//! inventory.
//! Artifact storage, write-once protection and runtime file ownership remain with the caller.
mod database;
mod entry;
mod error;
mod protocol;
mod server;
mod snapshot_directory;
pub mod transport;

pub use database::{migrations::MigrationStatus, DatabaseConfig};
pub use entry::SnapshotEntry;
pub use error::{MetaError, MetaResult};
pub use protocol::MetaClient;
pub use server::MetaServer;
pub use tarpc::{client::RpcError, context};
