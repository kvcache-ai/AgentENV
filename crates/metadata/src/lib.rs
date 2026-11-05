//! Snapshot registration and lookup over embedded and TCP transports.
//!
//! Distributed entries retain directory metadata (including resource values) and an internal
//! canonical digest of the committed payload. Callers store and read the immutable
//! `CommittedSnapshot` at a deterministic repository path keyed by snapshot ID, then supply it to
//! reconstruct a record; manifest storage, write-once protection and runtime resolution remain
//! their responsibility. Local entries retain public metadata, while the Node owns their usable
//! files. Native database handles stay private to this crate.
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
