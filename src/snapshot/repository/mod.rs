pub mod backends;
pub mod errors;
pub mod interfaces;

pub use errors::{RepositoryError, RepositoryResult};
pub use interfaces::{
    SnapshotListFilter, SnapshotRepository, SnapshotRuntimeResolver, VolumeRecordPage,
};

/// Returns a stable, evenly distributed two-hex-digit volume catalog shard.
pub(crate) fn volume_catalog_shard(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:02x}", hash & 0xff)
}
