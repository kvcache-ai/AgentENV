pub mod backend;
mod compression;
pub mod config;
pub mod dense_export;
pub mod download_gate;
pub mod ext4_stat;
mod image;
mod io;
mod layer;
mod lsmt;

pub mod format {
    pub use crate::lsmt::format::*;
}
#[cfg(feature = "full")]
pub mod image_file {
    pub use crate::image::image_file::*;
}
#[cfg(feature = "full")]
pub mod image_service {
    pub use crate::image::image_service::*;
}
pub mod index {
    pub use crate::lsmt::index::*;
}
pub mod index_file {
    pub use crate::lsmt::file::*;
}
pub mod layer_metadata {
    pub use crate::layer::layer_metadata::*;
}
pub mod helper {
    pub use crate::image::helper::*;
}
mod metrics;
pub mod pack_planner;
pub mod prefetch;
pub mod startup_manifest;
pub mod startup_pack;
pub mod snapshot {
    #[cfg(feature = "full")]
    pub use crate::image::snapshot::*;
}
mod sys;
pub mod tools;
pub mod vfile_io {
    pub use crate::io::vfile_io::*;
}
pub mod virtual_file {
    pub use crate::io::virtual_file::*;
}
pub mod zfile {
    pub use crate::compression::zfile::*;
}

#[cfg(feature = "full")]
/// A registry refused a layer read. The delay is in seconds, as on the HTTP wire.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("image registry rate limited (HTTP 429)")]
pub struct RegistryRateLimited {
    pub retry_after_seconds: Option<u64>,
}

#[cfg(feature = "full")]
impl RegistryRateLimited {
    pub fn from_retry_after(header: Option<&str>) -> Self {
        Self {
            retry_after_seconds: header
                .and_then(|h| retry_after_seconds(h.trim(), std::time::SystemTime::now())),
        }
    }
}

#[cfg(feature = "full")]
fn retry_after_seconds(value: &str, now: std::time::SystemTime) -> Option<u64> {
    if !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit()) {
        return Some(value.parse().unwrap_or(u64::MAX));
    }
    let delay = httpdate::parse_http_date(value)
        .ok()?
        .duration_since(now)
        .unwrap_or_default();
    Some(delay.as_secs() + u64::from(delay.subsec_nanos() != 0))
}

pub use image_file::{ImageFile, RestackSnapshotTerminalFailure};
#[cfg(feature = "full")]
pub use image_service::ImageService;
pub use index_file::LayerDescriptor;
#[cfg(feature = "full")]
pub use snapshot::export_upper_as_snapshot_layer;

// Re-export the compact_writer
pub use storage_util::compact_writer;
