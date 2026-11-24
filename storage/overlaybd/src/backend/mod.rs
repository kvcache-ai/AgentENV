pub mod cache;
pub mod local;
#[cfg(feature = "full")]
pub mod oss;
#[cfg(feature = "full")]
pub mod registryfs_v2;
pub mod switch;
pub mod tar;

// Backward-compatible path while implementation is aligned to overlaybd registryfs_v2.
#[cfg(feature = "full")]
pub use registryfs_v2 as remote;

/// A layer read that received HTTP 429. Outer image/cache fallbacks must
/// preserve the reader's retry budget when this reaches them.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{context}: http status 429")]
pub(crate) struct ThrottledRead {
    context: String,
    #[cfg(feature = "full")]
    retry_after: Option<std::time::Duration>,
}
