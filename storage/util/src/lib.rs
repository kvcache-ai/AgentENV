pub mod aligned_buffer;
mod always_send;
pub mod compact_writer;
mod id_allocator;
// `io_ring` is built on the `io-uring` crate, which `Cargo.toml` only pulls in
// for Linux targets, so enabling this feature anywhere else is a build
// configuration error rather than something to degrade gracefully. Reporting it
// here keeps `cfg(feature = "io-uring")` sufficient on its own at every use site
// across this crate and `overlaybd`: the feature cannot be enabled unless the
// target is Linux, so no call site has to repeat `target_os = "linux"`.
#[cfg(all(feature = "io-uring", not(target_os = "linux")))]
compile_error!("the `io-uring` feature requires target_os = \"linux\"");
#[cfg(feature = "io-uring")]
pub mod io_ring;
pub mod mmap_region;

pub use aligned_buffer::AlignedBuffer;
pub use always_send::AlwaysSend;
pub use compact_writer::{CompactBuffer, CompactWriter};
pub use id_allocator::ReloadableIDAllocator;
pub use mmap_region::{MMapRegion, MMapRegionSlice};
