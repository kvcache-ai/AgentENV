//! Network Block Device (NBD) serving for userspace block exports.
//!
//! The server implements the NBD transmission phase over any async stream;
//! [`kernel`] attaches an export to a Linux `/dev/nbdN` device.

#[cfg(target_os = "linux")]
pub mod kernel;
mod protocol;
mod server;

pub use server::NbdExport;
