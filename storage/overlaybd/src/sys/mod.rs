//! Platform-specific filesystem primitives.
//!
//! The module is split by *capability*, not by platform: each submodule owns
//! the portable public signature, whatever portable logic that capability
//! needs, and the per-OS implementations. Two consequences are deliberate:
//!
//! - The Linux and macOS versions of one syscall sit next to each other, so a
//!   reviewer can compare them without jumping between files.
//! - Bringing up a new target means adding one `imp` arm per capability file
//!   and touching nothing else. A platform that simply lacks a capability
//!   returns [`SysError::Unsupported`] from its own arm, right beside the
//!   implementations it is standing in for.
//!
//! Callers outside this module should never name a `libc` type or constant for
//! these operations.

use std::fmt;

use nix::errno::Errno;

mod fs_space;
mod open_flags;
mod page_cache;
mod release_space;
mod reserve_space;
mod sparse;
mod xattr;

pub use fs_space::fs_space;
pub use open_flags::{direct_io_open_flag, enable_direct_io};
pub use page_cache::evict_page_cache;
pub use release_space::release_space_hint;
pub use reserve_space::reserve_space;
pub use sparse::sparse_extents_are_reliable;
pub use xattr::{
    fgetxattr, flistxattr, fremovexattr, fsetxattr, getxattr, listxattr, removexattr, setxattr,
};

/// Failure of a platform primitive.
///
/// `Unsupported` is a separate variant rather than an errno because callers
/// legitimately disagree about what it means: page-cache eviction is a hint and
/// degrades to a no-op, a cache refill has to keep working without a space
/// reservation, while a failed hole punch has to surface. Distinguishing these by
/// matching on this enum is sturdier than downcasting an `anyhow::Error`.
#[derive(Debug)]
pub enum SysError {
    /// The current platform has no equivalent of this operation. The payload is
    /// the operation name, for diagnostics only.
    ///
    /// Constructed on every target: the macOS arms report the capabilities
    /// Darwin lacks, and `reserve_space` also reports it on Linux for
    /// filesystems without `fallocate`.
    Unsupported(&'static str),
    /// The syscall was attempted and failed.
    Errno(Errno),
}

impl SysError {
    /// True when the platform never attempted the operation.
    pub fn is_unsupported(&self) -> bool {
        matches!(self, Self::Unsupported(_))
    }

    /// Capture the thread's current `errno`.
    fn last() -> Self {
        Self::Errno(Errno::last())
    }
}

impl fmt::Display for SysError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(op) => {
                write!(f, "{op} is not supported on this platform")
            }
            Self::Errno(errno) => write!(f, "{errno}"),
        }
    }
}

impl std::error::Error for SysError {}

pub type SysResult<T> = Result<T, SysError>;

/// Convert a byte offset or length to `off_t`.
///
/// Reports `EOVERFLOW` rather than panicking; only reachable on targets with a
/// 32-bit `off_t`.
fn to_off_t(value: u64) -> SysResult<libc::off_t> {
    libc::off_t::try_from(value).map_err(|_| SysError::Errno(Errno::EOVERFLOW))
}
