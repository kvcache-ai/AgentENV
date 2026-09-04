//! Extended attributes.
//!
//! Darwin's xattr calls carry two extra parameters (`position` and `options`)
//! that Linux's do not, so every one of the eight entry points needs a
//! per-platform shim. What does *not* need duplicating is the awkward part: the
//! two-phase "ask for the size, then read" dance and the parsing of the
//! NUL-separated name list. Those live here once and are shared by both
//! platforms — before this module they existed as two separately written,
//! subtly different copies in `backend/local.rs` and
//! `backend/cache/cached_fs.rs`.
//!
//! Names and paths are taken as `&CStr` so that the "reject an interior NUL"
//! policy, and the error context that goes with it, stay with the caller.

use std::ffi::CStr;
use std::os::fd::BorrowedFd;

use nix::errno::Errno;

use super::{SysError, SysResult};

/// Read one attribute by descriptor.
pub fn fgetxattr(fd: BorrowedFd<'_>, name: &CStr) -> SysResult<Vec<u8>> {
    read_sized(|buf| imp::fgetxattr(fd, name, buf))
}

/// List attribute names by descriptor.
pub fn flistxattr(fd: BorrowedFd<'_>) -> SysResult<Vec<String>> {
    parse_nul_list(&read_sized(|buf| imp::flistxattr(fd, buf))?)
}

/// Write one attribute by descriptor.
///
/// `flags` carries Linux's `XATTR_*` semantics, and **only `0` is portable**.
/// The numeric values do not line up: Linux uses 1 for `XATTR_CREATE` and 2 for
/// `XATTR_REPLACE`, while Darwin uses 1 for `XATTR_NOFOLLOW`, 2 for
/// `XATTR_CREATE` and 4 for `XATTR_REPLACE`. Forwarding a Linux flag to Darwin
/// would therefore ask for a different operation — `XATTR_CREATE` would arrive
/// as `XATTR_NOFOLLOW`, dropping the create-only guarantee without any error.
///
/// Rather than reinterpret the bits, the Darwin arms reject a non-zero value
/// with [`SysError::Unsupported`](super::SysError::Unsupported). Every caller in
/// this crate passes 0, so nothing is lost today; a caller that genuinely needs
/// create/replace semantics should replace this parameter with a portable enum
/// and translate it per arm, rather than have the current one silently mean two
/// different things.
pub fn fsetxattr(fd: BorrowedFd<'_>, name: &CStr, value: &[u8], flags: i32) -> SysResult<()> {
    imp::fsetxattr(fd, name, value, flags)
}

/// Remove one attribute by descriptor.
pub fn fremovexattr(fd: BorrowedFd<'_>, name: &CStr) -> SysResult<()> {
    imp::fremovexattr(fd, name)
}

/// Read one attribute by path.
pub fn getxattr(path: &CStr, name: &CStr) -> SysResult<Vec<u8>> {
    read_sized(|buf| imp::getxattr(path, name, buf))
}

/// List attribute names by path.
pub fn listxattr(path: &CStr) -> SysResult<Vec<String>> {
    parse_nul_list(&read_sized(|buf| imp::listxattr(path, buf))?)
}

/// Write one attribute by path. See [`fsetxattr`] about `flags`.
pub fn setxattr(path: &CStr, name: &CStr, value: &[u8], flags: i32) -> SysResult<()> {
    imp::setxattr(path, name, value, flags)
}

/// Remove one attribute by path.
pub fn removexattr(path: &CStr, name: &CStr) -> SysResult<()> {
    imp::removexattr(path, name)
}

/// Run the two-phase size query shared by every reading xattr call.
///
/// `raw` is called with `None` to ask for the required length and with
/// `Some(buf)` to fill it. The value can change between the two calls, and the
/// two directions need different handling:
///
/// - It **shrank**: the fill call succeeds and reports fewer bytes than asked
///   for, so the buffer is truncated to what it reported.
/// - It **grew**: the buffer no longer fits and the fill call fails with
///   `ERANGE`. Retrying re-queries the new size, so the read is retried a
///   bounded number of times before giving up. Without this a concurrent
///   attribute update turns an otherwise valid read into an error.
///
/// The retry is bounded rather than unbounded: a value being rewritten in a
/// tight loop would otherwise spin here forever, and reporting `ERANGE` to the
/// caller is the honest outcome once several attempts in a row have lost the
/// race.
fn read_sized(raw: impl Fn(Option<&mut [u8]>) -> SysResult<usize>) -> SysResult<Vec<u8>> {
    /// Enough to ride out a racing writer without spinning on a pathological one.
    const MAX_ATTEMPTS: usize = 4;

    for _ in 0..MAX_ATTEMPTS {
        let need = raw(None)?;
        if need == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; need];
        match raw(Some(&mut buf)) {
            Ok(got) => {
                buf.truncate(got.min(need));
                return Ok(buf);
            }
            // Grew between the size query and the fill; ask again.
            Err(SysError::Errno(Errno::ERANGE)) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(SysError::Errno(Errno::ERANGE))
}

/// Split the NUL-separated name list returned by `listxattr` / `flistxattr`.
///
/// Empty runs are dropped, which covers both the trailing NUL that terminates
/// the last name and a zero-length buffer.
///
/// A name that is not valid UTF-8 is reported as `EILSEQ` rather than replaced
/// lossily. Both platforms allow arbitrary bytes in an attribute name, and the
/// surrounding API hands names back as `String` and builds a `CStr` from them to
/// read or remove an attribute — so a name containing U+FFFD in place of the
/// original bytes would not round-trip, and would quietly address a different
/// attribute or none at all. Failing is recoverable; fabricating a name is not.
fn parse_nul_list(buf: &[u8]) -> SysResult<Vec<String>> {
    buf.split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(|name| {
            std::str::from_utf8(name)
                .map(str::to_owned)
                .map_err(|_| SysError::Errno(Errno::EILSEQ))
        })
        .collect()
}

#[cfg(target_os = "linux")]
mod imp {
    use std::ffi::CStr;
    use std::os::fd::{AsRawFd, BorrowedFd};

    use super::super::{SysError, SysResult};

    /// Split a `read_sized` buffer argument into the raw pointer and length a
    /// query-or-fill xattr call expects. `None` becomes a null pointer with
    /// length 0, which every xattr call treats as "report the size".
    fn dst(buf: Option<&mut [u8]>) -> (*mut libc::c_void, usize) {
        match buf {
            Some(buf) => (buf.as_mut_ptr() as *mut libc::c_void, buf.len()),
            None => (std::ptr::null_mut(), 0),
        }
    }

    pub(super) fn fgetxattr(
        fd: BorrowedFd<'_>,
        name: &CStr,
        buf: Option<&mut [u8]>,
    ) -> SysResult<usize> {
        let (ptr, len) = dst(buf);
        // SAFETY: `fd` and `name` are live; `ptr` is valid for `len` bytes, or
        // null with `len == 0`.
        let ret = unsafe { libc::fgetxattr(fd.as_raw_fd(), name.as_ptr(), ptr, len) };
        // A negative return means the call failed; `try_from` rejects it and we
        // read `errno` while it is still fresh.
        usize::try_from(ret).map_err(|_| SysError::last())
    }

    pub(super) fn flistxattr(fd: BorrowedFd<'_>, buf: Option<&mut [u8]>) -> SysResult<usize> {
        let (ptr, len) = dst(buf);
        // SAFETY: as above; the list is written as `c_char` but the byte layout
        // is identical.
        let ret = unsafe { libc::flistxattr(fd.as_raw_fd(), ptr as *mut libc::c_char, len) };
        // A negative return means the call failed; `try_from` rejects it and we
        // read `errno` while it is still fresh.
        usize::try_from(ret).map_err(|_| SysError::last())
    }

    pub(super) fn fsetxattr(
        fd: BorrowedFd<'_>,
        name: &CStr,
        value: &[u8],
        flags: i32,
    ) -> SysResult<()> {
        // SAFETY: `fd`, `name` and `value` are live for the call.
        let ret = unsafe {
            libc::fsetxattr(
                fd.as_raw_fd(),
                name.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                flags,
            )
        };
        if ret == 0 {
            return Ok(());
        }
        Err(SysError::last())
    }

    pub(super) fn fremovexattr(fd: BorrowedFd<'_>, name: &CStr) -> SysResult<()> {
        // SAFETY: `fd` and `name` are live for the call.
        let ret = unsafe { libc::fremovexattr(fd.as_raw_fd(), name.as_ptr()) };
        if ret == 0 {
            return Ok(());
        }
        Err(SysError::last())
    }

    pub(super) fn getxattr(path: &CStr, name: &CStr, buf: Option<&mut [u8]>) -> SysResult<usize> {
        let (ptr, len) = dst(buf);
        // SAFETY: `path` and `name` are live; `ptr` is valid for `len` bytes.
        let ret = unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), ptr, len) };
        // A negative return means the call failed; `try_from` rejects it and we
        // read `errno` while it is still fresh.
        usize::try_from(ret).map_err(|_| SysError::last())
    }

    pub(super) fn listxattr(path: &CStr, buf: Option<&mut [u8]>) -> SysResult<usize> {
        let (ptr, len) = dst(buf);
        // SAFETY: as above.
        let ret = unsafe { libc::listxattr(path.as_ptr(), ptr as *mut libc::c_char, len) };
        // A negative return means the call failed; `try_from` rejects it and we
        // read `errno` while it is still fresh.
        usize::try_from(ret).map_err(|_| SysError::last())
    }

    pub(super) fn setxattr(path: &CStr, name: &CStr, value: &[u8], flags: i32) -> SysResult<()> {
        // SAFETY: all three pointers are live for the call.
        let ret = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                flags,
            )
        };
        if ret == 0 {
            return Ok(());
        }
        Err(SysError::last())
    }

    pub(super) fn removexattr(path: &CStr, name: &CStr) -> SysResult<()> {
        // SAFETY: both pointers are live for the call.
        let ret = unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) };
        if ret == 0 {
            return Ok(());
        }
        Err(SysError::last())
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use std::ffi::CStr;
    use std::os::fd::{AsRawFd, BorrowedFd};

    use super::super::{SysError, SysResult};

    /// Darwin supports reading an attribute from a byte offset. Nothing in this
    /// crate uses partial attributes, so every call starts at 0.
    const POSITION: u32 = 0;

    /// Darwin's `options` bitmask. 0 means "follow symlinks, no create/replace
    /// constraint", matching the Linux calls being emulated.
    const NO_OPTIONS: i32 = 0;

    /// See the Linux arm: `None` becomes a null pointer with length 0.
    fn dst(buf: Option<&mut [u8]>) -> (*mut libc::c_void, usize) {
        match buf {
            Some(buf) => (buf.as_mut_ptr() as *mut libc::c_void, buf.len()),
            None => (std::ptr::null_mut(), 0),
        }
    }

    /// Refuse a `flags` value that cannot be honoured here.
    ///
    /// The parameter carries Linux's `XATTR_*` numbering, and Darwin's differs:
    /// Linux `XATTR_CREATE` (1) is Darwin `XATTR_NOFOLLOW`, and Linux
    /// `XATTR_REPLACE` (2) is Darwin `XATTR_CREATE`. Passing the value straight
    /// through would perform a *different, silently successful* operation, so
    /// anything but 0 is refused. See [`super::fsetxattr`] for the full rationale.
    fn reject_nonportable_flags(flags: i32) -> SysResult<()> {
        if flags != 0 {
            return Err(SysError::Unsupported(
                "non-zero xattr flags on this platform",
            ));
        }
        Ok(())
    }

    pub(super) fn fgetxattr(
        fd: BorrowedFd<'_>,
        name: &CStr,
        buf: Option<&mut [u8]>,
    ) -> SysResult<usize> {
        let (ptr, len) = dst(buf);
        // SAFETY: `fd` and `name` are live; `ptr` is valid for `len` bytes, or
        // null with `len == 0`.
        let ret = unsafe {
            libc::fgetxattr(
                fd.as_raw_fd(),
                name.as_ptr(),
                ptr,
                len,
                POSITION,
                NO_OPTIONS,
            )
        };
        // A negative return means the call failed; `try_from` rejects it and we
        // read `errno` while it is still fresh.
        usize::try_from(ret).map_err(|_| SysError::last())
    }

    pub(super) fn flistxattr(fd: BorrowedFd<'_>, buf: Option<&mut [u8]>) -> SysResult<usize> {
        let (ptr, len) = dst(buf);
        // SAFETY: as above.
        let ret =
            unsafe { libc::flistxattr(fd.as_raw_fd(), ptr as *mut libc::c_char, len, NO_OPTIONS) };
        // A negative return means the call failed; `try_from` rejects it and we
        // read `errno` while it is still fresh.
        usize::try_from(ret).map_err(|_| SysError::last())
    }

    pub(super) fn fsetxattr(
        fd: BorrowedFd<'_>,
        name: &CStr,
        value: &[u8],
        flags: i32,
    ) -> SysResult<()> {
        reject_nonportable_flags(flags)?;
        // SAFETY: `fd`, `name` and `value` are live for the call.
        let ret = unsafe {
            libc::fsetxattr(
                fd.as_raw_fd(),
                name.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                POSITION,
                flags,
            )
        };
        if ret == 0 {
            return Ok(());
        }
        Err(SysError::last())
    }

    pub(super) fn fremovexattr(fd: BorrowedFd<'_>, name: &CStr) -> SysResult<()> {
        // SAFETY: `fd` and `name` are live for the call.
        let ret = unsafe { libc::fremovexattr(fd.as_raw_fd(), name.as_ptr(), NO_OPTIONS) };
        if ret == 0 {
            return Ok(());
        }
        Err(SysError::last())
    }

    pub(super) fn getxattr(path: &CStr, name: &CStr, buf: Option<&mut [u8]>) -> SysResult<usize> {
        let (ptr, len) = dst(buf);
        // SAFETY: `path` and `name` are live; `ptr` is valid for `len` bytes.
        let ret =
            unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), ptr, len, POSITION, NO_OPTIONS) };
        // A negative return means the call failed; `try_from` rejects it and we
        // read `errno` while it is still fresh.
        usize::try_from(ret).map_err(|_| SysError::last())
    }

    pub(super) fn listxattr(path: &CStr, buf: Option<&mut [u8]>) -> SysResult<usize> {
        let (ptr, len) = dst(buf);
        // SAFETY: as above.
        let ret =
            unsafe { libc::listxattr(path.as_ptr(), ptr as *mut libc::c_char, len, NO_OPTIONS) };
        // A negative return means the call failed; `try_from` rejects it and we
        // read `errno` while it is still fresh.
        usize::try_from(ret).map_err(|_| SysError::last())
    }

    pub(super) fn setxattr(path: &CStr, name: &CStr, value: &[u8], flags: i32) -> SysResult<()> {
        reject_nonportable_flags(flags)?;
        // SAFETY: all three pointers are live for the call.
        let ret = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                POSITION,
                flags,
            )
        };
        if ret == 0 {
            return Ok(());
        }
        Err(SysError::last())
    }

    pub(super) fn removexattr(path: &CStr, name: &CStr) -> SysResult<()> {
        // SAFETY: both pointers are live for the call.
        let ret = unsafe { libc::removexattr(path.as_ptr(), name.as_ptr(), NO_OPTIONS) };
        if ret == 0 {
            return Ok(());
        }
        Err(SysError::last())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    use std::ffi::CStr;
    use std::os::fd::BorrowedFd;

    use super::super::{SysError, SysResult};

    macro_rules! unsupported {
        ($name:ident) => {
            Err(SysError::Unsupported(stringify!($name)))
        };
    }

    pub(super) fn fgetxattr(
        _fd: BorrowedFd<'_>,
        _name: &CStr,
        _buf: Option<&mut [u8]>,
    ) -> SysResult<usize> {
        unsupported!(fgetxattr)
    }

    pub(super) fn flistxattr(_fd: BorrowedFd<'_>, _buf: Option<&mut [u8]>) -> SysResult<usize> {
        unsupported!(flistxattr)
    }

    pub(super) fn fsetxattr(
        _fd: BorrowedFd<'_>,
        _name: &CStr,
        _value: &[u8],
        _flags: i32,
    ) -> SysResult<()> {
        unsupported!(fsetxattr)
    }

    pub(super) fn fremovexattr(_fd: BorrowedFd<'_>, _name: &CStr) -> SysResult<()> {
        unsupported!(fremovexattr)
    }

    pub(super) fn getxattr(
        _path: &CStr,
        _name: &CStr,
        _buf: Option<&mut [u8]>,
    ) -> SysResult<usize> {
        unsupported!(getxattr)
    }

    pub(super) fn listxattr(_path: &CStr, _buf: Option<&mut [u8]>) -> SysResult<usize> {
        unsupported!(listxattr)
    }

    pub(super) fn setxattr(
        _path: &CStr,
        _name: &CStr,
        _value: &[u8],
        _flags: i32,
    ) -> SysResult<()> {
        unsupported!(setxattr)
    }

    pub(super) fn removexattr(_path: &CStr, _name: &CStr) -> SysResult<()> {
        unsupported!(removexattr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_nul_list_drops_the_trailing_terminator() {
        assert_eq!(
            parse_nul_list(b"user.a\0user.bb\0").expect("valid utf-8"),
            vec!["user.a".to_string(), "user.bb".to_string()]
        );
    }

    #[test]
    fn parse_nul_list_handles_empty_and_unterminated_input() {
        assert!(parse_nul_list(b"").expect("empty").is_empty());
        assert!(parse_nul_list(b"\0\0")
            .expect("only terminators")
            .is_empty());
        // An unterminated tail is still reported rather than silently dropped.
        assert_eq!(
            parse_nul_list(b"user.a").expect("valid utf-8"),
            vec!["user.a".to_string()]
        );
    }

    /// A name that is not UTF-8 must fail rather than come back with U+FFFD
    /// substituted: the caller turns these names back into a `CStr` to read or
    /// remove the attribute, and a substituted name addresses something else.
    #[test]
    fn parse_nul_list_rejects_a_non_utf8_name() {
        let err = parse_nul_list(b"user.ok\0user.\xff\xfe\0")
            .expect_err("a non-utf8 name must be rejected");
        assert!(!err.is_unsupported(), "expected an errno, got {err}");
    }

    #[test]
    fn read_sized_skips_the_second_call_when_empty() {
        let calls = std::cell::Cell::new(0);
        let out = read_sized(|_buf| {
            calls.set(calls.get() + 1);
            Ok(0)
        })
        .expect("read_sized");
        assert!(out.is_empty());
        assert_eq!(calls.get(), 1, "a zero-size attribute needs one call");
    }

    #[test]
    fn read_sized_truncates_to_the_second_calls_result() {
        // Size query reports 8, the fill reports only 3: the attribute shrank
        // between the calls and the result must follow the fill.
        let out = read_sized(|buf| match buf {
            None => Ok(8),
            Some(buf) => {
                buf[..3].copy_from_slice(b"abc");
                Ok(3)
            }
        })
        .expect("read_sized");
        assert_eq!(out, b"abc");
    }

    #[test]
    fn read_sized_clamps_an_overlong_fill_result() {
        let out = read_sized(|buf| match buf {
            None => Ok(4),
            Some(_) => Ok(99),
        })
        .expect("read_sized");
        assert_eq!(out.len(), 4, "must not report more than was allocated");
    }

    /// The attribute grows between the size query and the fill, so the fill
    /// reports `ERANGE`. Re-querying picks up the new size instead of failing a
    /// read that is perfectly valid on the next attempt.
    #[test]
    fn read_sized_retries_when_the_value_grows_between_the_calls() {
        let attempt = std::cell::Cell::new(0);
        let out = read_sized(|buf| match buf {
            // The first size query is already stale by the time it is used.
            None => Ok(if attempt.get() == 0 { 3 } else { 6 }),
            Some(buf) => {
                if attempt.get() == 0 {
                    attempt.set(1);
                    return Err(SysError::Errno(Errno::ERANGE));
                }
                buf[..6].copy_from_slice(b"abcdef");
                Ok(6)
            }
        })
        .expect("a grown attribute must be re-read, not reported as an error");
        assert_eq!(out, b"abcdef");
    }

    /// A value rewritten on every attempt must not spin forever; after a bounded
    /// number of tries the race is reported rather than retried.
    #[test]
    fn read_sized_gives_up_on_an_endlessly_growing_value() {
        let calls = std::cell::Cell::new(0);
        let err = read_sized(|buf| match buf {
            None => Ok(4),
            Some(_) => {
                calls.set(calls.get() + 1);
                Err(SysError::Errno(Errno::ERANGE))
            }
        })
        .expect_err("an unwinnable race must terminate with an error");
        assert!(!err.is_unsupported(), "expected an errno, got {err}");
        assert!(
            calls.get() > 1 && calls.get() <= 8,
            "expected a small bounded number of attempts, got {}",
            calls.get()
        );
    }

    /// Any other error from the fill is terminal — only `ERANGE` means "the size
    /// moved under us".
    #[test]
    fn read_sized_does_not_retry_other_errors() {
        let calls = std::cell::Cell::new(0);
        let err = read_sized(|buf| match buf {
            None => Ok(4),
            Some(_) => {
                calls.set(calls.get() + 1);
                Err(SysError::Errno(Errno::EPERM))
            }
        })
        .expect_err("EPERM must propagate");
        assert!(!err.is_unsupported(), "expected an errno, got {err}");
        assert_eq!(calls.get(), 1, "a non-ERANGE failure must not be retried");
    }
}
