//! NBD transmission-phase wire format.
//!
//! See <https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md>.
//! Only simple replies are produced; the Linux kernel client never
//! negotiates structured replies.

use std::io;

pub const REQUEST_MAGIC: u32 = 0x2560_9513;
pub const SIMPLE_REPLY_MAGIC: u32 = 0x6744_6698;
pub const REQUEST_HEADER_LEN: usize = 28;
pub const SIMPLE_REPLY_HEADER_LEN: usize = 16;

// Transmission flags advertised by the server. FLUSH and FUA are never
// advertised: exports are write-through, so the kernel sends no FLUSH.
pub const FLAG_HAS_FLAGS: u16 = 1 << 0;
pub const FLAG_SEND_TRIM: u16 = 1 << 5;

// Error values defined by the protocol (numerically equal to Linux errno).
pub const EPERM: u32 = 1;
pub const EIO: u32 = 5;
pub const ENOMEM: u32 = 12;
pub const EINVAL: u32 = 22;
pub const ENOSPC: u32 = 28;
pub const EOVERFLOW: u32 = 75;
pub const ENOTSUP: u32 = 95;
pub const ESHUTDOWN: u32 = 108;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Read,
    Write,
    Disconnect,
    Flush,
    Trim,
    Unknown(u16),
}

impl Command {
    fn from_wire(value: u16) -> Self {
        match value {
            0 => Self::Read,
            1 => Self::Write,
            2 => Self::Disconnect,
            3 => Self::Flush,
            4 => Self::Trim,
            other => Self::Unknown(other),
        }
    }

    #[cfg(test)]
    fn to_wire(self) -> u16 {
        match self {
            Self::Read => 0,
            Self::Write => 1,
            Self::Disconnect => 2,
            Self::Flush => 3,
            Self::Trim => 4,
            Self::Unknown(other) => other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    pub flags: u16,
    pub command: Command,
    pub cookie: u64,
    pub offset: u64,
    pub length: u32,
}

impl Request {
    pub fn parse(buf: &[u8; REQUEST_HEADER_LEN]) -> io::Result<Self> {
        let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != REQUEST_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid NBD request magic {magic:#x}"),
            ));
        }
        Ok(Self {
            flags: u16::from_be_bytes([buf[4], buf[5]]),
            command: Command::from_wire(u16::from_be_bytes([buf[6], buf[7]])),
            cookie: u64::from_be_bytes(buf[8..16].try_into().expect("8-byte slice")),
            offset: u64::from_be_bytes(buf[16..24].try_into().expect("8-byte slice")),
            length: u32::from_be_bytes(buf[24..28].try_into().expect("4-byte slice")),
        })
    }

    #[cfg(test)]
    pub fn encode(&self) -> [u8; REQUEST_HEADER_LEN] {
        let mut buf = [0u8; REQUEST_HEADER_LEN];
        buf[0..4].copy_from_slice(&REQUEST_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&self.flags.to_be_bytes());
        buf[6..8].copy_from_slice(&self.command.to_wire().to_be_bytes());
        buf[8..16].copy_from_slice(&self.cookie.to_be_bytes());
        buf[16..24].copy_from_slice(&self.offset.to_be_bytes());
        buf[24..28].copy_from_slice(&self.length.to_be_bytes());
        buf
    }
}

pub fn encode_simple_reply(error: u32, cookie: u64) -> [u8; SIMPLE_REPLY_HEADER_LEN] {
    let mut buf = [0u8; SIMPLE_REPLY_HEADER_LEN];
    buf[0..4].copy_from_slice(&SIMPLE_REPLY_MAGIC.to_be_bytes());
    buf[4..8].copy_from_slice(&error.to_be_bytes());
    buf[8..16].copy_from_slice(&cookie.to_be_bytes());
    buf
}

/// Map an I/O error onto the error values the protocol allows.
pub fn error_code(err: &io::Error) -> u32 {
    match err.raw_os_error() {
        Some(libc::EPERM | libc::EROFS | libc::EACCES) => EPERM,
        Some(libc::ENOMEM) => ENOMEM,
        Some(libc::EINVAL) => EINVAL,
        Some(libc::ENOSPC) => ENOSPC,
        Some(libc::EOVERFLOW) => EOVERFLOW,
        Some(libc::EOPNOTSUPP) => ENOTSUP,
        Some(libc::ESHUTDOWN) => ESHUTDOWN,
        Some(_) => EIO,
        None => match err.kind() {
            io::ErrorKind::PermissionDenied => EPERM,
            io::ErrorKind::InvalidInput => EINVAL,
            io::ErrorKind::Unsupported => ENOTSUP,
            io::ErrorKind::OutOfMemory => ENOMEM,
            _ => EIO,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip() {
        let request = Request {
            flags: 1,
            command: Command::Write,
            cookie: 0x0102_0304_0506_0708,
            offset: 1 << 40,
            length: 4096,
        };
        assert_eq!(Request::parse(&request.encode()).unwrap(), request);
    }

    #[test]
    fn request_rejects_bad_magic() {
        let mut buf = Request {
            flags: 0,
            command: Command::Read,
            cookie: 1,
            offset: 0,
            length: 512,
        }
        .encode();
        buf[0] ^= 0xff;
        let err = Request::parse(&buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn unknown_command_is_preserved() {
        let buf = Request {
            flags: 0,
            command: Command::Unknown(42),
            cookie: 1,
            offset: 0,
            length: 0,
        }
        .encode();
        assert_eq!(Request::parse(&buf).unwrap().command, Command::Unknown(42));
    }

    #[test]
    fn simple_reply_layout() {
        let reply = encode_simple_reply(EINVAL, 7);
        assert_eq!(&reply[0..4], &SIMPLE_REPLY_MAGIC.to_be_bytes());
        assert_eq!(&reply[4..8], &EINVAL.to_be_bytes());
        assert_eq!(&reply[8..16], &7u64.to_be_bytes());
    }

    #[test]
    fn error_code_mapping() {
        let code = |errno| error_code(&io::Error::from_raw_os_error(errno));
        assert_eq!(code(libc::EROFS), EPERM);
        assert_eq!(code(libc::EINVAL), EINVAL);
        assert_eq!(code(libc::EOPNOTSUPP), ENOTSUP);
        assert_eq!(code(libc::ETIMEDOUT), EIO);
        assert_eq!(
            error_code(&io::Error::new(io::ErrorKind::PermissionDenied, "ro")),
            EPERM
        );
        assert_eq!(error_code(&io::Error::other("boom")), EIO);
    }
}
