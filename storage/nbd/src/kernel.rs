//! Attach an [`NbdExport`] to a kernel `/dev/nbdN` device.
//!
//! The device is configured through the generic-netlink `nbd` family
//! (Linux 4.12+) with one end of a Unix socketpair; the export is served on
//! the other end. The kernel takes over an already-negotiated connection, so
//! no NBD handshake is involved. Configuring devices requires
//! `CAP_SYS_ADMIN`.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::server::{serve, transmission_flags, NbdExport};

const NBD_GENL_FAMILY_NAME: &str = "nbd";
const NBD_GENL_VERSION: u8 = 1;
const NBD_CMD_CONNECT: u8 = 1;
const NBD_CMD_DISCONNECT: u8 = 2;
const NBD_ATTR_INDEX: u16 = 1;
const NBD_ATTR_SIZE_BYTES: u16 = 2;
const NBD_ATTR_BLOCK_SIZE_BYTES: u16 = 3;
const NBD_ATTR_SERVER_FLAGS: u16 = 5;
const NBD_ATTR_SOCKETS: u16 = 7;
const NBD_SOCK_ITEM: u16 = 1;
const NBD_SOCK_FD: u16 = 1;

const GENL_ID_CTRL: u16 = 0x10;
const CTRL_CMD_GETFAMILY: u8 = 3;
const CTRL_VERSION: u8 = 2;
const CTRL_ATTR_FAMILY_ID: u16 = 1;
const CTRL_ATTR_FAMILY_NAME: u16 = 2;

const NLMSG_HDR_LEN: usize = 16;
const GENL_HDR_LEN: usize = 4;
const NLA_HDR_LEN: usize = 4;
const NLMSG_ERROR: u16 = 2;
const NLMSG_DONE: u16 = 3;
const NLM_F_REQUEST: u16 = 1;
const NLM_F_ACK: u16 = 4;
const NLA_F_NESTED: u16 = 1 << 15;
const NLA_TYPE_MASK: u16 = !(NLA_F_NESTED | (1 << 14));

/// How long [`NbdDevice::disconnect`] waits for in-flight requests to drain.
const SERVER_EXIT_TIMEOUT: Duration = Duration::from_secs(5);

/// A kernel NBD device backed by an export served in this process.
///
/// Dropping a still-connected device disconnects it best-effort.
pub struct NbdDevice<E: NbdExport> {
    index: u32,
    export: Arc<E>,
    server: Option<JoinHandle<io::Result<()>>>,
}

impl<E: NbdExport> NbdDevice<E> {
    /// Connect `export` to a free kernel NBD device.
    ///
    /// The kernel picks the device index. Must be called within a Tokio
    /// runtime, which then serves the device's requests.
    pub async fn connect(export: Arc<E>) -> io::Result<Self> {
        // Run in a task so a cancelled caller cannot strand a connected
        // device: the finished `NbdDevice` is then dropped, which disconnects.
        tokio::spawn(Self::connect_owned(export))
            .await
            .map_err(io::Error::other)?
    }

    async fn connect_owned(export: Arc<E>) -> io::Result<Self> {
        let (kernel_end, server_end) = UnixStream::pair()?;
        server_end.set_nonblocking(true)?;
        let (reader, writer) = tokio::net::UnixStream::from_std(server_end)?.into_split();

        // Serve before configuring the device: the kernel may issue I/O as
        // soon as the connection is attached.
        let server = tokio::spawn(serve(Arc::clone(&export), reader, writer));

        let size_bytes = export.size_bytes();
        let block_size = export.block_size();
        let server_flags = transmission_flags(export.as_ref());
        let connected = tokio::task::spawn_blocking(move || {
            // The kernel keeps its own reference to the socket, so our copy
            // of the kernel end closes when this closure returns.
            connect_device(kernel_end.as_raw_fd(), size_bytes, block_size, server_flags)
        })
        .await
        .map_err(io::Error::other)?;

        let index = match connected {
            Ok(index) => index,
            Err(err) => {
                // The kernel end is closed, so the server sees EOF and exits.
                server.abort();
                return Err(err);
            }
        };
        tracing::debug!(index, "nbd device connected");
        Ok(Self {
            index,
            export,
            server: Some(server),
        })
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn device_path(&self) -> PathBuf {
        PathBuf::from(format!("/dev/nbd{}", self.index))
    }

    pub fn export(&self) -> &Arc<E> {
        &self.export
    }

    /// Disconnect the kernel device and wait for the server to drain.
    ///
    /// If the kernel disconnect fails, the device stays owned and dropping
    /// it retries the disconnect.
    pub async fn disconnect(mut self) -> io::Result<()> {
        // Run in a task so a cancelled caller cannot interrupt the teardown.
        tokio::spawn(async move { self.disconnect_owned().await })
            .await
            .map_err(io::Error::other)?
    }

    async fn disconnect_owned(&mut self) -> io::Result<()> {
        let index = self.index;
        if self.server.is_none() {
            return Ok(());
        }
        tokio::task::spawn_blocking(move || disconnect_device(index))
            .await
            .map_err(io::Error::other)??;
        let Some(server) = self.server.take() else {
            return Ok(());
        };

        // The kernel shuts its socket end down, so the server reads EOF and
        // returns after answering accepted requests.
        let abort = server.abort_handle();
        match tokio::time::timeout(SERVER_EXIT_TIMEOUT, server).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(err))) => {
                tracing::warn!(index, error = %err, "nbd server exited with error");
            }
            Ok(Err(err)) => tracing::warn!(index, error = %err, "nbd server task failed"),
            Err(_) => {
                tracing::warn!(index, "nbd server did not exit after disconnect; aborting");
                abort.abort();
            }
        }
        tracing::debug!(index, "nbd device disconnected");
        Ok(())
    }
}

impl<E: NbdExport> Drop for NbdDevice<E> {
    fn drop(&mut self) {
        // The detached server keeps answering until the disconnect lands.
        if self.server.take().is_none() {
            return;
        }
        let index = self.index;
        let disconnect = move || {
            if let Err(err) = disconnect_device(index) {
                tracing::warn!(index, error = %err, "failed to disconnect dropped nbd device");
            }
        };
        // Netlink requests block; keep them off async workers.
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(disconnect)),
            Err(_) => disconnect(),
        }
    }
}

fn connect_device(
    sock_fd: RawFd,
    size_bytes: u64,
    block_size: u32,
    server_flags: u16,
) -> io::Result<u32> {
    let mut nl = GenlSocket::open()?;
    let family = nl.resolve_family(NBD_GENL_FAMILY_NAME)?;

    let mut msg = GenlMessage::new(family, NBD_CMD_CONNECT, NBD_GENL_VERSION);
    msg.put_u64(NBD_ATTR_SIZE_BYTES, size_bytes);
    msg.put_u64(NBD_ATTR_BLOCK_SIZE_BYTES, u64::from(block_size));
    msg.put_u64(NBD_ATTR_SERVER_FLAGS, u64::from(server_flags));
    let sockets = msg.begin_nested(NBD_ATTR_SOCKETS);
    let item = msg.begin_nested(NBD_SOCK_ITEM);
    msg.put_u32(
        NBD_SOCK_FD,
        u32::try_from(sock_fd).map_err(io::Error::other)?,
    );
    msg.end_nested(item);
    msg.end_nested(sockets);

    let reply = nl.request(msg)?.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "nbd connect returned no reply")
    })?;
    let index = find_attr(&reply, NBD_ATTR_INDEX)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_ne_bytes)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "nbd connect reply has no index")
        })?;
    Ok(index)
}

fn disconnect_device(index: u32) -> io::Result<()> {
    let mut nl = GenlSocket::open()?;
    let family = nl.resolve_family(NBD_GENL_FAMILY_NAME)?;
    let mut msg = GenlMessage::new(family, NBD_CMD_DISCONNECT, NBD_GENL_VERSION);
    msg.put_u32(NBD_ATTR_INDEX, index);
    nl.request(msg)?;
    Ok(())
}

/// Builder for one generic-netlink request message.
struct GenlMessage {
    family: u16,
    buf: Vec<u8>,
}

impl GenlMessage {
    fn new(family: u16, cmd: u8, version: u8) -> Self {
        let mut buf = vec![0u8; NLMSG_HDR_LEN];
        buf.extend_from_slice(&[cmd, version, 0, 0]);
        Self { family, buf }
    }

    fn put_attr(&mut self, ty: u16, payload: &[u8]) {
        let len = u16::try_from(NLA_HDR_LEN + payload.len()).expect("netlink attribute too large");
        self.buf.extend_from_slice(&len.to_ne_bytes());
        self.buf.extend_from_slice(&ty.to_ne_bytes());
        self.buf.extend_from_slice(payload);
        self.pad();
    }

    fn put_u32(&mut self, ty: u16, value: u32) {
        self.put_attr(ty, &value.to_ne_bytes());
    }

    fn put_u64(&mut self, ty: u16, value: u64) {
        self.put_attr(ty, &value.to_ne_bytes());
    }

    fn put_str(&mut self, ty: u16, value: &str) {
        let mut payload = Vec::with_capacity(value.len() + 1);
        payload.extend_from_slice(value.as_bytes());
        payload.push(0);
        self.put_attr(ty, &payload);
    }

    /// Start a nested attribute; returns its offset for [`Self::end_nested`].
    fn begin_nested(&mut self, ty: u16) -> usize {
        let start = self.buf.len();
        self.buf.extend_from_slice(&[0, 0]);
        self.buf
            .extend_from_slice(&(ty | NLA_F_NESTED).to_ne_bytes());
        start
    }

    fn end_nested(&mut self, start: usize) {
        let len = u16::try_from(self.buf.len() - start).expect("netlink attribute too large");
        self.buf[start..start + 2].copy_from_slice(&len.to_ne_bytes());
    }

    fn pad(&mut self) {
        let padded = self.buf.len().next_multiple_of(4);
        self.buf.resize(padded, 0);
    }

    fn finish(mut self, seq: u32) -> Vec<u8> {
        let len = u32::try_from(self.buf.len()).expect("netlink message too large");
        self.buf[0..4].copy_from_slice(&len.to_ne_bytes());
        self.buf[4..6].copy_from_slice(&self.family.to_ne_bytes());
        self.buf[6..8].copy_from_slice(&(NLM_F_REQUEST | NLM_F_ACK).to_ne_bytes());
        self.buf[8..12].copy_from_slice(&seq.to_ne_bytes());
        // nlmsg_pid stays 0: the message is addressed to the kernel.
        self.buf
    }
}

struct GenlSocket {
    fd: OwnedFd,
    seq: u32,
}

impl GenlSocket {
    fn open() -> io::Result<Self> {
        // SAFETY: plain socket(2) call; the returned fd is owned below.
        let fd = unsafe {
            libc::socket(
                libc::AF_NETLINK,
                libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                libc::NETLINK_GENERIC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a freshly created descriptor owned by nobody else.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };

        // SAFETY: sockaddr_nl is plain old data; all-zero is a valid value.
        let mut addr: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
        addr.nl_family = libc::AF_NETLINK as libc::sa_family_t;
        // SAFETY: `addr` is a valid sockaddr_nl and the length matches it.
        let ret = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                std::ptr::addr_of!(addr).cast::<libc::sockaddr>(),
                std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { fd, seq: 0 })
    }

    fn resolve_family(&mut self, name: &str) -> io::Result<u16> {
        let mut msg = GenlMessage::new(GENL_ID_CTRL, CTRL_CMD_GETFAMILY, CTRL_VERSION);
        msg.put_str(CTRL_ATTR_FAMILY_NAME, name);
        let reply = self.request(msg).map_err(|err| {
            if err.raw_os_error() == Some(libc::ENOENT) {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "generic netlink family {name:?} not found; is the nbd kernel module loaded?"
                    ),
                )
            } else {
                err
            }
        })?;
        reply
            .as_deref()
            .and_then(|attrs| find_attr(attrs, CTRL_ATTR_FAMILY_ID))
            .and_then(|value| value.try_into().ok())
            .map(u16::from_ne_bytes)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("netlink family {name:?} reply has no id"),
                )
            })
    }

    /// Send `msg` and wait for the kernel's acknowledgement.
    ///
    /// Returns the attributes of the first data reply, if any.
    fn request(&mut self, msg: GenlMessage) -> io::Result<Option<Vec<u8>>> {
        self.seq = self.seq.wrapping_add(1);
        let seq = self.seq;
        let buf = msg.finish(seq);
        // SAFETY: `buf` is a valid, initialized buffer of `buf.len()` bytes.
        let sent = unsafe { libc::send(self.fd.as_raw_fd(), buf.as_ptr().cast(), buf.len(), 0) };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }

        let mut reply = None;
        let mut recv_buf = vec![0u8; 32 * 1024];
        loop {
            // SAFETY: `recv_buf` is valid for writes of `recv_buf.len()` bytes.
            let received = unsafe {
                libc::recv(
                    self.fd.as_raw_fd(),
                    recv_buf.as_mut_ptr().cast(),
                    recv_buf.len(),
                    0,
                )
            };
            if received < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
            let received = received as usize;
            for message in NetlinkMessages::new(&recv_buf[..received]) {
                let message = message?;
                if message.seq != seq {
                    continue;
                }
                match message.ty {
                    NLMSG_ERROR => {
                        let errno = message
                            .payload
                            .get(..4)
                            .and_then(|bytes| bytes.try_into().ok())
                            .map(i32::from_ne_bytes)
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "truncated netlink error message",
                                )
                            })?;
                        if errno == 0 {
                            return Ok(reply);
                        }
                        return Err(io::Error::from_raw_os_error(-errno));
                    }
                    NLMSG_DONE => return Ok(reply),
                    _ => {
                        if reply.is_none() {
                            reply = message.payload.get(GENL_HDR_LEN..).map(<[u8]>::to_vec);
                        }
                    }
                }
            }
        }
    }
}

struct NetlinkMessage<'a> {
    ty: u16,
    seq: u32,
    payload: &'a [u8],
}

/// Iterator over the netlink messages packed in one datagram.
struct NetlinkMessages<'a> {
    buf: &'a [u8],
}

impl<'a> NetlinkMessages<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }
}

impl<'a> Iterator for NetlinkMessages<'a> {
    type Item = io::Result<NetlinkMessage<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.len() < NLMSG_HDR_LEN {
            return None;
        }
        let len = u32::from_ne_bytes(self.buf[0..4].try_into().expect("4-byte slice")) as usize;
        if len < NLMSG_HDR_LEN || len > self.buf.len() {
            self.buf = &[];
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed netlink message length",
            )));
        }
        let message = NetlinkMessage {
            ty: u16::from_ne_bytes(self.buf[4..6].try_into().expect("2-byte slice")),
            seq: u32::from_ne_bytes(self.buf[8..12].try_into().expect("4-byte slice")),
            payload: &self.buf[NLMSG_HDR_LEN..len],
        };
        let next = len.next_multiple_of(4).min(self.buf.len());
        self.buf = &self.buf[next..];
        Some(Ok(message))
    }
}

/// Find the payload of top-level attribute `ty` in a packed attribute list.
fn find_attr(mut attrs: &[u8], ty: u16) -> Option<&[u8]> {
    while attrs.len() >= NLA_HDR_LEN {
        let len = u16::from_ne_bytes([attrs[0], attrs[1]]) as usize;
        let attr_ty = u16::from_ne_bytes([attrs[2], attrs[3]]) & NLA_TYPE_MASK;
        if len < NLA_HDR_LEN || len > attrs.len() {
            return None;
        }
        if attr_ty == ty {
            return Some(&attrs[NLA_HDR_LEN..len]);
        }
        attrs = &attrs[len.next_multiple_of(4).min(attrs.len())..];
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attr(ty: u16, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&((NLA_HDR_LEN + payload.len()) as u16).to_ne_bytes());
        out.extend_from_slice(&ty.to_ne_bytes());
        out.extend_from_slice(payload);
        out.resize(out.len().next_multiple_of(4), 0);
        out
    }

    #[test]
    fn connect_message_layout() {
        let mut msg = GenlMessage::new(0x20, NBD_CMD_CONNECT, NBD_GENL_VERSION);
        msg.put_u64(NBD_ATTR_SIZE_BYTES, 1 << 30);
        let sockets = msg.begin_nested(NBD_ATTR_SOCKETS);
        let item = msg.begin_nested(NBD_SOCK_ITEM);
        msg.put_u32(NBD_SOCK_FD, 7);
        msg.end_nested(item);
        msg.end_nested(sockets);
        let buf = msg.finish(9);

        assert_eq!(
            u32::from_ne_bytes(buf[0..4].try_into().unwrap()) as usize,
            buf.len()
        );
        assert_eq!(u16::from_ne_bytes(buf[4..6].try_into().unwrap()), 0x20);
        assert_eq!(
            u16::from_ne_bytes(buf[6..8].try_into().unwrap()),
            NLM_F_REQUEST | NLM_F_ACK
        );
        assert_eq!(u32::from_ne_bytes(buf[8..12].try_into().unwrap()), 9);
        assert_eq!(&buf[16..18], &[NBD_CMD_CONNECT, NBD_GENL_VERSION]);

        let fd_attr = attr(NBD_SOCK_FD, &7u32.to_ne_bytes());
        let item_attr = attr(NBD_SOCK_ITEM | NLA_F_NESTED, &fd_attr);
        let sockets_attr = attr(NBD_ATTR_SOCKETS | NLA_F_NESTED, &item_attr);
        let mut expected = attr(NBD_ATTR_SIZE_BYTES, &(1u64 << 30).to_ne_bytes());
        expected.extend_from_slice(&sockets_attr);
        assert_eq!(&buf[NLMSG_HDR_LEN + GENL_HDR_LEN..], expected.as_slice());
    }

    #[test]
    fn string_attributes_are_nul_terminated_and_padded() {
        let mut msg = GenlMessage::new(GENL_ID_CTRL, CTRL_CMD_GETFAMILY, CTRL_VERSION);
        msg.put_str(CTRL_ATTR_FAMILY_NAME, "nbd");
        let buf = msg.finish(1);
        assert_eq!(
            &buf[NLMSG_HDR_LEN + GENL_HDR_LEN..],
            attr(CTRL_ATTR_FAMILY_NAME, b"nbd\0").as_slice()
        );
        assert_eq!(buf.len() % 4, 0);
    }

    #[test]
    fn find_attr_skips_padding_and_nested_flag() {
        let mut attrs = attr(CTRL_ATTR_FAMILY_NAME, b"nbd\0\0");
        attrs.extend_from_slice(&attr(
            CTRL_ATTR_FAMILY_ID | NLA_F_NESTED,
            &0x21u16.to_ne_bytes(),
        ));
        assert_eq!(
            find_attr(&attrs, CTRL_ATTR_FAMILY_ID),
            Some(&0x21u16.to_ne_bytes()[..])
        );
        assert_eq!(find_attr(&attrs, 9), None);
        assert_eq!(find_attr(&[8, 0, 1], CTRL_ATTR_FAMILY_ID), None);
    }

    #[test]
    fn netlink_messages_iterates_and_rejects_bad_lengths() {
        let mut datagram = Vec::new();
        for (ty, seq) in [(0x20u16, 1u32), (NLMSG_ERROR, 1)] {
            datagram.extend_from_slice(&20u32.to_ne_bytes());
            datagram.extend_from_slice(&ty.to_ne_bytes());
            datagram.extend_from_slice(&0u16.to_ne_bytes());
            datagram.extend_from_slice(&seq.to_ne_bytes());
            datagram.extend_from_slice(&0u32.to_ne_bytes());
            datagram.extend_from_slice(&0i32.to_ne_bytes());
        }
        let messages: Vec<_> = NetlinkMessages::new(&datagram)
            .map(Result::unwrap)
            .map(|m| (m.ty, m.seq, m.payload.len()))
            .collect();
        assert_eq!(messages, vec![(0x20, 1, 4), (NLMSG_ERROR, 1, 4)]);

        datagram[0..4].copy_from_slice(&999u32.to_ne_bytes());
        assert!(NetlinkMessages::new(&datagram).next().unwrap().is_err());
    }
}
