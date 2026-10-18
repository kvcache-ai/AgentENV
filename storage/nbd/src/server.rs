use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::protocol::{
    encode_simple_reply, error_code, Command, Request, FLAG_HAS_FLAGS, FLAG_SEND_TRIM,
    REQUEST_HEADER_LEN,
};

/// Largest request accepted. The Linux client caps requests at
/// `max_hw_sectors` (65536 sectors = 32 MiB).
const MAX_REQUEST_LEN: u32 = 32 << 20;

/// Socket buffering, so queued request headers and reply acks are batched
/// into fewer syscalls. Larger payloads bypass the buffers.
const SOCKET_BUFFER_LEN: usize = 64 << 10;

/// Requests executing concurrently per connection; matches the kernel
/// client's default tag depth.
const MAX_IN_FLIGHT: usize = 128;

/// A write-through block export served over NBD.
#[async_trait]
pub trait NbdExport: Send + Sync + 'static {
    /// Export capacity in bytes.
    fn size_bytes(&self) -> u64;
    /// Logical block size advertised to the client.
    fn block_size(&self) -> u32;
    /// Whether TRIM is advertised to the client.
    fn supports_trim(&self) -> bool;
    /// Read exactly `len` bytes at `offset`.
    async fn read(&self, offset: u64, len: usize) -> io::Result<Bytes>;
    async fn write(&self, offset: u64, data: &[u8]) -> io::Result<()>;
    async fn trim(&self, offset: u64, len: usize) -> io::Result<()>;
}

/// Transmission flags describing `export`.
pub(crate) fn transmission_flags<E: NbdExport>(export: &E) -> u16 {
    let mut flags = FLAG_HAS_FLAGS;
    if export.supports_trim() {
        flags |= FLAG_SEND_TRIM;
    }
    flags
}

struct Reply {
    cookie: u64,
    error: u32,
    data: Option<Bytes>,
}

/// Serve transmission-phase requests read from `reader`, replying on
/// `writer`, until the client sends `NBD_CMD_DISC` or closes the connection.
///
/// Requests run concurrently and replies are sent in completion order, which
/// the protocol permits. Requests accepted before disconnect are answered
/// before the function returns.
pub(crate) async fn serve<E, R, W>(export: Arc<E>, reader: R, writer: W) -> io::Result<()>
where
    E: NbdExport,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut reader = BufReader::with_capacity(SOCKET_BUFFER_LEN, reader);
    let (tx, rx) = mpsc::channel(MAX_IN_FLIGHT);
    let writer_task = tokio::spawn(write_replies(writer, rx));

    let mut handlers = JoinSet::new();
    let result = read_requests(&export, &mut reader, &tx, &mut handlers).await;
    while let Some(joined) = handlers.join_next().await {
        log_handler_panic(joined);
    }
    drop(tx);

    let write_result = writer_task.await.map_err(io::Error::other)?;
    result.and(write_result)
}

async fn read_requests<E, R>(
    export: &Arc<E>,
    reader: &mut R,
    tx: &mpsc::Sender<Reply>,
    handlers: &mut JoinSet<()>,
) -> io::Result<()>
where
    E: NbdExport,
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; REQUEST_HEADER_LEN];
    loop {
        // EOF between requests is a clean close; EOF inside a header is not.
        let first = reader.read(&mut header).await?;
        if first == 0 {
            return Ok(());
        }
        reader.read_exact(&mut header[first..]).await?;
        let request = Request::parse(&header)?;
        if request.command == Command::Disconnect {
            return Ok(());
        }
        if request.length > MAX_REQUEST_LEN
            && matches!(request.command, Command::Read | Command::Write)
        {
            // An oversized write payload cannot be skipped safely, so treat
            // either direction as a protocol violation.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "NBD request length {} exceeds {MAX_REQUEST_LEN}",
                    request.length
                ),
            ));
        }
        let mut payload = Vec::new();
        if request.command == Command::Write {
            let len = request.length as usize;
            payload.reserve_exact(len);
            (&mut *reader)
                .take(u64::from(request.length))
                .read_to_end(&mut payload)
                .await?;
            if payload.len() != len {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }

        while let Some(joined) = handlers.try_join_next() {
            log_handler_panic(joined);
        }
        while handlers.len() >= MAX_IN_FLIGHT {
            if let Some(joined) = handlers.join_next().await {
                log_handler_panic(joined);
            }
        }

        let export = Arc::clone(export);
        let tx = tx.clone();
        handlers.spawn(async move {
            let reply = handle_request(export.as_ref(), request, payload).await;
            // A closed channel means the reply stream already failed; the
            // read side observes the same connection error.
            let _ = tx.send(reply).await;
        });
    }
}

async fn handle_request<E: NbdExport>(export: &E, request: Request, payload: Vec<u8>) -> Reply {
    let len = request.length as usize;
    let result = match request.command {
        Command::Read => export.read(request.offset, len).await.and_then(|data| {
            if data.len() == len {
                Ok(Some(data))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("short read: expected {len} bytes, got {}", data.len()),
                ))
            }
        }),
        Command::Write => export.write(request.offset, &payload).await.map(|()| None),
        Command::Trim => export.trim(request.offset, len).await.map(|()| None),
        Command::Flush | Command::Disconnect | Command::Unknown(_) => {
            Err(io::Error::from_raw_os_error(libc::EINVAL))
        }
    };

    match result {
        Ok(data) => Reply {
            cookie: request.cookie,
            error: 0,
            data,
        },
        Err(err) => {
            tracing::debug!(
                command = ?request.command,
                offset = request.offset,
                length = request.length,
                error = %err,
                "nbd request failed"
            );
            Reply {
                cookie: request.cookie,
                error: error_code(&err),
                data: None,
            }
        }
    }
}

async fn write_replies<W>(writer: W, mut rx: mpsc::Receiver<Reply>) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut writer = BufWriter::with_capacity(SOCKET_BUFFER_LEN, writer);
    while let Some(reply) = rx.recv().await {
        writer
            .write_all(&encode_simple_reply(reply.error, reply.cookie))
            .await?;
        if let Some(data) = &reply.data {
            writer.write_all(data).await?;
        }
        // Batch replies that are already queued into one send.
        if rx.is_empty() {
            writer.flush().await?;
        }
    }
    writer.flush().await
}

fn log_handler_panic(joined: Result<(), tokio::task::JoinError>) {
    if let Err(err) = joined {
        tracing::error!(error = %err, "nbd request handler panicked; request left unanswered");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{EINVAL, EPERM, SIMPLE_REPLY_HEADER_LEN, SIMPLE_REPLY_MAGIC};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::io::DuplexStream;

    struct MemExport {
        data: Mutex<Vec<u8>>,
        read_only: bool,
        /// Delay reads at this offset so later requests complete first.
        slow_offset: Option<u64>,
    }

    impl MemExport {
        fn new(size: usize) -> Self {
            Self {
                data: Mutex::new(vec![0; size]),
                read_only: false,
                slow_offset: None,
            }
        }

        fn check_range(&self, offset: u64, len: usize) -> io::Result<std::ops::Range<usize>> {
            let size = self.data.lock().unwrap().len();
            let start = usize::try_from(offset).map_err(|_| einval())?;
            let end = start.checked_add(len).ok_or_else(einval)?;
            if end > size {
                return Err(einval());
            }
            Ok(start..end)
        }
    }

    fn einval() -> io::Error {
        io::Error::from_raw_os_error(libc::EINVAL)
    }

    #[async_trait]
    impl NbdExport for MemExport {
        fn size_bytes(&self) -> u64 {
            self.data.lock().unwrap().len() as u64
        }

        fn block_size(&self) -> u32 {
            512
        }

        fn supports_trim(&self) -> bool {
            !self.read_only
        }

        async fn read(&self, offset: u64, len: usize) -> io::Result<Bytes> {
            let range = self.check_range(offset, len)?;
            if self.slow_offset == Some(offset) {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(Bytes::copy_from_slice(&self.data.lock().unwrap()[range]))
        }

        async fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            if self.read_only {
                return Err(io::Error::from_raw_os_error(libc::EROFS));
            }
            let range = self.check_range(offset, data.len())?;
            self.data.lock().unwrap()[range].copy_from_slice(data);
            Ok(())
        }

        async fn trim(&self, offset: u64, len: usize) -> io::Result<()> {
            let range = self.check_range(offset, len)?;
            self.data.lock().unwrap()[range].fill(0);
            Ok(())
        }
    }

    struct Client {
        stream: DuplexStream,
    }

    impl Client {
        async fn send(&mut self, command: Command, cookie: u64, offset: u64, length: u32) {
            let request = Request {
                flags: 0,
                command,
                cookie,
                offset,
                length,
            };
            self.stream.write_all(&request.encode()).await.unwrap();
        }

        async fn write(&mut self, cookie: u64, offset: u64, data: &[u8]) {
            self.send(Command::Write, cookie, offset, data.len() as u32)
                .await;
            self.stream.write_all(data).await.unwrap();
        }

        /// Read one simple reply header, returning `(error, cookie)`.
        async fn reply(&mut self) -> (u32, u64) {
            let mut header = [0u8; SIMPLE_REPLY_HEADER_LEN];
            self.stream.read_exact(&mut header).await.unwrap();
            assert_eq!(&header[0..4], &SIMPLE_REPLY_MAGIC.to_be_bytes());
            let error = u32::from_be_bytes(header[4..8].try_into().unwrap());
            let cookie = u64::from_be_bytes(header[8..16].try_into().unwrap());
            (error, cookie)
        }

        async fn data(&mut self, len: usize) -> Vec<u8> {
            let mut buf = vec![0u8; len];
            self.stream.read_exact(&mut buf).await.unwrap();
            buf
        }
    }

    fn start(
        export: MemExport,
    ) -> (
        Arc<MemExport>,
        Client,
        tokio::task::JoinHandle<io::Result<()>>,
    ) {
        let export = Arc::new(export);
        let (client, server) = tokio::io::duplex(1 << 20);
        let (reader, writer) = tokio::io::split(server);
        let task = tokio::spawn(serve(Arc::clone(&export), reader, writer));
        (export, Client { stream: client }, task)
    }

    #[tokio::test]
    async fn write_then_read_round_trip() {
        let (export, mut client, task) = start(MemExport::new(8192));

        client.write(1, 4096, &[0xab; 1024]).await;
        assert_eq!(client.reply().await, (0, 1));

        client.send(Command::Read, 2, 4096, 1024).await;
        assert_eq!(client.reply().await, (0, 2));
        assert_eq!(client.data(1024).await, vec![0xab; 1024]);

        assert_eq!(&export.data.lock().unwrap()[4096..5120], &[0xab; 1024]);

        client.send(Command::Disconnect, 3, 0, 0).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn trim_zeroes_and_flush_is_rejected() {
        let (export, mut client, task) = start(MemExport::new(4096));

        client.write(1, 0, &[1; 512]).await;
        assert_eq!(client.reply().await, (0, 1));

        // FLUSH is never advertised.
        client.send(Command::Flush, 2, 0, 0).await;
        assert_eq!(client.reply().await, (EINVAL, 2));

        client.send(Command::Trim, 3, 0, 512).await;
        assert_eq!(client.reply().await, (0, 3));
        assert!(export.data.lock().unwrap()[..512].iter().all(|b| *b == 0));

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn errors_are_reported_per_request() {
        let mut export = MemExport::new(4096);
        export.read_only = true;
        let (_export, mut client, task) = start(export);

        // Out-of-range read: error reply without payload.
        client.send(Command::Read, 1, 4096, 512).await;
        assert_eq!(client.reply().await, (EINVAL, 1));

        // Write to a read-only export: the payload is consumed and EPERM returned.
        client.write(2, 0, &[1; 512]).await;
        assert_eq!(client.reply().await, (EPERM, 2));

        client.send(Command::Unknown(99), 3, 0, 0).await;
        assert_eq!(client.reply().await, (EINVAL, 3));

        // The connection keeps working after errors.
        client.send(Command::Read, 4, 0, 512).await;
        assert_eq!(client.reply().await, (0, 4));
        assert_eq!(client.data(512).await, vec![0; 512]);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn replies_follow_completion_order() {
        let mut export = MemExport::new(8192);
        export.slow_offset = Some(0);
        let (_export, mut client, task) = start(export);

        client.send(Command::Read, 1, 0, 512).await;
        client.send(Command::Read, 2, 4096, 512).await;

        assert_eq!(client.reply().await, (0, 2));
        client.data(512).await;
        assert_eq!(client.reply().await, (0, 1));
        client.data(512).await;

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn in_flight_requests_finish_before_disconnect_returns() {
        let mut export = MemExport::new(4096);
        export.slow_offset = Some(0);
        let (_export, mut client, task) = start(export);

        client.send(Command::Read, 1, 0, 512).await;
        client.send(Command::Disconnect, 2, 0, 0).await;

        assert_eq!(client.reply().await, (0, 1));
        assert_eq!(client.data(512).await, vec![0; 512]);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn bad_magic_is_a_protocol_error() {
        let (_export, mut client, task) = start(MemExport::new(4096));
        client
            .stream
            .write_all(&[0u8; REQUEST_HEADER_LEN])
            .await
            .unwrap();
        let err = task.await.unwrap().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn truncated_header_is_an_error() {
        let (_export, mut client, task) = start(MemExport::new(4096));
        client.stream.write_all(&[0u8; 4]).await.unwrap();
        drop(client);
        let err = task.await.unwrap().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn oversized_request_is_a_protocol_error() {
        let (_export, mut client, task) = start(MemExport::new(4096));
        client.send(Command::Read, 1, 0, MAX_REQUEST_LEN + 1).await;
        let err = task.await.unwrap().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn trim_flag_follows_export() {
        let mut export = MemExport::new(512);
        assert_eq!(transmission_flags(&export), FLAG_HAS_FLAGS | FLAG_SEND_TRIM);
        export.read_only = true;
        assert_eq!(transmission_flags(&export), FLAG_HAS_FLAGS);
    }
}
