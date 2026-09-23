use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::StreamExt;
#[cfg(not(unix))]
pub(crate) use tokio::net::TcpListener as BuildkitListener;
#[cfg(unix)]
pub(crate) use tokio::net::UnixListener as BuildkitListener;
use tokio::{io::AsyncWriteExt, task::JoinSet};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Error, Message},
};
use tokio_util::io::ReaderStream;

use super::Client;

pub(crate) async fn bind_local() -> Result<(tempfile::TempDir, BuildkitListener, String)> {
    let mut directory = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        directory.permissions(std::fs::Permissions::from_mode(0o700));
    }
    let work = directory.tempdir()?;
    #[cfg(unix)]
    let (listener, address) = {
        let socket = work.path().join("buildkit.sock");
        (
            BuildkitListener::bind(&socket)?,
            format!("unix://{}", socket.display()),
        )
    };
    #[cfg(not(unix))]
    let (listener, address) = {
        let listener = BuildkitListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = format!("tcp://{}", listener.local_addr()?);
        (listener, address)
    };
    Ok((work, listener, address))
}

impl Client {
    pub(crate) async fn build_request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<Vec<u8>> {
        let mut request = self
            .async_agent
            .request(method, self.url(path))
            .header("X-API-Key", &self.api_key);
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(serde_json::to_vec(&body)?);
        }
        let response = request.send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if !status.is_success() {
            return Err(super::format_status_error(
                status.as_u16(),
                &String::from_utf8_lossy(&bytes),
            ));
        }
        Ok(bytes.to_vec())
    }

    pub(crate) async fn buildkit_tunnel(
        &self,
        path: &str,
        listener: BuildkitListener,
    ) -> Result<()> {
        let mut url = reqwest::Url::parse(&self.url(path))?;
        let scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            _ => bail!("API URL must use HTTP or HTTPS"),
        };
        url.set_scheme(scheme)
            .map_err(|_| anyhow::anyhow!("invalid BuildKit URL"))?;
        let mut request = url.as_str().into_client_request()?;
        request
            .headers_mut()
            .insert("X-API-Key", self.api_key.parse()?);
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                connection = listener.accept(), if connections.len() < 32 => {
                    let (stream, _) = connection?;
                    #[cfg(not(unix))]
                    stream.set_nodelay(true)?;
                    let request = request.clone();
                    connections.spawn(async move {
                        let (socket, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(request)).await
                            .context("BuildKit connection timed out")??;
                        bridge(stream, socket).await
                    });
                }
                Some(result) = connections.join_next() => { result??; }
            }
        }
    }
}

async fn bridge<T, S>(stream: T, socket: tokio_tungstenite::WebSocketStream<S>) -> Result<()>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (read, mut write) = tokio::io::split(stream);
    let (sender, mut receiver) = socket.split();
    let upstream = ReaderStream::with_capacity(read, 64 * 1024)
        .map(|chunk| chunk.map(Message::Binary).map_err(Error::Io))
        .forward(sender);
    let downstream = async {
        while let Some(message) = receiver.next().await {
            match message? {
                Message::Binary(bytes) => match write.write_all(&bytes).await {
                    Ok(()) => {}
                    Err(error) if is_expected_disconnect(&error) => break,
                    Err(error) => return Err(error.into()),
                },
                Message::Close(_) => break,
                Message::Text(_) => bail!("expected binary BuildKit stream"),
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! {
        result = upstream => match result {
            Ok(()) => Ok(()),
            Err(Error::Io(error)) if is_expected_disconnect(&error) => Ok(()),
            Err(error) => Err(error.into()),
        },
        result = downstream => result,
    }
}

fn is_expected_disconnect(error: &std::io::Error) -> bool {
    // buildctl can close its sockets while final status traffic is still in flight.
    // Let its exit status and the template status determine whether the build succeeded.
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::NotConnected
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use futures::SinkExt;
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, UnixStream};

    #[tokio::test]
    async fn local_read_shutdown_does_not_fail_bridge() -> Result<()> {
        use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};

        let (stream, peer) = std::os::unix::net::UnixStream::pair()?;
        stream.set_nonblocking(true)?;
        // Keep the write side open so the bridge must handle the failed write,
        // rather than completing through upstream EOF.
        peer.shutdown(std::net::Shutdown::Read)?;
        let stream = UnixStream::from_std(stream)?;
        let (transport, remote) = tokio::io::duplex(1024);
        let socket = WebSocketStream::from_raw_socket(transport, Role::Client, None).await;
        let mut remote = WebSocketStream::from_raw_socket(remote, Role::Server, None).await;
        remote
            .send(Message::Binary(b"final status".as_slice().into()))
            .await?;

        tokio::time::timeout(Duration::from_secs(5), bridge(stream, socket)).await??;
        drop(peer);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)] // The WebSocket handshake fixes the callback error type.
    async fn private_socket_tunnels_authenticated_binary_traffic() -> Result<()> {
        let (work, listener, address) = bind_local().await?;
        assert_eq!(work.path().metadata()?.permissions().mode() & 0o077, 0);
        let path = address.strip_prefix("unix://").unwrap();
        let remote = TcpListener::bind("127.0.0.1:0").await?;
        let client = Client::new(&format!("http://{}", remote.local_addr()?), "test-key")?;
        let payload = vec![42; 256 * 1024];
        let expected = payload.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = remote.accept().await?;
            let mut socket = tokio_tungstenite::accept_hdr_async(
                stream,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.uri().path(), "/builder");
                    assert_eq!(request.headers()["X-API-Key"], "test-key");
                    Ok(response)
                },
            )
            .await?;
            let mut received = Vec::new();
            while received.len() < expected.len() {
                if let Message::Binary(bytes) = socket.next().await.context("tunnel closed")?? {
                    received.extend_from_slice(&bytes);
                }
            }
            assert_eq!(received, expected);
            socket
                .send(Message::Ping(b"keepalive".as_slice().into()))
                .await?;
            assert_eq!(
                socket
                    .next()
                    .await
                    .context("tunnel closed during keepalive")??,
                Message::Pong(b"keepalive".as_slice().into())
            );
            socket.send(Message::Binary(received.into())).await?;
            // Keep the WebSocket open until the client has read its response.
            while socket.next().await.is_some() {}
            Ok::<_, anyhow::Error>(())
        });
        let tunnel =
            tokio::spawn(async move { client.buildkit_tunnel("/builder", listener).await });
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let mut stream = UnixStream::connect(path).await?;
            stream.write_all(&payload).await?;
            let mut response = vec![0; payload.len()];
            stream.read_exact(&mut response).await?;
            assert_eq!(response, payload);
            Ok::<_, anyhow::Error>(())
        })
        .await;
        tunnel.abort();
        server.await??;
        result??;
        Ok(())
    }

    /// With the provider main installs, a `wss://` tunnel over an HTTPS API
    /// URL fails like any bad connection; without it, setting up TLS panics.
    #[tokio::test]
    async fn wss_connection_fails_instead_of_panicking() -> Result<()> {
        crate::install_crypto_provider();
        // Accept and hang up, so the client gets as far as setting up TLS.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });
        let result = connect_async(format!("wss://{address}/")).await;
        assert!(result.is_err(), "a hung-up TLS handshake must fail");
        Ok(())
    }
}
