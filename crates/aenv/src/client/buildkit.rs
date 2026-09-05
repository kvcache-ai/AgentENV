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
                Message::Binary(bytes) => write.write_all(&bytes).await?,
                Message::Close(_) => break,
                Message::Text(_) => bail!("expected binary BuildKit stream"),
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! { result = upstream => Ok(result?), result = downstream => result }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use futures::SinkExt;
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, UnixStream};

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
}
