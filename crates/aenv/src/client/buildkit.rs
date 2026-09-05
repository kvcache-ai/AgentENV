use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};

use super::Client;

impl Client {
    pub(crate) async fn buildkit_tunnel(&self, path: &str, listener: TcpListener) -> Result<()> {
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

async fn bridge<S>(stream: TcpStream, socket: tokio_tungstenite::WebSocketStream<S>) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    stream.set_nodelay(true)?;
    let (mut read, mut write) = stream.into_split();
    let (mut sender, mut receiver) = socket.split();
    let upstream = async {
        let mut buffer = vec![0u8; 64 * 1024];
        let mut ping = tokio::time::interval(Duration::from_secs(20));
        loop {
            tokio::select! {
                n = read.read(&mut buffer) => {
                    let n = n?;
                    if n == 0 { break; }
                    sender.send(Message::Binary(buffer[..n].to_vec().into())).await?;
                }
                _ = ping.tick() => sender.send(Message::Ping(Vec::new().into())).await?,
            }
        }
        Ok::<_, anyhow::Error>(())
    };
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
    tokio::select! { result = upstream => result, result = downstream => result }
}
