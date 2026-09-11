use super::{ApiImpl, SessionState};
use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use http::StatusCode;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch,
};
use tracing::debug;

pub(crate) fn router<I: AsRef<ApiImpl> + Clone + Send + Sync + 'static>(state: I) -> Router {
    Router::new()
        .route(
            "/templates/{template_id}/builds/{build_id}/builder",
            get(connect::<I>),
        )
        .with_state(state)
}

async fn connect<I: AsRef<ApiImpl>>(
    State(state): State<I>,
    Path((template_id, build_id)): Path<(String, String)>,
    ws: WebSocketUpgrade,
) -> Response {
    let result = async {
        let session = state.as_ref().session(&template_id, &build_id)?;
        let connection = session.connections.clone().read_owned().await;
        let SessionState::Ready(address) = *session.state.borrow() else {
            return Err(ApiImpl::error(409, "builder is not ready"));
        };
        let stream = tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(address))
            .await
            .map_err(|_| ApiImpl::error(504, "builder connection timed out"))?
            .map_err(|error| ApiImpl::error(502, format!("builder connection failed: {error}")))?;
        Ok((stream, session.state.subscribe(), connection))
    }
    .await;
    match result {
        Ok((stream, state, connection)) => ws
            .max_message_size(1024 * 1024)
            .max_frame_size(1024 * 1024)
            .on_upgrade(move |socket| async move {
                let _connection = connection;
                if let Err(error) = bridge(socket, stream, state).await {
                    debug!(%build_id, %error, "BuildKit connection closed");
                }
            }),
        Err(error) => (
            StatusCode::from_u16(error.code as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(error),
        )
            .into_response(),
    }
}

async fn bridge(
    socket: WebSocket,
    stream: TcpStream,
    mut state: watch::Receiver<SessionState>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let (mut sender, mut receiver) = socket.split();
    let (mut read, mut write) = stream.into_split();
    let upstream = async {
        while let Some(message) = receiver.next().await {
            match message? {
                Message::Binary(bytes) => write.write_all(&bytes).await?,
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Text(_) => anyhow::bail!("expected binary BuildKit stream"),
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    let downstream = async {
        let mut buffer = vec![0u8; 64 * 1024];
        let mut ping = tokio::time::interval(Duration::from_secs(20));
        loop {
            tokio::select! {
                n = read.read(&mut buffer) => {
                    let n = n?;
                    if n == 0 {
                        break;
                    }
                    sender.send(Message::Binary(buffer[..n].to_vec().into())).await?;
                }
                _ = ping.tick() => sender.send(Message::Ping(Vec::new().into())).await?,
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! {
        result = upstream => result,
        result = downstream => result,
        // Stopping a VM need not close its host TCP sockets. End the tunnel
        // explicitly on cancellation/finalization. Publication lets the Solve response drain.
        _ = state.wait_for(|state| matches!(state, SessionState::Cancelled | SessionState::Finished(_))) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{connect_async, tungstenite::Message as ClientMessage};

    #[tokio::test]
    async fn finished_session_closes_tunnel_even_when_worker_tcp_stays_open() -> Result<()> {
        let worker = TcpListener::bind("127.0.0.1:0").await?;
        let address = worker.local_addr()?;
        let (state, receiver) = watch::channel(SessionState::Ready(address));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}/", listener.local_addr()?);
        let router = Router::new().route(
            "/",
            get(move |ws: WebSocketUpgrade| {
                let receiver = receiver.clone();
                async move {
                    ws.on_upgrade(move |socket| async move {
                        let stream = TcpStream::connect(address).await.unwrap();
                        bridge(socket, stream, receiver).await.unwrap();
                    })
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let (mut socket, _) = connect_async(url).await?;
            let (mut stream, _) = worker.accept().await?;
            socket
                .send(ClientMessage::Binary(b"solve".as_slice().into()))
                .await?;
            let mut data = [0; 5];
            stream.read_exact(&mut data).await?;
            assert_eq!(&data, b"solve");
            state.send_replace(SessionState::Publishing);
            stream.write_all(b"final Solve response").await?;
            loop {
                match socket
                    .next()
                    .await
                    .context("tunnel closed during publication")??
                {
                    ClientMessage::Ping(_) => continue,
                    ClientMessage::Binary(bytes) => {
                        assert_eq!(bytes.as_ref(), b"final Solve response");
                        break;
                    }
                    other => panic!("unexpected message during publication: {other:?}"),
                }
            }
            state.send_replace(SessionState::Finished(None));
            // Ignore a queued keepalive. TCP deliberately remains open here.
            while let Some(message) = socket.next().await {
                match message {
                    Ok(ClientMessage::Ping(_) | ClientMessage::Pong(_)) => continue,
                    Ok(ClientMessage::Close(_)) | Err(_) => break,
                    other => panic!("unexpected message after session end: {other:?}"),
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        server.abort();
        result??;
        Ok(())
    }
}
