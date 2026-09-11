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

// buildctl uses multiple connections for control and session traffic.
pub(super) const MAX_TUNNEL_CONNECTIONS: u32 = 8;

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
        let SessionState::Ready(address) = *session.state.borrow() else {
            return Err(ApiImpl::error(409, "builder is not ready"));
        };
        // Include pending TCP connections and upgrades in the cap; never queue waiters.
        let connection = session.connections.clone().try_read_owned().map_err(|_| {
            ApiImpl::error(429, "builder tunnel connection limit reached; retry later")
        })?;
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
    async fn tunnel_limit_rejects_excess_and_releases_failed_or_closed_connections() -> Result<()> {
        use super::super::{tests::test_api, BuildSession};
        use crate::volume::VolumeLimits;
        use std::sync::Arc;
        use tokio_tungstenite::tungstenite::Error;

        let (_root, api, record) = test_api(VolumeLimits::default()).await?;
        let worker = TcpListener::bind("127.0.0.1:0").await?;
        let address = worker.local_addr()?;
        drop(worker);
        let session = BuildSession::new();
        assert!(session.ready(address));
        api.build_sessions
            .active
            .lock()
            .unwrap()
            .insert(record.id.to_string(), session.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!(
            "ws://{}/templates/{}/builds/{}/builder",
            listener.local_addr()?,
            record.id,
            record.id
        );
        let app = router(Arc::new(api));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            // Failed upstream connects must return their slots, even after more than the cap.
            for _ in 0..MAX_TUNNEL_CONNECTIONS + 1 {
                let Error::Http(response) = connect_async(&url).await.unwrap_err() else {
                    panic!("expected HTTP connection failure");
                };
                assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            }
            let worker = TcpListener::bind(address).await?;
            let mut sockets = Vec::new();
            let mut streams = Vec::new();
            for _ in 0..MAX_TUNNEL_CONNECTIONS {
                sockets.push(connect_async(&url).await?.0);
                streams.push(worker.accept().await?.0);
            }
            let Error::Http(response) = connect_async(&url).await.unwrap_err() else {
                panic!("expected HTTP saturation response");
            };
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            assert!(
                tokio::time::timeout(Duration::from_millis(50), worker.accept())
                    .await
                    .is_err()
            );

            sockets.pop().unwrap().close(None).await?;
            // The server receives the close asynchronously; the next connection must succeed.
            loop {
                match connect_async(&url).await {
                    Ok((socket, _)) => {
                        sockets.push(socket);
                        streams.push(worker.accept().await?.0);
                        break;
                    }
                    Err(Error::Http(response))
                        if response.status() == StatusCode::TOO_MANY_REQUESTS =>
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            session.state.send_replace(SessionState::Cancelled);
            // Every upgraded connection must also release its slot on cancellation.
            let _drained = session.connections.write().await;
            Ok::<_, anyhow::Error>(())
        })
        .await;
        server.abort();
        result??;
        Ok(())
    }

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
