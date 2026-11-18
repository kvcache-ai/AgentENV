use std::{future::Future, io, time::Duration};

use tokio::task::JoinHandle;

// Maximum wait for HTTP responses after sandbox and resource cleanup.
const HTTP_DRAIN_AFTER_CLEANUP: Duration = Duration::from_secs(5);

/// Call this only when returning from main. Remaining connection tasks are
/// closed when the Tokio runtime is dropped.
pub(super) async fn run(
    server: impl Future<Output = io::Result<()>>,
    mut cleanup: JoinHandle<()>,
) -> anyhow::Result<()> {
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => {
            cleanup.await?;
            result?;
        }
        result = &mut cleanup => {
            result?;
            // Cleanup has finished, including sandbox persistence. Give control
            // responses time to finish, then return so main drops the runtime
            // and closes connections whose clients have stopped reading.
            match tokio::time::timeout(HTTP_DRAIN_AFTER_CLEANUP, &mut server).await {
                Ok(result) => result?,
                Err(_) => tracing::warn!(target: "agentenv", "HTTP connections did not drain before process exit"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get, Router};
    use futures::{stream, StreamExt};
    use std::{future::IntoFuture, thread};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpSocket,
        sync::oneshot,
    };
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn stalled_download_cannot_hold_process_after_cleanup() {
        let (address_tx, address_rx) = oneshot::channel();
        let (finished_tx, mut finished_rx) = oneshot::channel();
        let (signal_tx, signal_rx) = oneshot::channel();
        let server_thread = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let socket = TcpSocket::new_v4().unwrap();
                socket.set_send_buffer_size(4096).unwrap();
                socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
                let listener = socket.listen(128).unwrap();
                address_tx.send(listener.local_addr().unwrap()).unwrap();
                let ended = CancellationToken::new();
                let request_ended = ended.clone();
                let app = Router::new().route(
                    "/",
                    get(move || {
                        let ended = request_ended.clone();
                        async move {
                            // Larger than the socket buffers: the client stops reading
                            // while Hyper still has bytes from this frame to send.
                            let data = stream::once(async move {
                                Ok::<_, io::Error>(vec![0u8; 8 * 1024 * 1024])
                            });
                            let cancellation = stream::once(async move {
                                ended.cancelled().await;
                                Err(io::Error::other("shutdown ended the response"))
                            });
                            Body::from_stream(data.chain(cancellation))
                        }
                    }),
                );
                let (cleanup_tx, cleanup_rx) = oneshot::channel();
                let cleanup = tokio::spawn(async move {
                    cleanup_rx.await.unwrap();
                    ended.cancel();
                });
                let server = axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        signal_rx.await.unwrap();
                        cleanup_tx.send(()).unwrap();
                    })
                    .into_future();
                let server = async {
                    server.await.unwrap();
                    panic!("stalled response drained without reaching the shutdown deadline");
                };
                run(server, cleanup).await.unwrap();
            });
            // This is the runtime drop performed after #[tokio::main] returns.
            drop(runtime);
            finished_tx.send(()).unwrap();
        });
        let socket = TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        let mut client = socket.connect(address_rx.await.unwrap()).await.unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), client.read_exact(&mut [0; 1024]))
            .await
            .unwrap()
            .unwrap();
        signal_tx.send(()).unwrap();
        let finished = tokio::time::timeout(Duration::from_secs(10), &mut finished_rx).await;
        // Release the connection before joining even when the regression returns.
        drop(client);
        server_thread.join().unwrap();
        finished
            .expect("a stalled download blocked process exit after cleanup")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_outlives_the_http_deadline() {
        for server_finished in [false, true] {
            let (finish_cleanup, wait_cleanup) = oneshot::channel();
            let cleanup = tokio::spawn(async move { wait_cleanup.await.unwrap() });
            let server = async move {
                if !server_finished {
                    std::future::pending::<()>().await;
                }
                Ok(())
            };
            let running = run(server, cleanup);
            tokio::pin!(running);
            assert!(futures::poll!(&mut running).is_pending());
            tokio::time::advance(Duration::from_secs(60)).await;
            assert!(futures::poll!(&mut running).is_pending());
            finish_cleanup.send(()).unwrap();
            running.await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn control_response_can_finish_after_cleanup() {
        let (finish_response, wait_response) = oneshot::channel();
        let (cleanup_done, wait_cleanup) = oneshot::channel();
        let cleanup = tokio::spawn(async move { cleanup_done.send(()).unwrap() });
        wait_cleanup.await.unwrap();
        let running = run(async { wait_response.await.unwrap() }, cleanup);
        tokio::pin!(running);
        assert!(futures::poll!(&mut running).is_pending());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(futures::poll!(&mut running).is_pending());
        finish_response.send(Ok(())).unwrap();
        running.await.unwrap();
    }
}
