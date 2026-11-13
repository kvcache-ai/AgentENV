//! One directory protocol over in-process channels or TCP.

use std::future::Future;
use std::io;

use futures::{FutureExt, StreamExt};
use tarpc::server::{BaseChannel, Channel};
use tarpc::{client, serde_transport};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_serde::formats::Json;

use crate::protocol::{Meta, MetaClient};
use crate::MetaServer;

pub fn embedded(server: MetaServer) -> MetaClient {
    let (client_transport, server_transport) = tarpc::transport::channel::unbounded();
    tokio::spawn(
        BaseChannel::with_defaults(server_transport)
            .execute(server.serve())
            .for_each_concurrent(None, |request| request),
    );
    MetaClient::new(client::Config::default(), client_transport).spawn()
}

pub async fn connect(address: &str) -> io::Result<MetaClient> {
    let transport = serde_transport::tcp::connect(address, Json::default).await?;
    Ok(MetaClient::new(client::Config::default(), transport).spawn())
}

/// Signal shutdown through `shutdown`, then await this function before closing the database.
pub async fn serve_tcp(
    listener: TcpListener,
    server: MetaServer,
    shutdown: impl Future<Output = io::Result<()>>,
) -> io::Result<()> {
    let mut incoming = serde_transport::tcp::listen_on(listener, Json::default).await?;
    let (stop, stopped) = oneshot::channel::<()>();
    let stopped = stopped.shared();
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);

    let mut result = loop {
        tokio::select! {
            result = &mut shutdown => break result,
            accepted = incoming.next() => {
                let transport = match accepted {
                    Some(Ok(transport)) => transport,
                    Some(Err(error)) => break Err(error),
                    None => break Ok(()),
                };
                let stopped = stopped.clone();
                let server = server.clone();
                connections.spawn(async move {
                    // Stop reading new requests while tarpc drains responses on the same sink.
                    BaseChannel::with_defaults(transport.take_until(stopped))
                        .execute(server.serve())
                        .for_each_concurrent(None, |request| request)
                        .await;
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    break Err(io::Error::other(error));
                }
            }
        }
    };
    drop(incoming);
    let _ = stop.send(());
    while let Some(joined) = connections.join_next().await {
        result = result.and(joined.map_err(io::Error::other));
    }
    result
}
