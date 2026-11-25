//! One directory protocol over in-process channels or auto-reconnecting TCP.

use std::future::Future;
use std::io;
use std::sync::Arc;

use agentenv_domain::{SnapshotId, SnapshotListFilter, SnapshotManifest, SnapshotMetadata};
use futures::{FutureExt, StreamExt};
use tarpc::client::{self, RpcError};
use tarpc::context::Context;
use tarpc::serde_transport;
use tarpc::server::{BaseChannel, Channel};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::task::JoinSet;
use tokio_serde::formats::Json;
use uuid::Uuid;

use crate::protocol::{Meta, MetaClient as RawMetaClient};
use crate::{GcReport, MetaResult, MetaServer};

/// Snapshot directory RPC client.
///
/// Embedded clients forward directly over an in-process channel. TCP clients
/// hold a shared connection: read-only calls transparently reconnect once if
/// the metadata service restarts, while mutating calls evict the broken socket
/// and return the transport error so callers keep lost-response recovery
/// semantics intact and subsequent calls dial a fresh connection.
#[derive(Clone, Debug)]
pub struct MetaClient {
    inner: MetaClientInner,
}

#[derive(Clone, Debug)]
enum MetaClientInner {
    Embedded(RawMetaClient),
    Tcp(Arc<ReconnectingTcpClient>),
}

#[derive(Debug)]
struct ReconnectingTcpClient {
    address: String,
    connection: RwLock<(u64, Option<RawMetaClient>)>,
    reconnect_lock: Mutex<()>,
}

impl ReconnectingTcpClient {
    async fn connect(address: String) -> io::Result<Self> {
        let client = dial_raw(&address).await?;
        Ok(Self {
            address,
            connection: RwLock::new((0, Some(client))),
            reconnect_lock: Mutex::new(()),
        })
    }

    async fn get_or_connect(&self) -> io::Result<(u64, RawMetaClient)> {
        {
            let current = self.connection.read().await;
            if let Some(client) = &current.1 {
                return Ok((current.0, client.clone()));
            }
        }
        let _guard = self.reconnect_lock.lock().await;
        {
            let current = self.connection.read().await;
            if let Some(client) = &current.1 {
                return Ok((current.0, client.clone()));
            }
        }
        let fresh = dial_raw(&self.address).await?;
        let mut current = self.connection.write().await;
        current.0 = current.0.wrapping_add(1);
        current.1 = Some(fresh.clone());
        Ok((current.0, fresh))
    }

    async fn invalidate(&self, failed_epoch: u64) {
        let _guard = self.reconnect_lock.lock().await;
        let mut current = self.connection.write().await;
        if current.0 == failed_epoch {
            current.0 = current.0.wrapping_add(1);
            current.1 = None;
        }
    }

    async fn reconnect(&self, failed_epoch: u64) -> io::Result<RawMetaClient> {
        let _guard = self.reconnect_lock.lock().await;
        {
            let current = self.connection.read().await;
            if current.0 != failed_epoch {
                if let Some(client) = &current.1 {
                    return Ok(client.clone());
                }
            }
        }
        let fresh = dial_raw(&self.address).await?;
        let mut current = self.connection.write().await;
        current.0 = current.0.wrapping_add(1);
        current.1 = Some(fresh.clone());
        Ok(fresh)
    }
}

async fn dial_raw(address: &str) -> io::Result<RawMetaClient> {
    let transport = serde_transport::tcp::connect(address, Json::default).await?;
    Ok(RawMetaClient::new(client::Config::default(), transport).spawn())
}

fn is_connection_error(error: &RpcError) -> bool {
    matches!(error, RpcError::Shutdown | RpcError::Channel(_))
}

impl MetaClient {
    async fn call_read<T, F, Fut>(&self, mut op: F) -> Result<T, RpcError>
    where
        F: FnMut(RawMetaClient) -> Fut,
        Fut: Future<Output = Result<T, RpcError>>,
    {
        match &self.inner {
            MetaClientInner::Embedded(client) => op(client.clone()).await,
            MetaClientInner::Tcp(tcp) => {
                let (epoch, client) = tcp.get_or_connect().await.map_err(|_| RpcError::Shutdown)?;
                match op(client).await {
                    Ok(value) => Ok(value),
                    Err(error) if is_connection_error(&error) => match tcp.reconnect(epoch).await {
                        Ok(fresh) => op(fresh).await,
                        Err(_) => {
                            tcp.invalidate(epoch).await;
                            Err(error)
                        }
                    },
                    Err(error) => Err(error),
                }
            }
        }
    }

    async fn call_write<T, F, Fut>(&self, op: F) -> Result<T, RpcError>
    where
        F: FnOnce(RawMetaClient) -> Fut,
        Fut: Future<Output = Result<T, RpcError>>,
    {
        match &self.inner {
            MetaClientInner::Embedded(client) => op(client.clone()).await,
            MetaClientInner::Tcp(tcp) => {
                let (epoch, client) = tcp.get_or_connect().await.map_err(|_| RpcError::Shutdown)?;
                match op(client).await {
                    Ok(value) => Ok(value),
                    Err(error) if is_connection_error(&error) => {
                        tcp.invalidate(epoch).await;
                        Err(error)
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }

    pub async fn health(&self, ctx: Context) -> Result<MetaResult<()>, RpcError> {
        self.call_read(|client| async move { client.health(ctx).await })
            .await
    }

    pub async fn get_snapshot(
        &self,
        ctx: Context,
        reference: String,
    ) -> Result<MetaResult<Option<SnapshotMetadata>>, RpcError> {
        self.call_read(|client| {
            let reference = reference.clone();
            async move { client.get_snapshot(ctx, reference).await }
        })
        .await
    }

    pub async fn list_snapshots(
        &self,
        ctx: Context,
        filter: SnapshotListFilter,
    ) -> Result<MetaResult<Vec<SnapshotMetadata>>, RpcError> {
        self.call_read(|client| {
            let filter = filter.clone();
            async move { client.list_snapshots(ctx, filter).await }
        })
        .await
    }

    pub async fn register_snapshot(
        &self,
        ctx: Context,
        metadata: SnapshotMetadata,
        manifest: SnapshotManifest,
        restored_layers: Vec<String>,
    ) -> Result<MetaResult<SnapshotMetadata>, RpcError> {
        self.call_write(|client| async move {
            client
                .register_snapshot(ctx, metadata, manifest, restored_layers)
                .await
        })
        .await
    }

    pub async fn promote_snapshot(
        &self,
        ctx: Context,
        metadata: SnapshotMetadata,
        manifest: SnapshotManifest,
        restored_layers: Vec<String>,
    ) -> Result<MetaResult<Option<SnapshotMetadata>>, RpcError> {
        self.call_write(|client| async move {
            client
                .promote_snapshot(ctx, metadata, manifest, restored_layers)
                .await
        })
        .await
    }

    pub async fn retire_snapshot(
        &self,
        ctx: Context,
        id: SnapshotId,
    ) -> Result<MetaResult<()>, RpcError> {
        self.call_write(|client| async move { client.retire_snapshot(ctx, id).await })
            .await
    }

    pub async fn pin_snapshot(
        &self,
        ctx: Context,
        id: SnapshotId,
        pin_id: Uuid,
        owner_node_id: Option<String>,
    ) -> Result<MetaResult<Option<SnapshotMetadata>>, RpcError> {
        self.call_write(|client| async move {
            client.pin_snapshot(ctx, id, pin_id, owner_node_id).await
        })
        .await
    }

    pub async fn release_pin(
        &self,
        ctx: Context,
        id: SnapshotId,
        pin_id: Uuid,
    ) -> Result<MetaResult<()>, RpcError> {
        self.call_write(|client| async move { client.release_pin(ctx, id, pin_id).await })
            .await
    }

    pub async fn collect_garbage(
        &self,
        ctx: Context,
        limit: u32,
    ) -> Result<MetaResult<GcReport>, RpcError> {
        self.call_write(|client| async move { client.collect_garbage(ctx, limit).await })
            .await
    }
}

pub fn embedded(server: MetaServer) -> MetaClient {
    let (client_transport, server_transport) = tarpc::transport::channel::unbounded();
    tokio::spawn(
        BaseChannel::with_defaults(server_transport)
            .execute(server.serve())
            .for_each_concurrent(None, |request| request),
    );
    MetaClient {
        inner: MetaClientInner::Embedded(
            RawMetaClient::new(client::Config::default(), client_transport).spawn(),
        ),
    }
}

pub async fn connect(address: &str) -> io::Result<MetaClient> {
    let tcp = ReconnectingTcpClient::connect(address.to_owned()).await?;
    Ok(MetaClient {
        inner: MetaClientInner::Tcp(Arc::new(tcp)),
    })
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
