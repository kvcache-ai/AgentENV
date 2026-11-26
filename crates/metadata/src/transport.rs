//! One metadata protocol over in-process channels or auto-reconnecting TCP.

use std::future::Future;
use std::io;
use std::sync::Arc;

use agentenv_domain::{
    BuildCacheState, BuildErrorReason, BuildJobRecord, BuildTarget, ImageTag, ImageTagRecord,
    PublishedImage, ResourceKind, SnapshotId, SnapshotListFilter, SnapshotRecord,
    SnapshotVisibility, VolumeRecord, VolumeRecordPage,
};
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
use crate::{GcReport, MetaResult, MetaServer, SnapshotEntry};

/// Unified metadata RPC client.
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

macro_rules! meta_client_methods {
    (
        $(read fn $rname:ident($($rarg:ident: $rty:ty),* $(,)?) -> $rret:ty;)*
        $(write fn $wname:ident($($warg:ident: $wty:ty),* $(,)?) -> $wret:ty;)*
    ) => {
        $(
            pub async fn $rname(&self, ctx: Context, $($rarg: $rty),*) -> Result<MetaResult<$rret>, RpcError> {
                self.call_read(|client| {
                    $(let $rarg = $rarg.clone();)*
                    async move { client.$rname(ctx, $($rarg),*).await }
                })
                .await
            }
        )*
        $(
            pub async fn $wname(&self, ctx: Context, $($warg: $wty),*) -> Result<MetaResult<$wret>, RpcError> {
                self.call_write(|client| async move { client.$wname(ctx, $($warg),*).await })
                    .await
            }
        )*
    };
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
            MetaClientInner::Embedded(client) => Box::pin(op(client.clone())).await,
            MetaClientInner::Tcp(tcp) => {
                let (epoch, client) = tcp.get_or_connect().await.map_err(|_| RpcError::Shutdown)?;
                match Box::pin(op(client)).await {
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

    pub async fn register_snapshot(
        &self,
        ctx: Context,
        record: SnapshotRecord,
        restored_layers: Vec<String>,
    ) -> Result<MetaResult<SnapshotEntry>, RpcError> {
        let record = Box::new(record);
        self.call_write(|client| async move {
            client.register_snapshot(ctx, record, restored_layers).await
        })
        .await
    }

    pub async fn promote_snapshot(
        &self,
        ctx: Context,
        record: SnapshotRecord,
        restored_layers: Vec<String>,
    ) -> Result<MetaResult<Option<SnapshotEntry>>, RpcError> {
        let record = Box::new(record);
        self.call_write(|client| async move {
            client.promote_snapshot(ctx, record, restored_layers).await
        })
        .await
    }

    meta_client_methods! {
        // Read-only calls (transparently reconnect once on TCP transport loss).
        read fn health() -> ();
        read fn get_snapshot(reference: String) -> Option<SnapshotEntry>;
        read fn get_snapshot_with_visibility(reference: String, visibility: SnapshotVisibility) -> Option<SnapshotEntry>;
        read fn list_snapshots(filter: SnapshotListFilter) -> Vec<SnapshotEntry>;
        read fn get_volume(reference: String) -> Option<VolumeRecord>;
        read fn list_volumes_page(after_volume_id: Option<String>, limit: u32, include_internal: bool) -> VolumeRecordPage;
        read fn get_build_cache_state() -> BuildCacheState;
        read fn get_image(digest: String) -> Option<PublishedImage>;
        read fn get_image_tag(tag: ImageTag) -> Option<ImageTagRecord>;
        read fn get_build_job(build_id: SnapshotId) -> Option<BuildJobRecord>;

        // Mutating calls (evict broken socket and surface error to preserve idempotency semantics).
        write fn retire_snapshot(id: SnapshotId) -> ();
        write fn pin_resource(kind: ResourceKind, target_id: String, pin_id: Uuid, owner_node_id: Option<String>) -> bool;
        write fn release_pin(kind: ResourceKind, target_id: String, pin_id: Uuid) -> ();
        write fn collect_garbage(limit: u32) -> GcReport;
        write fn create_volume(record: VolumeRecord, from_volume_id: Option<String>, restored_layers: Vec<String>) -> VolumeRecord;
        write fn update_volume(record: VolumeRecord, restored_layers: Vec<String>) -> VolumeRecord;
        write fn delete_volume(volume_id: String) -> ();
        write fn reserve_volume(volume_id: String, owner: String) -> Option<String>;
        write fn reserve_read_only_volume(volume_id: String, owner: String) -> ();
        write fn replace_volume_owner(volume_id: String, from: String, to: Option<String>) -> ();
        write fn replace_build_cache_head(volume_id: String) -> BuildCacheState;
        write fn put_image(image: PublishedImage, restored_layers: Vec<String>) -> String;
        write fn retire_image(digest: String) -> ();
        write fn put_image_tag(tag: ImageTag, digest: String, source_manifest_digest: Option<String>) -> ();
        write fn create_build_job(build_id: SnapshotId, target: BuildTarget) -> BuildJobRecord;
        write fn start_build_job(build_id: SnapshotId) -> bool;
        write fn finish_build_job(
            build_id: SnapshotId,
            result_snapshot_id: Option<SnapshotId>,
            result_image_digest: Option<String>,
        ) -> Option<BuildJobRecord>;
        write fn fail_build_job(build_id: SnapshotId, reason: BuildErrorReason) -> Option<BuildJobRecord>;
        write fn delete_build_job(build_id: SnapshotId, reserve_snapshot_tombstone: bool) -> bool;
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
