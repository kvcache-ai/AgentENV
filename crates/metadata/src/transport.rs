//! Unified metadata RPC protocol over direct in-process dispatch or auto-reconnecting TCP.

use std::future::Future;
use std::io;
use std::sync::Arc;

use agentenv_domain::{
    BuildJobRecord, BuildJobTransition, BuildTarget, ImageTag, ImageTagRecord, PublishedImage,
    ResourceKind, SnapshotId, SnapshotListFilter, SnapshotRecord, VolumeLeaseOp, VolumePurpose,
    VolumeRecord,
};
use futures::{FutureExt, StreamExt};
use tarpc::client::{self, RpcError};
use tarpc::context::{self, Context};
use tarpc::serde_transport;
use tarpc::server::{BaseChannel, Channel};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::task::JoinSet;
use tokio_serde::formats::Json;
use uuid::Uuid;

use crate::{GcReport, MetaError, MetaResult, MetaServer, SnapshotEntry};

/// Unified metadata client.
///
/// Embedded clients invoke [`MetaServer`] directly with zero channel overhead.
/// TCP clients hold a shared connection: read-only calls transparently reconnect
/// once if the metadata service restarts, while mutating calls evict the broken
/// socket and return [`MetaError::Backend`] so callers preserve lost-response
/// recovery semantics and subsequent calls dial a fresh connection.
#[derive(Clone)]
pub struct MetaClient {
    inner: MetaClientInner,
}

#[derive(Clone)]
enum MetaClientInner {
    Embedded(MetaServer),
    Tcp(Arc<ReconnectingTcpClient>),
}

impl std::fmt::Debug for MetaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            MetaClientInner::Embedded(_) => f.write_str("MetaClient::Embedded"),
            MetaClientInner::Tcp(tcp) => f
                .debug_tuple("MetaClient::Tcp")
                .field(&tcp.address)
                .finish(),
        }
    }
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
    /// Closes the underlying embedded [`MetaServer`] when running in-process.
    /// No-op for TCP clients, whose connection is owned by the remote server.
    pub async fn close(&self) {
        if let MetaClientInner::Embedded(server) = &self.inner {
            server.close().await;
        }
    }

    async fn tcp_read<T, F, Fut>(
        tcp: &ReconnectingTcpClient,
        op_name: &str,
        mut op: F,
    ) -> MetaResult<T>
    where
        F: FnMut(RawMetaClient) -> Fut,
        Fut: Future<Output = Result<MetaResult<T>, RpcError>>,
    {
        let (epoch, client) = tcp
            .get_or_connect()
            .await
            .map_err(|error| MetaError::backend(op_name, error))?;
        match op(client).await {
            Ok(result) => result,
            Err(error) if is_connection_error(&error) => match tcp.reconnect(epoch).await {
                Ok(fresh) => op(fresh)
                    .await
                    .map_err(|err| MetaError::backend(op_name, err))?,
                Err(_) => {
                    tcp.invalidate(epoch).await;
                    Err(MetaError::backend(op_name, error))
                }
            },
            Err(error) => Err(MetaError::backend(op_name, error)),
        }
    }

    async fn tcp_write<T, F, Fut>(
        tcp: &ReconnectingTcpClient,
        op_name: &str,
        op: F,
    ) -> MetaResult<T>
    where
        F: FnOnce(RawMetaClient) -> Fut,
        Fut: Future<Output = Result<MetaResult<T>, RpcError>>,
    {
        let (epoch, client) = tcp
            .get_or_connect()
            .await
            .map_err(|error| MetaError::backend(op_name, error))?;
        match Box::pin(op(client)).await {
            Ok(result) => result,
            Err(error) if is_connection_error(&error) => {
                tcp.invalidate(epoch).await;
                Err(MetaError::backend(op_name, error))
            }
            Err(error) => Err(MetaError::backend(op_name, error)),
        }
    }
}

/// Declares the metadata RPC wire protocol, server forwarding, and dual-mode client methods
/// from one single source of truth.
macro_rules! define_meta_rpc {
    (
        $(
            $kind:ident fn $name:ident($($arg:ident: $ty:ty => $server_arg:expr),* $(,)?) -> $ret:ty;
        )*
    ) => {
        #[allow(dead_code, clippy::large_enum_variant)]
        mod rpc {
            use super::*;

            #[tarpc::service]
            pub(super) trait RawMeta {
                async fn collect_garbage(limit: u32) -> MetaResult<GcReport>;
                $(async fn $name($($arg: $ty),*) -> MetaResult<$ret>;)*
            }
        }
        use rpc::{RawMeta, RawMetaClient};

        impl RawMeta for MetaServer {
            async fn collect_garbage(self, _: Context, limit: u32) -> MetaResult<GcReport> {
                // Keep claimed layer deletions running when an operator disconnects.
                let active = self.active_collections.clone().read_owned().await;
                tokio::spawn(async move {
                    let _active = active;
                    MetaServer::collect_garbage(&self, limit).await
                })
                .await
                .map_err(|error| MetaError::backend("finish collection", error))?
            }

            $(
                async fn $name(self, _: Context, $($arg: $ty),*) -> MetaResult<$ret> {
                    Box::pin(MetaServer::$name(&self, $($server_arg),*)).await
                }
            )*
        }

        impl MetaClient {
            pub async fn collect_garbage(&self, limit: u32) -> MetaResult<GcReport> {
                match &self.inner {
                    MetaClientInner::Embedded(server) => server.collect_garbage(limit).await,
                    MetaClientInner::Tcp(tcp) => {
                        let mut ctx = context::current();
                        // A pass can delete many remote objects after SQL selection finishes.
                        ctx.deadline =
                            std::time::Instant::now() + std::time::Duration::from_secs(3600);
                        Self::tcp_write(tcp, "collect_garbage", |client| async move {
                            client.collect_garbage(ctx, limit).await
                        })
                        .await
                    }
                }
            }

            $(
                define_meta_rpc!(@client $kind fn $name($($arg: $ty => $server_arg),*) -> $ret);
            )*
        }
    };
    (@client read fn $name:ident($($arg:ident: $ty:ty => $server_arg:expr),*) -> $ret:ty) => {
        pub async fn $name(&self, $($arg: $ty),*) -> MetaResult<$ret> {
            match &self.inner {
                MetaClientInner::Embedded(server) => {
                    Box::pin(MetaServer::$name(server, $($server_arg),*)).await
                }
                MetaClientInner::Tcp(tcp) => {
                    Self::tcp_read(tcp, stringify!($name), |client| {
                        $(let $arg = $arg.clone();)*
                        async move { client.$name(context::current(), $($arg),*).await }
                    })
                    .await
                }
            }
        }
    };
    (@client write fn $name:ident($($arg:ident: $ty:ty => $server_arg:expr),*) -> $ret:ty) => {
        pub async fn $name(&self, $($arg: $ty),*) -> MetaResult<$ret> {
            match &self.inner {
                MetaClientInner::Embedded(server) => {
                    Box::pin(MetaServer::$name(server, $($server_arg),*)).await
                }
                MetaClientInner::Tcp(tcp) => {
                    Self::tcp_write(tcp, stringify!($name), |client| async move {
                        client.$name(context::current(), $($arg),*).await
                    })
                    .await
                }
            }
        }
    };
}

define_meta_rpc! {
    // Health
    read fn health() -> ();

    // Snapshots
    read fn get_snapshot(reference: String => &reference, internal: bool => internal) -> Option<SnapshotEntry>;
    read fn list_snapshots(filter: SnapshotListFilter => filter) -> Vec<SnapshotEntry>;
    write fn register_snapshot(record: SnapshotRecord => record, restored_layers: Vec<String> => &restored_layers) -> SnapshotEntry;
    write fn promote_snapshot(record: SnapshotRecord => record, restored_layers: Vec<String> => &restored_layers) -> Option<SnapshotEntry>;
    write fn retire_snapshot(id: SnapshotId => id) -> ();

    // Shared pins (collect_garbage above)
    write fn pin_resource(
        kind: ResourceKind => kind,
        target_id: String => &target_id,
        pin_id: Uuid => pin_id,
        owner_node_id: Option<String> => owner_node_id,
    ) -> bool;
    write fn release_pin(kind: ResourceKind => kind, target_id: String => &target_id, pin_id: Uuid => pin_id) -> ();

    // Volumes & Build Cache
    read fn get_volume(reference: String => &reference) -> Option<VolumeRecord>;
    read fn list_volumes(
        purposes: Vec<VolumePurpose> => &purposes,
        after_volume_id: Option<String> => after_volume_id.as_deref(),
        limit: u32 => limit,
    ) -> (Vec<VolumeRecord>, Option<String>);
    write fn create_volume(
        record: VolumeRecord => record,
        from_volume_id: Option<String> => from_volume_id.as_deref(),
        restored_layers: Vec<String> => &restored_layers,
    ) -> VolumeRecord;
    write fn update_volume(record: VolumeRecord => record, restored_layers: Vec<String> => &restored_layers) -> VolumeRecord;
    write fn delete_volume(volume_id: String => &volume_id) -> ();
    write fn update_volume_lease(volume_id: String => &volume_id, op: VolumeLeaseOp => op) -> Option<String>;

    // Images & Tags
    read fn get_image(digest: String => &digest) -> Option<PublishedImage>;
    read fn get_image_tag(tag: ImageTag => &tag) -> Option<(ImageTagRecord, Option<PublishedImage>)>;
    write fn put_image(image: PublishedImage => &image, restored_layers: Vec<String> => &restored_layers) -> String;
    write fn put_image_tag(
        tag: ImageTag => &tag,
        digest: String => &digest,
        source_manifest_digest: Option<String> => source_manifest_digest.as_deref(),
    ) -> ();

    // Build Jobs
    read fn get_build_job(build_id: SnapshotId => &build_id) -> Option<BuildJobRecord>;
    write fn create_build_job(build_id: SnapshotId => &build_id, target: BuildTarget => &target) -> BuildJobRecord;
    write fn update_build_job(build_id: SnapshotId => &build_id, transition: BuildJobTransition => transition) -> Option<BuildJobRecord>;
    write fn delete_build_job(build_id: SnapshotId => &build_id, reserve_snapshot_tombstone: bool => reserve_snapshot_tombstone) -> bool;
}

pub fn embedded(server: MetaServer) -> MetaClient {
    MetaClient {
        inner: MetaClientInner::Embedded(server),
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
