//! Unified metadata RPC protocol over direct in-process dispatch or HTTP JSON.

use std::future::Future;
use std::io;
use std::time::Duration;

use agentenv_domain::{
    BuildJobRecord, BuildJobTransition, BuildTarget, ImageTag, ImageTagRecord, PublishedImage,
    SnapshotId, SnapshotListFilter, SnapshotRecord, VolumeLeaseOp, VolumeRecord,
};
use axum::extract::{DefaultBodyLimit, State};
use axum::routing::post;
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::{GcReport, MetaError, MetaResult, MetaServer, SnapshotEntry};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(60);
const GC_RPC_TIMEOUT: Duration = Duration::from_secs(3600);

/// Unified metadata client.
///
/// Embedded clients invoke [`MetaServer`] directly with zero serialization overhead.
/// HTTP clients use a pooled `reqwest::Client`: read-only calls retry once on
/// transport errors (for example a stale keep-alive socket after service restart),
/// while mutating calls never retry so callers preserve lost-response recovery semantics.
#[derive(Clone)]
pub struct MetaClient {
    inner: MetaClientInner,
}

#[derive(Clone)]
enum MetaClientInner {
    Embedded(MetaServer),
    Http {
        client: reqwest::Client,
        base_url: String,
    },
}

impl std::fmt::Debug for MetaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            MetaClientInner::Embedded(_) => f.write_str("MetaClient::Embedded"),
            MetaClientInner::Http { base_url, .. } => {
                f.debug_tuple("MetaClient::Http").field(base_url).finish()
            }
        }
    }
}

fn is_retryable_transport_error(error: &reqwest::Error) -> bool {
    error.is_connect() || error.is_request() || error.is_body()
}

impl MetaClient {
    /// Closes the underlying embedded [`MetaServer`] when running in-process.
    /// No-op for HTTP clients, whose server lifecycle is managed externally.
    pub async fn close(&self) {
        if let MetaClientInner::Embedded(server) = &self.inner {
            server.close().await;
        }
    }

    async fn http_call<Req, Resp>(
        client: &reqwest::Client,
        base_url: &str,
        op_name: &str,
        body: &Req,
        retry_on_transport_error: bool,
        timeout: Duration,
    ) -> MetaResult<Resp>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let url = format!("{base_url}/rpc/{op_name}");
        for attempt in 0..=usize::from(retry_on_transport_error) {
            let result = async {
                let response = client.post(&url).timeout(timeout).json(body).send().await?;
                if !response.status().is_success() {
                    return Ok(Err(MetaError::backend(
                        op_name,
                        format!("metadata service returned HTTP {}", response.status()),
                    )));
                }
                response.json::<MetaResult<Resp>>().await
            }
            .await;
            match result {
                Ok(payload) => return payload,
                Err(error)
                    if attempt == 0
                        && retry_on_transport_error
                        && is_retryable_transport_error(&error) =>
                {
                    continue;
                }
                Err(error) => return Err(MetaError::backend(op_name, error)),
            }
        }
        unreachable!("loop returns on final attempt")
    }
}

/// Declares the metadata HTTP RPC routes, server handlers, and dual-mode client methods
/// from one single source of truth.
macro_rules! define_meta_rpc {
    (
        $(
            $kind:ident fn $name:ident($($arg:ident: $ty:ty => $server_arg:expr),* $(,)?) -> $ret:ty;
        )*
    ) => {
        mod handlers {
            use super::*;

            pub(super) async fn collect_garbage(
                State(server): State<MetaServer>,
                Json((limit,)): Json<(u32,)>,
            ) -> Json<MetaResult<GcReport>> {
                // Keep claimed layer deletions running when an operator disconnects.
                let active = server.active_collections.clone().read_owned().await;
                let result = tokio::spawn(async move {
                    let _active = active;
                    MetaServer::collect_garbage(&server, limit).await
                })
                .await
                .map_err(|error| MetaError::backend("finish collection", error))
                .and_then(|res| res);
                Json(result)
            }

            $(
                pub(super) async fn $name(
                    State(server): State<MetaServer>,
                    Json(($($arg,)*)): Json<($($ty,)*)>,
                ) -> Json<MetaResult<$ret>> {
                    Json(Box::pin(MetaServer::$name(&server, $($server_arg),*)).await)
                }
            )*
        }

        fn build_router(server: MetaServer) -> Router {
            Router::new()
                .route("/rpc/collect_garbage", post(handlers::collect_garbage))
                $(.route(concat!("/rpc/", stringify!($name)), post(handlers::$name)))*
                .layer(DefaultBodyLimit::disable())
                .with_state(server)
        }

        impl MetaClient {
            pub async fn collect_garbage(&self, limit: u32) -> MetaResult<GcReport> {
                match &self.inner {
                    MetaClientInner::Embedded(server) => server.collect_garbage(limit).await,
                    MetaClientInner::Http { client, base_url } => {
                        Self::http_call(
                            client,
                            base_url,
                            "collect_garbage",
                            &(limit,),
                            false,
                            GC_RPC_TIMEOUT,
                        )
                        .await
                    }
                }
            }

            $(
                define_meta_rpc!(@client $kind fn $name($($arg: $ty => $server_arg),*) -> $ret);
            )*
        }
    };
    (@retry read) => { true };
    (@retry write) => { false };
    (@client $kind:ident fn $name:ident($($arg:ident: $ty:ty => $server_arg:expr),*) -> $ret:ty) => {
        pub async fn $name(&self, $($arg: $ty),*) -> MetaResult<$ret> {
            match &self.inner {
                MetaClientInner::Embedded(server) => {
                    Box::pin(MetaServer::$name(server, $($server_arg),*)).await
                }
                MetaClientInner::Http { client, base_url } => {
                    Self::http_call(
                        client,
                        base_url,
                        stringify!($name),
                        &($($arg,)*),
                        define_meta_rpc!(@retry $kind),
                        DEFAULT_RPC_TIMEOUT,
                    )
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
    read fn get_snapshot(reference: String => &reference) -> Option<SnapshotEntry>;
    read fn list_snapshots(filter: SnapshotListFilter => filter) -> Vec<SnapshotEntry>;
    write fn register_snapshot(record: SnapshotRecord => record, restored_layers: Vec<String> => &restored_layers) -> SnapshotEntry;
    write fn promote_snapshot(record: SnapshotRecord => record, restored_layers: Vec<String> => &restored_layers) -> Option<SnapshotEntry>;
    write fn retire_snapshot(id: SnapshotId => id) -> ();

    // Snapshot pins (collect_garbage above)
    write fn pin_snapshot(
        id: SnapshotId => &id,
        pin_id: Uuid => pin_id,
        owner_node_id: Option<String> => owner_node_id,
    ) -> bool;
    write fn release_snapshot_pin(id: SnapshotId => &id, pin_id: Uuid => pin_id) -> ();

    // Volumes
    read fn get_volume(reference: String => &reference) -> Option<VolumeRecord>;
    read fn list_volumes(
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
    read fn get_image_tag(tag: ImageTag => &tag) -> Option<(ImageTagRecord, PublishedImage)>;
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
    write fn delete_build_job(build_id: SnapshotId => &build_id) -> bool;
}

pub fn embedded(server: MetaServer) -> MetaClient {
    MetaClient {
        inner: MetaClientInner::Embedded(server),
    }
}

pub async fn connect(address: &str) -> io::Result<MetaClient> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .map_err(io::Error::other)?;
    let trimmed = address.trim_end_matches('/');
    let base_url = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_owned()
    } else {
        format!("http://{trimmed}")
    };
    let meta = MetaClient {
        inner: MetaClientInner::Http { client, base_url },
    };
    meta.health()
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    Ok(meta)
}

/// Signal shutdown through `shutdown`, then await this function before closing the database.
pub async fn serve_tcp(
    listener: TcpListener,
    server: MetaServer,
    shutdown: impl Future<Output = io::Result<()>> + Send + 'static,
) -> io::Result<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    axum::serve(listener, build_router(server))
        .with_graceful_shutdown(async move {
            let _ = shutdown_tx.send(shutdown.await);
        })
        .await?;
    match shutdown_rx.await {
        Ok(shutdown_result) => shutdown_result,
        Err(_) => Ok(()),
    }
}
