use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, StreamExt};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, Watcher};
use iroh_blobs::api::blobs::{AddPathOptions, ImportMode};
use iroh_blobs::api::downloader::Downloader;
use iroh_blobs::api::proto::ExportRangesItem;
use iroh_blobs::protocol::{ChunkRanges, ChunkRangesExt, GetRequest};
use iroh_blobs::store::fs::{options::Options as FsStoreOptions, FsStore};
use iroh_blobs::store::{GcConfig, ProtectOutcome};
use iroh_blobs::{BlobFormat, BlobsProtocol, HashAndFormat};
use tracing::{debug, info, instrument, trace, warn};

use super::catalog::{
    CatalogProtocol, CatalogRequest, CatalogResponse, PublishedArtifactCatalog, CATALOG_ALPN,
    MAX_CATALOG_RESPONSE_BYTES,
};
use super::IROH_BACKEND_ID;
use crate::digest;
use crate::p2p::config::ResolvedP2pConfig;
use crate::p2p::discovery::P2pPeerDiscovery;
use crate::p2p::error::{Error, Result};
use crate::p2p::transport::P2pTransport;
use crate::p2p::types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProvider, P2pArtifactProviderHint,
    P2pEndpoint, P2pPeer, P2pPublishMode, P2pPublishRequest, P2pPublishSource,
};
use crate::p2p::P2pByteStream;

const CATALOG_DB_DIR: &str = "catalog.db";
const MAX_CONCURRENT_CATALOG_LOOKUPS: usize = 4;
const ENDPOINT_ADDR_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_STORE_GC_INTERVAL: Duration = Duration::from_mins(5);
const PUBLISH_TAG_PREFIX: &str = "agentenv:p2p:v1:";

enum BlobFetchSource {
    Local {
        blob_hash: iroh_blobs::Hash,
    },
    Remote {
        blob_hash: iroh_blobs::Hash,
        providers: Vec<iroh::EndpointId>,
    },
}

/// P2P transport based on iroh + iroh-blobs.
///
/// The transport exposes two protocols over one iroh endpoint:
///
/// - iroh-blobs serves and downloads artifact bytes by blob hash.
/// - the AgentENV catalog protocol resolves [`P2pArtifactKey`] values into
///   transport-neutral descriptors whose backend locator contains the iroh
///   blob hash needed by this backend.
///
/// Consumers should keep depending on [`P2pTransport`]; this type is only
/// the concrete backend selected by P2P configuration.
pub struct IrohBlobsP2pTransport {
    node_id: String,
    peer_discovery: Arc<dyn P2pPeerDiscovery>,
    store: FsStore,
    router: Router,
    downloader: Downloader,
    lookup: MemoryLookup,
    /// Artifacts this node has published and can advertise to peers.
    published_catalog: PublishedArtifactCatalog,
    local_endpoint: P2pEndpoint,
    lookup_timeout: Duration,
    fetch_timeout: Duration,
    pending_gc: Arc<AtomicBool>,
}

impl IrohBlobsP2pTransport {
    pub async fn new(
        config: ResolvedP2pConfig,
        node_id: String,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
    ) -> Result<Self> {
        Self::new_with_gc_interval(config, node_id, peer_discovery, DEFAULT_STORE_GC_INTERVAL).await
    }

    async fn new_with_gc_interval(
        config: ResolvedP2pConfig,
        node_id: String,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
        gc_interval: Duration,
    ) -> Result<Self> {
        let store_dir = config.store_dir.join(IROH_BACKEND_ID);
        tokio::fs::create_dir_all(&store_dir)
            .await
            .with_context(|| format!("create P2P blob store dir {}", store_dir.display()))?;

        // Run one startup GC pass to clean up blobs whose retention tags were
        // removed before a previous process exited.
        let pending_gc = Arc::new(AtomicBool::new(true));
        let mut store_options = FsStoreOptions::new(&store_dir);
        store_options.gc = Some(gated_gc_config(gc_interval, pending_gc.clone()));
        let store = FsStore::load_with_opts(store_dir.join("blobs.db"), store_options)
            .await
            .with_context(|| format!("open P2P blob store {}", store_dir.display()))?;

        let lookup = MemoryLookup::new();
        let mut builder = Endpoint::builder(presets::Minimal).address_lookup(lookup.clone());

        if let Some(listen_addr) = config.listen_addr.as_deref() {
            let addr: SocketAddr = listen_addr
                .parse()
                .with_context(|| format!("parse p2p.listen_addr {listen_addr}"))?;
            builder = builder
                .clear_ip_transports()
                .bind_addr(addr)
                .map_err(|err| {
                    Error::internal_message("bind configured P2P listen address", err)
                })?;
        }

        let endpoint = builder.bind().await.context("bind P2P endpoint")?;
        let local_addr = wait_for_dialable_addr(&endpoint)
            .await
            .context("wait for P2P endpoint address")?;
        let local_endpoint = P2pEndpoint::from_iroh_addr(&local_addr)?;
        let catalog_path = store_dir.join(CATALOG_DB_DIR);
        let published_catalog =
            PublishedArtifactCatalog::load(&catalog_path, &node_id, &local_endpoint).await?;

        // Serve both data and metadata from the same endpoint. The scheduler
        // only passes endpoint addresses around; it does not proxy catalog
        // requests or artifact bytes.
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
            .accept(
                CATALOG_ALPN,
                CatalogProtocol::new(published_catalog.clone()),
            )
            .spawn();
        let downloader = Downloader::new(&store, router.endpoint());

        info!(
            node_id,
            store_dir = %store_dir.display(),
            endpoint = %router.endpoint().id(),
            "iroh artifact transport started"
        );

        Ok(Self {
            node_id,
            peer_discovery,
            store,
            router,
            downloader,
            lookup,
            published_catalog,
            local_endpoint,
            lookup_timeout: config.lookup_timeout,
            fetch_timeout: config.fetch_timeout,
            pending_gc,
        })
    }

    #[instrument(level = "debug", skip(self), fields(peer = %peer.node_id))]
    async fn lookup_peer(
        &self,
        peer: &P2pPeer,
        key: &P2pArtifactKey,
    ) -> Result<Option<P2pArtifactDescriptor>> {
        // Scheduler endpoints are opaque until this backend parses them. Add
        // the peer address to iroh's in-memory lookup before dialing.
        let addr = peer.endpoint.to_iroh_addr()?;
        self.lookup.add_endpoint_info(addr.clone());
        let conn = self
            .router
            .endpoint()
            .connect(addr, CATALOG_ALPN)
            .await
            .with_context(|| format!("connect to P2P catalog peer {}", peer.node_id))?;
        let (mut send, mut recv) = conn.open_bi().await.context("open P2P catalog stream")?;
        let request = CatalogRequest { key: key.clone() };
        let request_bytes =
            serde_json::to_vec(&request).context("serialize P2P catalog lookup request")?;
        send.write_all(&request_bytes)
            .await
            .context("send P2P catalog lookup request")?;
        send.finish().context("finish P2P catalog request stream")?;
        let response_bytes = recv
            .read_to_end(MAX_CATALOG_RESPONSE_BYTES)
            .await
            .context("read P2P catalog lookup response")?;
        let response: CatalogResponse =
            serde_json::from_slice(&response_bytes).context("parse P2P catalog response")?;
        conn.close(0_u32.into(), b"done");
        Ok(response
            .descriptor
            .and_then(|descriptor| resolve_remote_local_providers(descriptor, peer)))
    }

    async fn lookup_peer_with_timeout(
        &self,
        peer: &P2pPeer,
        key: &P2pArtifactKey,
    ) -> Result<Option<P2pArtifactDescriptor>> {
        tokio::time::timeout(self.lookup_timeout, self.lookup_peer(peer, key))
            .await
            .map_err(|_| Error::Timeout {
                operation: "lookup P2P artifact from peer",
            })?
    }

    async fn lookup_peers(
        &self,
        peers: Vec<P2pPeer>,
        key: &P2pArtifactKey,
    ) -> Option<P2pArtifactDescriptor> {
        first_some_buffered_in_order(peers, MAX_CONCURRENT_CATALOG_LOOKUPS, |peer| async move {
            if peer.node_id == self.node_id || peer.endpoint == self.local_endpoint {
                let descriptor = self.get_local(key).await;
                if descriptor.is_some() {
                    trace!("P2P lookup found local descriptor matching peer discovery");
                }
                return descriptor;
            }
            match self.lookup_peer_with_timeout(&peer, key).await {
                Ok(None) => None,
                Ok(Some(descriptor)) => {
                    trace!(peer = %peer.node_id, "P2P lookup found remote descriptor");
                    Some(descriptor)
                }
                Err(err) => {
                    debug!(
                        peer = %peer.node_id,
                        error = %err,
                        "P2P artifact catalog lookup failed; trying remaining peers"
                    );
                    None
                }
            }
        })
        .await
    }

    async fn get_local(&self, key: &P2pArtifactKey) -> Option<P2pArtifactDescriptor> {
        self.published_catalog.descriptor_for(key).await
    }

    /// Export a local blob to a caller-requested destination path and return its size in bytes.
    async fn export_local_blob(
        &self,
        blob_hash: iroh_blobs::Hash,
        destination: &Path,
    ) -> Result<u64> {
        self.store
            .export(blob_hash, destination)
            .await
            .map_err(|err| Error::internal_message("export local P2P blob", err))
    }

    async fn export_local_blob_range(
        &self,
        blob_hash: iroh_blobs::Hash,
        range: Range<u64>,
    ) -> Result<P2pByteStream> {
        let expected_len =
            range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| Error::InvalidDescriptor {
                    reason: "range end before start".to_string(),
                })?;
        let range_start = range.start;
        let range_end = range.end;
        let inner = Box::pin(self.store.export_ranges(blob_hash, range).stream());
        let stream = stream::unfold(
            (inner, expected_len, false),
            move |(mut inner, mut remaining, done_after_error)| async move {
                if done_after_error || remaining == 0 {
                    return None;
                }
                while let Some(item) = inner.next().await {
                    match item {
                        ExportRangesItem::Size(_) => {}
                        ExportRangesItem::Data(leaf) => {
                            let Ok(data_len) = u64::try_from(leaf.data.len()) else {
                                return Some((
                                    Err(Error::InvalidDescriptor {
                                        reason: "exported range chunk length does not fit u64"
                                            .to_string(),
                                    }),
                                    (inner, 0, true),
                                ));
                            };
                            let Some(leaf_end) = leaf.offset.checked_add(data_len) else {
                                return Some((
                                    Err(Error::InvalidDescriptor {
                                        reason: "exported range chunk end overflow".to_string(),
                                    }),
                                    (inner, 0, true),
                                ));
                            };
                            let overlap_start = leaf.offset.max(range_start);
                            let overlap_end = leaf_end.min(range_end);
                            if overlap_start >= overlap_end {
                                continue;
                            }
                            let start = (overlap_start - leaf.offset) as usize;
                            let end = (overlap_end - leaf.offset) as usize;
                            let bytes = leaf.data.slice(start..end);
                            remaining = remaining.saturating_sub(bytes.len() as u64);
                            return Some((Ok(bytes), (inner, remaining, false)));
                        }
                        ExportRangesItem::Error(err) => {
                            return Some((
                                Err(Error::internal_message("export local P2P blob range", err)),
                                (inner, 0, true),
                            ));
                        }
                    }
                }
                if remaining == 0 {
                    None
                } else {
                    Some((
                        Err(Error::InvalidDescriptor {
                            reason: format!("exported range ended {remaining} bytes short"),
                        }),
                        (inner, 0, true),
                    ))
                }
            },
        );
        Ok(Box::pin(stream))
    }

    async fn read_local_blob_bytes(&self, blob_hash: iroh_blobs::Hash) -> Result<Bytes> {
        self.store
            .get_bytes(blob_hash)
            .await
            .map_err(|err| Error::internal_message("read local P2P blob bytes", err))
    }

    async fn advertise_local_blob(
        &self,
        key: &P2pArtifactKey,
        blob_hash: iroh_blobs::Hash,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        self.store
            .tags()
            .set(publish_tag_name(key), HashAndFormat::raw(blob_hash))
            .await
            .map_err(|err| Error::internal_message("retain local P2P artifact blob", err))?;

        let local_descriptor = P2pArtifactDescriptor {
            key: key.clone(),
            // TODO: include other providers if this artifact was previously fetched from a peer.
            providers: vec![P2pArtifactProvider::Local],
            backend_locator: Some(blob_hash.to_string()),
            metadata: metadata.clone(),
        };
        self.published_catalog.upsert(local_descriptor).await?;
        Ok(())
    }

    async fn try_advertise_fetched_blob(
        &self,
        descriptor: &P2pArtifactDescriptor,
        blob_hash: iroh_blobs::Hash,
    ) {
        if let Err(err) = self
            .advertise_local_blob(&descriptor.key, blob_hash, &descriptor.metadata)
            .await
        {
            warn!(error = %err, "failed to advertise fetched P2P artifact");
            return;
        }
        if let Err(err) = self.peer_discovery.record_key(&descriptor.key).await {
            warn!(
                error = %err,
                "failed to record fetched P2P artifact in scheduler"
            );
        }
    }

    async fn download_blob(
        &self,
        request: GetRequest,
        providers: Vec<iroh::EndpointId>,
    ) -> Result<()> {
        let operation = "download P2P artifact blob";
        let download = self.downloader.download(request, providers);
        tokio::time::timeout(self.fetch_timeout, download)
            .await
            .map_err(|_| Error::Timeout { operation })?
            .map_err(|err| Error::internal_message(operation, err))?;
        Ok(())
    }

    fn resolve_fetch_source(&self, descriptor: &P2pArtifactDescriptor) -> Result<BlobFetchSource> {
        let blob_hash = blob_hash_from_descriptor(descriptor)?;

        if descriptor
            .providers
            .iter()
            .any(P2pArtifactProvider::is_local)
        {
            return Ok(BlobFetchSource::Local { blob_hash });
        }

        let providers: Vec<_> = descriptor
            .providers
            .iter()
            .filter_map(|provider| {
                let P2pArtifactProvider::Peer(peer) = provider else {
                    return None;
                };
                let addr = peer.endpoint.to_iroh_addr().ok()?;
                let provider_id = addr.id;
                self.lookup.add_endpoint_info(addr);
                Some(provider_id)
            })
            .collect();

        if providers.is_empty() {
            return Err(Error::InvalidDescriptor {
                reason: format!(
                    "no valid P2P providers found in descriptor for key {}",
                    descriptor.key
                ),
            });
        }

        Ok(BlobFetchSource::Remote {
            blob_hash,
            providers,
        })
    }
}

#[async_trait]
impl P2pTransport for IrohBlobsP2pTransport {
    #[instrument(skip(self, hints), fields(key = %key, hints_len = hints.len()))]
    async fn lookup_with_hints(
        &self,
        key: &P2pArtifactKey,
        hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>> {
        if let Some(descriptor) = self.get_local(key).await {
            trace!("P2P lookup found local descriptor");
            return Ok(Some(descriptor));
        }

        match self.peer_discovery.peers_for_key(key).await {
            Ok(peers) => {
                if let Some(descriptor) = self.lookup_peers(peers, key).await {
                    trace!("P2P lookup found descriptor through scheduler artifact index");
                    // Currently use short-circuit provider discovery for simplicity.
                    return Ok(Some(descriptor));
                }
            }
            Err(err) => {
                debug!(
                    error = %err,
                    "P2P scheduler artifact lookup failed; falling back to peer discovery"
                );
            }
        }

        let peers = self.peer_discovery.peers_with_hints(hints).await?;
        if let Some(descriptor) = self.lookup_peers(peers, key).await {
            return Ok(Some(descriptor));
        }
        trace!("P2P lookup completed without descriptor");
        Ok(None)
    }

    #[instrument(
        skip(self, descriptor),
        fields(key = %descriptor.key, destination = %destination.display())
    )]
    async fn fetch(&self, descriptor: &P2pArtifactDescriptor, destination: &Path) -> Result<u64> {
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("create P2P fetch destination dir {}", parent.display())
            })?;
        }

        let (blob_hash, providers) = match self.resolve_fetch_source(descriptor)? {
            BlobFetchSource::Local { blob_hash } => {
                // A local lookup hit still goes through export so callers get the
                // artifact at the requested destination rather than a store path.
                let size = self.export_local_blob(blob_hash, destination).await?;
                debug!(size, "fetched artifact from local P2P store");
                return Ok(size);
            }
            BlobFetchSource::Remote {
                blob_hash,
                providers,
            } => (blob_hash, providers),
        };
        let providers_count = providers.len();

        // Download into the local FsStore first, then export to the caller's destination.
        self.download_blob(GetRequest::blob(blob_hash), providers)
            .await?;
        self.try_advertise_fetched_blob(descriptor, blob_hash).await;
        let size = self.export_local_blob(blob_hash, destination).await?;
        debug!(providers_count, size, "fetched artifact from P2P provider");
        Ok(size)
    }

    #[instrument(skip(self, descriptor), fields(key = %descriptor.key))]
    async fn fetch_bytes(&self, descriptor: &P2pArtifactDescriptor) -> Result<Bytes> {
        let (blob_hash, providers) = match self.resolve_fetch_source(descriptor)? {
            BlobFetchSource::Local { blob_hash } => {
                let bytes = self.read_local_blob_bytes(blob_hash).await?;
                debug!(
                    size = bytes.len(),
                    "fetched artifact bytes from local P2P store"
                );
                return Ok(bytes);
            }
            BlobFetchSource::Remote {
                blob_hash,
                providers,
            } => (blob_hash, providers),
        };
        let providers_count = providers.len();

        // Full in-memory fetches download the complete blob into the local store
        // first, matching file fetch semantics and allowing this node to serve
        // the artifact to later peers.
        self.download_blob(GetRequest::blob(blob_hash), providers)
            .await?;
        self.try_advertise_fetched_blob(descriptor, blob_hash).await;
        let bytes = self.read_local_blob_bytes(blob_hash).await?;
        debug!(
            providers_count,
            size = bytes.len(),
            "fetched artifact bytes from P2P provider"
        );
        Ok(bytes)
    }

    #[instrument(
        skip(self, descriptor),
        fields(key = %descriptor.key, offset, len)
    )]
    async fn fetch_byte_range(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream> {
        if len == 0 {
            return Ok(Box::pin(stream::empty()));
        }
        let end = offset
            .checked_add(u64::try_from(len).map_err(|err| Error::InvalidDescriptor {
                reason: format!("range length does not fit u64: {err}"),
            })?)
            .ok_or_else(|| Error::InvalidDescriptor {
                reason: "range end overflow".to_string(),
            })?;
        let range = offset..end;

        let (blob_hash, providers) = match self.resolve_fetch_source(descriptor)? {
            BlobFetchSource::Local { blob_hash } => {
                let stream = self.export_local_blob_range(blob_hash, range).await?;
                debug!("fetched artifact range from local P2P store");
                return Ok(stream);
            }
            BlobFetchSource::Remote {
                blob_hash,
                providers,
            } => (blob_hash, providers),
        };
        let providers_count = providers.len();

        // Range reads are foreground acceleration only: the downloaded bytes
        // land in the iroh store but are not retention-tagged here, so GC may
        // reclaim them unless the full layer is later published by background
        // download.
        let ranges = ChunkRanges::bytes(range.clone());
        self.download_blob(GetRequest::blob_ranges(blob_hash, ranges), providers)
            .await?;
        let stream = self.export_local_blob_range(blob_hash, range).await?;
        debug!(providers_count, "fetched artifact range from P2P provider");
        Ok(stream)
    }

    #[instrument(
        skip(self, request),
        fields(key = %request.key, source = %request.source, publish_mode = ?request.publish_mode)
    )]
    async fn publish(&self, request: &P2pPublishRequest) -> Result<()> {
        let imported = match &request.source {
            P2pPublishSource::Path(source) => {
                let import_mode = match request.publish_mode {
                    P2pPublishMode::Copy => ImportMode::Copy,
                    P2pPublishMode::Reference => ImportMode::TryReference,
                };
                self.store
                    .add_path_with_opts(AddPathOptions {
                        path: source.clone(),
                        format: BlobFormat::Raw,
                        mode: import_mode,
                    })
                    .temp_tag()
                    .await
                    .map_err(|err| {
                        Error::internal_message("import file into iroh-blobs store", err)
                    })?
            }
            P2pPublishSource::Bytes(bytes) => self
                .store
                .add_bytes(bytes.clone())
                .temp_tag()
                .await
                .map_err(|err| {
                    Error::internal_message("import bytes into iroh-blobs store", err)
                })?,
        };
        self.advertise_local_blob(&request.key, imported.hash(), &request.metadata)
            .await?;
        if let Err(err) = self.peer_discovery.record_key(&request.key).await {
            warn!(error = %err, "failed to record P2P artifact in scheduler");
        }
        debug!("published artifact into P2P transport");
        Ok(())
    }

    #[instrument(skip(self), fields(key = %key))]
    async fn unpublish(&self, key: &P2pArtifactKey) -> Result<bool> {
        if self.published_catalog.remove(key).await?.is_none() {
            debug!("P2P unpublish skipped missing local artifact");
            return Ok(false);
        };

        self.store
            .tags()
            .delete(publish_tag_name(key))
            .await
            .map_err(|err| Error::internal_message("delete P2P artifact retention tag", err))?;
        self.pending_gc.store(true, Ordering::Release);
        if let Err(err) = self.peer_discovery.forget_key(key).await {
            warn!(error = %err, "failed to forget P2P artifact in scheduler");
        }
        debug!("unpublished artifact from P2P transport");
        Ok(true)
    }

    fn local_endpoint(&self) -> Option<P2pEndpoint> {
        Some(self.local_endpoint.clone())
    }

    async fn shutdown(&self) -> Result<()> {
        self.router
            .shutdown()
            .await
            .map_err(|err| Error::internal_message("shutdown embedded P2P endpoint", err))
    }
}

impl Drop for IrohBlobsP2pTransport {
    fn drop(&mut self) {
        if self.router.is_shutdown() {
            return;
        }
        let router = self.router.clone();
        tokio::spawn(async move {
            if let Err(err) = router.shutdown().await {
                debug!(error = %err, "embedded iroh-blobs artifact transport shutdown failed");
            }
        });
    }
}

/// Wait until iroh has produced an address that other nodes can dial.
///
/// Binding can complete before the endpoint has enough address information for
/// scheduler advertisement, especially when discovery transports are still
/// initializing.
async fn wait_for_dialable_addr(endpoint: &Endpoint) -> Result<EndpointAddr> {
    let mut watch_addr = endpoint.watch_addr();
    loop {
        let addr = watch_addr.get();
        if !addr.is_empty() {
            return Ok(addr);
        }
        tokio::time::timeout(ENDPOINT_ADDR_TIMEOUT, watch_addr.updated())
            .await
            .map_err(|_| Error::Timeout {
                operation: "wait for P2P endpoint network address",
            })?
            .context("P2P endpoint address watcher disconnected")?;
    }
}

fn blob_hash_from_descriptor(descriptor: &P2pArtifactDescriptor) -> Result<iroh_blobs::Hash> {
    let Some(blob_hash) = descriptor.backend_locator.as_deref() else {
        return Err(Error::InvalidDescriptor {
            reason: format!("missing iroh blob hash locator for key {}", descriptor.key),
        });
    };

    blob_hash.parse().map_err(|err| Error::InvalidDescriptor {
        reason: format!(
            "invalid iroh blob hash locator for key {}: {err}",
            descriptor.key
        ),
    })
}

async fn first_some_buffered_in_order<I, F, Fut, T>(
    items: impl IntoIterator<Item = I>,
    concurrency: usize,
    lookup: F,
) -> Option<T>
where
    F: FnMut(I) -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let mut results = stream::iter(items).map(lookup).buffered(concurrency.max(1));
    while let Some(result) = results.next().await {
        if result.is_some() {
            return result;
        }
    }
    None
}

fn resolve_remote_local_providers(
    mut descriptor: P2pArtifactDescriptor,
    response_peer: &P2pPeer,
) -> Option<P2pArtifactDescriptor> {
    let mut providers = Vec::with_capacity(descriptor.providers.len());
    let mut seen = HashSet::with_capacity(descriptor.providers.len());
    for provider in descriptor.providers {
        let peer = match provider {
            P2pArtifactProvider::Local => response_peer.clone(),
            P2pArtifactProvider::Peer(peer) => peer,
        };
        if seen.insert(peer.clone()) {
            providers.push(P2pArtifactProvider::from(peer));
        }
    }
    if providers.is_empty() {
        return None;
    }
    descriptor.providers = providers;
    Some(descriptor)
}

fn publish_tag_name(key: &P2pArtifactKey) -> String {
    format!("{PUBLISH_TAG_PREFIX}{}", digest::sha256_hex(key.as_bytes()))
}

fn gated_gc_config(interval: Duration, pending_gc: Arc<AtomicBool>) -> GcConfig {
    GcConfig {
        interval,
        add_protected: Some(Arc::new(move |_| {
            let pending_gc = pending_gc.clone();
            Box::pin(async move {
                if pending_gc.swap(false, Ordering::AcqRel) {
                    ProtectOutcome::Continue
                } else {
                    ProtectOutcome::Abort
                }
            })
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::{Barrier, Notify};

    use crate::cfg::P2pConfig;
    use crate::p2p::config::ResolvedP2pConfig;
    use crate::p2p::discovery::P2pPeerDiscovery;
    use crate::p2p::transport::P2pTransport;
    use crate::p2p::{NoopP2pPeerDiscovery, P2pPeer, P2pTransportKind, StaticP2pPeerDiscovery};

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    #[tokio::test]
    async fn buffered_lookup_preserves_candidate_order() {
        let release_first = Arc::new(Notify::new());
        let first_started = Arc::new(Notify::new());
        let second_finished = Arc::new(Notify::new());

        let lookup = tokio::spawn({
            let release_first = release_first.clone();
            let first_started = first_started.clone();
            let second_finished = second_finished.clone();
            async move {
                first_some_buffered_in_order([0, 1], 2, move |candidate| {
                    let release_first = release_first.clone();
                    let first_started = first_started.clone();
                    let second_finished = second_finished.clone();
                    async move {
                        match candidate {
                            0 => {
                                first_started.notify_one();
                                release_first.notified().await;
                                Some("first")
                            }
                            1 => {
                                second_finished.notify_one();
                                Some("second")
                            }
                            _ => None,
                        }
                    }
                })
                .await
            }
        });

        first_started.notified().await;
        second_finished.notified().await;
        assert!(
            !lookup.is_finished(),
            "a lower-priority result must wait for earlier candidates"
        );

        release_first.notify_one();
        assert_eq!(lookup.await.expect("lookup task"), Some("first"));
    }

    #[tokio::test]
    async fn buffered_lookup_limits_in_flight_candidates() {
        let current = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let first_window_ready = Arc::new(Barrier::new(5));
        let release_first_window = Arc::new(Barrier::new(5));

        let lookup = tokio::spawn({
            let current = current.clone();
            let maximum = maximum.clone();
            let first_window_ready = first_window_ready.clone();
            let release_first_window = release_first_window.clone();
            async move {
                first_some_buffered_in_order(0..8, 4, move |candidate| {
                    let current = current.clone();
                    let maximum = maximum.clone();
                    let first_window_ready = first_window_ready.clone();
                    let release_first_window = release_first_window.clone();
                    async move {
                        let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                        maximum.fetch_max(active, Ordering::SeqCst);
                        if candidate < 4 {
                            first_window_ready.wait().await;
                            release_first_window.wait().await;
                        }
                        current.fetch_sub(1, Ordering::SeqCst);
                        None::<()>
                    }
                })
                .await
            }
        });

        first_window_ready.wait().await;
        assert_eq!(current.load(Ordering::SeqCst), 4);
        release_first_window.wait().await;
        assert_eq!(lookup.await.expect("lookup task"), None);
        assert_eq!(maximum.load(Ordering::SeqCst), 4);
    }

    async fn collect_range_stream(mut stream: P2pByteStream) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out)
    }

    fn p2p_config(store_dir: std::path::PathBuf) -> P2pConfig {
        P2pConfig {
            enabled: true,
            transport: P2pTransportKind::Iroh,
            store_dir,
            listen_addr: "127.0.0.1:0".to_string(),
            lookup_timeout_ms: 10_000,
            fetch_timeout_ms: 10_000,
            ..P2pConfig::default()
        }
    }

    async fn test_transport(
        config: &P2pConfig,
        node_id: &str,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
    ) -> Result<IrohBlobsP2pTransport> {
        test_transport_with_gc_interval(config, node_id, peer_discovery, DEFAULT_STORE_GC_INTERVAL)
            .await
    }

    async fn test_transport_with_gc_interval(
        config: &P2pConfig,
        node_id: &str,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
        gc_interval: std::time::Duration,
    ) -> Result<IrohBlobsP2pTransport> {
        IrohBlobsP2pTransport::new_with_gc_interval(
            ResolvedP2pConfig::from_config(config),
            node_id.to_string(),
            peer_discovery,
            gc_interval,
        )
        .await
    }

    async fn test_provider_consumer(
        temp: &tempfile::TempDir,
    ) -> Result<(IrohBlobsP2pTransport, IrohBlobsP2pTransport)> {
        let provider = test_transport(
            &p2p_config(temp.path().join("provider-store")),
            "provider-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start provider P2P transport")?;
        let provider_endpoint = provider
            .local_endpoint()
            .context("provider transport should expose a local endpoint")?;
        let consumer = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "consumer-node",
            Arc::new(StaticP2pPeerDiscovery::new(vec![P2pPeer {
                node_id: "provider-node".to_string(),
                endpoint: provider_endpoint,
            }])),
        )
        .await
        .context("start consumer P2P transport")?;
        Ok((provider, consumer))
    }

    fn invalid_endpoint() -> P2pEndpoint {
        P2pEndpoint {
            backend: "iroh".to_string(),
            address: "not an iroh endpoint".to_string(),
        }
    }

    fn first_peer_provider(descriptor: &P2pArtifactDescriptor) -> &P2pPeer {
        descriptor
            .providers
            .iter()
            .find_map(|provider| match provider {
                P2pArtifactProvider::Local => None,
                P2pArtifactProvider::Peer(peer) => Some(peer),
            })
            .expect("descriptor should have a concrete peer provider")
    }

    #[test]
    fn remote_catalog_descriptor_resolves_local_providers_to_response_peer() {
        let peer = P2pPeer {
            node_id: "provider-node".to_string(),
            endpoint: invalid_endpoint(),
        };
        let descriptor = P2pArtifactDescriptor {
            key: "test/p2p/iroh/remote-local-provider".to_string(),
            providers: vec![
                P2pArtifactProvider::Local,
                P2pArtifactProvider::from(peer.clone()),
            ],
            backend_locator: Some("blob-hash".to_string()),
            metadata: serde_json::Value::Null,
        };

        let descriptor = resolve_remote_local_providers(descriptor, &peer)
            .expect("peer provider should keep descriptor usable");

        assert_eq!(descriptor.providers, vec![P2pArtifactProvider::from(peer)]);
    }

    #[test]
    fn remote_catalog_descriptor_with_only_local_provider_uses_response_peer() {
        let peer = P2pPeer {
            node_id: "provider-node".to_string(),
            endpoint: invalid_endpoint(),
        };
        let descriptor = P2pArtifactDescriptor {
            key: "test/p2p/iroh/remote-only-local-provider".to_string(),
            providers: vec![P2pArtifactProvider::Local],
            backend_locator: Some("blob-hash".to_string()),
            metadata: serde_json::Value::Null,
        };

        let descriptor = resolve_remote_local_providers(descriptor, &peer)
            .expect("response peer should make descriptor usable");

        assert_eq!(descriptor.providers, vec![P2pArtifactProvider::from(peer)]);
    }

    #[tokio::test]
    async fn transport_looks_up_and_fetches_from_peer_over_loopback() -> Result<()> {
        crate::logging::init_for_tests();

        tokio::time::timeout(TEST_TIMEOUT, async {
            let temp = tempfile::tempdir().context("create temp test dir")?;
            let (provider, consumer) = test_provider_consumer(&temp).await?;

            let key = "test/p2p/iroh/network-fetch".to_string();
            let bytes = b"artifact bytes served through the embedded iroh-blobs transport";
            provider
                .publish(
                    &P2pPublishRequest::bytes(key.clone(), bytes.as_slice())
                        .with_metadata(serde_json::json!({ "kind": "network-integration-test" })),
                )
                .await
                .context("publish provider bytes artifact")?;

            let sibling_key = "test/p2p/iroh/network-fetch-sibling".to_string();
            let sibling_source = temp.path().join("sibling-artifact.bin");
            tokio::fs::write(&sibling_source, b"sibling artifact bytes")
                .await
                .context("write sibling artifact")?;
            provider
                .publish(&P2pPublishRequest::file(
                    sibling_key.clone(),
                    sibling_source,
                ))
                .await
                .context("publish sibling provider artifact")?;

            assert!(provider.get_local(&sibling_key).await.is_some());
            assert!(
                consumer.get_local(&key).await.is_none(),
                "consumer must not satisfy lookup from its own local catalog"
            );

            let descriptor = consumer
                .lookup(&key)
                .await?
                .context("expected descriptor from provider")?;
            assert_eq!(descriptor.key, key);
            assert_eq!(first_peer_provider(&descriptor).node_id, "provider-node");
            assert_eq!(
                descriptor.metadata,
                serde_json::json!({ "kind": "network-integration-test" })
            );
            assert!(blob_hash_from_descriptor(&descriptor).is_ok());
            assert_eq!(first_peer_provider(&descriptor).endpoint.backend, "iroh");

            let destination = temp.path().join("downloaded").join("artifact.bin");
            let fetched_size = consumer
                .fetch(&descriptor, &destination)
                .await
                .context("fetch provider blob over P2P")?;
            assert_eq!(fetched_size, bytes.len() as u64);

            let downloaded = tokio::fs::read(&destination)
                .await
                .context("read downloaded artifact")?;
            assert_eq!(downloaded, bytes);
            let local_descriptor = consumer
                .get_local(&key)
                .await
                .context("consumer should advertise fetched artifact")?;
            assert_eq!(local_descriptor.providers, vec![P2pArtifactProvider::Local]);
            assert_eq!(
                blob_hash_from_descriptor(&local_descriptor)?,
                blob_hash_from_descriptor(&descriptor)?
            );

            let relay = test_transport(
                &p2p_config(temp.path().join("relay-store")),
                "relay-node",
                Arc::new(StaticP2pPeerDiscovery::new(vec![P2pPeer {
                    node_id: "consumer-node".to_string(),
                    endpoint: consumer
                        .local_endpoint()
                        .context("consumer transport should expose a local endpoint")?,
                }])),
            )
            .await
            .context("start relay P2P transport")?;

            let relay_descriptor = relay
                .lookup(&key)
                .await?
                .context("relay should find consumer as provider after fetch")?;
            assert_eq!(
                first_peer_provider(&relay_descriptor).node_id,
                "consumer-node"
            );
            assert_eq!(
                first_peer_provider(&relay_descriptor).endpoint,
                consumer.local_endpoint().unwrap()
            );
            assert_eq!(
                blob_hash_from_descriptor(&relay_descriptor)?,
                blob_hash_from_descriptor(&descriptor)?
            );

            let relay_destination = temp.path().join("relay-download.bin");
            let relay_fetched_size = relay
                .fetch(&relay_descriptor, &relay_destination)
                .await
                .context("relay fetch blob from consumer")?;
            assert_eq!(relay_fetched_size, bytes.len() as u64);
            let relay_downloaded = tokio::fs::read(&relay_destination)
                .await
                .context("read relay downloaded artifact")?;
            assert_eq!(relay_downloaded, bytes);

            relay.shutdown().await.context("shutdown relay P2P")?;
            consumer.shutdown().await.context("shutdown consumer P2P")?;
            provider.shutdown().await.context("shutdown provider P2P")?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("P2P network test timed out"))?
    }

    #[tokio::test]
    async fn transport_fetch_range_reads_exact_bytes_without_advertising_partial_blob() -> Result<()>
    {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let (provider, consumer) = test_provider_consumer(&temp).await?;

        let key = "test/p2p/iroh/range-fetch".to_string();
        let bytes: Vec<u8> = (0..(128 * 1024)).map(|idx| (idx % 251) as u8).collect();
        provider
            .publish(&P2pPublishRequest::bytes(key.clone(), bytes.clone()))
            .await
            .context("publish provider bytes artifact")?;

        let descriptor = consumer
            .lookup(&key)
            .await?
            .context("expected descriptor from provider")?;
        let offset = 7777_u64;
        let len = 33_333_usize;
        let fetched = consumer
            .fetch_byte_range(&descriptor, offset, len)
            .await
            .context("fetch range from provider")?;
        let fetched = collect_range_stream(fetched).await?;
        assert_eq!(
            fetched.as_slice(),
            &bytes[offset as usize..offset as usize + len]
        );
        assert!(
            consumer.get_local(&key).await.is_none(),
            "range-only fetch must not advertise this node as a full artifact provider"
        );

        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn transport_fetch_bytes_reads_full_blob_and_advertises_it() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let (provider, consumer) = test_provider_consumer(&temp).await?;

        let key = "test/p2p/iroh/bytes-fetch".to_string();
        let bytes: Vec<u8> = (0..(128 * 1024)).map(|idx| (idx % 251) as u8).collect();
        provider
            .publish(&P2pPublishRequest::bytes(key.clone(), bytes.clone()))
            .await
            .context("publish provider bytes artifact")?;

        let descriptor = consumer
            .lookup(&key)
            .await?
            .context("expected descriptor from provider")?;
        let fetched = consumer
            .fetch_bytes(&descriptor)
            .await
            .context("fetch full blob bytes from provider")?;
        assert_eq!(fetched.as_ref(), bytes.as_slice());
        let local_descriptor = consumer
            .get_local(&key)
            .await
            .context("consumer should advertise full bytes fetch")?;
        assert_eq!(local_descriptor.providers, vec![P2pArtifactProvider::Local]);
        assert_eq!(
            blob_hash_from_descriptor(&local_descriptor)?,
            blob_hash_from_descriptor(&descriptor)?
        );

        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn fetch_rejects_descriptor_without_blob_hash_locator() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let consumer = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "consumer-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start consumer P2P transport")?;
        let source = temp.path().join("source-artifact.bin");
        tokio::fs::write(&source, b"unpublished bytes")
            .await
            .context("write source artifact")?;
        let descriptor = P2pArtifactDescriptor {
            key: "test/p2p/iroh/missing-blob-hash".to_string(),
            providers: vec![P2pArtifactProvider::from(P2pPeer {
                node_id: "provider-node".to_string(),
                endpoint: invalid_endpoint(),
            })],
            backend_locator: None,
            metadata: serde_json::Value::Null,
        };

        let err = consumer
            .fetch(&descriptor, &temp.path().join("downloaded.bin"))
            .await
            .expect_err("fetch should fail");

        assert!(matches!(err, Error::InvalidDescriptor { .. }));
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn published_catalog_survives_transport_restart() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let store_dir = temp.path().join("provider-store");
        let config = p2p_config(store_dir);
        let key = "test/p2p/iroh/persisted-catalog".to_string();
        let bytes = b"artifact bytes retained across an iroh transport restart";

        let provider = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("start provider P2P transport")?;
        provider
            .publish(&P2pPublishRequest::bytes(key.clone(), bytes.as_slice()))
            .await
            .context("publish artifact")?;
        let before_restart = provider.get_local(&key).await;
        assert_eq!(
            before_restart
                .as_ref()
                .context("descriptor before restart")?
                .providers,
            vec![P2pArtifactProvider::Local]
        );
        provider.shutdown().await.context("shutdown provider P2P")?;
        drop(provider);

        let restarted = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("restart provider P2P transport")?;
        let descriptor = restarted
            .get_local(&key)
            .await
            .context("list local catalog after restart")?;
        assert_eq!(descriptor.key, key);
        assert_eq!(descriptor.providers, vec![P2pArtifactProvider::Local]);
        assert!(blob_hash_from_descriptor(&descriptor).is_ok());

        let destination = temp.path().join("downloaded-after-restart.bin");
        let fetched_size = restarted
            .fetch(&descriptor, &destination)
            .await
            .context("fetch persisted local artifact")?;
        assert_eq!(fetched_size, bytes.len() as u64);
        let downloaded = tokio::fs::read(&destination)
            .await
            .context("read downloaded artifact")?;
        assert_eq!(downloaded, bytes);
        restarted
            .shutdown()
            .await
            .context("shutdown restarted provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn unpublish_stops_remote_lookup_and_persists_across_restart() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let store_dir = temp.path().join("provider-store");
        let config = p2p_config(store_dir.clone());
        let key = "test/p2p/iroh/unpublish-persists".to_string();
        let provider = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("start provider P2P transport")?;
        let provider_endpoint = provider
            .local_endpoint()
            .context("provider transport should expose a local endpoint")?;
        let consumer = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "consumer-node",
            Arc::new(StaticP2pPeerDiscovery::new(vec![P2pPeer {
                node_id: "provider-node".to_string(),
                endpoint: provider_endpoint.clone(),
            }])),
        )
        .await
        .context("start consumer P2P transport")?;

        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"artifact bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;
        assert!(
            consumer.lookup(&key).await?.is_some(),
            "consumer should find provider before unpublish"
        );

        assert!(provider.unpublish(&key).await?);

        assert!(
            consumer.lookup(&key).await?.is_none(),
            "consumer should stop finding provider after unpublish"
        );
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        drop(provider);

        let restarted = test_transport(&config, "provider-node", Arc::new(NoopP2pPeerDiscovery))
            .await
            .context("restart provider P2P transport")?;
        assert!(restarted.get_local(&key).await.is_none());
        restarted
            .shutdown()
            .await
            .context("shutdown restarted provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn unpublish_deletes_retention_tag_and_removes_blob() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let provider = test_transport_with_gc_interval(
            &p2p_config(temp.path().join("provider-store")),
            "provider-node",
            Arc::new(NoopP2pPeerDiscovery),
            Duration::from_millis(100),
        )
        .await
        .context("start provider P2P transport")?;

        let key = "test/p2p/iroh/unpublish-gc".to_string();
        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"artifact bytes eligible for gc".as_slice(),
            ))
            .await
            .context("publish artifact")?;
        let descriptor = provider
            .get_local(&key)
            .await
            .context("descriptor before unpublish")?;
        let blob_hash = blob_hash_from_descriptor(&descriptor)?;
        let tag_name = publish_tag_name(&key);
        assert!(
            !provider
                .unpublish(&"test/p2p/iroh/missing-unpublish".to_string())
                .await?,
            "missing key unpublish should be a no-op"
        );
        assert!(
            provider
                .store
                .tags()
                .get(&tag_name)
                .await
                .map_err(|err| Error::internal_message("get publish tag", err))?
                .is_some(),
            "publish should create deterministic retention tag"
        );
        assert!(provider
            .store
            .blobs()
            .has(blob_hash)
            .await
            .map_err(|err| Error::internal_message("check blob presence", err))?);

        assert!(provider.unpublish(&key).await?);

        assert!(provider.get_local(&key).await.is_none());
        assert!(
            provider.lookup(&key).await?.is_none(),
            "local lookup should stop after unpublish"
        );
        assert!(
            provider
                .store
                .tags()
                .get(&tag_name)
                .await
                .map_err(|err| Error::internal_message("get publish tag", err))?
                .is_none(),
            "unpublish should remove deterministic retention tag"
        );

        // Wait for the GC pass to remove the blob from the store after unpublish.
        let start = std::time::Instant::now();
        loop {
            if !provider
                .store
                .blobs()
                .has(blob_hash)
                .await
                .map_err(|err| Error::internal_message("check blob presence", err))?
            {
                break;
            }
            if start.elapsed() >= Duration::from_secs(5) {
                return Err(Error::Timeout {
                    operation: "wait for P2P blob GC",
                });
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn fetch_rejects_descriptor_without_providers() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let (provider, consumer) = test_provider_consumer(&temp).await?;

        let key = "test/p2p/iroh/missing-provider-endpoint".to_string();
        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"artifact bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;
        let descriptor = consumer.lookup(&key).await?.context("descriptor")?;
        let descriptor = P2pArtifactDescriptor {
            providers: Vec::new(),
            ..descriptor
        };

        let err = consumer
            .fetch(&descriptor, &temp.path().join("downloaded.bin"))
            .await
            .expect_err("fetch should fail");

        assert!(matches!(err, Error::InvalidDescriptor { .. }));
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn lookup_ignores_unreachable_peer_and_returns_first_descriptor() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let provider = test_transport(
            &p2p_config(temp.path().join("provider-store")),
            "provider-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start provider P2P transport")?;
        let provider_endpoint = provider
            .local_endpoint()
            .context("provider transport should expose a local endpoint")?;
        let consumer = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "consumer-node",
            Arc::new(StaticP2pPeerDiscovery::new(vec![
                P2pPeer {
                    node_id: "bad-node".to_string(),
                    endpoint: invalid_endpoint(),
                },
                P2pPeer {
                    node_id: "provider-node".to_string(),
                    endpoint: provider_endpoint,
                },
            ])),
        )
        .await
        .context("start consumer P2P transport")?;

        let key = "test/p2p/iroh/lookup-skips-bad-peer".to_string();
        provider
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"artifact bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;

        let descriptor = consumer
            .lookup(&key)
            .await?
            .context("lookup provider catalog")?;

        assert_eq!(first_peer_provider(&descriptor).node_id, "provider-node");
        assert!(blob_hash_from_descriptor(&descriptor).is_ok());
        consumer.shutdown().await.context("shutdown consumer P2P")?;
        provider.shutdown().await.context("shutdown provider P2P")?;
        Ok(())
    }

    #[tokio::test]
    async fn lookup_skips_self_peer() -> Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let self_transport = test_transport(
            &p2p_config(temp.path().join("self-store")),
            "self-node",
            Arc::new(NoopP2pPeerDiscovery),
        )
        .await
        .context("start self P2P transport")?;
        let self_endpoint = self_transport
            .local_endpoint()
            .context("self transport should expose a local endpoint")?;
        self_transport
            .shutdown()
            .await
            .context("shutdown self P2P")?;
        let self_transport = test_transport(
            &p2p_config(temp.path().join("consumer-store")),
            "self-node",
            Arc::new(StaticP2pPeerDiscovery::new(vec![P2pPeer {
                node_id: "self-node".to_string(),
                endpoint: self_endpoint,
            }])),
        )
        .await
        .context("start consumer P2P transport")?;

        let key = "test/p2p/iroh/self-peer".to_string();
        self_transport
            .publish(&P2pPublishRequest::bytes(
                key.clone(),
                b"local artifact bytes".as_slice(),
            ))
            .await
            .context("publish artifact")?;

        let descriptor = self_transport
            .lookup(&key)
            .await?
            .context("lookup local catalog")?;

        assert_eq!(descriptor.providers, vec![P2pArtifactProvider::Local]);
        self_transport
            .shutdown()
            .await
            .context("shutdown self P2P")?;
        Ok(())
    }
}
