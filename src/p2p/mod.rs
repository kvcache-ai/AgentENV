mod config;
mod discovery;
mod error;
mod iroh;
#[cfg(test)]
pub(crate) mod mock;
mod transport;
mod types;

use std::sync::Arc;

use crate::identity::NodeIdentity;

pub use config::P2pTransportKind;
pub use discovery::{
    NoopP2pPeerDiscovery, P2pPeerDiscovery, SchedulerPeerDiscovery, StaticP2pPeerDiscovery,
};
pub use error::{Error as P2pError, Result as P2pResult};
pub use transport::{DisabledP2pTransport, P2pByteStream, P2pTransport};
pub use types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProvider, P2pArtifactProviderHint,
    P2pEndpoint, P2pFetchOptions, P2pPeer, P2pPublishMode, P2pPublishRequest, P2pPublishSource,
};

/// Construct an artifact transport from the app config, returning an error if the configured transport is invalid or fails to initialize.
pub async fn transport_from_config(
    config: &crate::cfg::AppConfig,
    node_identity: &NodeIdentity,
) -> anyhow::Result<Arc<dyn P2pTransport>> {
    let p2p = config::ResolvedP2pConfig::from_config(&config.p2p);
    match p2p.transport {
        P2pTransportKind::Disabled => Ok(Arc::new(DisabledP2pTransport)),
        P2pTransportKind::Iroh => {
            let peer_discovery = peer_discovery_from_config(config, &p2p, node_identity);
            Ok(Arc::new(
                iroh::IrohBlobsP2pTransport::new(p2p, node_identity.id.clone(), peer_discovery)
                    .await?,
            ))
        }
    }
}

fn peer_discovery_from_config(
    config: &crate::cfg::AppConfig,
    p2p: &config::ResolvedP2pConfig,
    node_identity: &NodeIdentity,
) -> Arc<dyn P2pPeerDiscovery> {
    let Some(scheduler_endpoint) = config.cluster.scheduler_endpoint.clone() else {
        return Arc::new(NoopP2pPeerDiscovery);
    };

    SchedulerPeerDiscovery::start(
        scheduler_endpoint,
        node_identity.id.clone(),
        node_identity.cluster_id.to_string(),
        p2p.peer_discovery_refresh_interval,
        p2p.transport.backend_id().map(ToString::to_string),
    )
}
