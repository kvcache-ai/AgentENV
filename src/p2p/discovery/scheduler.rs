use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use rand::RngExt;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint as GrpcEndpoint};
use tonic::Request;
use tracing::{debug, info, trace, warn};

use crate::p2p::discovery::P2pPeerDiscovery;
use crate::p2p::error::Result;
use crate::p2p::types::{P2pArtifactKey, P2pEndpoint, P2pPeer};
use crate::proto::scheduler::{self, scheduler_client::SchedulerClient};

const REFRESH_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const ARTIFACT_RPC_TIMEOUT: Duration = Duration::from_secs(5);

pub struct SchedulerPeerDiscovery {
    peers: Arc<RwLock<Vec<P2pPeer>>>,
    refresh_task: JoinHandle<()>,
    client: SchedulerClient<Channel>,
    local_node_id: String,
    cluster_id: String,
    backend: Option<String>,
}

impl SchedulerPeerDiscovery {
    pub fn start(
        scheduler_endpoint: String,
        local_node_id: String,
        cluster_id: String,
        refresh_interval: Duration,
        backend: Option<String>,
    ) -> Arc<dyn P2pPeerDiscovery> {
        info!(
            scheduler_endpoint,
            local_node_id,
            cluster_id,
            backend = backend.as_deref().unwrap_or_default(),
            refresh_interval = ?refresh_interval,
            "starting scheduler P2P peer discovery"
        );
        let peers = Arc::new(RwLock::new(Vec::new()));
        let channel = match GrpcEndpoint::from_shared(scheduler_endpoint.clone()) {
            Ok(endpoint) => endpoint.connect_lazy(),
            Err(err) => {
                warn!(
                    scheduler_endpoint,
                    error = %err,
                    "P2P scheduler discovery disabled because scheduler endpoint is invalid"
                );
                return Arc::new(crate::p2p::discovery::NoopP2pPeerDiscovery);
            }
        };
        let client = SchedulerClient::new(channel);
        let refresh_task = tokio::spawn(refresh_scheduler_peers(
            client.clone(),
            local_node_id.clone(),
            cluster_id.clone(),
            refresh_interval,
            backend.clone(),
            Arc::clone(&peers),
        ));
        Arc::new(Self {
            peers: Arc::clone(&peers),
            refresh_task,
            client,
            local_node_id,
            cluster_id,
            backend,
        })
    }
}

impl Drop for SchedulerPeerDiscovery {
    fn drop(&mut self) {
        self.refresh_task.abort();
    }
}

#[async_trait]
impl P2pPeerDiscovery for SchedulerPeerDiscovery {
    async fn peers(&self) -> Result<Vec<P2pPeer>> {
        Ok(self
            .peers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone())
    }

    async fn peers_for_key(&self, key: &P2pArtifactKey) -> Result<Vec<P2pPeer>> {
        let Some(backend) = self.backend.as_deref() else {
            return Ok(Vec::new());
        };
        let mut client = self.client.clone();
        let mut request = Request::new(scheduler::LookupP2pArtifactRequest {
            cluster_id: self.cluster_id.clone(),
            backend: backend.to_string(),
            key: key.clone(),
            exclude_node_id: self.local_node_id.clone(),
        });
        request.set_timeout(ARTIFACT_RPC_TIMEOUT);

        let response = client
            .lookup_p2p_artifact(request)
            .await
            .context("lookup P2P artifact in scheduler")?
            .into_inner();
        Ok(filter_scheduler_p2p_peers(response.peers, backend))
    }

    async fn record_key(&self, key: &P2pArtifactKey) -> Result<()> {
        let Some(backend) = self.backend.as_deref() else {
            return Ok(());
        };
        let mut client = self.client.clone();
        let mut request = Request::new(scheduler::RecordP2pArtifactRequest {
            cluster_id: self.cluster_id.clone(),
            backend: backend.to_string(),
            key: key.clone(),
            node_id: self.local_node_id.clone(),
        });
        request.set_timeout(ARTIFACT_RPC_TIMEOUT);
        client
            .record_p2p_artifact(request)
            .await
            .context("record P2P artifact in scheduler")?;
        Ok(())
    }

    async fn forget_key(&self, key: &P2pArtifactKey) -> Result<()> {
        let Some(backend) = self.backend.as_deref() else {
            return Ok(());
        };
        let mut client = self.client.clone();
        let mut request = Request::new(scheduler::ForgetP2pArtifactRequest {
            cluster_id: self.cluster_id.clone(),
            backend: backend.to_string(),
            key: key.clone(),
            node_id: self.local_node_id.clone(),
        });
        request.set_timeout(ARTIFACT_RPC_TIMEOUT);
        client
            .forget_p2p_artifact(request)
            .await
            .context("forget P2P artifact in scheduler")?;
        Ok(())
    }
}

async fn refresh_scheduler_peers(
    mut client: SchedulerClient<Channel>,
    local_node_id: String,
    cluster_id: String,
    refresh_interval: Duration,
    backend: Option<String>,
    peers: Arc<RwLock<Vec<P2pPeer>>>,
) {
    // Spread initial polls across up to one full refresh interval to avoid a
    // thundering herd when many nodes start simultaneously.
    let jitter = rand::rng().random_range(Duration::ZERO..refresh_interval);
    sleep(jitter).await;

    loop {
        match list_scheduler_p2p_peers(
            &mut client,
            &local_node_id,
            &cluster_id,
            backend.as_deref().unwrap_or_default(),
        )
        .await
        {
            Ok(discovered) => {
                trace!(
                    peer_count = discovered.len(),
                    "refreshed P2P peers from scheduler"
                );
                *peers
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = discovered;
            }
            Err(err) => {
                debug!(error = %err, "failed to refresh P2P peers from scheduler");
            }
        }
        sleep(refresh_interval).await;
    }
}

async fn list_scheduler_p2p_peers(
    client: &mut SchedulerClient<Channel>,
    local_node_id: &str,
    cluster_id: &str,
    backend: &str,
) -> anyhow::Result<Vec<P2pPeer>> {
    let mut request = Request::new(scheduler::ListP2pPeersRequest {
        cluster_id: cluster_id.to_string(),
        backend: backend.to_string(),
        exclude_node_id: local_node_id.to_string(),
    });
    request.set_timeout(REFRESH_RPC_TIMEOUT);

    let response = client
        .list_p2p_peers(request)
        .await
        .context("list P2P peers from scheduler")?
        .into_inner();
    Ok(filter_scheduler_p2p_peers(response.peers, backend))
}

fn filter_scheduler_p2p_peers(peers: Vec<scheduler::P2pPeer>, backend: &str) -> Vec<P2pPeer> {
    peers
        .into_iter()
        .filter_map(|peer| {
            let endpoint = peer.endpoint?;
            if endpoint.backend != backend || endpoint.address.is_empty() {
                return None;
            }
            Some(P2pPeer {
                node_id: peer.node_id,
                endpoint: P2pEndpoint {
                    backend: endpoint.backend,
                    address: endpoint.address,
                },
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduler_peer(node_id: &str, backend: &str, address: &str) -> scheduler::P2pPeer {
        scheduler::P2pPeer {
            node_id: node_id.to_string(),
            endpoint: Some(scheduler::P2pEndpoint {
                backend: backend.to_string(),
                address: address.to_string(),
            }),
        }
    }

    #[test]
    fn filter_scheduler_p2p_peers_keeps_only_matching_ready_peers() {
        let peers = filter_scheduler_p2p_peers(
            vec![
                scheduler_peer("node-a", "backend", "addr-a"),
                scheduler_peer("node-b", "other", "addr-b"),
                scheduler_peer("node-c", "backend", ""),
                scheduler::P2pPeer {
                    node_id: "node-d".to_string(),
                    endpoint: None,
                },
                scheduler_peer("node-e", "backend", "addr-e"),
            ],
            "backend",
        );

        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id, "node-a");
        assert_eq!(peers[0].endpoint.backend, "backend");
        assert_eq!(peers[0].endpoint.address, "addr-a");
        assert_eq!(peers[1].node_id, "node-e");
        assert_eq!(peers[1].endpoint.backend, "backend");
        assert_eq!(peers[1].endpoint.address, "addr-e");
    }
}
