#[cfg(feature = "p2p-iroh")]
use std::path::PathBuf;
#[cfg(feature = "p2p-iroh")]
use std::time::Duration;

use serde::Deserialize;

use crate::cfg::P2pConfig;

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum P2pTransportKind {
    Disabled,
    Iroh,
}

#[cfg(feature = "p2p-iroh")]
impl P2pTransportKind {
    pub(crate) fn backend_id(self) -> Option<&'static str> {
        match self {
            Self::Disabled => None,
            Self::Iroh => Some(super::iroh::IROH_BACKEND_ID),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedP2pConfig {
    pub transport: P2pTransportKind,
    #[cfg(feature = "p2p-iroh")]
    pub store_dir: PathBuf,
    #[cfg(feature = "p2p-iroh")]
    pub listen_addr: Option<String>,
    #[cfg(feature = "p2p-iroh")]
    pub lookup_timeout: Duration,
    #[cfg(feature = "p2p-iroh")]
    pub fetch_timeout: Duration,
    #[cfg(feature = "p2p-iroh")]
    pub peer_discovery_refresh_interval: Duration,
}

impl ResolvedP2pConfig {
    pub(crate) fn from_config(p2p: &P2pConfig) -> Self {
        let transport = if p2p.enabled {
            p2p.transport
        } else {
            P2pTransportKind::Disabled
        };

        Self {
            transport,
            #[cfg(feature = "p2p-iroh")]
            store_dir: p2p.store_dir.clone(),
            #[cfg(feature = "p2p-iroh")]
            listen_addr: Some(str::trim(p2p.listen_addr.as_str()))
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
            #[cfg(feature = "p2p-iroh")]
            lookup_timeout: Duration::from_millis(p2p.lookup_timeout_ms),
            #[cfg(feature = "p2p-iroh")]
            fetch_timeout: Duration::from_millis(p2p.fetch_timeout_ms),
            #[cfg(feature = "p2p-iroh")]
            peer_discovery_refresh_interval: Duration::from_secs(
                p2p.peer_discovery_refresh_interval_secs,
            )
            .max(Duration::from_secs(1)),
        }
    }
}
