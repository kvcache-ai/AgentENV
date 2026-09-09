use std::fmt;
use std::path::PathBuf;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Stable lookup identity for an artifact in the project-wide P2P catalog.
///
/// Producers encode the full cache context into this string and validate the
/// returned descriptor metadata before trusting fetched bytes.
pub type P2pArtifactKey = String;

/// Catalog entry returned by lookup and used by fetch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct P2pArtifactDescriptor {
    /// Stable artifact lookup key.
    pub key: P2pArtifactKey,
    /// Providers that can serve this artifact.
    pub providers: Vec<P2pArtifactProvider>,
    /// Transport-specific locator used by the matching backend to fetch bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_locator: Option<String>,
    /// Module-defined metadata used to validate and interpret the artifact.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// Provider entry in a P2P artifact descriptor.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum P2pArtifactProvider {
    /// Placeholder for the node that owns the local catalog entry.
    Local,
    /// Concrete provider safe to send to other nodes.
    Peer(P2pPeer),
}

impl P2pArtifactProvider {
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local)
    }
}

impl From<P2pPeer> for P2pArtifactProvider {
    fn from(peer: P2pPeer) -> Self {
        Self::Peer(peer)
    }
}

/// Local artifact source to publish into the P2P catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum P2pPublishSource {
    /// Path to local artifact bytes.
    Path(PathBuf),
    /// In-memory artifact bytes.
    Bytes(Bytes),
}

impl fmt::Display for P2pPublishSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => write!(f, "{}", path.display()),
            Self::Bytes(bytes) => write!(f, "<{} bytes>", bytes.len()),
        }
    }
}

/// Local artifact plus descriptor to publish into the P2P catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct P2pPublishRequest {
    /// Stable artifact lookup key.
    pub key: P2pArtifactKey,
    /// Artifact source.
    pub source: P2pPublishSource,
    /// Module-defined metadata advertised with the descriptor.
    pub metadata: serde_json::Value,
    /// Hint for how the transport should retain/import a path-backed local artifact.
    pub publish_mode: P2pPublishMode,
}

impl P2pPublishRequest {
    pub fn file(key: impl Into<P2pArtifactKey>, source: impl Into<PathBuf>) -> Self {
        Self {
            key: key.into(),
            source: P2pPublishSource::Path(source.into()),
            metadata: serde_json::Value::Null,
            publish_mode: P2pPublishMode::default(),
        }
    }

    pub fn bytes(key: impl Into<P2pArtifactKey>, bytes: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            source: P2pPublishSource::Bytes(bytes.into()),
            metadata: serde_json::Value::Null,
            publish_mode: P2pPublishMode::Copy,
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_publish_mode(mut self, publish_mode: P2pPublishMode) -> Self {
        self.publish_mode = publish_mode;
        self
    }
}

/// Import/retention hint for publish.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum P2pPublishMode {
    /// Transport may copy bytes into its own store.
    #[default]
    Copy,
    /// Transport may retain or index the existing local file when supported.
    Reference,
}

/// Serializable address for a node's artifact transport endpoint.
///
/// The scheduler stores this opaquely and returns it to other nodes; only the
/// matching transport backend is expected to parse `address`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct P2pEndpoint {
    /// Backend that understands `address`, currently `iroh`.
    pub backend: String,
    /// Backend-specific serialized endpoint address.
    pub address: String,
}

/// Candidate peer returned by discovery and used by transport backends.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct P2pPeer {
    /// AgentENV node ID from scheduler/observability.
    pub node_id: String,
    /// Artifact transport endpoint advertised by that node.
    pub endpoint: P2pEndpoint,
}

/// Optional provider information supplied by a module that already has
/// artifact locality context outside of scheduler discovery.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct P2pArtifactProviderHint {
    /// AgentENV node ID if the producer is known.
    pub node_id: Option<String>,
    /// Transport endpoint if the producer endpoint is known.
    pub endpoint: Option<P2pEndpoint>,
}

/// Options to fetch an artifact from a transport backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct P2pFetchOptions {
    /// The transport backend should automatically advertise the fetched artifact.
    pub advertise: bool,
}

impl Default for P2pFetchOptions {
    fn default() -> Self {
        Self { advertise: true }
    }
}
