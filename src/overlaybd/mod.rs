mod compact;
mod p2p;

pub(crate) use compact::{compact_layers, OverlaybdCompactOutput};
pub use p2p::OverlaybdP2pRuntime;
pub(crate) use p2p::{layer_key_from_digest, layer_key_from_uuid, LayerMetadata};
