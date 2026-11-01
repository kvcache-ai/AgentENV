mod compact;
mod p2p;

pub(crate) use compact::{
    compact_layers, write_dense_overlaybd_layer_to_file, OverlaybdCompactOutput,
};
pub use p2p::OverlaybdP2pRuntime;
pub(crate) use p2p::{layer_key_from_digest, layer_key_from_uuid, LayerMetadata};
