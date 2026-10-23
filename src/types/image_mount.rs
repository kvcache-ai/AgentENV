use serde::{Deserialize, Serialize};

/// An image mounted at a guest path through a sandbox-scoped exclusive volume,
/// which the sandbox owns and deletes with itself.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageMount {
    /// The image reference as requested.
    pub image: String,
    pub read_only: bool,
}
