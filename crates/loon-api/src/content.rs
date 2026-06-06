use crate::digest::sha256_digest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Kind of content reference.
pub enum ContentRefKind {
    /// Whole-file v0 content addressed by SHA-256.
    WholeFileV0,
    /// Placeholder for newer content kinds unknown to this client.
    #[serde(other)]
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Pointer to immutable file content.
///
/// A `ContentRef` is safe to publish only after the referenced bytes are
/// durable in the namespace's content store.
pub struct ContentRef {
    /// Content strategy used by the referenced object.
    pub kind: ContentRefKind,
    /// Digest string, currently `sha256:<64 lowercase hex>`.
    pub digest: String,
    /// Complete byte length of the referenced file content.
    pub size_bytes: u64,
}

impl ContentRef {
    /// Builds a whole-file v0 reference for these bytes.
    pub fn whole_file_v0(bytes: &[u8]) -> Self {
        Self {
            kind: ContentRefKind::WholeFileV0,
            digest: sha256_digest(bytes),
            size_bytes: bytes.len() as u64,
        }
    }
}
