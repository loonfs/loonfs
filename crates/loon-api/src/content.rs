use crate::digest::sha256_digest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentRefKind {
    WholeFileV0,
    #[serde(other)]
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRef {
    pub kind: ContentRefKind,
    pub digest: String,
    pub size_bytes: u64,
}

impl ContentRef {
    pub fn whole_file_v0(bytes: &[u8]) -> Self {
        Self {
            kind: ContentRefKind::WholeFileV0,
            digest: sha256_digest(bytes),
            size_bytes: bytes.len() as u64,
        }
    }
}
