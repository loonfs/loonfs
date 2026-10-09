//! Content bytes carried by a commit with their ordinary blob reference.

use bytes::Bytes;
use loonfs_types::{Checksum, ContentId, ContentRef, NamespaceId, Sha256State};

/// Keeps bytes with a reference built from them, so the two agree by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineContent {
    content_ref: ContentRef,
    bytes: Bytes,
    hash_state: Sha256State,
    crc64nvme: Checksum,
}

impl InlineContent {
    /// The caller draws a fresh `ContentId` for every value and never reuses it.
    /// A content ID belongs to exactly one staged upload or one committed inline value.
    pub fn new(owner_namespace_id: NamespaceId, content_id: ContentId, bytes: Bytes) -> Self {
        let mut hash_state = Sha256State::new();
        hash_state.update(&bytes);
        Self {
            content_ref: ContentRef::blob_v1_streamed(owner_namespace_id, content_id, &hash_state),
            crc64nvme: Checksum::crc64nvme(&bytes),
            hash_state,
            bytes,
        }
    }

    pub fn content_ref(&self) -> &ContentRef {
        &self.content_ref
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn hash_state(&self) -> &Sha256State {
        &self.hash_state
    }

    pub fn crc64nvme(&self) -> &Checksum {
        &self.crc64nvme
    }
}
