//! Content bytes carried by a commit with their ordinary blob reference.

use bytes::Bytes;
use loonfs_api::{ContentId, ContentRef, NamespaceGeneration, NamespaceId};

/// Keeps bytes with a reference built from them, so the two agree by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineContent {
    content_ref: ContentRef,
    bytes: Bytes,
}

impl InlineContent {
    /// The caller draws a fresh `ContentId` for every value and never reuses it.
    /// A content ID belongs to exactly one staged upload or one committed inline value.
    pub fn new(
        owner_namespace_id: NamespaceId,
        owner_generation: NamespaceGeneration,
        content_id: ContentId,
        bytes: Bytes,
    ) -> Self {
        Self {
            content_ref: ContentRef::blob_v1(
                owner_namespace_id,
                owner_generation,
                content_id,
                &bytes,
            ),
            bytes,
        }
    }

    pub fn content_ref(&self) -> &ContentRef {
        &self.content_ref
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}
