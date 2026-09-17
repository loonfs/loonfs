//! Metadata rows and inline content from the unfolded WAL tail.

use crate::metadata::MetadataState;
use bytes::Bytes;
use loonfs_api::wire::wal::WalInlineContent;
use loonfs_api::ContentId;
use std::collections::HashMap;

/// Holds the WAL tail after a manifest as rows and the inline content they name.
/// Replay already downloaded the resident bytes, so reads need no content request.
/// The existing projection budgets bound those bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectedWalTail {
    pub(crate) rows: MetadataState,
    inline_content: HashMap<ContentId, Bytes>,
    inline_bytes: usize,
}

impl ProjectedWalTail {
    pub(crate) fn from_rows(rows: MetadataState) -> Self {
        Self {
            rows,
            ..Self::default()
        }
    }

    pub(crate) fn inline_content(&self, content_id: &ContentId) -> Option<&Bytes> {
        self.inline_content.get(content_id)
    }

    pub(crate) fn has_inline_content(&self) -> bool {
        !self.inline_content.is_empty()
    }

    pub(crate) fn inline_bytes(&self) -> usize {
        self.inline_bytes
    }

    pub(crate) fn decoded_bytes(&self) -> usize {
        self.rows
            .decoded_bytes()
            .saturating_add(self.inline_bytes())
    }

    pub(crate) fn extend_inline_content(&mut self, values: &[WalInlineContent]) {
        for value in values {
            let bytes = Bytes::copy_from_slice(&value.bytes);
            self.inline_bytes += bytes.len();
            if let Some(previous) = self.inline_content.insert(value.content_id.clone(), bytes) {
                self.inline_bytes -= previous.len();
            }
        }
    }
}
