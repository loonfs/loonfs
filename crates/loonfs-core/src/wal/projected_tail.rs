//! Metadata rows and inline content from the unfolded WAL tail.

use super::frame::WalObjectError;
use crate::heap_bytes::{arc_bytes, hash_map_table_bytes, HeapBytes};
use crate::metadata::MetadataState;
use bytes::Bytes;
use loonfs_types::format::manifest::ManifestActivity;
use loonfs_types::format::wal::{committed_activity, WalCommitPayload};
use loonfs_types::{ContentId, ContentRef};
use std::collections::HashMap;

/// Holds the WAL tail after a manifest as rows and the inline content they name.
/// Replay already downloaded the resident bytes, so reads need no content request.
/// The head-state budgets bound those bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectedWalTail {
    pub(crate) rows: MetadataState,
    pub(crate) activity: ManifestActivity,
    inline_content: HashMap<ContentId, ProjectedInlineContent>,
    inline_bytes: usize,
    /// What the inline entries own besides their bytes: content ids and
    /// references.
    inline_entry_heap_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectedInlineContent {
    pub(crate) content_ref: ContentRef,
    pub(crate) bytes: Bytes,
}

impl ProjectedWalTail {
    pub(crate) fn from_rows(rows: MetadataState) -> Self {
        Self {
            rows,
            ..Self::default()
        }
    }

    pub(crate) fn apply_commit(&mut self, record: &WalCommitPayload) -> Result<(), WalObjectError> {
        let activity = committed_activity(&record.deltas)
            .and_then(|activity| self.activity.checked_add(activity))
            .ok_or(WalObjectError::ActivityOverflow)?;
        self.rows.apply_committed_wal_record_mut(record);
        self.activity = activity;
        Ok(())
    }

    pub(crate) fn inline_content(&self, content_ref: &ContentRef) -> Option<&Bytes> {
        self.inline_content
            .get(&content_ref.content_id)
            .filter(|value| &value.content_ref == content_ref)
            .map(|value| &value.bytes)
    }

    pub(crate) fn inline_values(&self) -> impl Iterator<Item = &ProjectedInlineContent> {
        self.inline_content.values()
    }

    pub(crate) fn inline_bytes(&self) -> usize {
        self.inline_bytes
    }

    /// The heap the projection holds behind the `Arc` every holder shares.
    pub fn decoded_bytes(&self) -> usize {
        arc_bytes::<Self>()
            + self.rows.decoded_bytes()
            + hash_map_table_bytes(&self.inline_content)
            + self.inline_entry_heap_bytes
            + self.inline_bytes()
    }

    pub(crate) fn insert_inline_content(&mut self, content_ref: ContentRef, bytes: Bytes) {
        self.inline_bytes += bytes.len();
        let content_id = content_ref.content_id.clone();
        let key_heap_bytes = content_id.heap_bytes();
        self.inline_entry_heap_bytes += key_heap_bytes + content_ref.heap_bytes();
        let value = ProjectedInlineContent { content_ref, bytes };
        if let Some(previous) = self.inline_content.insert(content_id, value) {
            self.inline_bytes -= previous.bytes.len();
            self.inline_entry_heap_bytes -= key_heap_bytes + previous.content_ref.heap_bytes();
        }
    }
}
