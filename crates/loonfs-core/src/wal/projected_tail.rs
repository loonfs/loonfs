//! Metadata rows and inline content from the unfolded WAL tail.

use super::frame::WalSegmentError;
use crate::metadata::{CommitReceiptRecord, MetadataState};
use bytes::Bytes;
use loonfs_api::wire::manifest::ManifestActivity;
use loonfs_api::wire::wal::{committed_activity, WalCommitDelta, WalCommitPayload};
use loonfs_api::{ContentId, ContentRef};
use std::collections::HashMap;

/// Holds the WAL tail after a manifest as rows and the inline content they name.
/// Replay already downloaded the resident bytes, so reads need no content request.
/// The existing projection budgets bound those bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectedWalTail {
    pub(crate) rows: MetadataState,
    pub(crate) activity: ManifestActivity,
    inline_content: HashMap<ContentId, ProjectedInlineContent>,
    inline_bytes: usize,
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

    pub(crate) fn apply_commit(
        &mut self,
        record: &WalCommitPayload,
    ) -> Result<(), WalSegmentError> {
        self.apply_commit_parts(
            CommitReceiptRecord {
                commit_id: record.commit_id.clone(),
                committed_by: record.committed_by.clone(),
                semantic_commit_fingerprint: record.semantic_commit_fingerprint.clone(),
                committed_seq: record.seq,
                committed_at_ms: record.committed_at_ms,
                message: record.message.clone(),
            },
            &record.deltas,
        )
    }

    pub(crate) fn apply_commit_parts(
        &mut self,
        receipt: CommitReceiptRecord,
        deltas: &[WalCommitDelta],
    ) -> Result<(), WalSegmentError> {
        let activity = committed_activity(deltas)
            .and_then(|activity| self.activity.checked_add(activity))
            .ok_or(WalSegmentError::ActivityOverflow)?;
        self.rows
            .apply_committed_wal_record_parts_mut(receipt, deltas);
        self.activity = activity;
        Ok(())
    }

    pub(crate) fn inline_content(&self, content_id: &ContentId) -> Option<&Bytes> {
        self.inline_content
            .get(content_id)
            .map(|value| &value.bytes)
    }

    pub(crate) fn inline_values(&self) -> impl Iterator<Item = &ProjectedInlineContent> {
        self.inline_content.values()
    }

    pub(crate) fn inline_bytes(&self) -> usize {
        self.inline_bytes
    }

    pub(crate) fn decoded_bytes(&self) -> usize {
        self.rows
            .decoded_bytes()
            .saturating_add(self.inline_bytes())
    }

    pub(crate) fn insert_inline_content(&mut self, content_ref: ContentRef, bytes: Bytes) {
        self.inline_bytes += bytes.len();
        let content_id = content_ref.content_id.clone();
        let value = ProjectedInlineContent { content_ref, bytes };
        if let Some(previous) = self.inline_content.insert(content_id, value) {
            self.inline_bytes -= previous.bytes.len();
        }
    }
}
