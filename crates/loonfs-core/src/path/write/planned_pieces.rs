//! Pieces of commits that are planned or accepted but not yet in the WAL tail.

use super::session::PublishPlanningSession;
use crate::wal::{ProjectedContent, ProjectedPiece};
use bytes::Bytes;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::WalInlineContent;
use loonfs_types::{ChangeSeq, ContentId, ContentRef};
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct PlannedPieces {
    contents: HashMap<ContentId, ProjectedContent>,
}

impl PlannedPieces {
    pub(super) fn content(&self, content_ref: &ContentRef) -> Option<&ProjectedContent> {
        self.contents
            .get(&content_ref.content_id)
            .filter(|content| {
                content.content_ref.owner_namespace_id == content_ref.owner_namespace_id
            })
    }

    /// A content this layer has not seen starts from `below`, the newest
    /// pieces the layers under it keep for it.
    pub(super) fn insert_piece(
        &mut self,
        below: Option<&ProjectedContent>,
        content_ref: &ContentRef,
        entry: &WalInlineContent,
        committed_seq: ChangeSeq,
    ) {
        let piece = ProjectedPiece {
            offset: entry.offset,
            bytes: Bytes::copy_from_slice(&entry.bytes),
        };
        self.contents
            .entry(content_ref.content_id.clone())
            .or_insert_with(|| {
                below
                    .cloned()
                    .unwrap_or_else(|| ProjectedContent::new(content_ref, committed_seq))
            })
            .insert_piece(content_ref, piece, committed_seq);
    }
}

/// What one commit's content planning reads: its own pieces over the
/// batch's accepted commits over the tail.
pub(super) struct CommitPieces<'a, S: ObjectStore + ?Sized> {
    session: &'a PublishPlanningSession<'a, S>,
    own: PlannedPieces,
}

impl<'a, S: ObjectStore + ?Sized> CommitPieces<'a, S> {
    pub(super) fn new(session: &'a PublishPlanningSession<'a, S>) -> Self {
        Self {
            session,
            own: PlannedPieces::default(),
        }
    }

    pub(super) fn content(&self, content_ref: &ContentRef) -> Option<ProjectedContent> {
        self.own
            .content(content_ref)
            .cloned()
            .or_else(|| self.session.content(content_ref))
    }

    pub(super) fn insert_piece(
        &mut self,
        content_ref: &ContentRef,
        entry: &WalInlineContent,
        committed_seq: ChangeSeq,
    ) {
        self.own.insert_piece(
            self.session.content(content_ref).as_ref(),
            content_ref,
            entry,
            committed_seq,
        );
    }
}
