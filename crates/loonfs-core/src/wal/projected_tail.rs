//! Metadata rows and unfolded content pieces from the WAL tail.

use super::frame::WalObjectError;
use crate::heap_bytes::{arc_bytes, hash_map_table_bytes, HeapBytes};
use crate::metadata::MetadataState;
use bytes::Bytes;
use loonfs_types::format::manifest::ManifestActivity;
use loonfs_types::format::wal::{committed_activity, WalCommitPayload, WalDelta, WalInlineContent};
use loonfs_types::{ChangeSeq, ContentId, ContentRef, NamespaceId};
use std::collections::{hash_map::Entry, HashMap};
use std::mem::size_of;

/// Holds the WAL tail after a manifest as rows and the content pieces they
/// name. Replay already downloaded the resident bytes, so reads need no
/// content request for them. The head-state budgets bound those bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectedWalTail {
    pub(crate) rows: MetadataState,
    pub(crate) activity: ManifestActivity,
    contents: Vec<ProjectedContent>,
    content_positions: HashMap<ContentId, usize>,
    inline_bytes: usize,
    /// What the content entries own besides their bytes: content ids,
    /// references, checksums, and piece slots.
    inline_entry_heap_bytes: usize,
}

/// One content id's unfolded pieces, ordered by offset, with the longest
/// reference the tail holds to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectedContent {
    pub(crate) content_ref: ContentRef,
    pub(crate) committed_seq: ChangeSeq,
    pub(crate) pieces: Vec<ProjectedPiece>,
}

impl ProjectedContent {
    pub(crate) fn new(content_ref: &ContentRef, committed_seq: ChangeSeq) -> Self {
        Self {
            content_ref: content_ref.clone(),
            committed_seq,
            pieces: Vec::new(),
        }
    }

    pub(crate) fn insert_piece(
        &mut self,
        content_ref: &ContentRef,
        entry: &WalInlineContent,
        committed_seq: ChangeSeq,
    ) -> Option<ProjectedPiece> {
        if content_ref.size_bytes > self.content_ref.size_bytes {
            self.content_ref = content_ref.clone();
            self.committed_seq = committed_seq;
        }
        let piece = ProjectedPiece {
            offset: entry.offset,
            bytes: Bytes::copy_from_slice(&entry.bytes),
        };
        match self
            .pieces
            .binary_search_by_key(&piece.offset, |piece| piece.offset)
        {
            Ok(index) => Some(std::mem::replace(&mut self.pieces[index], piece)),
            Err(index) => {
                self.pieces.insert(index, piece);
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectedPiece {
    pub(crate) offset: u64,
    pub(crate) bytes: Bytes,
}

impl ProjectedPiece {
    pub(crate) fn end(&self) -> u64 {
        self.offset + self.bytes.len() as u64
    }
}

impl ProjectedWalTail {
    pub(crate) fn from_rows(rows: MetadataState) -> Self {
        Self {
            rows,
            ..Self::default()
        }
    }

    /// Applies one commit of `namespace_id`'s WAL: its pieces, then its rows.
    pub(crate) fn apply_commit(
        &mut self,
        namespace_id: &NamespaceId,
        record: &WalCommitPayload,
    ) -> Result<(), WalObjectError> {
        let activity = committed_activity(record)
            .and_then(|activity| self.activity.checked_add(activity))
            .ok_or(WalObjectError::ActivityOverflow)?;
        self.apply_pieces(namespace_id, record);
        self.rows.apply_committed_wal_record_mut(record);
        self.activity = activity;
        Ok(())
    }

    fn apply_pieces(&mut self, namespace_id: &NamespaceId, record: &WalCommitPayload) {
        for (content_ref, entry) in inline_content_refs(namespace_id, record) {
            self.insert_piece(content_ref, entry, record.committed_seq);
        }
    }

    /// The unfolded pieces of the content `content_ref` names, when the
    /// tail holds any.
    pub(crate) fn content(&self, content_ref: &ContentRef) -> Option<&ProjectedContent> {
        self.content_positions
            .get(&content_ref.content_id)
            .map(|position| &self.contents[*position])
            .filter(|content| {
                content.content_ref.owner_namespace_id == content_ref.owner_namespace_id
            })
    }

    /// Content ids with pieces, in the order the tail first named them.
    pub(crate) fn contents(&self) -> &[ProjectedContent] {
        &self.contents
    }

    pub(crate) fn inline_bytes(&self) -> usize {
        self.inline_bytes
    }

    /// The heap the projection holds behind the `Arc` every holder shares.
    pub fn decoded_bytes(&self) -> usize {
        arc_bytes::<Self>()
            + self.rows.decoded_bytes()
            + hash_map_table_bytes(&self.content_positions)
            + self.contents.capacity() * size_of::<ProjectedContent>()
            + self.inline_entry_heap_bytes
            + self.inline_bytes()
    }

    pub(crate) fn insert_piece(
        &mut self,
        content_ref: &ContentRef,
        entry: &WalInlineContent,
        committed_seq: ChangeSeq,
    ) {
        let position = match self.content_positions.entry(content_ref.content_id.clone()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(vacant) => {
                self.inline_entry_heap_bytes +=
                    content_ref.content_id.heap_bytes() + content_ref.heap_bytes();
                vacant.insert(self.contents.len());
                self.contents
                    .push(ProjectedContent::new(content_ref, committed_seq));
                self.contents.len() - 1
            }
        };
        let content = &mut self.contents[position];
        let reference_bytes = content.content_ref.heap_bytes();
        let slots = content.pieces.capacity();
        let replaced = content.insert_piece(content_ref, entry, committed_seq);
        self.inline_entry_heap_bytes = self.inline_entry_heap_bytes
            + content.content_ref.heap_bytes()
            + (content.pieces.capacity() - slots) * size_of::<ProjectedPiece>()
            - reference_bytes;
        self.inline_bytes += entry.bytes.len();
        if let Some(replaced) = replaced {
            self.inline_bytes -= replaced.bytes.len();
        }
    }
}

/// Pairs each inline entry of `record` with the largest same-commit
/// reference `namespace_id` owns to its content.
pub(crate) fn inline_content_refs<'r>(
    namespace_id: &'r NamespaceId,
    record: &'r WalCommitPayload,
) -> impl Iterator<Item = (&'r ContentRef, &'r WalInlineContent)> + 'r {
    record.inline_content.iter().map(move |entry| {
        let content_ref = record
            .deltas
            .iter()
            .filter_map(|delta| match &delta.delta {
                WalDelta::AppendFileRevision { content_ref, .. }
                    if content_ref.content_id == entry.content_id
                        && &content_ref.owner_namespace_id == namespace_id =>
                {
                    Some(content_ref)
                }
                _ => None,
            })
            .max_by_key(|content_ref| content_ref.size_bytes)
            .expect("decoded inline content should have a same-commit reference");
        (content_ref, entry)
    })
}

#[cfg(test)]
mod tests {
    use super::ProjectedWalTail;
    use loonfs_types::format::wal::{WalCommitDelta, WalCommitPayload, WalDelta, WalInlineContent};
    use loonfs_types::{
        ChangeSeq, CommitId, ContentId, ContentRef, InodeId, NamespaceId, RevisionNo,
    };

    fn commit(
        namespace_id: &NamespaceId,
        seq: u64,
        content_id: &ContentId,
        whole: &[u8],
        offset: usize,
    ) -> WalCommitPayload {
        WalCommitPayload {
            committed_seq: ChangeSeq(seq),
            commit_id: CommitId::parse(format!("c_{seq:032x}")).expect("commit id"),
            committed_by: loonfs_test_support::test_actor(),
            semantic_commit_fingerprint: serde_json::from_str(r#""v1:sha256:test""#)
                .expect("fingerprint"),
            committed_at_ms: 0,
            message: None,
            deltas: vec![WalCommitDelta {
                semantic_operation_index: 0,
                delta: WalDelta::AppendFileRevision {
                    delta_index: 0,
                    inode_id: InodeId(2),
                    revision_no: RevisionNo(seq),
                    content_ref: ContentRef::blob_v1(
                        namespace_id.clone(),
                        content_id.clone(),
                        whole,
                        loonfs_types::ChecksumAlgorithm::Crc64nvme,
                    ),
                    layout: None,
                },
            }],
            inline_content: vec![WalInlineContent {
                content_id: content_id.clone(),
                offset: offset as u64,
                bytes: whole[offset..].to_vec(),
            }],
        }
    }

    #[test]
    fn contents_keep_first_commit_order_and_count_piece_bytes() {
        let namespace_id = NamespaceId::parse("pieces").expect("namespace id");
        let ids: Vec<_> = (0..64)
            .rev()
            .map(|number| ContentId::parse(format!("con_{number:032x}")).expect("content id"))
            .collect();
        let mut tail = ProjectedWalTail::default();
        for (seq, content_id) in (1..).zip(&ids) {
            tail.apply_commit(
                &namespace_id,
                &commit(&namespace_id, seq, content_id, b"value", 0),
            )
            .expect("value");
        }
        tail.apply_commit(
            &namespace_id,
            &commit(&namespace_id, 65, &ids[17], b"value appended", 5),
        )
        .expect("append");

        assert_eq!(
            tail.contents()
                .iter()
                .map(|content| &content.content_ref.content_id)
                .collect::<Vec<_>>(),
            ids.iter().collect::<Vec<_>>()
        );
        let appended = &tail.contents()[17];
        assert_eq!(appended.content_ref.size_bytes, 14);
        assert_eq!(
            appended
                .pieces
                .iter()
                .map(|piece| (piece.offset, piece.end()))
                .collect::<Vec<_>>(),
            [(0, 5), (5, 14)]
        );
        assert_eq!(tail.inline_bytes(), 64 * 5 + 9);
    }
}
