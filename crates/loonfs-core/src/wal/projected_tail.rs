//! Metadata rows and unfolded content pieces from the WAL tail.

use super::frame::WalObjectError;
use crate::heap_bytes::{arc_bytes, hash_map_table_bytes, HeapBytes};
use crate::metadata::MetadataState;
use bytes::Bytes;
use loonfs_types::format::manifest::ManifestActivity;
use loonfs_types::format::wal::{
    committed_activity, ContentBase, WalCommitPayload, WalDelta, WalInlineContent,
};
use loonfs_types::{Checksum, ContentId, ContentRef, NamespaceId};
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
    /// references, checksums, bases, and piece slots.
    inline_entry_heap_bytes: usize,
}

/// One content id's unfolded pieces, ordered by offset, with the longest
/// reference the tail holds to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectedContent {
    pub(crate) content_ref: ContentRef,
    /// The CRC-64/NVME the delta of `content_ref` recorded.
    pub(crate) crc64nvme: Option<Checksum>,
    pub(crate) pieces: Vec<ProjectedPiece>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectedPiece {
    pub(crate) offset: u64,
    pub(crate) bytes: Bytes,
    pub(crate) base: Option<ContentBase>,
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
        for entry in &record.inline_content {
            let (content_ref, crc64nvme) = record
                .deltas
                .iter()
                .filter_map(|delta| match &delta.delta {
                    WalDelta::AppendFileRevision {
                        content_ref,
                        crc64nvme,
                        ..
                    } if content_ref.content_id == entry.content_id
                        && &content_ref.owner_namespace_id == namespace_id =>
                    {
                        Some((content_ref, crc64nvme))
                    }
                    _ => None,
                })
                .max_by_key(|(content_ref, _)| content_ref.size_bytes)
                .expect("decoded inline content should have a same-commit reference");
            self.insert_piece(content_ref, crc64nvme, entry);
        }
        self.rows.apply_committed_wal_record_mut(record);
        self.activity = activity;
        Ok(())
    }

    /// The unfolded pieces of the content `content_ref` names, when the
    /// tail holds any.
    pub(crate) fn content(&self, content_ref: &ContentRef) -> Option<&ProjectedContent> {
        self.content_by_id(&content_ref.owner_namespace_id, &content_ref.content_id)
    }

    pub(crate) fn content_by_id(
        &self,
        owner_namespace_id: &NamespaceId,
        content_id: &ContentId,
    ) -> Option<&ProjectedContent> {
        self.content_positions
            .get(content_id)
            .map(|position| &self.contents[*position])
            .filter(|content| &content.content_ref.owner_namespace_id == owner_namespace_id)
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

    fn insert_piece(
        &mut self,
        content_ref: &ContentRef,
        crc64nvme: &Option<Checksum>,
        entry: &WalInlineContent,
    ) {
        let position = match self.content_positions.entry(content_ref.content_id.clone()) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(vacant) => {
                self.inline_entry_heap_bytes += content_ref.content_id.heap_bytes();
                vacant.insert(self.contents.len());
                self.contents.push(ProjectedContent {
                    content_ref: content_ref.clone(),
                    crc64nvme: crc64nvme.clone(),
                    pieces: Vec::new(),
                });
                self.inline_entry_heap_bytes += content_ref.heap_bytes() + crc64nvme.heap_bytes();
                self.contents.len() - 1
            }
        };
        let content = &mut self.contents[position];
        if content_ref.size_bytes > content.content_ref.size_bytes {
            self.inline_entry_heap_bytes -=
                content.content_ref.heap_bytes() + content.crc64nvme.heap_bytes();
            content.content_ref = content_ref.clone();
            content.crc64nvme = crc64nvme.clone();
            self.inline_entry_heap_bytes += content_ref.heap_bytes() + crc64nvme.heap_bytes();
        }
        let piece = ProjectedPiece {
            offset: entry.offset,
            bytes: Bytes::copy_from_slice(&entry.bytes),
            base: entry.base.clone(),
        };
        self.inline_bytes += piece.bytes.len();
        self.inline_entry_heap_bytes += piece.base.heap_bytes();
        let slots = content.pieces.capacity();
        match content
            .pieces
            .binary_search_by_key(&piece.offset, |piece| piece.offset)
        {
            Ok(index) => {
                let previous = std::mem::replace(&mut content.pieces[index], piece);
                self.inline_bytes -= previous.bytes.len();
                self.inline_entry_heap_bytes -= previous.base.heap_bytes();
            }
            Err(index) => content.pieces.insert(index, piece),
        }
        self.inline_entry_heap_bytes +=
            (content.pieces.capacity() - slots) * size_of::<ProjectedPiece>();
    }
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
                    ),
                    hash_state: None,
                    crc64nvme: None,
                },
            }],
            inline_content: vec![WalInlineContent {
                content_id: content_id.clone(),
                offset: offset as u64,
                bytes: whole[offset..].to_vec(),
                base: None,
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
