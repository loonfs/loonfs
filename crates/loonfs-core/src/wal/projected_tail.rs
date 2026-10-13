//! Metadata rows and unfolded content pieces from the WAL tail.

use super::frame::WalObjectError;
use crate::heap_bytes::{arc_bytes, hash_map_table_bytes, HeapBytes};
use crate::metadata::{
    CommitReceiptRecord, ContentLayoutRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
};
use bytes::Bytes;
use loonfs_types::format::manifest::ManifestActivity;
use loonfs_types::format::wal::{committed_activity, WalCommitPayload, WalDelta, WalInlineContent};
use loonfs_types::{ChangeSeq, CommitId, ContentId, ContentRef, NamespaceId};
use std::collections::{hash_map::Entry, HashMap};
use std::mem::size_of;
use std::sync::Arc;

/// Holds the WAL tail after a manifest as one chunk per WAL object, oldest
/// first. Replay already downloaded the resident bytes, so reads need no
/// content request for them. The head-state budgets bound those bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectedWalTail {
    chunks: Vec<Arc<TailChunk>>,
    activity: ManifestActivity,
    inline_bytes: usize,
}

/// One WAL object's commits: its rows, its content pieces, and its activity.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TailChunk {
    pub(crate) first_seq: ChangeSeq,
    pub(crate) rows: MetadataState,
    activity: ManifestActivity,
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
        piece: ProjectedPiece,
        committed_seq: ChangeSeq,
    ) -> Option<ProjectedPiece> {
        if content_ref.size_bytes > self.content_ref.size_bytes {
            self.content_ref = content_ref.clone();
            self.committed_seq = committed_seq;
        }
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

    fn merge(&mut self, later: &ProjectedContent) {
        for piece in &later.pieces {
            self.insert_piece(&later.content_ref, piece.clone(), later.committed_seq);
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
            chunks: vec![Arc::new(TailChunk::new(ChangeSeq(0), rows))],
            ..Self::default()
        }
    }

    /// Appends one WAL object of `namespace_id`'s WAL as a chunk and returns
    /// that chunk, or `None` for an object with no records.
    pub(crate) fn push_object(
        &mut self,
        namespace_id: &NamespaceId,
        records: &[WalCommitPayload],
    ) -> Result<Option<Arc<TailChunk>>, WalObjectError> {
        if records.is_empty() {
            return Ok(None);
        }
        let chunk = Arc::new(TailChunk::from_records(namespace_id, records)?);
        self.push_chunk(Arc::clone(&chunk))?;
        Ok(Some(chunk))
    }

    pub(crate) fn push_chunk(&mut self, chunk: Arc<TailChunk>) -> Result<(), WalObjectError> {
        self.activity = self
            .activity
            .checked_add(chunk.activity)
            .ok_or(WalObjectError::ActivityOverflow)?;
        self.inline_bytes += chunk.inline_bytes;
        self.chunks.push(chunk);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn chunks(&self) -> &[Arc<TailChunk>] {
        &self.chunks
    }

    /// The rows of every chunk a view at `visible_seq` can see, newest first.
    pub(crate) fn row_states(
        &self,
        visible_seq: ChangeSeq,
    ) -> impl Iterator<Item = &MetadataState> {
        self.chunks
            .iter()
            .rev()
            .filter(move |chunk| chunk.first_seq <= visible_seq)
            .map(|chunk| &chunk.rows)
    }

    /// The unfolded pieces of the content `content_ref` names, when the
    /// tail holds any.
    pub(crate) fn content(&self, content_ref: &ContentRef) -> Option<ProjectedContent> {
        let mut found = self
            .chunks
            .iter()
            .filter_map(|chunk| chunk.content(content_ref));
        let mut merged = found.next()?.clone();
        for later in found {
            merged.merge(later);
        }
        Some(merged)
    }

    /// Contents with pieces, in the order the tail first named them.
    pub(crate) fn contents(&self) -> Vec<ProjectedContent> {
        let mut positions: HashMap<&ContentId, usize> = HashMap::new();
        let mut merged: Vec<ProjectedContent> = Vec::new();
        for content in self.chunks.iter().flat_map(|chunk| &chunk.contents) {
            match positions.entry(&content.content_ref.content_id) {
                Entry::Occupied(position) => merged[*position.get()].merge(content),
                Entry::Vacant(vacant) => {
                    vacant.insert(merged.len());
                    merged.push(content.clone());
                }
            }
        }
        merged
    }

    pub(crate) fn merged_rows(&self) -> MetadataState {
        let rows = || self.chunks.iter().map(|chunk| &chunk.rows);
        MetadataState::from_rows(
            rows().flat_map(MetadataState::inodes).cloned().collect(),
            rows()
                .flat_map(MetadataState::direntry_binds)
                .cloned()
                .collect(),
            rows().flat_map(MetadataState::revisions).cloned().collect(),
            rows()
                .flat_map(MetadataState::subtree_tombstones)
                .cloned()
                .collect(),
            rows()
                .flat_map(MetadataState::commit_receipts)
                .cloned()
                .collect(),
            rows().flat_map(MetadataState::commits).cloned().collect(),
            rows()
                .flat_map(MetadataState::attributes_revisions)
                .cloned()
                .collect(),
            rows()
                .flat_map(MetadataState::content_layouts)
                .cloned()
                .collect(),
            rows()
                .flat_map(MetadataState::access_revisions)
                .cloned()
                .collect(),
        )
    }

    pub(crate) fn revisions(&self) -> impl Iterator<Item = &RevisionRecord> {
        self.chunks.iter().flat_map(|chunk| chunk.rows.revisions())
    }

    pub(crate) fn content_layouts(&self) -> impl Iterator<Item = &ContentLayoutRecord> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.rows.content_layouts())
    }

    pub(crate) fn subtree_tombstones(&self) -> impl Iterator<Item = &SubtreeTombstoneRecord> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.rows.subtree_tombstones())
    }

    pub(crate) fn find_commit_receipt(&self, commit_id: &CommitId) -> Option<&CommitReceiptRecord> {
        self.chunks
            .iter()
            .rev()
            .find_map(|chunk| chunk.rows.find_commit_receipt(commit_id))
    }

    pub(crate) fn activity(&self) -> ManifestActivity {
        self.activity
    }

    pub(crate) fn inline_bytes(&self) -> usize {
        self.inline_bytes
    }

    /// The heap the projection holds behind the `Arc` every holder shares.
    /// Each tail counts every chunk it holds, including chunks another tail
    /// shares.
    pub fn decoded_bytes(&self) -> usize {
        arc_bytes::<Self>()
            + self.chunks.capacity() * size_of::<Arc<TailChunk>>()
            + self
                .chunks
                .iter()
                .map(|chunk| chunk.decoded_bytes())
                .sum::<usize>()
    }
}

impl TailChunk {
    fn new(first_seq: ChangeSeq, rows: MetadataState) -> Self {
        Self {
            first_seq,
            rows,
            activity: ManifestActivity::default(),
            contents: Vec::new(),
            content_positions: HashMap::new(),
            inline_bytes: 0,
            inline_entry_heap_bytes: 0,
        }
    }

    /// Applies each commit of one WAL object of `namespace_id`'s WAL: its
    /// pieces, then its rows.
    fn from_records(
        namespace_id: &NamespaceId,
        records: &[WalCommitPayload],
    ) -> Result<Self, WalObjectError> {
        let first_seq = records
            .first()
            .expect("push_object should skip a WAL object with no records")
            .committed_seq;
        let mut chunk = Self::new(first_seq, MetadataState::default());
        for record in records {
            let activity = committed_activity(record)
                .and_then(|activity| chunk.activity.checked_add(activity))
                .ok_or(WalObjectError::ActivityOverflow)?;
            for (content_ref, entry) in inline_content_refs(namespace_id, record) {
                chunk.insert_piece(content_ref, entry, record.committed_seq);
            }
            chunk.rows.apply_committed_wal_record_mut(record);
            chunk.activity = activity;
        }
        Ok(chunk)
    }

    fn content(&self, content_ref: &ContentRef) -> Option<&ProjectedContent> {
        self.content_positions
            .get(&content_ref.content_id)
            .map(|position| &self.contents[*position])
            .filter(|content| {
                content.content_ref.owner_namespace_id == content_ref.owner_namespace_id
            })
    }

    fn decoded_bytes(&self) -> usize {
        arc_bytes::<Self>()
            + self.rows.decoded_bytes()
            + hash_map_table_bytes(&self.content_positions)
            + self.contents.capacity() * size_of::<ProjectedContent>()
            + self.inline_entry_heap_bytes
            + self.inline_bytes
    }

    fn insert_piece(
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
        let piece = ProjectedPiece {
            offset: entry.offset,
            bytes: Bytes::copy_from_slice(&entry.bytes),
        };
        let replaced = content.insert_piece(content_ref, piece, committed_seq);
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
            .expect("`validate_wal_inline_content` should reject inline content without a same-commit reference");
        (content_ref, entry)
    })
}

#[cfg(test)]
mod tests {
    use super::ProjectedWalTail;
    use crate::metadata::MetadataState;
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
            tail.push_object(
                &namespace_id,
                &[commit(&namespace_id, seq, content_id, b"value", 0)],
            )
            .expect("value");
        }
        tail.push_object(
            &namespace_id,
            &[commit(&namespace_id, 65, &ids[17], b"value appended", 5)],
        )
        .expect("append");

        let contents = tail.contents();
        assert_eq!(
            contents
                .iter()
                .map(|content| &content.content_ref.content_id)
                .collect::<Vec<_>>(),
            ids.iter().collect::<Vec<_>>()
        );
        let appended = &contents[17];
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

    #[test]
    fn appended_pieces_merge_across_chunks_and_an_earlier_view_skips_later_chunks() {
        let namespace_id = NamespaceId::parse("pieces").expect("namespace id");
        let content_id = ContentId::parse(format!("con_{:032x}", 1)).expect("content id");
        let mut tail = ProjectedWalTail::default();
        for (seq, whole, offset) in [
            (1, &b"value"[..], 0),
            (2, b"value appended", 5),
            (3, b"value appended twice", 14),
        ] {
            tail.push_object(
                &namespace_id,
                &[commit(&namespace_id, seq, &content_id, whole, offset)],
            )
            .expect("chunk");
        }
        let newest = ContentRef::blob_v1(
            namespace_id.clone(),
            content_id.clone(),
            b"value appended twice",
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );

        let content = tail.content(&newest).expect("merged content");
        assert_eq!(content.content_ref, newest);
        assert_eq!(content.committed_seq, ChangeSeq(3));
        assert_eq!(
            content
                .pieces
                .iter()
                .map(|piece| (piece.offset, piece.end()))
                .collect::<Vec<_>>(),
            [(0, 5), (5, 14), (14, 20)]
        );
        assert_eq!(tail.contents(), [content]);
        assert_eq!(
            tail.row_states(ChangeSeq(1))
                .flat_map(MetadataState::commits)
                .map(|record| record.committed_seq)
                .collect::<Vec<_>>(),
            [ChangeSeq(1)]
        );
    }
}
