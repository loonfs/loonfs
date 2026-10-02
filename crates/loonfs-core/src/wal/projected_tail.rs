//! Metadata rows and inline content from the unfolded WAL tail.

use super::frame::WalObjectError;
use crate::heap_bytes::{arc_bytes, hash_map_table_bytes, HeapBytes};
use crate::metadata::MetadataState;
use bytes::Bytes;
use loonfs_types::format::manifest::ManifestActivity;
use loonfs_types::format::wal::{committed_activity, WalCommitPayload};
use loonfs_types::{ContentId, ContentRef};
use std::collections::{hash_map::Entry, HashMap};
use std::mem::size_of;

/// Holds the WAL tail after a manifest as rows and the inline content they name.
/// Replay already downloaded the resident bytes, so reads need no content request.
/// The head-state budgets bound those bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectedWalTail {
    pub(crate) rows: MetadataState,
    pub(crate) activity: ManifestActivity,
    inline_content: Vec<ProjectedInlineContent>,
    inline_content_positions: HashMap<ContentId, usize>,
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
        self.inline_content_positions
            .get(&content_ref.content_id)
            .map(|position| &self.inline_content[*position])
            .filter(|value| &value.content_ref == content_ref)
            .map(|value| &value.bytes)
    }

    pub(crate) fn inline_values(&self) -> impl Iterator<Item = &ProjectedInlineContent> {
        self.inline_content.iter()
    }

    pub(crate) fn inline_bytes(&self) -> usize {
        self.inline_bytes
    }

    /// The heap the projection holds behind the `Arc` every holder shares.
    pub fn decoded_bytes(&self) -> usize {
        arc_bytes::<Self>()
            + self.rows.decoded_bytes()
            + hash_map_table_bytes(&self.inline_content_positions)
            + self.inline_content.capacity() * size_of::<ProjectedInlineContent>()
            + self.inline_entry_heap_bytes
            + self.inline_bytes()
    }

    pub(crate) fn insert_inline_content(&mut self, content_ref: ContentRef, bytes: Bytes) {
        self.inline_bytes += bytes.len();
        let content_id = content_ref.content_id.clone();
        let key_heap_bytes = content_id.heap_bytes();
        self.inline_entry_heap_bytes += content_ref.heap_bytes();
        let value = ProjectedInlineContent { content_ref, bytes };
        match self.inline_content_positions.entry(content_id) {
            Entry::Occupied(entry) => {
                let previous = std::mem::replace(&mut self.inline_content[*entry.get()], value);
                self.inline_bytes -= previous.bytes.len();
                self.inline_entry_heap_bytes -= previous.content_ref.heap_bytes();
            }
            Entry::Vacant(entry) => {
                entry.insert(self.inline_content.len());
                self.inline_content.push(value);
                self.inline_entry_heap_bytes += key_heap_bytes;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProjectedWalTail;
    use bytes::Bytes;
    use loonfs_types::{ContentId, ContentRef, NamespaceId};

    #[test]
    fn inline_values_keep_first_insertion_order_after_replacement() {
        let namespace_id = NamespaceId::parse("inline-order").expect("namespace id");
        let mut tail = ProjectedWalTail::default();
        let mut expected = Vec::new();
        for number in (0..64).rev() {
            let bytes = Bytes::from(format!("content-{number}"));
            let content_ref = ContentRef::blob_v1(
                namespace_id.clone(),
                ContentId::parse(format!("con_{number:032x}")).expect("content id"),
                &bytes,
            );
            tail.insert_inline_content(content_ref.clone(), bytes.clone());
            expected.push((content_ref, bytes));
        }
        assert_eq!(
            tail.inline_values()
                .map(|value| (&value.content_ref, &value.bytes))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|(reference, bytes)| (reference, bytes))
                .collect::<Vec<_>>()
        );

        let previous_bytes = tail.inline_bytes();
        let previous_decoded_bytes = tail.decoded_bytes();
        let (content_ref, bytes) = &mut expected[17];
        let previous_len = bytes.len();
        *bytes = Bytes::from_static(b"replacement content");
        *content_ref = ContentRef::blob_v1(namespace_id, content_ref.content_id.clone(), bytes);
        tail.insert_inline_content(content_ref.clone(), bytes.clone());
        assert_eq!(
            tail.inline_bytes(),
            previous_bytes - previous_len + bytes.len()
        );
        assert_eq!(
            tail.decoded_bytes(),
            previous_decoded_bytes - previous_len + bytes.len()
        );
        assert_eq!(
            tail.inline_values()
                .map(|value| (&value.content_ref, &value.bytes))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|(reference, bytes)| (reference, bytes))
                .collect::<Vec<_>>()
        );
        for (content_ref, bytes) in &expected {
            assert_eq!(tail.inline_content(content_ref), Some(bytes));
        }
    }
}
