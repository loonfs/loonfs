//! Heap weights charged to metadata caches, the per-view block memo,
//! head and WAL-tail budgets, and change feed pages.
//!
//! A weight counts the bytes a value asks the allocator for: every slot a
//! container reserved, filled or not, and every string, vector, map table,
//! tree node, and reference count the value owns. Allocator rounding and
//! bookkeeping stay outside every budget.

use crate::gc::{CompletedUploadSession, MemberState};
use crate::namespace::state::NamespaceReadState;
use crate::path::write::{CommitIntent, FilesystemOperation};
use crate::storage::content_admission::ContentTokenError;
use crate::storage::inline_content::InlineContent;
use loonfs_types::format::control::{
    ForkBasis, ManifestRef, PinOwner, PinPayload, RebasedFrom, WriterBlock,
};
use loonfs_types::format::envelope::VerifiedEnvelope;
use loonfs_types::format::manifest::{
    AccessRevisionRecord, ActiveDeletionRecord, ActiveDeletionRowAction, AttributesRevisionRecord,
    CommitReceiptRecord, ContentLayoutRecord, DeletedBinding, DirentryBindingRecord,
    ExtensionEntry, ExtensionRunRef, ExtensionSegmentRef, InodeRecord, MergeIndexRecord,
    MetadataRow, MetadataRunRef, MetadataSegmentRef, NamespaceAccess, NamespaceManifestEnvelope,
    NamespaceManifestPayload, RevisionRecord, SubtreeTombstoneRecord, TombstoneRowAction,
};
use loonfs_types::format::segment_blocks::{DecodedDataBlock, SegmentIndexEntry};
use loonfs_types::format::wal::{
    MergeRecord, WalCommitDelta, WalCommitPayload, WalDelta, WalInlineContent,
};
use loonfs_types::{
    AbsolutePath, AccessGrants, ActorId, AttributeKey, AttributeValue, Attributes, BindingVersion,
    ChangeSeq, Checksum, ChecksumHex, CommitFingerprint, CommitId, CommitPrecondition, ContentId,
    ContentRef, DisplayName, ExtensionName, FileContent, IndexSegmentId, InodeId,
    MetadataSegmentId, NameKey, NamespaceId, PathComponent, PayloadChecksum, PinId, PrincipalId,
    PrincipalScope, Subject, SubjectId, UploadId, WriterId,
};
use loonfs_types::{DiffEntry, FilesystemChange};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::size_of;

/// Heap a value owns outside its own inline size. Whatever contains the value
/// charges that inline size itself.
pub(crate) trait HeapBytes {
    fn heap_bytes(&self) -> usize;
}

/// One `Arc` allocation: two reference counts, then the value.
pub(crate) const fn arc_bytes<T>() -> usize {
    2 * size_of::<usize>() + size_of::<T>()
}

/// A decoded data block behind the `Arc` the caches share.
pub(crate) fn data_block_heap_bytes(block: &DecodedDataBlock) -> usize {
    arc_bytes::<DecodedDataBlock>() + block.row_keys.heap_bytes() + block.rows.heap_bytes()
}

/// A decoded manifest behind the `Arc` that its loaders share.
pub(crate) fn manifest_heap_bytes(envelope: &NamespaceManifestEnvelope) -> usize {
    arc_bytes::<NamespaceManifestEnvelope>() + envelope.heap_bytes()
}

/// A decoded index block behind the `Arc` the caches share.
pub(crate) fn index_block_heap_bytes(entries: &Vec<SegmentIndexEntry>) -> usize {
    arc_bytes::<Vec<SegmentIndexEntry>>() + entries.heap_bytes()
}

/// The table `std`'s hash map allocates for its capacity. The map keeps
/// eight buckets for every seven entries it can contain (four or eight buckets
/// below fourteen), one control byte per bucket, and one trailing group of
/// control bytes.
pub(crate) fn hash_map_table_bytes<K, V>(map: &HashMap<K, V>) -> usize {
    hash_table_bytes::<(K, V)>(map.capacity())
}

/// The table `std`'s hash set allocates for its capacity, laid out as the
/// map's.
pub(crate) fn hash_set_table_bytes<K>(set: &HashSet<K>) -> usize {
    hash_table_bytes::<K>(set.capacity())
}

fn hash_table_bytes<Entry>(capacity: usize) -> usize {
    const TRAILING_CONTROL_BYTES: usize = 16;
    let buckets = match capacity {
        0 => return 0,
        capacity if capacity < 8 => capacity + 1,
        capacity => capacity / 7 * 8,
    };
    buckets * (size_of::<Entry>() + 1) + TRAILING_CONTROL_BYTES
}

/// The nodes of a B-tree map filled by sorted inserts, which is how
/// decoding fills one. A full node of eleven slots splits into six and five
/// and moves one entry up to its parent. So each node after the first on a
/// level takes seven entries: six it keeps and one its parent keeps. The
/// parents form the next level and fill the same way, up to a single root.
pub(crate) fn btree_map_node_bytes<K, V>(map: &BTreeMap<K, V>) -> usize {
    btree_node_bytes::<K, V>(map.len())
}

/// The nodes of a B-tree map of `len` entries filled by sorted inserts (see
/// [`btree_map_node_bytes`]).
fn btree_node_bytes<K, V>(len: usize) -> usize {
    const NODE_SLOTS: usize = 11;
    let leaf = (size_of::<usize>()
        + 2 * size_of::<u16>()
        + NODE_SLOTS * (size_of::<K>() + size_of::<V>()))
    .next_multiple_of(size_of::<usize>());
    let internal = leaf + (NODE_SLOTS + 1) * size_of::<usize>();
    let nodes = |entries: usize| match entries {
        0 => 0,
        entries if entries <= NODE_SLOTS => 1,
        entries => 2 + (entries - NODE_SLOTS - 1) / 7,
    };
    let mut level = nodes(len);
    let mut bytes = level * leaf;
    while level > 1 {
        level = nodes(level - 1);
        bytes += level * internal;
    }
    bytes
}

impl HeapBytes for String {
    fn heap_bytes(&self) -> usize {
        self.capacity()
    }
}

impl<T: HeapBytes> HeapBytes for Vec<T> {
    fn heap_bytes(&self) -> usize {
        self.iter()
            .map(HeapBytes::heap_bytes)
            .fold(self.capacity() * size_of::<T>(), usize::saturating_add)
    }
}

impl<T: HeapBytes> HeapBytes for Option<T> {
    fn heap_bytes(&self) -> usize {
        self.as_ref().map_or(0, HeapBytes::heap_bytes)
    }
}

impl<A: HeapBytes, B: HeapBytes> HeapBytes for (A, B) {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes() + self.1.heap_bytes()
    }
}

// Validated text copies its input once validated, so it keeps exactly its
// length.
macro_rules! text_heap_bytes {
    ($($text:ty),+ $(,)?) => {
        $(impl HeapBytes for $text {
            fn heap_bytes(&self) -> usize {
                self.as_str().len()
            }
        })+
    };
}

text_heap_bytes!(
    ActorId,
    AttributeKey,
    AttributeValue,
    BindingVersion,
    CommitFingerprint,
    CommitId,
    ContentId,
    DisplayName,
    ChecksumHex,
    ExtensionName,
    IndexSegmentId,
    MetadataSegmentId,
    NameKey,
    NamespaceId,
    PayloadChecksum,
    PinId,
    PrincipalId,
    PrincipalScope,
    SubjectId,
    UploadId,
    WriterId,
);

impl HeapBytes for InodeId {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl HeapBytes for ChangeSeq {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl HeapBytes for Checksum {
    fn heap_bytes(&self) -> usize {
        self.value().heap_bytes()
    }
}

impl HeapBytes for CompletedUploadSession {
    fn heap_bytes(&self) -> usize {
        self.namespace_id.heap_bytes() + self.content_id.heap_bytes()
    }
}

impl HeapBytes for MemberState {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl HeapBytes for ContentRef {
    fn heap_bytes(&self) -> usize {
        self.content_id.heap_bytes() + self.checksum.heap_bytes()
    }
}

impl HeapBytes for InlineContent {
    fn heap_bytes(&self) -> usize {
        self.content_ref().heap_bytes() + self.bytes().len()
    }
}

impl HeapBytes for ContentTokenError {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Codec(message) => message.heap_bytes(),
            Self::Malformed
            | Self::BadSignature
            | Self::NamespaceMismatch
            | Self::ContentRefMismatch
            | Self::Expired
            | Self::TimeOverflow => 0,
        }
    }
}

impl HeapBytes for CommitIntent {
    fn heap_bytes(&self) -> usize {
        self.commit_id.heap_bytes()
            + self.actor_id.heap_bytes()
            + self.subject.heap_bytes()
            + self.message.heap_bytes()
            + self.operations.heap_bytes()
            + self.preconditions.heap_bytes()
    }
}

// A principal set is a B-tree set, whose nodes have no value slots.
impl HeapBytes for Subject {
    fn heap_bytes(&self) -> usize {
        self.principals.iter().map(HeapBytes::heap_bytes).fold(
            self.principal_scope.heap_bytes()
                + self.subject_id.heap_bytes()
                + btree_node_bytes::<PrincipalId, ()>(self.principals.len()),
            usize::saturating_add,
        )
    }
}

impl HeapBytes for CommitPrecondition {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::PathBinding {
                path,
                expected_binding_version,
                ..
            } => path.heap_bytes() + expected_binding_version.heap_bytes(),
            Self::InodeBinding {
                expected_binding_version,
                ..
            } => expected_binding_version.heap_bytes(),
            Self::PathAbsence { path } => path.heap_bytes(),
            Self::NameAbsence { display_name, .. } => display_name.heap_bytes(),
            Self::NamespaceHead { .. }
            | Self::FileRevision { .. }
            | Self::AttributesRevision { .. }
            | Self::AccessRevision { .. } => 0,
        }
    }
}

impl HeapBytes for FileContent {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Ref {
                content_ref,
                source_namespace_id,
            } => content_ref.heap_bytes() + source_namespace_id.heap_bytes(),
            Self::Inline(bytes) => bytes.capacity(),
        }
    }
}

impl HeapBytes for FilesystemOperation {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::CreateDirectory { path, .. }
            | Self::DeletePath { path, .. }
            | Self::RestoreRevision { path, .. } => path.heap_bytes(),
            Self::CreateDirectoryByInode { display_name, .. } => display_name.heap_bytes(),
            Self::PutFile { path, content, .. } => path.heap_bytes() + content.heap_bytes(),
            Self::CreateFileByInode {
                display_name,
                content,
                ..
            } => display_name.heap_bytes() + content.heap_bytes(),
            Self::PutFileRevisionByInode { content, .. } => content.heap_bytes(),
            Self::AppendFile {
                path,
                inline_content,
                ..
            } => path.heap_bytes() + inline_content.capacity(),
            Self::AppendFileByInode { inline_content, .. } => inline_content.capacity(),
            Self::DeleteByInode {
                expected_binding_version,
                ..
            } => expected_binding_version.heap_bytes(),
            Self::MovePath {
                source_path,
                destination_path,
                ..
            }
            | Self::CopyPath {
                source_path,
                destination_path,
                ..
            } => source_path.heap_bytes() + destination_path.heap_bytes(),
            Self::MoveByInode {
                expected_binding_version,
                destination_display_name,
                ..
            } => expected_binding_version.heap_bytes() + destination_display_name.heap_bytes(),
            Self::CopyByInode {
                destination_display_name,
                ..
            } => destination_display_name.heap_bytes(),
            Self::Undelete {
                destination_path,
                destination_display_name,
                ..
            } => destination_path.heap_bytes() + destination_display_name.heap_bytes(),
            Self::RestoreRevisionByInode { .. } => 0,
            Self::UpdateAttributes {
                path, set, remove, ..
            } => path.heap_bytes() + attribute_set_heap_bytes(set) + remove.heap_bytes(),
            Self::UpdateAttributesByInode { set, remove, .. } => {
                attribute_set_heap_bytes(set) + remove.heap_bytes()
            }
            Self::UpdateAccess { path, grants, .. } => path.heap_bytes() + grants.heap_bytes(),
            Self::UpdateAccessByInode { grants, .. } => grants.heap_bytes(),
        }
    }
}

/// The attributes that an update writes.
fn attribute_set_heap_bytes(set: &BTreeMap<AttributeKey, AttributeValue>) -> usize {
    set.iter()
        .map(|(key, value)| key.heap_bytes() + value.heap_bytes())
        .fold(btree_map_node_bytes(set), usize::saturating_add)
}

impl HeapBytes for FilesystemChange {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::DirectoryCreated {
                display_name,
                binding_version,
                ..
            }
            | Self::Undeleted {
                display_name,
                binding_version,
                ..
            } => display_name.heap_bytes() + binding_version.heap_bytes(),
            Self::FileCreated {
                display_name,
                binding_version,
                content_ref,
                ..
            } => {
                display_name.heap_bytes() + binding_version.heap_bytes() + content_ref.heap_bytes()
            }
            Self::ContentChanged { content_ref, .. } => content_ref.heap_bytes(),
            Self::Moved {
                source_display_name,
                destination_display_name,
                binding_version,
                ..
            } => {
                source_display_name.heap_bytes()
                    + destination_display_name.heap_bytes()
                    + binding_version.heap_bytes()
            }
            Self::Deleted {
                deleted_binding, ..
            } => deleted_binding.name_key.heap_bytes() + deleted_binding.display_name.heap_bytes(),
            Self::AttributesChanged { attributes, .. } => attributes.heap_bytes(),
            Self::AccessChanged { grants, .. } => grants.heap_bytes(),
            Self::Unknown => 0,
        }
    }
}

impl HeapBytes for DiffEntry {
    fn heap_bytes(&self) -> usize {
        self.path.heap_bytes() + self.previous_path.heap_bytes() + self.binding_version.heap_bytes()
    }
}

// The normalized spelling, and each component's own copy of its name in the
// component vector.
impl HeapBytes for AbsolutePath {
    fn heap_bytes(&self) -> usize {
        self.components()
            .iter()
            .map(|component| size_of::<PathComponent>() + component.as_str().len())
            .fold(self.as_str().len(), usize::saturating_add)
    }
}

impl HeapBytes for DeletedBinding {
    fn heap_bytes(&self) -> usize {
        self.name_key.heap_bytes() + self.display_name.heap_bytes()
    }
}

impl HeapBytes for Attributes {
    fn heap_bytes(&self) -> usize {
        self.iter()
            .map(|(key, value)| key.heap_bytes() + value.heap_bytes())
            .fold(btree_map_node_bytes(self.as_map()), usize::saturating_add)
    }
}

impl HeapBytes for AccessGrants {
    fn heap_bytes(&self) -> usize {
        self.iter()
            .map(|(principal_id, _)| principal_id.heap_bytes())
            .fold(btree_map_node_bytes(self.as_map()), usize::saturating_add)
    }
}

impl HeapBytes for MetadataRow {
    fn heap_bytes(&self) -> usize {
        match self {
            MetadataRow::Inode(record) => record.heap_bytes(),
            MetadataRow::DirentryBinding(record) => record.heap_bytes(),
            MetadataRow::FileRevision(record) => record.heap_bytes(),
            MetadataRow::Tombstone(record) => record.heap_bytes(),
            MetadataRow::ActiveDeletion(record) => record.heap_bytes(),
            MetadataRow::CommitReceipt(record) => record.heap_bytes(),
            MetadataRow::Merge(record) => record.heap_bytes(),
            MetadataRow::Commit(record) => record.heap_bytes(),
            MetadataRow::ContentLayout(record) => record.heap_bytes(),
            MetadataRow::AttributesRevision(record) => record.heap_bytes(),
            MetadataRow::AccessRevision(record) => record.heap_bytes(),
        }
    }
}

impl HeapBytes for InodeRecord {
    fn heap_bytes(&self) -> usize {
        self.commit_id.heap_bytes() + self.committed_by.heap_bytes()
    }
}

impl HeapBytes for DirentryBindingRecord {
    fn heap_bytes(&self) -> usize {
        self.name_key.heap_bytes()
            + self.child_created_by.heap_bytes()
            + self.commit_id.heap_bytes()
            + self.committed_by.heap_bytes()
            + self.display_name().map_or(0, HeapBytes::heap_bytes)
            + self
                .revision()
                .map_or(0, |revision| revision.revision_committed_by.heap_bytes())
    }
}

impl HeapBytes for RevisionRecord {
    fn heap_bytes(&self) -> usize {
        self.commit_id.heap_bytes() + self.committed_by.heap_bytes() + self.content_ref.heap_bytes()
    }
}

impl HeapBytes for SubtreeTombstoneRecord {
    fn heap_bytes(&self) -> usize {
        let action = match &self.action {
            TombstoneRowAction::Set {
                deleted_binding, ..
            } => deleted_binding.heap_bytes(),
            TombstoneRowAction::Revoke { .. } => 0,
        };
        self.commit_id.heap_bytes() + self.committed_by.heap_bytes() + action
    }
}

impl HeapBytes for ActiveDeletionRecord {
    fn heap_bytes(&self) -> usize {
        match &self.action {
            ActiveDeletionRowAction::Listed {
                deleted_by,
                deleted_binding,
                ..
            } => deleted_by.heap_bytes() + deleted_binding.heap_bytes(),
            ActiveDeletionRowAction::Removed { .. } => 0,
        }
    }
}

impl HeapBytes for ContentLayoutRecord {
    fn heap_bytes(&self) -> usize {
        self.content_id.heap_bytes() + self.layout.heap_bytes()
    }
}

impl HeapBytes for CommitReceiptRecord {
    fn heap_bytes(&self) -> usize {
        self.commit_id.heap_bytes()
    }
}

impl HeapBytes for AttributesRevisionRecord {
    fn heap_bytes(&self) -> usize {
        self.commit_id.heap_bytes() + self.committed_by.heap_bytes() + self.attributes.heap_bytes()
    }
}

impl HeapBytes for AccessRevisionRecord {
    fn heap_bytes(&self) -> usize {
        self.commit_id.heap_bytes() + self.committed_by.heap_bytes() + self.grants.heap_bytes()
    }
}

impl HeapBytes for WalCommitPayload {
    fn heap_bytes(&self) -> usize {
        self.commit_id.heap_bytes()
            + self.committed_by.heap_bytes()
            + self.semantic_commit_fingerprint.heap_bytes()
            + self.message.heap_bytes()
            + self.merge.heap_bytes()
            + self.deltas.heap_bytes()
            + self.inline_content.heap_bytes()
    }
}

impl HeapBytes for MergeRecord {
    fn heap_bytes(&self) -> usize {
        self.source_namespace_id.heap_bytes()
    }
}

impl HeapBytes for MergeIndexRecord {
    fn heap_bytes(&self) -> usize {
        self.source_namespace_id.heap_bytes()
    }
}

impl HeapBytes for WalCommitDelta {
    fn heap_bytes(&self) -> usize {
        match &self.delta {
            WalDelta::CreateInode { .. } | WalDelta::RevokeSubtreeTombstone { .. } => 0,
            WalDelta::BindDirentry {
                name_key,
                display_name,
                child_created_by,
                revision,
                ..
            } => {
                name_key.heap_bytes()
                    + display_name.heap_bytes()
                    + child_created_by.heap_bytes()
                    + revision
                        .as_ref()
                        .map_or(0, |revision| revision.revision_committed_by.heap_bytes())
            }
            WalDelta::UnbindDirentry {
                name_key,
                display_name,
                child_created_by,
                ..
            } => name_key.heap_bytes() + display_name.heap_bytes() + child_created_by.heap_bytes(),
            WalDelta::AppendFileRevision {
                name_key,
                display_name,
                child_created_by,
                content_ref,
                layout,
                ..
            } => {
                name_key.heap_bytes()
                    + display_name.heap_bytes()
                    + child_created_by.heap_bytes()
                    + content_ref.heap_bytes()
                    + layout.heap_bytes()
            }
            WalDelta::TombstoneSubtree {
                deleted_binding, ..
            } => deleted_binding.heap_bytes(),
            WalDelta::AppendAttributesRevision { attributes, .. } => attributes.heap_bytes(),
            WalDelta::AppendAccessRevision { grants, .. } => grants.heap_bytes(),
        }
    }
}

impl HeapBytes for WalInlineContent {
    fn heap_bytes(&self) -> usize {
        self.content_id.heap_bytes() + self.bytes.capacity()
    }
}

impl<T: HeapBytes> HeapBytes for VerifiedEnvelope<T> {
    fn heap_bytes(&self) -> usize {
        self.payload_checksum().heap_bytes() + self.payload().heap_bytes()
    }
}

impl HeapBytes for NamespaceManifestPayload {
    fn heap_bytes(&self) -> usize {
        self.namespace_id.heap_bytes()
            + self.created_by.heap_bytes()
            + self.access.heap_bytes()
            + self.fork_basis.heap_bytes()
            + self.family_root_id.heap_bytes()
            + self.writer.heap_bytes()
            + self.runs.heap_bytes()
            + self
                .extensions
                .iter()
                .map(|(name, entry)| name.heap_bytes() + entry.heap_bytes())
                .fold(
                    btree_map_node_bytes(&self.extensions),
                    usize::saturating_add,
                )
    }
}

impl HeapBytes for ExtensionEntry {
    fn heap_bytes(&self) -> usize {
        self.runs.heap_bytes() + json_object_heap_bytes(self.state())
    }
}

impl HeapBytes for ExtensionRunRef {
    fn heap_bytes(&self) -> usize {
        self.segments.heap_bytes()
    }
}

impl HeapBytes for ExtensionSegmentRef {
    fn heap_bytes(&self) -> usize {
        self.owner_namespace_id.heap_bytes()
            + self.segment_id.heap_bytes()
            + self.min_row_key.heap_bytes()
            + self.max_row_key.heap_bytes()
            + self.filter_inline.heap_bytes()
    }
}

/// The heap that a decoded JSON object owns: its map's nodes, its keys, and
/// each value's own heap, walked with a stack instead of recursion.
fn json_object_heap_bytes(object: &serde_json::Map<String, serde_json::Value>) -> usize {
    let object_bytes = |object: &serde_json::Map<String, serde_json::Value>| {
        object.keys().map(HeapBytes::heap_bytes).fold(
            btree_node_bytes::<String, serde_json::Value>(object.len()),
            usize::saturating_add,
        )
    };
    let mut bytes = object_bytes(object);
    let mut values: Vec<&serde_json::Value> = object.values().collect();
    while let Some(value) = values.pop() {
        match value {
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            }
            serde_json::Value::String(text) => bytes = bytes.saturating_add(text.heap_bytes()),
            serde_json::Value::Array(items) => {
                bytes = bytes.saturating_add(items.capacity() * size_of::<serde_json::Value>());
                values.extend(items);
            }
            serde_json::Value::Object(object) => {
                bytes = bytes.saturating_add(object_bytes(object));
                values.extend(object.values());
            }
        }
    }
    bytes
}

impl HeapBytes for NamespaceAccess {
    fn heap_bytes(&self) -> usize {
        match self {
            NamespaceAccess::Unrestricted {} => 0,
            NamespaceAccess::Acl {
                principal_scope,
                root_grants,
            } => principal_scope.heap_bytes() + root_grants.heap_bytes(),
        }
    }
}

impl HeapBytes for NamespaceReadState {
    fn heap_bytes(&self) -> usize {
        self.namespace_id.heap_bytes()
            + self.created_by.heap_bytes()
            + self.access.heap_bytes()
            + self.fork_basis.heap_bytes()
            + self.family_root_id.heap_bytes()
            + self.writer.heap_bytes()
    }
}

impl HeapBytes for ManifestRef {
    fn heap_bytes(&self) -> usize {
        self.owner_namespace_id.heap_bytes() + self.payload_checksum.heap_bytes()
    }
}

impl HeapBytes for PinPayload {
    fn heap_bytes(&self) -> usize {
        self.namespace_id.heap_bytes()
            + self.pin_id.heap_bytes()
            + self.payload_checksum.heap_bytes()
            + match &self.owner {
                PinOwner::User { name, .. } => name.heap_bytes(),
                PinOwner::Fork {
                    target_namespace_id,
                } => target_namespace_id.heap_bytes(),
            }
    }
}

impl HeapBytes for ForkBasis {
    fn heap_bytes(&self) -> usize {
        self.manifest.heap_bytes()
            + self.source_pin_id.heap_bytes()
            + self.rebased_from.heap_bytes()
    }
}

impl HeapBytes for RebasedFrom {
    fn heap_bytes(&self) -> usize {
        self.source_namespace_id.heap_bytes()
    }
}

impl HeapBytes for WriterBlock {
    fn heap_bytes(&self) -> usize {
        self.writer_id.heap_bytes()
    }
}

impl HeapBytes for MetadataRunRef {
    fn heap_bytes(&self) -> usize {
        self.segments.heap_bytes()
    }
}

impl HeapBytes for MetadataSegmentRef {
    fn heap_bytes(&self) -> usize {
        self.owner_namespace_id.heap_bytes()
            + self.segment_id.heap_bytes()
            + self.min_row_key.heap_bytes()
            + self.max_row_key.heap_bytes()
            + self.filter_inline.heap_bytes()
    }
}

impl HeapBytes for SegmentIndexEntry {
    fn heap_bytes(&self) -> usize {
        self.last_row_key.heap_bytes()
    }
}

impl HeapBytes for loonfs_types::ContentLayout {
    fn heap_bytes(&self) -> usize {
        self.creator_namespace_id.heap_bytes()
            + self.extents.capacity() * size_of::<loonfs_types::ContentExtent>()
            + self
                .extents
                .iter()
                .map(|extent| extent.content_id.heap_bytes())
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::HeapBytes;
    use loonfs_types::AccessGrants;

    #[test]
    fn a_decoded_grant_map_is_charged_for_every_level_of_nodes() {
        // A counting allocator measured these counts, with leaves of 288
        // bytes and internal nodes of 384, on maps built by sorted inserts.
        // The sizes sit on both sides of each point where the tree gains a
        // level.
        for (entries, leaves, internal_nodes) in [
            (11, 1, 0),
            (12, 2, 1),
            (88, 12, 1),
            (89, 13, 3),
            (627, 89, 13),
            (628, 90, 16),
            (1_000, 143, 24),
        ] {
            let json = (0..entries)
                .map(|entry| format!("\"p{entry:04}\":[\"read\"]"))
                .collect::<Vec<_>>()
                .join(",");
            let grants: AccessGrants =
                serde_json::from_str(&format!("{{{json}}}")).expect("grants");
            assert_eq!(
                grants.heap_bytes(),
                leaves * 288 + internal_nodes * 384 + 5 * entries,
                "{entries} entries"
            );
        }
    }
}
