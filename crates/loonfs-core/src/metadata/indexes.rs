//! At-head indexes over the in-memory metadata rows, maintained
//! incrementally as deltas apply so head reads skip the row scans.

use super::{
    AccessRevisionRecord, AttributesRevisionRecord, CommitReceiptRecord, ContentPublicationRecord,
    DirentryBindingRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
    TombstoneRowAction,
};
use loonfs_api::{ChangeSeq, CommitId, InodeId, NameKey};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataIndexes {
    indexed_seq: ChangeSeq,
    inode_by_id: HashMap<InodeId, InodeRecord>,
    latest_bind_by_parent_name: HashMap<(InodeId, NameKey), DirentryBindingRecord>,
    latest_binding_by_child: HashMap<InodeId, DirentryBindingRecord>,
    tombstone_by_root: HashMap<InodeId, SubtreeTombstoneRecord>,
    commit_receipt_by_id: HashMap<CommitId, CommitReceiptRecord>,
    content_publication_by_id: HashMap<loonfs_api::ContentId, ChangeSeq>,
}

impl Default for MetadataIndexes {
    fn default() -> Self {
        Self {
            indexed_seq: ChangeSeq(0),
            inode_by_id: HashMap::new(),
            latest_bind_by_parent_name: HashMap::new(),
            latest_binding_by_child: HashMap::new(),
            tombstone_by_root: HashMap::new(),
            commit_receipt_by_id: HashMap::new(),
            content_publication_by_id: HashMap::new(),
        }
    }
}

impl MetadataIndexes {
    pub(super) fn rebuild(state: &MetadataState) -> Self {
        let mut indexes = Self::default();

        for inode in &state.inodes {
            indexes.record_inode(inode);
        }

        for binding in &state.direntry_binds {
            indexes.record_binding(binding);
        }

        for revision in &state.revisions {
            indexes.record_revision(revision);
        }

        for tombstone in &state.subtree_tombstones {
            indexes.record_tombstone(tombstone);
        }

        for publication in &state.content_publications {
            indexes.record_content_publication(publication);
        }
        for receipt in &state.commit_receipts {
            indexes.record_commit_receipt(receipt);
        }

        for attributes_revision in &state.attributes_revisions {
            indexes.record_attributes_revision(attributes_revision);
        }

        for access_revision in &state.access_revisions {
            indexes.record_access_revision(access_revision);
        }

        indexes
    }

    pub(super) fn indexed_seq(&self) -> ChangeSeq {
        self.indexed_seq
    }

    pub(super) fn inode(&self, inode_id: InodeId) -> Option<InodeRecord> {
        self.inode_by_id.get(&inode_id).cloned()
    }

    pub(super) fn active_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Option<DirentryBindingRecord> {
        self.latest_bind(parent_inode_id, name_key)
            .filter(DirentryBindingRecord::is_bound)
    }

    pub(super) fn latest_bind(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Option<DirentryBindingRecord> {
        self.latest_bind_by_parent_name
            .get(&(parent_inode_id, name_key.clone()))
            .cloned()
    }

    pub(super) fn active_parent_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Option<DirentryBindingRecord> {
        self.latest_binding_by_child
            .get(&child_inode_id)
            .filter(|row| row.is_bound())
            .cloned()
    }

    pub(super) fn active_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Option<SubtreeTombstoneRecord> {
        // The index keeps the newest record per root, whatever its action;
        // a revoke as the newest record means no tombstone is active.
        self.tombstone_by_root
            .get(&root_inode_id)
            .filter(|tombstone| matches!(tombstone.action, TombstoneRowAction::Set { .. }))
            .cloned()
    }

    pub(super) fn commit_receipt(&self, commit_id: &CommitId) -> Option<&CommitReceiptRecord> {
        self.commit_receipt_by_id.get(commit_id)
    }

    pub(super) fn record_inode(&mut self, record: &InodeRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
        self.inode_by_id.insert(record.inode_id, record.clone());
    }

    pub(super) fn record_binding(&mut self, record: &DirentryBindingRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
        replace_if_newer(
            &mut self.latest_bind_by_parent_name,
            (record.parent_inode_id, record.name_key.clone()),
            record.clone(),
            DirentryBindingRecord::position,
        );
        replace_if_newer(
            &mut self.latest_binding_by_child,
            record.child_inode_id,
            record.clone(),
            DirentryBindingRecord::position,
        );
    }

    /// Revisions contribute only the seq watermark: no read consults an
    /// in-memory revision index — revision lookups scan the rows, which stay
    /// tail-sized in memory (the manifest segments answer the bulk).
    pub(super) fn record_revision(&mut self, record: &RevisionRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
    }

    pub(super) fn record_tombstone(&mut self, record: &SubtreeTombstoneRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
        replace_if_newer(
            &mut self.tombstone_by_root,
            record.root_inode_id,
            record.clone(),
            tombstone_order_key,
        );
    }

    pub(super) fn content_publication(
        &self,
        content_id: &loonfs_api::ContentId,
    ) -> Option<ChangeSeq> {
        self.content_publication_by_id.get(content_id).copied()
    }

    pub(super) fn record_content_publication(&mut self, record: &ContentPublicationRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
        self.content_publication_by_id
            .entry(record.content_id.clone())
            .and_modify(|seq| *seq = (*seq).max(record.committed_seq))
            .or_insert(record.committed_seq);
    }

    pub(super) fn record_commit_receipt(&mut self, record: &CommitReceiptRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
        replace_if_newer(
            &mut self.commit_receipt_by_id,
            record.commit_id.clone(),
            record.clone(),
            |receipt| receipt.committed_seq,
        );
    }

    /// Attribute revisions contribute only the seq watermark, like revisions:
    /// attribute lookups scan the rows, which stay tail-sized in memory
    /// because the manifest segments answer the bulk.
    pub(super) fn record_attributes_revision(&mut self, record: &AttributesRevisionRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
    }

    /// Access revisions contribute only the seq watermark, like revisions:
    /// access lookups scan the rows, which stay tail-sized in memory
    /// because the manifest segments answer the bulk.
    pub(super) fn record_access_revision(&mut self, record: &AccessRevisionRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
    }
}

fn replace_if_newer<K, V, O>(map: &mut HashMap<K, V>, key: K, value: V, order: impl Fn(&V) -> O)
where
    K: Eq + std::hash::Hash,
    O: Ord,
{
    let should_replace = map
        .get(&key)
        .is_none_or(|existing| order(&value) > order(existing));
    if should_replace {
        map.insert(key, value);
    }
}

fn tombstone_order_key(record: &SubtreeTombstoneRecord) -> (ChangeSeq, u32) {
    (record.committed_seq, record.delta_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::{DisplayName, NameKey};

    fn bind(parent: u64, name: &str, child: u64, seq: u64) -> DirentryBindingRecord {
        DirentryBindingRecord {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("valid name key"),
            state: loonfs_api::wire::manifest::DirentryBindingState::Bound {
                display_name: DisplayName::parse(name).expect("valid display name"),
            },
            child_inode_id: InodeId(child),
            committed_seq: ChangeSeq(seq),
            delta_index: 0,
        }
    }

    #[test]
    fn an_out_of_order_bind_does_not_evict_the_childs_newer_binding() {
        let mut indexes = MetadataIndexes::default();
        let newer = bind(2, "renamed", 7, 30);
        let older = bind(1, "original", 7, 10);

        let mut unbound = older.clone();
        unbound.committed_seq = ChangeSeq(20);
        unbound.state = loonfs_api::wire::manifest::DirentryBindingState::Unbound;
        indexes.record_binding(&unbound);
        indexes.record_binding(&newer);
        indexes.record_binding(&older);

        let parent = indexes
            .active_parent_for_child(InodeId(7))
            .expect("the child keeps its newer binding");
        assert_eq!(parent.committed_seq, ChangeSeq(30));
        assert!(indexes
            .active_child(
                InodeId(2),
                &NameKey::parse("renamed").expect("valid name key")
            )
            .is_some());
        assert!(indexes
            .active_child(
                InodeId(1),
                &NameKey::parse("original").expect("valid name key")
            )
            .is_none());
    }

    #[test]
    fn a_stale_bind_does_not_orphan_the_name_slots_current_child() {
        let mut indexes = MetadataIndexes::default();
        indexes.record_binding(&bind(1, "original", 8, 20));
        indexes.record_binding(&bind(1, "renamed", 7, 30));
        indexes.record_binding(&bind(1, "original", 7, 10));

        let seven = indexes
            .active_parent_for_child(InodeId(7))
            .expect("child 7 keeps its newer binding");
        assert_eq!(seven.committed_seq, ChangeSeq(30));
        let eight = indexes
            .active_parent_for_child(InodeId(8))
            .expect("child 8 keeps its binding");
        assert_eq!(eight.committed_seq, ChangeSeq(20));
        let original = indexes
            .active_child(
                InodeId(1),
                &NameKey::parse("original").expect("valid name key"),
            )
            .expect("the name slot still holds child 8");
        assert_eq!(original.child_inode_id, InodeId(8));
    }
}
