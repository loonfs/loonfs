use super::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, RevisionRecord,
    SubtreeTombstoneRecord,
};
use loonfs_api::{ChangeSeq, CommitId, InodeId, RevisionNo};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataIndexes {
    indexed_seq: ChangeSeq,
    inode_by_id: HashMap<InodeId, InodeRecord>,
    active_child_by_parent_name: HashMap<(InodeId, String), DirentryBindRecord>,
    active_parent_by_child: HashMap<InodeId, DirentryBindRecord>,
    unbound_binding_keys: HashSet<BindingKey>,
    tombstone_by_root: HashMap<InodeId, SubtreeTombstoneRecord>,
    latest_revision_by_inode: HashMap<InodeId, RevisionRecord>,
    revision_by_inode_no: HashMap<(InodeId, RevisionNo), RevisionRecord>,
    commit_receipt_by_id: HashMap<CommitId, CommitReceiptRecord>,
}

impl Default for MetadataIndexes {
    fn default() -> Self {
        Self {
            indexed_seq: ChangeSeq(0),
            inode_by_id: HashMap::new(),
            active_child_by_parent_name: HashMap::new(),
            active_parent_by_child: HashMap::new(),
            unbound_binding_keys: HashSet::new(),
            tombstone_by_root: HashMap::new(),
            latest_revision_by_inode: HashMap::new(),
            revision_by_inode_no: HashMap::new(),
            commit_receipt_by_id: HashMap::new(),
        }
    }
}

impl MetadataIndexes {
    pub(super) fn rebuild(
        inodes: &[InodeRecord],
        binds: &[DirentryBindRecord],
        unbinds: &[DirentryUnbindRecord],
        revisions: &[RevisionRecord],
        tombstones: &[SubtreeTombstoneRecord],
        receipts: &[CommitReceiptRecord],
    ) -> Self {
        let mut indexes = Self::default();

        for inode in inodes {
            indexes.record_inode(inode);
        }

        let mut latest_by_parent_name = HashMap::<(InodeId, String), DirentryBindRecord>::new();
        let mut latest_by_child = HashMap::<InodeId, DirentryBindRecord>::new();
        for bind in binds {
            indexes.indexed_seq = indexes.indexed_seq.max(bind.bind_seq);
            replace_if_newer_bind(
                &mut latest_by_parent_name,
                (bind.parent_inode_id, bind.name_key.clone()),
                bind.clone(),
            );
            replace_if_newer_bind(&mut latest_by_child, bind.child_inode_id, bind.clone());
        }

        for unbind in unbinds {
            indexes.record_unbind(unbind);
        }

        for bind in latest_by_parent_name.values() {
            if indexes.is_unbound(bind) {
                continue;
            }
            let Some(latest_child_bind) = latest_by_child.get(&bind.child_inode_id) else {
                continue;
            };
            if !same_binding(bind, latest_child_bind) {
                continue;
            }
            indexes
                .active_child_by_parent_name
                .insert((bind.parent_inode_id, bind.name_key.clone()), bind.clone());
            indexes
                .active_parent_by_child
                .insert(bind.child_inode_id, bind.clone());
        }

        for revision in revisions {
            indexes.record_revision(revision);
        }

        for tombstone in tombstones {
            indexes.record_tombstone(tombstone);
        }

        for receipt in receipts {
            indexes.record_commit_receipt(receipt);
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
        name_key: &str,
    ) -> Option<DirentryBindRecord> {
        self.active_child_by_parent_name
            .get(&(parent_inode_id, name_key.to_owned()))
            .cloned()
    }

    pub(super) fn active_children(&self, parent_inode_id: InodeId) -> Vec<DirentryBindRecord> {
        self.active_child_by_parent_name
            .values()
            .filter(|direntry| direntry.parent_inode_id == parent_inode_id)
            .cloned()
            .collect()
    }

    pub(super) fn active_parent_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Option<DirentryBindRecord> {
        self.active_parent_by_child.get(&child_inode_id).cloned()
    }

    pub(super) fn active_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Option<SubtreeTombstoneRecord> {
        self.tombstone_by_root.get(&root_inode_id).cloned()
    }

    pub(super) fn latest_revision(&self, inode_id: InodeId) -> Option<RevisionRecord> {
        self.latest_revision_by_inode.get(&inode_id).cloned()
    }

    pub(super) fn revision(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Option<RevisionRecord> {
        self.revision_by_inode_no
            .get(&(inode_id, revision_no))
            .cloned()
    }

    pub(super) fn commit_receipt(&self, commit_id: &CommitId) -> Option<&CommitReceiptRecord> {
        self.commit_receipt_by_id.get(commit_id)
    }

    pub(super) fn is_unbound(&self, record: &DirentryBindRecord) -> bool {
        self.unbound_binding_keys
            .contains(&BindingKey::from_bind(record))
    }

    pub(super) fn record_inode(&mut self, record: &InodeRecord) {
        self.indexed_seq = self.indexed_seq.max(record.created_seq);
        self.inode_by_id.insert(record.inode_id, record.clone());
    }

    pub(super) fn record_bind(&mut self, record: &DirentryBindRecord) {
        self.indexed_seq = self.indexed_seq.max(record.bind_seq);
        let parent_name_key = (record.parent_inode_id, record.name_key.clone());

        if let Some(previous_child_at_name) =
            self.active_child_by_parent_name.remove(&parent_name_key)
        {
            remove_active_parent_if_same(&mut self.active_parent_by_child, &previous_child_at_name);
        }

        if let Some(previous_parent_for_child) =
            self.active_parent_by_child.remove(&record.child_inode_id)
        {
            remove_active_child_if_same(
                &mut self.active_child_by_parent_name,
                &previous_parent_for_child,
            );
        }

        if !self.is_unbound(record) {
            self.active_child_by_parent_name
                .insert(parent_name_key, record.clone());
            self.active_parent_by_child
                .insert(record.child_inode_id, record.clone());
        }
    }

    pub(super) fn record_unbind(&mut self, record: &DirentryUnbindRecord) {
        self.indexed_seq = self.indexed_seq.max(record.unbind_seq);
        self.unbound_binding_keys
            .insert(BindingKey::from_unbind(record));

        let parent_name_key = (record.parent_inode_id, record.name_key.clone());
        if self
            .active_child_by_parent_name
            .get(&parent_name_key)
            .map(|active| unbind_matches_bind(record, active))
            .unwrap_or(false)
        {
            self.active_child_by_parent_name.remove(&parent_name_key);
        }
        if self
            .active_parent_by_child
            .get(&record.child_inode_id)
            .map(|active| unbind_matches_bind(record, active))
            .unwrap_or(false)
        {
            self.active_parent_by_child.remove(&record.child_inode_id);
        }
    }

    pub(super) fn record_revision(&mut self, record: &RevisionRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
        replace_if_newer_revision(
            &mut self.latest_revision_by_inode,
            record.inode_id,
            record.clone(),
        );
        replace_if_newer_revision(
            &mut self.revision_by_inode_no,
            (record.inode_id, record.revision_no),
            record.clone(),
        );
    }

    pub(super) fn record_tombstone(&mut self, record: &SubtreeTombstoneRecord) {
        self.indexed_seq = self.indexed_seq.max(record.tombstone_seq);
        replace_if_newer_tombstone(
            &mut self.tombstone_by_root,
            record.root_inode_id,
            record.clone(),
        );
    }

    pub(super) fn record_commit_receipt(&mut self, record: &CommitReceiptRecord) {
        self.indexed_seq = self.indexed_seq.max(record.committed_seq);
        replace_if_newer_receipt(
            &mut self.commit_receipt_by_id,
            record.commit_id.clone(),
            record.clone(),
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BindingKey {
    parent_inode_id: InodeId,
    name_key: String,
    child_inode_id: InodeId,
    bind_seq: ChangeSeq,
    bind_delta_index: u32,
}

impl BindingKey {
    fn from_bind(record: &DirentryBindRecord) -> Self {
        Self {
            parent_inode_id: record.parent_inode_id,
            name_key: record.name_key.clone(),
            child_inode_id: record.child_inode_id,
            bind_seq: record.bind_seq,
            bind_delta_index: record.bind_delta_index,
        }
    }

    fn from_unbind(record: &DirentryUnbindRecord) -> Self {
        Self {
            parent_inode_id: record.parent_inode_id,
            name_key: record.name_key.clone(),
            child_inode_id: record.child_inode_id,
            bind_seq: record.bind_seq,
            bind_delta_index: record.bind_delta_index,
        }
    }
}

fn replace_if_newer_bind<K>(
    map: &mut HashMap<K, DirentryBindRecord>,
    key: K,
    record: DirentryBindRecord,
) where
    K: Eq + std::hash::Hash,
{
    let should_replace = map
        .get(&key)
        .map(|existing| bind_order_key(&record) > bind_order_key(existing))
        .unwrap_or(true);
    if should_replace {
        map.insert(key, record);
    }
}

fn replace_if_newer_revision<K>(
    map: &mut HashMap<K, RevisionRecord>,
    key: K,
    record: RevisionRecord,
) where
    K: Eq + std::hash::Hash,
{
    let should_replace = map
        .get(&key)
        .map(|existing| revision_order_key(&record) > revision_order_key(existing))
        .unwrap_or(true);
    if should_replace {
        map.insert(key, record);
    }
}

fn replace_if_newer_receipt(
    map: &mut HashMap<CommitId, CommitReceiptRecord>,
    key: CommitId,
    record: CommitReceiptRecord,
) {
    let should_replace = map
        .get(&key)
        .map(|existing| record.committed_seq > existing.committed_seq)
        .unwrap_or(true);
    if should_replace {
        map.insert(key, record);
    }
}

fn replace_if_newer_tombstone(
    map: &mut HashMap<InodeId, SubtreeTombstoneRecord>,
    key: InodeId,
    record: SubtreeTombstoneRecord,
) {
    let should_replace = map
        .get(&key)
        .map(|existing| tombstone_order_key(&record) > tombstone_order_key(existing))
        .unwrap_or(true);
    if should_replace {
        map.insert(key, record);
    }
}

fn remove_active_parent_if_same(
    active_parent_by_child: &mut HashMap<InodeId, DirentryBindRecord>,
    record: &DirentryBindRecord,
) {
    if active_parent_by_child
        .get(&record.child_inode_id)
        .map(|active| same_binding(active, record))
        .unwrap_or(false)
    {
        active_parent_by_child.remove(&record.child_inode_id);
    }
}

fn remove_active_child_if_same(
    active_child_by_parent_name: &mut HashMap<(InodeId, String), DirentryBindRecord>,
    record: &DirentryBindRecord,
) {
    let key = (record.parent_inode_id, record.name_key.clone());
    if active_child_by_parent_name
        .get(&key)
        .map(|active| same_binding(active, record))
        .unwrap_or(false)
    {
        active_child_by_parent_name.remove(&key);
    }
}

fn same_binding(left: &DirentryBindRecord, right: &DirentryBindRecord) -> bool {
    left.parent_inode_id == right.parent_inode_id
        && left.name_key == right.name_key
        && left.child_inode_id == right.child_inode_id
        && left.bind_seq == right.bind_seq
        && left.bind_delta_index == right.bind_delta_index
}

fn unbind_matches_bind(unbind: &DirentryUnbindRecord, bind: &DirentryBindRecord) -> bool {
    unbind.parent_inode_id == bind.parent_inode_id
        && unbind.name_key == bind.name_key
        && unbind.child_inode_id == bind.child_inode_id
        && unbind.bind_seq == bind.bind_seq
        && unbind.bind_delta_index == bind.bind_delta_index
}

fn bind_order_key(record: &DirentryBindRecord) -> (ChangeSeq, u32) {
    (record.bind_seq, record.bind_delta_index)
}

fn revision_order_key(record: &RevisionRecord) -> (RevisionNo, ChangeSeq, u32) {
    (
        record.revision_no,
        record.committed_seq,
        record.revision_delta_index,
    )
}

fn tombstone_order_key(record: &SubtreeTombstoneRecord) -> (ChangeSeq, u32) {
    (record.tombstone_seq, record.tombstone_delta_index)
}
