use loon_api::{
    name_key_for_display_name, v0::CommitOpResult, ChangeSeq, ContentRef, InodeId, InodeKind,
    NamePolicy, RevisionNo, WalCommitPayload, WalOp,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MetadataState {
    #[serde(default)]
    pub inodes: Vec<InodeRecord>,
    #[serde(default)]
    pub direntries: Vec<DirentryRecord>,
    #[serde(default)]
    pub revisions: Vec<RevisionRecord>,
    #[serde(default)]
    pub subtree_tombstones: Vec<SubtreeTombstoneRecord>,
    #[serde(default)]
    pub request_receipts: Vec<RequestReceiptRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeRecord {
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub created_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentryRecord {
    pub parent_inode_id: InodeId,
    pub name_key: String,
    pub display_name: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    #[serde(default)]
    pub bind_op_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionRecord {
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
    pub committed_seq: ChangeSeq,
    #[serde(default)]
    pub revision_op_index: u32,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeTombstoneRecord {
    pub root_inode_id: InodeId,
    pub tombstone_seq: ChangeSeq,
    #[serde(default)]
    pub tombstone_op_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestReceiptRecord {
    pub request_id: String,
    pub semantic_fingerprint_sha256: String,
    pub committed_seq: ChangeSeq,
    pub commit_id: String,
    pub results: Vec<CommitOpResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMetadataState {
    pub metadata_state: MetadataState,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedVisiblePath {
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum VisiblePathError {
    #[error("invalid absolute path `{absolute_path}`")]
    InvalidAbsolutePath { absolute_path: String },
    #[error("canonical root inode is missing")]
    RootMissing,
    #[error("visible path not found: `{absolute_path}`")]
    PathNotFound { absolute_path: String },
    #[error(
        "path component traversal expected directory at `{absolute_path}` but found inode `{inode_id:?}` kind `{inode_kind:?}`"
    )]
    PathComponentNotDirectory {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataApplyError {
    RevisionOverflow {
        inode_id: InodeId,
        base_revision: RevisionNo,
    },
}

#[derive(Debug, Clone)]
pub struct IndexedMetadataState {
    head_seq: ChangeSeq,
    metadata_state: MetadataState,
    index: MetadataIndexMaps,
}

#[derive(Debug)]
pub struct MetadataOverlay<'a> {
    base: &'a IndexedMetadataState,
    visible_seq: ChangeSeq,
    rows: MetadataDeltaRows,
    index: MetadataIndexMaps,
}

#[derive(Debug, Clone, Default)]
struct MetadataIndexMaps {
    inode_by_id: HashMap<InodeId, InodeRecord>,
    latest_revision_by_inode: HashMap<InodeId, RevisionRecord>,
    revision_by_inode_no: HashMap<(InodeId, RevisionNo), RevisionRecord>,
    latest_binding_by_parent_name: HashMap<(InodeId, String), DirentryRecord>,
    latest_parent_binding_by_child: HashMap<InodeId, DirentryRecord>,
    tombstone_by_root: HashMap<InodeId, SubtreeTombstoneRecord>,
    request_receipt_by_id: HashMap<String, RequestReceiptRecord>,
}

#[derive(Debug, Clone, Default)]
struct MetadataDeltaRows {
    inodes: Vec<InodeRecord>,
    direntries: Vec<DirentryRecord>,
    revisions: Vec<RevisionRecord>,
    subtree_tombstones: Vec<SubtreeTombstoneRecord>,
    request_receipts: Vec<RequestReceiptRecord>,
}

pub(crate) trait CurrentMetadataView {
    fn head_seq(&self) -> ChangeSeq;
    fn inode(&self, inode_id: InodeId) -> Option<InodeRecord>;
    fn latest_revision_head(&self, inode_id: InodeId) -> Option<RevisionRecord>;
    fn revision(&self, inode_id: InodeId, revision_no: RevisionNo) -> Option<RevisionRecord>;
    fn bound_child(&self, parent_inode_id: InodeId, name_key: &str) -> Option<DirentryRecord>;
    fn current_parent_binding_for_child(&self, child_inode_id: InodeId) -> Option<DirentryRecord>;
    fn active_subtree_tombstone(&self, root_inode_id: InodeId) -> Option<SubtreeTombstoneRecord>;
    fn child_bindings_for_parent(&self, parent_inode_id: InodeId) -> Vec<DirentryRecord>;
    fn request_receipt(&self, request_id: &str) -> Option<RequestReceiptRecord>;

    fn covering_subtree_tombstone(&self, inode_id: InodeId) -> Option<SubtreeTombstoneRecord> {
        let mut current = Some(inode_id);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }

            if let Some(tombstone) = self.active_subtree_tombstone(candidate_inode_id) {
                return Some(tombstone);
            }

            current = self
                .current_parent_binding_for_child(candidate_inode_id)
                .map(|direntry| direntry.parent_inode_id);
        }

        None
    }

    fn visible_inode(&self, inode_id: InodeId) -> Option<InodeRecord> {
        let inode = self.inode(inode_id)?;
        if self.covering_subtree_tombstone(inode_id).is_some() {
            return None;
        }

        Some(inode)
    }

    fn current_revision_head(&self, inode_id: InodeId) -> Option<RevisionRecord> {
        self.visible_inode(inode_id)?;
        self.latest_revision_head(inode_id)
    }

    fn active_child_binding(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Option<DirentryRecord> {
        let direntry = self.bound_child(parent_inode_id, name_key)?;
        let latest_binding = self.current_parent_binding_for_child(direntry.child_inode_id)?;
        if latest_binding.parent_inode_id != direntry.parent_inode_id
            || latest_binding.name_key != direntry.name_key
            || latest_binding.bind_seq != direntry.bind_seq
            || latest_binding.bind_op_index != direntry.bind_op_index
        {
            return None;
        }

        Some(direntry)
    }

    fn visible_child(&self, parent_inode_id: InodeId, name_key: &str) -> Option<DirentryRecord> {
        let parent = self.visible_inode(parent_inode_id)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.active_child_binding(parent_inode_id, name_key)?;
        self.visible_inode(direntry.child_inode_id)?;
        Some(direntry)
    }

    fn visible_children(&self, parent_inode_id: InodeId) -> Vec<DirentryRecord> {
        let Some(parent) = self.visible_inode(parent_inode_id) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = self
            .child_bindings_for_parent(parent_inode_id)
            .into_iter()
            .filter(|direntry| {
                self.active_child_binding(parent_inode_id, &direntry.name_key)
                    .map(|active| {
                        active.child_inode_id == direntry.child_inode_id
                            && active.bind_seq == direntry.bind_seq
                            && active.bind_op_index == direntry.bind_op_index
                    })
                    .unwrap_or(false)
            })
            .filter(|direntry| self.visible_inode(direntry.child_inode_id).is_some())
            .collect::<Vec<_>>();
        children.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.child_inode_id.0.cmp(&right.child_inode_id.0))
        });
        children
    }

    fn resolve_visible_path(
        &self,
        absolute_path: &str,
    ) -> Result<ResolvedVisiblePath, VisiblePathError> {
        let components = parse_absolute_path_components(absolute_path)?;
        let root_inode_id = InodeId(1);
        let root = self
            .visible_inode(root_inode_id)
            .ok_or(VisiblePathError::RootMissing)?;
        if components.is_empty() {
            return Ok(ResolvedVisiblePath {
                absolute_path: "/".to_owned(),
                inode_id: root_inode_id,
                inode_kind: root.inode_kind,
                parent_inode_id: None,
                display_name: String::new(),
            });
        }

        let mut current_inode_id = root_inode_id;
        let mut current_absolute_path = "/".to_owned();
        let mut current_parent_inode_id = None;
        let mut current_display_name = String::new();

        for component in components {
            let current_inode =
                self.visible_inode(current_inode_id)
                    .ok_or(VisiblePathError::PathNotFound {
                        absolute_path: current_absolute_path.clone(),
                    })?;
            if current_inode.inode_kind != InodeKind::Dir {
                return Err(VisiblePathError::PathComponentNotDirectory {
                    absolute_path: current_absolute_path,
                    inode_id: current_inode_id,
                    inode_kind: current_inode.inode_kind,
                });
            }

            let requested_absolute_path = join_absolute_path(&current_absolute_path, &component);
            let direntry = self.visible_child(current_inode_id, &component).ok_or(
                VisiblePathError::PathNotFound {
                    absolute_path: requested_absolute_path,
                },
            )?;
            current_inode_id = direntry.child_inode_id;
            current_parent_inode_id = Some(direntry.parent_inode_id);
            current_display_name = direntry.display_name.clone();
            current_absolute_path =
                join_absolute_path(&current_absolute_path, &direntry.display_name);
        }

        let inode =
            self.visible_inode(current_inode_id)
                .ok_or_else(|| VisiblePathError::PathNotFound {
                    absolute_path: current_absolute_path.clone(),
                })?;
        Ok(ResolvedVisiblePath {
            absolute_path: current_absolute_path,
            inode_id: current_inode_id,
            inode_kind: inode.inode_kind,
            parent_inode_id: current_parent_inode_id,
            display_name: current_display_name,
        })
    }

    fn would_create_directory_cycle(&self, inode_id: InodeId, new_parent_inode: InodeId) -> bool {
        let mut current = Some(new_parent_inode);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if candidate_inode_id == inode_id {
                return true;
            }
            current = self
                .current_parent_binding_for_child(candidate_inode_id)
                .map(|direntry| direntry.parent_inode_id);
        }

        false
    }
}

impl IndexedMetadataState {
    pub fn from_metadata_state(metadata_state: MetadataState, head_seq: ChangeSeq) -> Self {
        let mut index = MetadataIndexMaps::default();

        for inode in metadata_state
            .inodes
            .iter()
            .filter(|inode| inode.created_seq <= head_seq)
        {
            index.index_inode(inode.clone());
        }
        for direntry in metadata_state
            .direntries
            .iter()
            .filter(|direntry| direntry.bind_seq <= head_seq)
        {
            index.index_direntry(direntry.clone());
        }
        for revision in metadata_state
            .revisions
            .iter()
            .filter(|revision| revision.committed_seq <= head_seq)
        {
            index.index_revision(revision.clone());
        }
        for tombstone in metadata_state
            .subtree_tombstones
            .iter()
            .filter(|tombstone| tombstone.tombstone_seq <= head_seq)
        {
            index.index_tombstone(tombstone.clone());
        }
        for receipt in metadata_state
            .request_receipts
            .iter()
            .filter(|receipt| receipt.committed_seq <= head_seq)
        {
            index.index_request_receipt(receipt.clone());
        }

        Self {
            head_seq,
            metadata_state,
            index,
        }
    }

    pub fn metadata_state(&self) -> &MetadataState {
        &self.metadata_state
    }

    pub fn into_metadata_state(self) -> MetadataState {
        self.metadata_state
    }

    pub fn apply_committed_wal_record_in_place(
        &mut self,
        record: &WalCommitPayload,
    ) -> Result<Vec<String>, MetadataApplyError> {
        let mut overlay = MetadataOverlay::new(self, record.seq);
        let mut checked_invariants = Vec::new();
        for op in &record.ops {
            for invariant in overlay.apply_committed_wal_op(record.seq, op)? {
                push_unique_invariant(&mut checked_invariants, &invariant);
            }
        }
        overlay.push_request_receipt(RequestReceiptRecord {
            request_id: record.request_id.clone(),
            semantic_fingerprint_sha256: record.semantic_fingerprint_sha256.clone(),
            committed_seq: record.seq,
            commit_id: record.commit_id.clone(),
            results: record.results.clone(),
        });
        push_unique_invariant(
            &mut checked_invariants,
            "wal_replay_records_request_receipt",
        );

        let rows = overlay.into_rows();
        self.append_rows(rows);
        self.head_seq = record.seq;
        Ok(checked_invariants)
    }

    fn append_rows(&mut self, rows: MetadataDeltaRows) {
        for inode in rows.inodes {
            self.index.index_inode(inode.clone());
            self.metadata_state.inodes.push(inode);
        }
        for direntry in rows.direntries {
            self.index.index_direntry(direntry.clone());
            self.metadata_state.direntries.push(direntry);
        }
        for revision in rows.revisions {
            self.index.index_revision(revision.clone());
            self.metadata_state.revisions.push(revision);
        }
        for tombstone in rows.subtree_tombstones {
            self.index.index_tombstone(tombstone.clone());
            self.metadata_state.subtree_tombstones.push(tombstone);
        }
        for receipt in rows.request_receipts {
            self.index.index_request_receipt(receipt.clone());
            self.metadata_state.request_receipts.push(receipt);
        }
    }
}

impl<'a> MetadataOverlay<'a> {
    pub fn new(base: &'a IndexedMetadataState, visible_seq: ChangeSeq) -> Self {
        Self {
            base,
            visible_seq,
            rows: MetadataDeltaRows::default(),
            index: MetadataIndexMaps::default(),
        }
    }

    pub fn apply_committed_wal_op(
        &mut self,
        committed_seq: ChangeSeq,
        op: &WalOp,
    ) -> Result<Vec<String>, MetadataApplyError> {
        let mut checked_invariants = Vec::new();
        match op {
            WalOp::CreateDir {
                op_index,
                inode_id,
                parent_inode,
                display_name,
            } => {
                self.push_inode(InodeRecord {
                    inode_id: *inode_id,
                    inode_kind: InodeKind::Dir,
                    created_seq: committed_seq,
                });
                self.push_direntry(DirentryRecord {
                    parent_inode_id: *parent_inode,
                    name_key: name_key_for_display_name(NamePolicy::default(), display_name),
                    display_name: display_name.clone(),
                    child_inode_id: *inode_id,
                    bind_seq: committed_seq,
                    bind_op_index: *op_index,
                });
                push_unique_invariant(
                    &mut checked_invariants,
                    "create_dir_writes_inode_and_direntry_rows",
                );
            }
            WalOp::CreateFile {
                op_index,
                inode_id,
                parent_inode,
                display_name,
                content_ref,
            } => {
                self.push_inode(InodeRecord {
                    inode_id: *inode_id,
                    inode_kind: InodeKind::File,
                    created_seq: committed_seq,
                });
                self.push_direntry(DirentryRecord {
                    parent_inode_id: *parent_inode,
                    name_key: name_key_for_display_name(NamePolicy::default(), display_name),
                    display_name: display_name.clone(),
                    child_inode_id: *inode_id,
                    bind_seq: committed_seq,
                    bind_op_index: *op_index,
                });
                self.push_revision(RevisionRecord {
                    inode_id: *inode_id,
                    revision_no: RevisionNo(1),
                    committed_seq,
                    revision_op_index: *op_index,
                    content_ref: content_ref.clone(),
                });
                push_unique_invariant(
                    &mut checked_invariants,
                    "create_file_writes_inode_direntry_and_initial_revision",
                );
            }
            WalOp::ReplaceFile {
                op_index,
                inode_id,
                base_revision,
                content_ref,
            } => {
                let next_revision = base_revision.0.checked_add(1).map(RevisionNo).ok_or(
                    MetadataApplyError::RevisionOverflow {
                        inode_id: *inode_id,
                        base_revision: *base_revision,
                    },
                )?;
                self.push_revision(RevisionRecord {
                    inode_id: *inode_id,
                    revision_no: next_revision,
                    committed_seq,
                    revision_op_index: *op_index,
                    content_ref: content_ref.clone(),
                });
                push_unique_invariant(
                    &mut checked_invariants,
                    "replace_file_appends_new_revision_head",
                );
            }
            WalOp::RestoreRevision {
                op_index,
                inode_id,
                base_revision,
                content_ref,
                ..
            } => {
                let next_revision = base_revision.0.checked_add(1).map(RevisionNo).ok_or(
                    MetadataApplyError::RevisionOverflow {
                        inode_id: *inode_id,
                        base_revision: *base_revision,
                    },
                )?;
                self.push_revision(RevisionRecord {
                    inode_id: *inode_id,
                    revision_no: next_revision,
                    committed_seq,
                    revision_op_index: *op_index,
                    content_ref: content_ref.clone(),
                });
                push_unique_invariant(&mut checked_invariants, "restore_creates_new_revision_head");
            }
            WalOp::DeleteFile { op_index, inode_id } => {
                self.push_tombstone(SubtreeTombstoneRecord {
                    root_inode_id: *inode_id,
                    tombstone_seq: committed_seq,
                    tombstone_op_index: *op_index,
                });
                push_unique_invariant(&mut checked_invariants, "delete_file_writes_tombstone_row");
            }
            WalOp::Rename {
                op_index,
                inode_id,
                new_parent_inode,
                new_display_name,
            } => {
                self.push_direntry(DirentryRecord {
                    parent_inode_id: *new_parent_inode,
                    name_key: name_key_for_display_name(NamePolicy::default(), new_display_name),
                    display_name: new_display_name.clone(),
                    child_inode_id: *inode_id,
                    bind_seq: committed_seq,
                    bind_op_index: *op_index,
                });
                push_unique_invariant(
                    &mut checked_invariants,
                    "rename_appends_new_direntry_binding",
                );
            }
            WalOp::DeleteSubtree {
                op_index,
                root_inode,
            } => {
                self.push_tombstone(SubtreeTombstoneRecord {
                    root_inode_id: *root_inode,
                    tombstone_seq: committed_seq,
                    tombstone_op_index: *op_index,
                });
                push_unique_invariant(
                    &mut checked_invariants,
                    "delete_subtree_writes_tombstone_row",
                );
            }
        }
        Ok(checked_invariants)
    }

    fn push_inode(&mut self, inode: InodeRecord) {
        self.index.index_inode(inode.clone());
        self.rows.inodes.push(inode);
    }

    fn push_direntry(&mut self, direntry: DirentryRecord) {
        self.index.index_direntry(direntry.clone());
        self.rows.direntries.push(direntry);
    }

    fn push_revision(&mut self, revision: RevisionRecord) {
        self.index.index_revision(revision.clone());
        self.rows.revisions.push(revision);
    }

    fn push_tombstone(&mut self, tombstone: SubtreeTombstoneRecord) {
        self.index.index_tombstone(tombstone.clone());
        self.rows.subtree_tombstones.push(tombstone);
    }

    fn push_request_receipt(&mut self, receipt: RequestReceiptRecord) {
        self.index.index_request_receipt(receipt.clone());
        self.rows.request_receipts.push(receipt);
    }

    fn into_rows(self) -> MetadataDeltaRows {
        self.rows
    }
}

impl MetadataIndexMaps {
    fn index_inode(&mut self, inode: InodeRecord) {
        self.inode_by_id.entry(inode.inode_id).or_insert(inode);
    }

    fn index_direntry(&mut self, direntry: DirentryRecord) {
        let parent_name = (direntry.parent_inode_id, direntry.name_key.clone());
        if self
            .latest_binding_by_parent_name
            .get(&parent_name)
            .map(|existing| direntry_is_newer(&direntry, existing))
            .unwrap_or(true)
        {
            self.latest_binding_by_parent_name
                .insert(parent_name, direntry.clone());
        }

        if self
            .latest_parent_binding_by_child
            .get(&direntry.child_inode_id)
            .map(|existing| direntry_is_newer(&direntry, existing))
            .unwrap_or(true)
        {
            self.latest_parent_binding_by_child
                .insert(direntry.child_inode_id, direntry);
        }
    }

    fn index_revision(&mut self, revision: RevisionRecord) {
        let revision_key = (revision.inode_id, revision.revision_no);
        if self
            .revision_by_inode_no
            .get(&revision_key)
            .map(|existing| revision_instance_is_newer(&revision, existing))
            .unwrap_or(true)
        {
            self.revision_by_inode_no
                .insert(revision_key, revision.clone());
        }

        if self
            .latest_revision_by_inode
            .get(&revision.inode_id)
            .map(|existing| revision_head_is_newer(&revision, existing))
            .unwrap_or(true)
        {
            self.latest_revision_by_inode
                .insert(revision.inode_id, revision);
        }
    }

    fn index_tombstone(&mut self, tombstone: SubtreeTombstoneRecord) {
        if self
            .tombstone_by_root
            .get(&tombstone.root_inode_id)
            .map(|existing| tombstone_is_newer(&tombstone, existing))
            .unwrap_or(true)
        {
            self.tombstone_by_root
                .insert(tombstone.root_inode_id, tombstone);
        }
    }

    fn index_request_receipt(&mut self, receipt: RequestReceiptRecord) {
        if self
            .request_receipt_by_id
            .get(&receipt.request_id)
            .map(|existing| receipt.committed_seq > existing.committed_seq)
            .unwrap_or(true)
        {
            self.request_receipt_by_id
                .insert(receipt.request_id.clone(), receipt);
        }
    }
}

impl CurrentMetadataView for IndexedMetadataState {
    fn head_seq(&self) -> ChangeSeq {
        self.head_seq
    }

    fn inode(&self, inode_id: InodeId) -> Option<InodeRecord> {
        self.index.inode_by_id.get(&inode_id).cloned()
    }

    fn latest_revision_head(&self, inode_id: InodeId) -> Option<RevisionRecord> {
        self.index.latest_revision_by_inode.get(&inode_id).cloned()
    }

    fn revision(&self, inode_id: InodeId, revision_no: RevisionNo) -> Option<RevisionRecord> {
        self.index
            .revision_by_inode_no
            .get(&(inode_id, revision_no))
            .cloned()
    }

    fn bound_child(&self, parent_inode_id: InodeId, name_key: &str) -> Option<DirentryRecord> {
        let canonical_name_key = name_key_for_display_name(NamePolicy::default(), name_key);
        self.index
            .latest_binding_by_parent_name
            .get(&(parent_inode_id, canonical_name_key))
            .cloned()
    }

    fn current_parent_binding_for_child(&self, child_inode_id: InodeId) -> Option<DirentryRecord> {
        self.index
            .latest_parent_binding_by_child
            .get(&child_inode_id)
            .cloned()
    }

    fn active_subtree_tombstone(&self, root_inode_id: InodeId) -> Option<SubtreeTombstoneRecord> {
        self.index.tombstone_by_root.get(&root_inode_id).cloned()
    }

    fn child_bindings_for_parent(&self, parent_inode_id: InodeId) -> Vec<DirentryRecord> {
        self.index
            .latest_binding_by_parent_name
            .values()
            .filter(|direntry| direntry.parent_inode_id == parent_inode_id)
            .cloned()
            .collect()
    }

    fn request_receipt(&self, request_id: &str) -> Option<RequestReceiptRecord> {
        self.index.request_receipt_by_id.get(request_id).cloned()
    }
}

impl CurrentMetadataView for MetadataOverlay<'_> {
    fn head_seq(&self) -> ChangeSeq {
        self.visible_seq
    }

    fn inode(&self, inode_id: InodeId) -> Option<InodeRecord> {
        self.index
            .inode_by_id
            .get(&inode_id)
            .cloned()
            .or_else(|| self.base.inode(inode_id))
    }

    fn latest_revision_head(&self, inode_id: InodeId) -> Option<RevisionRecord> {
        match (
            self.index.latest_revision_by_inode.get(&inode_id),
            self.base.latest_revision_head(inode_id),
        ) {
            (Some(overlay), Some(base)) if revision_head_is_newer(&base, overlay) => Some(base),
            (Some(overlay), _) => Some(overlay.clone()),
            (None, base) => base,
        }
    }

    fn revision(&self, inode_id: InodeId, revision_no: RevisionNo) -> Option<RevisionRecord> {
        match (
            self.index
                .revision_by_inode_no
                .get(&(inode_id, revision_no)),
            self.base.revision(inode_id, revision_no),
        ) {
            (Some(overlay), Some(base)) if revision_instance_is_newer(&base, overlay) => Some(base),
            (Some(overlay), _) => Some(overlay.clone()),
            (None, base) => base,
        }
    }

    fn bound_child(&self, parent_inode_id: InodeId, name_key: &str) -> Option<DirentryRecord> {
        let canonical_name_key = name_key_for_display_name(NamePolicy::default(), name_key);
        match (
            self.index
                .latest_binding_by_parent_name
                .get(&(parent_inode_id, canonical_name_key.clone())),
            self.base.bound_child(parent_inode_id, &canonical_name_key),
        ) {
            (Some(overlay), Some(base)) if direntry_is_newer(&base, overlay) => Some(base),
            (Some(overlay), _) => Some(overlay.clone()),
            (None, base) => base,
        }
    }

    fn current_parent_binding_for_child(&self, child_inode_id: InodeId) -> Option<DirentryRecord> {
        match (
            self.index
                .latest_parent_binding_by_child
                .get(&child_inode_id),
            self.base.current_parent_binding_for_child(child_inode_id),
        ) {
            (Some(overlay), Some(base)) if direntry_is_newer(&base, overlay) => Some(base),
            (Some(overlay), _) => Some(overlay.clone()),
            (None, base) => base,
        }
    }

    fn active_subtree_tombstone(&self, root_inode_id: InodeId) -> Option<SubtreeTombstoneRecord> {
        match (
            self.index.tombstone_by_root.get(&root_inode_id),
            self.base.active_subtree_tombstone(root_inode_id),
        ) {
            (Some(overlay), Some(base)) if tombstone_is_newer(&base, overlay) => Some(base),
            (Some(overlay), _) => Some(overlay.clone()),
            (None, base) => base,
        }
    }

    fn child_bindings_for_parent(&self, parent_inode_id: InodeId) -> Vec<DirentryRecord> {
        let mut by_parent_name = self
            .base
            .child_bindings_for_parent(parent_inode_id)
            .into_iter()
            .map(|direntry| {
                (
                    (direntry.parent_inode_id, direntry.name_key.clone()),
                    direntry,
                )
            })
            .collect::<HashMap<_, _>>();
        for direntry in self
            .index
            .latest_binding_by_parent_name
            .values()
            .filter(|direntry| direntry.parent_inode_id == parent_inode_id)
        {
            let key = (direntry.parent_inode_id, direntry.name_key.clone());
            if by_parent_name
                .get(&key)
                .map(|existing| direntry_is_newer(direntry, existing))
                .unwrap_or(true)
            {
                by_parent_name.insert(key, direntry.clone());
            }
        }
        by_parent_name.into_values().collect()
    }

    fn request_receipt(&self, request_id: &str) -> Option<RequestReceiptRecord> {
        match (
            self.index.request_receipt_by_id.get(request_id),
            self.base.request_receipt(request_id),
        ) {
            (Some(overlay), Some(base)) if base.committed_seq > overlay.committed_seq => Some(base),
            (Some(overlay), _) => Some(overlay.clone()),
            (None, base) => base,
        }
    }
}

fn direntry_is_newer(candidate: &DirentryRecord, existing: &DirentryRecord) -> bool {
    (candidate.bind_seq, candidate.bind_op_index) > (existing.bind_seq, existing.bind_op_index)
}

fn revision_head_is_newer(candidate: &RevisionRecord, existing: &RevisionRecord) -> bool {
    (
        candidate.revision_no,
        candidate.committed_seq,
        candidate.revision_op_index,
    ) > (
        existing.revision_no,
        existing.committed_seq,
        existing.revision_op_index,
    )
}

fn revision_instance_is_newer(candidate: &RevisionRecord, existing: &RevisionRecord) -> bool {
    (candidate.committed_seq, candidate.revision_op_index)
        > (existing.committed_seq, existing.revision_op_index)
}

fn tombstone_is_newer(
    candidate: &SubtreeTombstoneRecord,
    existing: &SubtreeTombstoneRecord,
) -> bool {
    (candidate.tombstone_seq, candidate.tombstone_op_index)
        > (existing.tombstone_seq, existing.tombstone_op_index)
}

impl MetadataState {
    pub fn apply_committed_wal_ops(
        &self,
        committed_seq: ChangeSeq,
        ops: &[WalOp],
    ) -> Result<AppliedMetadataState, MetadataApplyError> {
        let mut metadata_state = self.clone();
        let mut checked_invariants = Vec::new();

        for op in ops {
            match op {
                WalOp::CreateDir {
                    op_index,
                    inode_id,
                    parent_inode,
                    display_name,
                } => {
                    metadata_state.inodes.push(InodeRecord {
                        inode_id: *inode_id,
                        inode_kind: InodeKind::Dir,
                        created_seq: committed_seq,
                    });
                    metadata_state.direntries.push(DirentryRecord {
                        parent_inode_id: *parent_inode,
                        name_key: name_key_for_display_name(NamePolicy::default(), display_name),
                        display_name: display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                        bind_op_index: *op_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_dir_writes_inode_and_direntry_rows",
                    );
                }
                WalOp::CreateFile {
                    op_index,
                    inode_id,
                    parent_inode,
                    display_name,
                    content_ref,
                } => {
                    metadata_state.inodes.push(InodeRecord {
                        inode_id: *inode_id,
                        inode_kind: InodeKind::File,
                        created_seq: committed_seq,
                    });
                    metadata_state.direntries.push(DirentryRecord {
                        parent_inode_id: *parent_inode,
                        name_key: name_key_for_display_name(NamePolicy::default(), display_name),
                        display_name: display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                        bind_op_index: *op_index,
                    });
                    metadata_state.revisions.push(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: RevisionNo(1),
                        committed_seq,
                        revision_op_index: *op_index,
                        content_ref: content_ref.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_file_writes_inode_direntry_and_initial_revision",
                    );
                }
                WalOp::ReplaceFile {
                    op_index,
                    inode_id,
                    base_revision,
                    content_ref,
                } => {
                    let next_revision = base_revision.0.checked_add(1).map(RevisionNo).ok_or(
                        MetadataApplyError::RevisionOverflow {
                            inode_id: *inode_id,
                            base_revision: *base_revision,
                        },
                    )?;
                    metadata_state.revisions.push(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: next_revision,
                        committed_seq,
                        revision_op_index: *op_index,
                        content_ref: content_ref.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "replace_file_appends_new_revision_head",
                    );
                }
                WalOp::RestoreRevision {
                    op_index,
                    inode_id,
                    base_revision,
                    content_ref,
                    ..
                } => {
                    let next_revision = base_revision.0.checked_add(1).map(RevisionNo).ok_or(
                        MetadataApplyError::RevisionOverflow {
                            inode_id: *inode_id,
                            base_revision: *base_revision,
                        },
                    )?;
                    metadata_state.revisions.push(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: next_revision,
                        committed_seq,
                        revision_op_index: *op_index,
                        content_ref: content_ref.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "restore_creates_new_revision_head",
                    );
                }
                WalOp::DeleteFile { op_index, inode_id } => {
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *inode_id,
                            tombstone_seq: committed_seq,
                            tombstone_op_index: *op_index,
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "delete_file_writes_tombstone_row",
                    );
                }
                WalOp::Rename {
                    op_index,
                    inode_id,
                    new_parent_inode,
                    new_display_name,
                } => {
                    metadata_state.direntries.push(DirentryRecord {
                        parent_inode_id: *new_parent_inode,
                        name_key: name_key_for_display_name(
                            NamePolicy::default(),
                            new_display_name,
                        ),
                        display_name: new_display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                        bind_op_index: *op_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "rename_appends_new_direntry_binding",
                    );
                }
                WalOp::DeleteSubtree { .. } => {
                    let WalOp::DeleteSubtree {
                        op_index,
                        root_inode,
                    } = op
                    else {
                        unreachable!();
                    };
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *root_inode,
                            tombstone_seq: committed_seq,
                            tombstone_op_index: *op_index,
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "delete_subtree_writes_tombstone_row",
                    );
                }
            }
        }

        Ok(AppliedMetadataState {
            metadata_state,
            checked_invariants,
        })
    }

    pub fn apply_committed_wal_record(
        &self,
        record: &WalCommitPayload,
    ) -> Result<AppliedMetadataState, MetadataApplyError> {
        let mut applied = self.apply_committed_wal_ops(record.seq, &record.ops)?;
        applied
            .metadata_state
            .request_receipts
            .push(RequestReceiptRecord {
                request_id: record.request_id.clone(),
                semantic_fingerprint_sha256: record.semantic_fingerprint_sha256.clone(),
                committed_seq: record.seq,
                commit_id: record.commit_id.clone(),
                results: record.results.clone(),
            });
        push_unique_invariant(
            &mut applied.checked_invariants,
            "wal_replay_records_request_receipt",
        );
        Ok(applied)
    }

    pub fn inode_at_seq(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        self.inodes
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.created_seq <= base_seq)
            .cloned()
    }

    pub fn latest_revision_head_at_seq(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.revisions
            .iter()
            .filter(|revision| revision.inode_id == inode_id && revision.committed_seq <= base_seq)
            .max_by_key(|revision| {
                (
                    revision.revision_no,
                    revision.committed_seq,
                    revision.revision_op_index,
                )
            })
            .cloned()
    }

    pub fn revision_at_seq(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.revisions
            .iter()
            .filter(|revision| {
                revision.inode_id == inode_id
                    && revision.revision_no == revision_no
                    && revision.committed_seq <= base_seq
            })
            .max_by_key(|revision| (revision.committed_seq, revision.revision_op_index))
            .cloned()
    }

    pub fn bound_child_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryRecord> {
        let canonical_name_key = name_key_for_display_name(NamePolicy::default(), name_key);
        self.direntries
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && direntry.name_key == canonical_name_key
                    && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_op_index))
            .cloned()
    }

    pub fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryRecord> {
        self.latest_parent_binding_for_child_at_seq(child_inode_id, base_seq)
    }

    pub fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        self.subtree_tombstones
            .iter()
            .filter(|tombstone| {
                tombstone.root_inode_id == root_inode_id && tombstone.tombstone_seq <= base_seq
            })
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_op_index))
            .cloned()
    }

    pub fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        let mut current = Some(inode_id);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }

            if let Some(tombstone) = self.active_subtree_tombstone(candidate_inode_id, base_seq) {
                return Some(tombstone);
            }

            current = self
                .latest_parent_binding_for_child_at_seq(candidate_inode_id, base_seq)
                .map(|direntry| direntry.parent_inode_id);
        }

        None
    }

    pub fn visible_inode(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        let inode = self.inode_at_seq(inode_id, base_seq)?;
        if self
            .covering_subtree_tombstone(inode_id, base_seq)
            .is_some()
        {
            return None;
        }

        Some(inode)
    }

    pub fn current_revision_head(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.visible_inode(inode_id, base_seq)?;
        self.latest_revision_head_at_seq(inode_id, base_seq)
    }

    pub fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryRecord> {
        let parent = self.visible_inode(parent_inode_id, base_seq)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.active_child_binding_at_seq(parent_inode_id, name_key, base_seq)?;
        self.visible_inode(direntry.child_inode_id, base_seq)?;
        Some(direntry)
    }

    pub fn visible_children(
        &self,
        parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Vec<DirentryRecord> {
        let Some(parent) = self.visible_inode(parent_inode_id, base_seq) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = self
            .direntries
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id && direntry.bind_seq <= base_seq
            })
            .filter(|direntry| {
                self.active_child_binding_at_seq(parent_inode_id, &direntry.name_key, base_seq)
                    .map(|active| {
                        active.child_inode_id == direntry.child_inode_id
                            && active.bind_seq == direntry.bind_seq
                            && active.bind_op_index == direntry.bind_op_index
                    })
                    .unwrap_or(false)
            })
            .filter(|direntry| {
                self.visible_inode(direntry.child_inode_id, base_seq)
                    .is_some()
            })
            .cloned()
            .collect::<Vec<_>>();
        children.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.child_inode_id.0.cmp(&right.child_inode_id.0))
        });
        children
    }

    pub fn resolve_visible_path(
        &self,
        absolute_path: &str,
        base_seq: ChangeSeq,
    ) -> Result<ResolvedVisiblePath, VisiblePathError> {
        let components = parse_absolute_path_components(absolute_path)?;
        let root_inode_id = InodeId(1);
        let root = self
            .visible_inode(root_inode_id, base_seq)
            .ok_or(VisiblePathError::RootMissing)?;
        if components.is_empty() {
            return Ok(ResolvedVisiblePath {
                absolute_path: "/".to_owned(),
                inode_id: root_inode_id,
                inode_kind: root.inode_kind,
                parent_inode_id: None,
                display_name: String::new(),
            });
        }

        let mut current_inode_id = root_inode_id;
        let mut current_absolute_path = "/".to_owned();
        let mut current_parent_inode_id = None;
        let mut current_display_name = String::new();

        for component in components {
            let current_inode = self.visible_inode(current_inode_id, base_seq).ok_or(
                VisiblePathError::PathNotFound {
                    absolute_path: current_absolute_path.clone(),
                },
            )?;
            if current_inode.inode_kind != InodeKind::Dir {
                return Err(VisiblePathError::PathComponentNotDirectory {
                    absolute_path: current_absolute_path,
                    inode_id: current_inode_id,
                    inode_kind: current_inode.inode_kind,
                });
            }

            let requested_absolute_path = join_absolute_path(&current_absolute_path, &component);
            let direntry = self
                .visible_child(current_inode_id, &component, base_seq)
                .ok_or(VisiblePathError::PathNotFound {
                    absolute_path: requested_absolute_path,
                })?;
            current_inode_id = direntry.child_inode_id;
            current_parent_inode_id = Some(direntry.parent_inode_id);
            current_display_name = direntry.display_name.clone();
            current_absolute_path =
                join_absolute_path(&current_absolute_path, &direntry.display_name);
        }

        let inode = self
            .visible_inode(current_inode_id, base_seq)
            .ok_or_else(|| VisiblePathError::PathNotFound {
                absolute_path: current_absolute_path.clone(),
            })?;
        Ok(ResolvedVisiblePath {
            absolute_path: current_absolute_path,
            inode_id: current_inode_id,
            inode_kind: inode.inode_kind,
            parent_inode_id: current_parent_inode_id,
            display_name: current_display_name,
        })
    }

    fn active_child_binding_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryRecord> {
        let direntry = self.bound_child_at_seq(parent_inode_id, name_key, base_seq)?;
        let latest_binding =
            self.latest_parent_binding_for_child_at_seq(direntry.child_inode_id, base_seq)?;
        if latest_binding.parent_inode_id != direntry.parent_inode_id
            || latest_binding.name_key != direntry.name_key
            || latest_binding.bind_seq != direntry.bind_seq
            || latest_binding.bind_op_index != direntry.bind_op_index
        {
            return None;
        }

        Some(direntry)
    }

    fn latest_parent_binding_for_child_at_seq(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryRecord> {
        self.direntries
            .iter()
            .filter(|direntry| {
                direntry.child_inode_id == child_inode_id && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_op_index))
            .cloned()
    }

    pub fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode: InodeId,
        base_seq: ChangeSeq,
    ) -> bool {
        let mut current = Some(new_parent_inode);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if candidate_inode_id == inode_id {
                return true;
            }
            current = self
                .latest_parent_binding_for_child_at_seq(candidate_inode_id, base_seq)
                .map(|direntry| direntry.parent_inode_id);
        }

        false
    }
}

fn push_unique_invariant(invariants: &mut Vec<String>, name: &str) {
    if !invariants.iter().any(|existing| existing == name) {
        invariants.push(name.to_owned());
    }
}

fn parse_absolute_path_components(absolute_path: &str) -> Result<Vec<String>, VisiblePathError> {
    if !absolute_path.starts_with('/') {
        return Err(VisiblePathError::InvalidAbsolutePath {
            absolute_path: absolute_path.to_owned(),
        });
    }

    let mut components = Vec::new();
    for component in absolute_path.split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." {
            return Err(VisiblePathError::InvalidAbsolutePath {
                absolute_path: absolute_path.to_owned(),
            });
        }
        components.push(component.to_owned());
    }
    Ok(components)
}

fn join_absolute_path(base: &str, component: &str) -> String {
    if base == "/" {
        format!("/{component}")
    } else {
        format!("{base}/{component}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loon_api::{FenceToken, NamespaceId};

    fn content(bytes: &[u8]) -> ContentRef {
        ContentRef::whole_file_v0(bytes)
    }

    fn root_state() -> MetadataState {
        MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(0),
            }],
            direntries: Vec::new(),
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
            request_receipts: Vec::new(),
        }
    }

    fn state_after(sequences: &[Vec<WalOp>]) -> MetadataState {
        let mut state = root_state();
        for (offset, ops) in sequences.iter().enumerate() {
            state = state
                .apply_committed_wal_ops(ChangeSeq(offset as u64 + 1), ops)
                .expect("test WAL ops should apply")
                .metadata_state;
        }
        state
    }

    fn wal_record(seq: ChangeSeq, request_id: &str, ops: Vec<WalOp>) -> WalCommitPayload {
        WalCommitPayload {
            namespace_id: NamespaceId::from("test".to_owned()),
            seq,
            base_head_seq: ChangeSeq(seq.0.saturating_sub(1)),
            commit_id: request_id.to_owned(),
            request_id: request_id.to_owned(),
            request_checksum_sha256: format!("checksum-{request_id}"),
            semantic_fingerprint_sha256: format!("fingerprint-{request_id}"),
            source_request_checksum_sha256: None,
            writer_id: "writer".to_owned(),
            writer_fence_token: FenceToken(0),
            message: None,
            annotations: None,
            ops,
            preconditions: Vec::new(),
            results: Vec::new(),
        }
    }

    #[test]
    fn indexed_current_head_queries_match_vector_semantics() {
        let mut state = state_after(&[
            vec![
                WalOp::CreateDir {
                    op_index: 0,
                    inode_id: InodeId(2),
                    parent_inode: InodeId(1),
                    display_name: "docs".to_owned(),
                },
                WalOp::CreateFile {
                    op_index: 1,
                    inode_id: InodeId(3),
                    parent_inode: InodeId(2),
                    display_name: "b.txt".to_owned(),
                    content_ref: content(b"one"),
                },
                WalOp::CreateDir {
                    op_index: 2,
                    inode_id: InodeId(4),
                    parent_inode: InodeId(1),
                    display_name: "archive".to_owned(),
                },
            ],
            vec![WalOp::ReplaceFile {
                op_index: 0,
                inode_id: InodeId(3),
                base_revision: RevisionNo(1),
                content_ref: content(b"two"),
            }],
            vec![WalOp::Rename {
                op_index: 0,
                inode_id: InodeId(3),
                new_parent_inode: InodeId(4),
                new_display_name: "z.txt".to_owned(),
            }],
            vec![WalOp::CreateFile {
                op_index: 0,
                inode_id: InodeId(5),
                parent_inode: InodeId(2),
                display_name: "a.txt".to_owned(),
                content_ref: content(b"three"),
            }],
            vec![WalOp::DeleteSubtree {
                op_index: 0,
                root_inode: InodeId(2),
            }],
        ]);
        state.request_receipts.push(RequestReceiptRecord {
            request_id: "durable-request".to_owned(),
            semantic_fingerprint_sha256: "fingerprint".to_owned(),
            committed_seq: ChangeSeq(5),
            commit_id: "commit".to_owned(),
            results: Vec::new(),
        });

        let seq = ChangeSeq(5);
        let indexed = IndexedMetadataState::from_metadata_state(state.clone(), seq);

        for inode_id in [InodeId(1), InodeId(2), InodeId(3), InodeId(4), InodeId(5)] {
            assert_eq!(
                state.inode_at_seq(inode_id, seq),
                indexed.inode(inode_id),
                "raw inode {inode_id:?}"
            );
            assert_eq!(
                state.visible_inode(inode_id, seq),
                indexed.visible_inode(inode_id),
                "visible inode {inode_id:?}"
            );
            assert_eq!(
                state.covering_subtree_tombstone(inode_id, seq),
                indexed.covering_subtree_tombstone(inode_id),
                "covering tombstone {inode_id:?}"
            );
        }

        assert_eq!(
            state.current_revision_head(InodeId(3), seq),
            indexed.current_revision_head(InodeId(3))
        );
        assert_eq!(
            state.revision_at_seq(InodeId(3), RevisionNo(1), seq),
            indexed.revision(InodeId(3), RevisionNo(1))
        );
        assert_eq!(
            state.visible_child(InodeId(4), "z.txt", seq),
            indexed.visible_child(InodeId(4), "z.txt")
        );
        assert_eq!(
            state.current_parent_binding_for_child(InodeId(3), seq),
            indexed.current_parent_binding_for_child(InodeId(3))
        );
        assert_eq!(
            state.visible_children(InodeId(4), seq),
            indexed.visible_children(InodeId(4))
        );
        assert_eq!(
            state.resolve_visible_path("/archive/z.txt", seq),
            indexed.resolve_visible_path("/archive/z.txt")
        );
        assert_eq!(
            indexed
                .request_receipt("durable-request")
                .map(|receipt| receipt.commit_id),
            Some("commit".to_owned())
        );
    }

    #[test]
    fn metadata_overlay_reflects_intra_request_rows_without_mutating_base() {
        let state = state_after(&[vec![WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }]]);
        let indexed = IndexedMetadataState::from_metadata_state(state, ChangeSeq(1));
        let mut overlay = MetadataOverlay::new(&indexed, ChangeSeq(2));

        overlay
            .apply_committed_wal_op(
                ChangeSeq(2),
                &WalOp::CreateDir {
                    op_index: 0,
                    inode_id: InodeId(3),
                    parent_inode: InodeId(2),
                    display_name: "drafts".to_owned(),
                },
            )
            .expect("create dir should apply");
        overlay
            .apply_committed_wal_op(
                ChangeSeq(2),
                &WalOp::CreateFile {
                    op_index: 1,
                    inode_id: InodeId(4),
                    parent_inode: InodeId(3),
                    display_name: "note.txt".to_owned(),
                    content_ref: content(b"one"),
                },
            )
            .expect("create file should apply");
        overlay
            .apply_committed_wal_op(
                ChangeSeq(2),
                &WalOp::ReplaceFile {
                    op_index: 2,
                    inode_id: InodeId(4),
                    base_revision: RevisionNo(1),
                    content_ref: content(b"two"),
                },
            )
            .expect("replace file should apply");
        let source = overlay
            .revision(InodeId(4), RevisionNo(1))
            .expect("same-request source revision should resolve");
        overlay
            .apply_committed_wal_op(
                ChangeSeq(2),
                &WalOp::RestoreRevision {
                    op_index: 3,
                    inode_id: InodeId(4),
                    source_revision_no: RevisionNo(1),
                    base_revision: RevisionNo(2),
                    content_ref: source.content_ref,
                },
            )
            .expect("restore should apply");
        overlay
            .apply_committed_wal_op(
                ChangeSeq(2),
                &WalOp::Rename {
                    op_index: 4,
                    inode_id: InodeId(4),
                    new_parent_inode: InodeId(1),
                    new_display_name: "moved.txt".to_owned(),
                },
            )
            .expect("rename should apply");
        overlay
            .apply_committed_wal_op(
                ChangeSeq(2),
                &WalOp::DeleteFile {
                    op_index: 5,
                    inode_id: InodeId(4),
                },
            )
            .expect("delete should apply");

        assert!(indexed.resolve_visible_path("/docs/drafts").is_err());
        assert_eq!(
            overlay
                .resolve_visible_path("/docs/drafts")
                .expect("created parent should be visible")
                .inode_id,
            InodeId(3)
        );
        assert!(overlay
            .resolve_visible_path("/docs/drafts/note.txt")
            .is_err());
        assert!(overlay.visible_child(InodeId(1), "moved.txt").is_none());
        assert_eq!(
            overlay
                .covering_subtree_tombstone(InodeId(4))
                .map(|tombstone| tombstone.root_inode_id),
            Some(InodeId(4))
        );
        assert_eq!(
            overlay
                .latest_revision_head(InodeId(4))
                .map(|revision| revision.revision_no),
            Some(RevisionNo(3))
        );
    }

    #[test]
    fn in_place_indexed_wal_apply_matches_vector_apply() {
        let state = state_after(&[vec![WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }]]);
        let record = wal_record(
            ChangeSeq(2),
            "req-2",
            vec![WalOp::CreateFile {
                op_index: 0,
                inode_id: InodeId(3),
                parent_inode: InodeId(2),
                display_name: "file.txt".to_owned(),
                content_ref: content(b"body"),
            }],
        );

        let expected = state
            .apply_committed_wal_record(&record)
            .expect("vector apply should succeed")
            .metadata_state;
        let mut indexed = IndexedMetadataState::from_metadata_state(state, ChangeSeq(1));
        indexed
            .apply_committed_wal_record_in_place(&record)
            .expect("indexed apply should succeed");

        assert_eq!(indexed.metadata_state(), &expected);
        assert_eq!(
            indexed
                .request_receipt("req-2")
                .map(|receipt| receipt.committed_seq),
            Some(ChangeSeq(2))
        );
        assert_eq!(
            indexed
                .resolve_visible_path("/docs/file.txt")
                .expect("created file should resolve")
                .inode_id,
            InodeId(3)
        );
    }
}
