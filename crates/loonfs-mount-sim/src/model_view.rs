//! Reads of the model's metadata at one sequence: visibility, paths, and the current revision,
//! binding, and attributes of an inode.
//!
//! Visibility, path resolution, listings, and cycle checks come from `loonfs-model`. The helpers
//! here look up rows that the model keeps, such as an inode's newest revision row, and add no
//! rules of their own.

use crate::tokens::encode_binding_version;
use loonfs_model::metadata::{
    DirentryBindingRecord, DirentryBindingState, InodeRecord, MetadataState, RevisionRecord,
    SubtreeTombstoneAction, SubtreeTombstoneRecord,
};
use loonfs_model::visibility::{ListedEntry, PathLookup};
use loonfs_types::api::v0::{AttributesProjection, PathEntry, PathEntryKind};
use loonfs_types::{
    AbsolutePath, ActorId, AttributeKey, AttributeValue, Attributes, AttributesRevisionNo,
    BindingVersion, ChangeSeq, DisplayName, InodeId, InodeKind, NameKey, NamespaceId,
    NamespaceNaming, ROOT_INODE_ID,
};
use std::collections::BTreeMap;

/// A read of one namespace's metadata at one sequence.
#[derive(Clone, Copy)]
pub(crate) struct ModelView<'a> {
    pub(crate) state: &'a MetadataState,
    pub(crate) seq: ChangeSeq,
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) naming: NamespaceNaming,
}

/// An inode's attributes at one sequence.
pub(crate) struct AttributesAt {
    pub(crate) revision_no: AttributesRevisionNo,
    pub(crate) attributes: Attributes,
    pub(crate) updated: Option<(ActorId, u64)>,
}

impl<'a> ModelView<'a> {
    pub(crate) fn visible_inode(&self, inode_id: InodeId) -> Option<&'a InodeRecord> {
        self.state
            .visible_inode(inode_id, self.seq)
            .expect("the model's bindings should form a tree")
    }

    pub(crate) fn resolve_path(&self, path: &AbsolutePath) -> PathLookup<'a> {
        self.state
            .resolve_path(self.naming, path, self.seq)
            .expect("the model's bindings should form a tree")
    }

    pub(crate) fn list_directory(&self, directory_inode_id: InodeId) -> Vec<ListedEntry<'a>> {
        self.state
            .list_directory(directory_inode_id, self.seq)
            .expect("the model's bindings should form a tree")
    }

    pub(crate) fn name_key(&self, display_name: &DisplayName) -> NameKey {
        NameKey::for_display_name(self.naming, display_name)
    }

    /// Returns the visible child of `parent_inode_id` whose name key matches `display_name`.
    pub(crate) fn visible_child(
        &self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) -> Option<ListedEntry<'a>> {
        let name_key = self.name_key(display_name);
        self.list_directory(parent_inode_id)
            .into_iter()
            .find(|entry| entry.binding.name_key == name_key)
    }

    pub(crate) fn has_visible_children(&self, directory_inode_id: InodeId) -> bool {
        !self.list_directory(directory_inode_id).is_empty()
    }

    pub(crate) fn would_create_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    ) -> bool {
        self.state
            .would_create_cycle(inode_id, new_parent_inode_id, self.seq)
            .expect("the model's bindings should form a tree")
    }

    /// Returns the bound binding that places `inode_id` in a directory.
    pub(crate) fn current_binding(&self, inode_id: InodeId) -> Option<&'a DirentryBindingRecord> {
        self.state.parent_binding(inode_id, self.seq)
    }

    pub(crate) fn binding_version(&self, binding: &DirentryBindingRecord) -> BindingVersion {
        encode_binding_version(
            self.namespace_id,
            loonfs_model::metadata::DeltaPosition {
                seq: binding.committed_seq,
                delta_index: binding.delta_index,
            },
        )
    }

    /// Returns the path of a visible inode, spelled with the stored display names.
    pub(crate) fn inode_path(&self, inode_id: InodeId) -> AbsolutePath {
        let mut names = Vec::new();
        let mut current = inode_id;
        while let Some(binding) = self.current_binding(current) {
            names.push(display_name(binding));
            current = binding.parent_inode_id;
        }
        assert_eq!(
            current, ROOT_INODE_ID,
            "a visible inode's bindings should lead to the root"
        );
        names
            .iter()
            .rev()
            .fold(AbsolutePath::root(), |path, name| path.join(name))
    }

    pub(crate) fn current_revision(&self, inode_id: InodeId) -> Option<&'a RevisionRecord> {
        self.state
            .revisions
            .iter()
            .filter(|row| row.inode_id == inode_id && row.committed_seq <= self.seq)
            .max_by_key(|row| row.revision_no)
    }

    pub(crate) fn revision(
        &self,
        inode_id: InodeId,
        revision_no: loonfs_types::RevisionNo,
    ) -> Option<&'a RevisionRecord> {
        self.state.revisions.iter().find(|row| {
            row.inode_id == inode_id
                && row.revision_no == revision_no
                && row.committed_seq <= self.seq
        })
    }

    pub(crate) fn attributes(&self, inode_id: InodeId) -> AttributesAt {
        let Some(row) = self
            .state
            .attribute_revisions
            .iter()
            .filter(|row| row.inode_id == inode_id && row.committed_seq <= self.seq)
            .max_by_key(|row| row.revision_no)
        else {
            return AttributesAt {
                revision_no: AttributesRevisionNo(0),
                attributes: Attributes::default(),
                updated: None,
            };
        };
        let entries: BTreeMap<AttributeKey, AttributeValue> = row
            .entries
            .iter()
            .map(|entry| {
                (
                    AttributeKey::parse(&entry.key)
                        .expect("a stored attribute key should be valid"),
                    AttributeValue::parse(&entry.value)
                        .expect("a stored attribute value should be valid"),
                )
            })
            .collect();
        AttributesAt {
            revision_no: AttributesRevisionNo(row.revision_no),
            attributes: Attributes::new(entries).expect("a stored attribute map should be valid"),
            updated: Some((row.committed_by.clone(), row.committed_at_ms)),
        }
    }

    /// Returns the newest tombstone event of `root_inode_id`.
    pub(crate) fn latest_tombstone_event(
        &self,
        root_inode_id: InodeId,
    ) -> Option<&'a SubtreeTombstoneRecord> {
        self.state
            .subtree_tombstones
            .iter()
            .filter(|event| event.root_inode_id == root_inode_id && event.committed_seq <= self.seq)
            .max_by_key(|event| (event.committed_seq, event.delta_index))
    }

    /// Returns the deletion of `root_inode_id` that an undelete can still revoke.
    pub(crate) fn recoverable_deletion(
        &self,
        root_inode_id: InodeId,
    ) -> Option<&'a SubtreeTombstoneRecord> {
        self.latest_tombstone_event(root_inode_id)
            .filter(|event| matches!(event.action, SubtreeTombstoneAction::Set { .. }))
    }

    /// Returns an inode whose revisions a revision read can still read: one that's visible, or
    /// hidden only by deletions that haven't expired (api.md, "`GET /filesystem/content`").
    pub(crate) fn retained_inode(&self, inode_id: InodeId) -> Option<&'a InodeRecord> {
        let inode = self
            .state
            .inodes
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.committed_seq <= self.seq)?;
        let mut current = inode_id;
        loop {
            let latest = self.latest_tombstone_event(current);
            if latest
                .is_some_and(|event| matches!(event.action, SubtreeTombstoneAction::Expire { .. }))
            {
                return None;
            }
            match self.current_binding(current) {
                Some(binding) => current = binding.parent_inode_id,
                None if current == ROOT_INODE_ID => return Some(inode),
                None => return self.recoverable_deletion(current).map(|_| inode),
            }
        }
    }

    /// Builds the entry that the entry, inode, and listing routes return for a visible inode.
    pub(crate) fn path_entry(
        &self,
        inode: &InodeRecord,
        path: AbsolutePath,
        include_attributes: bool,
    ) -> PathEntry {
        let binding = self.current_binding(inode.inode_id);
        let kind = match inode.inode_kind {
            InodeKind::Directory => PathEntryKind::Directory {},
            InodeKind::File => {
                let revision = self
                    .current_revision(inode.inode_id)
                    .expect("a visible file should have a revision");
                PathEntryKind::File {
                    revision_no: revision.revision_no,
                    size_bytes: revision.content_ref.size_bytes,
                    content_ref: revision.content_ref.clone(),
                    revision_committed_by: revision.committed_by.clone(),
                    revision_committed_at_ms: revision.committed_at_ms,
                }
            }
        };
        let attributes = include_attributes.then(|| {
            let current = self.attributes(inode.inode_id);
            AttributesProjection {
                attributes_revision_no: current.revision_no,
                attributes_updated_by: current.updated.as_ref().map(|(by, _)| by.clone()),
                attributes_updated_at_ms: current.updated.as_ref().map(|(_, at_ms)| *at_ms),
                attributes: current.attributes,
            }
        });
        PathEntry {
            namespace_id: self.namespace_id.clone(),
            path,
            inode_id: inode.inode_id,
            created_by: inode.committed_by.clone(),
            created_at_ms: inode.committed_at_ms,
            kind,
            head_seq: self.seq,
            parent_inode_id: binding.map(|binding| binding.parent_inode_id),
            display_name: binding.map(display_name),
            binding_version: binding.map(|binding| self.binding_version(binding)),
            attributes,
        }
    }
}

/// Returns the display name of a bound binding.
pub(crate) fn display_name(binding: &DirentryBindingRecord) -> DisplayName {
    match &binding.state {
        DirentryBindingState::Bound { display_name } => Some(display_name.clone()),
        DirentryBindingState::Unbound => None,
    }
    .expect("the model returns only bound bindings as a parent binding")
}
