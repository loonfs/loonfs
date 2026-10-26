//! The deltas that the planner writes: each primitive builds the format's WAL deltas for one
//! change, applies them to the planner's model state, and records the change's event.

use super::planner::Planner;
use crate::tokens::encode_binding_version;
use loonfs_model::metadata::{DeltaPosition, DirentryBindingRecord, InodeRecord};
use loonfs_types::api::v0::{DirectoryBinding, FilesystemChange};
use loonfs_types::format::manifest::{self, DeletedBinding};
use loonfs_types::format::wal::WalDelta;
use loonfs_types::{
    Attributes, AttributesRevisionNo, BindingVersion, ContentRef, DisplayName, InodeId, InodeKind,
    RevisionNo,
};

impl Planner<'_> {
    fn delta_index(&mut self) -> u32 {
        let index = self.next_delta_index;
        self.next_delta_index += 1;
        index
    }

    fn apply(&mut self, deltas: &[WalDelta]) {
        self.state = self
            .state
            .apply_committed_wal_deltas(
                self.seq,
                self.commit_id,
                self.actor,
                self.committed_at_ms,
                deltas,
            )
            .expect("the planner's deltas should keep the binding rules");
    }

    fn binding_version_at(&self, delta_index: u32) -> BindingVersion {
        encode_binding_version(
            self.namespace_id,
            DeltaPosition {
                seq: self.seq,
                delta_index,
            },
        )
    }

    fn allocate_inode(&mut self, inode_kind: InodeKind) -> (InodeId, WalDelta) {
        let inode_id = InodeId(self.next_inode_id);
        self.next_inode_id += 1;
        let delta = WalDelta::CreateInode {
            delta_index: self.delta_index(),
            inode_id,
            inode_kind,
        };
        (inode_id, delta)
    }

    fn bind_delta(
        &mut self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        child: &InodeRecord,
    ) -> WalDelta {
        WalDelta::BindDirentry {
            delta_index: self.delta_index(),
            parent_inode_id,
            name_key: self.view().name_key(display_name),
            display_name: display_name.clone(),
            child_inode_id: child.inode_id,
            child_kind: child.inode_kind,
            child_created_by: child.committed_by.clone(),
            child_created_at_ms: child.committed_at_ms,
        }
    }

    fn new_inode_record(&self, inode_id: InodeId, inode_kind: InodeKind) -> InodeRecord {
        InodeRecord {
            inode_id,
            inode_kind,
            committed_seq: self.seq,
            commit_id: self.commit_id.clone(),
            committed_by: self.actor.clone(),
            committed_at_ms: self.committed_at_ms,
        }
    }

    /// Creates a directory, and returns its inode.
    pub(super) fn create_directory_at(
        &mut self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) -> InodeId {
        let (inode_id, create) = self.allocate_inode(InodeKind::Directory);
        let record = self.new_inode_record(inode_id, InodeKind::Directory);
        let bind = self.bind_delta(parent_inode_id, display_name, &record);
        let binding_version = self.binding_version_at(bind.delta_index());
        self.apply(&[create, bind]);
        self.events.push(FilesystemChange::DirectoryCreated {
            inode_id,
            parent_inode_id,
            display_name: display_name.clone(),
            binding_version,
        });
        inode_id
    }

    /// Creates a file with its first revision, and returns its inode.
    pub(super) fn create_file_at(
        &mut self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        content_ref: ContentRef,
    ) -> InodeId {
        let (inode_id, create) = self.allocate_inode(InodeKind::File);
        let record = self.new_inode_record(inode_id, InodeKind::File);
        let bind = self.bind_delta(parent_inode_id, display_name, &record);
        let binding_version = self.binding_version_at(bind.delta_index());
        let revision = WalDelta::AppendFileRevision {
            delta_index: self.delta_index(),
            inode_id,
            revision_no: RevisionNo(1),
            content_ref: content_ref.clone(),
            layout: None,
            append_offset: None,
        };
        self.apply(&[create, bind, revision]);
        self.events.push(FilesystemChange::FileCreated {
            inode_id,
            parent_inode_id,
            display_name: display_name.clone(),
            binding_version,
            revision_no: RevisionNo(1),
            content_ref,
        });
        inode_id
    }

    /// Publishes the next revision of a file.
    pub(super) fn add_revision(
        &mut self,
        inode_id: InodeId,
        content_ref: ContentRef,
        append_offset: Option<u64>,
    ) {
        let current = self
            .view()
            .current_revision(inode_id)
            .expect("a file should have a revision")
            .revision_no;
        let revision_no = RevisionNo(current.0 + 1);
        let delta = WalDelta::AppendFileRevision {
            delta_index: self.delta_index(),
            inode_id,
            revision_no,
            content_ref: content_ref.clone(),
            layout: None,
            append_offset,
        };
        self.apply(&[delta]);
        self.events.push(FilesystemChange::ContentChanged {
            inode_id,
            revision_no,
            content_ref,
            append_offset,
        });
    }

    /// Returns a visible inode that has a binding, and the binding.
    fn bound(&self, inode_id: InodeId) -> (InodeRecord, DirentryBindingRecord) {
        let view = self.view();
        let inode = view
            .visible_inode(inode_id)
            .expect("the planner moves and deletes visible inodes only")
            .clone();
        let binding = view
            .current_binding(inode_id)
            .expect("the planner moves and deletes named inodes only")
            .clone();
        (inode, binding)
    }

    fn unbind_delta(&mut self, inode: &InodeRecord, binding: &DirentryBindingRecord) -> WalDelta {
        WalDelta::UnbindDirentry {
            delta_index: self.delta_index(),
            parent_inode_id: binding.parent_inode_id,
            name_key: binding.name_key.clone(),
            display_name: crate::model_view::display_name(binding),
            child_inode_id: binding.child_inode_id,
            child_kind: inode.inode_kind,
            child_created_by: inode.committed_by.clone(),
            child_created_at_ms: inode.committed_at_ms,
            target: manifest::DeltaPosition {
                seq: binding.committed_seq,
                delta_index: binding.delta_index,
            },
        }
    }

    /// Moves an inode that has a binding to a new parent and name.
    pub(super) fn rebind(
        &mut self,
        inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) {
        let (inode, binding) = self.bound(inode_id);
        let unbind = self.unbind_delta(&inode, &binding);
        let bind = self.bind_delta(parent_inode_id, display_name, &inode);
        let binding_version = self.binding_version_at(bind.delta_index());
        self.apply(&[unbind, bind]);
        self.events.push(FilesystemChange::Moved {
            inode_id,
            source_parent_inode_id: binding.parent_inode_id,
            source_display_name: crate::model_view::display_name(&binding),
            destination_parent_inode_id: parent_inode_id,
            destination_display_name: display_name.clone(),
            binding_version,
        });
    }

    /// Deletes an inode that has a binding, and everything below it, with one unbind and one
    /// tombstone for the deleted inode.
    pub(super) fn delete(&mut self, inode_id: InodeId) {
        let (inode, binding) = self.bound(inode_id);
        let unbind = self.unbind_delta(&inode, &binding);
        let display_name = crate::model_view::display_name(&binding);
        let tombstone = WalDelta::TombstoneSubtree {
            delta_index: self.delta_index(),
            root_inode_id: inode_id,
            deleted_binding: DeletedBinding {
                parent_inode_id: binding.parent_inode_id,
                name_key: binding.name_key.clone(),
                display_name: display_name.clone(),
            },
        };
        self.apply(&[unbind, tombstone]);
        self.events.push(FilesystemChange::Deleted {
            inode_id,
            deleted_binding: DirectoryBinding {
                parent_inode_id: binding.parent_inode_id,
                name_key: binding.name_key,
                display_name,
            },
        });
    }

    /// Revokes the deletion at `deletion`, and binds its root at a new parent and name.
    pub(super) fn restore(
        &mut self,
        inode_id: InodeId,
        deletion: DeltaPosition,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) {
        let inode = self
            .state
            .inodes
            .iter()
            .find(|inode| inode.inode_id == inode_id)
            .expect("a deleted inode should have a row")
            .clone();
        let revoke = WalDelta::RevokeSubtreeTombstone {
            delta_index: self.delta_index(),
            root_inode_id: inode_id,
            target: manifest::DeltaPosition {
                seq: deletion.seq,
                delta_index: deletion.delta_index,
            },
        };
        let bind = self.bind_delta(parent_inode_id, display_name, &inode);
        let binding_version = self.binding_version_at(bind.delta_index());
        self.apply(&[revoke, bind]);
        self.events.push(FilesystemChange::Undeleted {
            inode_id,
            parent_inode_id,
            display_name: display_name.clone(),
            binding_version,
        });
    }

    /// Publishes the next attribute revision of an inode, after `base`.
    pub(super) fn set_attributes(
        &mut self,
        inode_id: InodeId,
        base: AttributesRevisionNo,
        attributes: Attributes,
    ) {
        let attributes_revision_no = AttributesRevisionNo(base.0 + 1);
        let delta = WalDelta::AppendAttributesRevision {
            delta_index: self.delta_index(),
            inode_id,
            attributes_revision_no,
            attributes: attributes.clone(),
        };
        self.apply(&[delta]);
        self.events.push(FilesystemChange::AttributesChanged {
            inode_id,
            attributes_revision_no,
            attributes,
        });
    }
}

trait DeltaIndex {
    fn delta_index(&self) -> u32;
}

impl DeltaIndex for WalDelta {
    fn delta_index(&self) -> u32 {
        match self {
            WalDelta::CreateInode { delta_index, .. }
            | WalDelta::BindDirentry { delta_index, .. }
            | WalDelta::UnbindDirentry { delta_index, .. }
            | WalDelta::AppendFileRevision { delta_index, .. }
            | WalDelta::TombstoneSubtree { delta_index, .. }
            | WalDelta::RevokeSubtreeTombstone { delta_index, .. }
            | WalDelta::ExpireSubtreeTombstone { delta_index, .. }
            | WalDelta::AppendAttributesRevision { delta_index, .. }
            | WalDelta::AppendAccessRevision { delta_index, .. } => *delta_index,
        }
    }
}
