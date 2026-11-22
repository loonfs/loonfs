//! The state checks that commit operation validators run: target
//! visibility, binding state, and cycles.

use super::super::CommitValidationError;
use super::error::CommitOperand;
use super::session::CommitValidationSession;
use crate::error::{CoreError, Result};
use crate::metadata::{
    Ancestry, DirentryBindingRecord, InodeRecord, RevisionRecord, SubtreeTombstoneRecord,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{DeltaPosition, RevisionSummary};

use loonfs_types::{CommitSeq, DisplayName, InodeId, InodeKind, NameKey, RevisionNo};

#[derive(Debug, Default)]
pub(super) struct CommitNumbering {
    next_op_index: u32,
    next_delta_index: u32,
}

impl CommitNumbering {
    pub(super) fn reserve_op_index(&mut self) -> std::result::Result<u32, CommitValidationError> {
        let op_index = self.next_op_index;
        self.next_op_index = self
            .next_op_index
            .checked_add(1)
            .ok_or(CommitValidationError::OperationIndexOverflow)?;
        Ok(op_index)
    }

    pub(super) fn reserve_delta_index(
        &mut self,
    ) -> std::result::Result<u32, CommitValidationError> {
        let delta_index = self.next_delta_index;
        self.next_delta_index = self
            .next_delta_index
            .checked_add(1)
            .ok_or(CommitValidationError::DeltaIndexOverflow)?;
        Ok(delta_index)
    }
}

impl<S: ObjectStore + ?Sized> CommitValidationSession<'_, S> {
    /// Ensures that `deletion`, the root's deletion in the trash, is the
    /// deletion that `deletion_seq` identifies, that `deletion_seq` is above
    /// the retention floor of the writer's basis, and that an earlier commit
    /// recorded it
    /// ([format section 1.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#16-file-and-subtree-deletion)).
    /// A request for a deletion that has expired, that a later event revoked,
    /// or that a later deletion replaced, fails here, so it can't cancel a
    /// different deletion.
    ///
    /// A base compaction relies on the floor check: the writer validates
    /// against a floor that only its own epoch raises, so no restore of a
    /// deletion at or below the floor is accepted after the floor moves.
    pub(super) fn ensure_restorable_deletion(
        &self,
        deletion: &SubtreeTombstoneRecord,
        deletion_seq: CommitSeq,
    ) -> Result<()> {
        let inode_id = deletion.root_inode_id;
        if deletion_seq <= self.view().retention_floor_seq() {
            return Err(CommitValidationError::RestoreDeletedTargetNotDeleted { inode_id }.into());
        }
        if deletion.committed_seq >= self.committed_seq {
            return Err(
                CommitValidationError::RestoreDeletedTargetsCurrentCommit { inode_id }.into(),
            );
        }
        if deletion.committed_seq != deletion_seq {
            return Err(CommitValidationError::RestoreDeletedSequenceMismatch {
                inode_id,
                requested_seq: deletion_seq,
                actual_deletion_seq: deletion.committed_seq,
            }
            .into());
        }
        Ok(())
    }

    pub(super) async fn resolve_inode_kind(
        &self,
        inode_id: InodeId,
        expected: InodeKind,
        operand: CommitOperand,
    ) -> Result<InodeRecord> {
        let inode = self
            .view()
            .inode_at_seq(inode_id)
            .await?
            .ok_or(CommitValidationError::InodeMissing { operand, inode_id })?;
        if inode.inode_kind != expected {
            return Err(CommitValidationError::InodeWrongKind {
                operand,
                inode_id,
                expected,
                actual: inode.inode_kind,
            }
            .into());
        }
        Ok(inode)
    }

    /// Ensures that the walk up the current parent bindings from `inode_id`
    /// reaches the root, because LoonFS never commits a change to a hidden
    /// inode (format spec, "Row retention during a base compaction").
    ///
    /// The walk ends in one of three ways. If it reaches the root, the check
    /// passes. If it reaches a hiding tombstone, the check fails with
    /// `TargetUnderSubtreeTombstone`. If it reaches an inode without a current
    /// parent binding, the check fails with `InodeMissing`, the error for an
    /// operand that isn't visible. The last case occurs only below an expired
    /// deletion, after a base compaction of the bindings group has removed the
    /// bindings below the deleted directory while the inode rows remain.
    pub(super) async fn ensure_ancestry_reaches_root(
        &self,
        inode_id: InodeId,
        operand: CommitOperand,
    ) -> Result<()> {
        match self.view().ancestry(inode_id).await? {
            Ancestry::Root => Ok(()),
            Ancestry::Hidden(tombstone) => {
                Err(CommitValidationError::TargetUnderSubtreeTombstone {
                    operand,
                    inode_id,
                    root_inode_id: tombstone.root_inode_id,
                    tombstone_seq: tombstone.committed_seq,
                }
                .into())
            }
            Ancestry::Disconnected => {
                Err(CommitValidationError::InodeMissing { operand, inode_id }.into())
            }
        }
    }

    pub(super) async fn resolve_name_absent(
        &self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        rebinding_inode_id: Option<InodeId>,
        operand: CommitOperand,
    ) -> Result<NameKey> {
        self.resolve_inode_kind(parent_inode_id, InodeKind::Directory, operand)
            .await?;

        let name_key = NameKey::for_display_name(display_name);
        if let Some(existing) = self
            .view()
            .visible_child(parent_inode_id, &name_key)
            .await?
        {
            if rebinding_inode_id != Some(existing.child_inode_id) {
                return Err(CommitValidationError::NameTaken {
                    operand,
                    parent_inode_id,
                    name_key,
                    child_inode_id: existing.child_inode_id,
                }
                .into());
            }
        }
        Ok(name_key)
    }

    /// One parent per child: a bind names an inode with no current binding, or
    /// with the binding its own operation unbinds first.
    pub(super) async fn ensure_child_unbound(
        &self,
        child_inode_id: InodeId,
        unbinding: Option<DeltaPosition>,
    ) -> Result<()> {
        match self
            .view()
            .current_parent_binding_for_child(child_inode_id)
            .await?
        {
            Some(current) if Some(current.bind_position) != unbinding => {
                Err(CoreError::NamespaceCorrupt(format!(
                    "a commit binds inode `{child_inode_id}`, which is already bound as `{}` under \
                     parent inode `{}`",
                    current.name_key, current.parent_inode_id
                )))
            }
            _ => Ok(()),
        }
    }

    /// Returns the current revision of the file whose next revision an
    /// operation writes. The planner found the file with a revision, so a file
    /// without one is corrupt.
    pub(super) async fn resolve_current_revision_no(
        &self,
        inode_id: InodeId,
    ) -> Result<RevisionNo> {
        let revision = self
            .view()
            .latest_revision_record(inode_id)
            .await?
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!("file `{inode_id}` has no current revision"))
            })?;
        Ok(revision.revision_no)
    }

    /// Returns the current binding of the file whose revision an operation
    /// writes. The revision delta includes the binding's edge, and replay
    /// writes the new revision's summary row under that edge
    /// ([format section 1.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings)).
    /// A file with no current binding is deleted or disconnected, and can't
    /// take a revision.
    pub(super) async fn resolve_current_binding(
        &self,
        inode_id: InodeId,
        operand: CommitOperand,
    ) -> Result<DirentryBindingRecord> {
        Ok(self
            .view()
            .current_parent_binding_for_child(inode_id)
            .await?
            .ok_or(CommitValidationError::InodeMissing { operand, inode_id })?)
    }

    /// Returns the revision summary that a bind of `inode` copies into the
    /// new edge, which is `None` for a directory.
    pub(super) async fn resolve_revision_summary(
        &self,
        inode: &InodeRecord,
    ) -> Result<Option<RevisionSummary>> {
        if inode.inode_kind == InodeKind::Directory {
            return Ok(None);
        }
        let revision = self
            .view()
            .latest_revision_record(inode.inode_id)
            .await?
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "file `{}` has no revision to copy into its binding",
                    inode.inode_id
                ))
            })?;
        Ok(Some(RevisionSummary {
            revision_no: revision.revision_no,
            content_ref: revision.content_ref,
            revision_committed_by: revision.committed_by,
            revision_committed_at_ms: revision.committed_at_ms,
        }))
    }

    /// Resolves the revision that a restore copies. A revision at or below
    /// the retention floor is found only while it's the file's newest at or
    /// below the floor. A base compaction removes the older ones once the
    /// floor has moved, and this refusal is what lets it: every commit
    /// validated against the floor leaves them alone, so none copies a
    /// revision whose content the collector reclaims
    /// ([format section 10.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#103-row-retention-during-a-base-compaction)).
    pub(super) async fn resolve_restore_source_revision(
        &self,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
    ) -> Result<RevisionRecord> {
        let missing = || CommitValidationError::RestoreRevisionSourceRevisionMissing {
            inode_id,
            source_revision_no,
        };
        let view = self.view();
        let revision = view
            .revision_at_head(inode_id, source_revision_no)
            .await?
            .ok_or_else(missing)?;
        let floor_seq = view.retention_floor_seq();
        if revision.committed_seq <= floor_seq {
            let newest_at_floor = view
                .at_seq(floor_seq)
                .latest_revision_record(inode_id)
                .await?;
            if newest_at_floor.is_none_or(|newest| newest.revision_no != revision.revision_no) {
                return Err(missing().into());
            }
        }
        Ok(revision)
    }

    pub(super) async fn ensure_rename_does_not_cycle(
        &self,
        inode: &InodeRecord,
        new_parent_inode_id: InodeId,
    ) -> Result<()> {
        if inode.inode_kind == InodeKind::Directory
            && self
                .view()
                .would_create_directory_cycle(inode.inode_id, new_parent_inode_id)
                .await?
        {
            return Err(CommitValidationError::RenameWouldCycleDirectory {
                inode_id: inode.inode_id,
                new_parent_inode_id,
            }
            .into());
        }
        Ok(())
    }
}
