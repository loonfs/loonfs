//! The writes of one commit: each write reserves its operation index,
//! checks the rules about its own rows, and records the WAL deltas that it
//! builds. A write reserves its index before it checks anything, so an index
//! overflow is reported before the write's own errors.

use super::commit_session::CommitSession;
use super::numbering::CommitNumbering;
use crate::commit::{CommitOperand, CommitValidationError};
use crate::error::Result;
use crate::metadata::InodeRecord;
use crate::path::write::{
    AvailableName, DeletionInTrash, NewInodeId, ResolvedBinding, ResolvedTarget,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{DeletedBinding, DeltaPosition, RevisionSummary};
use loonfs_types::format::wal::WalDelta;
use loonfs_types::{
    next_public_ordinal, AccessGrants, AccessRevisionNo, Attributes, AttributesRevisionNo,
    CommitSeq, ContentRef, InodeId, InodeKind, RevisionNo,
};

/// The kind of inode a create makes, with a file's initial content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NewChild {
    Directory,
    /// Immutable initial bytes, which must have valid preparation proof
    /// before publication.
    File(ContentRef),
}

impl<S: ObjectStore + ?Sized> CommitSession<'_, S> {
    /// Creates a directory or file with the ID `new_inode` at `name`.
    pub(crate) async fn create(
        &mut self,
        new_inode: NewInodeId,
        name: AvailableName,
        child: NewChild,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let child_inode_id = new_inode.inode_id();
        self.ensure_child_unbound(child_inode_id, None).await?;
        let parent_inode_id = name.parent_inode_id();
        let name_key = name.name_key();
        let (inode_kind, revision) = match &child {
            NewChild::Directory => (InodeKind::Directory, None),
            NewChild::File(content_ref) => (
                InodeKind::File,
                Some(RevisionSummary {
                    revision_no: RevisionNo(1),
                    content_ref: content_ref.clone(),
                    revision_committed_by: self.actor_id.clone(),
                    revision_committed_at_ms: self.committed_at_ms,
                }),
            ),
        };
        let create_index = self.numbering.reserve_delta_index()?;
        let bind_position = DeltaPosition {
            seq: self.committed_seq,
            delta_index: self.numbering.reserve_delta_index()?,
        };
        let mut deltas = vec![
            WalDelta::CreateInode {
                delta_index: create_index,
                inode_id: child_inode_id,
                inode_kind,
            },
            WalDelta::BindDirentry {
                delta_index: bind_position.delta_index,
                parent_inode_id,
                name_key: name_key.clone(),
                display_name: name.display_name().clone(),
                child_inode_id,
                child_kind: inode_kind,
                child_created_by: self.actor_id.clone(),
                child_created_at_ms: self.committed_at_ms,
                revision,
            },
        ];
        if let NewChild::File(content_ref) = child {
            deltas.push(WalDelta::AppendFileRevision {
                delta_index: self.numbering.reserve_delta_index()?,
                inode_id: child_inode_id,
                parent_inode_id,
                name_key,
                display_name: name.display_name().clone(),
                bind_position,
                child_created_by: self.actor_id.clone(),
                child_created_at_ms: self.committed_at_ms,
                revision_no: RevisionNo(1),
                content_ref,
                layout: None,
                append_offset: None,
            });
        }
        self.record_operation(op_index, deltas);
        Ok(())
    }

    /// Appends a new revision to the file that `binding` binds, after its
    /// current one.
    ///
    /// `append_offset` is where an append's bytes begin, which is the current
    /// revision's size. It's `None` for a put or a copy.
    pub(crate) async fn replace_file(
        &mut self,
        binding: ResolvedBinding,
        content_ref: &ContentRef,
        append_offset: Option<u64>,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let inode_id = binding.child_inode_id();
        let current_revision_no = self.resolve_current_revision_no(inode_id).await?;
        let revision_no = next_revision_no(inode_id, current_revision_no)?;
        let delta = revision_delta(
            &mut self.numbering,
            &binding,
            revision_no,
            content_ref.clone(),
            append_offset,
        )?;
        self.record_operation(op_index, vec![delta]);
        Ok(())
    }

    /// Restores an earlier revision of the file that `binding` binds as a new
    /// current revision.
    pub(crate) async fn restore_revision(
        &mut self,
        binding: ResolvedBinding,
        source_revision_no: RevisionNo,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let inode_id = binding.child_inode_id();
        let current_revision_no = self.resolve_current_revision_no(inode_id).await?;
        let source_revision = self
            .resolve_restore_source_revision(inode_id, source_revision_no)
            .await?;
        let revision_no = next_revision_no(inode_id, current_revision_no)?;
        let delta = revision_delta(
            &mut self.numbering,
            &binding,
            revision_no,
            source_revision.content_ref,
            None,
        )?;
        self.record_operation(op_index, vec![delta]);
        Ok(())
    }

    /// Deletes the file or directory subtree that `binding` binds.
    pub(crate) async fn delete(&mut self, binding: ResolvedBinding) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let inode_id = binding.child_inode_id();
        let inode = self.view().inode_at_seq(inode_id).await?.ok_or(
            CommitValidationError::InodeMissing {
                operand: CommitOperand::DeleteTarget,
                inode_id,
            },
        )?;
        let deltas = vec![
            unbind_delta(&mut self.numbering, &binding, &inode)?,
            WalDelta::TombstoneSubtree {
                delta_index: self.numbering.reserve_delta_index()?,
                root_inode_id: inode.inode_id,
                root_kind: inode.inode_kind,
                deleted_binding: DeletedBinding {
                    parent_inode_id: binding.parent_inode_id(),
                    name_key: binding.name_key().clone(),
                    display_name: binding.display_name().clone(),
                },
            },
        ];
        self.record_operation(op_index, deltas);
        Ok(())
    }

    /// Rebinds the inode that `source_binding` binds at `destination`.
    pub(crate) async fn rename(
        &mut self,
        source_binding: ResolvedBinding,
        destination: AvailableName,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let new_parent_inode_id = destination.parent_inode_id();
        let inode_id = source_binding.child_inode_id();
        let inode = self.view().inode_at_seq(inode_id).await?.ok_or(
            CommitValidationError::InodeMissing {
                operand: CommitOperand::RenameSource,
                inode_id,
            },
        )?;
        self.ensure_child_unbound(inode_id, Some(source_binding.bind_position()))
            .await?;
        self.ensure_rename_does_not_cycle(&inode, new_parent_inode_id)
            .await?;
        let revision = self.resolve_revision_summary(&inode).await?;
        let deltas = vec![
            unbind_delta(&mut self.numbering, &source_binding, &inode)?,
            WalDelta::BindDirentry {
                delta_index: self.numbering.reserve_delta_index()?,
                parent_inode_id: new_parent_inode_id,
                name_key: destination.name_key(),
                display_name: destination.display_name().clone(),
                child_inode_id: inode_id,
                child_kind: inode.inode_kind,
                child_created_by: inode.committed_by,
                child_created_at_ms: inode.committed_at_ms,
                revision,
            },
        ];
        self.record_operation(op_index, deltas);
        Ok(())
    }

    /// Recovers a deleted file or subtree: revokes `deletion`, the root's
    /// deletion in the trash, which the request identified by `deletion_seq`
    /// (the delete's committed sequence, which the delete and the change feed
    /// report), and binds the inode at `destination`. Scoping recovery to
    /// the observed position keeps a stale request from cancelling a later
    /// deletion of the same inode.
    pub(crate) async fn restore_deleted(
        &mut self,
        deletion: DeletionInTrash,
        deletion_seq: CommitSeq,
        destination: AvailableName,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let inode_id = deletion.root_inode_id();
        let inode = self.view().inode_at_seq(inode_id).await?.ok_or(
            CommitValidationError::InodeMissing {
                operand: CommitOperand::RestoreDeletedTarget,
                inode_id,
            },
        )?;
        self.ensure_restorable_deletion(&deletion, deletion_seq)?;
        self.ensure_child_unbound(inode_id, None).await?;
        let revision = self.resolve_revision_summary(&inode).await?;
        let deltas = vec![
            WalDelta::RevokeSubtreeTombstone {
                delta_index: self.numbering.reserve_delta_index()?,
                root_inode_id: inode_id,
                target: deletion.position(),
            },
            WalDelta::BindDirentry {
                delta_index: self.numbering.reserve_delta_index()?,
                parent_inode_id: destination.parent_inode_id(),
                name_key: destination.name_key(),
                display_name: destination.display_name().clone(),
                child_inode_id: inode_id,
                child_kind: inode.inode_kind,
                child_created_by: inode.committed_by,
                child_created_at_ms: inode.committed_at_ms,
                revision,
            },
        ];
        self.record_operation(op_index, deltas);
        Ok(())
    }

    /// Replaces the attributes of `target` with `attributes`, the complete
    /// map after the update.
    pub(crate) async fn update_attributes(
        &mut self,
        target: ResolvedTarget<'_>,
        attributes: &Attributes,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let inode_id = target.inode_id();
        let current_attributes_revision_no = self
            .view()
            .attributes(inode_id)
            .await?
            .attributes_revision_no;
        let attributes_revision_no =
            next_attributes_revision_no(inode_id, current_attributes_revision_no)?;
        let delta = WalDelta::AppendAttributesRevision {
            delta_index: self.numbering.reserve_delta_index()?,
            inode_id,
            attributes_revision_no,
            attributes: attributes.clone(),
        };
        self.record_operation(op_index, vec![delta]);
        Ok(())
    }

    /// Replaces the access row of `target` with `boundary` and `grants`.
    pub(crate) async fn update_access(
        &mut self,
        target: ResolvedTarget<'_>,
        boundary: bool,
        grants: &AccessGrants,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let inode_id = target.inode_id();
        let current_access_revision_no = self
            .view()
            .latest_access_revision(inode_id)
            .await?
            .map_or(AccessRevisionNo(0), |revision| revision.access_revision_no);
        let access_revision_no = next_access_revision_no(inode_id, current_access_revision_no)?;
        let delta = WalDelta::AppendAccessRevision {
            delta_index: self.numbering.reserve_delta_index()?,
            inode_id,
            access_revision_no,
            boundary,
            grants: grants.clone(),
        };
        self.record_operation(op_index, vec![delta]);
        Ok(())
    }
}

/// The delta of a file's next revision. The delta includes the file's
/// current binding, and replay writes the revision's summary row under that
/// binding
/// ([format section 1.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings)).
fn revision_delta(
    numbering: &mut CommitNumbering,
    binding: &ResolvedBinding,
    revision_no: RevisionNo,
    content_ref: ContentRef,
    append_offset: Option<u64>,
) -> std::result::Result<WalDelta, CommitValidationError> {
    Ok(WalDelta::AppendFileRevision {
        delta_index: numbering.reserve_delta_index()?,
        inode_id: binding.child_inode_id(),
        parent_inode_id: binding.parent_inode_id(),
        name_key: binding.name_key().clone(),
        display_name: binding.display_name().clone(),
        bind_position: binding.bind_position(),
        child_created_by: binding.child_created_by().clone(),
        child_created_at_ms: binding.child_created_at_ms(),
        revision_no,
        content_ref,
        layout: None,
        append_offset,
    })
}

fn unbind_delta(
    numbering: &mut CommitNumbering,
    binding: &ResolvedBinding,
    inode: &InodeRecord,
) -> std::result::Result<WalDelta, CommitValidationError> {
    Ok(WalDelta::UnbindDirentry {
        delta_index: numbering.reserve_delta_index()?,
        parent_inode_id: binding.parent_inode_id(),
        name_key: binding.name_key().clone(),
        display_name: binding.display_name().clone(),
        child_inode_id: binding.child_inode_id(),
        child_kind: inode.inode_kind,
        child_created_by: inode.committed_by.clone(),
        child_created_at_ms: inode.committed_at_ms,
        target: binding.bind_position(),
    })
}

fn next_access_revision_no(
    inode_id: InodeId,
    current_access_revision_no: AccessRevisionNo,
) -> std::result::Result<AccessRevisionNo, CommitValidationError> {
    next_public_ordinal(current_access_revision_no.0)
        .map(AccessRevisionNo)
        .ok_or(CommitValidationError::UpdateAccessRevisionOverflow {
            inode_id,
            base_access_revision_no: current_access_revision_no,
        })
}

fn next_revision_no(
    inode_id: InodeId,
    current_revision_no: RevisionNo,
) -> std::result::Result<RevisionNo, CommitValidationError> {
    next_public_ordinal(current_revision_no.0)
        .map(RevisionNo)
        .ok_or(CommitValidationError::RevisionOverflow {
            inode_id,
            base_revision_no: current_revision_no,
        })
}

fn next_attributes_revision_no(
    inode_id: InodeId,
    current_attributes_revision_no: AttributesRevisionNo,
) -> std::result::Result<AttributesRevisionNo, CommitValidationError> {
    next_public_ordinal(current_attributes_revision_no.0)
        .map(AttributesRevisionNo)
        .ok_or(CommitValidationError::UpdateAttributesRevisionOverflow {
            inode_id,
            base_attributes_revision_no: current_attributes_revision_no,
        })
}

#[cfg(test)]
mod ordinal_tests {
    use super::*;
    use loonfs_types::MAX_PUBLIC_INTEGER;
    use std::assert_matches;

    #[test]
    fn revision_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        let at_maximum = next_revision_no(InodeId(2), RevisionNo(MAX_PUBLIC_INTEGER - 1))
            .expect("advance to public maximum");
        assert_eq!(at_maximum, RevisionNo(MAX_PUBLIC_INTEGER));

        assert_matches!(
            next_revision_no(InodeId(2), RevisionNo(MAX_PUBLIC_INTEGER)),
            Err(CommitValidationError::RevisionOverflow {
                inode_id: InodeId(2),
                base_revision_no: RevisionNo(MAX_PUBLIC_INTEGER),
            })
        );
    }

    #[test]
    fn attribute_revision_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        assert_eq!(
            next_attributes_revision_no(InodeId(2), AttributesRevisionNo(MAX_PUBLIC_INTEGER - 1),)
                .expect("advance to public maximum"),
            AttributesRevisionNo(MAX_PUBLIC_INTEGER)
        );
        assert_matches!(
            next_attributes_revision_no(InodeId(2), AttributesRevisionNo(MAX_PUBLIC_INTEGER),),
            Err(CommitValidationError::UpdateAttributesRevisionOverflow {
                inode_id: InodeId(2),
                base_attributes_revision_no: AttributesRevisionNo(MAX_PUBLIC_INTEGER),
            })
        );
    }
}
