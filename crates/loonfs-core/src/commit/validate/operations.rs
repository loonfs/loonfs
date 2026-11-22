//! The steps of a commit: the checks that each step runs and the WAL deltas
//! that it writes.

use super::super::CommitValidationError;
use super::error::CommitOperand;
use super::session::CommitValidationSession;
use crate::commit::validate::checks::CommitNumbering;
use crate::error::Result;
use crate::metadata::{DirentryBindingRecord, InodeRecord, SubtreeTombstoneRecord};
use crate::path::write::{ResolvedBinding, ResolvedTarget};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{DeletedBinding, DeltaPosition, RevisionSummary};
use loonfs_types::format::wal::WalDelta;
use loonfs_types::{
    next_public_ordinal, AccessGrants, AccessRevisionNo, Attributes, AttributesRevisionNo,
    CommitSeq, ContentRef, DisplayName, InodeId, InodeKind, RevisionNo,
};

/// The kind of inode a create makes, with a file's initial content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NewChild {
    Directory,
    /// Immutable initial bytes, which must have valid preparation proof
    /// before publication.
    File(ContentRef),
}

impl<S: ObjectStore + ?Sized> CommitValidationSession<'_, S> {
    /// Creates a directory or file under a parent inode.
    pub(crate) async fn create(
        &mut self,
        child_inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        child: NewChild,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let deltas = self
            .validate_create(child_inode_id, parent_inode_id, display_name, child)
            .await?;
        self.record_step(op_index, deltas);
        Ok(())
    }

    /// Appends a new revision to the file that `binding` binds, after its
    /// current one.
    ///
    /// `append_offset` is where an append's bytes begin, which is the current
    /// revision's size. It's `None` for a put or a copy.
    pub(crate) async fn replace_file(
        &mut self,
        binding: &ResolvedBinding,
        content_ref: &ContentRef,
        append_offset: Option<u64>,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let deltas = self
            .validate_replace_file(binding.child_inode_id(), content_ref, append_offset)
            .await?;
        self.record_step(op_index, deltas);
        Ok(())
    }

    /// Restores an earlier revision of the file that `binding` binds as a new
    /// current revision.
    pub(crate) async fn restore_revision(
        &mut self,
        binding: &ResolvedBinding,
        source_revision_no: RevisionNo,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let deltas = self
            .validate_restore_revision(binding.child_inode_id(), source_revision_no)
            .await?;
        self.record_step(op_index, deltas);
        Ok(())
    }

    /// Deletes the file or directory subtree that `binding` binds.
    pub(crate) async fn delete(&mut self, binding: &ResolvedBinding) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let deltas = self.validate_delete(binding).await?;
        self.record_step(op_index, deltas);
        Ok(())
    }

    /// Rebinds the inode that `source_binding` binds under
    /// `new_parent_inode_id` as `new_display_name`.
    pub(crate) async fn rename(
        &mut self,
        source_binding: &ResolvedBinding,
        new_parent_inode_id: InodeId,
        new_display_name: &DisplayName,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let deltas = self
            .validate_rename(source_binding, new_parent_inode_id, new_display_name)
            .await?;
        self.record_step(op_index, deltas);
        Ok(())
    }

    /// Recovers a deleted file or subtree: revokes `deletion`, the root's
    /// deletion in the trash, which the request identified by `deletion_seq`
    /// (the delete's committed sequence, which the delete and the change feed
    /// report), and binds the inode under a visible parent directory. Scoping
    /// recovery to the observed position keeps a stale request from
    /// cancelling a later deletion of the same inode.
    pub(crate) async fn restore_deleted(
        &mut self,
        deletion: &SubtreeTombstoneRecord,
        deletion_seq: CommitSeq,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let deltas = self
            .validate_restore_deleted(deletion, deletion_seq, parent_inode_id, display_name)
            .await?;
        self.record_step(op_index, deltas);
        Ok(())
    }

    /// Replaces the attributes of `target` with `attributes`, the complete
    /// map after the update.
    pub(crate) async fn update_attributes(
        &mut self,
        target: &ResolvedTarget<'_>,
        attributes: &Attributes,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let deltas = self
            .validate_update_attributes(target.inode_id(), attributes)
            .await?;
        self.record_step(op_index, deltas);
        Ok(())
    }

    /// Replaces the access row of `target` with `boundary` and `grants`.
    pub(crate) async fn update_access(
        &mut self,
        target: &ResolvedTarget<'_>,
        boundary: bool,
        grants: &AccessGrants,
    ) -> Result<()> {
        let op_index = self.numbering.reserve_op_index()?;
        let deltas = self
            .validate_update_access(target.inode_id(), boundary, grants)
            .await?;
        self.record_step(op_index, deltas);
        Ok(())
    }

    async fn validate_create(
        &mut self,
        child_inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        child: NewChild,
    ) -> Result<Vec<WalDelta>> {
        let name_key = self
            .resolve_name_absent(
                parent_inode_id,
                display_name,
                None,
                CommitOperand::CreateParent,
            )
            .await?;
        self.ensure_child_unbound(child_inode_id, None).await?;
        self.ensure_ancestry_reaches_root(parent_inode_id, CommitOperand::CreateParent)
            .await?;
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
                display_name: display_name.clone(),
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
                display_name: display_name.clone(),
                bind_position,
                child_created_by: self.actor_id.clone(),
                child_created_at_ms: self.committed_at_ms,
                revision_no: RevisionNo(1),
                content_ref,
                layout: None,
                append_offset: None,
            });
        }
        Ok(deltas)
    }

    async fn validate_replace_file(
        &mut self,
        inode_id: InodeId,
        content_ref: &ContentRef,
        append_offset: Option<u64>,
    ) -> Result<Vec<WalDelta>> {
        let current_revision_no = self.resolve_current_revision_no(inode_id).await?;
        let revision_no = next_revision_no(inode_id, current_revision_no)?;
        let edge = self
            .resolve_current_binding(inode_id, CommitOperand::ReplaceTarget)
            .await?;
        Ok(vec![revision_delta(
            &mut self.numbering,
            &edge,
            revision_no,
            content_ref.clone(),
            append_offset,
        )?])
    }

    async fn validate_restore_revision(
        &mut self,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
    ) -> Result<Vec<WalDelta>> {
        let current_revision_no = self.resolve_current_revision_no(inode_id).await?;
        let source_revision = self
            .resolve_restore_source_revision(inode_id, source_revision_no)
            .await?;
        let revision_no = next_revision_no(inode_id, current_revision_no)?;
        let edge = self
            .resolve_current_binding(inode_id, CommitOperand::RestoreTarget)
            .await?;
        Ok(vec![revision_delta(
            &mut self.numbering,
            &edge,
            revision_no,
            source_revision.content_ref,
            None,
        )?])
    }

    async fn validate_delete(&mut self, binding: &ResolvedBinding) -> Result<Vec<WalDelta>> {
        let inode_id = binding.child_inode_id();
        let inode = self.view().inode_at_seq(inode_id).await?.ok_or(
            CommitValidationError::InodeMissing {
                operand: CommitOperand::DeleteTarget,
                inode_id,
            },
        )?;
        delete_deltas(&mut self.numbering, &inode, binding)
    }

    async fn validate_rename(
        &mut self,
        source_binding: &ResolvedBinding,
        new_parent_inode_id: InodeId,
        new_display_name: &DisplayName,
    ) -> Result<Vec<WalDelta>> {
        let inode_id = source_binding.child_inode_id();
        let inode = self.view().inode_at_seq(inode_id).await?.ok_or(
            CommitValidationError::InodeMissing {
                operand: CommitOperand::RenameSource,
                inode_id,
            },
        )?;
        let new_name_key = self
            .resolve_name_absent(
                new_parent_inode_id,
                new_display_name,
                Some(inode_id),
                CommitOperand::RenameTargetParent,
            )
            .await?;
        self.ensure_child_unbound(inode_id, Some(source_binding.bind_position()))
            .await?;
        self.ensure_rename_does_not_cycle(&inode, new_parent_inode_id)
            .await?;
        self.ensure_ancestry_reaches_root(new_parent_inode_id, CommitOperand::RenameTargetParent)
            .await?;
        let revision = self.resolve_revision_summary(&inode).await?;
        Ok(vec![
            unbind_delta(&mut self.numbering, source_binding, &inode)?,
            WalDelta::BindDirentry {
                delta_index: self.numbering.reserve_delta_index()?,
                parent_inode_id: new_parent_inode_id,
                name_key: new_name_key,
                display_name: new_display_name.clone(),
                child_inode_id: inode_id,
                child_kind: inode.inode_kind,
                child_created_by: inode.committed_by,
                child_created_at_ms: inode.committed_at_ms,
                revision,
            },
        ])
    }

    async fn validate_restore_deleted(
        &mut self,
        deletion: &SubtreeTombstoneRecord,
        deletion_seq: CommitSeq,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) -> Result<Vec<WalDelta>> {
        let inode_id = deletion.root_inode_id;
        let inode = self.view().inode_at_seq(inode_id).await?.ok_or(
            CommitValidationError::InodeMissing {
                operand: CommitOperand::RestoreDeletedTarget,
                inode_id,
            },
        )?;
        self.ensure_restorable_deletion(deletion, deletion_seq)?;
        let name_key = self
            .resolve_name_absent(
                parent_inode_id,
                display_name,
                None,
                CommitOperand::RestoreDeletedTarget,
            )
            .await?;
        self.ensure_child_unbound(inode_id, None).await?;
        self.ensure_ancestry_reaches_root(parent_inode_id, CommitOperand::RestoreDeletedTarget)
            .await?;
        let revision = self.resolve_revision_summary(&inode).await?;
        Ok(vec![
            WalDelta::RevokeSubtreeTombstone {
                delta_index: self.numbering.reserve_delta_index()?,
                root_inode_id: inode_id,
                target: deletion.position(),
            },
            WalDelta::BindDirentry {
                delta_index: self.numbering.reserve_delta_index()?,
                parent_inode_id,
                name_key,
                display_name: display_name.clone(),
                child_inode_id: inode_id,
                child_kind: inode.inode_kind,
                child_created_by: inode.committed_by,
                child_created_at_ms: inode.committed_at_ms,
                revision,
            },
        ])
    }

    async fn validate_update_attributes(
        &mut self,
        inode_id: InodeId,
        attributes: &Attributes,
    ) -> Result<Vec<WalDelta>> {
        let current_attributes_revision_no = self
            .view()
            .attributes(inode_id)
            .await?
            .attributes_revision_no;
        let attributes_revision_no =
            next_attributes_revision_no(inode_id, current_attributes_revision_no)?;
        Ok(vec![WalDelta::AppendAttributesRevision {
            delta_index: self.numbering.reserve_delta_index()?,
            inode_id,
            attributes_revision_no,
            attributes: attributes.clone(),
        }])
    }

    async fn validate_update_access(
        &mut self,
        inode_id: InodeId,
        boundary: bool,
        grants: &AccessGrants,
    ) -> Result<Vec<WalDelta>> {
        let current_access_revision_no = self
            .view()
            .latest_access_revision(inode_id)
            .await?
            .map_or(AccessRevisionNo(0), |revision| revision.access_revision_no);
        let access_revision_no = next_access_revision_no(inode_id, current_access_revision_no)?;
        Ok(vec![WalDelta::AppendAccessRevision {
            delta_index: self.numbering.reserve_delta_index()?,
            inode_id,
            access_revision_no,
            boundary,
            grants: grants.clone(),
        }])
    }
}

/// The delta of a file's next revision, which writes the revision's summary
/// row under the file's current binding `edge`.
fn revision_delta(
    numbering: &mut CommitNumbering,
    edge: &DirentryBindingRecord,
    revision_no: RevisionNo,
    content_ref: ContentRef,
    append_offset: Option<u64>,
) -> std::result::Result<WalDelta, CommitValidationError> {
    Ok(WalDelta::AppendFileRevision {
        delta_index: numbering.reserve_delta_index()?,
        inode_id: edge.child_inode_id,
        parent_inode_id: edge.parent_inode_id,
        name_key: edge.name_key.clone(),
        display_name: edge
            .display_name()
            .expect("a current binding should be bound")
            .clone(),
        bind_position: edge.bind_position,
        child_created_by: edge.child_created_by.clone(),
        child_created_at_ms: edge.child_created_at_ms,
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

fn delete_deltas(
    numbering: &mut CommitNumbering,
    inode: &InodeRecord,
    binding: &ResolvedBinding,
) -> Result<Vec<WalDelta>> {
    Ok(vec![
        unbind_delta(numbering, binding, inode)?,
        WalDelta::TombstoneSubtree {
            delta_index: numbering.reserve_delta_index()?,
            root_inode_id: inode.inode_id,
            root_kind: inode.inode_kind,
            deleted_binding: DeletedBinding {
                parent_inode_id: binding.parent_inode_id(),
                name_key: binding.name_key().clone(),
                display_name: binding.display_name().clone(),
            },
        },
    ])
}

fn next_access_revision_no(
    inode_id: InodeId,
    base_access_revision_no: AccessRevisionNo,
) -> std::result::Result<AccessRevisionNo, CommitValidationError> {
    next_public_ordinal(base_access_revision_no.0)
        .map(AccessRevisionNo)
        .ok_or(CommitValidationError::UpdateAccessRevisionOverflow {
            inode_id,
            base_access_revision_no,
        })
}

fn next_revision_no(
    inode_id: InodeId,
    base_revision_no: RevisionNo,
) -> std::result::Result<RevisionNo, CommitValidationError> {
    next_public_ordinal(base_revision_no.0)
        .map(RevisionNo)
        .ok_or(CommitValidationError::RevisionOverflow {
            inode_id,
            base_revision_no,
        })
}

fn next_attributes_revision_no(
    inode_id: InodeId,
    base_attributes_revision_no: AttributesRevisionNo,
) -> std::result::Result<AttributesRevisionNo, CommitValidationError> {
    next_public_ordinal(base_attributes_revision_no.0)
        .map(AttributesRevisionNo)
        .ok_or(CommitValidationError::UpdateAttributesRevisionOverflow {
            inode_id,
            base_attributes_revision_no,
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
