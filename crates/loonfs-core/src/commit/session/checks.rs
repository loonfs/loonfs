//! The rules that the writes of a commit check against the commit's state,
//! and the reads of the rows that the writes copy into their deltas.

use super::commit_session::CommitSession;
use crate::commit::CommitValidationError;
use crate::error::{CoreError, Result};
use crate::metadata::{InodeRecord, RevisionRecord};
use crate::path::write::DeletionInTrash;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{DeltaPosition, RevisionSummary};

use loonfs_types::{CommitSeq, InodeId, InodeKind, RevisionNo};

impl<S: ObjectStore + ?Sized> CommitSession<'_, S> {
    /// Ensures that `deletion`, the root's deletion in the trash, is the
    /// deletion that `deletion_seq` identifies, that `deletion_seq` is above
    /// the retention floor of the writer's basis, and that an earlier commit
    /// recorded it
    /// ([format section 1.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#16-file-and-subtree-deletion)).
    /// A request for a deletion that has expired, that a later event revoked,
    /// or that a later deletion replaced, fails here, so it can't cancel a
    /// different deletion.
    ///
    /// A base compaction relies on the floor check. Only the writer's own
    /// epoch raises the floor that the writer checks against, so no restore
    /// of a deletion at or below the floor is accepted after the floor moves.
    pub(super) fn ensure_restorable_deletion(
        &self,
        deletion: &DeletionInTrash,
        deletion_seq: CommitSeq,
    ) -> Result<()> {
        let inode_id = deletion.root_inode_id();
        let deletion_committed_seq = deletion.position().seq;
        if deletion_seq <= self.view().retention_floor_seq() {
            return Err(CommitValidationError::RestoreDeletedTargetNotDeleted { inode_id }.into());
        }
        if deletion_committed_seq >= self.committed_seq {
            return Err(
                CommitValidationError::RestoreDeletedTargetsCurrentCommit { inode_id }.into(),
            );
        }
        if deletion_committed_seq != deletion_seq {
            return Err(CommitValidationError::RestoreDeletedSequenceMismatch {
                inode_id,
                requested_seq: deletion_seq,
                actual_deletion_seq: deletion_committed_seq,
            }
            .into());
        }
        Ok(())
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

    /// Returns the inode record of `inode_id`, whose kind, creator and
    /// creation time a restore of a deleted item copies into its binding.
    ///
    /// The planner found the inode's deletion in the trash, so an inode
    /// without a record means that the namespace is corrupt.
    pub(super) async fn resolve_inode(&self, inode_id: InodeId) -> Result<InodeRecord> {
        self.view().inode_at_seq(inode_id).await?.ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!("inode `{inode_id}` has no inode record"))
        })
    }

    /// Returns the current revision of the file `inode_id`, which a write
    /// numbers the file's next revision after, or copies into a new binding
    /// of the file. A file has a revision from its create on, so a file
    /// without one means the namespace is corrupt.
    pub(super) async fn resolve_current_revision(
        &self,
        inode_id: InodeId,
    ) -> Result<RevisionRecord> {
        self.view()
            .latest_revision_record(inode_id)
            .await?
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!("file `{inode_id}` has no current revision"))
            })
    }

    /// Returns the revision summary that a bind of `inode_id`, of the kind
    /// `inode_kind`, copies into the new edge, which is `None` for a
    /// directory.
    pub(super) async fn resolve_revision_summary(
        &self,
        inode_id: InodeId,
        inode_kind: InodeKind,
    ) -> Result<Option<RevisionSummary>> {
        if inode_kind == InodeKind::Directory {
            return Ok(None);
        }
        let revision = self.resolve_current_revision(inode_id).await?;
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
    /// checked against the floor leaves them alone, so none copies a
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
        inode_id: InodeId,
        inode_kind: InodeKind,
        new_parent_inode_id: InodeId,
    ) -> Result<()> {
        if inode_kind == InodeKind::Directory
            && self
                .view()
                .would_create_directory_cycle(inode_id, new_parent_inode_id)
                .await?
        {
            return Err(CommitValidationError::RenameWouldCycleDirectory {
                inode_id,
                new_parent_inode_id,
            }
            .into());
        }
        Ok(())
    }
}
