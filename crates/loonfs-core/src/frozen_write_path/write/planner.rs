//! The planning of one filesystem operation into the commit operations it
//! needs.

use super::planned_pieces::CommitPieces;
use super::preconditions::{
    validate_expected_file_state, validate_revision_precondition, PreconditionFields,
};
use super::publish_path_planning::{
    CompiledFilesystemOperation, Destination, PlanningView, Target,
};
use crate::commit::engine::TemporaryMove;
use crate::error::{CoreError, Result};
use crate::frozen_write_path::ops::NewChild;
use crate::path::write::FilesystemOperation;
use crate::storage::tail_content::ContentWrites;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{CommitSeq, DestinationBehavior, FileContent, InodeId};
use std::num::NonZeroU32;

/// The planning of one operation: the planning view, the commit's pieces,
/// where content writes go, the commit's sequence, and where the IDs of the
/// inodes it creates come from.
pub(super) struct OperationPlanningSession<'p, S: ObjectStore + ?Sized> {
    pub(super) view: PlanningView<'p, S>,
    pub(super) pieces: &'p CommitPieces<'p, S>,
    pub(super) content_writes: ContentWrites<'p, S>,
    /// The sequence that the commit is assigned.
    pub(super) committed_seq: CommitSeq,
    pub(super) created_inode_ids: CreatedInodeIds<'p>,
    /// The leg of a merge's move through a temporary name that the operation
    /// is, if it's one.
    pub(super) temporary_move: Option<TemporaryMove>,
}

/// Where the IDs of the inodes that one operation creates come from.
pub(super) enum CreatedInodeIds<'p> {
    /// Each ID is built from the commit's sequence and the create number of
    /// the next inode, which every operation in the commit shares.
    Generated { next_create_no: &'p mut NonZeroU32 },
    /// The ID that the inode has in the fork, for a merge's operation, which
    /// creates at most one inode. The operation takes the ID when it creates
    /// the inode.
    Merge(Option<InodeId>),
}

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, S> {
    /// Returns the ID of the next inode that the commit creates.
    ///
    /// A merge whose operation creates an inode without an assigned ID, or
    /// more than one inode, fails with `InvalidCommitRequest`. Only a bug in
    /// the merge's planner causes that. A panic would return
    /// `outcome_unknown` and panic again on every retry, so the merge
    /// fails with an error that the caller doesn't retry.
    pub(super) fn next_inode_id(&mut self) -> Result<InodeId> {
        match &mut self.created_inode_ids {
            CreatedInodeIds::Generated { next_create_no } => {
                let create_no = **next_create_no;
                **next_create_no = create_no.checked_add(1).expect(
                    "a commit should create fewer inodes than its 32-bit delta positions allow",
                );
                Ok(InodeId::generate(self.committed_seq, create_no))
            }
            CreatedInodeIds::Merge(assigned) => assigned.take().ok_or_else(|| {
                CoreError::InvalidCommitRequest(
                    "a merge operation creates an inode but has no assigned inode ID".to_owned(),
                )
            }),
        }
    }

    /// Plans `operation`, mapping its path or inode address onto the one
    /// planner for its kind.
    pub(super) async fn plan(
        &mut self,
        operation: &FilesystemOperation,
    ) -> Result<CompiledFilesystemOperation> {
        use FilesystemOperation as Op;
        match operation {
            Op::CreateDirectory { path, parents } => {
                self.plan_create_directory(path, *parents).await
            }
            Op::CreateDirectoryByInode {
                parent_inode_id,
                display_name,
            } => {
                let destination = Destination::name(*parent_inode_id, display_name);
                self.plan_create(destination, NewChild::Directory).await
            }
            Op::CreateFileByInode {
                parent_inode_id,
                display_name,
                content,
            } => {
                let child = NewChild::File(prepared_reference(content)?.clone());
                let destination = Destination::name(*parent_inode_id, display_name);
                self.plan_create(destination, child).await
            }
            Op::PutFile {
                path,
                content,
                behavior,
                expected_inode_id,
                expected_revision_no,
            } => {
                let expected = validate_expected_file_state(
                    *behavior,
                    *expected_inode_id,
                    *expected_revision_no,
                    PreconditionFields::Put,
                )?;
                let content_ref = prepared_reference(content)?.clone();
                self.plan_put_file_content_ref(path, content_ref, *behavior, expected)
                    .await
            }
            Op::PutFileRevisionByInode {
                inode_id,
                content,
                expected_revision_no,
            } => {
                let content_ref = prepared_reference(content)?.clone();
                self.plan_put_file_revision_by_inode(*inode_id, content_ref, *expected_revision_no)
                    .await
            }
            Op::AppendFile {
                path,
                inline_content,
                expected_inode_id,
                expected_revision_no,
            } => {
                let expected = validate_expected_file_state(
                    DestinationBehavior::Replace,
                    *expected_inode_id,
                    *expected_revision_no,
                    PreconditionFields::Put,
                )?;
                let target = Target::path(path, expected.map(|expected| expected.inode_id));
                let expected_revision_no = expected.and_then(|expected| expected.revision_no);
                self.plan_append(target, expected_revision_no, inline_content)
                    .await
            }
            Op::AppendFileByInode {
                inode_id,
                inline_content,
                expected_revision_no,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_append(target, *expected_revision_no, inline_content)
                    .await
            }
            Op::DeletePath {
                path,
                behavior,
                expected_inode_id,
            } => {
                let target = Target::path(path, *expected_inode_id);
                self.plan_delete(target, None, *behavior).await
            }
            Op::DeleteByInode {
                inode_id,
                expected_binding_version,
                behavior,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_delete(target, Some(expected_binding_version), *behavior)
                    .await
            }
            Op::MovePath {
                source_path,
                destination_path,
                precondition,
            } => {
                let source = Target::path(source_path, None);
                let destination = Destination::Path(destination_path);
                self.plan_move(source, None, destination, precondition)
                    .await
            }
            Op::MoveByInode {
                inode_id,
                expected_binding_version,
                destination_parent_inode_id,
                destination_display_name,
                precondition,
            } => {
                let source = Target::Inode(*inode_id);
                let destination =
                    Destination::name(*destination_parent_inode_id, destination_display_name);
                self.plan_move(
                    source,
                    Some(expected_binding_version),
                    destination,
                    precondition,
                )
                .await
            }
            Op::CopyPath {
                source_path,
                destination_path,
                precondition,
            } => {
                let source = Target::path(source_path, None);
                let destination = Destination::Path(destination_path);
                self.plan_copy(source, destination, precondition).await
            }
            Op::CopyByInode {
                inode_id,
                destination_parent_inode_id,
                destination_display_name,
                precondition,
            } => {
                let source = Target::Inode(*inode_id);
                let destination =
                    Destination::name(*destination_parent_inode_id, destination_display_name);
                self.plan_copy(source, destination, precondition).await
            }
            Op::RestoreRevision {
                path,
                source_revision_no,
            } => {
                let target = Target::path(path, None);
                self.plan_restore_revision(target, *source_revision_no)
                    .await
            }
            Op::RestoreRevisionByInode {
                inode_id,
                source_revision_no,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_restore_revision(target, *source_revision_no)
                    .await
            }
            Op::RestoreDeleted {
                inode_id,
                deletion_seq,
                destination,
            } => {
                self.plan_restore_deleted(*inode_id, *deletion_seq, destination.clone())
                    .await
            }
            Op::UpdateAttributes {
                path,
                set,
                remove,
                expected_inode_id,
                expected_attributes_revision_no: expected,
            } => {
                validate_revision_precondition(
                    *expected_inode_id,
                    expected.is_some(),
                    "expected_attributes_revision_no",
                )?;
                let target = Target::path(path, *expected_inode_id);
                self.plan_update_attributes(target, set, remove, *expected)
                    .await
            }
            Op::UpdateAttributesByInode {
                inode_id,
                set,
                remove,
                expected_attributes_revision_no: expected,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_update_attributes(target, set, remove, *expected)
                    .await
            }
            Op::UpdateAccess {
                path,
                boundary,
                grants,
                expected_inode_id,
                expected_access_revision_no: expected,
            } => {
                validate_revision_precondition(
                    *expected_inode_id,
                    expected.is_some(),
                    "expected_access_revision_no",
                )?;
                let target = Target::path(path, *expected_inode_id);
                self.plan_update_access(target, *boundary, grants, *expected)
                    .await
            }
            Op::UpdateAccessByInode {
                inode_id,
                boundary,
                grants,
                expected_access_revision_no: expected,
            } => {
                let target = Target::Inode(*inode_id);
                self.plan_update_access(target, *boundary, grants, *expected)
                    .await
            }
        }
    }
}

fn prepared_reference(content: &FileContent) -> Result<&loonfs_types::ContentRef> {
    match content {
        FileContent::Ref(reference) => Ok(reference),
        FileContent::Inline(_) => Err(CoreError::InvalidCommitRequest(
            "content must be prepared before planning".to_owned(),
        )),
    }
}
