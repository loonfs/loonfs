//! The planner: it turns a request's operations into WAL deltas one operation at a time, and
//! applies each operation's deltas to a working copy of the model state, so that each operation
//! sees what the earlier ones did.

use crate::content_store::{ContentDraft, ContentStore};
use crate::entries::path_lookup_error;
use crate::error::{MountSimError, Result};
use crate::model_view::ModelView;
use crate::state::IdSource;
use loonfs_model::metadata::MetadataState;
use loonfs_model::visibility::PathLookup;
use loonfs_types::api::v0::{FilesystemChange, FilesystemOperation};
use loonfs_types::{
    AbsolutePath, ActorId, ChangeSeq, CommitId, ErrorCode, InodeId, InodeKind, NamespaceId,
    NamespaceNaming, RevisionNo, ROOT_INODE_ID,
};

/// The commit that a planner is building, and the inputs it reads.
pub(super) struct Planner<'a> {
    pub(super) namespace_id: &'a NamespaceId,
    naming: NamespaceNaming,
    /// The sequence that the commit will have.
    pub(super) seq: ChangeSeq,
    pub(super) commit_id: &'a CommitId,
    pub(super) actor: &'a ActorId,
    pub(super) committed_at_ms: u64,
    /// The model state with every delta planned so far applied.
    pub(super) state: MetadataState,
    pub(super) next_delta_index: u32,
    pub(super) next_inode_id: u64,
    pub(super) events: Vec<FilesystemChange>,
    pub(super) content: ContentDraft,
    pub(super) store: &'a ContentStore,
    pub(super) ids: &'a mut IdSource,
}

/// Everything a planned commit changes.
pub(super) struct PlannedCommit {
    pub(super) metadata: MetadataState,
    pub(super) events: Vec<FilesystemChange>,
    pub(super) content: ContentDraft,
    pub(super) next_inode_id: u64,
}

/// The inputs that identify one commit.
pub(super) struct CommitContext<'a> {
    pub(super) namespace_id: &'a NamespaceId,
    pub(super) naming: NamespaceNaming,
    pub(super) seq: ChangeSeq,
    pub(super) commit_id: &'a CommitId,
    pub(super) actor: &'a ActorId,
    pub(super) committed_at_ms: u64,
}

impl<'a> Planner<'a> {
    pub(super) fn new(
        context: CommitContext<'a>,
        state: MetadataState,
        next_inode_id: u64,
        store: &'a ContentStore,
        ids: &'a mut IdSource,
    ) -> Self {
        Self {
            namespace_id: context.namespace_id,
            naming: context.naming,
            seq: context.seq,
            commit_id: context.commit_id,
            actor: context.actor,
            committed_at_ms: context.committed_at_ms,
            state,
            next_delta_index: 0,
            next_inode_id,
            events: Vec::new(),
            content: ContentDraft::default(),
            store,
            ids,
        }
    }

    /// Returns a view of the state that the next operation sees.
    pub(super) fn view(&self) -> ModelView<'_> {
        ModelView {
            state: &self.state,
            seq: self.seq,
            namespace_id: self.namespace_id,
            naming: self.naming,
        }
    }

    /// Plans one operation.
    pub(super) fn plan(&mut self, operation: &FilesystemOperation) -> Result<()> {
        use FilesystemOperation as Op;
        match operation {
            Op::CreateDirectory { path, parents } => self.create_directory(path, *parents),
            Op::PutFile {
                path,
                content_ref,
                inline_content,
                behavior,
                expected_inode_id,
                expected_revision_no,
            } => self.put_file(
                path,
                content_ref.as_ref(),
                inline_content.as_deref(),
                *behavior,
                *expected_inode_id,
                *expected_revision_no,
            ),
            Op::PutFileRevisionByInode {
                inode_id,
                content_ref,
                inline_content,
                expected_revision_no,
            } => self.put_file_revision_by_inode(
                *inode_id,
                content_ref.as_ref(),
                inline_content.as_deref(),
                *expected_revision_no,
            ),
            Op::AppendFile {
                path,
                inline_content,
                expected_inode_id,
                expected_revision_no,
            } => self.append_file(
                path,
                inline_content,
                *expected_inode_id,
                *expected_revision_no,
            ),
            Op::AppendFileByInode {
                inode_id,
                inline_content,
                expected_revision_no,
            } => self.append_file_by_inode(*inode_id, inline_content, *expected_revision_no),
            Op::MovePath {
                source_path,
                destination_path,
                precondition,
            } => self.move_path(source_path, destination_path, precondition),
            Op::MoveByInode {
                inode_id,
                expected_binding_version,
                destination_parent_inode_id,
                destination_display_name,
                precondition,
            } => self.move_by_inode(
                *inode_id,
                expected_binding_version,
                *destination_parent_inode_id,
                destination_display_name,
                precondition,
            ),
            Op::CopyByInode {
                inode_id,
                destination_parent_inode_id,
                destination_display_name,
                precondition,
            } => self.copy_by_inode(
                *inode_id,
                *destination_parent_inode_id,
                destination_display_name,
                precondition,
            ),
            Op::DeletePath {
                path,
                behavior,
                expected_inode_id,
            } => self.delete_path(path, *behavior, *expected_inode_id),
            Op::DeleteByInode {
                inode_id,
                expected_binding_version,
                behavior,
            } => self.delete_by_inode(*inode_id, expected_binding_version, *behavior),
            Op::Undelete {
                inode_id,
                deletion_seq,
                destination_path,
                destination_parent_inode_id,
                destination_display_name,
            } => self.undelete(
                *inode_id,
                *deletion_seq,
                destination_path.as_ref(),
                *destination_parent_inode_id,
                destination_display_name.as_ref(),
            ),
            Op::UpdateAttributes {
                path,
                set,
                remove,
                expected_inode_id,
                expected_attributes_revision_no,
            } => self.update_attributes(
                path,
                set,
                remove,
                *expected_inode_id,
                *expected_attributes_revision_no,
            ),
            Op::UpdateAttributesByInode {
                inode_id,
                set,
                remove,
                expected_attributes_revision_no,
            } => self.update_attributes_by_inode(
                *inode_id,
                set,
                remove,
                *expected_attributes_revision_no,
            ),
            Op::UpdateAccess { .. } | Op::UpdateAccessByInode { .. } => Err(MountSimError::new(
                ErrorCode::NamespaceUnrestricted,
                "the namespace is unrestricted, so it has no access rows",
            )),
            Op::CreateDirectoryByInode { .. } => {
                Err(MountSimError::not_simulated("`create_directory_by_inode`"))
            }
            Op::CreateFileByInode { .. } => {
                Err(MountSimError::not_simulated("`create_file_by_inode`"))
            }
            Op::CopyPath { .. } => Err(MountSimError::not_simulated("`copy_path`")),
            Op::RestoreRevision { .. } => Err(MountSimError::not_simulated("`restore_revision`")),
            Op::RestoreRevisionByInode { .. } => {
                Err(MountSimError::not_simulated("`restore_revision_by_inode`"))
            }
        }
    }

    pub(super) fn finish(self) -> PlannedCommit {
        PlannedCommit {
            metadata: self.state,
            events: self.events,
            content: self.content,
            next_inode_id: self.next_inode_id,
        }
    }

    /// Returns the directory that `path`'s last component is created in, and creates each
    /// missing directory above it, as a put does.
    pub(super) fn ensure_parent_directories(&mut self, path: &AbsolutePath) -> Result<InodeId> {
        let components = path.components();
        let mut current = ROOT_INODE_ID;
        let mut creating = false;
        for component in &components[..components.len().saturating_sub(1)] {
            let display_name = component.to_display_name();
            if !creating {
                if let Some(child) = self.view().visible_child(current, &display_name) {
                    if child.child.inode_kind != InodeKind::Directory {
                        return Err(MountSimError::invalid_request(format!(
                            "path component `{display_name}` isn't a directory"
                        )));
                    }
                    current = child.child.inode_id;
                    continue;
                }
                creating = true;
            }
            current = self.create_directory_at(current, &display_name);
        }
        Ok(current)
    }

    /// Returns the existing directory that `path`'s last component would be bound in.
    pub(super) fn parent_directory(&self, path: &AbsolutePath) -> Result<InodeId> {
        let parent_path = path.parent().unwrap_or_else(AbsolutePath::root);
        let view = self.view();
        match view.resolve_path(&parent_path) {
            PathLookup::Found { inode, .. } if inode.inode_kind == InodeKind::Directory => {
                Ok(inode.inode_id)
            }
            PathLookup::Found { .. } => {
                Err(path_conflict(format!("`{parent_path}` isn't a directory")))
            }
            lookup => Err(path_lookup_error(&lookup)),
        }
    }
}

pub(super) fn root_mutation() -> MountSimError {
    MountSimError::invalid_request("the root can't be the target of this operation")
}

pub(super) fn path_conflict(message: impl Into<String>) -> MountSimError {
    MountSimError::new(ErrorCode::PathConflict, message)
}

/// Returns the error for an `expected_inode_id` that the target doesn't match.
pub(super) fn ensure_expected_inode(expected: Option<InodeId>, actual: InodeId) -> Result<()> {
    match expected {
        Some(expected) if expected != actual => Err(path_conflict(format!(
            "the target is inode `{}`, not the expected `{}`",
            loonfs_types::public_inode_id::encode(actual),
            loonfs_types::public_inode_id::encode(expected)
        ))
        .with_details(|details| {
            details.expected_inode_id = Some(expected);
            details.actual_inode_id = Some(actual);
        })),
        _ => Ok(()),
    }
}

/// Returns the error for an `expected_revision_no` that isn't the file's current revision.
pub(super) fn stale_revision(
    inode_id: InodeId,
    expected: RevisionNo,
    actual: Option<RevisionNo>,
) -> MountSimError {
    MountSimError::new(
        ErrorCode::StaleRevision,
        format!("the file's current revision isn't the expected revision {expected}"),
    )
    .with_details(|details| {
        details.inode_id = Some(inode_id);
        details.expected_revision_no = Some(expected);
        details.actual_revision_no = actual;
    })
}

/// Checks that a file is still at `expected`, when the caller states an expected revision.
pub(super) fn ensure_expected_revision(
    view: &ModelView<'_>,
    inode_id: InodeId,
    expected: Option<RevisionNo>,
) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let actual = view.current_revision(inode_id).map(|row| row.revision_no);
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(stale_revision(inode_id, expected, actual))
    }
}
