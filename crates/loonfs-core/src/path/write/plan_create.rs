//! Publish plans that create, recover, or replace path content.

use super::planner::OperationPlanningSession;
use super::preconditions::{ensure_expected_inode, ExpectedFileState};
use super::publish_path_planning::{
    is_missing_visible_path, CompiledFilesystemOperation, Destination, Target,
};
use crate::authorize::Absence;
use crate::commit::{CommitOperation, CommitValidationError, NewChild};
use crate::error::{CoreError, Result};
use crate::metadata::VisiblePathError;
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::TombstoneRowAction;
use loonfs_types::options::RestoreDeletedDestination;
use loonfs_types::{
    AbsolutePath, AccessRight, CommitSeq, ContentRef, DestinationBehavior, DisplayName, InodeId,
    InodeKind, NameKey, RevisionNo, ROOT_INODE_ID,
};

pub(super) struct ResolvedParents {
    /// The directory the new entry is bound under, possibly created here.
    pub(super) parent_inode_id: InodeId,
    /// The deepest directory that already existed, whose `create` right
    /// covers every directory created below it.
    pub(super) deepest_existing: InodeId,
}

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, S> {
    /// Plans a new directory or file at a vacant `destination`.
    pub(super) async fn plan_create(
        &mut self,
        destination: Destination<'_>,
        child: NewChild,
    ) -> Result<CompiledFilesystemOperation> {
        destination.ensure_mutation()?;
        let destination = self.view.resolve_destination(destination).await?;
        self.view.ensure_vacant_destination(&destination).await?;
        let child_inode_id = self.next_inode_id()?;
        let parent_inode_id = destination.parent_inode_id;
        let display_name = destination.name;
        let op = match child {
            NewChild::Directory => CommitOperation::CreateDirectory {
                child_inode_id,
                parent_inode_id,
                display_name,
            },
            NewChild::File(content_ref) => CommitOperation::CreateFile {
                child_inode_id,
                parent_inode_id,
                display_name,
                content_ref,
            },
        };
        Ok(CompiledFilesystemOperation::new(vec![op]))
    }

    /// Plans a new directory at `absolute_path`, and with `parents`, the
    /// missing directories above it.
    pub(super) async fn plan_create_directory(
        &mut self,
        absolute_path: &AbsolutePath,
        parents: bool,
    ) -> Result<CompiledFilesystemOperation> {
        if !parents {
            return self
                .plan_create(Destination::Path(absolute_path), NewChild::Directory)
                .await;
        }
        ensure_mutation_path(absolute_path)?;
        let mut ops = Vec::new();
        let parents = self
            .ensure_parent_directories(absolute_path, &mut ops)
            .await?;
        self.view
            .authorize(
                parents.deepest_existing,
                AccessRight::Create.into(),
                Absence::Path(absolute_path.as_str()),
            )
            .await?;
        self.view.ensure_vacant_path(absolute_path).await?;
        let display_name = final_component(absolute_path)?;
        let child_inode_id = self.next_inode_id()?;
        ops.push(CommitOperation::CreateDirectory {
            child_inode_id,
            parent_inode_id: parents.parent_inode_id,
            display_name,
        });
        Ok(CompiledFilesystemOperation::new(ops))
    }

    pub(super) async fn plan_restore_deleted(
        &self,
        inode_id: InodeId,
        deletion_seq: CommitSeq,
        destination: RestoreDeletedDestination,
    ) -> Result<CompiledFilesystemOperation> {
        let deletion = self.view.metadata.recoverable_deletion(inode_id).await?;
        let deleted_binding = match deletion.map(|record| record.action) {
            Some(TombstoneRowAction::Set {
                deleted_binding, ..
            }) => deleted_binding,
            _ => {
                let authorization_target = self
                    .view
                    .metadata
                    .current_parent_binding_for_child(inode_id)
                    .await?
                    .map_or(inode_id, |binding| binding.parent_inode_id);
                self.view
                    .authorize(
                        authorization_target,
                        AccessRight::Remove.into(),
                        Absence::Inode,
                    )
                    .await?;
                return Err(
                    CommitValidationError::RestoreDeletedTargetNotDeleted { inode_id }.into(),
                );
            }
        };
        let saved_parent = deleted_binding.parent_inode_id;
        self.view
            .authorize(saved_parent, AccessRight::Remove.into(), Absence::Inode)
            .await?;
        let (parent_inode_id, display_name) = match destination {
            RestoreDeletedDestination::Path(absolute_path) => {
                self.resolve_restore_deleted_destination(
                    inode_id,
                    saved_parent,
                    Destination::Path(&absolute_path),
                )
                .await?
            }
            RestoreDeletedDestination::Name {
                parent_inode_id,
                display_name,
            } => {
                let destination = Destination::Name {
                    parent_inode_id,
                    display_name: &display_name,
                };
                self.resolve_restore_deleted_destination(inode_id, saved_parent, destination)
                    .await?
            }
            RestoreDeletedDestination::Recorded => {
                self.view
                    .authorize(saved_parent, AccessRight::Create.into(), Absence::Inode)
                    .await?;
                (saved_parent, deleted_binding.display_name)
            }
        };
        Ok(CompiledFilesystemOperation::new(vec![
            CommitOperation::RestoreDeleted {
                inode_id,
                deletion_seq,
                parent_inode_id,
                display_name,
            },
        ]))
    }

    /// Resolves the new destination of a restore, which has to be vacant,
    /// and authorizes moving the deleted inode there from its saved parent.
    async fn resolve_restore_deleted_destination(
        &self,
        inode_id: InodeId,
        saved_parent: InodeId,
        destination: Destination<'_>,
    ) -> Result<(InodeId, DisplayName)> {
        destination.ensure_mutation()?;
        let destination = self.view.resolve_destination(destination).await?;
        self.view.ensure_vacant_destination(&destination).await?;
        self.view
            .authorize_relocation(inode_id, saved_parent, destination.parent_inode_id)
            .await?;
        Ok((destination.parent_inode_id, destination.name))
    }

    pub(super) async fn plan_put_file_revision_by_inode(
        &self,
        inode_id: InodeId,
        content_ref: ContentRef,
        expected_revision_no: RevisionNo,
    ) -> Result<CompiledFilesystemOperation> {
        let target = self
            .view
            .resolve_authorized_target(Target::Inode(inode_id), AccessRight::Write.into())
            .await?;
        target.ensure_file()?;
        Ok(CompiledFilesystemOperation::new(vec![
            CommitOperation::ReplaceFile {
                inode_id,
                base_revision_no: expected_revision_no,
                content_ref,
                append_offset: None,
            },
        ]))
    }

    pub(super) async fn plan_put_file_content_ref(
        &mut self,
        absolute_path: &AbsolutePath,
        content_ref: ContentRef,
        behavior: DestinationBehavior,
        expected_file_state: Option<ExpectedFileState>,
    ) -> Result<CompiledFilesystemOperation> {
        ensure_mutation_path(absolute_path)?;
        let target = self
            .view
            .resolve_visible_path_for_authorization(
                absolute_path,
                AccessRight::Create.into(),
                Absence::Path(absolute_path.as_str()),
            )
            .await;

        let mut ops = Vec::new();
        let final_name = final_component(absolute_path)?;

        match target {
            Ok(existing) => {
                if behavior == DestinationBehavior::NoReplace {
                    self.view
                        .authorize(
                            existing.parent_inode_id.unwrap_or(ROOT_INODE_ID),
                            AccessRight::Create.into(),
                            Absence::Path(absolute_path.as_str()),
                        )
                        .await?;
                    return Err(CoreError::DestinationExists {
                        path: absolute_path.as_str().to_owned(),
                        existing_display_name: Some(existing.display_name.clone()),
                    });
                }
                self.view
                    .authorize(
                        existing.inode_id,
                        AccessRight::Write.into(),
                        Absence::Path(absolute_path.as_str()),
                    )
                    .await?;
                ensure_expected_inode(
                    &existing,
                    expected_file_state.map(|expected| expected.inode_id),
                    &final_name,
                )?;
                if existing.inode_kind != InodeKind::File {
                    return Err(CoreError::ExpectedFile {
                        target: absolute_path.as_str().to_owned(),
                        kind: existing.inode_kind,
                    });
                }
                let revision = self
                    .view
                    .metadata
                    .latest_revision_head(existing.inode_id)
                    .await?
                    .ok_or_else(|| CoreError::PathNotFound(absolute_path.as_str().to_owned()))?;
                let base_revision_no = expected_file_state
                    .and_then(|expected| expected.revision_no)
                    .unwrap_or(revision.revision_no);
                ops.push(CommitOperation::ReplaceFile {
                    inode_id: existing.inode_id,
                    base_revision_no,
                    content_ref: content_ref.clone(),
                    append_offset: None,
                });
            }
            Err(error) if is_missing_visible_path(&error) => {
                let parents = self
                    .ensure_parent_directories(absolute_path, &mut ops)
                    .await?;
                self.view
                    .authorize(
                        parents.deepest_existing,
                        AccessRight::Create.into(),
                        Absence::Path(absolute_path.as_str()),
                    )
                    .await?;
                if expected_file_state.is_some() {
                    return Err(CoreError::PathNotFound(absolute_path.as_str().to_owned()));
                }
                let child_inode_id = self.next_inode_id()?;
                ops.push(CommitOperation::CreateFile {
                    child_inode_id,
                    parent_inode_id: parents.parent_inode_id,
                    display_name: final_name.clone(),
                    content_ref,
                });
            }
            Err(other) => return Err(other),
        }

        Ok(CompiledFilesystemOperation::new(ops))
    }

    async fn ensure_parent_directories(
        &mut self,
        absolute_path: &AbsolutePath,
        ops: &mut Vec<CommitOperation>,
    ) -> Result<ResolvedParents> {
        let components = absolute_path.components();
        if components.len() <= 1 {
            return Ok(ResolvedParents {
                parent_inode_id: ROOT_INODE_ID,
                deepest_existing: ROOT_INODE_ID,
            });
        }

        let mut current_inode = ROOT_INODE_ID;
        let mut current_path = AbsolutePath::root();
        let mut creating_missing_ancestors = false;
        let mut deepest_existing = current_inode;
        for component in &components[..components.len() - 1] {
            let display_name = component.to_display_name();
            let name_key = NameKey::for_display_name(&display_name);
            if !creating_missing_ancestors {
                if let Some(child) = self
                    .view
                    .metadata
                    .visible_child(current_inode, &name_key)
                    .await?
                {
                    current_path = current_path.join(
                        child
                            .display_name()
                            .expect("visible binding should be bound"),
                    );
                    if child.child_kind != InodeKind::Directory {
                        self.view
                            .authorize(
                                child.child_inode_id,
                                AccessRight::Create.into(),
                                Absence::Path(absolute_path.as_str()),
                            )
                            .await?;
                        return Err(VisiblePathError::PathComponentNotDirectory {
                            absolute_path: current_path.to_string(),
                            inode_id: child.child_inode_id,
                            inode_kind: child.child_kind,
                        }
                        .into());
                    }
                    current_inode = child.child_inode_id;
                    continue;
                }
                deepest_existing = current_inode;
                creating_missing_ancestors = true;
            }

            let child_inode_id = self.next_inode_id()?;
            ops.push(CommitOperation::CreateDirectory {
                child_inode_id,
                parent_inode_id: current_inode,
                display_name,
            });
            current_inode = child_inode_id;
        }
        if !creating_missing_ancestors {
            deepest_existing = current_inode;
        }
        Ok(ResolvedParents {
            parent_inode_id: current_inode,
            deepest_existing,
        })
    }
}
