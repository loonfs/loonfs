use super::frame::{validate_commit_request_frame, validate_commit_request_frame_parts};
use super::metadata_preview::{MetadataPreview, PublishMetadataPreview};
use super::{
    push_unique_invariant, CommitOp, CommitPlan, CommitRequest, CommitValidationContext,
    CommitValidationError, Precondition, ResolvedBinding, ValidatedOp,
};
use crate::invariants::InvariantId;
use crate::metadata::MetadataState;
use crate::path::read::CurrentManifestTailView;
use crate::{error::CoreError, metadata::RevisionRecord};
use loonfs_api::wire::control::{HeadState, LeaseState};
use loonfs_api::{
    name_key_for_display_name, ChangeSeq, ContentRef, DisplayName, InodeId, InodeKind, NamePolicy,
    RevisionNo,
};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeMap;

struct CommitShape {
    assigned_seq: ChangeSeq,
    allocated_inode_ids: Vec<InodeId>,
    resulting_next_inode_id: InodeId,
}

struct ValidatedMetadataOps {
    validated_ops: Vec<ValidatedOp>,
}

#[derive(Clone, Copy)]
pub(crate) struct PublishCommitValidationContext<'a, S: ObjectStore + ?Sized> {
    pub(crate) head: &'a HeadState,
    pub(crate) lease: &'a LeaseState,
    pub(crate) now_ms: u64,
    pub(crate) metadata_view: CurrentManifestTailView<'a, S>,
    pub(crate) accepted_rows: &'a MetadataState,
}

impl<'a, S: ObjectStore + ?Sized> PublishCommitValidationContext<'a, S> {
    fn preview(&self) -> PublishMetadataPreview<'a, S> {
        PublishMetadataPreview::new(self.metadata_view, self.accepted_rows)
    }
}

pub(crate) async fn resolve_restore_content_refs_for_publish<S: ObjectStore + ?Sized>(
    request: &CommitRequest,
    context: &PublishCommitValidationContext<'_, S>,
) -> Result<Vec<Option<ContentRef>>, CoreError> {
    let mut resolved_request_revisions = BTreeMap::<(InodeId, RevisionNo), ContentRef>::new();
    let preview = context.preview();
    let mut resolved = Vec::with_capacity(request.ops.len());

    for op in &request.ops {
        match op {
            CommitOp::ReplaceFile {
                inode_id,
                base_revision_no,
                content_ref,
            } => {
                if let Some(next_revision) = base_revision_no.0.checked_add(1).map(RevisionNo) {
                    resolved_request_revisions
                        .insert((*inode_id, next_revision), content_ref.clone());
                }
                resolved.push(None);
            }
            CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            } => {
                let content_ref = if let Some(content_ref) =
                    resolved_request_revisions.get(&(*inode_id, *source_revision_no))
                {
                    Some(content_ref.clone())
                } else {
                    preview
                        .revision_at_seq(*inode_id, *source_revision_no, context.head.seq)
                        .await?
                        .map(|revision| revision.content_ref)
                };
                if let (Some(next_revision), Some(content_ref)) = (
                    base_revision_no.0.checked_add(1).map(RevisionNo),
                    content_ref.clone(),
                ) {
                    resolved_request_revisions.insert((*inode_id, next_revision), content_ref);
                }
                resolved.push(content_ref);
            }
            _ => resolved.push(None),
        }
    }

    Ok(resolved)
}

pub fn build_commit_plan(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<CommitPlan, CommitValidationError> {
    validate_commit_request_frame(request, context)?;

    let mut checked_invariants = vec![
        InvariantId::StaleWriterCannotPublish,
        InvariantId::HeadAndLeaseFenceTokensAgree,
        InvariantId::NextInodeIdIsMonotonic,
    ];
    let shape = compute_commit_shape(request, context)?;
    let validated_metadata = validate_metadata_preconditions(
        request,
        context.metadata_state,
        shape.assigned_seq,
        &shape.allocated_inode_ids,
        context.head.name_policy,
        &mut checked_invariants,
    )?;

    if !shape.allocated_inode_ids.is_empty() {
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::CreateMutationConsumesNextInodeId,
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::CreateFile { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::CreateFileRequiresDurableContent,
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::ReplaceFile { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::ReplaceFileRequiresDurableContent,
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::RestoreRevision { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::RestoreRevisionRequiresDurableContent,
        );
    }

    Ok(CommitPlan {
        namespace_id: request.namespace_id.clone(),
        commit_id: request.commit_id.clone(),
        apply_after_seq: context.head.seq,
        assigned_seq: shape.assigned_seq,
        validated_ops: validated_metadata.validated_ops,
        resulting_next_inode_id: shape.resulting_next_inode_id,
        checked_invariants,
    })
}

pub(crate) async fn build_commit_plan_for_publish<S: ObjectStore + ?Sized>(
    request: &CommitRequest,
    context: &PublishCommitValidationContext<'_, S>,
) -> Result<CommitPlan, CoreError> {
    validate_commit_request_frame_parts(request, context.head, context.lease, context.now_ms)?;

    let mut checked_invariants = vec![
        InvariantId::StaleWriterCannotPublish,
        InvariantId::HeadAndLeaseFenceTokensAgree,
        InvariantId::NextInodeIdIsMonotonic,
    ];
    let shape = compute_commit_shape_from_head(request, context.head)?;
    let validated_metadata = validate_publish_metadata_preconditions(
        request,
        context.preview(),
        shape.assigned_seq,
        &shape.allocated_inode_ids,
        context.head.name_policy,
        &mut checked_invariants,
    )
    .await?;

    if !shape.allocated_inode_ids.is_empty() {
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::CreateMutationConsumesNextInodeId,
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::CreateFile { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::CreateFileRequiresDurableContent,
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::ReplaceFile { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::ReplaceFileRequiresDurableContent,
        );
    }
    if request
        .ops
        .iter()
        .any(|op| matches!(op, CommitOp::RestoreRevision { .. }))
    {
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::RestoreRevisionRequiresDurableContent,
        );
    }

    Ok(CommitPlan {
        namespace_id: request.namespace_id.clone(),
        commit_id: request.commit_id.clone(),
        apply_after_seq: context.head.seq,
        assigned_seq: shape.assigned_seq,
        validated_ops: validated_metadata.validated_ops,
        resulting_next_inode_id: shape.resulting_next_inode_id,
        checked_invariants,
    })
}

fn compute_commit_shape(
    request: &CommitRequest,
    context: &CommitValidationContext,
) -> Result<CommitShape, CommitValidationError> {
    compute_commit_shape_from_head(request, &context.head)
}

fn compute_commit_shape_from_head(
    request: &CommitRequest,
    head: &HeadState,
) -> Result<CommitShape, CommitValidationError> {
    let assigned_seq = head
        .seq
        .0
        .checked_add(1)
        .map(ChangeSeq)
        .ok_or(CommitValidationError::SeqOverflow)?;
    let create_op_count = request
        .ops
        .iter()
        .filter(|op| matches!(op, CommitOp::CreateDir { .. } | CommitOp::CreateFile { .. }))
        .count();
    let allocated_inode_ids = (0..create_op_count)
        .map(|offset| {
            let offset =
                u64::try_from(offset).map_err(|_| CommitValidationError::NextInodeOverflow)?;
            head.next_inode_id
                .0
                .checked_add(offset)
                .map(InodeId)
                .ok_or(CommitValidationError::NextInodeOverflow)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let resulting_next_inode_id = head
        .next_inode_id
        .0
        .checked_add(
            u64::try_from(create_op_count).map_err(|_| CommitValidationError::NextInodeOverflow)?,
        )
        .map(InodeId)
        .ok_or(CommitValidationError::NextInodeOverflow)?;

    Ok(CommitShape {
        assigned_seq,
        allocated_inode_ids,
        resulting_next_inode_id,
    })
}

fn validate_metadata_preconditions(
    request: &CommitRequest,
    metadata_state: &MetadataState,
    committed_seq: ChangeSeq,
    allocated_inode_ids: &[InodeId],
    name_policy: NamePolicy,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<ValidatedMetadataOps, CommitValidationError> {
    let mut ephemeral_metadata_state = MetadataPreview::new(metadata_state);
    let mut allocated_inode_ids = allocated_inode_ids.iter().copied();
    let mut validated_ops = Vec::with_capacity(request.ops.len());
    let mut next_delta_index = 0u32;

    validate_explicit_preconditions(
        &request.preconditions,
        &ephemeral_metadata_state,
        committed_seq,
        checked_invariants,
    )?;

    for (op_index, op) in request.ops.iter().enumerate() {
        let op_index =
            u32::try_from(op_index).map_err(|_| CommitValidationError::OpIndexOverflow)?;
        let validated_op = match op {
            CommitOp::CreateDir {
                parent_inode,
                display_name,
            } => {
                let name_key = validate_child_name_absent(
                    &ephemeral_metadata_state,
                    *parent_inode,
                    display_name,
                    committed_seq,
                    name_policy,
                )?;
                validate_ancestors_not_subtree_deleted(
                    &ephemeral_metadata_state,
                    *parent_inode,
                    committed_seq,
                    checked_invariants,
                    true,
                )?;
                ValidatedOp::CreateDir {
                    op_index,
                    parent_inode: *parent_inode,
                    display_name: display_name.clone(),
                    name_key,
                    child_inode: next_allocated_inode(
                        &mut allocated_inode_ids,
                        CommitValidationError::NextInodeOverflow,
                    )?,
                    create_inode_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    bind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::CreateFile {
                parent_inode,
                display_name,
                content_ref,
            } => {
                let name_key = validate_child_name_absent(
                    &ephemeral_metadata_state,
                    *parent_inode,
                    display_name,
                    committed_seq,
                    name_policy,
                )?;
                validate_ancestors_not_subtree_deleted(
                    &ephemeral_metadata_state,
                    *parent_inode,
                    committed_seq,
                    checked_invariants,
                    true,
                )?;
                ValidatedOp::CreateFile {
                    op_index,
                    parent_inode: *parent_inode,
                    display_name: display_name.clone(),
                    name_key,
                    child_inode: next_allocated_inode(
                        &mut allocated_inode_ids,
                        CommitValidationError::NextInodeOverflow,
                    )?,
                    content_ref: content_ref.clone(),
                    create_inode_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    bind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    revision_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::ReplaceFile {
                inode_id,
                base_revision_no,
                content_ref,
            } => {
                validate_inode_revision_is(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *base_revision_no,
                    committed_seq,
                )?;
                let revision_no = next_revision_no(*inode_id, *base_revision_no, true)?;
                validate_ancestors_not_subtree_deleted(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                    false,
                )?;
                ValidatedOp::ReplaceFile {
                    op_index,
                    inode_id: *inode_id,
                    revision_no,
                    content_ref: content_ref.clone(),
                    revision_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            } => {
                validate_restore_target(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *base_revision_no,
                    committed_seq,
                )?;
                let source_revision = validate_restore_source_revision(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *source_revision_no,
                    committed_seq,
                )?;
                let revision_no = next_revision_no(*inode_id, *base_revision_no, false)?;
                validate_restore_not_covered(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )?;
                ValidatedOp::RestoreRevision {
                    op_index,
                    inode_id: *inode_id,
                    source_revision_no: *source_revision_no,
                    revision_no,
                    content_ref: source_revision.content_ref,
                    revision_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::DeleteFile { inode_id } => {
                let source_binding = resolve_current_binding_for_mutation(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                )?;
                validate_delete_file_target(&ephemeral_metadata_state, *inode_id, committed_seq)?;
                validate_delete_file_not_covered(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )?;
                ValidatedOp::DeleteFile {
                    op_index,
                    inode_id: *inode_id,
                    source_binding,
                    unbind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    tombstone_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::Rename {
                inode_id,
                new_parent_inode,
                new_display_name,
                behavior,
            } => {
                if *behavior != loonfs_api::v0::MoveBehavior::NoReplace {
                    return Err(CommitValidationError::UnsupportedMoveBehavior {
                        behavior: *behavior,
                    });
                }
                let source_binding = resolve_current_binding_for_mutation(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                )?;
                validate_rename_source(&ephemeral_metadata_state, *inode_id, committed_seq)?;
                let new_name_key = validate_rename_target_name_absent(
                    &ephemeral_metadata_state,
                    *new_parent_inode,
                    new_display_name,
                    committed_seq,
                    name_policy,
                )?;
                validate_rename_does_not_cycle(
                    &ephemeral_metadata_state,
                    *inode_id,
                    *new_parent_inode,
                    committed_seq,
                )?;
                validate_rename_inode_not_covered(
                    &ephemeral_metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )?;
                validate_rename_target_parent_not_covered(
                    &ephemeral_metadata_state,
                    *new_parent_inode,
                    committed_seq,
                    checked_invariants,
                )?;
                ValidatedOp::Rename {
                    op_index,
                    inode_id: *inode_id,
                    source_binding,
                    new_parent_inode: *new_parent_inode,
                    new_display_name: new_display_name.clone(),
                    new_name_key,
                    unbind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    bind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::DeleteSubtree { root_inode } => {
                let source_binding = resolve_current_binding_for_mutation(
                    &ephemeral_metadata_state,
                    *root_inode,
                    committed_seq,
                )?;
                validate_delete_subtree_root(
                    &ephemeral_metadata_state,
                    *root_inode,
                    committed_seq,
                )?;
                validate_delete_subtree_not_covered(
                    &ephemeral_metadata_state,
                    *root_inode,
                    committed_seq,
                    checked_invariants,
                )?;
                ValidatedOp::DeleteSubtree {
                    op_index,
                    root_inode: *root_inode,
                    source_binding,
                    unbind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    tombstone_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
        };
        ephemeral_metadata_state.apply_validated_op_mut(committed_seq, &validated_op);
        validated_ops.push(validated_op);
    }

    Ok(ValidatedMetadataOps { validated_ops })
}

async fn validate_publish_metadata_preconditions<S: ObjectStore + ?Sized>(
    request: &CommitRequest,
    mut metadata_state: PublishMetadataPreview<'_, S>,
    committed_seq: ChangeSeq,
    allocated_inode_ids: &[InodeId],
    name_policy: NamePolicy,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<ValidatedMetadataOps, CoreError> {
    let mut allocated_inode_ids = allocated_inode_ids.iter().copied();
    let mut validated_ops = Vec::with_capacity(request.ops.len());
    let mut next_delta_index = 0u32;

    validate_publish_explicit_preconditions(
        &request.preconditions,
        &metadata_state,
        committed_seq,
        checked_invariants,
    )
    .await?;

    for (op_index, op) in request.ops.iter().enumerate() {
        let op_index =
            u32::try_from(op_index).map_err(|_| CommitValidationError::OpIndexOverflow)?;
        let validated_op = match op {
            CommitOp::CreateDir {
                parent_inode,
                display_name,
            } => {
                let name_key = validate_publish_child_name_absent(
                    &metadata_state,
                    *parent_inode,
                    display_name,
                    committed_seq,
                    name_policy,
                )
                .await?;
                validate_publish_ancestors_not_subtree_deleted(
                    &metadata_state,
                    *parent_inode,
                    committed_seq,
                    checked_invariants,
                    true,
                )
                .await?;
                ValidatedOp::CreateDir {
                    op_index,
                    parent_inode: *parent_inode,
                    display_name: display_name.clone(),
                    name_key,
                    child_inode: next_allocated_inode(
                        &mut allocated_inode_ids,
                        CommitValidationError::NextInodeOverflow,
                    )?,
                    create_inode_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    bind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::CreateFile {
                parent_inode,
                display_name,
                content_ref,
            } => {
                let name_key = validate_publish_child_name_absent(
                    &metadata_state,
                    *parent_inode,
                    display_name,
                    committed_seq,
                    name_policy,
                )
                .await?;
                validate_publish_ancestors_not_subtree_deleted(
                    &metadata_state,
                    *parent_inode,
                    committed_seq,
                    checked_invariants,
                    true,
                )
                .await?;
                ValidatedOp::CreateFile {
                    op_index,
                    parent_inode: *parent_inode,
                    display_name: display_name.clone(),
                    name_key,
                    child_inode: next_allocated_inode(
                        &mut allocated_inode_ids,
                        CommitValidationError::NextInodeOverflow,
                    )?,
                    content_ref: content_ref.clone(),
                    create_inode_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    bind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    revision_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::ReplaceFile {
                inode_id,
                base_revision_no,
                content_ref,
            } => {
                validate_publish_inode_revision_is(
                    &metadata_state,
                    *inode_id,
                    *base_revision_no,
                    committed_seq,
                )
                .await?;
                let revision_no = next_revision_no(*inode_id, *base_revision_no, true)?;
                validate_publish_ancestors_not_subtree_deleted(
                    &metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                    false,
                )
                .await?;
                ValidatedOp::ReplaceFile {
                    op_index,
                    inode_id: *inode_id,
                    revision_no,
                    content_ref: content_ref.clone(),
                    revision_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            } => {
                validate_publish_restore_target(
                    &metadata_state,
                    *inode_id,
                    *base_revision_no,
                    committed_seq,
                )
                .await?;
                let source_revision = validate_publish_restore_source_revision(
                    &metadata_state,
                    *inode_id,
                    *source_revision_no,
                    committed_seq,
                )
                .await?;
                let revision_no = next_revision_no(*inode_id, *base_revision_no, false)?;
                validate_publish_restore_not_covered(
                    &metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )
                .await?;
                ValidatedOp::RestoreRevision {
                    op_index,
                    inode_id: *inode_id,
                    source_revision_no: *source_revision_no,
                    revision_no,
                    content_ref: source_revision.content_ref,
                    revision_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::DeleteFile { inode_id } => {
                let source_binding = resolve_publish_current_binding_for_mutation(
                    &metadata_state,
                    *inode_id,
                    committed_seq,
                )
                .await?;
                validate_publish_delete_file_target(&metadata_state, *inode_id, committed_seq)
                    .await?;
                validate_publish_delete_file_not_covered(
                    &metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )
                .await?;
                ValidatedOp::DeleteFile {
                    op_index,
                    inode_id: *inode_id,
                    source_binding,
                    unbind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    tombstone_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::Rename {
                inode_id,
                new_parent_inode,
                new_display_name,
                behavior,
            } => {
                if *behavior != loonfs_api::v0::MoveBehavior::NoReplace {
                    return Err(CommitValidationError::UnsupportedMoveBehavior {
                        behavior: *behavior,
                    }
                    .into());
                }
                let source_binding = resolve_publish_current_binding_for_mutation(
                    &metadata_state,
                    *inode_id,
                    committed_seq,
                )
                .await?;
                validate_publish_rename_source(&metadata_state, *inode_id, committed_seq).await?;
                let new_name_key = validate_publish_rename_target_name_absent(
                    &metadata_state,
                    *new_parent_inode,
                    new_display_name,
                    committed_seq,
                    name_policy,
                )
                .await?;
                validate_publish_rename_does_not_cycle(
                    &metadata_state,
                    *inode_id,
                    *new_parent_inode,
                    committed_seq,
                )
                .await?;
                validate_publish_rename_inode_not_covered(
                    &metadata_state,
                    *inode_id,
                    committed_seq,
                    checked_invariants,
                )
                .await?;
                validate_publish_rename_target_parent_not_covered(
                    &metadata_state,
                    *new_parent_inode,
                    committed_seq,
                    checked_invariants,
                )
                .await?;
                ValidatedOp::Rename {
                    op_index,
                    inode_id: *inode_id,
                    source_binding,
                    new_parent_inode: *new_parent_inode,
                    new_display_name: new_display_name.clone(),
                    new_name_key,
                    unbind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    bind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
            CommitOp::DeleteSubtree { root_inode } => {
                let source_binding = resolve_publish_current_binding_for_mutation(
                    &metadata_state,
                    *root_inode,
                    committed_seq,
                )
                .await?;
                validate_publish_delete_subtree_root(&metadata_state, *root_inode, committed_seq)
                    .await?;
                validate_publish_delete_subtree_not_covered(
                    &metadata_state,
                    *root_inode,
                    committed_seq,
                    checked_invariants,
                )
                .await?;
                ValidatedOp::DeleteSubtree {
                    op_index,
                    root_inode: *root_inode,
                    source_binding,
                    unbind_delta_index: reserve_delta_index(&mut next_delta_index)?,
                    tombstone_delta_index: reserve_delta_index(&mut next_delta_index)?,
                }
            }
        };
        metadata_state.apply_validated_op_mut(committed_seq, &validated_op);
        validated_ops.push(validated_op);
    }

    Ok(ValidatedMetadataOps { validated_ops })
}

fn next_allocated_inode(
    allocated_inode_ids: &mut impl Iterator<Item = InodeId>,
    error: CommitValidationError,
) -> Result<InodeId, CommitValidationError> {
    allocated_inode_ids.next().ok_or(error)
}

fn reserve_delta_index(next_delta_index: &mut u32) -> Result<u32, CommitValidationError> {
    let delta_index = *next_delta_index;
    *next_delta_index = next_delta_index
        .checked_add(1)
        .ok_or(CommitValidationError::DeltaIndexOverflow)?;
    Ok(delta_index)
}

fn next_revision_no(
    inode_id: InodeId,
    base_revision_no: RevisionNo,
    is_replace: bool,
) -> Result<RevisionNo, CommitValidationError> {
    base_revision_no.0.checked_add(1).map(RevisionNo).ok_or({
        if is_replace {
            CommitValidationError::ReplaceFileRevisionOverflow {
                inode_id,
                base_revision_no,
            }
        } else {
            CommitValidationError::RestoreRevisionOverflow {
                inode_id,
                base_revision_no,
            }
        }
    })
}

async fn validate_publish_explicit_preconditions<S: ObjectStore + ?Sized>(
    preconditions: &[Precondition],
    metadata_state: &PublishMetadataPreview<'_, S>,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CoreError> {
    for precondition in preconditions {
        match precondition {
            Precondition::InodeRevisionIs {
                inode_id,
                revision_no,
            } => {
                validate_publish_inode_revision_is(
                    metadata_state,
                    *inode_id,
                    *revision_no,
                    base_seq,
                )
                .await?;
            }
            Precondition::AncestorsNotSubtreeDeleted { inode_id } => {
                validate_publish_ancestors_not_subtree_deleted(
                    metadata_state,
                    *inode_id,
                    base_seq,
                    checked_invariants,
                    false,
                )
                .await?;
            }
            Precondition::ChildNameAbsent {
                parent_inode,
                name_key,
            } => {
                validate_publish_child_name_absent_precondition(
                    metadata_state,
                    *parent_inode,
                    name_key,
                    base_seq,
                )
                .await?;
            }
            Precondition::BindingIs {
                parent_inode,
                name_key,
                child_inode,
                bind_seq,
                bind_delta_index,
            } => {
                validate_publish_binding_is_precondition(
                    metadata_state,
                    *parent_inode,
                    name_key,
                    *child_inode,
                    *bind_seq,
                    *bind_delta_index,
                    base_seq,
                )
                .await?;
            }
            Precondition::DirectoryEmpty { inode_id } => {
                validate_publish_directory_empty_precondition(metadata_state, *inode_id, base_seq)
                    .await?;
            }
        }
    }

    Ok(())
}

async fn validate_publish_child_name_absent_precondition<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    parent_inode: InodeId,
    name_key: &str,
    base_seq: ChangeSeq,
) -> Result<(), CoreError> {
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .await?
        .ok_or(CommitValidationError::NamePreconditionParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::NamePreconditionParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        }
        .into());
    }

    if let Some(existing) = metadata_state
        .visible_child(parent_inode, name_key, base_seq)
        .await?
    {
        return Err(CommitValidationError::CreateChildNameCollision {
            parent_inode,
            name_key: name_key.to_owned(),
            child_inode: existing.child_inode_id,
        }
        .into());
    }

    Ok(())
}

async fn resolve_publish_current_binding_for_mutation<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<ResolvedBinding, CoreError> {
    let binding = metadata_state
        .current_parent_binding_for_child(inode_id, base_seq)
        .await?
        .ok_or(CommitValidationError::SourceBindingMissing { inode_id })?;
    Ok(ResolvedBinding {
        parent_inode: binding.parent_inode_id,
        name_key: binding.name_key,
        display_name: binding.display_name,
        child_inode: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

#[allow(clippy::too_many_arguments)]
async fn validate_publish_binding_is_precondition<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    parent_inode: InodeId,
    name_key: &str,
    child_inode: InodeId,
    bind_seq: ChangeSeq,
    bind_delta_index: u32,
    base_seq: ChangeSeq,
) -> Result<(), CoreError> {
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .await?
        .ok_or(CommitValidationError::NamePreconditionParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::NamePreconditionParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        }
        .into());
    }

    let Some(existing) = metadata_state
        .visible_child(parent_inode, name_key, base_seq)
        .await?
    else {
        return Err(CommitValidationError::BindingPreconditionMissing {
            parent_inode,
            name_key: name_key.to_owned(),
        }
        .into());
    };
    if existing.child_inode_id != child_inode
        || existing.bind_seq != bind_seq
        || existing.bind_delta_index != bind_delta_index
    {
        return Err(CommitValidationError::BindingPreconditionMismatch {
            parent_inode,
            name_key: name_key.to_owned(),
            expected_child_inode: child_inode,
            actual_child_inode: Some(existing.child_inode_id),
        }
        .into());
    }

    Ok(())
}

async fn validate_publish_directory_empty_precondition<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CoreError> {
    let inode = metadata_state
        .visible_inode(inode_id, base_seq)
        .await?
        .ok_or(CommitValidationError::DirectoryEmptyPreconditionInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::Dir {
        return Err(
            CommitValidationError::DirectoryEmptyPreconditionInodeNotDirectory {
                inode_id,
                actual_kind: inode.inode_kind,
            }
            .into(),
        );
    }

    if !metadata_state
        .visible_children(inode_id, base_seq)
        .await?
        .is_empty()
    {
        return Err(CommitValidationError::DirectoryEmptyPreconditionNotEmpty { inode_id }.into());
    }

    Ok(())
}

async fn validate_publish_child_name_absent<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    parent_inode: InodeId,
    display_name: &str,
    base_seq: ChangeSeq,
    name_policy: NamePolicy,
) -> Result<String, CoreError> {
    validate_display_name(display_name)?;
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .await?
        .ok_or(CommitValidationError::CreateParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::CreateParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        }
        .into());
    }

    let name_key = name_key_for_display_name(name_policy, display_name);
    if let Some(existing) = metadata_state
        .visible_child(parent_inode, &name_key, base_seq)
        .await?
    {
        return Err(CommitValidationError::CreateChildNameCollision {
            parent_inode,
            name_key,
            child_inode: existing.child_inode_id,
        }
        .into());
    }

    Ok(name_key)
}

async fn validate_publish_inode_revision_is<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<(), CoreError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .await?
        .ok_or(CommitValidationError::ReplaceFileInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::File {
        return Err(CommitValidationError::ReplaceFileInodeNotFile {
            inode_id,
            actual_kind: inode.inode_kind,
        }
        .into());
    }

    let actual_revision_no = metadata_state
        .latest_revision_head_at_seq(inode_id, base_seq)
        .await?
        .map(|revision| revision.revision_no);
    if actual_revision_no != Some(expected_revision_no) {
        return Err(CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id,
            expected: expected_revision_no,
            actual: actual_revision_no,
        }
        .into());
    }

    Ok(())
}

async fn validate_publish_restore_target<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<(), CoreError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .await?
        .ok_or(CommitValidationError::RestoreRevisionInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::File {
        return Err(CommitValidationError::RestoreRevisionInodeNotFile {
            inode_id,
            actual_kind: inode.inode_kind,
        }
        .into());
    }

    let actual_revision_no = metadata_state
        .latest_revision_head_at_seq(inode_id, base_seq)
        .await?
        .map(|revision| revision.revision_no);
    if actual_revision_no != Some(expected_revision_no) {
        return Err(CommitValidationError::RestoreRevisionBaseRevisionMismatch {
            inode_id,
            expected: expected_revision_no,
            actual: actual_revision_no,
        }
        .into());
    }

    Ok(())
}

async fn validate_publish_restore_source_revision<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    source_revision_no: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<RevisionRecord, CoreError> {
    metadata_state
        .revision_at_seq(inode_id, source_revision_no, base_seq)
        .await?
        .ok_or(
            CommitValidationError::RestoreRevisionSourceRevisionMissing {
                inode_id,
                source_revision_no,
            }
            .into(),
        )
}

async fn validate_publish_ancestors_not_subtree_deleted<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
    is_create: bool,
) -> Result<(), CoreError> {
    if let Some(tombstone) = metadata_state
        .covering_subtree_tombstone(inode_id, base_seq)
        .await?
    {
        return if is_create {
            Err(CommitValidationError::CreateUnderSubtreeTombstone {
                parent_inode: inode_id,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            }
            .into())
        } else {
            Err(CommitValidationError::ReplaceFileUnderSubtreeTombstone {
                inode_id,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            }
            .into())
        };
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

async fn validate_publish_restore_not_covered<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CoreError> {
    if let Some(tombstone) = metadata_state
        .covering_subtree_tombstone(inode_id, base_seq)
        .await?
    {
        return Err(
            CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
                inode_id,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            }
            .into(),
        );
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

async fn validate_publish_delete_subtree_root<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    root_inode: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CoreError> {
    let inode = metadata_state
        .inode_at_seq(root_inode, base_seq)
        .await?
        .ok_or(CommitValidationError::DeleteSubtreeRootMissing { root_inode })?;
    if inode.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::DeleteSubtreeRootNotDirectory {
            root_inode,
            actual_kind: inode.inode_kind,
        }
        .into());
    }

    Ok(())
}

async fn validate_publish_delete_file_target<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CoreError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .await?
        .ok_or(CommitValidationError::DeleteFileInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::File {
        return Err(CommitValidationError::DeleteFileInodeNotFile {
            inode_id,
            actual_kind: inode.inode_kind,
        }
        .into());
    }

    Ok(())
}

async fn validate_publish_delete_file_not_covered<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CoreError> {
    if let Some(tombstone) = metadata_state
        .covering_subtree_tombstone(inode_id, base_seq)
        .await?
    {
        return Err(CommitValidationError::DeleteFileCoveredByTombstone {
            inode_id,
            covering_root_inode: tombstone.root_inode_id,
            tombstone_seq: tombstone.tombstone_seq,
        }
        .into());
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

async fn validate_publish_delete_subtree_not_covered<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    root_inode: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CoreError> {
    if let Some(tombstone) = metadata_state
        .covering_subtree_tombstone(root_inode, base_seq)
        .await?
    {
        return Err(CommitValidationError::DeleteSubtreeRootCoveredByTombstone {
            root_inode,
            covering_root_inode: tombstone.root_inode_id,
            tombstone_seq: tombstone.tombstone_seq,
        }
        .into());
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

async fn validate_publish_rename_source<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CoreError> {
    metadata_state
        .inode_at_seq(inode_id, base_seq)
        .await?
        .ok_or(CommitValidationError::RenameInodeMissing { inode_id })?;
    if metadata_state
        .current_parent_binding_for_child(inode_id, base_seq)
        .await?
        .is_none()
    {
        return Err(CommitValidationError::RenameSourceBindingMissing { inode_id }.into());
    }

    Ok(())
}

async fn validate_publish_rename_target_name_absent<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    parent_inode: InodeId,
    display_name: &str,
    base_seq: ChangeSeq,
    name_policy: NamePolicy,
) -> Result<String, CoreError> {
    validate_display_name(display_name)?;
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .await?
        .ok_or(CommitValidationError::RenameTargetParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::RenameTargetParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        }
        .into());
    }

    let name_key = name_key_for_display_name(name_policy, display_name);
    if let Some(existing) = metadata_state
        .visible_child(parent_inode, &name_key, base_seq)
        .await?
    {
        return Err(CommitValidationError::RenameTargetNameCollision {
            parent_inode,
            name_key,
            child_inode: existing.child_inode_id,
        }
        .into());
    }

    Ok(name_key)
}

async fn validate_publish_rename_does_not_cycle<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    new_parent_inode: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CoreError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .await?
        .ok_or(CommitValidationError::RenameInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::Dir {
        return Ok(());
    }
    if metadata_state
        .would_create_directory_cycle(inode_id, new_parent_inode, base_seq)
        .await?
    {
        return Err(CommitValidationError::RenameWouldCycleDirectory {
            inode_id,
            new_parent_inode,
        }
        .into());
    }

    Ok(())
}

async fn validate_publish_rename_inode_not_covered<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CoreError> {
    if let Some(tombstone) = metadata_state
        .covering_subtree_tombstone(inode_id, base_seq)
        .await?
    {
        return Err(CommitValidationError::RenameInodeUnderSubtreeTombstone {
            inode_id,
            root_inode: tombstone.root_inode_id,
            tombstone_seq: tombstone.tombstone_seq,
        }
        .into());
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

async fn validate_publish_rename_target_parent_not_covered<S: ObjectStore + ?Sized>(
    metadata_state: &PublishMetadataPreview<'_, S>,
    parent_inode: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CoreError> {
    if let Some(tombstone) = metadata_state
        .covering_subtree_tombstone(parent_inode, base_seq)
        .await?
    {
        return Err(
            CommitValidationError::RenameTargetParentUnderSubtreeTombstone {
                parent_inode,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            }
            .into(),
        );
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

fn validate_explicit_preconditions(
    preconditions: &[Precondition],
    metadata_state: &MetadataPreview<'_>,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CommitValidationError> {
    for precondition in preconditions {
        match precondition {
            Precondition::InodeRevisionIs {
                inode_id,
                revision_no,
            } => {
                validate_inode_revision_is(metadata_state, *inode_id, *revision_no, base_seq)?;
            }
            Precondition::AncestorsNotSubtreeDeleted { inode_id } => {
                validate_ancestors_not_subtree_deleted(
                    metadata_state,
                    *inode_id,
                    base_seq,
                    checked_invariants,
                    false,
                )?;
            }
            Precondition::ChildNameAbsent {
                parent_inode,
                name_key,
            } => {
                validate_child_name_absent_precondition(
                    metadata_state,
                    *parent_inode,
                    name_key,
                    base_seq,
                )?;
            }
            Precondition::BindingIs {
                parent_inode,
                name_key,
                child_inode,
                bind_seq,
                bind_delta_index,
            } => {
                validate_binding_is_precondition(
                    metadata_state,
                    *parent_inode,
                    name_key,
                    *child_inode,
                    *bind_seq,
                    *bind_delta_index,
                    base_seq,
                )?;
            }
            Precondition::DirectoryEmpty { inode_id } => {
                validate_directory_empty_precondition(metadata_state, *inode_id, base_seq)?;
            }
        }
    }

    Ok(())
}

fn validate_child_name_absent_precondition(
    metadata_state: &MetadataPreview<'_>,
    parent_inode: InodeId,
    name_key: &str,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .ok_or(CommitValidationError::NamePreconditionParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::NamePreconditionParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        });
    }

    if let Some(existing) = metadata_state.visible_child(parent_inode, name_key, base_seq) {
        return Err(CommitValidationError::CreateChildNameCollision {
            parent_inode,
            name_key: name_key.to_owned(),
            child_inode: existing.child_inode_id,
        });
    }

    Ok(())
}

fn resolve_current_binding_for_mutation(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<ResolvedBinding, CommitValidationError> {
    let binding = metadata_state
        .current_parent_binding_for_child(inode_id, base_seq)
        .ok_or(CommitValidationError::SourceBindingMissing { inode_id })?;
    Ok(ResolvedBinding {
        parent_inode: binding.parent_inode_id,
        name_key: binding.name_key,
        display_name: binding.display_name,
        child_inode: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

fn validate_binding_is_precondition(
    metadata_state: &MetadataPreview<'_>,
    parent_inode: InodeId,
    name_key: &str,
    child_inode: InodeId,
    bind_seq: ChangeSeq,
    bind_delta_index: u32,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .ok_or(CommitValidationError::NamePreconditionParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::NamePreconditionParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        });
    }

    let Some(existing) = metadata_state.visible_child(parent_inode, name_key, base_seq) else {
        return Err(CommitValidationError::BindingPreconditionMissing {
            parent_inode,
            name_key: name_key.to_owned(),
        });
    };
    if existing.child_inode_id != child_inode
        || existing.bind_seq != bind_seq
        || existing.bind_delta_index != bind_delta_index
    {
        return Err(CommitValidationError::BindingPreconditionMismatch {
            parent_inode,
            name_key: name_key.to_owned(),
            expected_child_inode: child_inode,
            actual_child_inode: Some(existing.child_inode_id),
        });
    }

    Ok(())
}

fn validate_directory_empty_precondition(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .visible_inode(inode_id, base_seq)
        .ok_or(CommitValidationError::DirectoryEmptyPreconditionInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::Dir {
        return Err(
            CommitValidationError::DirectoryEmptyPreconditionInodeNotDirectory {
                inode_id,
                actual_kind: inode.inode_kind,
            },
        );
    }

    if !metadata_state
        .visible_children(inode_id, base_seq)
        .is_empty()
    {
        return Err(CommitValidationError::DirectoryEmptyPreconditionNotEmpty { inode_id });
    }

    Ok(())
}

fn validate_child_name_absent(
    metadata_state: &MetadataPreview<'_>,
    parent_inode: InodeId,
    display_name: &str,
    base_seq: ChangeSeq,
    name_policy: NamePolicy,
) -> Result<String, CommitValidationError> {
    validate_display_name(display_name)?;
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .ok_or(CommitValidationError::CreateParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::CreateParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        });
    }

    let name_key = name_key_for_display_name(name_policy, display_name);
    if let Some(existing) = metadata_state.visible_child(parent_inode, &name_key, base_seq) {
        return Err(CommitValidationError::CreateChildNameCollision {
            parent_inode,
            name_key,
            child_inode: existing.child_inode_id,
        });
    }

    Ok(name_key)
}

fn validate_inode_revision_is(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::ReplaceFileInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::File {
        return Err(CommitValidationError::ReplaceFileInodeNotFile {
            inode_id,
            actual_kind: inode.inode_kind,
        });
    }

    let actual_revision_no = metadata_state
        .latest_revision_head_at_seq(inode_id, base_seq)
        .map(|revision| revision.revision_no);
    if actual_revision_no != Some(expected_revision_no) {
        return Err(CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id,
            expected: expected_revision_no,
            actual: actual_revision_no,
        });
    }

    Ok(())
}

fn validate_restore_target(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    expected_revision_no: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::RestoreRevisionInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::File {
        return Err(CommitValidationError::RestoreRevisionInodeNotFile {
            inode_id,
            actual_kind: inode.inode_kind,
        });
    }

    let actual_revision_no = metadata_state
        .latest_revision_head_at_seq(inode_id, base_seq)
        .map(|revision| revision.revision_no);
    if actual_revision_no != Some(expected_revision_no) {
        return Err(CommitValidationError::RestoreRevisionBaseRevisionMismatch {
            inode_id,
            expected: expected_revision_no,
            actual: actual_revision_no,
        });
    }

    Ok(())
}

fn validate_restore_source_revision(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    source_revision_no: RevisionNo,
    base_seq: ChangeSeq,
) -> Result<crate::metadata::RevisionRecord, CommitValidationError> {
    metadata_state
        .revision_at_seq(inode_id, source_revision_no, base_seq)
        .ok_or(
            CommitValidationError::RestoreRevisionSourceRevisionMissing {
                inode_id,
                source_revision_no,
            },
        )
}

fn validate_ancestors_not_subtree_deleted(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
    is_create: bool,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id, base_seq) {
        return if is_create {
            Err(CommitValidationError::CreateUnderSubtreeTombstone {
                parent_inode: inode_id,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            })
        } else {
            Err(CommitValidationError::ReplaceFileUnderSubtreeTombstone {
                inode_id,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            })
        };
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

fn validate_restore_not_covered(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id, base_seq) {
        return Err(
            CommitValidationError::RestoreRevisionUnderSubtreeTombstone {
                inode_id,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            },
        );
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

fn validate_delete_subtree_root(
    metadata_state: &MetadataPreview<'_>,
    root_inode: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(root_inode, base_seq)
        .ok_or(CommitValidationError::DeleteSubtreeRootMissing { root_inode })?;
    if inode.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::DeleteSubtreeRootNotDirectory {
            root_inode,
            actual_kind: inode.inode_kind,
        });
    }

    Ok(())
}

fn validate_delete_file_target(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::DeleteFileInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::File {
        return Err(CommitValidationError::DeleteFileInodeNotFile {
            inode_id,
            actual_kind: inode.inode_kind,
        });
    }

    Ok(())
}

fn validate_delete_file_not_covered(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id, base_seq) {
        return Err(CommitValidationError::DeleteFileCoveredByTombstone {
            inode_id,
            covering_root_inode: tombstone.root_inode_id,
            tombstone_seq: tombstone.tombstone_seq,
        });
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

fn validate_delete_subtree_not_covered(
    metadata_state: &MetadataPreview<'_>,
    root_inode: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(root_inode, base_seq) {
        return Err(CommitValidationError::DeleteSubtreeRootCoveredByTombstone {
            root_inode,
            covering_root_inode: tombstone.root_inode_id,
            tombstone_seq: tombstone.tombstone_seq,
        });
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

fn validate_rename_source(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::RenameInodeMissing { inode_id })?;
    if metadata_state
        .current_parent_binding_for_child(inode_id, base_seq)
        .is_none()
    {
        return Err(CommitValidationError::RenameSourceBindingMissing { inode_id });
    }

    Ok(())
}

fn validate_rename_target_name_absent(
    metadata_state: &MetadataPreview<'_>,
    parent_inode: InodeId,
    display_name: &str,
    base_seq: ChangeSeq,
    name_policy: NamePolicy,
) -> Result<String, CommitValidationError> {
    validate_display_name(display_name)?;
    let parent = metadata_state
        .inode_at_seq(parent_inode, base_seq)
        .ok_or(CommitValidationError::RenameTargetParentMissing { parent_inode })?;
    if parent.inode_kind != InodeKind::Dir {
        return Err(CommitValidationError::RenameTargetParentNotDirectory {
            parent_inode,
            actual_kind: parent.inode_kind,
        });
    }

    let name_key = name_key_for_display_name(name_policy, display_name);
    if let Some(existing) = metadata_state.visible_child(parent_inode, &name_key, base_seq) {
        return Err(CommitValidationError::RenameTargetNameCollision {
            parent_inode,
            name_key,
            child_inode: existing.child_inode_id,
        });
    }

    Ok(name_key)
}

fn validate_display_name(display_name: &str) -> Result<(), CommitValidationError> {
    DisplayName::parse(display_name).map(|_| ()).map_err(|_| {
        CommitValidationError::InvalidDisplayName {
            display_name: display_name.to_owned(),
        }
    })
}

fn validate_rename_does_not_cycle(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    new_parent_inode: InodeId,
    base_seq: ChangeSeq,
) -> Result<(), CommitValidationError> {
    let inode = metadata_state
        .inode_at_seq(inode_id, base_seq)
        .ok_or(CommitValidationError::RenameInodeMissing { inode_id })?;
    if inode.inode_kind != InodeKind::Dir {
        return Ok(());
    }
    if metadata_state.would_create_directory_cycle(inode_id, new_parent_inode, base_seq) {
        return Err(CommitValidationError::RenameWouldCycleDirectory {
            inode_id,
            new_parent_inode,
        });
    }

    Ok(())
}

fn validate_rename_inode_not_covered(
    metadata_state: &MetadataPreview<'_>,
    inode_id: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(inode_id, base_seq) {
        return Err(CommitValidationError::RenameInodeUnderSubtreeTombstone {
            inode_id,
            root_inode: tombstone.root_inode_id,
            tombstone_seq: tombstone.tombstone_seq,
        });
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}

fn validate_rename_target_parent_not_covered(
    metadata_state: &MetadataPreview<'_>,
    parent_inode: InodeId,
    base_seq: ChangeSeq,
    checked_invariants: &mut Vec<InvariantId>,
) -> Result<(), CommitValidationError> {
    if let Some(tombstone) = metadata_state.covering_subtree_tombstone(parent_inode, base_seq) {
        return Err(
            CommitValidationError::RenameTargetParentUnderSubtreeTombstone {
                parent_inode,
                root_inode: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            },
        );
    }

    push_unique_invariant(
        checked_invariants,
        InvariantId::SubtreeTombstoneBlocksDescendantMutation,
    );
    Ok(())
}
