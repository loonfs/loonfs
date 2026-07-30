//! Builds one commit plan: frame validation, precondition checks over the
//! chosen validation view, and delta-index assignment.

use super::super::frame::validate_commit_request_frame;
use super::super::{CommitPlan, CommitRequest, CommitValidationContext, CommitValidationError};
use super::checks::validate_metadata_preconditions;
use super::view::{CommitValidationView, InMemoryValidationView, PublishValidationView};
use crate::error::CoreError;
use crate::metadata::{MetadataState, MetadataView};
use loonfs_api::v0::CommitOp;
use loonfs_api::wire::control::HeadState;
use loonfs_api::{ChangeSeq, InodeId};
use loonfs_objectstore::ObjectStore;

struct CommitShape {
    assigned_seq: ChangeSeq,
    allocated_inode_ids: Vec<InodeId>,
    resulting_next_inode_id: InodeId,
}

#[derive(Clone, Copy)]
pub(crate) struct PublishCommitValidationContext<'a, S: ObjectStore + ?Sized> {
    pub(crate) head: &'a HeadState,
    pub(crate) metadata_view: MetadataView<'a, 'a, S>,
    pub(crate) accepted_rows: &'a MetadataState,
}

pub async fn build_commit_plan(
    request: &CommitRequest,
    committed_at_ms: u64,
    context: &CommitValidationContext<'_>,
) -> Result<CommitPlan, CommitValidationError> {
    let shape = compute_commit_shape(request, &context.head)?;
    let view = InMemoryValidationView::new(context.metadata_state, shape.assigned_seq);
    build_commit_plan_with_view(request, committed_at_ms, &context.head, shape, view).await
}

pub(crate) async fn build_commit_plan_for_publish<S: ObjectStore + ?Sized>(
    request: &CommitRequest,
    committed_at_ms: u64,
    context: &PublishCommitValidationContext<'_, S>,
) -> Result<CommitPlan, CoreError> {
    let shape = compute_commit_shape(request, context.head)?;
    let committed_seq = shape.assigned_seq;
    build_commit_plan_with_view(
        request,
        committed_at_ms,
        context.head,
        shape,
        PublishValidationView::new(context.metadata_view, context.accepted_rows, committed_seq),
    )
    .await
}

/// The single commit plan builder behind [`build_commit_plan`] and
/// [`build_commit_plan_for_publish`]; only the metadata view (and with it
/// the error surface) differs between the two entry points.
async fn build_commit_plan_with_view<V: CommitValidationView>(
    request: &CommitRequest,
    committed_at_ms: u64,
    head: &HeadState,
    shape: CommitShape,
    metadata_state: V,
) -> Result<CommitPlan, V::Error> {
    validate_commit_request_frame(request, head)?;

    let validated_metadata = validate_metadata_preconditions(
        request,
        metadata_state,
        shape.assigned_seq,
        committed_at_ms,
        &shape.allocated_inode_ids,
    )
    .await?;

    Ok(CommitPlan {
        namespace_id: request.namespace_id.clone(),
        commit_id: request.commit_id.clone(),
        apply_after_seq: head.seq,
        assigned_seq: shape.assigned_seq,
        validated_ops: validated_metadata.validated_ops,
        resulting_next_inode_id: shape.resulting_next_inode_id,
    })
}

fn compute_commit_shape(
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
        .filter(|op| {
            matches!(
                op,
                CommitOp::CreateDirectory { .. } | CommitOp::CreateFile { .. }
            )
        })
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
