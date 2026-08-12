//! Builds one commit plan: frame validation, per-operation checks over the
//! chosen validation view, and delta-index assignment.

use super::super::frame::validate_commit_request_frame;
use super::super::{CandidateAllocation, CommitIr, CommitPlan, CommitValidationError};
use super::checks::{validate_ops, OpValidationCursor};
use super::view::PublishValidationView;
use crate::error::CoreError;
use crate::metadata::{MetadataState, MetadataView};
use loonfs_api::wire::control::HeadState;
use loonfs_api::ChangeSeq;
use loonfs_objectstore::ObjectStore;

#[derive(Clone, Copy)]
pub(crate) struct PublishCommitValidationContext<'a, S: ObjectStore + ?Sized> {
    pub(crate) head: &'a HeadState,
    pub(crate) metadata_view: MetadataView<'a, 'a, S>,
    pub(crate) accepted_rows: &'a MetadataState,
}

pub(crate) async fn build_commit_plan_for_publish<S: ObjectStore + ?Sized>(
    request: &CommitIr,
    committed_at_ms: u64,
    allocation: &CandidateAllocation,
    context: &PublishCommitValidationContext<'_, S>,
) -> Result<CommitPlan, CoreError> {
    let committed_seq = context
        .head
        .seq
        .0
        .checked_add(1)
        .map(ChangeSeq)
        .ok_or(CommitValidationError::SeqOverflow)?;
    build_commit_plan(
        request,
        committed_at_ms,
        context.head,
        committed_seq,
        allocation,
        PublishValidationView::new(context.metadata_view, context.accepted_rows, committed_seq),
    )
    .await
}

async fn build_commit_plan<S: ObjectStore + ?Sized>(
    request: &CommitIr,
    committed_at_ms: u64,
    head: &HeadState,
    committed_seq: ChangeSeq,
    allocation: &CandidateAllocation,
    mut metadata_state: PublishValidationView<'_, S>,
) -> Result<CommitPlan, CoreError> {
    validate_commit_request_frame(request, head)?;

    let validated_ops = validate_ops(
        &request.ops,
        &mut metadata_state,
        &mut OpValidationCursor::new(),
        committed_seq,
        committed_at_ms,
    )
    .await?;

    Ok(CommitPlan {
        namespace_id: request.namespace_id.clone(),
        commit_id: request.commit_id.clone(),
        apply_after_seq: head.seq,
        assigned_seq: committed_seq,
        validated_ops,
        resulting_next_inode_id: allocation.resulting_next_inode_id(),
    })
}
