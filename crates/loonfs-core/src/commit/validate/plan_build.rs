//! Builds one commit plan: frame validation, per-operation checks over the
//! chosen validation view, and delta-index assignment.

use super::super::frame::validate_commit_request_frame;
use super::super::{CommitFingerprint, CommitIr, CommitValidationError, ValidatedCommitPlan};
use super::checks::{validate_ops, OpValidationCursor};
use super::view::PublishValidationView;
use crate::error::CoreError;
use crate::metadata::{MetadataState, MetadataView};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{next_public_ordinal, ChangeSeq};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn validate_commit_for_publish<S: ObjectStore + ?Sized>(
    request: &CommitIr,
    semantic_identity: CommitFingerprint,
    committed_at_ms: u64,
    head: &HeadState,
    metadata_view: MetadataView<'_, '_, S>,
    accepted_rows: &MetadataState,
) -> Result<ValidatedCommitPlan, CoreError> {
    let committed_seq = next_public_ordinal(head.seq.0)
        .map(ChangeSeq)
        .ok_or(CommitValidationError::SeqOverflow)?;
    build_commit_plan(
        request,
        semantic_identity,
        committed_at_ms,
        head,
        committed_seq,
        PublishValidationView::new(metadata_view, accepted_rows, committed_seq),
    )
    .await
}

async fn build_commit_plan<S: ObjectStore + ?Sized>(
    request: &CommitIr,
    semantic_identity: CommitFingerprint,
    committed_at_ms: u64,
    head: &HeadState,
    committed_seq: ChangeSeq,
    mut metadata_state: PublishValidationView<'_, S>,
) -> Result<ValidatedCommitPlan, CoreError> {
    validate_commit_request_frame(request, head)?;

    let validated_ops = validate_ops(
        &request.ops,
        &mut metadata_state,
        &mut OpValidationCursor::new(),
        committed_seq,
        &request.actor,
        committed_at_ms,
    )
    .await?;

    Ok(ValidatedCommitPlan {
        namespace_id: request.namespace_id.clone(),
        commit_id: request.commit_id.clone(),
        actor: request.actor.clone(),
        writer_epoch: request.writer_epoch,
        message: request.message.clone(),
        semantic_identity,
        apply_after_seq: head.seq,
        assigned_seq: committed_seq,
        validated_ops,
    })
}
