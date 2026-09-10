//! Flushes the WAL and creates a verified pin for the resulting manifest.

use super::flush::{try_flush_wal, TryFlushWal};
use super::record::{
    release_checkpoint_record, verify_checkpoint_basis, write_checkpoint_record,
    CheckpointBasisVerification,
};
use crate::commit::WalPublishError;
use crate::context::MutationContext;
use crate::control_update::{retry_while_contended, CasAttempt};
use crate::error::CoreError;
use crate::error::Result;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::control::{CheckpointOwner, CheckpointRecordState};
use loonfs_api::{Checkpoint, CheckpointId, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) use crate::limits::CHECKPOINT_VERIFY_BUDGET_MS;

/// Longest accepted user checkpoint name. A label bound, not a durable
/// format limit.
const CHECKPOINT_NAME_MAX_CHARS: usize = 128;

pub(crate) async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    owner: CheckpointOwner,
    context: &MutationContext,
) -> Result<Checkpoint> {
    validate_checkpoint_owner(&owner)?;
    let timer = &StdMonotonicTimer::default();
    let owner = &owner;
    let created = retry_while_contended(
        || async move {
            let basis = match try_flush_wal(store, namespace_id, context, timer).await? {
                TryFlushWal::Settled(basis) => basis,
                TryFlushWal::RaceLost => {
                    return Ok(CasAttempt::Contended(CoreError::WalPublish(
                        WalPublishError::StaleHead,
                    )))
                }
            };

            match create_checkpoint_at_basis(
                store,
                namespace_id,
                owner.clone(),
                basis.manifest.clone(),
                basis.head_commit_id.clone(),
                context,
            )
            .await
            {
                Ok(checkpoint) => Ok(CasAttempt::Settled(checkpoint)),
                Err(error @ CoreError::CheckpointUnavailable(_)) => {
                    Ok(CasAttempt::Contended(error))
                }
                Err(error) => Err(error),
            }
        },
        |_, ()| async { Ok(crate::control_update::WriteEvidence::Unknown) },
    )
    .await?;
    created
}

pub(crate) async fn create_checkpoint_at_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    owner: CheckpointOwner,
    manifest: loonfs_api::wire::control::ManifestRef,
    head_commit_id: loonfs_api::CommitId,
    context: &MutationContext,
) -> Result<Checkpoint> {
    validate_checkpoint_owner(&owner)?;
    let timer = StdMonotonicTimer::default();
    let checkpoint_id = CheckpointId::generate(manifest.manifest_no);
    let record = CheckpointRecordState {
        pin_id: checkpoint_id.clone(),
        namespace_id: namespace_id.clone(),
        manifest_no: manifest.manifest_no,
        manifest_head_seq: manifest.manifest_head_seq,
        manifest_payload_checksum: manifest.manifest_payload_checksum,
        head_commit_id,
        created_at_ms: context.now_ms,
        owner,
    };
    let verify_started_ms = timer.monotonic_now_ms();
    write_checkpoint_record(store, &record).await?;

    let verification = match verify_checkpoint_basis(store, &record).await {
        Ok(verification) => verification,
        Err(error) => {
            // Cleanup is best effort on an error and must not replace its
            // original classification.
            if let Err(cleanup_error) =
                release_checkpoint_record(store, namespace_id, &checkpoint_id).await
            {
                tracing::warn!(
                    namespace_id = %namespace_id,
                    checkpoint_id = %checkpoint_id,
                    original_error = %error,
                    cleanup_error = %cleanup_error,
                    "failed to release a checkpoint record after basis verification failed"
                );
            }
            return Err(error);
        }
    };
    let within_budget =
        timer.monotonic_now_ms().saturating_sub(verify_started_ms) <= CHECKPOINT_VERIFY_BUDGET_MS;
    if verification == CheckpointBasisVerification::Verified && within_budget {
        return Ok(super::checkpoint_summary(record));
    }

    // Overrunning the budget counts as verification failure: the record
    // may have raced the grace window, so it must not stand as a root.
    release_checkpoint_record(store, namespace_id, &checkpoint_id).await?;
    Err(CoreError::CheckpointUnavailable(
        "checkpoint publication retry exhausted".to_owned(),
    ))
}

fn validate_checkpoint_owner(owner: &CheckpointOwner) -> Result<()> {
    match owner {
        CheckpointOwner::User { name, .. } => validate_checkpoint_name(name),
        CheckpointOwner::Snapshot {
            name,
            expires_at_ms,
        } => {
            validate_checkpoint_name(name)?;
            if *expires_at_ms == 0 {
                return Err(CoreError::InvalidCheckpointRequest(
                    "snapshot expiry must not be zero".to_owned(),
                ));
            }
            Ok(())
        }
        CheckpointOwner::Fork { .. } => Ok(()),
    }
}

fn validate_checkpoint_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CoreError::InvalidCheckpointRequest(
            "checkpoint name must not be empty".to_owned(),
        ));
    }
    if name.chars().count() > CHECKPOINT_NAME_MAX_CHARS {
        return Err(CoreError::InvalidCheckpointRequest(format!(
            "checkpoint name exceeds {CHECKPOINT_NAME_MAX_CHARS} characters"
        )));
    }
    Ok(())
}
