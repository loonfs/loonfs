//! Checkpoint creation: advance the metadata root to cover the current head,
//! then pin the resulting manifest under one durable checkpoint record.

use super::flush::{try_flush_wal, TryFlushWal};
use super::record::{
    release_checkpoint_record, verify_checkpoint_basis, write_checkpoint_record,
    CheckpointBasisVerification,
};
#[cfg(test)]
use super::row::manifest_rows_for_family;
#[cfg(test)]
use super::runs::CHECKPOINT_TABLE_FAMILIES;
use crate::commit::CommitHeadPublishError;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::error::Result;
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
#[cfg(test)]
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::control::{
    CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState,
};
use loonfs_api::{CheckpointId, CreateCheckpointResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;

#[cfg(test)]
use super::load::append_rows_to_metadata;
#[cfg(test)]
use crate::metadata::MetadataStateBuilder;

pub(crate) use crate::limits::CHECKPOINT_VERIFY_BUDGET_MS;

/// Longest accepted user checkpoint name. A label bound, not a durable
/// format limit.
const CHECKPOINT_NAME_MAX_CHARS: usize = 128;

pub(crate) async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    owner: CheckpointOwner,
    expires_at_ms: Option<u64>,
    context: &MutationContext,
) -> Result<CreateCheckpointResponse> {
    // Checkpoint creation pins a manifest version as a first-class record
    // under `checkpoints/`, write-then-verify (format spec, "Checkpoints"):
    //
    // 1. Choose a basis manifest that the metadata root references,
    //    flushing the WAL tail first when it lags the head (`flush.rs`).
    // 2. Write `checkpoints/{id}.json` with state = active, under a freshly
    //    generated id — one logical pin, one record, never a reuse of some
    //    earlier record's key.
    // 3. Verify, after the write is durable, that the floor has not passed
    //    the basis and the basis manifest still loads, under the verify
    //    budget.
    // 4. On verification failure, release the record — terminally — and
    //    retry against a newer basis under a new id.
    validate_checkpoint_owner(&owner, expires_at_ms)?;
    let timer = StdMonotonicTimer::default();
    let mut saw_root_cas_race = false;
    for _publication_attempt in 0..CONTENTION_RETRY_LIMIT {
        let basis = match try_flush_wal(store, namespace_id, context, &timer).await? {
            TryFlushWal::Settled(basis) => basis,
            TryFlushWal::RaceLost => {
                saw_root_cas_race = true;
                continue;
            }
        };

        let checkpoint_id = CheckpointId::generate();
        let record = CheckpointRecordState {
            checkpoint_id: checkpoint_id.clone(),
            namespace_id: namespace_id.clone(),
            manifest_id: basis.manifest_id,
            manifest_object_id: basis.manifest_object_id.clone(),
            manifest_head_seq: basis.manifest_head_seq,
            manifest_payload_checksum: basis.manifest_payload_checksum.clone(),
            head_commit_id: basis.head_commit_id.clone(),
            created_at_ms: context.now_ms,
            expires_at_ms,
            owner: owner.clone(),
            state: CheckpointRecordLifecycle::Active {},
        };
        let verify_started_ms = timer.monotonic_now_ms();
        write_checkpoint_record(store, &record).await?;

        let verification = match verify_checkpoint_basis(store, &record).await {
            Ok(verification) => verification,
            Err(error) => {
                // Cleanup is best effort on an error and must not replace its
                // original classification.
                if let Err(cleanup_error) =
                    release_checkpoint_record(store, namespace_id, &checkpoint_id, context.now_ms)
                        .await
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
        let within_budget = timer.monotonic_now_ms().saturating_sub(verify_started_ms)
            <= CHECKPOINT_VERIFY_BUDGET_MS;
        if verification == CheckpointBasisVerification::Verified && within_budget {
            return Ok(CreateCheckpointResponse {
                namespace_id: namespace_id.clone(),
                checkpoint_id,
                checkpoint_seq: basis.manifest_head_seq,
                manifest_id: basis.manifest_id,
                current_manifest_id: Some(basis.manifest_id.max(basis.root_manifest_id_at_load)),
                expires_at_ms,
            });
        }

        // Overrunning the budget counts as verification failure: the record
        // may have raced the grace window, so it must not stand as a root.
        release_checkpoint_record(store, namespace_id, &checkpoint_id, context.now_ms).await?;
    }

    if saw_root_cas_race {
        Err(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
    } else {
        Err(CoreError::CheckpointUnavailable(
            "checkpoint publication retry exhausted".to_owned(),
        ))
    }
}

fn validate_checkpoint_owner(owner: &CheckpointOwner, expires_at_ms: Option<u64>) -> Result<()> {
    match owner {
        CheckpointOwner::User { name } => {
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
        CheckpointOwner::Fork { .. } => {
            // A fork record is leased: the expiry bounds the attempt, not
            // the finished fork. Once the target head exists, the live
            // target protects the record whatever the lease says, and an
            // attempt that never got that far is exactly what the lease is
            // for (`namespace/fork.rs`).
            if expires_at_ms.is_none() {
                return Err(CoreError::InvalidCheckpointRequest(
                    "fork-owned checkpoints must carry a lease expiry".to_owned(),
                ));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
pub(super) async fn load_checkpoint_projection_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(HeadState, crate::metadata::MetadataState)> {
    use crate::error::MetadataProjectionLoadError;

    let projection = super::flush::load_root_projection(store, namespace_id).await?;
    let mut metadata_state = MetadataStateBuilder::default();
    for family in CHECKPOINT_TABLE_FAMILIES {
        let mut rows = projection
            .manifest_tables
            .scan_prefix(family, "")
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
        rows.extend(manifest_rows_for_family(&projection.tail_state, family));
        rows.sort_by_key(|row| row.row_key_for_family(family));
        append_rows_to_metadata(&mut metadata_state, family, "checkpoint projection", &rows)
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    }
    Ok((projection.head, metadata_state.finish()))
}
