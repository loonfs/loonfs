//! Checkpoint creation: advance the metadata root to cover the current head,
//! then pin the resulting manifest under one durable checkpoint record.

use super::build::{build_manifest_tables, debug_assert_manifest_table_segments_do_not_overlap};
use super::flush::{try_flush_wal, TryFlushWal};
use super::record::{
    deterministic_checkpoint_id, renew_checkpoint_record, set_checkpoint_record_state,
    verify_checkpoint_basis, write_checkpoint_record, CheckpointRecordWrite,
};
#[cfg(test)]
use super::row::manifest_rows_for_family;
#[cfg(test)]
use super::runs::CHECKPOINT_TABLE_FAMILIES;
use super::runs::{flatten_manifest_tables, MetadataLsmPolicy, CHECKPOINT_BASE_RUN_LEVEL};
use crate::commit::CommitHeadPublishError;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::error::Result;
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::namespace::bootstrap::bootstrap_metadata_state;
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::control::{
    CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState,
};
use loonfs_api::wire::manifest::{NamespaceManifestEnvelope, NamespaceManifestPayload};
use loonfs_api::{ChangeSeq, CreateCheckpointResponse, ManifestId, ManifestObjectId, NamespaceId};
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
    // 2. Write `checkpoints/{id}.json` with state = active.
    // 3. Verify, after the write is durable, that the floor has not passed
    //    the basis and the basis manifest still loads, under the verify
    //    budget.
    // 4. On verification failure, flip the record to released and retry
    //    against a newer basis. An existing condemned record is absorbing:
    //    renewal refuses it until the next GC pass deletes the name.
    validate_checkpoint_owner(&owner, expires_at_ms)?;
    let timer = StdMonotonicTimer::default();
    let mut saw_root_cas_race = false;
    for _publication_attempt in 0..CONTENTION_RETRY_LIMIT {
        let basis = match try_flush_wal(store, namespace_id, context, &timer).await? {
            TryFlushWal::Flushed(basis) => basis,
            TryFlushWal::RaceLost => {
                saw_root_cas_race = true;
                continue;
            }
        };

        let checkpoint_id = deterministic_checkpoint_id(
            namespace_id,
            basis.manifest_id,
            &basis.manifest_object_id,
            &basis.manifest_payload_checksum,
            &owner,
        );
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
            state: CheckpointRecordLifecycle::Active,
        };
        let verify_started_ms = timer.monotonic_now_ms();
        let written = write_checkpoint_record(store, &record, &context.writer_version).await?;

        let verified = verify_checkpoint_basis(store, &record).await?;
        let within_budget = timer.monotonic_now_ms().saturating_sub(verify_started_ms)
            <= CHECKPOINT_VERIFY_BUDGET_MS;
        if verified && within_budget {
            if let CheckpointRecordWrite::Existing = written {
                // Deterministic ids make re-creation a renewal: the durable
                // expiry becomes exactly what this create requested (last
                // write wins, shrink and clear included), and a released
                // record for the same verified basis and owner is revived
                // rather than duplicated — so the response below always
                // echoes the durable state.
                renew_checkpoint_record(
                    store,
                    namespace_id,
                    &checkpoint_id,
                    expires_at_ms,
                    &context.writer_version,
                )
                .await?;
            }
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
        set_checkpoint_record_state(
            store,
            namespace_id,
            &checkpoint_id,
            CheckpointRecordLifecycle::Released,
            &context.writer_version,
        )
        .await?;
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
            // A fork pin lives exactly as long as its target may read the
            // basis; wall-clock expiry can never bound that.
            if expires_at_ms.is_some() {
                return Err(CoreError::InvalidCheckpointRequest(
                    "fork-owned checkpoints must not carry an expiry".to_owned(),
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

pub(crate) async fn build_initial_namespace_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    initial_head: &HeadState,
    writer_version: &str,
) -> Result<NamespaceManifestEnvelope> {
    let manifest_id = ManifestId(initial_head.seq.0);
    let manifest_object_id = ManifestObjectId::generate(manifest_id);
    let metadata_state = bootstrap_metadata_state();
    let run_tables = build_manifest_tables(
        store,
        namespace_id,
        initial_head.seq,
        CHECKPOINT_BASE_RUN_LEVEL,
        &metadata_state,
        MetadataLsmPolicy::default().max_rows_per_segment,
    )
    .await?;
    debug_assert_manifest_table_segments_do_not_overlap(&run_tables);

    NamespaceManifestEnvelope::from_payload(
        writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id,
            manifest_object_id,
            head_seq: initial_head.seq,
            head_commit_id: initial_head.head_commit_id.clone(),
            base_seq: initial_head.seq,
            writer_epoch: initial_head.writer_epoch,
            next_inode_id: initial_head.next_inode_id,
            // Bootstrap precedes the floor object; nothing is retained
            // below the genesis seq.
            retention_floor_seq: ChangeSeq(0),
            fork: None,
            metadata_files: flatten_manifest_tables(run_tables),
        },
    )
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace manifest envelope: {err}"
        ))
    })
}
