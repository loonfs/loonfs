//! Namespace forking: installs a target namespace whose head points at a
//! fork-owned source checkpoint, sharing content bytes and reading the
//! source's metadata until the target flushes its own.

use crate::checkpoint::{
    create_checkpoint, load_namespace_manifest_envelope, read_checkpoint_record,
};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::{FORK_CHECKPOINT_LEASE_MS, FORK_GUARD_MARGIN_MS};
use crate::namespace::bootstrap::{install_namespace_head, NamespaceHeadInstall};
use crate::options::DeleteNamespaceOptions;
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::control::{
    CheckpointOwner, CheckpointRecordLifecycle, ForkBasis, HeadState, NamespaceState, WriterBlock,
};
use loonfs_api::{NamespaceId, NamespaceSummary, WriterEpoch};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<NamespaceSummary> {
    // The guard at the end needs to know how long this attempt has been
    // running, so it starts here, before the first write.
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    // Fork routes through a fork-owned source checkpoint: the record is the
    // reachability root protecting every source-owned metadata file the
    // target will reference, for as long as the target lives.
    //
    // Every attempt creates its own leased record. There is no reuse of an
    // earlier attempt's record and no way back from a release, so an attempt
    // that dies before publishing its target simply lets the lease pass, and
    // garbage collection releases and reaps the record on that alone.
    let checkpoint = create_checkpoint(
        store,
        source_namespace_id,
        CheckpointOwner::Fork {
            target_namespace_id: new_namespace_id.clone(),
        },
        Some(context.now_ms.saturating_add(FORK_CHECKPOINT_LEASE_MS)),
        context,
    )
    .await?;
    let source_record =
        read_checkpoint_record(store, source_namespace_id, &checkpoint.checkpoint_id)
            .await?
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "source checkpoint `{}` disappeared during fork",
                    checkpoint.checkpoint_id
                ))
            })?
            .state;
    let source_manifest = load_namespace_manifest_envelope(
        store,
        source_namespace_id,
        &source_record.manifest_object_id,
    )
    .await
    .map_err(|err| CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(err)))?;
    let source_head = crate::namespace::control::read_head_object(store, source_namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .envelope
        .state;
    let fork_seq = source_record.manifest_head_seq;

    // The target head is the whole installation: it carries the source's
    // content store (the fork shares content bytes copy-on-write), the
    // source's name policy, and the basis that authorizes reading the
    // source's manifest until the target publishes its own.
    let head = HeadState {
        namespace_id: new_namespace_id.clone(),
        content_store_id: source_head.content_store_id.clone(),
        name_policy: source_head.name_policy,
        fork_basis: Some(ForkBasis {
            source_namespace_id: source_namespace_id.clone(),
            source_manifest_object_id: source_record.manifest_object_id.clone(),
            source_manifest_checksum: source_manifest.payload_checksum.clone(),
            source_checkpoint_id: source_record.checkpoint_id.clone(),
            fork_seq,
        }),
        seq: fork_seq,
        head_commit_id: source_record.head_commit_id.clone(),
        writer_epoch: WriterEpoch(0),
        writer: Some(WriterBlock {
            writer_id: context.writer_id.clone(),
            writer_session_id: context.writer_session_id.clone(),
            acquired_at_ms: context.now_ms,
        }),
        next_inode_id: source_manifest.payload.next_inode_id,
        visible_wal_tip: None,
        recent_segments: Vec::new(),
        state: NamespaceState::Active,
    };
    match install_namespace_head(store, new_namespace_id, &head, &context.writer_version).await? {
        NamespaceHeadInstall::Landed => {}
        NamespaceHeadInstall::Exists => {
            return Err(CoreError::NamespaceExists {
                namespace_id: new_namespace_id.clone(),
            })
        }
        NamespaceHeadInstall::Deleted => {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: new_namespace_id.clone(),
            })
        }
    }

    // A forker that stalled between creating the record and publishing the
    // target could have slept past its own lease, and a garbage-collection
    // pass could have released the pin, leaving a target whose basis nothing
    // protects. The guard closes that window: an inactive record, or one too
    // close to its lease to trust, means this target must not exist, so it
    // is deleted through the ordinary delete path and the checkpoint failure
    // is what the caller sees.
    if let Err(error) = ensure_fork_checkpoint_lease_holds(
        store,
        source_namespace_id,
        &source_record.checkpoint_id,
        new_namespace_id,
        context
            .now_ms
            .saturating_add(timer.monotonic_now_ms().saturating_sub(started_ms)),
    )
    .await
    {
        if let Err(delete_error) = crate::commit_engine::delete_namespace(
            store,
            new_namespace_id,
            DeleteNamespaceOptions::default(),
            context,
        )
        .await
        {
            // Both halves failed. The caller hears why the fork failed,
            // which is the actionable half; the target that could not be
            // deleted is left for an operator, named here.
            tracing::error!(
                namespace_id = %new_namespace_id,
                source_namespace_id = %source_namespace_id,
                %delete_error,
                "fork could not delete the target it published after losing its source checkpoint",
            );
        }
        return Err(error);
    }

    Ok(NamespaceSummary {
        namespace_id: new_namespace_id.clone(),
    })
}

/// Proves, after the target head is durable, that the source record still
/// pins the basis and will keep pinning it long enough for the new target to
/// take over that job.
///
/// The record must be active, and its lease must outlast this read by
/// [`FORK_GUARD_MARGIN_MS`]. The margin is what makes the check sound where a
/// bare re-read raced: garbage collection releases a fork record only once
/// its lease has passed, so a lease with more than one provider operation
/// left cannot legally be released between the read and the caller acting on
/// it. After that point the target head itself is the protection — a fork
/// record whose target namespace exists and is not deleted is retained by
/// every pass, lease or no lease.
async fn ensure_fork_checkpoint_lease_holds<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    checkpoint_id: &loonfs_api::CheckpointId,
    new_namespace_id: &NamespaceId,
    now_ms: u64,
) -> Result<()> {
    let lost = |reason: String| {
        Err(CoreError::CheckpointUnavailable(format!(
            "fork of `{source_namespace_id}` into `{new_namespace_id}` lost its source \
             checkpoint `{checkpoint_id}`: {reason}"
        )))
    };
    let Some(record) = read_checkpoint_record(store, source_namespace_id, checkpoint_id)
        .await?
        .map(|loaded| loaded.state)
    else {
        return lost("the record is gone".to_owned());
    };
    if record.state != CheckpointRecordLifecycle::Active {
        return lost(format!("the record is `{}`", record.state));
    }
    let holds = record
        .expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms > now_ms.saturating_add(FORK_GUARD_MARGIN_MS));
    if !holds {
        return lost(format!(
            "the lease ({:?}) is inside the {FORK_GUARD_MARGIN_MS}ms guard margin at {now_ms}",
            record.expires_at_ms
        ));
    }
    Ok(())
}
