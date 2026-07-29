//! Namespace forking: installs a target namespace whose head points at a
//! fork-owned source checkpoint, sharing content bytes and reading the
//! source's metadata until the target flushes its own.

use crate::checkpoint::{
    create_checkpoint, freshen_fork_checkpoint, load_namespace_manifest_envelope,
    read_checkpoint_record,
};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::bootstrap::{install_namespace_head, NamespaceHeadInstall};
use crate::options::DeleteNamespaceOptions;
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
    // Fork routes through a fork-owned source checkpoint: the record is the
    // reachability root protecting every source-owned metadata file the
    // target will reference, for as long as the target lives.
    let checkpoint = create_checkpoint(
        store,
        source_namespace_id,
        CheckpointOwner::Fork {
            target_namespace_id: new_namespace_id.clone(),
        },
        None,
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
    // Freshen the record before the target head lands: the compare-and-swap
    // serializes this fork against a concurrent GC release, and the fresh
    // provider timestamp keeps the abandoned-fork age rule from firing under
    // a live retry.
    freshen_fork_checkpoint(
        store,
        source_namespace_id,
        &source_record.checkpoint_id,
        new_namespace_id,
        &context.writer_version,
    )
    .await?;

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

    // A forker that stalled between freshening the record and publishing
    // the target could have slept past a garbage-collection pass that
    // released the pin, leaving a target whose basis nothing protects.
    // Re-reading the record after the head lands closes that window
    // conservatively: an inactive record means this target must not exist,
    // so it is deleted through the ordinary delete path and the checkpoint
    // failure is what the caller sees. The lasting fix is a monotonic
    // checkpoint lifecycle, not a wider retry here.
    if let Err(error) = ensure_fork_checkpoint_still_active(
        store,
        source_namespace_id,
        &source_record.checkpoint_id,
        new_namespace_id,
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

async fn ensure_fork_checkpoint_still_active<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    checkpoint_id: &loonfs_api::CheckpointId,
    new_namespace_id: &NamespaceId,
) -> Result<()> {
    let record = read_checkpoint_record(store, source_namespace_id, checkpoint_id)
        .await?
        .map(|loaded| loaded.state);
    match record {
        Some(record) if record.state == CheckpointRecordLifecycle::Active => Ok(()),
        Some(record) => Err(CoreError::CheckpointUnavailable(format!(
            "fork of `{source_namespace_id}` into `{new_namespace_id}` lost its source \
             checkpoint `{checkpoint_id}`: the record is `{}`",
            record.state
        ))),
        None => Err(CoreError::CheckpointUnavailable(format!(
            "fork of `{source_namespace_id}` into `{new_namespace_id}` lost its source \
             checkpoint `{checkpoint_id}`: the record is gone"
        ))),
    }
}
