//! Forks a namespace from a checkpoint of the source.

use crate::checkpoint::{
    create_checkpoint, load_checkpoint_record, load_namespace_manifest_envelope,
};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::{FORK_CHECKPOINT_LEASE_MS, FORK_GUARD_MARGIN_MS};
use crate::namespace::bootstrap::{install_namespace_head, NamespaceHeadInstall};
use crate::options::DeleteNamespaceOptions;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::control::{
    CheckpointOwner, CheckpointStatus, ForkBasis, HeadState, NamespaceStatus, WriterBlock,
};
use loonfs_api::{Namespace, NamespaceId, WriterEpoch};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<Namespace> {
    // Include the whole fork in the lease-age calculation.
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    // The checkpoint keeps source metadata alive while the target refers to it.
    // Garbage collection removes the checkpoint if the fork fails.
    let checkpoint = create_checkpoint(
        store,
        source_namespace_id,
        CheckpointOwner::Fork {
            target_namespace_id: new_namespace_id.clone(),
            expires_at_ms: context.now_ms.saturating_add(FORK_CHECKPOINT_LEASE_MS),
        },
        context,
    )
    .await?;
    let source_record =
        load_checkpoint_record(store, source_namespace_id, &checkpoint.checkpoint_id)
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
        &source_record.manifest.manifest_object_id,
    )
    .await
    .map_err(|err| CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(err)))?;
    let source_head = crate::namespace::control::load_head_object(store, source_namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
        .state;
    let fork_basis = ForkBasis {
        manifest: source_record.manifest.clone(),
        source_checkpoint_id: source_record.checkpoint_id.clone(),
    };
    let fork_seq = fork_basis.manifest.manifest_head_seq;

    // The target shares the source's content and starts from its pinned manifest.
    let head = HeadState {
        namespace_id: new_namespace_id.clone(),
        content_store_id: source_head.content_store_id.clone(),
        created_at_ms: context.now_ms,
        fork_basis: Some(fork_basis),
        seq: fork_seq,
        head_commit_id: source_record.head_commit_id.clone(),
        writer_epoch: WriterEpoch(0),
        writer: Some(WriterBlock {
            writer_id: context.writer_id.clone(),
            acquired_at_ms: context.now_ms,
        }),
        next_inode_id: source_manifest.payload.next_inode_id,
        visible_wal_tip: None,
        recent_segments: Vec::new(),
        status: NamespaceStatus::Active {},
    };
    // Refuse to create a target if its checkpoint may expire during installation.
    ensure_fork_checkpoint_lease_holds(
        store,
        source_namespace_id,
        &source_record.checkpoint_id,
        new_namespace_id,
        context
            .now_ms
            .saturating_add(timer.monotonic_now_ms().saturating_sub(started_ms)),
    )
    .await?;
    match install_namespace_head(store, new_namespace_id, &head).await? {
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

    // Confirm that the checkpoint remained valid while the target was installed.
    // If it did not, retire the target before returning the error.
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
            tracing::error!(
                namespace_id = %new_namespace_id,
                source_namespace_id = %source_namespace_id,
                %delete_error,
                "fork could not delete the target it published after losing its source checkpoint",
            );
        }
        return Err(error);
    }

    crate::namespace::status::load_namespace(store, new_namespace_id).await
}

/// Checks that the fork checkpoint is active and remains valid through one
/// provider operation.
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
    let Some(record) = load_checkpoint_record(store, source_namespace_id, checkpoint_id)
        .await?
        .map(|loaded| loaded.state)
    else {
        return lost("the record is gone".to_owned());
    };
    if record.status != (CheckpointStatus::Active {}) {
        return lost(format!("the record is `{}`", record.status));
    }
    let CheckpointOwner::Fork { expires_at_ms, .. } = record.owner else {
        return lost("the record is not fork-owned".to_owned());
    };
    if expires_at_ms <= now_ms.saturating_add(FORK_GUARD_MARGIN_MS) {
        return lost(format!(
            "its lease expires at {expires_at_ms} ms, inside the \
             {FORK_GUARD_MARGIN_MS}ms guard margin at {now_ms}"
        ));
    }
    Ok(())
}
