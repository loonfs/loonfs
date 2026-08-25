//! Namespace forking: installs a target namespace whose head points at a
//! fork-owned source checkpoint, sharing content bytes and reading the
//! source's metadata until the target flushes its own.

use crate::checkpoint::record::renew_fork_checkpoint_for_install;
use crate::checkpoint::{
    create_checkpoint, load_checkpoint_record, load_namespace_manifest_envelope,
};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::{FORK_CHECKPOINT_LEASE_MS, FORK_INSTALL_MARGIN_MS};
use crate::namespace::bootstrap::{install_namespace_head, NamespaceHeadInstall};
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::control::{
    CheckpointOwner, ForkBasis, HeadState, NamespaceStatus, WriterBlock,
};
use loonfs_api::{Namespace, NamespaceId, WriterEpoch};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<Namespace> {
    // Include time already spent on the fork when renewing its lease.
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    // Each attempt creates a checkpoint that keeps the target's source
    // metadata alive. GC removes abandoned checkpoints after their lease.
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

    // The target shares content bytes and starts from the source manifest.
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
    // Renew before creating the target so this races safely with GC release.
    let checkpoint_expires_at_ms = renew_fork_checkpoint_for_install(
        store,
        source_namespace_id,
        &source_record.checkpoint_id,
        new_namespace_id,
        context
            .now_ms
            .saturating_add(timer.monotonic_now_ms().saturating_sub(started_ms)),
    )
    .await?;
    let install_started_at_ms = context
        .now_ms
        .saturating_add(timer.monotonic_now_ms().saturating_sub(started_ms));
    if checkpoint_expires_at_ms <= install_started_at_ms.saturating_add(FORK_INSTALL_MARGIN_MS) {
        return Err(CoreError::CheckpointUnavailable(format!(
            "fork of `{source_namespace_id}` into `{new_namespace_id}` cannot install before its \
             source checkpoint `{}` expires",
            source_record.checkpoint_id
        )));
    }
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

    crate::namespace::status::load_namespace(store, new_namespace_id).await
}
