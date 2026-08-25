//! Namespace forking: installs a target namespace whose head points at a
//! fork-owned source checkpoint, sharing content bytes and reading the
//! source's metadata until the target flushes its own.
//!
//! One window is not fenced: an attempt that stalls longer than a full
//! fresh lease between its renewal and its head write can still install a
//! target after a collector released the record. Garbage collection's
//! backstop is that a live target naming a record exactly retains it
//! whatever the record's status says.

use crate::checkpoint::record::renew_fork_checkpoint_for_install;
use crate::checkpoint::{
    create_checkpoint, load_checkpoint_record, load_namespace_manifest_envelope,
};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::FORK_CHECKPOINT_LEASE_MS;
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
    // The renewal stamps how long this attempt has already run, so the
    // timer starts before the first write.
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
    // Start the target from the manifest pinned by the source checkpoint.
    let fork_basis = ForkBasis {
        manifest: source_record.manifest.clone(),
        source_checkpoint_id: source_record.checkpoint_id.clone(),
    };
    let fork_seq = fork_basis.manifest.manifest_head_seq;

    // The target shares the source's content store and begins from the source
    // manifest until it publishes its own.
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
    // The handoff: the target head is written only after a compare-and-swap
    // proved this attempt still owns an active pin.
    renew_fork_checkpoint_for_install(
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

    crate::namespace::status::load_namespace(store, new_namespace_id).await
}
