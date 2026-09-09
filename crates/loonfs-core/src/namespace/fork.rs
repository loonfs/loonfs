//! Fork installation copies pinned source runs into target manifest 1.

use crate::checkpoint::record::{release_checkpoint_record, renew_fork_checkpoint_for_install};
use crate::checkpoint::{
    classify_live_snapshot, create_checkpoint, create_checkpoint_at_basis, load_checkpoint_record,
    load_namespace_manifest_envelope,
};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::{FORK_CHECKPOINT_LEASE_MS, FORK_INSTALL_MARGIN_MS};
use crate::namespace::bootstrap::{install_namespace_manifest, NamespaceInstall};
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::control::{CheckpointOwner, ForkBasis, NamespaceStatus};
use loonfs_api::{CheckpointId, Namespace, NamespaceId, WriterEpoch};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    snapshot_id: Option<&CheckpointId>,
    context: &MutationContext,
) -> Result<Namespace> {
    // Include time already spent on the fork when renewing its lease.
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let owner = CheckpointOwner::Fork {
        target_namespace_id: new_namespace_id.clone(),
        expires_at_ms: context.now_ms.saturating_add(FORK_CHECKPOINT_LEASE_MS),
    };
    let checkpoint = if let Some(snapshot_id) = snapshot_id {
        create_snapshot_fork_checkpoint(store, source_namespace_id, snapshot_id, owner, context)
            .await?
    } else {
        create_checkpoint(store, source_namespace_id, owner, context).await?
    };
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
        &source_record.manifest.manifest_no,
    )
    .await
    .map_err(|err| CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(err)))?;
    crate::checkpoint::ensure_manifest_reference_matches(
        "fork checkpoint",
        &source_record.manifest,
        &source_manifest,
    )?;
    let fork_basis = ForkBasis {
        manifest: source_record.manifest.clone(),
        source_checkpoint_id: source_record.checkpoint_id.clone(),
    };
    let fork_seq = fork_basis.manifest.manifest_head_seq;

    let manifest = loonfs_api::wire::manifest::NamespaceManifestPayload {
        namespace_id: new_namespace_id.clone(),
        created_at_ms: context.now_ms,
        fork_basis: Some(fork_basis),
        manifest_no: loonfs_api::ManifestNo(1),
        retention_floor_seq: fork_seq,
        retention_floor_wal_no: loonfs_api::WalNo(0),
        last_folded_wal_no: loonfs_api::WalNo(0),
        writer_epoch: WriterEpoch(0),
        writer: None,
        compactor_epoch: 0,
        status: NamespaceStatus::Active {},
        ..source_manifest.payload().clone()
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
    match install_namespace_manifest(store, &manifest, || {
        let install_started_at_ms = context.now_ms.saturating_add(timer.monotonic_now_ms().saturating_sub(started_ms));
        if checkpoint_expires_at_ms <= install_started_at_ms.saturating_add(FORK_INSTALL_MARGIN_MS) {
            return Err(CoreError::CheckpointUnavailable(format!(
                "fork of `{source_namespace_id}` into `{new_namespace_id}` cannot install before source checkpoint `{}` expires",
                source_record.checkpoint_id
            )));
        }
        Ok(())
    }).await? {
        NamespaceInstall::Landed => {}
        NamespaceInstall::Exists => {
            return Err(CoreError::NamespaceExists {
                namespace_id: new_namespace_id.clone(),
            })
        }
        NamespaceInstall::Deleted => {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: new_namespace_id.clone(),
            })
        }
    }

    crate::namespace::status::load_namespace(store, new_namespace_id).await
}

async fn create_snapshot_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    snapshot_id: &CheckpointId,
    owner: CheckpointOwner,
    context: &MutationContext,
) -> Result<loonfs_api::Checkpoint> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let snapshot = classify_live_snapshot(
        load_checkpoint_record(store, source_namespace_id, snapshot_id)
            .await?
            .filter(|record| matches!(record.state.owner, CheckpointOwner::Snapshot { .. })),
        snapshot_id,
        context.now_ms,
    )?
    .state;
    let checkpoint = create_checkpoint_at_basis(
        store,
        source_namespace_id,
        owner,
        snapshot.manifest,
        snapshot.head_commit_id,
        context,
    )
    .await?;
    let rechecked = load_checkpoint_record(store, source_namespace_id, snapshot_id)
        .await
        .and_then(|record| {
            classify_live_snapshot(
                record,
                snapshot_id,
                context
                    .now_ms
                    .saturating_add(timer.monotonic_now_ms().saturating_sub(started_ms)),
            )
        });
    if let Err(error) = rechecked {
        release_checkpoint_record(
            store,
            source_namespace_id,
            &checkpoint.checkpoint_id,
            context.now_ms,
        )
        .await?;
        return Err(match error {
            CoreError::SnapshotNotFound { .. } => CoreError::SnapshotGone {
                snapshot_id: snapshot_id.clone(),
                reason: "released".to_owned(),
            },
            error => error,
        });
    }
    Ok(checkpoint)
}
