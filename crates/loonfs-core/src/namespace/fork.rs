//! Fork installation copies pinned source runs into target manifest 1.

use crate::checkpoint::record::release_checkpoint_record;
use crate::checkpoint::{
    classify_live_snapshot, create_checkpoint, create_checkpoint_at_basis, load_checkpoint_record,
    load_namespace_manifest_envelope,
};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::FORK_INSTALL_BUDGET_MS;
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
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let owner = CheckpointOwner::Fork {
        target_namespace_id: new_namespace_id.clone(),
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
        &source_record.pin_id.manifest_no(),
    )
    .await
    .map_err(|err| CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(err)))?;
    crate::checkpoint::ensure_manifest_reference_matches(
        "fork checkpoint",
        &source_record.manifest(),
        &source_manifest,
    )?;
    let fork_basis = ForkBasis {
        manifest: source_record.manifest(),
        source_checkpoint_id: source_record.pin_id.clone(),
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
    match install_namespace_manifest(store, &manifest, || {
        if timer.monotonic_now_ms().saturating_sub(started_ms) > FORK_INSTALL_BUDGET_MS {
            return Err(CoreError::CheckpointUnavailable(format!(
                "fork of `{source_namespace_id}` into `{new_namespace_id}` exceeded its installation budget"
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
        snapshot.manifest(),
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
        release_checkpoint_record(store, source_namespace_id, &checkpoint.checkpoint_id).await?;
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
