//! Fork installation copies pinned source runs into a new target generation.

use super::recreation::{write_retired_pin, GenerationSuccessor};
use crate::checkpoint::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::checkpoint::record::{
    checkpoint_is_visible, delete_checkpoint_record, write_checkpoint_record,
};
use crate::checkpoint::{
    classify_live_snapshot, create_checkpoint, load_checkpoint_record,
    load_namespace_manifest_envelope,
};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::{FORK_INSTALL_BUDGET_MS, PIN_VERIFY_BUDGET_MS};
use crate::namespace::bootstrap::{
    install_namespace_manifest, write_content_store_descriptor, NamespaceInstall,
};
use crate::namespace::control::load_current_manifest;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::control::{ForkBasis, NamespaceStatus, PinOwner, PinPayload};
use loonfs_api::wire::manifest::NamespaceManifestPayload;
use loonfs_api::{ManifestNo, Namespace, NamespaceGeneration, NamespaceId, PinId, WriterEpoch};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    actor_id: &loonfs_api::ActorId,
    snapshot_id: Option<&PinId>,
    context: &MutationContext,
) -> Result<Namespace> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let owner = PinOwner::Fork {
        target_namespace_id: new_namespace_id.clone(),
    };
    let source_record = if let Some(snapshot_id) = snapshot_id {
        create_snapshot_fork_checkpoint(store, source_namespace_id, snapshot_id, owner, context)
            .await?
    } else {
        let checkpoint = create_checkpoint(store, source_namespace_id, owner, context).await?;
        load_checkpoint_record(store, source_namespace_id, &checkpoint.checkpoint_id)
            .await?
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "source checkpoint `{}` disappeared during fork",
                    checkpoint.checkpoint_id
                ))
            })?
            .state
    };
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
        source_pin_id: source_record.pin_id.clone(),
        source_generation: source_manifest.payload().generation,
    };
    let fork_seq = fork_basis.manifest.head_seq;

    let manifest = NamespaceManifestPayload {
        namespace_id: new_namespace_id.clone(),
        created_at_ms: context.now_ms,
        created_by: actor_id.clone(),
        fork_basis: Some(fork_basis),
        manifest_no: ManifestNo(1),
        generation: NamespaceGeneration(1),
        generation_first_manifest_no: ManifestNo(1),
        retention_floor_seq: fork_seq,
        folded_wal_no: loonfs_api::WalNo(0),
        writer_epoch: WriterEpoch(0),
        writer: None,
        compactor_epoch: 0,
        status: NamespaceStatus::Active {},
        activity: Default::default(),
        ..source_manifest.payload().clone()
    };
    let validate_install = || {
        if timer.monotonic_now_ms().saturating_sub(started_ms) > FORK_INSTALL_BUDGET_MS {
            return Err(CoreError::CheckpointUnavailable(format!(
                "fork of `{source_namespace_id}` into `{new_namespace_id}` exceeded its installation budget"
            )));
        }
        Ok(())
    };
    match install_namespace_manifest(store, &manifest, validate_install).await? {
        NamespaceInstall::Landed => {}
        NamespaceInstall::Exists => {
            return Err(CoreError::NamespaceExists {
                namespace_id: new_namespace_id.clone(),
            })
        }
        NamespaceInstall::Deleted => {
            recreate_fork_namespace(store, &manifest, &timer, started_ms, validate_install).await?;
        }
    }

    crate::namespace::status::load_namespace(store, new_namespace_id).await
}

async fn recreate_fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    fork_manifest: &NamespaceManifestPayload,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
    validate_install: impl Fn() -> Result<()>,
) -> Result<()> {
    loop {
        let current = load_current_manifest(store, &fork_manifest.namespace_id).await?;
        let tombstone = current.envelope.payload();
        if !tombstone.status.is_deleted() {
            return Err(CoreError::NamespaceExists {
                namespace_id: fork_manifest.namespace_id.clone(),
            });
        }
        let successor = GenerationSuccessor::from_tombstone(tombstone)?;
        let mut payload = fork_manifest.clone();
        successor.apply_to(&mut payload);
        let manifest = encode_manifest(payload)?;

        validate_install()?;
        write_retired_pin(store, &current, fork_manifest.created_at_ms).await?;
        write_content_store_descriptor(
            store,
            &fork_manifest.content_store_id,
            fork_manifest.created_at_ms,
        )
        .await?;
        validate_install()?;
        if let ManifestPublicationOutcome::Published(_) = publish_manifest(
            store,
            &fork_manifest.namespace_id,
            manifest,
            Some(tombstone.manifest_no),
            timer,
            started_ms,
        )
        .await?
        {
            return Ok(());
        }
    }
}

async fn create_snapshot_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    snapshot_id: &PinId,
    owner: PinOwner,
    context: &MutationContext,
) -> Result<PinPayload> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let source_head =
        crate::namespace::control::load_namespace_read_state(store, source_namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
    crate::namespace::control::ensure_namespace_live(&source_head)?;
    if !checkpoint_is_visible(&source_head, snapshot_id) {
        return Err(CoreError::SnapshotNotFound {
            snapshot_id: snapshot_id.clone(),
        });
    }
    let snapshot = classify_live_snapshot(
        load_checkpoint_record(store, source_namespace_id, snapshot_id)
            .await?
            .filter(|record| matches!(record.state.owner, PinOwner::Snapshot { .. })),
        snapshot_id,
        context.now_ms,
    )?
    .state;
    let record = PinPayload {
        pin_id: PinId::generate(snapshot.manifest_no),
        created_at_ms: context.now_ms,
        owner,
        ..snapshot
    };
    write_checkpoint_record(store, &record).await?;
    let rechecked = verify_snapshot_fork_basis(
        store,
        source_namespace_id,
        snapshot_id,
        context.now_ms,
        &timer,
        started_ms,
    )
    .await;
    if let Err(error) = rechecked {
        delete_checkpoint_record(store, source_namespace_id, &record.pin_id).await?;
        return Err(error);
    }
    if timer.monotonic_now_ms().saturating_sub(started_ms) > PIN_VERIFY_BUDGET_MS {
        delete_checkpoint_record(store, source_namespace_id, &record.pin_id).await?;
        return Err(CoreError::CheckpointUnavailable(
            "snapshot fork verification exceeded its budget".to_owned(),
        ));
    }
    Ok(record)
}

async fn verify_snapshot_fork_basis<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    snapshot_id: &PinId,
    now_ms: u64,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
) -> Result<()> {
    let snapshot = load_checkpoint_record(store, source_namespace_id, snapshot_id)
        .await?
        .ok_or_else(|| CoreError::SnapshotGone {
            snapshot_id: snapshot_id.clone(),
            reason: "deleted".to_owned(),
        })?;
    classify_live_snapshot(
        Some(snapshot),
        snapshot_id,
        now_ms.saturating_add(timer.monotonic_now_ms().saturating_sub(started_ms)),
    )?;
    Ok(())
}
