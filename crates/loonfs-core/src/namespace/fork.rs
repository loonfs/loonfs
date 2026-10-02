//! Fork installation pins a source manifest and lists its run references in
//! a new namespace's first manifest; no segment or content object is copied.

use super::control::load_current_manifest_if_present;
use super::create::publish_namespace;
use crate::cache::MetadataSegmentCache;
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::manifest::{load_namespace_manifest_envelope, MetadataLsmPolicy};
use crate::pin::record::{delete_failed_pin, load_owned_pin, write_pin, PinOwnerKind};
use crate::pin::{classify_live_snapshot, create_pin};
use crate::time::{Deadline, MonotonicTimer};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, NamespaceStatus, PinOwner, PinPayload};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{ManifestNo, NamespaceId, NamespaceMetadata, PinId, WriterEpoch};
use std::sync::Arc;

#[allow(
    clippy::too_many_arguments,
    reason = "fork inputs include the caller's fold policy and cache"
)]
pub(crate) async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    actor_id: &loonfs_types::ActorId,
    snapshot_id: Option<&PinId>,
    context: &MutationContext,
    timer: Arc<dyn MonotonicTimer>,
    fold_policy: MetadataLsmPolicy,
    segment_cache: Option<&MetadataSegmentCache>,
) -> Result<NamespaceMetadata> {
    let deadline = Deadline::start(timer);
    let target = super::control::load_current_manifest_if_present(store, new_namespace_id).await?;
    if let Some(target) = target {
        return Err(if target.state.envelope.payload().status.is_deleted() {
            CoreError::NamespaceDeleted {
                namespace_id: new_namespace_id.clone(),
            }
        } else {
            CoreError::NamespaceExists {
                namespace_id: new_namespace_id.clone(),
            }
        });
    }
    let owner = PinOwner::Fork {
        target_namespace_id: new_namespace_id.clone(),
    };
    let source_record = if let Some(snapshot_id) = snapshot_id {
        create_snapshot_fork_pin(
            store,
            source_namespace_id,
            snapshot_id,
            owner,
            context,
            &deadline,
        )
        .await?
    } else {
        create_pin(
            store,
            source_namespace_id,
            owner,
            context,
            fold_policy,
            segment_cache,
        )
        .await?
    };
    let source_manifest = load_namespace_manifest_envelope(
        store,
        source_namespace_id,
        &source_record.pin_id.manifest_no(),
    )
    .await
    .map_err(|err| CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(err)))?;
    crate::manifest::ensure_manifest_reference_matches(
        "fork pin",
        &source_record.manifest(),
        &source_manifest,
    )?;
    let fork_basis = ForkBasis {
        manifest: source_record.manifest(),
        source_pin_id: source_record.pin_id.clone(),
    };
    let fork_seq = fork_basis.manifest.head_seq;

    let manifest = NamespaceManifestPayload {
        namespace_id: new_namespace_id.clone(),
        created_at_ms: context.now_ms,
        created_by: actor_id.clone(),
        fork_basis: Some(fork_basis),
        manifest_no: ManifestNo(1),
        retention_floor_seq: fork_seq,
        folded_wal_no: loonfs_types::WalNo(0),
        writer_epoch: WriterEpoch(0),
        writer: None,
        compactor_epoch: loonfs_types::CompactorEpoch(0),
        status: NamespaceStatus::Active {},
        activity: Default::default(),
        ..source_manifest.payload().clone()
    };
    if let Err(error) = publish_namespace(store, &manifest, &deadline).await {
        if matches!(
            error,
            CoreError::NamespaceExists { .. } | CoreError::NamespaceDeleted { .. }
        ) {
            match target_retains_pin(store, &source_record, new_namespace_id).await {
                Ok(true) => {}
                Ok(false) => {
                    delete_failed_pin(store, source_namespace_id, &source_record.pin_id, &error)
                        .await;
                }
                Err(check_error) => tracing::warn!(
                    namespace_id = %source_namespace_id,
                    pin_id = %source_record.pin_id,
                    original_error = %error,
                    check_error = %check_error,
                    "kept a fork pin whose target could not be read after installation failed"
                ),
            }
        }
        return Err(error);
    }
    crate::namespace::status::load_namespace(store, new_namespace_id).await
}

async fn create_snapshot_fork_pin<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    snapshot_id: &PinId,
    owner: PinOwner,
    context: &MutationContext,
    deadline: &Deadline,
) -> Result<PinPayload> {
    let snapshot =
        load_snapshot_fork_basis(store, source_namespace_id, snapshot_id, context, deadline)
            .await?;
    let record = PinPayload {
        pin_id: PinId::generate(snapshot.pin_id.manifest_no()),
        created_at_ms: context.now_ms,
        owner,
        ..snapshot
    };
    let verification = async {
        write_pin(store, &record).await?;
        load_snapshot_fork_basis(store, source_namespace_id, snapshot_id, context, deadline).await
    }
    .await;
    if let Err(error) = verification {
        delete_failed_pin(store, source_namespace_id, &record.pin_id, &error).await;
        return Err(error);
    }
    Ok(record)
}

/// Loads a live snapshot of a live source namespace.
async fn load_snapshot_fork_basis<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    snapshot_id: &PinId,
    context: &MutationContext,
    deadline: &Deadline,
) -> Result<PinPayload> {
    let source_head =
        crate::namespace::control::load_namespace_read_state(store, source_namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
    crate::namespace::control::ensure_namespace_live(&source_head)?;
    let snapshot = load_owned_pin(
        store,
        source_namespace_id,
        snapshot_id,
        PinOwnerKind::Snapshot,
    )
    .await?;
    Ok(classify_live_snapshot(snapshot, context.now_at(deadline))?.state)
}

pub(crate) async fn target_retains_pin<S: ObjectStore + ?Sized>(
    store: &S,
    record: &PinPayload,
    target_namespace_id: &NamespaceId,
) -> Result<bool> {
    let Some(target) = load_current_manifest_if_present(store, target_namespace_id).await? else {
        return Ok(false);
    };
    let Some(basis) = target.state.envelope.payload().fork_basis.as_ref() else {
        return Ok(false);
    };
    if basis.source_pin_id != record.pin_id {
        return Ok(false);
    }
    if basis.manifest != record.manifest() {
        return Err(CoreError::NamespaceCorrupt(format!(
            "fork target `{target_namespace_id}` names pin `{}` with a different manifest reference",
            record.pin_id
        )));
    }
    Ok(true)
}
