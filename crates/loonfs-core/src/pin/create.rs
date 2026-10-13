//! Folds the WAL and creates a verified pin for the resulting manifest.

use super::record::{delete_failed_pin, verify_pin_basis, write_pin, PinBasisVerification};
use crate::cache::MetadataSegmentCache;
use crate::context::MutationContext;
use crate::control_update::{retry_while_contended, CasOutcome};
use crate::error::CoreError;
use crate::error::Result;
use crate::manifest::{try_fold_wal, MetadataRunPolicy, TryFoldWalOutcome};
use crate::time::{Deadline, MonotonicTimer};
use crate::wal::WalPublishError;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{PinOwner, PinPayload};
use loonfs_types::{NamespaceId, PinId};
use std::sync::Arc;

/// Longest accepted user checkpoint name. A label bound, not a durable
/// format limit.
const MAX_CHECKPOINT_NAME_CHARS: usize = 128;

#[allow(
    clippy::too_many_arguments,
    reason = "pin creation folds first, so it takes the caller's fold policy, cache, write permits, and timer"
)]
pub(crate) async fn create_pin<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    owner: PinOwner,
    context: &MutationContext,
    policy: MetadataRunPolicy,
    segment_cache: Option<&MetadataSegmentCache>,
    content_writes: &tokio::sync::Semaphore,
    timer: Arc<dyn MonotonicTimer>,
) -> Result<PinPayload> {
    validate_pin_owner(&owner)?;
    let deadline = Deadline::start(timer);
    let deadline = &deadline;
    let owner = &owner;
    let created = retry_while_contended(|| async move {
        let basis = match try_fold_wal(
            store,
            namespace_id,
            deadline,
            policy,
            segment_cache,
            content_writes,
        )
        .await?
        {
            TryFoldWalOutcome::Settled(basis) => basis,
            TryFoldWalOutcome::RaceLost(_) => {
                return Ok(CasOutcome::Contended(CoreError::WalPublish(
                    WalPublishError::NumberTaken,
                )))
            }
        };

        match create_pin_at_basis(
            store,
            namespace_id,
            owner.clone(),
            basis.manifest.clone(),
            context,
        )
        .await
        {
            Ok(pin) => Ok(CasOutcome::Settled(pin)),
            Err(error @ CoreError::CheckpointUnavailable(_)) => Ok(CasOutcome::Contended(error)),
            Err(error) => Err(error),
        }
    })
    .await?;
    created
}

pub(crate) async fn create_pin_at_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    owner: PinOwner,
    manifest: loonfs_types::format::control::ManifestRef,
    context: &MutationContext,
) -> Result<PinPayload> {
    validate_pin_owner(&owner)?;
    let pin_id = PinId::generate(manifest.manifest_no);
    let record = PinPayload {
        pin_id: pin_id.clone(),
        namespace_id: namespace_id.clone(),
        head_seq: manifest.head_seq,
        payload_checksum: manifest.payload_checksum,
        created_at_ms: context.now_ms,
        owner,
    };
    let verification = async {
        write_pin(store, &record).await?;
        verify_pin_basis(store, &record).await
    }
    .await;
    let error = match verification {
        Ok(PinBasisVerification::Verified) => return Ok(record),
        Ok(PinBasisVerification::Invalid) => {
            CoreError::CheckpointUnavailable("checkpoint publication retry exhausted".to_owned())
        }
        Err(error) => error,
    };
    delete_failed_pin(store, namespace_id, &pin_id, &error).await;
    Err(error)
}

fn validate_pin_owner(owner: &PinOwner) -> Result<()> {
    match owner {
        PinOwner::User { name, .. } => validate_checkpoint_name(name),
        PinOwner::Snapshot {
            name,
            expires_at_ms,
        } => {
            validate_checkpoint_name(name)?;
            if *expires_at_ms == 0 {
                return Err(CoreError::InvalidCheckpointRequest(
                    "snapshot expiry must not be zero".to_owned(),
                ));
            }
            Ok(())
        }
        PinOwner::Fork { .. } => Ok(()),
    }
}

fn validate_checkpoint_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CoreError::InvalidCheckpointRequest(
            "checkpoint name must not be empty".to_owned(),
        ));
    }
    if name.chars().count() > MAX_CHECKPOINT_NAME_CHARS {
        return Err(CoreError::InvalidCheckpointRequest(format!(
            "checkpoint name exceeds {MAX_CHECKPOINT_NAME_CHARS} characters"
        )));
    }
    Ok(())
}
