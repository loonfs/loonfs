//! Pin deletion decisions, the collection rules for fork pins, and grace
//! window age checks.

use super::live_set::LiveSet;
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::fork::target_retains_pin;
use crate::pin::record::load_pin_at_key;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::control::{ForkBasis, PinOwner, PinPayload};
use loonfs_types::{NamespaceId, RetainedReason};

pub(super) enum PinSweep {
    DeleteFork,
    DeleteUser,
    Gone,
    /// Kept. `reclaimable_at_ms` is when a user pin becomes deletable.
    Retain {
        reclaimable_at_ms: Option<u64>,
    },
}

pub(super) async fn sweep_pin<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    live: &LiveSet,
    context: &MutationContext,
) -> Result<PinSweep> {
    let record = match load_pin_at_key(store, key).await {
        Ok(loaded) => loaded.state,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(PinSweep::Gone),
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let deletion = match &record.owner {
        PinOwner::User { .. } => PinSweep::DeleteUser,
        PinOwner::Fork {
            target_namespace_id,
        } => {
            return Ok(
                match fork_pin_is_retained(
                    store,
                    &record,
                    target_namespace_id,
                    grace_window_ms,
                    context,
                )
                .await?
                {
                    false => PinSweep::DeleteFork,
                    true => {
                        tracing::debug!(object_key = key, "retaining fork pin");
                        PinSweep::Retain {
                            reclaimable_at_ms: None,
                        }
                    }
                },
            );
        }
    };
    let expires_at_ms = record
        .owner
        .expires_at_ms()
        .map(|expiry| expiry.saturating_add(grace_window_ms));
    let ages_out_at_ms = live
        .current_tombstone
        .is_some()
        .then(|| record.created_at_ms.saturating_add(grace_window_ms));
    let reclaimable_at_ms = expires_at_ms.into_iter().chain(ages_out_at_ms).min();
    Ok(match reclaimable_at_ms {
        Some(at_ms) if context.now_ms >= at_ms => deletion,
        _ => PinSweep::Retain { reclaimable_at_ms },
    })
}

async fn fork_pin_is_retained<S: ObjectStore + ?Sized>(
    store: &S,
    record: &PinPayload,
    target_namespace_id: &NamespaceId,
    grace_window_ms: u64,
    context: &MutationContext,
) -> Result<bool> {
    if context.now_ms.saturating_sub(record.created_at_ms) < grace_window_ms {
        return Ok(true);
    }
    match target_retains_pin(store, record, target_namespace_id).await {
        Err(CoreError::ControlObjectLoad(error @ ControlObjectLoadError::Store { .. })) => {
            tracing::warn!(
                namespace_id = %target_namespace_id,
                error = %error,
                "the fork target did not read; retaining its source pin"
            );
            Ok(true)
        }
        result => result,
    }
}

pub(super) async fn delete_source_pin<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &ForkBasis,
) -> Result<bool> {
    let key =
        loonfs_objectstore::keys::pin(&basis.manifest.owner_namespace_id, &basis.source_pin_id);
    let present = store
        .get_with_metadata(&key)
        .await
        .map_err(|error| CoreError::store(&key, &error))?
        .is_some();
    crate::pin::record::delete_pin(
        store,
        &basis.manifest.owner_namespace_id,
        &basis.source_pin_id,
    )
    .await?;
    Ok(present)
}

/// Where one unreferenced candidate stands against the grace window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraceAge {
    /// The window has passed over the object.
    Aged,
    /// Younger than the window by its own provider timestamp.
    Young,
    /// The provider reported no last-modified time, so the object's age is
    /// unknown and it is treated as young (rule 1).
    Unknown,
    /// The object is not there any more.
    Gone,
}

impl GraceAge {
    /// Compares a listed modification time with the pass's grace window.
    pub fn of(last_modified_at_ms: Option<u64>, grace_window_ms: u64, now_ms: u64) -> Self {
        match last_modified_at_ms {
            None => Self::Unknown,
            Some(modified) if now_ms.saturating_sub(modified) < grace_window_ms => Self::Young,
            Some(_) => Self::Aged,
        }
    }

    /// The retention reason this outcome is, for a caller reporting why a
    /// pass kept what it kept. `None` for the two outcomes that retained
    /// nothing.
    pub fn retained_reason(self) -> Option<RetainedReason> {
        match self {
            Self::Aged | Self::Gone => None,
            Self::Young => Some(RetainedReason::WithinGraceWindow),
            Self::Unknown => Some(RetainedReason::NoProviderTimestamp),
        }
    }
}

/// Reads where one object stands against the grace window by its provider timestamp.
pub async fn grace_age<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    now_ms: u64,
) -> std::result::Result<GraceAge, ObjectStoreError> {
    let Some(metadata) = store.head(key).await? else {
        return Ok(GraceAge::Gone);
    };
    Ok(GraceAge::of(
        metadata.last_modified_at_ms,
        grace_window_ms,
        now_ms,
    ))
}
