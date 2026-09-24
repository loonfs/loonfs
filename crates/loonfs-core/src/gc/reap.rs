//! Age checks and pin deletion decisions.

use super::fork_checkpoints::fork_checkpoint_is_retained;
use super::live_set::LiveSet;
use crate::checkpoint::record::load_checkpoint_record_at_key;
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::PinOwner;
use loonfs_api::RetainedReason;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

pub(super) enum CheckpointSweep {
    DeleteFork,
    DeleteUser,
    DeleteSnapshot,
    Gone,
    /// Kept; a user or snapshot pin says when it becomes deletable.
    Retain {
        reclaimable_at_ms: Option<u64>,
    },
}

pub(super) async fn sweep_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    live: &LiveSet,
    context: &MutationContext,
) -> Result<CheckpointSweep> {
    let record = match load_checkpoint_record_at_key(store, key).await {
        Ok(loaded) => loaded.state,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(CheckpointSweep::Gone),
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let deletion = match &record.owner {
        PinOwner::User { .. } => CheckpointSweep::DeleteUser,
        PinOwner::Snapshot { .. } => CheckpointSweep::DeleteSnapshot,
        PinOwner::Fork {
            target_namespace_id,
        } => {
            return Ok(
                match fork_checkpoint_is_retained(
                    store,
                    &record,
                    target_namespace_id,
                    grace_window_ms,
                    context,
                )
                .await?
                {
                    false => CheckpointSweep::DeleteFork,
                    true => {
                        tracing::debug!(object_key = key, "retaining fork pin");
                        CheckpointSweep::Retain {
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
        .namespace_deleted
        .then(|| record.created_at_ms.saturating_add(grace_window_ms));
    let reclaimable_at_ms = expires_at_ms.into_iter().chain(ages_out_at_ms).min();
    Ok(match reclaimable_at_ms {
        Some(at_ms) if context.now_ms >= at_ms => deletion,
        _ => CheckpointSweep::Retain { reclaimable_at_ms },
    })
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

pub(super) async fn grace_age<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    now_ms: u64,
) -> std::result::Result<GraceAge, ObjectStoreError> {
    let Some(metadata) = store.head(key).await? else {
        return Ok(GraceAge::Gone);
    };
    let Some(last_modified_ms) = metadata.last_modified_ms else {
        return Ok(GraceAge::Unknown);
    };
    Ok(
        match now_ms.saturating_sub(last_modified_ms) < grace_window_ms {
            true => GraceAge::Young,
            false => GraceAge::Aged,
        },
    )
}

/// Deletes an unreferenced object once its provider timestamp passes the age gate.
pub async fn delete_if_aged<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    now_ms: u64,
) -> std::result::Result<GraceAge, ObjectStoreError> {
    let age = grace_age(store, key, grace_window_ms, now_ms).await?;
    if age == GraceAge::Aged {
        store.delete(key).await?;
    }
    Ok(age)
}
