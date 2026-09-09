//! Age checks and pin deletion decisions.

use super::fork_checkpoints::{classify_fork_checkpoint, ForkCheckpointReachability};
use crate::checkpoint::record::load_checkpoint_record_at_key;
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::CheckpointOwner;
use loonfs_api::RetainedReason;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

pub(super) enum CheckpointSweep {
    DeleteFork,
    DeleteUser,
    DeleteSnapshot,
    Gone,
    Retain,
}

pub(super) async fn sweep_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    namespace_deleted: bool,
    context: &MutationContext,
) -> Result<CheckpointSweep> {
    let record = match load_checkpoint_record_at_key(store, key).await {
        Ok(loaded) => loaded.state,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(CheckpointSweep::Gone),
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let deletion = match &record.owner {
        CheckpointOwner::User { .. } => CheckpointSweep::DeleteUser,
        CheckpointOwner::Snapshot { .. } => CheckpointSweep::DeleteSnapshot,
        CheckpointOwner::Fork {
            target_namespace_id,
        } => {
            return Ok(
                match classify_fork_checkpoint(
                    store,
                    &record,
                    target_namespace_id,
                    grace_window_ms,
                    context,
                )
                .await?
                {
                    ForkCheckpointReachability::Reclaimable => CheckpointSweep::DeleteFork,
                    ForkCheckpointReachability::Retained { reason } => {
                        tracing::debug!(object_key = key, reason, "retaining fork pin");
                        CheckpointSweep::Retain
                    }
                },
            );
        }
    };
    let expired = record.owner.expires_at_ms().is_some_and(|expiry| {
        context.now_ms >= expiry && context.now_ms.saturating_sub(expiry) >= grace_window_ms
    });
    let deleted =
        namespace_deleted && context.now_ms.saturating_sub(record.created_at_ms) >= grace_window_ms;
    Ok(if expired || deleted {
        deletion
    } else {
        CheckpointSweep::Retain
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
