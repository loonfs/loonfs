//! Reaping: age-gated deletion of unreachable objects.
//!
//! Immutable objects use provider timestamps for age checks. Checkpoints use
//! lifecycle timestamps stored in their records. The namespace sweep combines
//! object age with the reference anchor from `gc/live_set.rs`.

use crate::checkpoint::record::{
    load_checkpoint_record_at_key, release_inspected_checkpoint_record, CheckpointRelease,
};
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::{CheckpointOwner, CheckpointRecordState, CheckpointStatus};
use loonfs_api::{NamespaceId, RetainedReason};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CheckpointSweep {
    /// The record is released and its release has aged past the grace
    /// window; the key may be deleted.
    Delete,
    /// This pass flipped the record `active -> released`.
    Released,
    /// This pass flipped a snapshot record `active -> released`.
    ReleasedSnapshot,
    Retain,
}

/// True once a record's own lease has passed. Checkpoint aging reads the
/// record, never the object's provider timestamp: the record carries every
/// instant its lifecycle depends on.
pub(super) fn lease_expired(record: &CheckpointRecordState, now_ms: u64) -> bool {
    record
        .owner
        .expires_at_ms()
        .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
}

/// Advances one collectable checkpoint record along the only path it has.
///
/// An active record whose lease has passed — or one on a terminally deleted
/// namespace, where nothing can read it again — is released by
/// compare-and-swap on the exact etag inspected, stamping the release
/// instant. A released record is deletable once that stamp is a grace window
/// old. Nothing here reads a provider timestamp: `released_at_ms` and
/// `created_at_ms` are what the record's own age is measured from, and
/// `expires_at_ms` is what its lease is measured from.
///
/// A failed CAS means the record changed and is retained without retry.
pub(super) async fn sweep_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    key: &str,
    grace_window_ms: u64,
    namespace_deleted: bool,
    context: &MutationContext,
) -> Result<CheckpointSweep> {
    let loaded = load_checkpoint_record_at_key(store, key).await;
    let loaded = match loaded {
        Ok(loaded) => loaded,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(CheckpointSweep::Retain),
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let record = &loaded.state;
    if let CheckpointStatus::Released { released_at_ms } = record.status {
        let aged = context.now_ms.saturating_sub(released_at_ms) >= grace_window_ms;
        return Ok(if aged {
            CheckpointSweep::Delete
        } else {
            CheckpointSweep::Retain
        });
    }
    // A fork pin is never released here, whatever its lease says: only the
    // fork arm knows whether the target is still reading through it
    // (`fork_checkpoints.rs`), and it has already had its say by this point.
    if matches!(record.owner, CheckpointOwner::Fork { .. }) {
        return Ok(CheckpointSweep::Retain);
    }
    // An unexpired pin on a live namespace is exactly what a checkpoint is
    // for. On a tombstone every pin is dead weight, but a create still in
    // flight must not be raced, so that arm waits out the grace window from
    // the record's own creation stamp.
    let releasable = lease_expired(record, context.now_ms)
        || (namespace_deleted
            && context.now_ms.saturating_sub(record.created_at_ms) >= grace_window_ms);
    if !releasable {
        return Ok(CheckpointSweep::Retain);
    }
    let snapshot = matches!(record.owner, CheckpointOwner::Snapshot { .. });
    match release_inspected_checkpoint_record(store, key, loaded, context.now_ms).await? {
        CheckpointRelease::Released if snapshot => Ok(CheckpointSweep::ReleasedSnapshot),
        CheckpointRelease::Released => Ok(CheckpointSweep::Released),
        CheckpointRelease::LostRace => {
            tracing::debug!(
                namespace_id = %namespace_id,
                object_key = key,
                "checkpoint release lost its inspected etag; retaining"
            );
            Ok(CheckpointSweep::Retain)
        }
    }
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

/// Reads how one candidate stands against the grace window.
///
/// Store failures surface unmapped so each collector keeps its own error
/// vocabulary; everything about the reading itself — which timestamp, what
/// an absent one means, what the window is measured against — is the same
/// wherever objects age out, so it is decided once here. What being aged
/// entitles a candidate to is the caller's question: the namespace sweep
/// wants durable evidence of when the object stopped being referenced as
/// well (`gc/live_set.rs`, `ReferenceAnchor`).
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

/// Deletes one unreferenced candidate if the grace window has passed over
/// it, and says what it decided otherwise.
///
/// This is the whole decision where the window's only job is to cover a
/// write that may still be publishing: install debris a repair proved
/// non-completable, and the keyspaces extensions collect for themselves.
/// Where an object can also stop being referenced while something is still
/// reading it, the window has to run from that moment instead, and the
/// namespace sweep is what works out when it was.
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
