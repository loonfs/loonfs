//! The decisions that a collection's sweeps make about listed objects, and
//! the bounded concurrent deletes that carry them out.

use super::snapshots::GraceAge;
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_WRITES_IN_FLIGHT;
use futures::{Stream, TryStreamExt};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::{GcResponse, PoolGcResponse, PoolRetainedReason, RetainedReason};
use std::pin::pin;

/// The report counter that one decision adds to.
pub(super) type Counter<R> = fn(&mut R) -> &mut u64;

/// The report of one collection, which counts what each decision did.
pub(super) trait CollectionReport {
    /// Why the collection kept one candidate.
    type Reason;
    /// The reason for an unreferenced candidate younger than the grace window.
    const WITHIN_GRACE_WINDOW: Self::Reason;
    /// The reason for an unreferenced candidate without a provider timestamp.
    const NO_PROVIDER_TIMESTAMP: Self::Reason;

    /// Counts one kept candidate, and keeps the soonest of the times at which
    /// the kept candidates can be reclaimed.
    fn record_retained(&mut self, reason: Self::Reason, reclaimable_at_ms: Option<u64>);
}

impl CollectionReport for GcResponse {
    type Reason = RetainedReason;
    const WITHIN_GRACE_WINDOW: RetainedReason = RetainedReason::WithinGraceWindow;
    const NO_PROVIDER_TIMESTAMP: RetainedReason = RetainedReason::NoProviderTimestamp;

    fn record_retained(&mut self, reason: RetainedReason, reclaimable_at_ms: Option<u64>) {
        self.retain(reason);
        self.next_reclamation_at_ms = soonest(self.next_reclamation_at_ms, reclaimable_at_ms);
    }
}

impl CollectionReport for PoolGcResponse {
    type Reason = PoolRetainedReason;
    const WITHIN_GRACE_WINDOW: PoolRetainedReason = PoolRetainedReason::WithinGraceWindow;
    const NO_PROVIDER_TIMESTAMP: PoolRetainedReason = PoolRetainedReason::NoProviderTimestamp;

    fn record_retained(&mut self, reason: PoolRetainedReason, reclaimable_at_ms: Option<u64>) {
        self.retain(reason);
        self.next_reclamation_at_ms = soonest(self.next_reclamation_at_ms, reclaimable_at_ms);
    }
}

fn soonest(current_ms: Option<u64>, candidate_ms: Option<u64>) -> Option<u64> {
    current_ms.into_iter().chain(candidate_ms).min()
}

/// What a collection does with one listed object, or with one upload
/// session's records.
pub(super) enum Decision<R: CollectionReport> {
    Retain {
        reason: R::Reason,
        reclaimable_at_ms: Option<u64>,
    },
    /// Deletes `keys` one after another, in order, and counts one deletion.
    Delete {
        keys: Vec<String>,
        counter: Counter<R>,
    },
    Gone,
}

impl<R: CollectionReport> Decision<R> {
    /// Keeps a candidate for `reason`, with a reclamation time only when it's
    /// later than `now_ms`.
    pub(super) fn retain(reason: R::Reason, reclaimable_at_ms: Option<u64>, now_ms: u64) -> Self {
        Self::Retain {
            reason,
            reclaimable_at_ms: reclaimable_at_ms.filter(|at_ms| *at_ms > now_ms),
        }
    }

    /// Deletes an unreferenced `entry` once its provider age is at least
    /// `grace_window_ms` at `now_ms`, and keeps it otherwise.
    pub(super) fn aged(
        entry: ListedObject,
        grace_window_ms: u64,
        now_ms: u64,
        counter: Counter<R>,
    ) -> Self {
        let reason = match GraceAge::of(entry.last_modified_at_ms, grace_window_ms, now_ms) {
            GraceAge::Aged => {
                return Self::Delete {
                    keys: vec![entry.key],
                    counter,
                };
            }
            GraceAge::Young => R::WITHIN_GRACE_WINDOW,
            GraceAge::Unknown => R::NO_PROVIDER_TIMESTAMP,
        };
        Self::retain(reason, None, now_ms)
    }
}

/// Runs the decisions with at most `STORE_WRITES_IN_FLIGHT` deletes in flight and
/// records each outcome in `report`. A delete decision is counted only after
/// its deletes succeeded.
pub(super) async fn apply_decisions<S: ObjectStore + ?Sized, R: CollectionReport>(
    store: &S,
    decisions: impl Stream<Item = Result<Decision<R>>>,
    report: &mut R,
) -> Result<()> {
    let mut outcomes = pin!(decisions
        .map_ok(|decision| async move {
            if let Decision::Delete { keys, .. } = &decision {
                for key in keys {
                    store
                        .delete(key)
                        .await
                        .map_err(|error| CoreError::store(key, &error))?;
                }
            }
            Ok(decision)
        })
        .try_buffer_unordered(STORE_WRITES_IN_FLIGHT));
    while let Some(decision) = outcomes.try_next().await? {
        match decision {
            Decision::Retain {
                reason,
                reclaimable_at_ms,
            } => report.record_retained(reason, reclaimable_at_ms),
            Decision::Delete { counter, .. } => *counter(report) += 1,
            Decision::Gone => {}
        }
    }
    Ok(())
}
