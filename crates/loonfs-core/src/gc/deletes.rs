//! Bounded concurrent deletes for one collection pass.

use super::sweep::Decision;
use crate::error::{CoreError, Result};
use crate::store_waves::STORE_WRITE_WAVE;
use futures::{future, FutureExt, Stream, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::GcResponse;
use std::pin::pin;

pub(super) type DeletedCounter = fn(&mut GcResponse) -> &mut u64;

/// Runs the decisions with at most `STORE_WRITE_WAVE` deletes in flight and
/// records each outcome in `report`. A `Delete` reaches `record` only after
/// its delete succeeded.
pub(super) async fn apply_decisions<S: ObjectStore + ?Sized>(
    store: &S,
    decisions: impl Stream<Item = Result<Decision>>,
    report: &mut GcResponse,
) -> Result<()> {
    let mut outcomes = pin!(decisions
        .map_ok(|decision| match decision {
            Decision::Delete { key, counter } => async move {
                store
                    .delete(&key)
                    .await
                    .map_err(|error| CoreError::store(&key, &error))?;
                Ok(Decision::Delete { key, counter })
            }
            .left_future(),
            decision => future::ready(Ok(decision)).right_future(),
        })
        .try_buffer_unordered(STORE_WRITE_WAVE));
    while let Some(decision) = outcomes.try_next().await? {
        record(decision, report);
    }
    Ok(())
}

fn record(decision: Decision, report: &mut GcResponse) {
    match decision {
        Decision::Retain {
            reason,
            reclaimable_at_ms,
        } => {
            report.retain(reason);
            if let Some(at_ms) = reclaimable_at_ms {
                report.next_reclamation_at_ms = Some(
                    report
                        .next_reclamation_at_ms
                        .map_or(at_ms, |soonest_ms| soonest_ms.min(at_ms)),
                );
            }
        }
        Decision::Delete { counter, .. } => *counter(report) += 1,
        Decision::Gone => {}
    }
}
