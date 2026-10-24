//! Bounded concurrent deletes for one collection pass.

use super::sweep::Decision;
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_WRITES_IN_FLIGHT;
use crate::upload_records::delete_upload_session_records;
use futures::{Stream, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::GcResponse;
use std::pin::pin;

pub(super) type DeletedCounter = fn(&mut GcResponse) -> &mut u64;

/// Runs the decisions with at most `STORE_WRITES_IN_FLIGHT` deletes in flight and
/// records each outcome in `report`. A delete decision reaches `record` only
/// after its deletes succeeded.
pub(super) async fn apply_decisions<S: ObjectStore + ?Sized>(
    store: &S,
    decisions: impl Stream<Item = Result<Decision>>,
    report: &mut GcResponse,
) -> Result<()> {
    let mut outcomes = pin!(decisions
        .map_ok(|decision| async move {
            match &decision {
                Decision::Delete { key, .. } => store
                    .delete(key)
                    .await
                    .map_err(|error| CoreError::store(key, &error))?,
                Decision::DeleteUploadSession {
                    namespace_id,
                    upload_id,
                } => delete_upload_session_records(store, namespace_id, upload_id).await?,
                Decision::Retain { .. } | Decision::Gone => {}
            }
            Ok(decision)
        })
        .try_buffer_unordered(STORE_WRITES_IN_FLIGHT));
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
        Decision::DeleteUploadSession { .. } => report.deleted.upload_sessions += 1,
        Decision::Gone => {}
    }
}
