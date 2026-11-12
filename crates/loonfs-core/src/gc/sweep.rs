//! The decisions of a namespace's collection about the objects that it lists
//! under the namespace's prefix.

use super::deletes::{Counter, Decision};
use super::families::CandidateFamily;
use super::live_set::LiveSet;
use super::snapshots::SnapshotSweep;
use crate::error::Result;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::{GcResponse, RetainedReason};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct Sweep<'a, S: ObjectStore> {
    pub(super) namespace: &'a NamespaceStore<S>,
    pub(super) grace_window_ms: u64,
    pub(super) clock: &'a OperationClock,
    pub(super) live: &'a LiveSet,
    /// The bytes of the listed objects that reached this sweep, as the
    /// listings reported them.
    pub(super) listed_bytes: AtomicU64,
}

impl<S: ObjectStore> Sweep<'_, S> {
    pub(super) async fn candidate(
        &self,
        family: CandidateFamily,
        entry: ListedObject,
    ) -> Result<Decision<GcResponse>> {
        self.listed_bytes
            .fetch_add(entry.size_bytes, Ordering::Relaxed);
        if !family.recognizes(&entry.key) {
            return Ok(self.retain(RetainedReason::UnrecognizedKey, None));
        }
        let Some(deleted) = family.deleted_counter() else {
            return self.process_snapshot(entry.key).await;
        };
        if self.live.protects(family, &entry.key) {
            return Ok(self.retain(RetainedReason::Referenced, None));
        }
        Ok(Decision::aged(
            entry,
            family.min_age_ms(self.grace_window_ms),
            self.clock.request_at_ms(),
            deleted,
        ))
    }

    async fn process_snapshot(&self, key: String) -> Result<Decision<GcResponse>> {
        // The collection read its snapshot records while it loaded its roots. A
        // record changes only when an unexpired user snapshot is extended, so a
        // record that the collection read earlier leads to the same decision
        // as one that it reads now.
        let decision = match self.live.snapshot_records.get(&key) {
            Some(record) => {
                self.namespace
                    .sweep_snapshot_record(
                        &key,
                        record,
                        self.grace_window_ms,
                        self.live,
                        self.clock,
                    )
                    .await?
            }
            None => {
                self.namespace
                    .sweep_snapshot(&key, self.grace_window_ms, self.live, self.clock)
                    .await?
            }
        };
        let counter: Counter<GcResponse> = match decision {
            SnapshotSweep::DeleteFork => |report| &mut report.deleted_snapshots_by_owner.fork,
            SnapshotSweep::DeleteUser => |report| &mut report.deleted_snapshots_by_owner.user,
            SnapshotSweep::Gone => return Ok(Decision::Gone),
            SnapshotSweep::Retain { reclaimable_at_ms } => {
                return Ok(self.retain(RetainedReason::SnapshotNotDeletable, reclaimable_at_ms));
            }
        };
        Ok(Decision::Delete {
            keys: vec![key],
            counter,
        })
    }

    fn retain(
        &self,
        reason: RetainedReason,
        reclaimable_at_ms: Option<u64>,
    ) -> Decision<GcResponse> {
        Decision::retain(reason, reclaimable_at_ms, self.clock.request_at_ms())
    }
}
